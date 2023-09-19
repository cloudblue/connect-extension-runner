import asyncio
import logging
import time

import pytest

from connect.eaas.core.decorators import (
    transformation,
)
from connect.eaas.core.enums import (
    ResultType,
    TaskCategory,
)
from connect.eaas.core.extension import (
    TransformationsApplicationBase,
)
from connect.eaas.core.proto import (
    SetupResponse,
    Task,
    TaskOutput,
)
from connect.eaas.core.responses import (
    RowTransformationResponse,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.constants import (
    ROW_DELETED_MARKER,
)
from connect.eaas.runner.handlers.transformations import (
    TfnApp,
)
from connect.eaas.runner.managers import (
    TransformationTasksManager,
)
from connect.eaas.runner.managers.transformation import (
    RowTransformationError,
)


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.parametrize('max_parallel_lines', (1, 20))
@pytest.mark.flaky(max_runs=3, min_passes=1)
@pytest.mark.asyncio
async def test_submit(
    mocker, default_env, tfn_settings_payload, responses,
    httpx_mock, unused_port, task_type_prefix,
    max_parallel_lines,
):
    mocker.patch(
        'connect.eaas.runner.managers.transformation.TRANSFORMATION_TASK_MAX_PARALLEL_LINES',
        max_parallel_lines,
    )
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    mocker.patch.object(ConfigHelper, 'get_api_address', return_value=api_address)

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**tfn_settings_payload))

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection?and(eq(id,TFR-000),eq(status,pending))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/TFR-000',
        json={
            'id': 'TFR-000',
            'status': 'pending',
            'files': {
                'input': {
                    'id': 'MFL-0001',
                    'name': '/path/to/input.xlsx',
                },
                'output': {
                    'id': None,
                    'name': None,
                },
            },
            'batch': {'id': 'TRB-0001'},
            'transformation': {
                'columns': {
                    'input': [
                        {'name': 'id', 'type': 'str'},
                        {'name': 'price', 'type': 'str', 'nullable': True},
                        {'name': 'sub_id', 'type': 'str', 'precision': 2},
                        {'name': 'delivered', 'type': 'bool', 'precision': 2},
                        {'name': 'purchase_time', 'type': 'str', 'precision': 2},
                    ],
                    'output': [
                        {'name': 'id', 'type': 'str'},
                    ],
                },
            },
            'stats': {
                'rows': {
                    'total': 7,
                    'processed': 0,
                },
            },
        },
    )

    httpx_mock.add_response(
        method='PUT',
        url=f'{api_url}/{task_type_prefix}/requests/TFR-000',
        status_code=200,
    )

    responses.add(
        responses.PUT,
        f'{api_url}/{task_type_prefix}/requests/TFR-000',
        status=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/{task_type_prefix}/requests/TFR-000/process',
        status_code=201,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/media/folders/streams_batches/TRB-0001/files',
        status_code=201,
        content=b'{"id": "MFL-001"}',
    )

    with open('tests/test_data/input_file_example.xlsx', 'rb') as input_file:
        responses.add(
            responses.GET,
            f'{api_address}/path/to/input.xlsx',
            body=input_file.read(),
            status=200,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    class MyExtension(TransformationsApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'audience': ['vendor'],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @transformation(
            name='my transformation',
            description='The my transformation',
            edit_dialog_ui='/static/my_settings.html',
        )
        def transform_it(self, row):
            return RowTransformationResponse.done({'id': row['id']})

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocked_time = mocker.patch('connect.eaas.runner.managers.transformation.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = TfnApp(config)

    result_queue = asyncio.Queue()
    manager = TransformationTasksManager(config, handler, result_queue.put)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.TRANSFORMATION,
            'api_key': 'ApiKey SU-000:xxxx',
        },
        input={
            'event_type': f'{task_type_prefix}_transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.5)

    task.output = TaskOutput(
        result=ResultType.SUCCESS,
        runtime=1.0,
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task

    requests = httpx_mock.get_requests()
    assert len(requests) == 5

    assert len(responses.calls) == 8


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.parametrize('max_parallel_lines', (1, 20))
@pytest.mark.flaky(max_runs=3, min_passes=1)
@pytest.mark.asyncio
async def test_submit_with_error_in_tfn_function(
    mocker, default_env, tfn_settings_payload, responses,
    httpx_mock, unused_port, task_type_prefix,
    max_parallel_lines,
):
    mocker.patch(
        'connect.eaas.runner.managers.transformation.TRANSFORMATION_TASK_MAX_PARALLEL_LINES',
        max_parallel_lines,
    )
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    mocker.patch.object(ConfigHelper, 'get_api_address', return_value=api_address)

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**tfn_settings_payload))

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection?and(eq(id,TFR-000),eq(status,pending))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/TFR-000',
        json={
            'id': 'TFR-000',
            'status': 'pending',
            'files': {
                'input': {
                    'id': 'MFL-0001',
                    'name': '/public/v1/path/to/input.xlsx',
                },
                'output': {
                    'id': None,
                    'name': None,
                },
            },
            'transformation': {
                'columns': {
                    'input': [
                        {'name': 'id', 'type': 'str'},
                        {'name': 'price', 'type': 'str', 'nullable': True},
                        {'name': 'sub_id', 'type': 'str', 'precision': 2},
                        {'name': 'delivered', 'type': 'bool', 'precision': 2},
                        {'name': 'purchase_time', 'type': 'str', 'precision': 2},
                    ],
                    'output': [
                        {'name': 'id', 'type': 'str'},
                        {'name': 'price', 'type': 'str', 'nullable': True},
                    ],
                },
            },
            'stats': {
                'rows': {
                    'total': 7,
                    'processed': 0,
                },
            },
        },
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/{task_type_prefix}/requests/TFR-000/fail',
        status_code=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/conversations/TFR-000/messages',
        status_code=201,
    )

    with open('tests/test_data/input_file_example.xlsx', 'rb') as input_file:
        responses.add(
            responses.GET,
            f'{api_address}/public/v1/path/to/input.xlsx',
            body=input_file.read(),
            status=200,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    class MyExtension(TransformationsApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'audience': ['vendor'],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @transformation(
            name='my transformation',
            description='The my transformation',
            edit_dialog_ui='/static/my_settings.html',
        )
        async def transform_it(self, row):
            if row['id'] == 6:
                raise ValueError('Ooops')
            return RowTransformationResponse.done({
                'id': row['id'],
                'price': row['price'],
            })

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocked_time = mocker.patch('connect.eaas.runner.managers.transformation.time')
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = TfnApp(config)

    result_queue = asyncio.Queue()
    mocker.patch.object(TransformationTasksManager, 'send_stat_update')
    mocker.patch.object(TransformationTasksManager, 'write_excel')
    manager = TransformationTasksManager(config, handler, result_queue.put)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.TRANSFORMATION,
            'api_key': 'ApiKey SU-000:xxxx',
        },
        input={
            'event_type': f'{task_type_prefix}_transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.5)

    task.output = TaskOutput(
        result=ResultType.FAIL,
        runtime=1.0,
        message='Error applying transformation function transform_it to row #7: Ooops.',
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.flaky(max_runs=3, min_passes=1)
@pytest.mark.asyncio
async def test_submit_with_error_in_tfn_function_sync(
    mocker, default_env, tfn_settings_payload, responses,
    httpx_mock, unused_port, task_type_prefix,
):
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    mocker.patch.object(ConfigHelper, 'get_api_address', return_value=api_address)

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**tfn_settings_payload))

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection?and(eq(id,TFR-000),eq(status,pending))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/TFR-000',
        json={
            'id': 'TFR-000',
            'status': 'pending',
            'files': {
                'input': {
                    'id': 'MFL-0001',
                    'name': '/public/v1/path/to/input.xlsx',
                },
                'output': {
                    'id': None,
                    'name': None,
                },
            },
            'transformation': {
                'columns': {
                    'input': [
                        {'name': 'id', 'type': 'str'},
                        {'name': 'price', 'type': 'str', 'nullable': True},
                        {'name': 'sub_id', 'type': 'str', 'precision': 2},
                        {'name': 'delivered', 'type': 'bool', 'precision': 2},
                        {'name': 'purchase_time', 'type': 'str', 'precision': 2},
                    ],
                    'output': [
                        {'name': 'id', 'type': 'str'},
                        {'name': 'price', 'type': 'str', 'nullable': True},
                    ],
                },
            },
            'stats': {
                'rows': {
                    'total': 7,
                    'processed': 0,
                },
            },
        },
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/{task_type_prefix}/requests/TFR-000/fail',
        status_code=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/conversations/TFR-000/messages',
        status_code=201,
    )

    with open('tests/test_data/input_file_example.xlsx', 'rb') as input_file:
        responses.add(
            responses.GET,
            f'{api_address}/public/v1/path/to/input.xlsx',
            body=input_file.read(),
            status=200,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    class MyExtension(TransformationsApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'audience': ['vendor'],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @transformation(
            name='my transformation',
            description='The my transformation',
            edit_dialog_ui='/static/my_settings.html',
        )
        def transform_it(self, row):
            if row['id'] == 6:
                raise ValueError('Ooops')
            return RowTransformationResponse.done({
                'id': row['id'],
                'price': row['price'],
            })

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocker.patch.object(TransformationTasksManager, 'send_stat_update')
    mocker.patch.object(TransformationTasksManager, 'write_excel')
    mocked_time = mocker.patch('connect.eaas.runner.managers.transformation.time')
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = TfnApp(config)

    result_queue = asyncio.Queue()
    manager = TransformationTasksManager(config, handler, result_queue.put)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.TRANSFORMATION,
            'api_key': 'ApiKey SU-000:xxxxx',
        },
        input={
            'event_type': f'{task_type_prefix}_transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.5)

    task.output = TaskOutput(
        result=ResultType.FAIL,
        runtime=1.0,
        message='Error applying transformation function transform_it to row #7: Ooops.',
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.asyncio
async def test_build_response_exception(
    mocker, default_env, task_payload, httpx_mock, unused_port, task_type_prefix,
):
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/{task_type_prefix}/requests/TFR-000/fail',
        status_code=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/conversations/TFR-000/messages',
        status_code=201,
    )
    config = ConfigHelper()
    manager = TransformationTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = Task(
        **task_payload(
            TaskCategory.TRANSFORMATION,
            f'{task_type_prefix}_transformation_request',
            'TFR-000',
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Ooops'))
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == ResultType.FAIL
    assert 'Ooops' in response.output.message
    manager.log_exception.assert_called_once()


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.asyncio
async def test_build_response_exception_fail_failing_trans_req(
    mocker, default_env, task_payload, httpx_mock, unused_port, caplog, task_type_prefix,
):
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/{task_type_prefix}/requests/TFR-000/fail',
        status_code=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/conversations/TFR-000/messages',
        status_code=400,
    )
    config = ConfigHelper()
    manager = TransformationTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = Task(
        **task_payload(
            TaskCategory.TRANSFORMATION,
            f'{task_type_prefix}_transformation_request',
            'TFR-000',
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Ooops'))
    with caplog.at_level(logging.ERROR):
        response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == ResultType.FAIL
    assert 'Ooops' in response.output.message
    manager.log_exception.assert_called_once()
    assert 'Cannot fail the transformation request' in caplog.text


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.asyncio
async def test_send_skip_response(
    mocker, default_env, unused_port, tfn_settings_payload, httpx_mock, task_type_prefix,
):
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**tfn_settings_payload))

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection?and(eq(id,TFR-000),eq(status,pending))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/0'},
    )

    class MyExtension(TransformationsApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'audience': ['vendor'],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @transformation(
            name='my transformation',
            description='The my transformation',
            edit_dialog_ui='/static/my_settings.html',
        )
        def transform_it(self, row):
            pass

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocked_time = mocker.patch('connect.eaas.runner.managers.transformation.time')
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = TfnApp(config)

    result_queue = asyncio.Queue()
    manager = TransformationTasksManager(config, handler, result_queue.put)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.TRANSFORMATION,
        },
        input={
            'event_type': f'{task_type_prefix}_transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.5)

    task.output = TaskOutput(
        result=ResultType.SKIP,
        runtime=1.0,
        message='The request status does not match the supported statuses: .',
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task


@pytest.mark.asyncio
async def test_transform_row_invalid_response(mocker, default_env):
    manager = TransformationTasksManager(ConfigHelper(), mocker.MagicMock(), mocker.MagicMock())

    async def tfn(row):
        return 33

    with pytest.raises(RowTransformationError) as cv:
        await manager.transform_row(
            tfn,
            3,
            {'row': 'data'},
            {},
        )

    assert str(cv.value).endswith('invalid row tranformation response: 33.')


@pytest.mark.asyncio
async def test_transform_row_fail_response(mocker, default_env):
    manager = TransformationTasksManager(ConfigHelper(), mocker.MagicMock(), mocker.MagicMock())

    async def tfn(row):
        return RowTransformationResponse.fail(output='Failed by me')

    with pytest.raises(RowTransformationError) as cv:
        await manager.transform_row(
            tfn,
            3,
            {'row': 'data'},
            {},
        )

    assert str(cv.value).endswith('row transformation failed: Failed by me.')


@pytest.mark.asyncio
async def test_transform_row_new_version(mocker, default_env):
    manager = TransformationTasksManager(ConfigHelper(), mocker.MagicMock(), mocker.MagicMock())

    response = RowTransformationResponse.done(
        {'row': 'row'},
        {'row': 'style'},
    )

    async def tfn(row, row_styles):
        return response

    result_store_mock = mocker.MagicMock()
    result_store_mock.put = mocker.AsyncMock()

    assert await manager.transform_row(
        tfn,
        3,
        {'row': 'data'},
        {'row': 'style'},
    ) == response


@pytest.mark.asyncio
async def test_transform_row_deleted_row(mocker, default_env):
    manager = TransformationTasksManager(ConfigHelper(), mocker.MagicMock(), mocker.MagicMock())

    tfn = mocker.AsyncMock()
    tfn.__name__ = 'my_func'

    result = await manager.transform_row(
        tfn,
        3,
        {'row': ROW_DELETED_MARKER},
        {},
    )

    assert result.status == ResultType.DELETE
    tfn.assert_not_awaited()


def test_generate_output_row_skip(mocker):
    column_names = ['A', 'B']
    manager = TransformationTasksManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    response = RowTransformationResponse.skip()

    row = manager.generate_output_row(mocker.MagicMock(), column_names, response)

    assert row == ['#N/A', '#N/A']


def test_generate_output_row_delete(mocker):
    column_names = ['A', 'B']
    manager = TransformationTasksManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    response = RowTransformationResponse.delete()

    row = manager.generate_output_row(mocker.MagicMock(), column_names, response)

    assert row == ['#INSTRUCTION/DELETE_ROW', '#INSTRUCTION/DELETE_ROW']


def test_generate_output_row_invalid_status(mocker):
    column_names = ['A', 'B']
    manager = TransformationTasksManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    response = RowTransformationResponse('reschedule')

    with pytest.raises(Exception) as cv:
        manager.generate_output_row(mocker.MagicMock(), column_names, response)

    assert str(cv.value) == 'Invalid row transformation response status: reschedule.'
