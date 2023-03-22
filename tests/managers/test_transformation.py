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
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.transformations import (
    TfnApp,
)
from connect.eaas.runner.managers import (
    TransformationTasksManager,
)


@pytest.mark.asyncio
async def test_submit(mocker, tfn_settings_payload, responses, httpx_mock, unused_port):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
            'transformation_task_max_execution_time': 300,
            'transformation_write_queue_timeout': 600,
            'row_transformation_task_max_execution_time': 60,
        },
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
        url=f'{api_url}/billing/requests/TFR-000',
        status_code=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/billing/requests/TFR-000/process',
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
            return {'id': row['id']}

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
        },
        input={
            'event_type': 'transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(1)

    task.output = TaskOutput(
        result=ResultType.SUCCESS,
        runtime=1.0,
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task

    requests = httpx_mock.get_requests()
    assert len(requests) == 12


@pytest.mark.asyncio
async def test_submit_with_error_in_tfn_function(
    mocker, tfn_settings_payload, responses, httpx_mock, unused_port,
):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
            'transformation_task_max_execution_time': 300,
            'transformation_write_queue_timeout': 0.2,
            'row_transformation_task_max_execution_time': 60,
        },
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
        method='PUT',
        url=f'{api_url}/billing/requests/TFR-000',
        status_code=200,
    )

    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/billing/requests/TFR-000/fail',
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
            return {
                'id': row['id'],
                'price': row['price'],
            }

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocker.patch('connect.eaas.runner.managers.transformation.Workbook')
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
        },
        input={
            'event_type': 'transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(1)

    task.output = TaskOutput(
        result=ResultType.FAIL,
        runtime=1.0,
        message='Ooops',
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task


@pytest.mark.asyncio
async def test_build_response_exception(mocker, task_payload, httpx_mock, unused_port):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
            'transformation_task_max_execution_time': 300,
            'transformation_write_queue_timeout': 0.2,
            'row_transformation_task_max_execution_time': 60,
        },
    )
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/billing/requests/TFR-000/fail',
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
            'transformation_request',
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


@pytest.mark.asyncio
async def test_build_response_exception_fail_failing_trans_req(
    mocker, task_payload, httpx_mock, unused_port, caplog,
):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
            'transformation_task_max_execution_time': 300,
            'transformation_write_queue_timeout': 0.2,
            'row_transformation_task_max_execution_time': 60,
        },
    )
    api_address = f'https://127.0.0.1:{unused_port}'
    api_url = f'{api_address}/public/v1'
    httpx_mock.add_response(
        method='POST',
        url=f'{api_url}/billing/requests/TFR-000/fail',
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
            'transformation_request',
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


@pytest.mark.asyncio
async def test_send_skip_response(
    mocker, task_payload, unused_port, tfn_settings_payload, httpx_mock,
):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
            'transformation_task_max_execution_time': 300,
        },
    )

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
    mocked_time.sleep = time.sleep
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
            'event_type': 'transformation_request',
            'object_id': 'TFR-000',
            'data': {'method': 'transform_it'},
        },
    )

    await manager.submit(task)
    await asyncio.sleep(1)

    task.output = TaskOutput(
        result=ResultType.SKIP,
        runtime=1.0,
        message='The request status does not match the supported statuses: .',
    )
    assert result_queue.qsize() == 1
    result = await result_queue.get()
    assert result == task
