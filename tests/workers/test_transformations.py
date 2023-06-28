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
    Message,
    MessageType,
    SetupRequest,
    SetupResponse,
    Task,
)
from connect.eaas.core.responses import (
    TransformationResponse,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.transformations import (
    TfnApp,
)
from connect.eaas.runner.workers.transformations import (
    TransformationWorker,
    start_tfnapp_worker_process,
)
from tests.utils import (
    WSHandler,
)


@pytest.mark.asyncio
async def test_extension_settings(mocker, ws_server, unused_port, tfn_settings_payload):
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
            'webapp_port': 53575,
        },
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
            return row

    mocker.patch.object(
        TfnApp,
        'load_application',
        return_value=MyExtension,
    )

    mocker.patch('connect.eaas.runner.workers.transformations.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(**tfn_settings_payload),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/tfnapp',
        data_to_send,
        ['receive', 'send'],
    )
    worker = None

    config = ConfigHelper(secure=False)
    ext_handler = TfnApp(config)

    async with ws_server(handler):
        worker = TransformationWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    msg = Message(
        version=2,
        message_type=MessageType.SETUP_REQUEST,
        data=SetupRequest(
            transformations=[{
                'name': 'my transformation',
                'description': 'The my transformation',
                'edit_dialog_ui': '/static/my_settings.html',
                'method': 'transform_it',
            }],
            repository={
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            },
            runner_version='24.1',
            audience=['vendor'],
        ),
    )

    handler.assert_received(msg.dict())


@pytest.mark.asyncio
async def test_shutdown(mocker, ws_server, unused_port, tfn_settings_payload):

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
            return row

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**tfn_settings_payload),
        ).dict(),
        Message(message_type=MessageType.SHUTDOWN).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/tfnapp',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = TfnApp(config)

    async with ws_server(handler):
        worker = TransformationWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        assert worker.run_event.is_set() is False


@pytest.mark.asyncio
async def test_shutdown_pending_task_timeout(
    mocker, ws_server, unused_port, tfn_settings_payload, caplog,
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
            'transformation_task_max_execution_time': .1,
        },
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
            return row

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocker.patch(
        'connect.eaas.runner.workers.transformations.RESULT_SENDER_WAIT_GRACE_SECONDS',
        .1,
    )

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**tfn_settings_payload),
        ).dict(),
        Message(message_type=MessageType.SHUTDOWN).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/tfnapp',
        data_to_send,
        ['receive', 'send', 'send'] + ['receive' for _ in range(10)],
    )

    task_result = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': 'transformation',
        },
        input={
            'event_type': 'transformation_request',
            'object_id': 'TRF-000',
        },
    )

    config = ConfigHelper(secure=False)
    ext_handler = TfnApp(config)

    with caplog.at_level(logging.ERROR):
        async with ws_server(handler):
            worker = TransformationWorker(
                ext_handler,
                mocker.MagicMock(),
                mocker.MagicMock(),
                mocker.MagicMock(),
            )
            for _ in range(10):
                await worker.results_queue.put(task_result)
            asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
    assert 'Cannot send all results timeout of 0.2 exceeded, cancel task' in caplog.text


def test_start_tfnapp_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocker.patch.object(TfnApp, 'load_application')
    mocker.patch.object(TransformationWorker, 'start', start_mock)
    mocked_configure_logger = mocker.patch(
        'connect.eaas.runner.workers.transformations.configure_logger',
    )

    handler_class_mock = mocker.MagicMock()
    config_mock = mocker.MagicMock()

    start_tfnapp_worker_process(
        handler_class_mock,
        config_mock,
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
        True,
        False,
    )

    mocked_configure_logger.assert_called_once_with(True, False)
    start_mock.assert_awaited_once()
    handler_class_mock.assert_called_once_with(config_mock)


@pytest.mark.parametrize('task_type_prefix', ('billing', 'pricing'))
@pytest.mark.asyncio
async def test_task(
    mocker, ws_server, unused_port, httpx_mock, tfn_settings_payload, task_type_prefix,
):

    task_data = {'id': 'TRF-000', 'method': 'process_it', 'status': 'pending'}

    api_url = f'https://127.0.0.1:{unused_port}/public/v1'

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/collection?'
            'and(eq(id,TRF-000),eq(status,pending))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/TRF-000',
        json=task_data,
    )

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
            return row

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocker.patch(
        'connect.eaas.runner.workers.transformations.get_version',
        return_value='24.1',
    )

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**tfn_settings_payload),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.TRANSFORMATION,
                },
                input={
                    'event_type': f'{task_type_prefix}_transformation_request',
                    'object_id': 'TRF-000',
                    'data': {'method': 'transform_it'},
                },
            ),
        ).dict(),
    ]
    mocked_time = mocker.patch('connect.eaas.runner.managers.transformation.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/tfnapp',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = TfnApp(config)

    async with ws_server(handler):
        worker = TransformationWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        worker.manager.process_transformation = mocker.AsyncMock(
            return_value=TransformationResponse.done(),
        )
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.TRANSFORMATION,
                },
                input={
                    'event_type': f'{task_type_prefix}_transformation_request',
                    'object_id': 'TRF-000',
                    'data': {'method': 'transform_it'},
                },
                output={
                    'result': ResultType.SUCCESS,
                    'runtime': 1.0,
                },
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_sender_max_retries_exceeded(mocker, tfn_settings_payload, task_payload, caplog):
    mocker.patch('connect.eaas.runner.workers.transformations.RESULT_SENDER_MAX_RETRIES', 3)
    mocker.patch(
        'connect.eaas.runner.workers.transformations.DELAY_ON_CONNECT_EXCEPTION_SECONDS',
        0.001,
    )
    mocker.patch.object(TfnApp, 'load_application')

    config = ConfigHelper(secure=False)
    ext_handler = TfnApp(config)

    with caplog.at_level(logging.WARNING):
        worker = TransformationWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        worker.get_extension_message = mocker.MagicMock(return_value={})
        worker.config.update_dynamic_config(SetupResponse(**tfn_settings_payload))
        worker.send = mocker.AsyncMock(
            side_effect=[Exception('retry') for _ in range(3)],
        )
        worker.run = mocker.AsyncMock()
        worker.ws = mocker.AsyncMock(closed=False)
        await worker.results_queue.put(
            Task(**task_payload(TaskCategory.TRANSFORMATION, 'test', 'TQ-000')),
        )
        assert worker.results_queue.empty() is False
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.01)
        worker.stop()
        await task

    assert (
        'Max retries exceeded (3) for sending results of transformation task TQ-000'
        in caplog.text
    )
