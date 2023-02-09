import asyncio

import pytest

from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
    SetupResponse,
)
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.transformations import TfnApp
from connect.eaas.runner.workers.transformations import (
    start_tfnapp_worker_process,
    TransformationWorker,
)

from tests.utils import WSHandler


@pytest.mark.asyncio
async def test_extension_settings(mocker, ws_server, unused_port, settings_payload):
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
            'webapp_port': 53575,
        },
    )

    mocker.patch.object(TfnApp, 'load_application')
    mocker.patch.object(TfnApp, 'get_descriptor', return_value={
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'audience': ['vendor'],
    })
    mocked_trans = mocker.patch.object(
        TfnApp,
        'transformations',
        new_callable=mocker.PropertyMock,
    )
    mocked_trans.return_value = [{
        'name': 'my transformation',
        'description': 'The my transformation',
        'edit_dialog_ui': '/static/my_settings.html',
        'method': 'transform_it',
    }]

    mocker.patch('connect.eaas.runner.workers.transformations.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(**settings_payload),
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
                'readme_url': 'https://readme.com',
                'changelog_url': 'https://changelog.org',
            },
            runner_version='24.1',
            audience=['vendor'],
        ),
    )

    handler.assert_received(msg.dict())


@pytest.mark.asyncio
async def test_shutdown(mocker, ws_server, unused_port, settings_payload):

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
        },
    )

    mocker.patch.object(TfnApp, 'load_application')
    mocker.patch.object(TfnApp, 'get_descriptor', return_value={
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'audience': ['vendor'],
    })

    mocked_trans = mocker.patch.object(
        TfnApp,
        'transformations',
        new_callable=mocker.PropertyMock,
    )
    mocked_trans.return_value = [{
        'name': 'my transformation',
        'description': 'The my transformation',
        'edit_dialog_ui': '/static/my_settings.html',
        'method': 'transform_it',
    }]

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
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
