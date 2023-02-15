import asyncio

import pytest

from connect.eaas.core.decorators import (
    anvil_key_variable,
)
from connect.eaas.core.extension import (
    AnvilApplicationBase,
)
from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
    SetupResponse,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.anvil import (
    AnvilApp,
)
from connect.eaas.runner.workers.anvil import (
    AnvilWorker,
    start_anvilapp_worker_process,
)
from tests.utils import (
    WSHandler,
)


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

    @anvil_key_variable('MY_ANVIL_API_KEY')
    class MyExtension(AnvilApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        AnvilApp,
        'load_application',
        return_value=MyExtension,
    )
    mocker.patch.object(AnvilApp, 'start')
    mocker.patch.object(AnvilApp, 'stop')
    mocker.patch('connect.eaas.runner.workers.anvil.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(**settings_payload),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/anvilapp',
        data_to_send,
        ['receive', 'send'],
    )
    worker = None

    config = ConfigHelper(secure=False)
    ext_handler = AnvilApp(config)

    async with ws_server(handler):
        worker = AnvilWorker(
            ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock(),
        )
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    msg = Message(
        version=2,
        message_type=MessageType.SETUP_REQUEST,
        data=SetupRequest(
            event_subscriptions=None,
            variables=[
                {'name': 'MY_ANVIL_API_KEY', 'initial_value': 'changeme!', 'secure': True},
            ],
            schedulables=None,
            anvil_callables=MyExtension.get_anvil_callables(),
            repository={
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            },
            runner_version='24.1',
        ),
    )

    handler.assert_received(msg.dict())

    assert worker.config.variables == {
        var['name']: var['value']
        for var in settings_payload['variables']
    }
    assert worker.config.logging_api_key == settings_payload['logging']['logging_api_key']
    assert worker.config.environment_type == settings_payload['environment_type']
    assert worker.config.account_id == settings_payload['logging']['meta']['account_id']
    assert worker.config.account_name == settings_payload['logging']['meta']['account_name']
    assert worker.config.service_id == settings_payload['logging']['meta']['service_id']


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

    @anvil_key_variable('MY_ANVIL_API_KEY')
    class MyExtension(AnvilApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        AnvilApp,
        'load_application',
        return_value=MyExtension,
    )

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(message_type=MessageType.SHUTDOWN).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/anvilapp',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = AnvilApp(config)

    async with ws_server(handler):
        worker = AnvilWorker(
            ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock(),
        )
        asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        assert worker.run_event.is_set() is False


def test_start_anvilapp_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocker.patch.object(
        AnvilApp,
        'load_application',
    )
    mocker.patch.object(AnvilWorker, 'start', start_mock)
    mocked_configure_logger = mocker.patch('connect.eaas.runner.workers.anvil.configure_logger')

    start_anvilapp_worker_process(
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
        True,
        False,
    )

    mocked_configure_logger.assert_called_once_with(True, False)
    start_mock.assert_awaited_once()
