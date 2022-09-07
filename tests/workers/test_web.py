import asyncio
import copy

import pytest

from connect.eaas.core.extension import WebAppExtension
from connect.eaas.core.proto import (
    HttpRequest,
    Message,
    MessageType,
    SetupRequest,
    SetupResponse,
    WebTask,
    WebTaskOptions,
)
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.workers.web import start_webapp_worker_process, WebWorker
from connect.eaas.runner.handlers.web import WebApp

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

    ui_modules = {
        'settings': {
            'label': 'Settings',
            'url': '/static/settings.html',
        },
    }

    class MyExtension(WebAppExtension):
        @classmethod
        def get_descriptor(cls):
            return {
                'ui': ui_modules,
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
        return_value=MyExtension,
    )

    mocker.patch('connect.eaas.runner.workers.web.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(**settings_payload),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/webapp',
        data_to_send,
        ['receive', 'send'],
    )
    worker = None

    config = ConfigHelper(secure=False)
    ext_handler = WebApp(config)

    async with ws_server(handler):
        worker = WebWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    msg = Message(
        version=2,
        message_type=MessageType.SETUP_REQUEST,
        data=SetupRequest(
            app_type='web',
            event_subscriptions=None,
            ui_modules=ui_modules,
            variables=[],
            schedulables=None,
            repository={
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            },
            runner_version='24.1',
        ),
    )

    handler.assert_received(msg.dict())

    assert worker.config.variables == settings_payload['variables']
    assert worker.config.logging_api_key == settings_payload['logging']['logging_api_key']
    assert worker.config.environment_type == settings_payload['environment_type']
    assert worker.config.account_id == settings_payload['logging']['meta']['account_id']
    assert worker.config.account_name == settings_payload['logging']['meta']['account_name']
    assert worker.config.service_id == settings_payload['logging']['meta']['service_id']


@pytest.mark.asyncio
async def test_http_call(mocker, ws_server, unused_port, httpx_mock, settings_payload):
    setup_response = copy.deepcopy(settings_payload)
    setup_response['logging']['logging_api_key'] = 'logging_api_key'
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

    ui_modules = {
        'settings': {
            'label': 'Settings',
            'url': '/static/settings.html',
        },
    }

    class MyExtension(WebAppExtension):
        @classmethod
        def get_descriptor(cls):
            return {
                'ui': ui_modules,
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
        return_value=MyExtension,
    )

    mocker.patch('connect.eaas.runner.workers.web.get_version', return_value='24.1')

    mocked_cycle = mocker.AsyncMock()

    mocked_cycle_cls = mocker.patch(
        'connect.eaas.runner.workers.web.RequestResponseCycle',
        return_value=mocked_cycle,
    )

    web_task = WebTask(
        options=WebTaskOptions(
            correlation_id='correlation_id',
            reply_to='reply_to',
            api_key='api_key',
            installation_id='installation_id',
        ),
        request=HttpRequest(
            method='GET',
            url='/test/url',
            headers={'X-My-Header': 'my-value'},
        ),
    )

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**setup_response),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.WEB_TASK,
            data=web_task,
        ).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/webapp',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = WebApp(config)

    worker = None
    async with ws_server(handler):
        worker = WebWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                app_type='web',
                event_subscriptions=None,
                ui_modules=ui_modules,
                variables=[],
                schedulables=None,
                repository={
                    'readme_url': 'https://read.me',
                    'changelog_url': 'https://change.log',
                },
                runner_version='24.1',
            ),
        ),
    )
    assert mocked_cycle_cls.call_count == 1
    assert mocked_cycle_cls.mock_calls[0].args[0] == config
    assert mocked_cycle_cls.mock_calls[0].args[1] == ext_handler.app
    assert mocked_cycle_cls.mock_calls[0].args[2].dict() == web_task.dict()
    assert mocked_cycle_cls.mock_calls[0].args[3] == worker.send
    mocked_cycle.assert_awaited_once()


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
            'webapp_port': 53575,
        },
    )

    ui_modules = {
        'settings': {
            'label': 'Settings',
            'url': '/static/settings.html',
        },
    }

    class MyExtension(WebAppExtension):
        @classmethod
        def get_descriptor(cls):
            return {
                'ui': ui_modules,
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
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
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/webapp',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = WebApp(config)

    async with ws_server(handler):
        worker = WebWorker(ext_handler)
        asyncio.create_task(worker.start())
        await asyncio.sleep(1)
        assert worker.run_event.is_set() is False


def test_start_webapp_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
    )
    mocker.patch.object(WebWorker, 'start', start_mock)

    start_webapp_worker_process(mocker.MagicMock())

    start_mock.assert_awaited_once()
