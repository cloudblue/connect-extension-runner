import asyncio
import copy
import logging

import pytest
from websockets.exceptions import ConnectionClosedOK

from connect.eaas.core.decorators import (
    account_settings_page, router, web_app,
)
from connect.eaas.core.extension import WebAppExtension
from connect.eaas.core.proto import (
    HttpRequest,
    HttpResponse,
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

    @account_settings_page('Settings', '/static/settings.html')
    class MyExtension(WebAppExtension):
        @classmethod
        def get_descriptor(cls):
            return {
                'name': 'name',
                'description': 'description',
                'version': '0.1.2',
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


@pytest.mark.parametrize(
    'task_options',
    (
        WebTaskOptions(
            correlation_id='correlation_id',
            reply_to='reply_to',
            api_key='api_key',
            installation_id='installation_id',
        ),
        WebTaskOptions(
            correlation_id='correlation_id',
            reply_to='reply_to',
            api_key='api_key',
            installation_id='installation_id',
            connect_correlation_id='connect_correlation_id',
            user_id='user_id',
            account_id='account_id',
            account_role='account_role',
            call_type='user',
            call_source='ui',
        ),
    ),
)
@pytest.mark.asyncio
async def test_http_call(mocker, ws_server, unused_port, settings_payload, task_options):
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

    @account_settings_page('Settings', '/static/settings.html')
    @web_app(router)
    class MyExtension(WebAppExtension):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

        @classmethod
        def get_static_root(cls):
            return None

        @router.get('/test/url')
        def test_url(self):
            return {'test': 'ok'}

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
        return_value=MyExtension,
    )

    mocker.patch('connect.eaas.runner.workers.web.get_version', return_value='24.1')

    web_task = WebTask(
        options=task_options,
        request=HttpRequest(
            method='GET',
            url='/api/test/url',
            headers={},
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
        ['receive', 'send', 'send', 'receive'],
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
    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.WEB_TASK,
            data=WebTask(
                options=task_options,
                request=HttpRequest(
                    method='GET',
                    url='/api/test/url',
                    headers={},
                ),
                response=HttpResponse(
                    status=200,
                    headers={'content-length': '13', 'content-type': 'application/json'},
                    content='eyJ0ZXN0Ijoib2sifQ==\n',
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_http_call_exception(mocker, ws_server, unused_port, settings_payload):
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

    @account_settings_page('Settings', '/static/settings.html')
    @web_app(router)
    class MyExtension(WebAppExtension):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

        @classmethod
        def get_static_root(cls):
            return None

        @router.get('/test/url')
        def test_url(self):
            return {'test': 'ok'}

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
        return_value=MyExtension,
    )

    mocker.patch('connect.eaas.runner.workers.web.get_version', return_value='24.1')
    client_mock = mocker.AsyncMock()
    client_mock.__aenter__.side_effect = Exception('test exception')
    mocker.patch(
        'connect.eaas.runner.workers.web.httpx.AsyncClient',
        return_value=client_mock,
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
            url='/api/test/url',
            headers={},
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
        ['receive', 'send', 'send', 'receive'],
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
    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.WEB_TASK,
            data=WebTask(
                options=WebTaskOptions(
                    correlation_id='correlation_id',
                    reply_to='reply_to',
                    api_key='api_key',
                    installation_id='installation_id',
                ),
                request=HttpRequest(
                    method='GET',
                    url='/api/test/url',
                    headers={},
                ),
                response=HttpResponse(
                    status=500,
                    headers={},
                    content='dGVzdCBleGNlcHRpb24=\n',
                ),
            ),
        ),
    )


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


@pytest.mark.asyncio
async def test_close_connection_with_reason(
    mocker,
    ws_server,
    unused_port,
    settings_payload,
    caplog,
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
        exception = ConnectionClosedOK(
            rcvd=mocker.MagicMock(reason='Somereason'),
            sent=mocker.MagicMock(),
        )
        worker.receive = mocker.MagicMock(side_effect=exception)
        with caplog.at_level(logging.WARNING):
            asyncio.create_task(worker.start())
            await asyncio.sleep(1)
        assert worker.run_event.is_set() is False
        assert 'The WS connection has been closed, reason: Somereason' in caplog.text
