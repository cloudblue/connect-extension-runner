import asyncio
import copy
import logging
import re
import time

import pytest
from websockets.exceptions import (
    ConnectionClosedOK,
)
from websockets.utils import (
    accept_key,
)

from connect.eaas.core.decorators import (
    account_settings_page,
    customer_pages,
    web_app,
)
from connect.eaas.core.extension import (
    WebApplicationBase,
)
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
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.web import (
    WebApp,
)
from connect.eaas.runner.workers.web import (
    WebWorker,
    start_webapp_worker_process,
)
from tests.utils import (
    CloseAfterSetupWSHandler,
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

    ui_modules = {
        'settings': {
            'label': 'Settings',
            'url': '/static/settings.html',
        },
    }

    proxied_connect_api = ['/public/v1/endpoint']

    @account_settings_page('Settings', '/static/settings.html')
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'name': 'name',
                'description': 'description',
                'version': '0.1.2',
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

        @classmethod
        def get_proxied_connect_api(cls):
            return proxied_connect_api

    mocker.patch.object(
        WebApp,
        'load_application',
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
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
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
            proxied_connect_api=proxied_connect_api,
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
    assert worker.config.repository_tag == settings_payload['logging']['meta']['repository_tag']
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
            tier_account_id='tier_account_id',
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
async def test_http_call(mocker, ws_server, unused_port, settings_payload, task_options, router):
    setup_response = copy.deepcopy(settings_payload)
    setup_response['logging']['logging_api_key'] = 'logging_api_key'
    setup_response['logging']['log_level'] = None
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
        'customer': [
            {
                'label': 'Customer Home Page',
                'url': '/static/customer.html',
                'icon': '/static/icon.png',
            },
        ],
    }

    @account_settings_page('Settings', '/static/settings.html')
    @customer_pages(
        [
            {
                'label': 'Customer Home Page',
                'url': '/static/customer.html',
                'icon': '/static/icon.png',
            },
        ],
    )
    @web_app(router)
    class MyExtension(WebApplicationBase):
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
        'load_application',
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
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
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
                proxied_connect_api=[],
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
            tier_account_id='tier_account_id',
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
async def test_http_call_redirect(
    mocker, ws_server, unused_port, settings_payload, task_options, router,
):
    setup_response = copy.deepcopy(settings_payload)
    setup_response['logging']['logging_api_key'] = 'logging_api_key'
    setup_response['logging']['log_level'] = None
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
    class MyExtension(WebApplicationBase):
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
        'load_application',
        return_value=MyExtension,
    )

    mocker.patch('connect.eaas.runner.workers.web.get_version', return_value='24.1')

    web_task = WebTask(
        options=task_options,
        request=HttpRequest(
            method='GET',
            url='/api/test/url/',
            headers={
                'X-Forwarded-Host': 'my.proxy.host',
                'X-Forwarded-Proto': 'https',
            },
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
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
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
                proxied_connect_api=[],
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
                    url='/api/test/url/',
                    headers={},
                ),
                response=HttpResponse(
                    status=307,
                    headers={
                        'content-length': '0',
                        'location': 'https://my.proxy.host/api/test/url',
                    },
                    content='',
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_http_call_exception(mocker, ws_server, unused_port, settings_payload, router):
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
    class MyExtension(WebApplicationBase):
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
        'load_application',
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
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
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
                proxied_connect_api=[],
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

    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'ui': ui_modules,
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        WebApp,
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
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/webapp',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = WebApp(config)

    async with ws_server(handler):
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
        asyncio.create_task(worker.start())
        await asyncio.sleep(1)
        assert worker.run_event.is_set() is False


def test_start_webapp_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocker.patch.object(
        WebApp,
        'load_application',
    )
    mocker.patch.object(WebWorker, 'start', start_mock)
    mocked_configure_logger = mocker.patch('connect.eaas.runner.workers.web.configure_logger')

    start_webapp_worker_process(
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


@pytest.mark.asyncio
async def test_trigger_lifecycle_events_sync(mocker, ws_server, unused_port, settings_payload):
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
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'name': 'name',
                'description': 'description',
                'version': '0.1.2',
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

        @classmethod
        def on_startup(cls, logger, config):
            pass

        @classmethod
        def on_shutdown(cls, logger, config):
            pass

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    mocked_on_startup = mocker.patch.object(MyExtension, 'on_startup')
    mocked_on_startup.__self__ = MyExtension
    mocked_on_shutdown = mocker.patch.object(MyExtension, 'on_shutdown')
    mocked_on_shutdown.__self__ = MyExtension

    mocked_inspect = mocker.patch('connect.eaas.runner.workers.base.inspect')
    mocked_inspect.ismethod.return_value = True
    mocked_inspect.iscoroutinefunction.return_value = False

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
        worker = WebWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(value=0),
            mocker.MagicMock(value=0),
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
            ui_modules=ui_modules,
            variables=[],
            schedulables=None,
            repository={
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            },
            runner_version='24.1',
            proxied_connect_api=[],
        ),
    )

    handler.assert_received(msg.dict())
    mocked_on_startup.assert_called_once()
    mocked_on_shutdown.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_lifecycle_events_async(mocker, ws_server, unused_port, settings_payload):
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
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'name': 'name',
                'description': 'description',
                'version': '0.1.2',
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

        @classmethod
        async def on_startup(cls, logger, config):
            pass

        @classmethod
        async def on_shutdown(cls, logger, config):
            pass

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    mocked_on_startup = mocker.patch.object(MyExtension, 'on_startup')
    mocked_on_startup.__self__ = MyExtension
    mocked_on_shutdown = mocker.patch.object(MyExtension, 'on_shutdown')
    mocked_on_shutdown.__self__ = MyExtension

    mocked_inspect = mocker.patch('connect.eaas.runner.workers.base.inspect')
    mocked_inspect.ismethod.return_value = True
    mocked_inspect.iscoroutinefunction.return_value = True

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
        worker = WebWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(value=0),
            mocker.MagicMock(value=0),
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
            ui_modules=ui_modules,
            variables=[],
            schedulables=None,
            repository={
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            },
            runner_version='24.1',
            proxied_connect_api=[],
        ),
    )

    handler.assert_received(msg.dict())
    mocked_on_startup.assert_awaited_once()
    mocked_on_shutdown.assert_awaited_once()


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
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', .1)

    ui_modules = {
        'settings': {
            'label': 'Settings',
            'url': '/static/settings.html',
        },
    }

    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'ui': ui_modules,
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(
        WebApp,
        'get_application',
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
        worker = WebWorker(
            ext_handler,
            mocker.MagicMock(),
            mocker.MagicMock(value=0),
            mocker.MagicMock(value=0),
        )
        exception = ConnectionClosedOK(
            rcvd=mocker.MagicMock(reason='Somereason'),
            sent=mocker.MagicMock(),
            rcvd_then_sent=True,
        )
        worker.receive = mocker.MagicMock(side_effect=exception)
        with caplog.at_level(logging.WARNING):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(1)
        # A clean close is not a stop request: the worker reconnects (LITE-35241).
        assert worker.run_event.is_set() is True
        assert 'The WS connection has been closed, reason: Somereason' in caplog.text
        assert 'WebWorker: try to reconnect in 0.1s' in caplog.text
        worker.stop()
        await task


@pytest.mark.asyncio
async def test_proper_internal_headers(mocker, ws_server, unused_port, settings_payload, router):

    task_options = WebTaskOptions(
        correlation_id='correlation_id',
        reply_to='reply_to',
        api_key='api_key',
        installation_id='installation_id',
        tier_account_id='tier_account_id',
        connect_correlation_id='connect_correlation_id',
        user_id='user_id',
        account_id='account_id',
        account_role='account_role',
        call_type='user',
        call_source='ui',
    )

    setup_response = copy.deepcopy(settings_payload)
    setup_response['logging']['logging_api_key'] = 'logging_api_key'
    setup_response['logging']['log_level'] = None
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

    @account_settings_page('Settings', '/static/settings.html')
    @web_app(router)
    class MyExtension(WebApplicationBase):
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
        'load_application',
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
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        mocker.patch.object(
            worker.config,
            'get_user_agent',
            lambda: {'User-Agent': 'useragent'},
        )
        assert worker.get_internal_headers(task=web_task) == {
            'X-Connect-Api-Gateway-Url': f'https://127.0.0.1:{unused_port}/public/v1',
            'X-Connect-User-Agent': 'useragent',
            'X-Connect-Extension-Id': 'service_id',
            'X-Connect-Environment-Id': 'ENV-000-0001',
            'X-Connect-Environment-Type': 'development',
            'X-Connect-Logging-Level': 'WARNING',
            'X-Connect-Config': '{"conf1": "val1"}',
            'X-Connect-Installation-Api-Key': 'api_key',
            'X-Connect-Installation-Id': 'installation_id',
            'X-Connect-Tier-Account-Id': 'tier_account_id',
            'X-Connect-Correlation-Id': 'connect_correlation_id',
            'X-Connect-User-Id': 'user_id',
            'X-Connect-Account-Id': 'account_id',
            'X-Connect-Account-Role': 'account_role',
            'X-Connect-Call-Type': 'user',
            'X-Connect-Call-Source': 'ui',
            'X-Connect-Logging-Api-Key': 'logging_api_key',
            'X-Connect-Logging-Metadata': (
                '{"api_address": "127.0.0.1:' + f'{unused_port}", "service_id": "service_id", '
                '"environment_id": "ENV-000-0001", "environment_type": "development", '
                '"instance_id": "INS-000-0002", "account_id": "account_id", "account_name": '
                '"account_name", "repository_tag": "repository_tag"}'
            ),
        }
        worker.stop()
        await task


@pytest.mark.asyncio
async def test_optional_internal_headers(mocker, ws_server, unused_port, settings_payload, router):

    task_options = WebTaskOptions(
        correlation_id='correlation_id',
        reply_to='reply_to',
        tier_account_id='tier_account_id',
        connect_correlation_id='connect_correlation_id',
        user_id='user_id',
        account_id='account_id',
        account_role='account_role',
        call_type='user',
        call_source='ui',
    )

    setup_response = copy.deepcopy(settings_payload)
    setup_response['logging']['logging_api_key'] = None
    setup_response['logging']['log_level'] = None
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

    @account_settings_page('Settings', '/static/settings.html')
    @web_app(router)
    class MyExtension(WebApplicationBase):
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
        'load_application',
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
        worker = WebWorker(ext_handler, mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        mocker.patch.object(worker.config, 'get_user_agent', lambda: {'User-Agent': 'useragent'})
        assert worker.get_internal_headers(task=web_task) == {
            'X-Connect-Api-Gateway-Url': f'https://127.0.0.1:{unused_port}/public/v1',
            'X-Connect-User-Agent': 'useragent',
            'X-Connect-Extension-Id': 'service_id',
            'X-Connect-Environment-Id': 'ENV-000-0001',
            'X-Connect-Environment-Type': 'development',
            'X-Connect-Logging-Level': 'WARNING',
            'X-Connect-Config': '{"conf1": "val1"}',
            'X-Connect-Tier-Account-Id': 'tier_account_id',
            'X-Connect-Correlation-Id': 'connect_correlation_id',
            'X-Connect-User-Id': 'user_id',
            'X-Connect-Account-Id': 'account_id',
            'X-Connect-Account-Role': 'account_role',
            'X-Connect-Call-Type': 'user',
            'X-Connect-Call-Source': 'ui',
        }
        worker.stop()
        await task


def test_prettify(mocker):
    logger = logging.getLogger('connect.eaas.runner')
    worker = WebWorker(
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
        mocker.MagicMock(),
    )

    logger.setLevel(logging.DEBUG)
    assert worker.prettify('message') == "'message'"

    logger.setLevel(logging.INFO)
    assert worker.prettify('message') == '<...>'


WEBAPP_WS_PATH = '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/webapp'


def _setup_reconnect_test(mocker, unused_port):
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
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', .1)

    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://read.me',
                'changelog_url': 'https://change.log',
            }

    mocker.patch.object(WebApp, 'load_application', return_value=MyExtension)
    return WebWorker(
        WebApp(ConfigHelper(secure=False)),
        mocker.MagicMock(),
        mocker.MagicMock(value=0),
        mocker.MagicMock(value=0),
    )


def _setup_response(settings_payload):
    return Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(**settings_payload),
    ).dict()


@pytest.mark.parametrize('close_code', (1000, 1001))
@pytest.mark.asyncio
async def test_reconnect_after_clean_close(
    mocker, ws_server, unused_port, settings_payload, close_code, caplog,
):
    """LITE-34090 R1: a clean close (ConnectionClosedOK) must not kill the worker."""
    handler = CloseAfterSetupWSHandler(
        WEBAPP_WS_PATH, _setup_response(settings_payload), close_code,
    )
    async with ws_server(handler):
        worker = _setup_reconnect_test(mocker, unused_port)
        with caplog.at_level(logging.WARNING):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(1.5)
        try:
            assert 'The WS connection has been closed, reason: closed by the test server' in (
                caplog.text
            )
            assert handler.connections >= 2
            assert worker.stop_event.is_set() is False
        finally:
            worker.stop()
            await task


@pytest.mark.parametrize('reject_status', (403, 502))
@pytest.mark.asyncio
async def test_reconnect_after_max_retry_time(
    mocker, ws_server, unused_port, settings_payload, reject_status, caplog,
):
    """LITE-34090 R2: when backoff gives up, the worker must keep trying, not stop."""
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', .3)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_MAINTENANCE_SECONDS', .3)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', .05)
    handler = CloseAfterSetupWSHandler(
        WEBAPP_WS_PATH, _setup_response(settings_payload), 1011,
    )
    async with ws_server(handler):
        worker = _setup_reconnect_test(mocker, unused_port)
        with caplog.at_level(logging.WARNING):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.2)
            handler.reject_status = reject_status
            await asyncio.sleep(1.5)
            handler.reject_status = None
            await asyncio.sleep(1)
        try:
            assert 'max connection attemps reached' in caplog.text
            assert handler.rejected > 0
            assert handler.connections >= 2
            assert worker.stop_event.is_set() is False
        finally:
            worker.stop()
            await task


@pytest.mark.parametrize('close_code', (None, 1011))
@pytest.mark.asyncio
async def test_reconnect_after_abnormal_close(
    mocker, ws_server, unused_port, settings_payload, close_code, caplog,
):
    """LITE-34090 R3: an abnormal close (ConnectionClosedError) reconnects."""
    handler = CloseAfterSetupWSHandler(
        WEBAPP_WS_PATH, _setup_response(settings_payload), close_code,
    )
    async with ws_server(handler):
        worker = _setup_reconnect_test(mocker, unused_port)
        with caplog.at_level(logging.WARNING):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(1.5)
        try:
            assert 'disconnected from:' in caplog.text
            assert handler.connections >= 2
            assert worker.stop_event.is_set() is False
        finally:
            worker.stop()
            await task


@pytest.mark.asyncio
async def test_no_reconnect_after_shutdown_message(
    mocker, ws_server, unused_port, settings_payload,
):
    """LITE-34090 R4: a SHUTDOWN from the gateway still stops the worker for good."""
    handler = CloseAfterSetupWSHandler(
        WEBAPP_WS_PATH,
        _setup_response(settings_payload),
        1000,
        extra_messages=[Message(message_type=MessageType.SHUTDOWN).dict()],
    )
    async with ws_server(handler):
        worker = _setup_reconnect_test(mocker, unused_port)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(1.5)
        assert task.done() is True
        assert worker.run_event.is_set() is False
        assert handler.connections == 1


@pytest.mark.asyncio
async def test_no_reconnect_after_signal(mocker, ws_server, unused_port, settings_payload):
    """LITE-34090 R4: a signal (SIGINT/SIGTERM) still stops the worker for good."""
    mocker.patch('connect.eaas.runner.workers.base.SHUTDOWN_WAIT_GRACE_SECONDS', 0)
    handler = CloseAfterSetupWSHandler(
        WEBAPP_WS_PATH, _setup_response(settings_payload), 1000,
    )
    async with ws_server(handler):
        worker = _setup_reconnect_test(mocker, unused_port)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.1)
        worker.handle_signal()
        await asyncio.sleep(1.4)
        assert task.done() is True
        assert worker.run_event.is_set() is False
        assert handler.connections == 1


class NoSetupResponseFirstWSHandler(CloseAfterSetupWSHandler):
    """Never answer the setup request of the first connection."""
    first_close_code = None

    async def __call__(self, ws, path):
        if self.connections:
            return await super().__call__(ws, path)
        self.connections += 1
        await ws.recv()
        await ws.wait_closed()
        self.first_close_code = ws.close_code


@pytest.mark.asyncio
async def test_reconnect_after_handshake_timeout(
    mocker, ws_server, unused_port, settings_payload, caplog,
):
    """LITE-35241: after a handshake timeout the socket is closed and the retry reconnects."""
    mocker.patch('connect.eaas.runner.workers.base.HANDSHAKE_TIMEOUT_SECONDS', .2)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', .05)
    handler = NoSetupResponseFirstWSHandler(
        WEBAPP_WS_PATH, _setup_response(settings_payload), 1000,
    )
    async with ws_server(handler):
        worker = _setup_reconnect_test(mocker, unused_port)
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(1)
        try:
            assert handler.first_close_code == 1000
            assert handler.connections >= 2
            assert f'WebWorker connected to ws://127.0.0.1:{unused_port}' in caplog.text
        finally:
            worker.stop()
            await task


@pytest.mark.asyncio
async def test_no_reconnect_delay_on_clean_close_after_stop(mocker, unused_port, caplog):
    worker = _setup_reconnect_test(mocker, unused_port)
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 10)
    worker.ensure_connection = mocker.AsyncMock()

    def stop_and_close():
        worker.stop()
        raise ConnectionClosedOK(
            rcvd=mocker.MagicMock(reason='shutdown'),
            sent=mocker.MagicMock(),
            rcvd_then_sent=True,
        )

    worker.receive = mocker.AsyncMock(side_effect=stop_and_close)
    worker.run_event.set()
    with caplog.at_level(logging.WARNING):
        await asyncio.wait_for(worker.run(), timeout=1)
    assert 'The WS connection has been closed, reason: shutdown' in caplog.text
    assert 'try to reconnect' not in caplog.text


@pytest.mark.asyncio
async def test_exit_if_main_loop_ends_without_stop(mocker, unused_port, caplog):
    """LITE-35241: a main loop that ends without stop() exits the process with code 1."""
    mocked_exit = mocker.patch(
        'connect.eaas.runner.workers.base.os._exit',
        side_effect=SystemExit(1),
    )
    worker = _setup_reconnect_test(mocker, unused_port)
    worker.run = mocker.AsyncMock(side_effect=asyncio.CancelledError())
    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit):
            await worker.start()
    worker.stop()
    mocked_exit.assert_called_once_with(1)
    assert 'WebWorker: unexpected exit without a stop request (main: <Task cancelled' in caplog.text


@pytest.mark.asyncio
async def test_exit_if_master_dies(mocker, unused_port, caplog):
    """LITE-35241: a worker whose master died (new parent pid) exits with code 1."""
    mocker.patch('connect.eaas.runner.workers.base.PROCESS_CHECK_INTERVAL_SECS', .01)
    mocker.patch('connect.eaas.runner.workers.base.os.getppid', side_effect=[100, 100, 1])
    mocked_exit = mocker.patch(
        'connect.eaas.runner.workers.base.os._exit',
        side_effect=SystemExit(1),
    )
    worker = _setup_reconnect_test(mocker, unused_port)

    async def run_forever():
        await asyncio.sleep(10)

    worker.run = run_forever
    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit):
            await worker.start()
    worker.main_task.cancel()
    worker.stop()
    mocked_exit.assert_called_once_with(1)
    assert 'WebWorker: master process 100 is gone' in caplog.text
    assert 'WebWorker: unexpected exit without a stop request (main: <Task pending' in caplog.text


@pytest.mark.asyncio
async def test_reconnect_after_ping_timeout(mocker, unused_port, caplog):
    """
    LITE-35241: a peer that accepts the websocket upgrade and then never answers
    makes the ping time out. The socket is closed quickly and the worker reconnects.
    """
    mocker.patch('connect.eaas.runner.workers.base.HANDSHAKE_TIMEOUT_SECONDS', .2)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', .05)
    connections = []

    async def silent_peer(reader, writer):
        connections.append(time.monotonic())
        request = await reader.readuntil(b'\r\n\r\n')
        key = re.search(rb'Sec-WebSocket-Key: (\S+)', request, re.I).group(1).decode()
        writer.write(
            'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
            f'Connection: Upgrade\r\nSec-WebSocket-Accept: {accept_key(key)}\r\n\r\n'.encode(),
        )
        while await reader.read(1024):
            pass
        writer.close()

    server = await asyncio.start_server(silent_peer, '127.0.0.1', unused_port)
    async with server:
        worker = _setup_reconnect_test(mocker, unused_port)
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            for _ in range(80):
                if len(connections) >= 2:
                    break
                await asyncio.sleep(.1)
        try:
            assert len(connections) >= 2
            # With the default close_timeout (10 s) the close takes far longer than 6 s.
            assert connections[1] - connections[0] < 6
            assert 'received an unexpected exception' in caplog.text
        finally:
            worker.stop()
            await asyncio.wait_for(task, 10)
