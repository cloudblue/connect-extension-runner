import asyncio
import copy
import base64
import json

import pytest

from connect.eaas.core.extension import UIExtension
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
from connect.eaas.runner.webworker import WebWorker
from connect.eaas.runner.webapp import WebApp

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

    class MyExtension(UIExtension):
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
    mocker.patch.object(WebApp, 'start')
    mocker.patch.object(WebApp, 'stop')
    mocker.patch('connect.eaas.runner.webworker.get_version', return_value='24.1')

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
        worker = WebWorker(config, ext_handler)
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

    class MyExtension(UIExtension):
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
    mocker.patch.object(WebApp, 'start')
    mocker.patch.object(WebApp, 'stop')
    mocker.patch('connect.eaas.runner.webworker.get_version', return_value='24.1')

    httpx_mock.add_response(
        method='GET',
        url='http://localhost:53575/test/url',
        json={
            'test_response': 'value',
        },
        headers={'X-Response-Header': 'my-value'},
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
            data=WebTask(
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
            ),
        ).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002/webapp',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = WebApp(config)

    async with ws_server(handler):
        worker = WebWorker(config, ext_handler)
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
                    url='/test/url',
                    headers={},
                ),
                response=HttpResponse(
                    status=200,
                    headers={
                        'x-response-header': 'my-value',
                        'content-type': 'application/json',
                        'content-length': '26',
                    },
                    content=base64.encodebytes(
                        json.dumps({'test_response': 'value'}).encode('utf-8'),
                    ).decode('utf-8'),
                ),
            ),
        ).dict(),
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

    class MyExtension(UIExtension):
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
    mocker.patch.object(WebApp, 'start')
    mocked_stop = mocker.patch.object(
        WebApp,
        'stop',
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
        worker = WebWorker(config, ext_handler)
        asyncio.create_task(worker.start())
        await asyncio.sleep(1)
        assert worker.run_event.is_set() is False
        mocked_stop.assert_called_once()
