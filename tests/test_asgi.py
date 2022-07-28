import base64

import pytest

from connect.eaas.core.proto import (
    HttpRequest,
    WebTask,
    WebTaskOptions,
)
from connect.eaas.runner.asgi import RequestResponseCycle


@pytest.mark.parametrize(
    ('logging_api_key', 'metadata'),
    (
        (None, None),
        ('api_key', {'meta': 'data'}),
    ),
)
def test_build_scope(mocker, logging_api_key, metadata):
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

    config = mocker.MagicMock()
    config.get_api_url.return_value = 'https://api.example.com'
    config.get_user_agent.return_value = {'User-Agent': 'myuseragent'}
    config.logging_level = 'INFO'
    config.logging_api_key = logging_api_key
    config.metadata = metadata
    config.service_id = 'extension_id'

    expected_headers = [
        (b'x-my-header', b'my-value'),
        (b'x-connect-api-gateway-url', b'https://api.example.com'),
        (b'x-connect-user-agent', b'myuseragent'),
        (b'x-connect-extension-id', b'extension_id'),
        (b'x-connect-installation-api-key', b'api_key'),
        (b'x-connect-installation-id', b'installation_id'),
        (b'x-connect-logging-level', b'INFO'),
    ]

    if logging_api_key:
        expected_headers.extend(
            [
                (b'x-connect-logging-api-key', b'api_key'),
                (b'x-connect-logging-metadata', b'{"meta": "data"}'),
            ],
        )

    cycle = RequestResponseCycle(config, mocker.MagicMock(), web_task, mocker.MagicMock())
    scope = cycle.build_scope()

    assert scope == {
        'type': 'http',
        'http_version': '1.1',
        'method': 'GET',
        'path': '/test/url',
        'query_string': b'',
        'headers': expected_headers,
    }


@pytest.mark.asyncio
async def test_receive_without_body(mocker):
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

    cycle = RequestResponseCycle(
        mocker.MagicMock(), mocker.MagicMock(), web_task, mocker.MagicMock(),
    )

    request = await cycle.receive()

    assert request == {
        'type': 'http.request',
        'body': b'',
        'more_body': False,
    }


@pytest.mark.asyncio
async def test_receive_with_body(mocker):
    web_task = WebTask(
        options=WebTaskOptions(
            correlation_id='correlation_id',
            reply_to='reply_to',
            api_key='api_key',
            installation_id='installation_id',
        ),
        request=HttpRequest(
            method='POST',
            url='/test/url',
            headers={'X-My-Header': 'my-value'},
            content=base64.encodebytes('hello world'.encode('utf-8')).decode('utf-8'),
        ),
    )

    cycle = RequestResponseCycle(
        mocker.MagicMock(), mocker.MagicMock(), web_task, mocker.MagicMock(),
    )

    request = await cycle.receive()

    assert request == {
        'type': 'http.request',
        'body': b'hello world',
        'more_body': False,
    }


@pytest.mark.asyncio
async def test_send(mocker):
    web_task = WebTask(
        options=WebTaskOptions(
            correlation_id='correlation_id',
            reply_to='reply_to',
            api_key='api_key',
            installation_id='installation_id',
        ),
        request=HttpRequest(
            method='POST',
            url='/test/url',
            headers={'X-My-Header': 'my-value'},
            content=base64.encodebytes('hello world'.encode('utf-8')).decode('utf-8'),
        ),
    )

    callback = mocker.AsyncMock()

    cycle = RequestResponseCycle(
        mocker.MagicMock(), mocker.MagicMock(), web_task, callback,
    )

    await cycle.send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [(b'my-header', b'my_value')],
    })

    await cycle.send({
        'type': 'http.response.body',
        'body': b'hello world',
    })

    callback.assert_awaited_once_with(
        {
            'message_type': 'web_task',
            'version': 2,
            'data': {
                'model_type': 'web_task',
                'options': {
                    'api_key': 'api_key',
                    'correlation_id': 'correlation_id',
                    'installation_id': 'installation_id',
                    'reply_to': 'reply_to',
                },
                'request': {
                    'method': 'POST',
                    'url': '/test/url',
                    'headers': {},
                    'content': None,
                },
                'response': {
                    'status': 200,
                    'headers': {'my-header': 'my_value'},
                    'content': 'aGVsbG8gd29ybGQ=\n',
                },
            },
        },
    )


@pytest.mark.asyncio
async def test_send_with_more_body(mocker):
    web_task = WebTask(
        options=WebTaskOptions(
            correlation_id='correlation_id',
            reply_to='reply_to',
            api_key='api_key',
            installation_id='installation_id',
        ),
        request=HttpRequest(
            method='POST',
            url='/test/url',
            headers={'X-My-Header': 'my-value'},
            content=base64.encodebytes('hello world'.encode('utf-8')).decode('utf-8'),
        ),
    )

    callback = mocker.AsyncMock()

    cycle = RequestResponseCycle(
        mocker.MagicMock(), mocker.MagicMock(), web_task, callback,
    )

    await cycle.send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [(b'my-header', b'my_value')],
    })

    await cycle.send({
        'type': 'http.response.body',
        'body': b'hello',
        'more_body': True,
    })

    await cycle.send({
        'type': 'http.response.body',
        'body': b' world',
    })

    callback.assert_awaited_once_with(
        {
            'message_type': 'web_task',
            'version': 2,
            'data': {
                'model_type': 'web_task',
                'options': {
                    'api_key': 'api_key',
                    'correlation_id': 'correlation_id',
                    'installation_id': 'installation_id',
                    'reply_to': 'reply_to',
                },
                'request': {
                    'method': 'POST',
                    'url': '/test/url',
                    'headers': {},
                    'content': None,
                },
                'response': {
                    'status': 200,
                    'headers': {'my-header': 'my_value'},
                    'content': 'aGVsbG8gd29ybGQ=\n',
                },
            },
        },
    )


@pytest.mark.asyncio
async def test_call(mocker):
    mocker.patch.object(RequestResponseCycle, 'build_scope', return_value={'scope': 'data'})

    app = mocker.AsyncMock()

    cycle = RequestResponseCycle(
        mocker.MagicMock(), app, mocker.MagicMock(), mocker.MagicMock(),
    )

    await cycle()

    app.assert_awaited_once_with(
        {'scope': 'data'},
        cycle.receive,
        cycle.send,
    )
