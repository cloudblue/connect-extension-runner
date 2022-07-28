import base64
import json
import logging
from io import BytesIO
from urllib.parse import unquote, urlparse

from connect.eaas.core.proto import (
    HttpRequest,
    HttpResponse,
    Message,
    MessageType,
    WebTask,
)


logger = logging.getLogger(__name__)


class RequestResponseCycle:

    def __init__(self, config, app, task, callback):
        self.config = config
        self.app = app
        self.task = task
        self.callback = callback
        self.response_status = None
        self.response_headers = None
        self.body_parts = BytesIO()

    def build_scope(self):
        parsed = urlparse(self.task.request.url)
        return {
            'type': 'http',
            'http_version': '1.1',
            'method': self.task.request.method.upper(),
            'path': unquote(parsed.path),
            'query_string': parsed.query.encode('utf-8'),
            'headers': self.build_headers(),
        }

    def build_headers(self):
        headers = [
            (k.lower().encode('utf-8'), v.encode('utf-8'))
            for k, v in self.task.request.headers.items()
        ]
        headers.extend(
            [
                (
                    'X-Connect-Api-Gateway-Url'.lower().encode('utf-8'),
                    self.config.get_api_url().encode('utf-8'),
                ),
                (
                    'X-Connect-User-Agent'.lower().encode('utf-8'),
                    self.config.get_user_agent()['User-Agent'].encode('utf-8'),
                ),
                (
                    'X-Connect-Extension-Id'.lower().encode('utf-8'),
                    self.config.service_id.encode('utf-8'),
                ),
                (
                    'X-Connect-Installation-Api-Key'.lower().encode('utf-8'),
                    self.task.options.api_key.encode('utf-8'),
                ),
                (
                    'X-Connect-Installation-Id'.lower().encode('utf-8'),
                    self.task.options.installation_id.encode('utf-8'),
                ),
                (
                    'X-Connect-Logging-Level'.lower().encode('utf-8'),
                    self.config.logging_level.encode('utf-8')
                    if self.config.logging_level else b'DEBUG',
                ),
            ],
        )
        if self.config.logging_api_key is not None:
            headers.extend(
                [
                    (
                        'X-Connect-Logging-Api-Key'.lower().encode('utf-8'),
                        self.config.logging_api_key.encode('utf-8'),
                    ),
                    (
                        'X-Connect-Logging-Metadata'.lower().encode('utf-8'),
                        json.dumps(self.config.metadata).encode('utf-8'),
                    ),
                ],
            )
        return headers

    async def receive(self):
        body = (
            base64.decodebytes(self.task.request.content.encode('utf-8'))
            if self.task.request.content else b''
        )
        message = {
            'type': 'http.request',
            'body': body,
            'more_body': False,
        }

        return message

    def build_response(self, status, headers, body):
        logger.debug(f'body type: {type(body)}')
        log = logger.info if status < 500 else logger.error
        log(
            f'{self.task.request.method.upper()} {self.task.request.url} {status} -  {len(body)}',
        )
        task_response = WebTask(
            options=self.task.options,
            request=HttpRequest(
                method=self.task.request.method.upper(),
                url=self.task.request.url,
                headers={},
            ),
            response=HttpResponse(
                status=status,
                headers=headers,
                content=base64.encodebytes(body).decode('utf-8'),
            ),
        )

        message = Message(
            version=2,
            message_type=MessageType.WEB_TASK,
            data=task_response,
        )
        return message.serialize()

    async def send(self, message):
        if message['type'] == 'http.response.start':
            self.response_status = message['status']
            self.response_headers = {
                header[0].lower().decode(): header[1].decode()
                for header in message.get('headers', [])
            }
        elif message['type'] == 'http.response.body':
            self.body_parts.write(message['body'])
            if message.get('more_body', False):
                return
            await self.callback(
                self.build_response(
                    self.response_status,
                    self.response_headers,
                    self.body_parts.getvalue(),
                ),
            )

    async def __call__(self):
        scope = self.build_scope()
        await self.app(
            scope,
            self.receive,
            self.send,
        )
