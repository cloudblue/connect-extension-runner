import base64
import copy

import httpx

from connect.eaas.core.proto import (
    HttpRequest,
    HttpResponse,
    Message,
    MessageType,
    SetupRequest,
    WebTask,
)
from connect.eaas.runner.base import WorkerBase
from connect.eaas.runner.helpers import get_version


class WebWorker(WorkerBase):
    """
    The Worker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, config, handler, runner_type=None):
        super().__init__(config)
        self.runner_type = runner_type
        self.handler = handler
        self.handler.start()

    def get_url(self):
        return self.config.get_webapp_ws_url()

    def get_setup_request(self):
        return Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                ui_modules=self.handler.ui_modules,
                variables=self.handler.variables,
                repository={
                    'readme_url': self.handler.readme,
                    'changelog_url': self.handler.changelog,
                },
                runner_version=get_version(),
            ),
        ).dict()

    async def stopping(self):
        pass

    async def process_message(self, data):
        message = Message.deserialize(data)
        if message.message_type == MessageType.SETUP_RESPONSE:
            self.process_setup_response(message.data)
        elif message.message_type == MessageType.WEB_TASK:
            await self.process_task(message.data)
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def shutdown(self):
        self.handler.stop()
        await super().shutdown()

    async def process_task(self, task):
        headers = copy.copy(task.request.headers)
        headers.update(
            {
                'X-Connect-Api-Gateway-Url': self.config.get_api_url(),
                'X-Connect-User-Agent': self.config.get_user_agent()['User-Agent'],
                'X-Connect-Installation-Api-Key': task.options.api_key or 'testame la key',
                'X-Connect-Installation-Id': task.options.installation_id or '',
            },
        )

        async with httpx.AsyncClient() as client:
            url = f'http://localhost:{self.config.webapp_port}{task.request.url}'
            response = await client.request(
                task.request.method,
                url,
                headers=headers,
                content=base64.decodebytes(task.request.content) if task.request.content else None,
            )
            response_body = None

            if response.content:
                response_body = base64.encodebytes(response.content).decode('utf-8')

            task_response = WebTask(
                options=task.options,
                request=HttpRequest(
                    method=task.request.method,
                    url=task.request.url,
                    headers={},
                ),
                response=HttpResponse(
                    status=response.status_code,
                    headers=response.headers,
                    content=response_body,
                ),
            )

            message = Message(
                version=2,
                message_type=MessageType.WEB_TASK,
                data=task_response,
            )
            await self.send(message.serialize())
