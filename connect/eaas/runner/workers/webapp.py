#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio

from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
)
from connect.eaas.runner.asgi import RequestResponseCycle
from connect.eaas.runner.workers.base import WorkerBase
from connect.eaas.runner.helpers import get_version


class WebWorker(WorkerBase):
    """
    The Worker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, config, handler):
        super().__init__(config)
        self.handler = handler

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
        await super().shutdown()

    async def process_task(self, task):
        cycle = RequestResponseCycle(
            self.config,
            self.handler.app,
            task,
            self.send,
        )
        asyncio.create_task(cycle())
