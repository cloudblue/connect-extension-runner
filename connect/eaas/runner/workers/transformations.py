#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import logging
import signal

from devtools import pformat

from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
)
from connect.eaas.runner.workers.base import WorkerBase
from connect.eaas.runner.helpers import configure_logger, get_version


logger = logging.getLogger(__name__)


class TransformationWorker(WorkerBase):
    """
    The TransformationWorker is responsible to handle the websocket connection
    with the server. It will send found transformations to
    the server and wait for tasks that need to be processed using
    the task manager.
    """

    def get_url(self):
        return self.config.get_tfnapp_ws_url()

    def get_setup_request(self):
        msg = Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                repository={
                    'readme_url': self.handler.readme,
                    'changelog_url': self.handler.changelog,
                },
                audience=self.handler.audience,
                transformations=self.handler.transformations,
                runner_version=get_version(),
            ),
        )
        logger.debug(f'Sending setup request: {pformat(msg)}')
        return msg.dict()

    async def stopping(self):
        pass

    async def process_message(self, data):
        message = Message.deserialize(data)
        logger.debug(f'Received message: {pformat(message)}')
        if message.message_type == MessageType.SETUP_RESPONSE:
            self.process_setup_response(message.data)
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def shutdown(self):
        await super().shutdown()


def start_tfnapp_worker_process(handler, debug, no_rich):
    configure_logger(debug, no_rich)
    worker = TransformationWorker(handler)
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT,
        worker.handle_signal,
    )
    loop.add_signal_handler(
        signal.SIGTERM,
        worker.handle_signal,
    )
    loop.run_until_complete(worker.start())
