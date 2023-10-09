#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import logging
import signal

from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
)
from connect.eaas.runner.helpers import (
    configure_logger,
    get_version,
)
from connect.eaas.runner.workers.base import (
    WorkerBase,
)


logger = logging.getLogger(__name__)


class AnvilWorker(WorkerBase):
    """
    The EventsWorker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """

    def get_url(self):
        return self.config.get_anvilapp_ws_url()

    def get_setup_request(self):
        msg = Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                variables=self.handler.variables,
                anvil_callables=self.handler.callables,
                repository={
                    'readme_url': self.handler.readme,
                    'changelog_url': self.handler.changelog,
                },
                audience=self.handler.audience,
                runner_version=get_version(),
            ),
        )
        logger.debug(f'Sending setup request: {self.prettify(msg)}')
        return msg.dict()

    async def stopping(self):
        pass

    async def process_message(self, data):
        message = Message.deserialize(data)
        logger.debug(f'Received message: {self.prettify(message)}')
        if message.message_type == MessageType.SETUP_RESPONSE:
            await self.process_setup_response(message.data)
            self.handler.start()
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def shutdown(self):
        self.handler.stop()
        await super().shutdown()


def start_anvilapp_worker_process(
        handler_class, config, lock, startup_fired, shutdown_fired, debug, no_rich,
):
    handler = handler_class(config)
    configure_logger(debug, no_rich)
    worker = AnvilWorker(handler, lock, startup_fired, shutdown_fired)
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
