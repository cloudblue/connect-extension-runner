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
from connect.eaas.runner.constants import (
    DELAY_ON_CONNECT_EXCEPTION_SECONDS,
    RESULT_SENDER_MAX_RETRIES,
    RESULT_SENDER_WAIT_GRACE_SECONDS,
)
from connect.eaas.runner.helpers import (
    configure_logger,
    get_version,
)
from connect.eaas.runner.managers.transformation import (
    TransformationTasksManager,
)
from connect.eaas.runner.workers.base import (
    WorkerBase,
)


logger = logging.getLogger(__name__)


class TransformationWorker(WorkerBase):
    """
    The TransformationWorker is responsible to handle the websocket connection
    with the server. It will send found transformations to
    the server and wait for tasks that need to be processed using
    the task manager.
    """
    def __init__(self, handler, lock, startup_fired, shutdown_fired, runner_type=None):
        super().__init__(handler, lock, startup_fired, shutdown_fired)
        self.runner_type = runner_type
        self.results_queue = asyncio.Queue()
        self.manager = TransformationTasksManager(
            self.config,
            self.handler,
            self.results_queue.put,
        )

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
        logger.debug(f'Sending setup request: {self.prettify(msg)}')
        return msg.dict()

    async def stopping(self):
        timeout = self.config.get_timeout('transformation') + RESULT_SENDER_WAIT_GRACE_SECONDS
        try:
            await asyncio.wait_for(
                self.results_task,
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                f'Cannot send all results timeout of {timeout} exceeded, cancel task',
            )
            self.results_task.cancel()
            try:
                await self.results_task
            except asyncio.CancelledError:
                logger.info('Result sender task has been cancelled')

    async def process_message(self, data):
        message = Message.deserialize(data)
        logger.debug(f'Received message: {self.prettify(message)}')
        if message.message_type == MessageType.SETUP_RESPONSE:
            await self.process_setup_response(message.data)
        elif message.message_type == MessageType.TASK:
            logger.info(
                f'Received new transformation {message.data.options.task_id}',
            )
            await self.manager.submit(message.data)
            logger.info(f'Task {message.data.options.task_id} submitted for processing')
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def result_sender(self):  # noqa: CCR001
        """
        Dequeues results from the results queue and send it to
        the EaaS backend.
        """
        await self.run_event.wait()
        while True:
            if self.results_queue.empty():
                if not self.run_event.is_set() and self.manager.running_tasks == 0:
                    logger.info(f'{self} exiting and no more running tasks: exit!')
                    return
                await asyncio.sleep(.5)
                continue
            logger.info(
                f'Current transformation processing status: '
                f'running={self.manager.running_tasks}, results={self.results_queue.qsize()}',
            )
            result = await self.results_queue.get()
            logger.info(f'Got a transformation result from queue: {result.options.task_id}')
            retries = 0
            while retries < RESULT_SENDER_MAX_RETRIES:
                try:
                    message = Message(
                        version=2,
                        message_type=MessageType.TASK,
                        data=result,
                    )
                    await self.ensure_connection()
                    logger.debug(f'Sending message: {self.prettify(message)}')
                    await self.send(message.serialize())
                    logger.info(
                        f'Result for transformation task {result.options.task_id} has been sent.',
                    )
                    break
                except Exception:
                    logger.warning(
                        f'Attempt {retries} to send results for task '
                        f'{result.options.task_id} has failed.',
                    )
                    retries += 1
                    await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
            else:
                logger.warning(
                    f'Max retries exceeded ({RESULT_SENDER_MAX_RETRIES})'
                    f' for sending results of transformation task {result.options.task_id}',
                )

            if not self.run_event.is_set():
                logger.info(
                    f'Current transformation processing status: '
                    f'running={self.manager.running_tasks}, results={self.results_queue.qsize()}',
                )

    async def start(self):
        """
        Start the runner.
        """
        self.results_task = asyncio.create_task(self.result_sender())
        await super().start()


def start_tfnapp_worker_process(
    handler_class,
    config,
    lifecycle_lock,
    on_startup_fired,
    on_shutdown_fired,
    debug,
    no_rich,
):
    handler = handler_class(config)
    configure_logger(debug, no_rich)
    worker = TransformationWorker(
        handler,
        lifecycle_lock,
        on_startup_fired,
        on_shutdown_fired,
    )
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
