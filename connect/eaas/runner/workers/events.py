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
from connect.eaas.runner.managers import (
    BackgroundTasksManager,
    InteractiveTasksManager,
    ScheduledTasksManager,
)
from connect.eaas.runner.workers.base import (
    WorkerBase,
)


logger = logging.getLogger(__name__)


class EventsWorker(WorkerBase):
    """
    The EventsWorker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, handler, lock, startup_fired, shutdown_fired, runner_type=None):
        super().__init__(handler, lock, startup_fired, shutdown_fired)
        self.runner_type = runner_type
        self.results_queue = asyncio.Queue()
        self.background_manager = BackgroundTasksManager(
            self.config,
            self.handler,
            self.results_queue.put,
        )
        self.interactive_manager = InteractiveTasksManager(
            self.config,
            self.handler,
            self.results_queue.put,
        )
        self.scheduled_manager = ScheduledTasksManager(
            self.config,
            self.handler,
            self.results_queue.put,
        )
        self.ws = None
        self.main_task = None
        self.results_task = None

    @property
    def running_tasks(self):
        return (
            self.background_manager.running_tasks
            + self.interactive_manager.running_tasks
            + self.scheduled_manager.running_tasks
        )

    def get_url(self):
        url = self.config.get_events_ws_url()
        url = f'{url}?running_tasks={self.background_manager.running_tasks}'
        url = f'{url}&running_scheduled_tasks={self.scheduled_manager.running_tasks}'
        if self.runner_type:
            url = f'{url}&runner_type={self.runner_type}'
        return url

    def get_setup_request(self):
        msg = Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions={
                    event_type: event['statuses'] or []
                    for event_type, event in self.handler.events.items()
                },
                variables=self.handler.variables,
                schedulables=self.handler.schedulables,
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

    async def process_message(self, data):
        """
        Process a message received from the websocket server.
        """
        message = Message.deserialize(data)
        logger.debug(f'Received message: {self.prettify(message)}')
        if message.message_type == MessageType.SETUP_RESPONSE:
            await self.process_setup_response(message.data)
        elif message.message_type == MessageType.TASK:
            await self.process_task(message.data)
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def process_task(self, task_data):
        """Send a task to a manager based on task category."""
        logger.info(
            f'received new {task_data.options.task_category} task: {task_data.options.task_id}',
        )
        manager = getattr(self, f'{task_data.options.task_category}_manager')
        await manager.submit(task_data)
        logger.info(f'task {task_data.options.task_id} submitted for processing')

    async def result_sender(self):  # noqa: CCR001
        """
        Dequeues results from the results queue and send it to
        the EaaS backend.
        """
        await self.run_event.wait()
        while True:
            if self.results_queue.empty():
                if not self.run_event.is_set() and self.running_tasks == 0:
                    logger.info(f'{self} exiting and no more running tasks: exit!')
                    return
                await asyncio.sleep(.5)
                continue
            logger.info(
                f'Current processing status: running={self.running_tasks} '
                f'results={self.results_queue.qsize()}',
            )
            result = await self.results_queue.get()
            logger.info(f'Got a result from queue: {result.options.task_id}')
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
                    logger.info(f'Result for task {result.options.task_id} has been sent.')
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
                    f' for sending results of task {result.options.task_id}',
                )

            if not self.run_event.is_set():
                logger.info(
                    f'Current processing status: running={self.running_tasks} '
                    f'results={self.results_queue.qsize()}',
                )

    async def stopping(self):
        result_timeout = self.config.get_timeout('background') + RESULT_SENDER_WAIT_GRACE_SECONDS
        try:
            await asyncio.wait_for(
                self.results_task,
                timeout=result_timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                f'Cannot send all results timeout of {result_timeout} exceeded, cancel task',
            )
            self.results_task.cancel()
            try:
                await self.results_task
            except asyncio.CancelledError:
                logger.info('Result sender task has been cancelled')

    async def start(self):
        """
        Start the runner.
        """
        self.results_task = asyncio.create_task(self.result_sender())
        await super().start()

    def __repr__(self):
        if self.runner_type:
            return f'{self.runner_type.capitalize()}{self.__class__.__name__}'
        return super().__repr__()


def _start_event_worker_process(
    handler_class,
    config,
    lifecycle_lock,
    on_startup_fired,
    on_shutdown_fired,
    runner_type,
    debug,
    no_rich,
):
    handler = handler_class(config)
    configure_logger(debug, no_rich)
    worker = EventsWorker(
        handler,
        lifecycle_lock,
        on_startup_fired,
        on_shutdown_fired,
        runner_type=runner_type,
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


def start_interactive_worker_process(
    handler_class,
    config,
    lifecycle_lock,
    on_startup_fired,
    on_shutdown_fired,
    debug,
    no_rich,
):
    _start_event_worker_process(
        handler_class,
        config,
        lifecycle_lock,
        on_startup_fired,
        on_shutdown_fired,
        'interactive',
        debug,
        no_rich,
    )


def start_background_worker_process(
    handler_class,
    config,
    lifecycle_lock,
    on_startup_fired,
    on_shutdown_fired,
    debug,
    no_rich,
):
    _start_event_worker_process(
        handler_class,
        config,
        lifecycle_lock,
        on_startup_fired,
        on_shutdown_fired,
        'background',
        debug,
        no_rich,
    )
