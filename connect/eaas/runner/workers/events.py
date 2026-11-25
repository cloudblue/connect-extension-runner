#
# This file is part of the CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2025 CloudBlue. All Rights Reserved.
#
import asyncio
import logging
import signal
from dataclasses import (
    dataclass,
)

from opentelemetry import (
    trace,
)

from connect.eaas.core.enums import (
    ResultType,
    TaskCategory,
)
from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
    Task,
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
from connect.eaas.runner.tracing import (
    configure_worker_tracing,
    start_interactive_span,
)
from connect.eaas.runner.workers.base import (
    WorkerBase,
)


logger = logging.getLogger(__name__)


@dataclass
class TracedResult:
    task: Task
    span: trace.Span


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
        self._interactive_spans = set()
        self.interactive_manager = InteractiveTasksManager(
            self.config,
            self.handler,
            self._enqueue_interactive_result,
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
        span = None
        if task_data.options.task_category == TaskCategory.INTERACTIVE:
            span = start_interactive_span(task_data)
        if span is None or not span.is_recording():
            await manager.submit(task_data)
        else:
            self._interactive_spans.add(span)
            scheduled = False
            try:
                with trace.use_span(span, record_exception=False, set_status_on_exception=False):
                    scheduled = await manager.submit(task_data)
            finally:
                if not scheduled:
                    self._end_interactive_span(span, failed=True)
        logger.info(f'task {task_data.options.task_id} submitted for processing')

    def _end_interactive_span(self, span, failed):
        self._interactive_spans.discard(span)
        if failed:
            span.set_status(trace.StatusCode.ERROR)
        span.end()

    async def _enqueue_interactive_result(self, task):
        span = trace.get_current_span()
        if span in self._interactive_spans:
            span.add_event('result.queued')
            await self.results_queue.put(TracedResult(task, span))
        else:
            await self.results_queue.put(task)

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
            item = await self.results_queue.get()
            if isinstance(item, TracedResult):
                item.span.add_event('result.dequeued')
                sent = False
                try:
                    sent = await self._send_result(item.task, item.span)
                finally:
                    failed = not sent or item.task.output.result == ResultType.FAIL
                    self._end_interactive_span(item.span, failed)
            else:
                await self._send_result(item)

            if not self.run_event.is_set():
                logger.info(
                    f'Current processing status: running={self.running_tasks} '
                    f'results={self.results_queue.qsize()}',
                )

    async def _send_result(self, result, span=None):
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
                if span is not None:
                    span.add_event('websocket.send', {'attempt': retries + 1})
                await self.send(message.serialize())
                logger.info(f'Result for task {result.options.task_id} has been sent.')
                return True
            except Exception:
                logger.warning(
                    f'Attempt {retries} to send results for task '
                    f'{result.options.task_id} has failed.',
                )
                retries += 1
                await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
        logger.warning(
            f'Max retries exceeded ({RESULT_SENDER_MAX_RETRIES})'
            f' for sending results of task {result.options.task_id}',
        )
        return False

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
        try:
            await super().start()
        finally:
            for span in list(self._interactive_spans):
                self._end_interactive_span(span, failed=True)

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
    provider = configure_worker_tracing()
    try:
        loop.run_until_complete(worker.start())
    finally:
        if provider is not None:
            provider.shutdown()


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
