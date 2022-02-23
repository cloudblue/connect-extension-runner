#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import json
import logging
import time
from asyncio.exceptions import TimeoutError

import backoff
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatusCode,
)


from connect.eaas.config import ConfigHelper
from connect.eaas.constants import (
    DELAY_ON_CONNECT_EXCEPTION_SECONDS,
    MAX_RETRY_DELAY_TIME_SECONDS,
    MAX_RETRY_TIME_GENERIC_SECONDS,
    MAX_RETRY_TIME_MAINTENANCE_SECONDS,
    RESULT_SENDER_MAX_RETRIES,
    RESULT_SENDER_WAIT_GRACE_SECONDS,
)
from connect.eaas.dataclasses import (
    CapabilitiesPayload,
    Message,
    MessageType,
    parse_message,
)
from connect.eaas.exceptions import (
    CommunicationError,
    MaintenanceError,
    StopBackoffError,
)
from connect.eaas.handler import ExtensionHandler
from connect.eaas.helpers import to_ordinal
from connect.eaas.managers import (
    BackgroundTasksManager,
    InteractiveTasksManager,
    ScheduledTasksManager,
)


logger = logging.getLogger(__name__)


def _get_max_retry_time_maintenance():
    return MAX_RETRY_TIME_MAINTENANCE_SECONDS


def _get_max_retry_time_generic():
    return MAX_RETRY_TIME_GENERIC_SECONDS


def _get_max_retry_delay_time():
    return MAX_RETRY_DELAY_TIME_SECONDS


class Worker:
    """
    The Worker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, secure=True, runner_type=None):
        self.runner_type = runner_type
        self.config = ConfigHelper(secure)
        self.handler = ExtensionHandler(self.config)
        self.lock = asyncio.Lock()
        self.results_queue = asyncio.Queue()
        self.run_event = asyncio.Event()
        self.stop_event = asyncio.Event()
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
        url = self.config.get_ws_url()
        url = f'{url}?running_tasks={self.background_manager.running_tasks}'
        url = f'{url}&running_scheduled_tasks={self.scheduled_manager.running_tasks}'
        if self.runner_type:
            url = f'{url}&runner_type={self.runner_type}'
        return url

    async def ensure_connection(self):  # noqa: CCR001
        """
        Ensure that a websocket connection is established.
        """
        @backoff.on_exception(
            backoff.expo,
            CommunicationError,
            max_time=_get_max_retry_time_generic,
            max_value=_get_max_retry_delay_time,
            on_backoff=self._backoff_log,
            giveup=self._backoff_shutdown,
        )
        @backoff.on_exception(
            backoff.expo,
            MaintenanceError,
            max_time=_get_max_retry_time_maintenance,
            max_value=_get_max_retry_delay_time,
            on_backoff=self._backoff_log,
            giveup=self._backoff_shutdown,
        )
        async def _connect():
            async with self.lock:
                if self.ws is None or not self.ws.open:
                    try:
                        url = self.get_url()
                        self.ws = await websockets.connect(
                            url,
                            extra_headers=self.config.get_headers(),
                            ping_interval=60,
                            ping_timeout=60,
                            max_queue=128,
                        )
                        await (await self.ws.ping())
                        await self.do_handshake()
                        logger.info(f'Connected to {url}')
                    except InvalidStatusCode as ic:
                        if ic.status_code == 502:
                            logger.warning('Maintenance in progress...')
                            raise MaintenanceError()
                        else:
                            logger.warning(
                                f'Received an unexpected status from server: {ic.status_code}...',
                            )
                            raise CommunicationError()
                    except Exception as e:
                        logger.warning(f'Received an unexpected exception: {e}...')
                        raise CommunicationError()

        await _connect()

    async def do_handshake(self):
        await self.send(self.get_capabilities())
        message = await asyncio.wait_for(self.ws.recv(), timeout=5)
        message = parse_message(json.loads(message))
        await self.process_configuration(message.data)

    async def send(self, message):
        """
        Send a message to the websocket server.
        """
        await self.ws.send(json.dumps(message))

    async def receive(self):
        """
        Receive a message from the websocket server.
        """
        try:
            message = await asyncio.wait_for(self.ws.recv(), timeout=1)
            # message = await self.ws.recv()
            return json.loads(message)
        except TimeoutError:  # pragma: no cover
            pass

    def get_capabilities(self):
        return dataclasses.asdict(
            Message(
                message_type=MessageType.CAPABILITIES,
                data=CapabilitiesPayload(
                    self.handler.capabilities,
                    self.handler.variables,
                    self.handler.schedulables,
                    self.handler.readme,
                    self.handler.changelog,
                ),
            ),
        )

    async def run(self):  # noqa: CCR001
        """
        Main loop for the websocket connection.
        Once started, this worker will send the capabilities message to
        the websocket server and start a loop to receive messages from the
        websocket server.
        """
        await self.run_event.wait()
        while self.run_event.is_set():
            try:
                await self.ensure_connection()
                while self.run_event.is_set():
                    message = await self.receive()
                    if not message:
                        continue
                    logger.debug('New message received via WS')
                    await self.process_message(message)
            except (ConnectionClosedOK, StopBackoffError):
                self.run_event.clear()
                continue
            except (CommunicationError, MaintenanceError):
                logger.error('Max connection attemps reached, exit!')
                self.run_event.clear()
                continue
            except ConnectionClosedError:
                logger.warning(
                    f'Disconnected from: {self.get_url()}'
                    f', try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                )
                await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
            except InvalidStatusCode as ic:
                if ic.status_code == 502:
                    logger.warning(
                        'Maintenance in progress'
                        f', try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                    )
                    await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
                else:
                    logger.warning(
                        f'Received an unexpected status from server: {ic.status_code}'
                        f', try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                    )
                    await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
            except Exception as e:
                logger.exception(
                    f'Unexpected exception {e}'
                    f', try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                )
                await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
        logger.info('Consumer loop exited!')

    async def process_message(self, data):
        """
        Process a message received from the websocket server.
        """
        message = parse_message(data)
        if message.message_type == MessageType.CONFIGURATION:
            await self.process_configuration(message.data)
        elif message.message_type == MessageType.TASK:
            await self.process_task(message.data)
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def process_task(self, task_data):
        """Send a task to a manager based on task category."""
        logger.info(f'received new {task_data.task_category} task: {task_data.task_id}')
        manager = getattr(self, f'{task_data.task_category}_manager')
        await manager.submit(task_data)
        logger.info(f'task {task_data.task_id} submitted for processing')

    async def result_sender(self):  # noqa: CCR001
        """
        Dequeues results from the results queue and send it to
        the EaaS backend.
        """
        await self.run_event.wait()
        while True:
            if self.results_queue.empty():
                if not self.run_event.is_set() and self.running_tasks == 0:
                    logger.info('Worker exiting and no more running tasks: exit!')
                    return
                await asyncio.sleep(.5)
                continue
            logger.info(
                f'Current processing status: running={self.running_tasks} '
                f'results={self.results_queue.qsize()}',
            )
            result = await self.results_queue.get()
            logger.info(f'Got a result from queue: {result.task_id}')
            retries = 0
            while retries < RESULT_SENDER_MAX_RETRIES:
                try:
                    message = Message(
                        message_type=MessageType.TASK,
                        data=result,
                    )
                    await self.ensure_connection()
                    await self.send(dataclasses.asdict(message))
                    logger.info(f'Result for task {result.task_id} has been sent.')
                    break
                except Exception:
                    logger.warning(
                        f'Attemp {retries} to send results for task {result.task_id} has failed.',
                    )
                    retries += 1
                    await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
            else:
                logger.warning(
                    f'Max retries exceeded ({RESULT_SENDER_MAX_RETRIES})'
                    f' for sending results of task {result.task_id}',
                )

            if not self.run_event.is_set():
                logger.info(
                    f'Current processing status: running={self.running_tasks} '
                    f'results={self.results_queue.qsize()}',
                )

    async def process_configuration(self, data):
        """
        Process the configuration message.
        It will stop the tasks manager so the extension can be
        reconfigured, then restart the tasks manager.
        """
        self.config.update_dynamic_config(data)
        logger.info('Extension configuration has been updated.')

    async def shutdown(self):
        """
        Shutdown the extension runner.
        """
        logger.info('Shutdown extension runner.')
        self.stop()

    async def start(self):
        """
        Start the runner.
        """
        logger.info('Starting control worker...')
        self.main_task = asyncio.create_task(self.run())
        self.results_task = asyncio.create_task(self.result_sender())
        self.run_event.set()
        logger.info('Control worker started')
        await self.stop_event.wait()
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

        await self.main_task
        if self.ws:
            await self.ws.close()
        logger.info('Control worker stopped')

    def stop(self):
        """
        Stop the runner.
        """
        logger.info('Stopping control worker...')
        self.run_event.clear()
        self.stop_event.set()

    async def send_shutdown(self):
        msg = Message(MessageType.SHUTDOWN)
        await self.send(dataclasses.asdict(msg))

    def handle_signal(self):
        asyncio.create_task(self.send_shutdown())
        time.sleep(1)
        self.stop()

    def _backoff_shutdown(self, _):
        if not self.run_event.is_set():
            logger.info('Worker exiting, stop backoff loop')
            raise StopBackoffError()

    def _backoff_log(self, details):
        logger.info(
            f'{to_ordinal(details["tries"])} communication attempt failed, backing off waiting '
            f'{details["wait"]:.2f} seconds after next retry. '
            f'Elapsed time: {details["elapsed"]:.2f}'
            ' seconds.',
        )
