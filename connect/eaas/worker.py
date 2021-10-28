#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import json
import logging
from asyncio.exceptions import TimeoutError

import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatusCode,
    WebSocketException,
)

from connect.eaas.config import ConfigHelper
from connect.eaas.constants import RESULT_SENDER_MAX_RETRIES
from connect.eaas.dataclasses import (
    CapabilitiesPayload,
    Message,
    MessageType,
    parse_message,
)
from connect.eaas.handler import ExtensionHandler
from connect.eaas.managers import (
    BackgroundTasksManager,
    InteractiveTasksManager,
    ScheduledTasksManager,
)


logger = logging.getLogger(__name__)


class Worker:
    """
    The Worker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, secure=True):
        self.config = ConfigHelper(secure)
        self.handler = ExtensionHandler(self.config)
        self.results_queue = asyncio.Queue()
        self.run_event = asyncio.Event()
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
        self.paused = False

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
        return f'{url}&running_scheduled_tasks={self.scheduled_manager.running_tasks}'

    async def ensure_connection(self):
        """
        Ensure that a websocket connection is established.
        """
        if self.ws is None or self.ws.closed:
            url = self.get_url()
            self.ws = await websockets.connect(
                url,
                extra_headers=self.config.get_headers(),
            )
            await (await self.ws.ping())
            logger.info(f'Connected to {url}')

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
                await self.send(self.get_capabilities())
                while self.run_event.is_set():
                    await self.ensure_connection()
                    message = await self.receive()
                    if not message:
                        continue
                    await self.process_message(message)
            except ConnectionClosedOK:
                break
            except ConnectionClosedError:
                logger.warning(f'Disconnected from: {self.get_url()}, retry in 2s')
                await asyncio.sleep(2)
            except InvalidStatusCode as ic:
                if ic.status_code == 502:
                    logger.warning('Maintenance in progress, try to reconnect in 2s')
                    await asyncio.sleep(2)
                else:
                    logger.warning(f'Received an unexpected status from server: {ic.status_code}')
                    await asyncio.sleep(2)
            except WebSocketException:
                logger.exception('Unexpected websocket exception.')
                await asyncio.sleep(2)

        if self.ws:
            await self.ws.close()

    async def process_message(self, data):
        """
        Process a message received from the websocket server.
        """
        message = parse_message(data)
        if message.message_type == MessageType.CONFIGURATION:
            await self.process_configuration(message.data)
        elif message.message_type == MessageType.TASK:
            await self.process_task(message.data)
        elif message.message_type == MessageType.PAUSE:
            await self.pause()
        elif message.message_type == MessageType.RESUME:
            await self.resume()
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def process_task(self, task_data):
        """Send a task to a manager based on task category."""
        manager = getattr(self, f'{task_data.task_category}_manager')
        await manager.submit(task_data)

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
                await asyncio.sleep(.1)
                continue
            if self.ws is None or self.ws.closed:
                if not self.run_event.is_set() and self.running_tasks == 0:
                    logger.info('WS has been closed, worker shutting down and no more task: exit!')
                    return
                logger.debug('Wait WS reconnection before resuming result sender')
                await asyncio.sleep(.1)
                continue

            if self.paused:
                if not self.run_event.is_set() and self.running_tasks == 0:
                    return
                await asyncio.sleep(.1)
                continue
            result = await self.results_queue.get()
            logger.info(f'Got a result from queue: {result.task_id}')
            retries = 0
            while retries < RESULT_SENDER_MAX_RETRIES:
                try:
                    message = Message(
                        message_type=MessageType.TASK,
                        data=result,
                    )
                    await self.send(dataclasses.asdict(message))
                    logger.info(f'Result for task {result.task_id} has been sent.')
                    break
                except Exception:
                    logger.warning(
                        f'Attemp {retries} to send results for task {result.task_id} has failed.',
                    )
                    retries += 1
                    await asyncio.sleep(.1)
            else:
                logger.warning(
                    f'Max retries exceeded ({RESULT_SENDER_MAX_RETRIES})'
                    f' for sending results of task {result.task_id}',
                )

            if not self.run_event.is_set():
                logger.debug(
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

    async def pause(self):
        """
        Stop the task manager. No task will be consumed
        until a "resume" message is received.
        """
        self.paused = True
        logger.info('Pause task manager operations.')

    async def resume(self):
        """
        Restart the task manager so it will consume tasks once again.
        """
        self.paused = False
        logger.info('Resume task manager operations.')

    async def shutdown(self):
        """
        Shutdown the extension runner.
        """
        logger.info('Shutdown extension runner.')
        await self.pause()
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
        await self.results_task
        await self.main_task
        logger.info('Control worker stopped')

    def stop(self):
        """
        Stop the runner.
        """
        logger.info('Stopping control worker...')
        self.run_event.clear()
