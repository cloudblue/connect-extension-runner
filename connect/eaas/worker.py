#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
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

from connect.client import AsyncConnectClient, ConnectClient
from connect.eaas.dataclasses import CapabilitiesPayload, Message, MessageType
from connect.eaas.helpers import (
    get_environment,
    get_extension_class,
    get_extension_type,
)
from connect.eaas.logging import ExtensionLogHandler, RequestLogger
from connect.eaas.manager import TasksManager


logger = logging.getLogger(__name__)


class Worker:
    """
    The Worker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, secure=True):
        self.secure = secure
        env = get_environment()
        self.ws_address = env['ws_address']
        self.api_address = env['api_address']
        self.api_key = env['api_key']
        self.environment_id = env['environment_id']
        self.instance_id = env['instance_id']
        self.headers = (('Authorization', self.api_key),)
        proto = 'wss' if secure else 'ws'
        self.base_ws_url = f'{proto}://{self.ws_address}/public/v1/devops/ws'
        self.run_event = asyncio.Event()
        self.ws = None
        self.extension_class = get_extension_class()
        self.extension_type = get_extension_type(self.extension_class)
        descriptor = self.extension_class.get_descriptor()
        self.capabilities = descriptor['capabilities']
        self.readme_url = descriptor['readme_url']
        self.changelog_url = descriptor['changelog_url']
        self.extension_config = None
        self.logging_api_key = None
        self.main_task = None
        self.tasks_manager = None
        self.paused = False
        self.logging_handler = None
        self.environment_type = None

    async def ensure_connection(self):
        """
        Ensure that a websocket connection is established.
        """
        if self.ws is None or self.ws.closed:
            url = self.get_url()
            self.ws = await websockets.connect(
                url,
                extra_headers=self.headers,
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
        except TimeoutError:
            pass

    def get_client(self):
        """
        Get an instance of the Connect Openapi Client. If the extension is asyncrhonous
        it returns an instance of the AsyncConnectClient otherwise the ConnectClient.
        """
        client_class = ConnectClient if self.extension_type == 'sync' else AsyncConnectClient
        return client_class(
            self.api_key,
            endpoint=f'https://{self.api_address}/public/v1',
            use_specs=False,
            logger=RequestLogger(
                self.get_extension_logger(self.logging_api_key),
            ),
        )

    def get_extension_logger(self, token):
        """
        Returns a logger instance configured with the LogZ.io handler.
        This logger will be used by the extension to send logging records
        to the Logz.io service.
        """
        logger = logging.getLogger('eaas.extension')
        if self.logging_handler is None and token is not None:
            self.logging_handler = ExtensionLogHandler(
                token,
                default_extra_fields={
                    'environment_id': self.environment_id,
                    'instance_id': self.instance_id,
                    'environment_type': self.environment_type,
                },
            )
            logger.addHandler(self.logging_handler)
        return logger

    async def stop_tasks_manager(self):
        if self.tasks_manager:
            logger.debug('shutting down tasks worker....')
            await self.tasks_manager.stop()

    def start_tasks_manager(self):
        logger.info('Starting tasks worker...')
        self.tasks_manager = TasksManager(self)
        self.tasks_manager.start()
        logger.info('Task worker started')

    def ensure_tasks_manager_running(self):
        if not self.paused and (self.tasks_manager is None or not self.tasks_manager.is_running()):
            self.start_tasks_manager()

    def get_url(self):
        running_tasks = (
            self.tasks_manager.running_tasks
            if self.tasks_manager else 0
        )
        url = f'{self.base_ws_url}/{self.environment_id}/{self.instance_id}'
        return f'{url}?running_tasks={running_tasks}'

    def get_extension(self):
        return self.extension_class(
            self.get_client(),
            self.get_extension_logger(self.logging_api_key),
            self.extension_config,
        )

    async def run(self):  # noqa: CCR001
        """
        Main loop for the websocket connection.
        Once started, this worker will send the capabilities message to
        the websocket server and start a loop to receive messages from the
        websocket server.
        """
        while self.run_event.is_set():
            try:
                await self.ensure_connection()
                message = Message(
                    message_type=MessageType.CAPABILITIES,
                    data=CapabilitiesPayload(
                        self.capabilities,
                        self.readme_url,
                        self.changelog_url,
                    ),
                )
                await self.send(message.to_json())
                while self.run_event.is_set():
                    await self.ensure_connection()
                    self.ensure_tasks_manager_running()
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

        await self.stop_tasks_manager()
        if self.ws:
            await self.ws.close()

    async def process_message(self, data):
        """
        Process a message received from the websocket server.
        """
        message = Message(**data)
        if message.message_type == MessageType.CONFIGURATION:
            await self.configuration(message.data)
        elif message.message_type == MessageType.TASK:
            await self.tasks_manager.submit_task(message.data)
        elif message.message_type == MessageType.PAUSE:
            await self.pause()
        elif message.message_type == MessageType.RESUME:
            await self.resume()
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def configuration(self, data):
        """
        Process the configuration message.
        It will stop the tasks manager so the extension can be
        reconfigured, then restart the tasks manager.
        """
        if data.configuration:
            self.extension_config = data.configuration
        if data.logging_api_key:
            self.logging_api_key = data.logging_api_key
        if data.environment_type:
            self.environment_type = data.environment_type
        if data.log_level:
            logger.info(f'Change extesion logger level to {data.log_level}')
            logging.getLogger('eaas.extension').setLevel(
                getattr(logging, data.log_level),
            )
        if data.runner_log_level:
            logging.getLogger('connect.eaas').setLevel(
                getattr(logging, data.runner_log_level),
            )
        logger.info('Extension configuration has been updated.')

    async def pause(self):
        """
        Stop the task manager. No task will be consumed
        until a "resume" message is received.
        """
        self.paused = True
        logger.info('Pause task manager operations.')
        await self.stop_tasks_manager()

    async def resume(self):
        """
        Restart the task manager so it will consume tasks once again.
        """
        self.paused = False
        logger.info('Resume task manager operations.')
        self.start_tasks_manager()

    async def shutdown(self):
        """
        Shutdown the extension runner.
        """
        logger.info('Shutdown extension runner.')
        await self.pause()
        self.stop()

    async def start(self):
        logger.info('Starting control worker...')
        self.main_task = asyncio.create_task(self.run())
        logger.info('Control worker started')
        self.run_event.set()
        await self.main_task
        logger.info('Control worker stopped')

    def stop(self):
        logger.info('Stopping control worker...')
        self.run_event.clear()
