#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from asyncio.exceptions import TimeoutError

import backoff
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatusCode,
)

from connect.eaas.core.proto import (
    Message,
    MessageType,
)
from connect.eaas.runner.constants import (
    DELAY_ON_CONNECT_EXCEPTION_SECONDS,
    MAX_RETRY_DELAY_TIME_SECONDS,
    MAX_RETRY_TIME_GENERIC_SECONDS,
    MAX_RETRY_TIME_MAINTENANCE_SECONDS,
)
from connect.eaas.runner.exceptions import (
    CommunicationError,
    MaintenanceError,
    StopBackoffError,
)
from connect.eaas.runner.helpers import to_ordinal


logger = logging.getLogger(__name__)


def _get_max_retry_time_maintenance():
    return MAX_RETRY_TIME_MAINTENANCE_SECONDS


def _get_max_retry_time_generic():
    return MAX_RETRY_TIME_GENERIC_SECONDS


def _get_max_retry_delay_time():
    return MAX_RETRY_DELAY_TIME_SECONDS


class WorkerBase(ABC):
    """
    The Worker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, config):
        self.config = config
        self.lock = asyncio.Lock()
        self.run_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.ws = None
        self.main_task = None
        self.results_task = None

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
                    except ConnectionClosedError:
                        logger.warning(
                            'Connection closed by the host...',
                        )
                        raise CommunicationError()
                    except Exception as e:
                        logger.exception(f'Received an unexpected exception: {e}...')
                        raise CommunicationError()

        await _connect()

    async def do_handshake(self):
        await self.send(self.get_setup_request())
        message = await asyncio.wait_for(self.ws.recv(), timeout=5)
        message = Message.deserialize(json.loads(message))
        self.process_setup_response(message.data)

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
            print(message)
            return json.loads(message)
        except TimeoutError:  # pragma: no cover
            pass

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

    def process_setup_response(self, data):
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
        self.run_event.set()
        logger.info('Control worker started')
        await self.stop_event.wait()
        await self.stopping()
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
        try:
            msg = Message(version=2, message_type=MessageType.SHUTDOWN)
            await self.send(msg.serialize())
        except ConnectionClosedError:
            pass
        except Exception:
            logger.exception('Cannot send shutdown message')

    def handle_signal(self):
        asyncio.create_task(self.send_shutdown())
        time.sleep(1)
        self.stop()

    @abstractmethod
    def get_url(self):
        raise NotImplementedError()

    @abstractmethod
    def get_setup_request(self):
        raise NotImplementedError()

    @abstractmethod
    async def stopping(self):
        raise NotImplementedError()

    @abstractmethod
    async def process_message(self, data):
        raise NotImplementedError()

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
