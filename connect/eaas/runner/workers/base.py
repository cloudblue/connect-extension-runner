#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import inspect
import json
import logging
import time
from abc import (
    ABC,
    abstractmethod,
)
from asyncio.exceptions import (
    TimeoutError,
)

import backoff
import websockets
from devtools import (
    pformat,
)
from python_socks.async_.asyncio import (
    Proxy,
)
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
    SHUTDOWN_WAIT_GRACE_SECONDS,
)
from connect.eaas.runner.exceptions import (
    CommunicationError,
    MaintenanceError,
    StopBackoffError,
)
from connect.eaas.runner.helpers import (
    to_ordinal,
)


logger = logging.getLogger(__name__)


def _get_max_retry_time_maintenance():
    return MAX_RETRY_TIME_MAINTENANCE_SECONDS


def _get_max_retry_time_generic():
    return MAX_RETRY_TIME_GENERIC_SECONDS


def _get_max_retry_delay_time():
    return MAX_RETRY_DELAY_TIME_SECONDS


class WorkerBase(ABC):
    """
    The EventsWorker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, handler, lifecycle_lock, on_startup_fired, on_shutdown_fired):
        self.handler = handler
        self.config = handler.config
        self.lock = asyncio.Lock()
        self.run_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.ws = None
        self.main_task = None
        self.results_task = None
        self.lifecycle_lock = lifecycle_lock
        self.on_startup_fired = on_startup_fired
        self.on_shutdown_fired = on_shutdown_fired

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
                            max_size=2**21,
                            **await self.get_proxy_config(),
                        )
                        await (await self.ws.ping())
                        await self.do_handshake()
                        logger.info(f'{self} connected to {url}')
                    except InvalidStatusCode as ic:
                        if ic.status_code == 502:
                            logger.warning(f'{self}: maintenance in progress...')
                            raise MaintenanceError()
                        else:
                            logger.warning(
                                f'{self}: received an unexpected status '
                                f'from server: {ic.status_code}...',
                            )
                            raise CommunicationError()
                    except ConnectionClosedError:
                        logger.warning(
                            f'{self} connection closed by the host...',
                        )
                        raise CommunicationError()
                    except Exception as e:
                        logger.exception(f'{self}: received an unexpected exception: {e}...')
                        raise CommunicationError()

        await _connect()

    async def get_proxy_config(self):
        if not self.config.proxy:
            return {}
        proxy = Proxy.from_url(self.config.proxy)
        sock = await proxy.connect(
            dest_host=self.config.ws_address,
            dest_port=self.config.ws_port,
        )
        return {
            'sock': sock,
            'server_hostname': self.config.ws_address,
        }

    async def do_handshake(self):
        setup_request = self.get_setup_request()
        await self.send(setup_request)
        message = await asyncio.wait_for(self.ws.recv(), timeout=5)
        await self.process_message(json.loads(message))

    async def send(self, message):
        """
        Send a message to the websocket server.
        """
        if self.ws:
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
                    await self.process_message(message)
            except (ConnectionClosedOK, StopBackoffError) as exc:
                self.stop()
                if isinstance(exc, ConnectionClosedOK):
                    logger.warning(f'The WS connection has been closed, reason: {exc.reason}')
                continue
            except (CommunicationError, MaintenanceError):
                logger.error(f'{self}: max connection attemps reached, exit!')
                self.stop()
                continue
            except ConnectionClosedError:
                logger.warning(
                    f'{self}: disconnected from: {self.get_url()}'
                    f', try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                )
                await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
            except InvalidStatusCode as ic:
                if ic.status_code == 502:
                    logger.warning(
                        f'{self}: maintenance in progress, '
                        f'try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                    )
                    await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
                else:
                    logger.warning(
                        f'{self}: received an unexpected status from server: {ic.status_code}, '
                        f'try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                    )
                    await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
            except Exception as e:
                logger.exception(
                    f'{self}: unexpected exception {e}, '
                    f'try to reconnect in {DELAY_ON_CONNECT_EXCEPTION_SECONDS}s',
                )
                await asyncio.sleep(DELAY_ON_CONNECT_EXCEPTION_SECONDS)
        logger.info(f'{self} main loop exited!')

    async def process_setup_response(self, data):
        """
        Process the configuration message.
        It will stop the tasks manager so the extension can be
        reconfigured, then restart the tasks manager.
        """
        self.config.update_dynamic_config(data)
        await self.trigger_event('on_startup')
        logger.info('Extension configuration has been updated.')

    async def trigger_event(self, event):
        with self.lifecycle_lock:
            if event == 'on_startup':
                if self.on_startup_fired.value:
                    return
                self.on_startup_fired.value = 1
            if event == 'on_shutdown':
                if self.on_shutdown_fired.value:
                    return
                self.on_shutdown_fired.value = 1

        application = self.handler.get_application()
        event_handler = getattr(application, event, None)
        if (
            event_handler
            and inspect.ismethod(event_handler)
            and event_handler.__self__ is application
        ):
            if inspect.iscoroutinefunction(event_handler):
                await event_handler(self.handler.get_logger(), self.config.variables)
            else:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    event_handler,
                    self.handler.get_logger(),
                    self.config.variables,
                )

    async def shutdown(self):
        """
        Shutdown the extension runner.
        """
        logger.info(f'{self}: shutting down...')
        self.stop()

    async def start(self):
        """
        Start the runner.
        """
        logger.info(f'Starting {self}...')
        self.main_task = asyncio.create_task(self.run())
        self.run_event.set()
        logger.info(f'{self} started')
        await self.stop_event.wait()
        await self.stopping()
        await self.trigger_event('on_shutdown')
        await self.main_task
        if self.ws:
            await self.ws.close()
        logger.info(f'{self} stopped')

    def stop(self):
        """
        Stop the runner.
        """
        logger.info(f'Stopping {self}...')
        self.run_event.clear()
        self.stop_event.set()

    async def send_shutdown(self):
        try:
            msg = Message(version=2, message_type=MessageType.SHUTDOWN)
            logger.debug(f'Sending message: {self.prettify(msg)}')
            await self.send(msg.serialize())
        except ConnectionClosedError:
            pass
        except Exception:
            logger.exception(f'{self}: cannot send shutdown message')

    def handle_signal(self):
        asyncio.create_task(self.send_shutdown())
        time.sleep(SHUTDOWN_WAIT_GRACE_SECONDS)
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

    def prettify(self, msg):
        if logger.isEnabledFor(logging.DEBUG):
            return pformat(msg)
        return '<...>'

    def _backoff_shutdown(self, _):
        if not self.run_event.is_set():
            logger.info(f'{self} exiting, stop backoff loop')
            raise StopBackoffError()

    def _backoff_log(self, details):
        logger.info(
            f'{self} {to_ordinal(details["tries"])} communication attempt failed, '
            f'backing off waiting {details["wait"]:.2f} seconds after next retry. '
            f'Elapsed time: {details["elapsed"]:.2f}'
            ' seconds.',
        )

    def __repr__(self):
        return self.__class__.__name__
