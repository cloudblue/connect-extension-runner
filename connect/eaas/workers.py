#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import inspect
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import websockets
from logzio.handler import LogzioHandler

from connect.client import AsyncConnectClient, ConnectClient
from connect.eaas.constants import OBJ_TYPE_EXT_METHOD_MAP, OBJ_TYPE_TASK_CATEGORY
from connect.eaas.helpers import (
    get_environment,
    get_extension_class,
    get_extension_type,
)

logger = logging.getLogger('eaas.workers')


class JSONWorkerMixin:
    async def send(self, message):
        await self.ws.send(json.dumps(message))

    async def receive(self):
        message = await self.ws.recv()
        return json.loads(message)


class TasksWorker(JSONWorkerMixin):
    def __init__(
        self,
        server_address,
        api_key,
        environment_id,
        instance_id,
        extension,
        secure=True,
    ):
        self.server_address = server_address
        self.api_key = api_key
        self.environment_id = environment_id
        self.instance_id = instance_id
        proto = 'wss' if secure else 'ws'
        self.base_ws_url = f'{proto}://{self.server_address}/public/v1/devops/ws'
        self.extension = extension
        self.background_executor = ThreadPoolExecutor()
        self.interactive_executor = ThreadPoolExecutor()
        self.loop = asyncio.get_running_loop()
        self.run_event = asyncio.Event()
        self.client = AsyncConnectClient(
            self.api_key,
            endpoint=f'https://{self.server_address}/public/v1',
            use_specs=False,
        )
        self.ws = None

    async def run(self):
        await self.run_event.wait()
        headers = (('Authorization', self.api_key),)
        while self.run_event.is_set():
            try:
                async with websockets.connect(self.get_url(), extra_headers=headers) as self.ws:
                    while self.run_event.is_set():
                        task_data = await self.receive()
                        task_id = task_data['task_id']
                        object_id = task_data['object_id']
                        object_type = task_data['object_type']
                        method_name = OBJ_TYPE_EXT_METHOD_MAP[object_type]
                        method = getattr(self.extension, method_name)
                        request = await self.get_request(object_id, object_type)
                        if inspect.iscoroutinefunction(method):
                            future = asyncio.create_task(method(request))
                        else:
                            future = self.loop.run_in_executor(
                                self.get_executor(object_type),
                                method,
                                request,
                            )
                        asyncio.create_task(self.send_result(task_id, object_type, future))
            except websockets.exceptions.ConnectionClosedOK:
                break

    def get_executor(self, object_type):
        if OBJ_TYPE_TASK_CATEGORY[object_type] == 'bg':
            return self.background_executor
        return self.interactive_executor

    def get_url(self):
        return f'{self.base_ws_url}/tasks/{self.environment_id}/{self.instance_id}'

    async def get_request(self, object_id, object_type):
        if object_type in ('asset_request', 'asset_request_validation'):
            return await self.client.requests[object_id].get()
        if object_type in ('tier_config_request', 'tier_config_request_validation'):
            return await self.client('tier').config_requests[object_id].get()

    async def build_bg_response(self, task_id, future):
        result = None
        try:
            result = await future
        except Exception:
            return {
                'task_id': task_id,
                'status': 'retry',
            }

        response = {
            'task_id': task_id,
            'status': result.status,
        }
        if result.status == 'reschedule':
            response['countdown'] = result.countdown
        return response

    async def send_result(self, task_id, object_type, future):
        if OBJ_TYPE_TASK_CATEGORY[object_type] == 'bg':
            await self.send(await self.build_bg_response(task_id, future))
        else:
            await self.send(await self.build_interactive_response(task_id, future))

    async def build_interactive_response(self, task_id, future):
        result = None
        try:
            result = await future
        except Exception:
            return {
                'task_id': task_id,
                'status': 'failed',
            }

        return {
            'task_id': task_id,
            'status': result.status,
            'data': result.data,
        }


class ControlWorker(JSONWorkerMixin):

    def __init__(self, secure=True):
        self.secure = secure
        env = get_environment()
        self.server_address = env['server_address']
        self.api_key = env['api_key']
        self.environment_id = env['environment_id']
        self.instance_id = env['instance_id']
        self.extension_class = get_extension_class()
        self.extension_type = get_extension_type(self.extension_class)
        self.capabilities = self.extension_class.get_descriptor()['capabilities']
        proto = 'wss' if secure else 'ws'
        self.base_ws_url = f'{proto}://{self.server_address}/public/v1/devops/ws'
        self.run_event = asyncio.Event()
        self.worker = None
        self.worker_future = None
        self.ws = None

    def get_client(self):
        client_class = ConnectClient if self.extension_type == 'sync' else AsyncConnectClient
        return client_class(
            self.api_key,
            endpoint=f'https://{self.server_address}/public/v1',
            use_specs=False,
        )

    def get_extension_logger(self, token):
        logger = logging.getLogger('eaas.extension')
        if token:
            handler = LogzioHandler(token)
            logger.addHandler(handler)
        return logger

    async def shutdown_worker(self):
        if self.worker:
            self.worker.run_event.clear()
            await self.worker_future

    async def run(self):
        url_tail = f'{self.environment_id}/{self.instance_id}'
        control_url = f'{self.base_ws_url}/control/{url_tail}'
        headers = (('Authorization', self.api_key),)

        while self.run_event.is_set():
            try:
                async with websockets.connect(control_url, extra_headers=headers) as self.ws:
                    await self.send({
                        'message_type': 'capabilities',
                        'data': {
                            'capabilities': self.capabilities,
                        },
                    })
                    while self.run_event.is_set():
                        message = await self.receive()
                        if message['message_type'] == 'configuration':
                            await self.shutdown_worker()
                            config = message['data']['configuration']
                            logging_api_key = message['data'].get('logging_api_key')
                            extension = self.extension_class(
                                self.get_client(),
                                self.get_extension_logger(logging_api_key),
                                config,
                            )
                            workers_args = (
                                self.server_address,
                                self.api_key,
                                self.environment_id,
                                self.instance_id,
                                extension,
                                self.secure,
                            )

                            self.worker = TasksWorker(*workers_args)
                            self.worker_future = asyncio.create_task(self.worker.run())

                            self.worker.run_event.set()
            except websockets.exceptions.ConnectionClosedOK:
                break

        await self.shutdown_worker()
