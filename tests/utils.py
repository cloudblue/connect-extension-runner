#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import json
import logging


logger = logging.getLogger('connect.eaas')


class WSHandler:
    def __init__(self, path, data, actions):
        self.path = path
        self.data = data if isinstance(data, list) else [data]
        self.actions = actions
        self.headers = None
        self.received = []

    async def __call__(self, ws, path):
        if path != self.path:
            return
        for action in self.actions:
            if action == 'send':
                await self.send_data(ws)
            if action == 'receive':
                await self.receive_data(ws)
            await asyncio.sleep(.1)
        await asyncio.sleep(.1)

    async def send_data(self, ws):
        try:
            data = self.data.pop(0)
            await ws.send(json.dumps(data))
        except Exception:
            pass

    async def receive_data(self, ws):
        data = await ws.recv()
        if data:
            self.received.append(json.loads(data))

    def assert_received(self, data):
        assert data in self.received

    def assert_header(self, name, value):
        assert name in self.headers
        assert self.headers[name] == value

    def assert_path(self, expected):
        assert self.path == expected

    async def process_request(self, path, headers):
        self.headers = headers


class CloseAfterSetupWSHandler:
    """
    Fake gateway socket: answer the setup request, optionally send more
    messages, then close the socket with `close_code` (None drops the TCP
    connection). Counts connections so tests can check reconnects.
    """
    def __init__(self, path, setup_response, close_code, extra_messages=None):
        self.path = path
        self.setup_response = setup_response
        self.close_code = close_code
        self.extra_messages = extra_messages or []
        self.connections = 0
        self.rejected = 0
        self.reject_status = None

    async def __call__(self, ws, path):
        if path != self.path:
            return
        self.connections += 1
        await ws.recv()
        await ws.send(json.dumps(self.setup_response))
        for message in self.extra_messages:
            await ws.send(json.dumps(message))
        await asyncio.sleep(.3)
        if self.close_code is None:
            ws.transport.abort()
        else:
            await ws.close(code=self.close_code, reason='closed by the test server')

    async def process_request(self, path, headers):
        if self.reject_status:
            self.rejected += 1
            return self.reject_status, [], b''
