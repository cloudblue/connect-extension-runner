#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import logging
import json


logger = logging.getLogger('ws-handler')


class WSHandler:
    def __init__(self, path, data, send_first=False, receive_count=1):
        self.path = path
        self.data = data
        self.send_first = send_first
        self.headers = None
        self.receive_count = receive_count
        self.received = []

    async def __call__(self, ws, path):
        if path != self.path:
            return
        if self.send_first:
            logger.info('server send first')
            await self.send_data(ws)
            await self.receive_data(ws)
        else:
            logger.info('client send first')
            await self.receive_data(ws)
            await self.send_data(ws)

    async def send_data(self, ws):
        if isinstance(self.data, dict):
            logger.info(f'send single message to client: {self.data}')
            await ws.send(json.dumps(self.data))
            return

        for message in self.data:
            logger.info(f'send message to client: {message}')
            await ws.send(json.dumps(message))

    async def receive_data(self, ws):
        i = 0
        logger.info(f'receiving {self.receive_count} message/s from client')
        while i < self.receive_count:
            message = json.loads(await ws.recv())
            logger.info(f'received message #{i} from client: {message}')
            self.received.append(message)
            i += 1

    def assert_received(self, data):
        assert data in self.received

    def assert_header(self, name, value):
        assert name in self.headers
        assert self.headers[name] == value

    def assert_path(self, expected):
        assert self.path == expected

    async def process_request(self, path, headers):
        self.headers = headers
