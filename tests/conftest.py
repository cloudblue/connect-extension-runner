#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import contextlib
import socket

import pytest
import websockets

from connect.eaas.extension import Extension, ProcessingResponse


@pytest.fixture(scope='session')
def unused_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def ws_server(unused_port):
    @contextlib.asynccontextmanager
    async def _ws_server(handler):
        async with websockets.serve(
            handler,
            '127.0.0.1',
            unused_port,
            process_request=handler.process_request,
        ):
            yield
    return _ws_server


@pytest.fixture
def extension_cls():
    def _extension(method_name, result=None, async_impl=False):
        class TestExtension(Extension):
            pass

        def ext_method(self, request):
            return result or ProcessingResponse.done()

        async def async_ext_method(self, request):
            return result or ProcessingResponse.done()

        if async_impl:
            setattr(TestExtension, method_name, async_ext_method)
        else:
            setattr(TestExtension, method_name, ext_method)

        return TestExtension

    return _extension
