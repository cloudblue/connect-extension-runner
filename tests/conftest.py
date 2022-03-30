#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import contextlib
import socket

import pytest
import websockets

from connect.eaas.core.extension import Extension, ProcessingResponse


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
    def _extension(method_name, result=None, async_impl=False, exception=None):
        class TestExtension(Extension):
            pass

        def ext_method(self, request):
            if exception:
                raise exception
            return result or ProcessingResponse.done()

        async def async_ext_method(self, request):
            if exception:
                raise exception
            return result or ProcessingResponse.done()

        if async_impl:
            setattr(TestExtension, method_name, async_ext_method)
        else:
            setattr(TestExtension, method_name, ext_method)

        return TestExtension

    return _extension


@pytest.fixture(scope='session')
def settings_payload():
    return {
        'variables': {'conf1': 'val1'},
        'environment_type': 'development',
        'logging': {
            'logging_api_key': None,
            'log_level': 'DEBUG',
            'runner_log_level': 'INFO',
        },
        'service': {
            'account_id': 'account_id',
            'account_name': 'account_name',
            'service_id': 'service_id',
            'product_id': 'product_id',
            'hub_id': 'HB-0000',
        },
    }


@pytest.fixture
def task_payload():
    def _task_payload(task_category, event_type, object_id, runtime=0.0):
        return {
            'options': {
                'task_id': 'TQ-000',
                'task_category': task_category,
                'runtime': runtime,
            },
            'input': {
                'event_type': event_type,
                'object_id': object_id,
            },

        }
    return _task_payload
