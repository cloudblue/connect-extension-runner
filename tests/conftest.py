#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import contextlib
import socket

import pytest
import responses as sentry_responses
import websockets

from connect.eaas.core.extension import (
    Extension,
)
from connect.eaas.core.responses import (
    ProcessingResponse,
)
from connect.eaas.runner.constants import (
    BACKGROUND_EVENT_TYPES,
)


@pytest.fixture
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
            async_ext_method.__name__ = method_name
            setattr(TestExtension, method_name, async_ext_method)
        else:
            ext_method.__name__ = method_name
            setattr(TestExtension, method_name, ext_method)

        return TestExtension

    return _extension


@pytest.fixture(scope='session')
def settings_payload():
    return {
        'variables': [{'name': 'conf1', 'value': 'val1', 'secure': False}],
        'environment_type': 'development',
        'logging': {
            'logging_api_key': None,
            'log_level': 'DEBUG',
            'runner_log_level': 'INFO',
            'meta': {
                'account_id': 'account_id',
                'account_name': 'account_name',
                'service_id': 'service_id',
                'products': ['product_id'],
                'hub_id': 'HB-0000',
            },
        },
        'event_definitions': [
            {
                'event_type': evt_type,
                'api_collection_endpoint': 'collection',
                'api_resource_endpoint': 'collection/{pk}',
                'api_collection_filter': 'and(eq(id,${_object_id_}),in(status,${_statuses_}))',
            }
            for evt_type in BACKGROUND_EVENT_TYPES
        ],
        'model_type': 'setup_response',
        'proxied_connect_api': [],
    }


@pytest.fixture(scope='session')
def tfn_settings_payload():
    return {
        'variables': [{'name': 'conf1', 'value': 'val1', 'secure': False}],
        'environment_type': 'development',
        'logging': {
            'logging_api_key': None,
            'log_level': 'DEBUG',
            'runner_log_level': 'INFO',
            'meta': {
                'account_id': 'account_id',
                'account_name': 'account_name',
                'service_id': 'service_id',
                'products': ['product_id'],
                'hub_id': 'HB-0000',
            },
        },
        'event_definitions': [
            {
                'event_type': 'transformation_request',
                'api_collection_endpoint': 'collection',
                'api_resource_endpoint': 'collection/{pk}',
                'api_collection_filter': 'and(eq(id,${_object_id_}),eq(status,pending))',
            },
        ],
        'model_type': 'setup_response',
    }


@pytest.fixture
def task_payload():
    def _task_payload(
        task_category, event_type, object_id,
        runtime=0.0, api_key=None, installation_id=None,
        connect_correlation_id=None,
    ):
        return {
            'options': {
                'task_id': 'TQ-000',
                'task_category': task_category,
                'runtime': runtime,
                'api_key': api_key,
                'installation_id': installation_id,
                'connect_correlation_id': connect_correlation_id,
            },
            'input': {
                'event_type': event_type,
                'object_id': object_id,
            },

        }
    return _task_payload


@pytest.fixture
def responses():
    with sentry_responses.RequestsMock() as rsps:
        yield rsps
