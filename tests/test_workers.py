#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio

import pytest

from connect.eaas.extension import Extension, OK, Reschedule, SKIP
from connect.eaas.workers import ControlWorker, TasksWorker

from tests.utils import WSHandler


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('response', 'expected'),
    (
        (OK, {'task_id': 'TQ-000-000', 'status': OK.status}),
        (SKIP, {'task_id': 'TQ-000-000', 'status': SKIP.status}),
        (
            Reschedule(),
            {
                'task_id': 'TQ-000-000',
                'status': Reschedule().status,
                'countdown': Reschedule().countdown,
            },
        ),
        (
            Reschedule(50),
            {
                'task_id': 'TQ-000-000',
                'status': Reschedule(50).status,
                'countdown': Reschedule(50).countdown,
            },
        ),
    ),
)
async def test_worker_bg(ws_server, unused_port, httpx_mock, response, expected):

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
        json=pr_data,
    )

    class MyExtension(Extension):
        def process_asset_request(self, request):
            assert request == pr_data
            return response

    ext = MyExtension(None, None, None)

    data_to_send = {
        'task_id': 'TQ-000-000',
        'object_id': 'PR-000',
        'object_type': 'asset_request',
    }

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received(expected)
    handler.assert_header('Authorization', 'SU-000:XXXX')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('response', 'expected'),
    (
        (OK, {'task_id': 'TQ-000-000', 'status': OK.status}),
        (SKIP, {'task_id': 'TQ-000-000', 'status': SKIP.status}),
        (
            Reschedule(),
            {
                'task_id': 'TQ-000-000',
                'status': Reschedule().status,
                'countdown': Reschedule().countdown,
            },
        ),
        (
            Reschedule(50),
            {
                'task_id': 'TQ-000-000',
                'status': Reschedule(50).status,
                'countdown': Reschedule(50).countdown,
            },
        ),
    ),
)
async def test_worker_bg_async_handler(
    ws_server, unused_port, httpx_mock, response, expected,
):

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
    )

    class MyExtension(Extension):
        async def process_asset_request(self, request):
            return response

    ext = MyExtension(None, None, None)

    data_to_send = {
        'task_id': 'TQ-000-000',
        'object_id': 'PR-000',
        'object_type': 'asset_request',
    }

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received(expected)
    handler.assert_header('Authorization', 'SU-000:XXXX')


@pytest.mark.asyncio
async def test_worker_bg_multi_message(ws_server, unused_port, httpx_mock):

    for i in range(3):
        httpx_mock.add_response(
            method='GET',
            url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-00{i}',
        )

    class MyExtension(Extension):
        def process_asset_request(self, request):
            return OK

    ext = MyExtension(None, None, None)

    data_to_send = [
        {
            'task_id': f'TQ-000-00{i}',
            'object_id': f'PR-00{i}',
            'object_type': 'asset_request',
        }
        for i in range(3)
    ]

    expected = [
        {
            'task_id': f'TQ-000-00{i}',
            'status': OK.status,
        }
        for i in range(3)
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
        receive_count=3,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    for exp in expected:
        handler.assert_received(exp)


@pytest.mark.asyncio
async def test_worker_exception(ws_server, unused_port, httpx_mock):

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
    )

    class MyExtension(Extension):
        def process_asset_request(self, request):
            raise Exception('should be captured')

    ext = MyExtension(None, None, None)

    data_to_send = {
        'task_id': 'TQ-000-000',
        'object_id': 'PR-000',
        'object_type': 'asset_request',
    }

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received({'task_id': 'TQ-000-000', 'status': 'retry'})


@pytest.mark.asyncio
async def test_worker_bg_tcr(ws_server, unused_port, httpx_mock):

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/tier/config-requests/TCR-000',
    )

    class MyExtension(Extension):
        def process_tier_config_request(self, request):
            return OK

    ext = MyExtension(None, None, None)

    data_to_send = {
        'task_id': 'TQ-000-000',
        'object_id': 'TCR-000',
        'object_type': 'tier_config_request',
    }

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received({'task_id': 'TQ-000-000', 'status': 'succeeded'})
    handler.assert_header('Authorization', 'SU-000:XXXX')


@pytest.mark.asyncio
async def test_worker_interactive(ws_server, unused_port, httpx_mock):

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
        json=pr_data,
    )

    class MyExtension(Extension):
        def validate_asset_request(self, request):
            assert request == pr_data
            request['valid'] = True
            return OK(request)

    ext = MyExtension(None, None, None)

    data_to_send = {
        'task_id': 'TQ-000-000',
        'object_id': 'PR-000',
        'object_type': 'asset_request_validation',
    }
    expected = {
        'task_id': 'TQ-000-000',
        'status': 'succeeded',
        'data': dict(**pr_data, valid=True),
    }

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received(expected)
    handler.assert_header('Authorization', 'SU-000:XXXX')


@pytest.mark.asyncio
async def test_worker_interactive_exception(ws_server, unused_port, httpx_mock):

    pr_data = {'id': 'PR-000', 'status': 'pending'}
    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
        json=pr_data,
    )

    class MyExtension(Extension):
        def validate_asset_request(self, request):
            raise Exception('should be captured')

    ext = MyExtension(None, None, None)

    data_to_send = {
        'task_id': 'TQ-000-000',
        'object_id': 'PR-000',
        'object_type': 'asset_request_validation',
        'data': {'id': 'PR-000', 'status': 'draft'},
    }

    handler = WSHandler(
        '/public/v1/devops/ws/tasks/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=True,
    )

    async with ws_server(handler):
        worker = TasksWorker(
            f'127.0.0.1:{unused_port}',
            'SU-000:XXXX',
            'ENV-000-0001',
            'INS-000-0002',
            ext,
            secure=False,
        )
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received({'task_id': 'TQ-000-000', 'status': 'failed'})


@pytest.mark.asyncio
async def test_control_worker(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.workers.get_environment',
        return_value={
            'server_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
        },
    )

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': {
                    'process_asset_request': ['pending', 'inquiring'],
                    'validate_asset_request': ['draft'],
                },
            }

    mocker.patch('connect.eaas.workers.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.workers.get_extension_type', return_value='sync')

    data_to_send = {
        'message_type': 'configuration',
        'data': {
            'configuration': {
                'var1': 'value1',
                'var2': 'value2',
            },
            'logging_api_key': 'token',
        },
    }

    handler = WSHandler(
        '/public/v1/devops/ws/control/ENV-000-0001/INS-000-0002',
        data_to_send,
        send_first=False,
    )

    async with ws_server(handler):
        worker = ControlWorker(secure=False)
        task = asyncio.create_task(worker.run(), name='control-worker')
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
        await task
    handler.assert_received(
        {
            'message_type': 'capabilities',
            'data': {
                'capabilities': {
                    'process_asset_request': ['pending', 'inquiring'],
                    'validate_asset_request': ['draft'],
                },
            },
        },
    )
