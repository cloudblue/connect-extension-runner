import asyncio
import copy
import logging
import time

import pytest
from websockets.exceptions import ConnectionClosedError, InvalidStatusCode, WebSocketException

from connect.eaas.core.decorators import event, schedulable, variables
from connect.eaas.core.enums import EventType, ResultType, TaskCategory
from connect.eaas.core.extension import Extension
from connect.eaas.core.proto import (
    Message,
    MessageType,
    SetupRequest,
    SetupResponse,
    Task,
)
from connect.eaas.core.responses import ProcessingResponse, ScheduledExecutionResponse
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.exceptions import (
    CommunicationError,
    MaintenanceError,
    StopBackoffError,
)
from connect.eaas.runner.handlers.events import EventsApp
from connect.eaas.runner.workers.events import (
    EventsWorker,
    start_background_worker_process,
    start_interactive_worker_process,
)

from tests.utils import WSHandler


@pytest.mark.asyncio
async def test_extension_settings(mocker, ws_server, unused_port, settings_payload):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(**settings_payload),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )
    worker = None

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions=capabilities,
                variables=[],
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )

    assert worker.config.variables == {
        var['name']: var['value']
        for var in settings_payload['variables']
    }
    assert worker.config.logging_api_key == settings_payload['logging']['logging_api_key']
    assert worker.config.environment_type == settings_payload['environment_type']
    assert worker.config.account_id == settings_payload['logging']['meta']['account_id']
    assert worker.config.account_name == settings_payload['logging']['meta']['account_name']
    assert worker.config.service_id == settings_payload['logging']['meta']['service_id']
    assert worker.config.products == settings_payload['logging']['meta']['products']
    assert worker.config.hub_id == settings_payload['logging']['meta']['hub_id']


@pytest.mark.asyncio
async def test_pr_task(mocker, ws_server, unused_port, httpx_mock, settings_payload):

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    api_url = f'https://127.0.0.1:{unused_port}/public/v1'

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/PR-000',
        json=pr_data,
    )

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        def process_asset_purchase_request(self, request):
            self.logger.info('test log message')
            assert request == pr_data
            return ProcessingResponse.done()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.BACKGROUND,
                },
                input={
                    'event_type': EventType.ASSET_PURCHASE_REQUEST_PROCESSING,
                    'object_id': 'PR-000',
                },
            ),
        ).dict(),
    ]

    mocked_time = mocker.patch('connect.eaas.runner.managers.background.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions=capabilities,
                variables=[],
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.BACKGROUND,
                },
                input={
                    'event_type': EventType.ASSET_PURCHASE_REQUEST_PROCESSING,
                    'object_id': 'PR-000',
                },
                output={
                    'result': ResultType.SUCCESS,
                    'runtime': 1.0,
                },
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_pr_task_decorated(mocker, ws_server, unused_port, httpx_mock, settings_payload):

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    api_url = f'https://127.0.0.1:{unused_port}/public/v1'

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/PR-000',
        json=pr_data,
    )

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @event(
            EventType.ASSET_PURCHASE_REQUEST_PROCESSING,
            statuses=['pending', 'inquiring'],
        )
        def process_purchase(self, request):
            self.logger.info('test log message')
            assert request == pr_data
            return ProcessingResponse.done()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.BACKGROUND,
                },
                input={
                    'event_type': EventType.ASSET_PURCHASE_REQUEST_PROCESSING,
                    'object_id': 'PR-000',
                },
            ),
        ).dict(),
    ]

    mocked_time = mocker.patch('connect.eaas.runner.managers.background.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions={
                    EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
                },
                variables=[],
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.BACKGROUND,
                },
                input={
                    'event_type': EventType.ASSET_PURCHASE_REQUEST_PROCESSING,
                    'object_id': 'PR-000',
                },
                output={
                    'result': ResultType.SUCCESS,
                    'runtime': 1.0,
                },
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_tcr_task(mocker, ws_server, unused_port, httpx_mock, settings_payload):

    tcr_data = {'id': 'TCR-000', 'status': 'pending'}

    api_url = f'https://127.0.0.1:{unused_port}/public/v1'

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/collection?'
            'and(eq(id,TCR-000),in(status,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/collection/TCR-000',
        json=tcr_data,
    )

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.TIER_CONFIG_SETUP_REQUEST_PROCESSING: ['pending'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        def process_tier_config_setup_request(self, request):
            assert request == tcr_data
            return ProcessingResponse.done()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.BACKGROUND,
                },
                input={
                    'event_type': EventType.TIER_CONFIG_SETUP_REQUEST_PROCESSING,
                    'object_id': 'TCR-000',
                },
            ),
        ).dict(),
    ]
    mocked_time = mocker.patch('connect.eaas.runner.managers.background.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions=capabilities,
                variables=[],
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ).dict(),
        ),
    )
    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.BACKGROUND,
                },
                input={
                    'event_type': EventType.TIER_CONFIG_SETUP_REQUEST_PROCESSING,
                    'object_id': 'TCR-000',
                },
                output={
                    'result': ResultType.SUCCESS,
                    'runtime': 1.0,
                },
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_scheduled_task(mocker, ws_server, unused_port, httpx_mock, settings_payload):

    schedule_data = {
        'id': 'EFS-000',
        'method': 'run_scheduled_task',
        'parameter': {'param': 'data'},
    }

    schedule_url = f'https://127.0.0.1:{unused_port}/public/v1/devops'
    service_id = settings_payload['logging']['meta']['service_id']
    schedule_url = f'{schedule_url}/services/{service_id}/environments/ENV-000-0001'
    schedule_url = f'{schedule_url}/schedules/{schedule_data["id"]}'

    httpx_mock.add_response(
        method='GET',
        url=schedule_url,
        json=schedule_data,
    )

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.TIER_CONFIG_SETUP_REQUEST_PROCESSING: ['pending'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [
                    {
                        'method': 'run_scheduled_task',
                        'name': 'Run scheduled task',
                        'description': 'Description',
                    },
                ],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        def run_scheduled_task(self, schedule):
            assert schedule == schedule_data
            return ScheduledExecutionResponse.done()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.SCHEDULED,
                },
                input={
                    'event_type': EventType.SCHEDULED_EXECUTION,
                    'object_id': schedule_data['id'],
                },
            ),
        ).dict(),
    ]

    mocked_time = mocker.patch('connect.eaas.runner.managers.scheduled.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions=capabilities,
                variables=[],
                schedulables=[
                    {
                        'method': 'run_scheduled_task',
                        'name': 'Run scheduled task',
                        'description': 'Description',
                    },
                ],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.SCHEDULED,
                },
                input={
                    'event_type': EventType.SCHEDULED_EXECUTION,
                    'object_id': schedule_data['id'],
                },
                output={
                    'result': ResultType.SUCCESS,
                    'runtime': 1.0,
                },
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_scheduled_task_decorated(
    mocker, ws_server, unused_port, httpx_mock, settings_payload,
):

    schedule_data = {
        'id': 'EFS-000',
        'method': 'run_scheduled_task',
        'parameter': {'param': 'data'},
    }

    schedule_url = f'https://127.0.0.1:{unused_port}/public/v1/devops'
    service_id = settings_payload['logging']['meta']['service_id']
    schedule_url = f'{schedule_url}/services/{service_id}/environments/ENV-000-0001'
    schedule_url = f'{schedule_url}/schedules/{schedule_data["id"]}'

    httpx_mock.add_response(
        method='GET',
        url=schedule_url,
        json=schedule_data,
    )

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @schedulable('name', 'description')
        def run_scheduled_task(self, schedule):
            assert schedule == schedule_data
            return ScheduledExecutionResponse.done()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.SCHEDULED,
                },
                input={
                    'event_type': EventType.SCHEDULED_EXECUTION,
                    'object_id': schedule_data['id'],
                },
            ),
        ).dict(),
    ]

    mocked_time = mocker.patch('connect.eaas.runner.managers.scheduled.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions={},
                variables=[],
                schedulables=[
                    {
                        'method': 'run_scheduled_task',
                        'name': 'name',
                        'description': 'description',
                    },
                ],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.TASK,
            data=Task(
                options={
                    'task_id': 'TQ-000',
                    'task_category': TaskCategory.SCHEDULED,
                },
                input={
                    'event_type': EventType.SCHEDULED_EXECUTION,
                    'object_id': schedule_data['id'],
                },
                output={
                    'result': ResultType.SUCCESS,
                    'runtime': 1.0,
                },
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_shutdown(mocker, ws_server, unused_port, settings_payload):

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(message_type=MessageType.SHUTDOWN).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        assert worker.run_event.is_set() is False


@pytest.mark.asyncio
async def test_connection_closed_error(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 0.1)
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        [],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        worker.do_handshake = mocker.AsyncMock()
        worker.receive = mocker.AsyncMock(side_effect=ConnectionClosedError(1006, 'disconnected'))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert (
        f'disconnected from: ws://127.0.0.1:{unused_port}'
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002'
        '?running_tasks=0&running_scheduled_tasks=0'
    ) in caplog.text


@pytest.mark.asyncio
async def test_connection_websocket_exception(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 0.1)
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        [],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        worker.do_handshake = mocker.AsyncMock()
        worker.receive = mocker.AsyncMock(side_effect=WebSocketException('test error'))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert 'unexpected exception test error, try to reconnect in 0.1s' in caplog.text


@pytest.mark.asyncio
async def test_connection_maintenance(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 0.1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_MAINTENANCE_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        [],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        worker.do_handshake = mocker.AsyncMock()
        worker.receive = mocker.AsyncMock(side_effect=InvalidStatusCode(502, None))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert 'maintenance in progress, try to reconnect in 0.1s' in caplog.text


@pytest.mark.asyncio
async def test_connection_internal_server_error(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.runner.workers.base.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 0.1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )
    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        [],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        worker.do_handshake = mocker.AsyncMock()
        worker.receive = mocker.AsyncMock(side_effect=InvalidStatusCode(500, None))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(0.5)
            worker.stop()
            await task

    assert 'received an unexpected status from server: 500' in caplog.text


@pytest.mark.asyncio
async def test_start_stop(mocker, ws_server, unused_port, caplog):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        ['receive', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            assert f'{worker} started' in caplog.text
            worker.stop()
            await task
            assert f'{worker} stopped' in caplog.text


@pytest.mark.asyncio
async def test_extension_settings_with_vars(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    variables = [
        {'name': 'foo_var', 'initial_value': 'foo_value'},
        {'name': 'bar_var', 'initial_value': 'bar_value'},
    ]

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': variables,
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(
            variables=[
                {
                    'name': 'var1',
                    'value': 'value1',
                    'secure': False,
                },
                {
                    'name': 'var2',
                    'value': 'value2',
                    'secure': False,
                },
            ],
            logging={'logging_api_key': 'token'},
            environment_type='development',
        ),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions=capabilities,
                variables=variables,
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_extension_settings_with_vars_decorated(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    vars = [
        {'name': 'foo_var', 'initial_value': 'foo_value'},
        {'name': 'bar_var', 'initial_value': 'bar_value'},
    ]

    @variables(vars)
    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(
            variables=[
                {
                    'name': 'var1',
                    'value': 'value1',
                    'secure': False,
                },
                {
                    'name': 'var2',
                    'value': 'value2',
                    'secure': False,
                },
            ],
            logging={'logging_api_key': 'token'},
            environment_type='development',
        ),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions={},
                variables=vars,
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_extension_settings_without_vars(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.get_version', return_value='24.1')

    data_to_send = Message(
        version=2,
        message_type=MessageType.SETUP_RESPONSE,
        data=SetupResponse(
            variables=[
                {
                    'name': 'var1',
                    'value': 'value1',
                    'secure': False,
                },
                {
                    'name': 'var2',
                    'value': 'value2',
                    'secure': False,
                },
            ],
            logging={'logging_api_key': 'token'},
            environment_type='development',
        ),
    ).dict()

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                event_subscriptions=capabilities,
                variables=[],
                schedulables=[],
                repository={
                    'readme_url': 'https://example.com/README.md',
                    'changelog_url': 'https://example.com/CHANGELOG.md',
                },
                runner_version='24.1',
            ),
        ).dict(),
    )


@pytest.mark.asyncio
async def test_sender_retries(mocker, settings_payload, task_payload, caplog):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    with caplog.at_level(logging.WARNING):
        worker = EventsWorker(ext_handler)
        worker.get_extension_message = mocker.MagicMock(return_value={})
        worker.config.update_dynamic_config(SetupResponse(**settings_payload))
        worker.run = mocker.AsyncMock()
        worker.send = mocker.AsyncMock(side_effect=[Exception('retry'), None])
        worker.ws = mocker.AsyncMock(closed=False)
        await worker.results_queue.put(
            Task(**task_payload(TaskCategory.BACKGROUND, 'test', 'TQ-000')),
        )
        assert worker.results_queue.empty() is False
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.01)
        worker.stop()
        await task

    assert (
        'Attempt 0 to send results for task TQ-000 has failed.'
        in [r.message for r in caplog.records]
    )


@pytest.mark.asyncio
async def test_sender_max_retries_exceeded(mocker, settings_payload, task_payload, caplog):
    mocker.patch('connect.eaas.runner.workers.events.RESULT_SENDER_MAX_RETRIES', 3)
    mocker.patch('connect.eaas.runner.workers.events.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 0.001)
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    with caplog.at_level(logging.WARNING):
        worker = EventsWorker(ext_handler)
        worker.get_extension_message = mocker.MagicMock(return_value={})
        worker.config.update_dynamic_config(SetupResponse(**settings_payload))
        worker.run = mocker.AsyncMock()
        worker.send = mocker.AsyncMock(
            side_effect=[Exception('retry') for _ in range(3)],
        )
        worker.ws = mocker.AsyncMock(closed=False)
        await worker.results_queue.put(
            Task(**task_payload(TaskCategory.BACKGROUND, 'test', 'TQ-000')),
        )
        assert worker.results_queue.empty() is False
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.01)
        worker.stop()
        await task

    assert (
        (
            'Max retries exceeded (3)'
            ' for sending results of task TQ-000'
        )
        in [r.message for r in caplog.records]
    )


@pytest.mark.parametrize(
    ('tries', 'ordinal'),
    (
        (14, 'th'),
        (21, 'st'),
        (22, 'nd'),
        (23, 'rd'),
    ),
)
def test_backoff_log(mocker, caplog, tries, ordinal):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    details = {'tries': tries, 'elapsed': 2.2, 'wait': 1.1}
    expected = (
        f'{tries}{ordinal} communication attempt failed, backing off waiting '
        f'{details["wait"]:.2f} seconds after next retry. Elapsed time: {details["elapsed"]:.2f}'
        ' seconds.'
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    w = EventsWorker(ext_handler)
    with caplog.at_level(logging.INFO):
        w._backoff_log(details)
    assert expected in caplog.records[0].message


@pytest.mark.asyncio
async def test_ensure_connection_maintenance(mocker, caplog):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_MAINTENANCE_SECONDS', 1)
    mocker.patch(
        'connect.eaas.runner.workers.base.websockets.connect',
        side_effect=InvalidStatusCode(502, None),
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    worker = EventsWorker(ext_handler)
    worker.run_event.set()
    worker.get_url = lambda: 'ws://test'

    with pytest.raises(MaintenanceError):
        with caplog.at_level(logging.INFO):
            await worker.ensure_connection()

    assert '1st communication attempt failed, backing off waiting' in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize('status', (400, 401, 403, 500, 501))
async def test_ensure_connection_other_statuses(mocker, caplog, status):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_MAINTENANCE_SECONDS', 1)
    mocker.patch(
        'connect.eaas.runner.workers.base.websockets.connect',
        side_effect=InvalidStatusCode(status, None),
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    worker = EventsWorker(ext_handler)
    worker.run_event.set()
    worker.get_url = lambda: 'ws://test'

    with pytest.raises(CommunicationError):
        with caplog.at_level(logging.INFO):
            await worker.ensure_connection()

    assert '1st communication attempt failed, backing off waiting' in caplog.text


@pytest.mark.asyncio
async def test_ensure_connection_generic_exception(mocker, caplog):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_MAINTENANCE_SECONDS', 1)
    mocker.patch(
        'connect.eaas.runner.workers.base.websockets.connect',
        side_effect=RuntimeError('generic error'),
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    worker = EventsWorker(ext_handler)
    worker.run_event.set()
    worker.get_url = lambda: 'ws://test'

    with pytest.raises(CommunicationError):
        with caplog.at_level(logging.INFO):
            await worker.ensure_connection()

    assert '1st communication attempt failed, backing off waiting' in caplog.text


@pytest.mark.asyncio
async def test_ensure_connection_exit_backoff(mocker, caplog):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 600)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch(
        'connect.eaas.runner.workers.base.websockets.connect',
        side_effect=RuntimeError('generic error'),
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    worker = EventsWorker(ext_handler)
    worker.run_event.set()
    worker.get_url = lambda: 'ws://test'

    with pytest.raises(StopBackoffError):
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.ensure_connection())
            worker.run_event.clear()
            await task

    assert 'EventsWorker exiting, stop backoff loop' in caplog.text


@pytest.mark.asyncio
async def test_ensure_connection_exit_max_attemps(mocker, caplog):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_TIME_GENERIC_SECONDS', 10)
    mocker.patch('connect.eaas.runner.workers.base.MAX_RETRY_DELAY_TIME_SECONDS', 1)
    mocker.patch(
        'connect.eaas.runner.workers.base.websockets.connect',
        side_effect=RuntimeError('generic error'),
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    worker = EventsWorker(ext_handler)
    worker.run_event.set()
    worker.get_url = lambda: 'ws://test'

    with caplog.at_level(logging.ERROR):
        task = asyncio.create_task(worker.run())
        await task

    assert 'max connection attemps reached, exit!' in caplog.text


@pytest.mark.asyncio
async def test_shutdown_pending_task_timeout(mocker, ws_server, unused_port, settings_payload):

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': .1,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch('connect.eaas.runner.workers.events.RESULT_SENDER_WAIT_GRACE_SECONDS', .1)

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=SetupResponse(**settings_payload),
        ).dict(),
        Message(message_type=MessageType.SHUTDOWN).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send'] + ['receive' for _ in range(100)],
    )

    task_result = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.BACKGROUND,
        },
        input={
            'event_type': EventType.ASSET_PURCHASE_REQUEST_PROCESSING,
            'object_id': 'PR-000',
        },
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        for _ in range(100):
            await worker.results_queue.put(task_result)
        asyncio.create_task(worker.start())
        await asyncio.sleep(.5)


@pytest.mark.asyncio
async def test_update_configuration(mocker, ws_server, unused_port, settings_payload):

    mocker.patch(
        'connect.eaas.runner.config.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
            'background_task_max_execution_time': 300,
            'interactive_task_max_execution_time': 120,
            'scheduled_task_max_execution_time': 43200,
        },
    )

    capabilities = {
        EventType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        EventType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
        return_value=MyExtension,
    )

    dyn_config = SetupResponse(**settings_payload)
    second_payload = copy.deepcopy(settings_payload)
    second_payload['variables'] = [{'name': 'conf2', 'value': 'val2', 'secure': False}]
    updated_config = SetupResponse(**second_payload)

    data_to_send = [
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=dyn_config,
        ).dict(),
        Message(
            version=2,
            message_type=MessageType.SETUP_RESPONSE,
            data=updated_config,
        ).dict(),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    async with ws_server(handler):
        worker = EventsWorker(ext_handler)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task
        assert worker.config.variables == {'conf2': 'val2'}


@pytest.mark.asyncio
async def test_handle_signal(mocker, settings_payload):
    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )

    config = ConfigHelper(secure=False)
    ext_handler = EventsApp(config)

    worker = EventsWorker(ext_handler)
    worker.config.update_dynamic_config(SetupResponse(**settings_payload))
    worker.run = mocker.AsyncMock()
    worker.send = mocker.AsyncMock()
    worker.result_sender = mocker.AsyncMock()
    worker.ws = mocker.AsyncMock(open=True)
    task = asyncio.create_task(worker.start())
    worker.handle_signal()
    await task
    worker.send.assert_awaited_once_with({'data': None, 'message_type': 'shutdown', 'version': 2})


def test_start_interactive_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch.object(EventsWorker, 'start', start_mock)
    mocked_configure_logger = mocker.patch('connect.eaas.runner.workers.events.configure_logger')

    start_interactive_worker_process(mocker.MagicMock(), True, False)

    mocked_configure_logger.assert_called_once_with(True, False)
    start_mock.assert_awaited_once()


def test_start_background_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocker.patch.object(
        EventsApp,
        'get_extension_class',
    )
    mocker.patch.object(EventsWorker, 'start', start_mock)
    mocked_configure_logger = mocker.patch('connect.eaas.runner.workers.events.configure_logger')

    start_background_worker_process(mocker.MagicMock(), True, False)

    mocked_configure_logger.assert_called_once_with(True, False)
    start_mock.assert_awaited_once()
