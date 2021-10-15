import asyncio
import dataclasses
import logging

import pytest
from websockets.exceptions import ConnectionClosedError, InvalidStatusCode, WebSocketException

from connect.eaas.constants import RESULT_SENDER_MAX_RETRIES
from connect.eaas.dataclasses import (
    CapabilitiesPayload,
    ConfigurationPayload,
    Message,
    MessageType,
    ResultType,
    TaskCategory,
    TaskPayload,
    TaskType,
)
from connect.eaas.extension import Extension, ProcessingResponse, ScheduledExecutionResponse
from connect.eaas.worker import Worker

from tests.utils import WSHandler


@pytest.mark.asyncio
async def test_capabilities_configuration(mocker, ws_server, unused_port, config_payload):
    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = dataclasses.asdict(
        Message(
            MessageType.CONFIGURATION,
            ConfigurationPayload(**config_payload),
        ),
    )

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )
    worker = None
    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        dataclasses.asdict(
            Message(
                MessageType.CAPABILITIES,
                CapabilitiesPayload(
                    capabilities,
                    [],
                    [],
                    'https://example.com/README.md',
                    'https://example.com/CHANGELOG.md',
                ),
            ),
        ),
    )

    assert worker.config.variables == config_payload['configuration']
    assert worker.config.logging_api_key == config_payload['logging_api_key']
    assert worker.config.environment_type == config_payload['environment_type']
    assert worker.config.account_id == config_payload['account_id']
    assert worker.config.account_name == config_payload['account_name']
    assert worker.config.service_id == config_payload['service_id']
    assert worker.config.product_id == config_payload['product_id']
    assert worker.config.hub_id == config_payload['hub_id']


@pytest.mark.asyncio
async def test_pr_task(mocker, ws_server, unused_port, httpx_mock, config_payload):

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
        json=pr_data,
    )

    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    dyn_config = ConfigurationPayload(**config_payload)

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, dyn_config)),
        dataclasses.asdict(Message(MessageType.TASK, TaskPayload(
            'TQ-000',
            TaskCategory.BACKGROUND,
            TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
            'PR-000',
        ))),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )
    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        dataclasses.asdict(
            Message(
                MessageType.CAPABILITIES,
                CapabilitiesPayload(
                    capabilities,
                    [],
                    [],
                    'https://example.com/README.md',
                    'https://example.com/CHANGELOG.md',
                ),
            ),
        ),
    )
    handler.assert_received(
        dataclasses.asdict(
            Message(MessageType.TASK, TaskPayload(
                'TQ-000',
                TaskCategory.BACKGROUND,
                TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
                'PR-000',
                result=ResultType.SUCCESS,
            )),
        ),
    )


@pytest.mark.asyncio
async def test_tcr_task(mocker, ws_server, unused_port, httpx_mock, config_payload):

    tcr_data = {'id': 'TCR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/tier/config-requests/TCR-000',
        json=tcr_data,
    )

    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING: ['pending'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            **config_payload,
        ))),
        dataclasses.asdict(
            Message(MessageType.TASK, TaskPayload(
                'TQ-000',
                TaskCategory.BACKGROUND,
                TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING,
                'TCR-000',
            )),
        ),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        dataclasses.asdict(
            Message(
                MessageType.CAPABILITIES,
                CapabilitiesPayload(
                    capabilities,
                    [],
                    [],
                    'https://example.com/README.md',
                    'https://example.com/CHANGELOG.md',
                ),
            ),
        ),
    )
    handler.assert_received(
        dataclasses.asdict(
            Message(MessageType.TASK, TaskPayload(
                'TQ-000',
                TaskCategory.BACKGROUND,
                TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING,
                'TCR-000',
                result=ResultType.SUCCESS,
            )),
        ),
    )


@pytest.mark.asyncio
async def test_scheduled_task(mocker, ws_server, unused_port, httpx_mock, config_payload):

    schedule_data = {
        'id': 'EFS-000',
        'method': 'run_scheduled_task',
        'parameter': {'param': 'data'},
    }

    schedule_url = f'https://127.0.0.1:{unused_port}/public/v1/devops'
    service_id = config_payload['service_id']
    schedule_url = f'{schedule_url}/services/{service_id}/environments/ENV-000-0001'
    schedule_url = f'{schedule_url}/schedules/{schedule_data["id"]}'

    httpx_mock.add_response(
        method='GET',
        url=schedule_url,
        json=schedule_data,
    )

    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING: ['pending'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            **config_payload,
        ))),
        dataclasses.asdict(
            Message(MessageType.TASK, TaskPayload(
                'TQ-000',
                TaskCategory.SCHEDULED,
                TaskType.SCHEDULED_EXECUTION,
                schedule_data['id'],
            )),
        ),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'receive'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        dataclasses.asdict(
            Message(
                MessageType.CAPABILITIES,
                CapabilitiesPayload(
                    capabilities,
                    [],
                    [
                        {
                            'method': 'run_scheduled_task',
                            'name': 'Run scheduled task',
                            'description': 'Description',
                        },
                    ],
                    'https://example.com/README.md',
                    'https://example.com/CHANGELOG.md',
                ),
            ),
        ),
    )
    handler.assert_received(
        dataclasses.asdict(
            Message(MessageType.TASK, TaskPayload(
                'TQ-000',
                TaskCategory.SCHEDULED,
                TaskType.SCHEDULED_EXECUTION,
                schedule_data['id'],
                result=ResultType.SUCCESS,
            )),
        ),
    )


@pytest.mark.asyncio
async def test_pause(mocker, ws_server, unused_port, config_payload):

    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            **config_payload,
        ))),
        dataclasses.asdict(Message(MessageType.PAUSE)),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        assert worker.paused is True
        worker.stop()
        await task


@pytest.mark.asyncio
async def test_resume(mocker, ws_server, unused_port, config_payload):

    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            **config_payload,
        ))),
        dataclasses.asdict(Message(MessageType.PAUSE)),
        dataclasses.asdict(Message(MessageType.RESUME)),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        worker.paused = True
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        assert worker.paused is False
        worker.stop()
        await task


@pytest.mark.asyncio
async def test_shutdown(mocker, ws_server, unused_port, config_payload):

    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            **config_payload,
        ))),
        dataclasses.asdict(Message(MessageType.SHUTDOWN)),
    ]

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        assert worker.run_event.is_set() is False


@pytest.mark.asyncio
async def test_connection_closed_error(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocker.patch(
        'connect.eaas.config.get_environment',
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

    async with ws_server(handler):
        worker = Worker(secure=False)
        worker.send = mocker.AsyncMock(side_effect=ConnectionClosedError(1006, 'disconnected'))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert (
        f'Disconnected from: ws://127.0.0.1:{unused_port}'
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002'
        '?running_tasks=0&running_scheduled_tasks=0, retry in 2s'
    ) in caplog.text


@pytest.mark.asyncio
async def test_connection_websocket_exception(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocker.patch(
        'connect.eaas.config.get_environment',
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

    async with ws_server(handler):
        worker = Worker(secure=False)
        worker.send = mocker.AsyncMock(side_effect=WebSocketException())
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert 'Unexpected websocket exception' in caplog.text


@pytest.mark.asyncio
async def test_connection_maintenance(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocker.patch(
        'connect.eaas.config.get_environment',
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

    async with ws_server(handler):
        worker = Worker(secure=False)
        worker.send = mocker.AsyncMock(side_effect=InvalidStatusCode(502, None))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert 'Maintenance in progress, try to reconnect in 2s' in caplog.text


@pytest.mark.asyncio
async def test_connection_internal_server_error(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocker.patch(
        'connect.eaas.config.get_environment',
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

    async with ws_server(handler):
        worker = Worker(secure=False)
        worker.send = mocker.AsyncMock(side_effect=InvalidStatusCode(500, None))
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            worker.stop()
            await task

    assert 'Received an unexpected status from server: 500' in caplog.text


@pytest.mark.asyncio
async def test_start_stop(mocker, ws_server, unused_port, caplog):
    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        ['receive', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(worker.start())
            await asyncio.sleep(.5)
            assert 'Control worker started' in caplog.text
            worker.stop()
            await task
            assert 'Control worker stopped' in caplog.text


@pytest.mark.asyncio
async def test_capabilities_configuration_with_vars(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = dataclasses.asdict(
        Message(
            MessageType.CONFIGURATION,
            ConfigurationPayload(
                {
                    'var1': 'value1',
                    'var2': 'value2',
                },
                'token',
                'development',
            ),
        ),
    )

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        dataclasses.asdict(
            Message(
                MessageType.CAPABILITIES,
                CapabilitiesPayload(
                    capabilities,
                    variables,
                    [],
                    'https://example.com/README.md',
                    'https://example.com/CHANGELOG.md',
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_capabilities_configuration_without_vars(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.config.get_environment',
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
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    class MyExtension(Extension):
        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': capabilities,
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.handler.get_extension_type', return_value='sync')

    data_to_send = dataclasses.asdict(
        Message(
            MessageType.CONFIGURATION,
            ConfigurationPayload(
                {
                    'var1': 'value1',
                    'var2': 'value2',
                },
                'token',
                'development',
            ),
        ),
    )

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        data_to_send,
        ['receive', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.5)
        worker.stop()
        await task

    handler.assert_received(
        dataclasses.asdict(
            Message(
                MessageType.CAPABILITIES,
                CapabilitiesPayload(
                    capabilities,
                    None,
                    None,
                    'https://example.com/README.md',
                    'https://example.com/CHANGELOG.md',
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_sender_retries(mocker, config_payload, task_payload, caplog):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')

    with caplog.at_level(logging.WARNING):
        worker = Worker(secure=True)
        worker.config.update_dynamic_config(ConfigurationPayload(**config_payload))
        worker.run = mocker.AsyncMock()
        worker.send = mocker.AsyncMock(side_effect=[Exception('retry'), None])
        worker.ws = mocker.MagicMock(closed=False)
        await worker.results_queue.put(
            TaskPayload(**task_payload(TaskCategory.BACKGROUND, 'test', 'TQ-000')),
        )
        assert worker.results_queue.empty() is False
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.01)
        worker.stop()
        await task

    assert (
        'Attemp 0 to send results for task TQ-000 has failed.'
        in [r.message for r in caplog.records]
    )


@pytest.mark.asyncio
async def test_sender_max_retries_exceeded(mocker, config_payload, task_payload, caplog):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')

    with caplog.at_level(logging.WARNING):
        worker = Worker(secure=True)
        worker.config.update_dynamic_config(ConfigurationPayload(**config_payload))
        worker.run = mocker.AsyncMock()
        worker.send = mocker.AsyncMock(
            side_effect=[Exception('retry') for _ in range(RESULT_SENDER_MAX_RETRIES)],
        )
        worker.ws = mocker.MagicMock(closed=False)
        await worker.results_queue.put(
            TaskPayload(**task_payload(TaskCategory.BACKGROUND, 'test', 'TQ-000')),
        )
        assert worker.results_queue.empty() is False
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(.01)
        worker.stop()
        await task

    assert (
        (
            f'Max retries exceeded ({RESULT_SENDER_MAX_RETRIES})'
            ' for sending results of task TQ-000'
        )
        in [r.message for r in caplog.records]
    )


@pytest.mark.asyncio
async def test_sender_paused(mocker, config_payload, task_payload):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')

    worker = Worker(secure=True)
    worker.config.update_dynamic_config(ConfigurationPayload(**config_payload))
    worker.run = mocker.AsyncMock()
    worker.send = mocker.AsyncMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.paused = True
    await worker.results_queue.put(
        TaskPayload(**task_payload(TaskCategory.BACKGROUND, 'test', 'TQ-000')),
    )
    assert worker.results_queue.empty() is False
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(.1)
    worker.stop()
    await task
    worker.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_sender_ws_closed(mocker, config_payload, task_payload):
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')

    worker = Worker(secure=True)
    worker.config.update_dynamic_config(ConfigurationPayload(**config_payload))
    worker.run = mocker.AsyncMock()
    worker.send = mocker.AsyncMock()
    worker.ws = mocker.MagicMock(closed=True)
    await worker.results_queue.put(
        TaskPayload(**task_payload(TaskCategory.BACKGROUND, 'test', 'TQ-000')),
    )
    assert worker.results_queue.empty() is False
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(.1)
    worker.stop()
    await task
    worker.send.assert_not_awaited()
