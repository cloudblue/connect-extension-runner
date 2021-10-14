import asyncio
import dataclasses
import logging

import pytest
from logzio.sender import LogzioSender
from websockets.exceptions import ConnectionClosedError, WebSocketException

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
async def test_capabilities_configuration(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
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


@pytest.mark.asyncio
async def test_pr_task(mocker, ws_server, unused_port, httpx_mock, caplog):

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/requests/PR-000',
        json=pr_data,
    )

    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
        },
    )

    capabilities = {
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending', 'inquiring'],
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft'],
    }

    mocked_send_logs = mocker.patch.object(
        LogzioSender,
        'append',
    )

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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            {'var': 'val'}, 'api_key', 'development', 'account_id', 'account_name',
        ))),
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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.5)
        worker.run_event.clear()
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

    log_msg = mocked_send_logs.mock_calls[0].args[0]
    assert log_msg['message'] == 'test log message'
    assert log_msg['account_id'] == 'account_id'
    assert log_msg['account_name'] == 'account_name'
    assert log_msg['environment_id'] == 'ENV-000-0001'
    assert log_msg['environment_type'] == 'development'
    assert log_msg['instance_id'] == 'INS-000-0002'
    assert log_msg['task_id'] == 'TQ-000'


@pytest.mark.asyncio
async def test_tcr_task(mocker, ws_server, unused_port, httpx_mock):

    tcr_data = {'id': 'TCR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'https://127.0.0.1:{unused_port}/public/v1/tier/config-requests/TCR-000',
        json=tcr_data,
    )

    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            {'var': 'val'}, 'api_key', 'development',
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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.5)
        worker.run_event.clear()
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
async def test_scheduled_task(mocker, ws_server, unused_port, httpx_mock):

    schedule_data = {
        'id': 'EFS-000',
        'method': 'run_scheduled_task',
        'parameter': {'param': 'data'},
    }

    schedule_url = f'https://127.0.0.1:{unused_port}/public/v1/devops'
    schedule_url = f'{schedule_url}/services/SVC-0000/environments/ENV-000-0001'
    schedule_url = f'{schedule_url}/schedules/{schedule_data["id"]}'

    httpx_mock.add_response(
        method='GET',
        url=schedule_url,
        json=schedule_data,
    )

    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            {'var': 'val'}, 'api_key', 'development', service_id='SVC-0000',
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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.5)
        worker.run_event.clear()
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
async def test_pause(mocker, ws_server, unused_port):

    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            {'var': 'val'}, 'api_key', 'development',
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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.5)
        assert worker.paused is True
        worker.run_event.clear()
        await task


@pytest.mark.asyncio
async def test_resume(mocker, ws_server, unused_port):

    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            {'var': 'val'}, 'api_key', 'development',
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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.5)
        assert worker.paused is False
        worker.run_event.clear()
        await task


@pytest.mark.asyncio
async def test_shutdown(mocker, ws_server, unused_port):

    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    data_to_send = [
        dataclasses.asdict(Message(MessageType.CONFIGURATION, ConfigurationPayload(
            {'var': 'val'}, 'api_key', 'development',
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
        asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.5)
        assert worker.run_event.is_set() is False


@pytest.mark.asyncio
async def test_connection_closed_error(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.worker.get_extension_class')
    mocker.patch('connect.eaas.worker.get_extension_type')
    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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
            task = asyncio.create_task(worker.run())
            worker.run_event.set()
            await asyncio.sleep(.1)
            worker.run_event.clear()
            await task

    assert (
        f'Disconnected from: ws://127.0.0.1:{unused_port}'
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002'
        '?running_tasks=0&running_scheduled_tasks=0, retry in 2s'
    ) in caplog.text


@pytest.mark.asyncio
async def test_connection_websocket_exception(mocker, ws_server, unused_port, caplog):
    mocker.patch('connect.eaas.worker.get_extension_class')
    mocker.patch('connect.eaas.worker.get_extension_type')
    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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
            task = asyncio.create_task(worker.run())
            worker.run_event.set()
            await asyncio.sleep(.1)
            worker.run_event.clear()
            await task

    assert 'Unexpected websocket exception' in caplog.text


@pytest.mark.asyncio
async def test_start_stop(mocker, ws_server, unused_port, caplog):
    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

    handler = WSHandler(
        '/public/v1/devops/ws/ENV-000-0001/INS-000-0002?running_tasks=0&running_scheduled_tasks=0',
        None,
        ['receive', 'send'],
    )

    async with ws_server(handler):
        worker = Worker(secure=False)
        with caplog.at_level(logging.INFO):
            await worker.start()
            await asyncio.sleep(.1)
            assert 'Control worker started' in caplog.text
            worker.stop()
            assert 'Control worker stopped' in caplog.text


@pytest.mark.asyncio
async def test_capabilities_configuration_with_vars(mocker, ws_server, unused_port):
    mocker.patch(
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
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
        'connect.eaas.worker.get_environment',
        return_value={
            'ws_address': f'127.0.0.1:{unused_port}',
            'api_address': f'127.0.0.1:{unused_port}',
            'api_key': 'SU-000:XXXX',
            'environment_id': 'ENV-000-0001',
            'instance_id': 'INS-000-0002',
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

    mocker.patch('connect.eaas.worker.get_extension_class', return_value=MyExtension)
    mocker.patch('connect.eaas.worker.get_extension_type', return_value='sync')

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
        task = asyncio.create_task(worker.run())
        worker.run_event.set()
        await asyncio.sleep(.1)
        worker.run_event.clear()
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
