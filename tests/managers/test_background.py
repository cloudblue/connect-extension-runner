import asyncio
import time

import pytest

from connect.eaas.core.dataclasses import (
    EventType,
    Message,
    MessageType,
    ResultType,
    SettingsPayload,
    TaskCategory,
    TaskPayload,
)
from connect.eaas.core.extension import ProcessingResponse
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.constants import (
    ASSET_REQUEST_EVENT_TYPES,
    BACKGROUND_EVENT_TYPES,
    EVENT_TYPE_EXT_METHOD_MAP,
    LISTING_REQUEST_EVENT_TYPES,
    TIER_CONFIG_REQUEST_EVENT_TYPES,
)
from connect.eaas.runner.handler import ExtensionHandler
from connect.eaas.runner.managers import BackgroundTasksManager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    BACKGROUND_EVENT_TYPES,
)
async def test_sync(mocker, extension_cls, event_type, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['pending']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.runner.managers.background.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(EVENT_TYPE_EXT_METHOD_MAP[event_type])
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    task = TaskPayload(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.BACKGROUND,
            'runtime': 1.0,
        },
        input={
            'event_type': event_type,
            'object_id': 'PR-000',
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(version=2, message_type=MessageType.TASK, data=task)
    message.data.options.result = ResultType.SUCCESS
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    BACKGROUND_EVENT_TYPES,
)
async def test_async(mocker, extension_cls, event_type, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['pending']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.runner.managers.background.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(EVENT_TYPE_EXT_METHOD_MAP[event_type], async_impl=True)
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    task = TaskPayload(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.BACKGROUND,
            'runtime': 1.0,
        },
        input={
            'event_type': event_type,
            'object_id': 'PR-000',
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(version=2, message_type=MessageType.TASK, data=task)
    message.data.options.result = ResultType.SUCCESS
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    ASSET_REQUEST_EVENT_TYPES,
)
async def test_get_argument_subscription(
    mocker, httpx_mock, extension_cls, event_type,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['pending']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(EVENT_TYPE_EXT_METHOD_MAP[event_type], async_impl=True)
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    pr_data = {'id': 'PR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/requests?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0',
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/requests/PR-000',
        json=pr_data,
    )
    task = TaskPayload(
        **task_payload(TaskCategory.BACKGROUND, event_type, 'PR-000'),
    )
    assert await manager.get_argument(task) == pr_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    TIER_CONFIG_REQUEST_EVENT_TYPES,
)
async def test_get_argument_tcr(
    mocker, httpx_mock, extension_cls, event_type,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['pending']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(EVENT_TYPE_EXT_METHOD_MAP[event_type], async_impl=True)
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    tcr_data = {'id': 'TCR-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/tier/config-requests?'
            'and(eq(id,TCR-000),in(status,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/tier/config-requests/TCR-000',
        json=tcr_data,
    )
    task = TaskPayload(
        **task_payload(TaskCategory.BACKGROUND, event_type, 'TCR-000'),
    )
    assert await manager.get_argument(task) == tcr_data


@pytest.mark.asyncio
async def test_get_argument_tar(
    mocker, httpx_mock, extension_cls,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/tier/account-requests?'
            'and(eq(id,TAR-000),in(status,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    tar_data = {
        'id': 'TAR-000',
        'status': 'pending',
        'account': {'id': 'TA-000'},
        'product': {'id': 'PRD-000'},
    }

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/tier/account-requests/TAR-000',
        json=tar_data,
    )

    assets_filter = (
        'and(eq(product.id,PRD-000),eq(connection.type,preview),'
        'or(eq(tiers.tier2.id,TA-000),eq(tiers.tier1.id,TA-000),eq(tiers.customer.id,TA-000)))'
        '&limit=0&offset=0'
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/assets?{assets_filter}',
        headers={'Content-Range': 'items 0-1/1'},
    )

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING,
            'TAR-000',
        ),
    )
    assert await manager.get_argument(task) == tar_data


@pytest.mark.asyncio
async def test_get_argument_tar_no_assets(
    mocker, httpx_mock, extension_cls,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.send_skip_response = mocker.MagicMock()

    tar_data = {
        'id': 'TAR-000',
        'status': 'pending',
        'account': {'id': 'TA-000'},
        'product': {'id': 'PRD-000'},
    }

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/tier/account-requests?'
            'and(eq(id,TAR-000),in(status,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/tier/account-requests/TAR-000',
        json=tar_data,
    )

    assets_filter = (
        'and(eq(product.id,PRD-000),eq(connection.type,preview),'
        'or(eq(tiers.tier2.id,TA-000),eq(tiers.tier1.id,TA-000),eq(tiers.customer.id,TA-000)))'
        '&limit=0&offset=0'
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/assets?{assets_filter}',
        headers={'Content-Range': 'items 0-0/0'},
    )

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING,
            'TAR-000',
        ),
    )
    assert await manager.get_argument(task) is None
    manager.send_skip_response.assert_called_once_with(
        task,
        (
            'The Tier Account related to this request does not '
            'have assets with a preview connection.'
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    LISTING_REQUEST_EVENT_TYPES,
)
async def test_get_argument_listing_request(
    mocker, httpx_mock, extension_cls, event_type,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={event_type: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        event_type,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    lstr_data = {
        'id': 'LSTR-000',
        'state': 'pending',
        'listing': {'contract': {'marketplace': {'id': 'MP-0000'}}},
        'product': {'id': 'PRD-000'},
    }

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/listing-requests?'
            'and(eq(id,LSTR-000),in(state,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/listing-requests/LSTR-000',
        json=lstr_data,
    )

    marketplace_data = {
        'hubs': [
            {
                'hub': {'id': 'HB-0000'},
            },
        ],
    }

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/marketplaces/MP-0000',
        json=marketplace_data,
    )

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND,
            event_type,
            'LSTR-000',
        ),
    )
    assert await manager.get_argument(task) == lstr_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    LISTING_REQUEST_EVENT_TYPES,
)
async def test_get_argument_listing_request_vendor(
    mocker, httpx_mock, extension_cls, event_type,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    dyn_cfg = SettingsPayload(**settings_payload)
    dyn_cfg.service.hub_id = None
    config.update_dynamic_config(dyn_cfg)
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={event_type: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        event_type,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    lstr_data = {
        'id': 'LSTR-000',
        'status': 'pending',
        'listing': {'contract': {'marketplace': {'id': 'MP-0000'}}},
        'product': {'id': 'PRD-000'},
    }

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/listing-requests?'
            'and(eq(id,LSTR-000),in(state,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/listing-requests/LSTR-000',
        json=lstr_data,
    )

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND,
            event_type,
            'LSTR-000',
        ),
    )
    assert await manager.get_argument(task) == lstr_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    LISTING_REQUEST_EVENT_TYPES,
)
async def test_get_argument_listing_request_no_hub(
    mocker, httpx_mock, extension_cls, event_type,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={event_type: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        event_type,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.send_skip_response = mocker.MagicMock()

    lstr_data = {
        'id': 'LSTR-000',
        'status': 'pending',
        'listing': {'contract': {'marketplace': {'id': 'MP-0000'}}},
        'product': {'id': 'PRD-000'},
    }

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/listing-requests?'
            'and(eq(id,LSTR-000),in(state,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/listing-requests/LSTR-000',
        json=lstr_data,
    )

    marketplace_data = {
        'hubs': [
            {
                'hub': {'id': 'HB-0001'},
            },
        ],
    }

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/marketplaces/MP-0000',
        json=marketplace_data,
    )

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND,
            event_type,
            'LSTR-000',
        ),
    )
    assert await manager.get_argument(task) is None
    manager.send_skip_response.assert_called_once_with(
        task,
        (
            'The marketplace MP-0000 does not belong '
            f'to hub {config.hub_id}.'
        ),
    )


@pytest.mark.asyncio
async def test_get_argument_usage_file(
    mocker, httpx_mock, extension_cls,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={EventType.USAGE_FILE_REQUEST_PROCESSING: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[EventType.USAGE_FILE_REQUEST_PROCESSING],
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    uf_data = {'id': 'UF-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/usage/files?'
            'and(eq(id,UF-000),in(status,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/usage/files/UF-000',
        json=uf_data,
    )
    task = TaskPayload(
        **task_payload(TaskCategory.BACKGROUND, EventType.USAGE_FILE_REQUEST_PROCESSING, 'UF-000'),
    )
    assert await manager.get_argument(task) == uf_data


@pytest.mark.asyncio
async def test_get_argument_usage_chunks(
    mocker, httpx_mock, extension_cls,
    settings_payload, task_payload, unused_port,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={EventType.PART_USAGE_FILE_REQUEST_PROCESSING: ['pending']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[EventType.PART_USAGE_FILE_REQUEST_PROCESSING],
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)

    uf_data = {'id': 'UFC-000', 'status': 'pending'}

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/usage/chunks?'
            'and(eq(id,UFC-000),in(status,(pending)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/1'},
    )

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/usage/chunks/UFC-000',
        json=uf_data,
    )
    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'UFC-000',
        ),
    )
    assert await manager.get_argument(task) == uf_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('event_type', 'status_field', 'endpoint'),
    (
        (EventType.ASSET_PURCHASE_REQUEST_PROCESSING, 'status', '/requests'),
        (EventType.ASSET_CHANGE_REQUEST_PROCESSING, 'status', '/requests'),
        (EventType.ASSET_SUSPEND_REQUEST_PROCESSING, 'status', '/requests'),
        (EventType.ASSET_RESUME_REQUEST_PROCESSING, 'status', '/requests'),
        (EventType.ASSET_CANCEL_REQUEST_PROCESSING, 'status', '/requests'),
        (EventType.ASSET_ADJUSTMENT_REQUEST_PROCESSING, 'status', '/requests'),
        (EventType.TIER_CONFIG_SETUP_REQUEST_PROCESSING, 'status', '/tier/config-requests'),
        (EventType.TIER_CONFIG_CHANGE_REQUEST_PROCESSING, 'status', '/tier/config-requests'),
        (EventType.TIER_CONFIG_ADJUSTMENT_REQUEST_PROCESSING, 'status', '/tier/config-requests'),
        (EventType.LISTING_NEW_REQUEST_PROCESSING, 'state', '/listing-requests'),
        (EventType.LISTING_REMOVE_REQUEST_PROCESSING, 'state', '/listing-requests'),
        (EventType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING, 'status', '/tier/account-requests'),
        (EventType.USAGE_FILE_REQUEST_PROCESSING, 'status', '/usage/files'),
        (EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'status', '/usage/chunks'),
    ),
)
async def test_get_argument_unsupported_status(
    mocker, httpx_mock, extension_cls,
    settings_payload, task_payload, unused_port,
    event_type, status_field, endpoint,
):
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
    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SettingsPayload(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(
            return_value={event_type: ['supported']},
        ),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[event_type],
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.send_skip_response = mocker.MagicMock()

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}{endpoint}?'
            f'and(eq(id,OBJ-000),in({status_field},(supported)))&limit=0&offset=0'
        ),
        json=[],
        headers={'Content-Range': 'items 0-0/0'},
    )
    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, event_type, 'OBJ-000',
        ),
    )
    assert await manager.get_argument(task) is None
    manager.send_skip_response.assert_called_once_with(
        task,
        'The request status does not match the supported statuses: supported.',
    )


@pytest.mark.asyncio
async def test_build_response_done(task_payload):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'UFC-000',
        ),
    )
    future = asyncio.Future()
    result = ProcessingResponse.done()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.options.result == result.status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'result',
    (
        ProcessingResponse.fail(output='message'),
        ProcessingResponse.skip(output='message'),
    ),
)
async def test_build_response_fail_skip(task_payload, result):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'UFC-000',
        ),
    )
    future = asyncio.Future()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.options.result == result.status
    assert response.options.output == result.output


@pytest.mark.asyncio
async def test_build_response_reschedule(task_payload):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'UFC-000',
        ),
    )
    future = asyncio.Future()
    result = ProcessingResponse.reschedule(countdown=99)
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.options.result == result.status
    assert response.options.countdown == result.countdown


@pytest.mark.asyncio
async def test_build_response_exception(mocker, task_payload):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'UFC-000',
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.options.result == ResultType.RETRY
    assert 'Awesome error message' in response.options.output
    manager.log_exception.assert_called_once()


@pytest.mark.asyncio
async def test_send_skip_response(mocker, task_payload):
    config = ConfigHelper()
    mocked_put = mocker.AsyncMock()
    mocked_time = mocker.patch('connect.eaas.runner.managers.background.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    manager = BackgroundTasksManager(config, None, mocked_put)

    task = TaskPayload(
        **task_payload(
            TaskCategory.BACKGROUND, EventType.PART_USAGE_FILE_REQUEST_PROCESSING, 'UFC-000', 1.0,
        ),
    )

    manager.send_skip_response(task, 'test output')
    await asyncio.sleep(.01)
    task.options.result = ResultType.SKIP
    task.options.output = 'test output'

    mocked_put.assert_awaited_once_with(task)
