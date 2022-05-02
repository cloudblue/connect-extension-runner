import asyncio
import time

import pytest

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.constants import (
    EVENT_TYPE_EXT_METHOD_MAP,
    INTERACTIVE_EVENT_TYPES,
    OTHER_INTERACTIVE_EVENT_TYPES,
    VALIDATION_EVENT_TYPES,
)
from connect.eaas.core.enums import (
    EventType,
    ResultType,
    TaskCategory,
)
from connect.eaas.core.extension import (
    CustomEventResponse,
    ProductActionResponse,
    ValidationResponse,
)
from connect.eaas.core.proto import (
    SetupResponse,
    Task,
    TaskOutput,
)
from connect.eaas.runner.handler import ExtensionHandler
from connect.eaas.runner.managers import InteractiveTasksManager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    VALIDATION_EVENT_TYPES,
)
async def test_validation_sync(mocker, extension_cls, event_type, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['draft']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.runner.managers.interactive.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)

    task_response_data = {'task': 'data', 'valid': True}

    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[event_type],
        result=ValidationResponse.done(task_response_data),
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.INTERACTIVE,
        },
        input={
            'event_type': event_type,
            'object_id': 'ID-000',
        },
    )
    task.input.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    task.output = TaskOutput(
        result=ResultType.SUCCESS, runtime=1.0, data={'task': 'data', 'valid': True},
    )
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    VALIDATION_EVENT_TYPES,
)
async def test_validation_async(mocker, extension_cls, event_type, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['draft']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.runner.managers.interactive.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)

    task_response_data = {'task': 'data', 'valid': True}

    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[event_type],
        result=ValidationResponse.done(task_response_data),
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.INTERACTIVE,
        },
        input={
            'event_type': event_type,
            'object_id': 'ID-000',
        },
    )
    task.input.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    task.output = TaskOutput(
        result=ResultType.SUCCESS, runtime=1.0, data={'task': 'data', 'valid': True},
    )
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('event_type', 'result'),
    (
        (
            EventType.PRODUCT_ACTION_EXECUTION,
            ProductActionResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            EventType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            CustomEventResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
    ),
)
async def test_others_sync(mocker, extension_cls, event_type, result, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['draft']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.runner.managers.interactive.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)

    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[event_type],
        result=result,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.INTERACTIVE,
        },
        input={
            'event_type': event_type,
            'object_id': 'ID-000',
        },
    )
    task.input.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    task.output = TaskOutput(
        result=ResultType.SUCCESS,
        runtime=1.0,
        data={
            'http_status': 200,
            'headers': {'X-Test': 'value'},
            'body': {'response': 'data'},
        },
    )
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('event_type', 'result'),
    (
        (
            EventType.PRODUCT_ACTION_EXECUTION,
            ProductActionResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            EventType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            CustomEventResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
    ),
)
async def test_others_async(mocker, extension_cls, event_type, result, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={event_type: ['draft']}),
    )
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.runner.managers.interactive.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)

    handler.extension_class = extension_cls(
        EVENT_TYPE_EXT_METHOD_MAP[event_type],
        result=result,
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.INTERACTIVE,
        },
        input={
            'event_type': event_type,
            'object_id': 'ID-000',
        },
    )
    task.input.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    task.output = TaskOutput(
        result=ResultType.SUCCESS,
        runtime=1.0,
        data={
            'http_status': 200,
            'headers': {'X-Test': 'value'},
            'body': {'response': 'data'},
        },
    )
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
@pytest.mark.parametrize('event_type', INTERACTIVE_EVENT_TYPES)
async def test_get_argument(task_payload, event_type):
    task = Task(**task_payload(TaskCategory.INTERACTIVE, event_type, 'PR-000'))
    task.input.data = {'some': 'data'}

    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)

    assert await manager.get_argument(task) == task.input.data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('event_type', 'result'),
    (
        (
            EventType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            CustomEventResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            EventType.PRODUCT_ACTION_EXECUTION,
            ProductActionResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            EventType.ASSET_PURCHASE_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
        (
            EventType.ASSET_CHANGE_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
        (
            EventType.TIER_CONFIG_SETUP_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
        (
            EventType.TIER_CONFIG_CHANGE_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
    ),
)
async def test_build_response_done(task_payload, event_type, result):
    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)
    task = Task(**task_payload(TaskCategory.INTERACTIVE, event_type, 'ID-000'))
    future = asyncio.Future()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == result.status
    assert response.output.data == result.data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    VALIDATION_EVENT_TYPES,
)
async def test_build_response_exception_validation(mocker, event_type, task_payload):
    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = Task(**task_payload(TaskCategory.INTERACTIVE, event_type, 'ID-000'))
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == ResultType.FAIL
    assert 'Awesome error message' in response.output.message
    manager.log_exception.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_type',
    OTHER_INTERACTIVE_EVENT_TYPES,
)
async def test_build_response_exception_others(mocker, event_type, task_payload):
    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = Task(**task_payload(TaskCategory.INTERACTIVE, event_type, 'ID-000'))
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == ResultType.FAIL
    assert 'Awesome error message' in response.output.message
    assert response.output.data['http_status'] == 400
    assert response.output.data['headers'] is None
    assert response.output.data['body'] == response.output.message
    manager.log_exception.assert_called_once()
