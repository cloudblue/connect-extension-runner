import asyncio

import pytest

from connect.eaas.config import ConfigHelper
from connect.eaas.constants import (
    INTERACTIVE_TASK_TYPES,
    OTHER_INTERACTIVE_TASK_TYPES,
    TASK_TYPE_EXT_METHOD_MAP,
    VALIDATION_TASK_TYPES,
)
from connect.eaas.dataclasses import (
    ConfigurationPayload,
    Message,
    MessageType,
    ResultType,
    TaskCategory,
    TaskPayload,
    TaskType,
)
from connect.eaas.extension import (
    CustomEventResponse,
    ProductActionResponse,
    ValidationResponse,
)
from connect.eaas.handler import ExtensionHandler
from connect.eaas.managers import InteractiveTasksManager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    VALIDATION_TASK_TYPES,
)
async def test_validation_sync(mocker, extension_cls, task_type, config_payload):

    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={task_type: ['draft']}),
    )
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    task_response_data = {'task': 'data', 'valid': True}

    handler.extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=ValidationResponse.done(task_response_data),
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        task_type,
        'ID-000',
    )

    task.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(message_type=MessageType.TASK, data=task)
    message.data.result = ResultType.SUCCESS
    message.data.data = task_response_data
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    VALIDATION_TASK_TYPES,
)
async def test_validation_async(mocker, extension_cls, task_type, config_payload):

    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={task_type: ['draft']}),
    )
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    task_response_data = {'task': 'data', 'valid': True}

    handler.extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=ValidationResponse.done(task_response_data),
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        task_type,
        'ID-000',
    )

    task.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(message_type=MessageType.TASK, data=task)
    message.data.result = ResultType.SUCCESS
    message.data.data = task_response_data
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('task_type', 'result'),
    (
        (
            TaskType.PRODUCT_ACTION_EXECUTION,
            ProductActionResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            CustomEventResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
    ),
)
async def test_others_sync(mocker, extension_cls, task_type, result, config_payload):

    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={task_type: ['draft']}),
    )
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    handler.extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=result,
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        task_type,
        'ID-000',
    )

    task.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(message_type=MessageType.TASK, data=task)
    message.data.result = ResultType.SUCCESS
    message.data.data = {
        'http_status': 200,
        'headers': {'X-Test': 'value'},
        'body': {'response': 'data'},
    }
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('task_type', 'result'),
    (
        (
            TaskType.PRODUCT_ACTION_EXECUTION,
            ProductActionResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            CustomEventResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
    ),
)
async def test_others_async(mocker, extension_cls, task_type, result, config_payload):

    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))
    mocker.patch.object(
        ExtensionHandler,
        'capabilities',
        new_callable=mocker.PropertyMock(return_value={task_type: ['draft']}),
    )
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    handler.extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=result,
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = InteractiveTasksManager(config, handler, result_queue)

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        task_type,
        'ID-000',
    )

    task.data = {'task': 'data'}

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(message_type=MessageType.TASK, data=task)
    message.data.result = ResultType.SUCCESS
    message.data.data = {
        'http_status': 200,
        'headers': {'X-Test': 'value'},
        'body': {'response': 'data'},
    }
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
@pytest.mark.parametrize('task_type', INTERACTIVE_TASK_TYPES)
async def test_get_argument(task_payload, task_type):
    task = TaskPayload(
        **task_payload(
            TaskCategory.INTERACTIVE,
            task_type,
            'PR-000',
        ),
    )
    task.data = {'some': 'data'}

    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)

    assert await manager.get_argument(task) == task.data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('task_type', 'result'),
    (
        (
            TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            CustomEventResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            TaskType.PRODUCT_ACTION_EXECUTION,
            ProductActionResponse.done(headers={'X-Test': 'value'}, body={'response': 'data'}),
        ),
        (
            TaskType.ASSET_PURCHASE_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
        (
            TaskType.ASSET_CHANGE_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
        (
            TaskType.TIER_CONFIG_SETUP_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
        (
            TaskType.TIER_CONFIG_CHANGE_REQUEST_VALIDATION,
            ValidationResponse.done({'response': 'data'}),
        ),
    ),
)
async def test_build_response_done(task_payload, task_type, result):
    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)
    task = TaskPayload(
        **task_payload(
            TaskCategory.INTERACTIVE, task_type, 'ID-000',
        ),
    )
    future = asyncio.Future()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.task_id == task.task_id
    assert response.result == result.status
    assert response.data == result.data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    VALIDATION_TASK_TYPES,
)
async def test_build_response_exception_validation(mocker, task_type, task_payload):
    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = TaskPayload(
        **task_payload(
            TaskCategory.INTERACTIVE, task_type, 'ID-000',
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.task_id == task.task_id
    assert response.result == ResultType.FAIL
    assert 'Awesome error message' in response.output
    manager.log_exception.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    OTHER_INTERACTIVE_TASK_TYPES,
)
async def test_build_response_exception_others(mocker, task_type, task_payload):
    config = ConfigHelper()
    manager = InteractiveTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = TaskPayload(
        **task_payload(
            TaskCategory.INTERACTIVE, task_type, 'ID-000',
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.task_id == task.task_id
    assert response.result == ResultType.FAIL
    assert 'Awesome error message' in response.output
    assert response.data['http_status'] == 400
    assert response.data['headers'] is None
    assert response.data['body'] == response.output
    manager.log_exception.assert_called_once()
