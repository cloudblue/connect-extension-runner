import asyncio
import logging

import pytest

from connect.client import ClientError
from connect.eaas.constants import (
    BACKGROUND_TASK_TYPES,
    INTERACTIVE_TASK_TYPES,
    RESULT_SENDER_MAX_RETRIES,
    TASK_TYPE_EXT_METHOD_MAP,
)
from connect.eaas.dataclasses import (
    Message,
    MessageType,
    ResultType,
    TaskCategory,
    TaskPayload,
    TaskType,
)
from connect.eaas.extension import ProcessingResponse, ValidationResponse
from connect.eaas.manager import TasksManager


@pytest.mark.asyncio
async def test_start_stop_is_running(mocker, caplog):
    manager = TasksManager(mocker.MagicMock())
    manager.start()
    assert manager.run_event.is_set() is True
    assert manager.is_running() is True
    with caplog.at_level(logging.DEBUG):
        await manager.stop()

    assert 'Worker exiting and no more running tasks: exit!' in caplog.text
    assert manager.run_event.is_set() is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    BACKGROUND_TASK_TYPES,
)
async def test_background_task_sync(mocker, extension_cls, task_type):
    extension_class = extension_cls(TASK_TYPE_EXT_METHOD_MAP[task_type])
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()
    worker.capabilities = {task_type: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        task_type,
        'PR-000',
    )

    await manager.submit_task(task)
    assert manager.running_tasks == 1

    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.SUCCESS
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    BACKGROUND_TASK_TYPES,
)
async def test_background_task_sync_reschedule(mocker, extension_cls, task_type):
    extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=ProcessingResponse.reschedule(126),
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()
    worker.capabilities = {task_type: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        task_type,
        'PR-000',
    )

    await manager.submit_task(task)
    assert manager.running_tasks == 1

    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.RESCHEDULE
    json_msg['data']['countdown'] = 126
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
async def test_background_task_sync_unsupported_status(mocker, extension_cls):
    extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[TaskType.ASSET_PURCHASE_REQUEST_PROCESSING],
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()
    worker.capabilities = {TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'approved'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
        'PR-000',
    )

    await manager.submit_task(task)
    assert manager.running_tasks == 1

    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.SKIP
    json_msg['data']['output'] = 'The status approved is not supported by the extension.'
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    BACKGROUND_TASK_TYPES,
)
async def test_background_task_async(mocker, extension_cls, task_type):
    extension_class = extension_cls(TASK_TYPE_EXT_METHOD_MAP[task_type], async_impl=True)
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()
    worker.capabilities = {task_type: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        task_type,
        'PR-000',
    )

    await manager.submit_task(task)
    assert manager.running_tasks == 1

    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.SUCCESS
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    INTERACTIVE_TASK_TYPES,
)
async def test_interactive_task_sync(mocker, extension_cls, task_type):
    extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=ValidationResponse.done({'test': 'data'}),
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()

    manager = TasksManager(worker)

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        task_type,
        'PR-000',
        data={'test': 'data'},
    )

    await manager.submit_task(task)
    assert manager.running_tasks == 1

    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.SUCCESS
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'task_type',
    INTERACTIVE_TASK_TYPES,
)
async def test_interactive_task_async(mocker, extension_cls, task_type):
    extension_class = extension_cls(
        TASK_TYPE_EXT_METHOD_MAP[task_type],
        result=ValidationResponse.done({'test': 'data'}),
        async_impl=True,
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()

    manager = TasksManager(worker)

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        task_type,
        'PR-000',
        data={'test': 'data'},
    )

    await manager.submit_task(task)
    assert manager.running_tasks == 1

    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.SUCCESS
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
async def test_background_task_request_error(mocker, extension_cls):
    extension_class = extension_cls('process_asset_purchase_request')
    extension = extension_class(None, mocker.MagicMock(), None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(side_effect=ClientError('Request not found', 404))

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
        'PR-000',
    )

    await manager.submit_task(task)
    await asyncio.sleep(.1)
    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = 'retry'
    json_msg['data']['output'] = 'Request not found'
    worker.send.assert_awaited_once_with(json_msg)


@pytest.mark.asyncio
async def test_result_sender_retries(mocker, extension_cls):
    extension_class = extension_cls(
        'process_asset_purchase_request',
        result=ProcessingResponse.done(),
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock(side_effect=[Exception('retry'), None])
    worker.capabilities = {TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
        'PR-000',
    )

    await manager.submit_task(task)
    await asyncio.sleep(.1)
    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.SUCCESS
    assert worker.send.mock_calls[1].args[0] == json_msg


@pytest.mark.asyncio
async def test_result_sender_max_retries_exceeded(mocker, extension_cls, caplog):
    extension_class = extension_cls(
        'process_asset_purchase_request',
        result=ProcessingResponse.done(),
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock(
        side_effect=[Exception('retry') for _ in range(RESULT_SENDER_MAX_RETRIES)],
    )
    worker.capabilities = {TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
        'PR-000',
    )
    with caplog.at_level(logging.WARNING):
        await manager.submit_task(task)
        await asyncio.sleep(.1)
        await manager.stop()

    assert 'max retries exceeded for sending results of task TQ-000' in caplog.text


@pytest.mark.asyncio
async def test_result_sender_wait_reconnection(mocker, extension_cls, caplog):
    extension_class = extension_cls(
        'process_asset_purchase_request',
        result=ProcessingResponse.done(),
    )
    extension = extension_class(None, None, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=True)
    worker.get_extension.return_value = extension
    worker.send = mocker.AsyncMock()
    worker.capabilities = {TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: ['pending']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'pending'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.BACKGROUND,
        TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
        'PR-000',
    )
    with caplog.at_level(logging.DEBUG):
        await manager.submit_task(task)
        await asyncio.sleep(.2)
        await manager.stop()

    assert 'wait WS reconnection before resuming result sender' in [
        record.message for record in caplog.records
    ]


@pytest.mark.asyncio
async def test_interactive_task_exception(mocker, extension_cls):
    extension_class = extension_cls(
        'validate_asset_purchase_request',
        exception=Exception('validation exception'),
    )
    logger_mock = mocker.MagicMock()
    extension = extension_class(None, logger_mock, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.capabilities = {TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: ['draft']}

    manager = TasksManager(worker)
    manager.get_request = mocker.AsyncMock(return_value={'id': 'PR-000', 'status': 'draft'})

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        TaskType.ASSET_PURCHASE_REQUEST_VALIDATION,
        'PR-000',
    )

    await manager.submit_task(task)
    await asyncio.sleep(.1)
    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.FAIL
    json_msg['data']['output'] = 'validation exception'
    assert worker.send.mock_calls[1].args[0] == json_msg


@pytest.mark.asyncio
async def test_interactive_task_exception_product_action(mocker, extension_cls):
    extension_class = extension_cls(
        'execute_product_action',
        exception=Exception('validation exception'),
    )
    logger_mock = mocker.MagicMock()
    extension = extension_class(None, logger_mock, None)

    worker = mocker.MagicMock()
    worker.ws = mocker.MagicMock(closed=False)
    worker.get_extension.return_value = extension
    worker.capabilities = {TaskType.PRODUCT_ACTION_EXECUTION: []}

    manager = TasksManager(worker)

    manager.start()

    task = TaskPayload(
        'TQ-000',
        TaskCategory.INTERACTIVE,
        TaskType.PRODUCT_ACTION_EXECUTION,
        'PR-000',
    )

    await manager.submit_task(task)
    await asyncio.sleep(.1)
    await manager.stop()
    message = Message(message_type=MessageType.TASK, data=task)
    json_msg = message.to_json()
    json_msg['data']['result'] = ResultType.FAIL
    json_msg['data']['data'] = {
        'http_status': 400,
        'headers': None,
        'body': 'validation exception',
    }
    json_msg['data']['output'] = 'validation exception'
    assert worker.send.mock_calls[1].args[0] == json_msg
