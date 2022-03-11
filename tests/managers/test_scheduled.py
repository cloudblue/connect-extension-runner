import asyncio
import time

import pytest

from connect.eaas.config import ConfigHelper
from connect.eaas.dataclasses import (
    ConfigurationPayload,
    Message,
    MessageType,
    ResultType,
    TaskCategory,
    TaskPayload,
    TaskType,
)
from connect.eaas.extension import ScheduledExecutionResponse
from connect.eaas.handler import ExtensionHandler
from connect.eaas.managers import ScheduledTasksManager


@pytest.mark.asyncio
async def test_sync(mocker, extension_cls, config_payload):

    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.managers.scheduled.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        'my_sync_schedulable_method',
        result=ScheduledExecutionResponse.done(),
    )
    handler.extension_type = 'sync'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = ScheduledTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(
        return_value={'id': 'EFS-000', 'method': 'my_sync_schedulable_method'},
    )

    task = TaskPayload(
        'TQ-000',
        TaskCategory.SCHEDULED,
        TaskType.SCHEDULED_EXECUTION,
        'EFS-000',
        runtime=1.0,
    )

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(message_type=MessageType.TASK, data=task)
    message.data.result = ResultType.SUCCESS
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
async def test_async(mocker, extension_cls, config_payload):

    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocked_time = mocker.patch('connect.eaas.managers.scheduled.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls(
        'my_async_schedulable_method',
        result=ScheduledExecutionResponse.done(),
        async_impl=True,
    )
    handler.extension_type = 'async'

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = ScheduledTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(
        return_value={'id': 'EFS-000', 'method': 'my_async_schedulable_method'},
    )

    task = TaskPayload(
        'TQ-000',
        TaskCategory.SCHEDULED,
        TaskType.SCHEDULED_EXECUTION,
        'EFS-000',
        runtime=1.0,
    )

    await manager.submit(task)
    await asyncio.sleep(.01)
    message = Message(message_type=MessageType.TASK, data=task)
    message.data.result = ResultType.SUCCESS
    result_queue.assert_awaited_once_with(message.data)


@pytest.mark.asyncio
async def test_get_argument(
    mocker, httpx_mock, extension_cls,
    config_payload, task_payload, unused_port,
):

    api_url = f'https://127.0.0.1:{unused_port}/public/v1'
    mocker.patch.object(ConfigHelper, 'get_api_url', return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(ConfigurationPayload(**config_payload))

    manager = ScheduledTasksManager(config, None, None)

    schedule_data = {'id': 'EFS-000', 'method': 'my_schedulable_method'}

    httpx_mock.add_response(
        method='GET',
        url=(
            f'{api_url}/devops/services/{config.service_id}'
            f'/environments/{config.environment_id}'
            f'/schedules/EFS-000'
        ),
        json=schedule_data,
    )
    task = TaskPayload(
        **task_payload(
            TaskCategory.SCHEDULED,
            TaskType.SCHEDULED_EXECUTION,
            'EFS-000',
        ),
    )
    assert await manager.get_argument(task) == schedule_data


@pytest.mark.asyncio
async def test_build_response_done(task_payload):
    config = ConfigHelper()
    manager = ScheduledTasksManager(config, None, None)
    task = TaskPayload(
        **task_payload(
            TaskCategory.SCHEDULED, TaskType.SCHEDULED_EXECUTION, 'EFS-000',
        ),
    )
    result = ScheduledExecutionResponse.done()
    future = asyncio.Future()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.task_id == task.task_id
    assert response.result == result.status


@pytest.mark.asyncio
async def test_build_response_exception(mocker, task_payload):
    config = ConfigHelper()
    manager = ScheduledTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = TaskPayload(
        **task_payload(
            TaskCategory.SCHEDULED, TaskType.SCHEDULED_EXECUTION, 'EFS-000',
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.task_id == task.task_id
    assert response.result == ResultType.RETRY
    assert 'Awesome error message' in response.output
    manager.log_exception.assert_called_once()
