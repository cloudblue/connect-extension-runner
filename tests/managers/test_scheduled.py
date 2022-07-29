import asyncio
import time

import pytest

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.core.enums import (
    EventType,
    ResultType,
    TaskCategory,
)
from connect.eaas.core.proto import (
    SetupResponse,
    Task,
    TaskOutput,
)
from connect.eaas.core.responses import ScheduledExecutionResponse
from connect.eaas.runner.handlers.events import EventsApp
from connect.eaas.runner.managers import ScheduledTasksManager


@pytest.mark.asyncio
async def test_sync(mocker, extension_cls, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    cls = extension_cls(
        'my_sync_schedulable_method',
        result=ScheduledExecutionResponse.done(),
    )
    mocker.patch.object(cls, 'get_descriptor')
    mocker.patch.object(EventsApp, 'get_extension_class', return_value=cls)
    mocked_time = mocker.patch('connect.eaas.runner.managers.scheduled.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = ScheduledTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(
        return_value={'id': 'EFS-000', 'method': 'my_sync_schedulable_method'},
    )

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.SCHEDULED,
        },
        input={
            'event_type': EventType.SCHEDULED_EXECUTION,
            'object_id': 'EFS-000',
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.01)
    task.output = TaskOutput(
        result=ResultType.SUCCESS,
        runtime=1.0,
    )
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_async(mocker, extension_cls, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    cls = extension_cls(
        'my_async_schedulable_method',
        result=ScheduledExecutionResponse.done(),
        async_impl=True,
    )
    mocker.patch.object(cls, 'get_descriptor')
    mocker.patch.object(EventsApp, 'get_extension_class', return_value=cls)
    mocked_time = mocker.patch('connect.eaas.runner.managers.scheduled.time')
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, 'put')
    manager = ScheduledTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(
        return_value={'id': 'EFS-000', 'method': 'my_async_schedulable_method'},
    )

    task = Task(
        options={
            'task_id': 'TQ-000',
            'task_category': TaskCategory.SCHEDULED,
        },
        input={
            'event_type': EventType.SCHEDULED_EXECUTION,
            'object_id': 'EFS-000',
        },
    )

    await manager.submit(task)
    await asyncio.sleep(.01)
    task.output = TaskOutput(
        result=ResultType.SUCCESS,
        runtime=1.0,
    )
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_get_argument(mocker, httpx_mock, extension_cls, settings_payload,
                            task_payload, unused_port):
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
    config.update_dynamic_config(SetupResponse(**settings_payload))

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
    task = Task(
        **task_payload(TaskCategory.SCHEDULED, EventType.SCHEDULED_EXECUTION, 'EFS-000'),
    )
    assert await manager.get_argument(task) == schedule_data


@pytest.mark.asyncio
async def test_build_response_done(task_payload):
    config = ConfigHelper()
    manager = ScheduledTasksManager(config, None, None)
    task = Task(
        **task_payload(TaskCategory.SCHEDULED, EventType.SCHEDULED_EXECUTION, 'EFS-000'),
    )
    result = ScheduledExecutionResponse.done()
    future = asyncio.Future()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == result.status


@pytest.mark.asyncio
async def test_build_response_exception(mocker, task_payload):
    config = ConfigHelper()
    manager = ScheduledTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = Task(
        **task_payload(TaskCategory.SCHEDULED, EventType.SCHEDULED_EXECUTION, 'EFS-000'),
    )
    future = asyncio.Future()
    future.set_exception(Exception('Awesome error message'))
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == ResultType.RETRY
    assert 'Awesome error message' in response.output.message
    manager.log_exception.assert_called_once()
