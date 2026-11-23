import asyncio
import time

import pytest

from connect.client import (
    ClientError,
)
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
from connect.eaas.core.responses import (
    ProcessingResponse,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.constants import (
    BACKGROUND_EVENT_TYPES,
    EVENT_TYPE_EXT_METHOD_MAP,
)
from connect.eaas.runner.handlers.events import (
    EventsApp,
)
from connect.eaas.runner.managers import (
    BackgroundTasksManager,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    BACKGROUND_EVENT_TYPES,
)
async def test_sync(mocker, extension_cls, event_type, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    method = EVENT_TYPE_EXT_METHOD_MAP[event_type]
    mocker.patch.object(
        EventsApp,
        "events",
        new_callable=mocker.PropertyMock(
            return_value={
                event_type: {
                    "statuses": ["pending"],
                    "method": EVENT_TYPE_EXT_METHOD_MAP[event_type],
                },
            }
        ),
    )

    cls = extension_cls(method)
    mocker.patch.object(cls, "get_descriptor")
    mocker.patch.object(EventsApp, "load_application", return_value=cls)
    mocked_time = mocker.patch("connect.eaas.runner.managers.background.time")
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, "put")
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(
        return_value={"id": "PR-000", "status": "pending"}
    )

    task = Task(
        options={
            "task_id": "TQ-000",
            "task_category": TaskCategory.BACKGROUND,
        },
        input={
            "event_type": event_type,
            "object_id": "PR-000",
        },
        output={
            "result": ResultType.SUCCESS,
            "runtime": 1.0,
        },
    )

    await manager.submit(task)
    await asyncio.sleep(0.05)
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    BACKGROUND_EVENT_TYPES,
)
async def test_async(mocker, extension_cls, event_type, settings_payload):

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    method = EVENT_TYPE_EXT_METHOD_MAP[event_type]
    mocker.patch.object(
        EventsApp,
        "events",
        new_callable=mocker.PropertyMock(
            return_value={
                event_type: {
                    "statuses": ["pending"],
                    "method": EVENT_TYPE_EXT_METHOD_MAP[event_type],
                },
            }
        ),
    )

    cls = extension_cls(method, async_impl=True)
    mocker.patch.object(cls, "get_descriptor")
    mocker.patch.object(EventsApp, "load_application", return_value=cls)
    mocked_time = mocker.patch("connect.eaas.runner.managers.background.time")
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, "put")
    manager = BackgroundTasksManager(config, handler, result_queue)
    manager.get_argument = mocker.AsyncMock(
        return_value={"id": "PR-000", "status": "pending"}
    )

    task = Task(
        options={
            "task_id": "TQ-000",
            "task_category": TaskCategory.BACKGROUND,
        },
        input={
            "event_type": event_type,
            "object_id": "PR-000",
        },
        output={
            "result": ResultType.SUCCESS,
            "runtime": 1.0,
        },
    )

    await manager.submit(task)
    await asyncio.sleep(0.01)
    result_queue.assert_awaited_once_with(task)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    BACKGROUND_EVENT_TYPES,
)
async def test_get_argument(
    mocker,
    httpx_mock,
    extension_cls,
    event_type,
    settings_payload,
    task_payload,
    unused_port,
):
    mocker.patch(
        "connect.eaas.runner.config.get_environment",
        return_value={
            "ws_address": f"127.0.0.1:{unused_port}",
            "api_address": f"127.0.0.1:{unused_port}",
            "api_key": "SU-000:XXXX",
            "environment_id": "ENV-000-0001",
            "instance_id": "INS-000-0002",
            "background_task_max_execution_time": 300,
            "interactive_task_max_execution_time": 120,
            "scheduled_task_max_execution_time": 43200,
        },
    )

    api_url = f"https://127.0.0.1:{unused_port}/public/v1"
    mocker.patch.object(ConfigHelper, "get_api_url", return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    method = EVENT_TYPE_EXT_METHOD_MAP[event_type]
    mocker.patch.object(
        EventsApp,
        "events",
        new_callable=mocker.PropertyMock(
            return_value={
                event_type: {
                    "statuses": ["pending"],
                    "method": EVENT_TYPE_EXT_METHOD_MAP[event_type],
                },
            }
        ),
    )

    cls = extension_cls(method, async_impl=True)
    mocker.patch.object(cls, "get_descriptor")
    mocker.patch.object(EventsApp, "load_application", return_value=cls)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, "put")
    manager = BackgroundTasksManager(config, handler, result_queue)

    pr_data = {"id": "PR-000", "status": "pending"}

    httpx_mock.add_response(
        method="GET",
        url=f"{api_url}/collection?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0",
        json=[],
        headers={"Content-Range": "items 0-0/1"},
    )

    httpx_mock.add_response(
        method="GET",
        url=f"{api_url}/collection/PR-000",
        json=pr_data,
    )
    task = Task(
        **task_payload(TaskCategory.BACKGROUND, event_type, "PR-000"),
    )
    assert await manager.get_argument(task) == pr_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    BACKGROUND_EVENT_TYPES,
)
async def test_get_argument_multi_account(
    mocker,
    httpx_mock,
    extension_cls,
    event_type,
    settings_payload,
    task_payload,
    unused_port,
):
    mocker.patch(
        "connect.eaas.runner.config.get_environment",
        return_value={
            "ws_address": f"127.0.0.1:{unused_port}",
            "api_address": f"127.0.0.1:{unused_port}",
            "api_key": "SU-000:XXXX",
            "environment_id": "ENV-000-0001",
            "instance_id": "INS-000-0002",
            "background_task_max_execution_time": 300,
            "interactive_task_max_execution_time": 120,
            "scheduled_task_max_execution_time": 43200,
        },
    )

    api_url = f"https://127.0.0.1:{unused_port}/public/v1"
    mocker.patch.object(ConfigHelper, "get_api_url", return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    method = EVENT_TYPE_EXT_METHOD_MAP[event_type]
    mocker.patch.object(
        EventsApp,
        "events",
        new_callable=mocker.PropertyMock(
            return_value={
                event_type: {
                    "statuses": ["pending"],
                    "method": EVENT_TYPE_EXT_METHOD_MAP[event_type],
                },
            }
        ),
    )

    cls = extension_cls(method, async_impl=True)
    mocker.patch.object(cls, "get_descriptor")
    mocker.patch.object(EventsApp, "load_application", return_value=cls)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, "put")
    manager = BackgroundTasksManager(config, handler, result_queue)

    pr_data = {"id": "PR-000", "status": "pending"}

    httpx_mock.add_response(
        method="GET",
        url=f"{api_url}/collection?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0",
        json=[],
        headers={"Content-Range": "items 0-0/1"},
    )

    httpx_mock.add_response(
        method="GET",
        url=f"{api_url}/collection/PR-000",
        json=pr_data,
    )
    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            event_type,
            "PR-000",
            api_key="inst_api_key",
        ),
    )
    assert await manager.get_argument(task) == pr_data
    requests = httpx_mock.get_requests()
    for req in requests:
        auth_header = req.headers["Authorization"]
        assert auth_header == "inst_api_key"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    BACKGROUND_EVENT_TYPES,
)
async def test_get_argument_status_changed(
    mocker,
    httpx_mock,
    extension_cls,
    event_type,
    settings_payload,
    task_payload,
    unused_port,
):
    mocker.patch(
        "connect.eaas.runner.config.get_environment",
        return_value={
            "ws_address": f"127.0.0.1:{unused_port}",
            "api_address": f"127.0.0.1:{unused_port}",
            "api_key": "SU-000:XXXX",
            "environment_id": "ENV-000-0001",
            "instance_id": "INS-000-0002",
            "background_task_max_execution_time": 300,
            "interactive_task_max_execution_time": 120,
            "scheduled_task_max_execution_time": 43200,
        },
    )

    api_url = f"https://127.0.0.1:{unused_port}/public/v1"
    mocker.patch.object(ConfigHelper, "get_api_url", return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    method = EVENT_TYPE_EXT_METHOD_MAP[event_type]
    mocker.patch.object(
        EventsApp,
        "events",
        new_callable=mocker.PropertyMock(
            return_value={
                event_type: {
                    "statuses": ["pending"],
                    "method": EVENT_TYPE_EXT_METHOD_MAP[event_type],
                },
            }
        ),
    )

    cls = extension_cls(method, async_impl=True)
    mocker.patch.object(cls, "get_descriptor")
    mocker.patch.object(EventsApp, "load_application", return_value=cls)
    handler = EventsApp(config)

    result_queue = mocker.patch.object(asyncio.Queue, "put")
    mocked_skip = mocker.patch.object(BackgroundTasksManager, "send_skip_response")
    manager = BackgroundTasksManager(config, handler, result_queue)

    httpx_mock.add_response(
        method="GET",
        url=f"{api_url}/collection?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0",
        json=[],
        headers={"Content-Range": "items 0-0/0"},
    )

    task = Task(
        **task_payload(TaskCategory.BACKGROUND, event_type, "PR-000"),
    )
    assert await manager.get_argument(task) is None
    mocked_skip.assert_called_once()


@pytest.mark.asyncio
async def test_build_response_done(task_payload):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.PART_USAGE_FILE_REQUEST_PROCESSING,
            "UFC-000",
        ),
    )
    future = asyncio.Future()
    result = ProcessingResponse.done()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == result.status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    (
        ProcessingResponse.fail(output="message"),
        ProcessingResponse.skip(output="message"),
    ),
)
async def test_build_response_fail_skip(task_payload, result):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.PART_USAGE_FILE_REQUEST_PROCESSING,
            "UFC-000",
        ),
    )
    future = asyncio.Future()
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == result.status
    assert response.output.message == result.output


@pytest.mark.asyncio
async def test_build_response_reschedule(task_payload):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.PART_USAGE_FILE_REQUEST_PROCESSING,
            "UFC-000",
        ),
    )
    future = asyncio.Future()
    result = ProcessingResponse.reschedule(countdown=99)
    future.set_result(result)
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == result.status
    assert response.output.countdown == result.countdown


@pytest.mark.asyncio
async def test_build_response_exception(mocker, task_payload):
    config = ConfigHelper()
    manager = BackgroundTasksManager(config, None, None)
    manager.log_exception = mocker.MagicMock()

    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.PART_USAGE_FILE_REQUEST_PROCESSING,
            "UFC-000",
        ),
    )
    future = asyncio.Future()
    future.set_exception(Exception("Awesome error message"))
    response = await manager.build_response(task, future)

    assert response.options.task_id == task.options.task_id
    assert response.output.result == ResultType.RETRY
    assert "Awesome error message" in response.output.message
    manager.log_exception.assert_called_once()


@pytest.mark.asyncio
async def test_send_skip_response(mocker, task_payload):
    config = ConfigHelper()
    mocked_put = mocker.AsyncMock()
    mocked_time = mocker.patch("connect.eaas.runner.managers.background.time")
    mocked_time.sleep = time.sleep
    mocked_time.monotonic.side_effect = (1.0, 2.0)
    manager = BackgroundTasksManager(config, None, mocked_put)

    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.PART_USAGE_FILE_REQUEST_PROCESSING,
            "UFC-000",
        ),
    )

    manager.send_skip_response(task, "test output")
    await asyncio.sleep(0.01)
    task.output = TaskOutput(result=ResultType.SKIP, message="test output", runtime=1.0)

    mocked_put.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_send_transient_error_response(mocker, task_payload, caplog):
    config = ConfigHelper()
    config.env["background_transient_error_reschedule_countdown"] = 42
    mocked_put = mocker.AsyncMock()
    manager = BackgroundTasksManager(config, None, mocked_put)
    manager.running_tasks = 1

    task = Task(
        **task_payload(
            TaskCategory.BACKGROUND,
            EventType.ASSET_CHANGE_REQUEST_PROCESSING,
            "PR-000",
        ),
    )

    manager.send_transient_error_response(
        task,
        ClientError("Service unavailable", status_code=503),
    )
    await asyncio.sleep(0.01)

    mocked_put.assert_awaited_once()
    response = mocked_put.await_args.args[0]
    assert response.output.result == ResultType.RESCHEDULE
    assert response.output.countdown == 42
    assert manager.running_tasks == 0
    assert "rescheduling in 42s" in caplog.text
    assert "(status_code: 503)" in caplog.text
    assert "Service unavailable" not in caplog.text


@pytest.mark.asyncio
async def test_submit_get_argument_503_reschedules(
    mocker,
    httpx_mock,
    extension_cls,
    settings_payload,
    task_payload,
    unused_port,
):
    """LITE-35595: a Connect 503 while fetching the request must reschedule, not RETRY."""
    mocker.patch(
        "connect.eaas.runner.config.get_environment",
        return_value={
            "ws_address": f"127.0.0.1:{unused_port}",
            "api_address": f"127.0.0.1:{unused_port}",
            "api_key": "SU-000:XXXX",
            "environment_id": "ENV-000-0001",
            "instance_id": "INS-000-0002",
            "background_task_max_execution_time": 300,
            "interactive_task_max_execution_time": 120,
            "scheduled_task_max_execution_time": 43200,
            "background_transient_error_reschedule_countdown": 120,
        },
    )
    mocker.patch("connect.client.mixins.time.sleep")
    api_url = f"https://127.0.0.1:{unused_port}/public/v1"
    mocker.patch.object(ConfigHelper, "get_api_url", return_value=api_url)
    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    event_type = EventType.ASSET_CHANGE_REQUEST_PROCESSING
    method = EVENT_TYPE_EXT_METHOD_MAP[event_type]
    mocker.patch.object(
        EventsApp,
        "events",
        new_callable=mocker.PropertyMock(
            return_value={
                event_type: {"statuses": ["pending"], "method": method},
            }
        ),
    )
    cls = extension_cls(method, async_impl=True)
    mocker.patch.object(cls, "get_descriptor")
    mocker.patch.object(EventsApp, "load_application", return_value=cls)
    handler = EventsApp(config)
    mocked_invoke = mocker.patch.object(BackgroundTasksManager, "invoke")
    mocked_put = mocker.AsyncMock()
    manager = BackgroundTasksManager(config, handler, mocked_put)

    httpx_mock.add_response(
        method="GET",
        url=f"{api_url}/collection?and(eq(id,PR-000),in(status,(pending)))&limit=0&offset=0",
        json=[],
        headers={"Content-Range": "items 0-0/1"},
    )
    for _ in range(4):  # first call + client max_retries (3)
        httpx_mock.add_response(
            method="GET",
            url=f"{api_url}/collection/PR-000",
            status_code=503,
            json={"error_code": "SYS_001", "errors": ["Service unavailable"]},
        )

    task = Task(**task_payload(TaskCategory.BACKGROUND, event_type, "PR-000"))
    await manager.submit(task)
    await asyncio.sleep(0.01)

    mocked_invoke.assert_not_called()
    mocked_put.assert_awaited_once()
    response = mocked_put.await_args.args[0]
    assert response.output.result == ResultType.RESCHEDULE
    assert response.output.countdown == 120
    assert manager.running_tasks == 0
