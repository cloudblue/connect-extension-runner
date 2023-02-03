import asyncio
import logging

import pytest
from connect.client import (
    ClientError,
)

from connect.eaas.core.proto import (
    SetupResponse,
    Task,
    TaskOutput,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.events import (
    EventsApp,
)
from connect.eaas.runner.managers.base import (
    TasksManagerBase,
)


@pytest.mark.asyncio
async def test_submit(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            pass

        def get_method_name(self, task_data, argument):
            return 'my_method'

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    mocked_get_argument.return_value = {'test': 'data'}
    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    mocked_get_method = mocker.patch.object(EventsApp, 'get_method')

    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    config = ConfigHelper()
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    handler = EventsApp(config)

    extension_instance = cls(None, None, None)

    mocked_get_method.return_value = extension_instance.my_method

    manager = TaskManager(config, handler, None)

    task_data = Task(**task_payload('category', 'type', 'ID'))
    assert manager.running_tasks == 0
    await manager.submit(task_data)

    mocked_get_method.assert_called_once_with(
        'type',
        task_data.options.task_id,
        'my_method',
        installation=None,
        api_key=None,
        connect_correlation_id=None,
    )
    mocked_get_argument.assert_awaited_once_with(task_data)
    mocked_invoke.assert_awaited_once_with(
        task_data, extension_instance.my_method, {'test': 'data'},
    )
    assert manager.running_tasks == 1


@pytest.mark.asyncio
async def test_submit_multi_account(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            pass

        def get_method_name(self, task_data, argument):
            return 'my_method'

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    mocked_get_argument.return_value = {'test': 'data'}
    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    mocked_get_method = mocker.patch.object(EventsApp, 'get_method')
    mocked_get_installation = mocker.patch.object(
        TaskManager,
        'get_installation',
        return_value={'installation': 'data'},
    )

    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    config = ConfigHelper()
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    handler = EventsApp(config)

    extension_instance = cls(None, None, None)

    mocked_get_method.return_value = extension_instance.my_method

    manager = TaskManager(config, handler, None)

    task_data = Task(**task_payload(
        'category', 'type', 'ID',
        api_key='api_key', installation_id='installation_id',
        connect_correlation_id='correlation_id',
    ))
    assert manager.running_tasks == 0
    await manager.submit(task_data)

    mocked_get_method.assert_called_once_with(
        'type',
        task_data.options.task_id,
        'my_method',
        installation={'installation': 'data'},
        api_key='api_key',
        connect_correlation_id='correlation_id',
    )
    mocked_get_argument.assert_awaited_once_with(task_data)
    mocked_invoke.assert_awaited_once_with(
        task_data, extension_instance.my_method, {'test': 'data'},
    )
    assert manager.running_tasks == 1
    mocked_get_installation.assert_called_once_with(task_data)


@pytest.mark.asyncio
async def test_submit_client_error(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            try:
                await future
            except Exception as e:
                task_data.output = TaskOutput(result='fail', message=str(e))
                return task_data

        async def get_argument(self, task_data):
            pass

        def get_method_name(self, task_data, argument):
            return 'my_method'

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    exc = ClientError('test error')
    mocked_get_argument.side_effect = exc
    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')

    mocked_put = mocker.AsyncMock()

    mocked_get_method = mocker.patch.object(EventsApp, 'get_method')

    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    config = ConfigHelper()
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    handler = EventsApp(config)

    manager = TaskManager(config, handler, mocked_put)

    task_data = Task(**task_payload('category', 'type', 'ID'))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_get_argument.assert_awaited_once_with(task_data)
    mocked_get_method.assert_not_called()
    mocked_invoke.assert_not_awaited()
    task_data.output.message = exc.message
    mocked_put.assert_awaited_once_with(task_data)
    assert manager.running_tasks == 0


@pytest.mark.asyncio
async def test_submit_exception(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            try:
                await future
            except Exception as e:
                task_data.output = TaskOutput(result='fail', message=str(e))
                return task_data

        async def get_argument(self, task_data):
            pass

        def get_method_name(self, task_data, argument):
            return 'my_method'

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    mocker.patch.object(TaskManager, 'invoke', side_effect=Exception('invoke exc'))
    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    config = ConfigHelper()
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    handler = EventsApp(config)

    mocked_put = mocker.AsyncMock()

    manager = TaskManager(config, handler, mocked_put)

    task_data = Task(**task_payload('category', 'type', 'ID'))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_get_argument.assert_awaited_once_with(task_data)
    task_data.output.message = 'invoke exc'
    mocked_put.assert_awaited_once_with(task_data)
    assert manager.running_tasks == 0


@pytest.mark.asyncio
async def test_submit_no_argument(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            return None

        def get_method_name(self, task_data, argument):
            return 'my_method'

    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)

    config = ConfigHelper()
    handler = EventsApp(config)

    manager = TaskManager(config, handler, None)

    task_data = Task(**task_payload('category', 'type', 'ID'))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_invoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_no_method(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            return True

        def get_method_name(self, task_data, argument):
            return 'my_method'

    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')

    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    mocker.patch.object(EventsApp, 'get_method', return_value=None)

    config = ConfigHelper()
    handler = EventsApp(config)

    manager = TaskManager(config, handler, None)

    task_data = Task(**task_payload('category', 'type', 'ID'))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_invoke.assert_not_awaited()
    assert manager.running_tasks == 0


@pytest.mark.asyncio
async def test_log_exception(mocker, extension_cls, settings_payload, task_payload, caplog):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            return True

        def get_method_name(self, task_data, argument):
            return None

    config = ConfigHelper()
    dyn = SetupResponse(**settings_payload)
    dyn.logging.logging_api_key = None
    config.update_dynamic_config(dyn)
    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    handler = EventsApp(config)

    manager = TaskManager(config, handler, None)

    task_data = Task(**task_payload('category', 'type', 'ID'))

    with caplog.at_level(logging.ERROR):
        manager.log_exception(task_data, Exception('test exc'))

    assert caplog.record_tuples[-1] == (
        'eaas.eventsapp',
        logging.ERROR,
        'Unhandled exception during execution of task TQ-000',
    )


@pytest.mark.asyncio
async def test_get_installation(
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

    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            pass

        def get_method_name(self, task_data, argument):
            return 'my_method'

    cls = extension_cls('my_method')
    mocker.patch.object(cls, 'get_descriptor')
    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    mocker.patch.object(EventsApp, 'load_application', return_value=cls)
    handler = EventsApp(config)

    manager = TaskManager(config, handler, None)

    task_data = Task(**task_payload(
        'category', 'type', 'ID', api_key='api_key', installation_id='installation_id',
    ))

    installation = {'installation': 'data'}

    httpx_mock.add_response(
        method='GET',
        url=f'{api_url}/devops/services/{config.service_id}/installations/installation_id',
        json=installation,
    )
    assert await manager.get_installation(task_data) == installation
