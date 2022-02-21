import asyncio
import logging

import pytest

from connect.client import ClientError
from connect.eaas.config import ConfigHelper
from connect.eaas.dataclasses import ConfigurationPayload, TaskPayload
from connect.eaas.managers.base import TasksManagerBase
from connect.eaas.handler import ExtensionHandler


@pytest.mark.asyncio
async def test_submit(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            pass

        def get_method(self, task_data, extension, argument):
            return extension.my_method

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    mocked_get_argument.return_value = {'test': 'data'}
    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    mocked_new_extension = mocker.patch.object(ExtensionHandler, 'new_extension')

    config = ConfigHelper()
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    cls = extension_cls('my_method')

    extension_instance = cls(None, None, None)

    mocked_new_extension.return_value = extension_instance

    handler.extension_class = cls
    handler.extension_type = 'sync'

    manager = TaskManager(config, handler, None)

    task_data = TaskPayload(**task_payload(
        'category',
        'type',
        'ID',
    ))
    assert manager.running_tasks == 0
    await manager.submit(task_data)

    mocked_new_extension.assert_called_once_with(task_data.task_id)
    mocked_get_argument.assert_awaited_once_with(task_data)
    mocked_invoke.assert_awaited_once_with(
        task_data, extension_instance.my_method, {'test': 'data'},
    )
    assert manager.running_tasks == 1


@pytest.mark.asyncio
async def test_submit_client_error(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            try:
                await future
            except Exception as e:
                task = TaskPayload(**task_payload(
                    'category',
                    'type',
                    'ID',
                ))
                task.output = str(e)
                return task

        async def get_argument(self, task_data):
            pass

        def get_method(self, task_data, extension, argument):
            return extension.my_method

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    exc = ClientError('test error')
    mocked_get_argument.side_effect = exc
    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    mocked_new_extension = mocker.patch.object(ExtensionHandler, 'new_extension')
    mocked_put = mocker.AsyncMock()

    config = ConfigHelper()
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    cls = extension_cls('my_method')

    extension_instance = cls(None, None, None)

    mocked_new_extension.return_value = extension_instance

    handler.extension_class = cls
    handler.extension_type = 'sync'

    manager = TaskManager(config, handler, mocked_put)

    task_data = TaskPayload(**task_payload(
        'category',
        'type',
        'ID',
    ))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_new_extension.assert_called_once_with(task_data.task_id)
    mocked_get_argument.assert_awaited_once_with(task_data)
    mocked_invoke.assert_not_awaited()
    task_data.output = exc.message
    mocked_put.assert_awaited_once_with(task_data)
    assert manager.running_tasks == 0


@pytest.mark.asyncio
async def test_submit_exception(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            try:
                await future
            except Exception as e:
                task = TaskPayload(**task_payload(
                    'category',
                    'type',
                    'ID',
                ))
                task.output = str(e)
                return task

        async def get_argument(self, task_data):
            pass

        def get_method(self, task_data, extension, argument):
            return extension.my_method

    mocked_get_argument = mocker.patch.object(TaskManager, 'get_argument')
    mocker.patch.object(TaskManager, 'invoke', side_effect=Exception('invoke exc'))
    mocked_new_extension = mocker.patch.object(ExtensionHandler, 'new_extension')
    mocked_put = mocker.AsyncMock()

    config = ConfigHelper()
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    cls = extension_cls('my_method')

    extension_instance = cls(None, None, None)

    mocked_new_extension.return_value = extension_instance

    handler.extension_class = cls
    handler.extension_type = 'sync'

    manager = TaskManager(config, handler, mocked_put)

    task_data = TaskPayload(**task_payload(
        'category',
        'type',
        'ID',
    ))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_new_extension.assert_called_once_with(task_data.task_id)
    mocked_get_argument.assert_awaited_once_with(task_data)
    task_data.output = 'invoke exc'
    mocked_put.assert_awaited_once_with(task_data)
    assert manager.running_tasks == 0


@pytest.mark.asyncio
async def test_submit_no_argument(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            return None

        def get_method(self, task_data, extension, argument):
            return extension.my_method

    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    mocked_new_extension = mocker.patch.object(ExtensionHandler, 'new_extension')

    config = ConfigHelper()
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    cls = extension_cls('my_method')

    extension_instance = cls(None, None, None)

    mocked_new_extension.return_value = extension_instance

    handler.extension_class = cls
    handler.extension_type = 'sync'

    manager = TaskManager(config, handler, None)

    task_data = TaskPayload(**task_payload(
        'category',
        'type',
        'ID',
    ))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_new_extension.assert_called_once_with(task_data.task_id)
    mocked_invoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_no_method(mocker, extension_cls, task_payload):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            return True

        def get_method(self, task_data, extension, argument):
            return None

    mocked_invoke = mocker.patch.object(TaskManager, 'invoke')
    mocked_new_extension = mocker.patch.object(ExtensionHandler, 'new_extension')

    config = ConfigHelper()
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    cls = extension_cls('my_method')

    extension_instance = cls(None, None, None)

    mocked_new_extension.return_value = extension_instance

    handler.extension_class = cls
    handler.extension_type = 'sync'

    manager = TaskManager(config, handler, None)

    task_data = TaskPayload(**task_payload(
        'category',
        'type',
        'ID',
    ))
    await manager.submit(task_data)
    await asyncio.sleep(.01)
    mocked_new_extension.assert_called_once_with(task_data.task_id)
    mocked_invoke.assert_not_awaited()
    assert manager.running_tasks == 0


@pytest.mark.asyncio
async def test_log_exception(mocker, extension_cls, config_payload, task_payload, caplog):
    class TaskManager(TasksManagerBase):
        async def build_response(self, task_data, future):
            pass

        async def get_argument(self, task_data):
            return True

        def get_method(self, task_data, extension, argument):
            return None

    config = ConfigHelper()
    dyn = ConfigurationPayload(**config_payload)
    dyn.logging_api_key = None
    config.update_dynamic_config(dyn)
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    handler = ExtensionHandler(config)

    handler.extension_class = extension_cls('my_method')
    handler.extension_type = 'sync'

    manager = TaskManager(config, handler, None)

    task_data = TaskPayload(**task_payload(
        'category',
        'type',
        'ID',
    ))

    with caplog.at_level(logging.ERROR):
        manager.log_exception(task_data, Exception('test exc'))

    assert caplog.record_tuples[-1] == (
        'eaas.extension',
        logging.ERROR,
        'Unhandled exception during execution of task TQ-000',
    )
