#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import inspect
import logging
from abc import (
    ABC,
    abstractmethod,
)
from asyncio.futures import (
    Future,
)
from concurrent.futures import (
    ThreadPoolExecutor,
)
from string import (
    Template,
)
from typing import (
    Union,
)

from connect.client import (
    AsyncConnectClient,
    ClientError,
)
from connect.client.models import (
    AsyncCollection,
)
from connect.eaas.core.enums import (
    TaskCategory,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.events import (
    EventsApp,
)
from connect.eaas.runner.handlers.transformations import (
    TfnApp,
)


logger = logging.getLogger(__name__)


class TasksManagerBase(ABC):
    """
    Base class for Tasks managers.
    """
    def __init__(self, config: ConfigHelper, handler: Union[EventsApp, TfnApp], enqueue):
        self.config = config
        self.handler = handler
        self.enqueue = enqueue
        self.lock = asyncio.Lock()
        self.executor = ThreadPoolExecutor(max_workers=100)
        self.client = AsyncConnectClient(
            self.config.api_key,
            endpoint=self.config.get_api_url(),
            use_specs=False,
            default_headers=self.config.get_user_agent(),
        )
        self._task_api_key_clients = {}
        self.running_tasks = 0

    async def filter_collection_by_event_definition(self, client, task_data):
        definition = self.config.event_definitions[task_data.input.event_type]
        supported_statuses = []
        template_args = {'_object_id_': task_data.input.object_id}
        if 'events' in self.handler.get_features():
            supported_statuses = self.handler.events[task_data.input.event_type]['statuses']
            template_args['_statuses_'] = f'({",".join(supported_statuses)})'

        rql_filter = Template(definition.api_collection_filter).substitute(
            template_args,
        )

        collection = AsyncCollection(client, definition.api_collection_endpoint)

        count = await collection.filter(rql_filter).count()
        if count == 0:
            logger.info(
                f'Send skip response for {task_data.options.task_id} since '
                'the current request status is not supported.',
            )
            self.send_skip_response(
                task_data,
                'The request status does not match the '
                f'supported statuses: {",".join(supported_statuses)}.',
            )

        return count

    async def submit(self, task_data):
        """
        Submit a new task for its processing.

        :param task_data: Task data.
        :type task_data: connect.eaas.core.proto.Task
        """
        try:
            method = None
            argument = None
            async with self.lock:
                self.running_tasks += 1
            logger.info(
                f'new {task_data.options.task_category} task received: '
                f'{task_data.options.task_id}, running tasks: {self.running_tasks}',
            )
            argument = await self.get_argument(task_data)
            if not argument:
                return
            installation = None
            if task_data.options.installation_id:
                installation = await self.get_installation(task_data)
            method_name = self.get_method_name(task_data, argument)
            kwargs = {
                'installation': installation,
                'api_key': task_data.options.api_key,
                'connect_correlation_id': task_data.options.connect_correlation_id,
            }
            if task_data.options.task_category == TaskCategory.TRANSFORMATION:
                kwargs['transformation_request'] = argument
            method = self.handler.get_method(
                task_data.input.event_type,
                task_data.options.task_id,
                method_name,
                **kwargs,
            )
            if not method:
                async with self.lock:
                    self.running_tasks -= 1
                return

            logger.info(f'invoke method {method.__name__} for task {task_data.options.task_id}')
            await self.invoke(task_data, method, argument)
        except ClientError as ce:
            logger.warning(
                f'Cannot retrieve object {task_data.input.object_id} '
                f'for task {task_data.options.task_id}',
            )
            self.send_exception_response(task_data, ce)
            return
        except Exception as e:
            logger.error(
                f'Exceptions while processing task {task_data.options.task_id}',
            )
            self.send_exception_response(task_data, e)
            return

    def send_exception_response(self, data, e):
        """
        Enqueue a future to send back to EaaS a failed task
        with exception info.
        :param data: Task data.
        :type data: connect.eaas.core.proto.Task
        :param e: The exception that need to be sent back.
        :type e: Exception
        """
        future = Future()
        future.set_exception(e)
        asyncio.create_task(self.enqueue_result(data, future))

    async def enqueue_result(self, task_data, future):
        """
        Build a result object for a task and put it in the
        result queue.
        """
        await self.enqueue(
            await self.build_response(task_data, future),
        )
        async with self.lock:
            self.running_tasks -= 1

    async def invoke(self, task_data, method, argument):
        """
        Invokes a method of the extension class instance
        to process a task. If the method is synchronous
        it will be executed on a ThreadPoolExecutor.

        :param task_data: Data of the task to be processed.
        :type task_data: connect.eaas.core.proto.Task
        :param method: The method that has to be invoked.
        :param argument: The Connect object that need to be processed.
        :type argument: dict
        """
        if inspect.iscoroutinefunction(method):
            future = asyncio.create_task(method(argument))
        else:
            future = asyncio.get_running_loop().run_in_executor(
                self.executor,
                method,
                argument,
            )
        logger.info(f'enqueue result for task {task_data.options.task_id}')
        asyncio.create_task(self.enqueue_result(task_data, future))

    def log_exception(self, task_data, e):
        """
        Logs an unhandled exception both with the runner logger
        and the extension one.
        """
        logger.warning(
            f'Got exception during execution of task {task_data.options.task_id}: {e}',
        )
        ext_logger = self.handler.get_logger(
            extra={'task_id': task_data.options.task_id},
        )
        ext_logger.exception(
            f'Unhandled exception during execution of task {task_data.options.task_id}',
        )

    @abstractmethod
    async def build_response(self, task_data, future):  # pragma: no cover
        """
        Wait for a task future to be completed, set the results
        on the task object and returns it.
        """
        pass

    @abstractmethod
    async def get_argument(self, task_data):  # pragma: no cover
        """
        Retrieves the Connect object source of the event that
        generates the task in order to pass it as the extension
        method argument.
        """
        pass

    async def get_installation(self, task_data):
        """
        Get the related event installation for multi-account
        extension.
        """
        return await self.client(
            'devops',
        ).services[self.config.service_id].installations[task_data.options.installation_id].get()

    @abstractmethod
    def get_method_name(self, task_data, argument):  # pragma: no cover
        """
        Returns the extension method has to be invoked.
        """
        pass
