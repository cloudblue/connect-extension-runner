#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from asyncio.futures import Future
from concurrent.futures import ThreadPoolExecutor

from connect.client import AsyncConnectClient, ClientError
from connect.eaas.config import ConfigHelper
from connect.eaas.handler import ExtensionHandler


logger = logging.getLogger(__name__)


class TasksManagerBase(ABC):
    """
    Base class for Tasks managers.
    """
    def __init__(self, config: ConfigHelper, handler: ExtensionHandler, enqueue):
        self.config = config
        self.handler = handler
        self.enqueue = enqueue
        self.executor = ThreadPoolExecutor()
        self.client = AsyncConnectClient(
            self.config.api_key,
            endpoint=self.config.get_api_url(),
            use_specs=False,
            default_headers=self.config.get_user_agent(),
        )
        self.running_tasks = 0

    async def submit(self, task_data):
        """
        Submit a new task for its processing.

        :param task_data: Task data.
        :type task_data: connect.eaas.dataclasses.TaskPayload
        """
        try:
            extension = self.handler.new_extension(task_data.task_id)
            method = None
            argument = None
            self.running_tasks += 1
            logger.info(
                f'new background task received: {task_data.task_id}, '
                f'running tasks: {self.running_tasks}',
            )
            argument = await self.get_argument(task_data)

            if not argument:
                self.running_tasks -= 1
                return
            method = self.get_method(task_data, extension, argument)
            if not method:
                self.running_tasks -= 1
                return

            logger.info(f'invoke method {method.__name__}')
            await self.invoke(task_data, method, argument)
        except ClientError as ce:
            logger.warning(
                f'Cannot retrieve object {task_data.object_id} for task {task_data.task_id}',
            )
            self.send_exception_response(task_data, ce)
            return
        except Exception as e:
            logger.error(
                f'Exceptions while processing task {task_data.task_id}',
            )
            self.send_exception_response(task_data, e)
            return

    def send_exception_response(self, data, e):
        """
        Enqueue a future to send back to EaaS a failed task
        with exception info.
        :param data: Task data.
        :type data: connect.eaas.dataclasses.TaskPayload
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
        self.running_tasks -= 1
        await self.enqueue(
            await self.build_response(task_data, future),
        )

    async def invoke(self, task_data, method, argument):
        """
        Invokes a method of the extension class instance
        to process a task. If the method is synchronous
        it will be executed on a ThreadPoolExecutor.

        :param task_data: Data of the task to be processed.
        :type task_data: connect.eaas.dataclasses.TaskPayload
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
        logger.info(f'enqueue result for task {task_data.task_id}')
        asyncio.create_task(self.enqueue_result(task_data, future))

    def log_exception(self, task_data, e):
        """
        Logs an unhandled exception both with the runner logger
        and the extension one.
        """
        logger.warning(f'Got exception during execution of task {task_data.task_id}: {e}')
        self.handler.new_extension(task_data.task_id).logger.exception(
            f'Unhandled exception during execution of task {task_data.task_id}',
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

    @abstractmethod
    def get_method(self, task_data, extension, argument):  # pragma: no cover
        """
        Returns the extension method has to be invoked.
        """
        pass
