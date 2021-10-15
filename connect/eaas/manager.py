#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import inspect
import logging
import traceback
from asyncio.futures import Future
from concurrent.futures import ThreadPoolExecutor

from connect.client import AsyncConnectClient, ClientError
from connect.eaas.constants import (
    ASSET_REQUEST_TASK_TYPES,
    RESULT_SENDER_MAX_RETRIES,
    TASK_TYPE_EXT_METHOD_MAP,
    TIER_CONFIG_REQUEST_TASK_TYPES,
)
from connect.eaas.dataclasses import (
    Message,
    MessageType,
    ResultType,
    TaskCategory,
    TaskPayload,
    TaskType,
)
from connect.eaas.extension import ProcessingResponse


logger = logging.getLogger(__name__)


class TasksManager:
    """
    The TasksManager is responsible for dispatching tasks to the
    corresponding methods of the extension.
    When a task is submitted, the TasksManager executes the method
    of the extension in a worker thread if the extension method is
    synchronous otherwise it schedules its execution as an asyncio task.
    Once submitted, an asyncio task will be started that will wait for
    the future returned by the thread pool to be completed.
    Once completed, it will push the result to a queue from which, the
    result_sender task will pick it and send the result to the backend.
    """
    def __init__(self, worker):
        self.worker = worker
        self.background_executor = ThreadPoolExecutor()
        self.interactive_executor = ThreadPoolExecutor()
        self.scheduled_executor = ThreadPoolExecutor()
        self.run_event = asyncio.Event()
        self.loop = asyncio.get_running_loop()
        self.client = AsyncConnectClient(
            self.worker.api_key,
            endpoint=f'https://{self.worker.api_address}/public/v1',
            use_specs=False,
        )
        self.results_queue = asyncio.Queue()
        self.running_background_tasks = 0
        self.running_scheduled_tasks = 0
        self.running_interactive_tasks = 0
        env = self.worker.env
        self.background_task_max_execution_time = env['background_task_max_execution_time']
        self.interactive_task_max_execution_time = env['interactive_task_max_execution_time']
        self.scheduled_task_max_execution_time = env['scheduled_task_max_execution_time']
        self.main_task = None
        self.sender_task = None

    def start(self):
        logger.info('Starting tasks manager...')
        self.main_task = asyncio.create_task(self.result_sender())
        logger.info('Tasks manager started')
        self.run_event.set()

    async def stop(self):
        logger.info('Stopping tasks manager...')
        self.run_event.clear()
        await self.main_task
        logger.info('Tasks manager stopped')

    def is_running(self):
        return self.run_event.is_set()

    @property
    def running_tasks(self):
        return (
            self.running_background_tasks
            + self.running_interactive_tasks
            + self.running_scheduled_tasks
        )

    async def submit_task(self, data):
        """
        Submit a new task to a worker thread if the extension handler
        method is synchronous, otherwise schedules its execution as an
        asyncio task. It also retrieves the task's related object through
        the Connect public API and pass it to the handler method.
        """
        logger.info(
            f'submit new task: id={data.task_id}, '
            f'type={data.task_type}, object={data.object_id}',
        )
        object_id = data.object_id
        task_type = data.task_type
        extension = self.worker.get_extension(data.task_id)
        request = None
        if data.task_category == TaskCategory.BACKGROUND:
            self.running_background_tasks += 1
            method_name = TASK_TYPE_EXT_METHOD_MAP[task_type]
            method = getattr(extension, method_name)
            logger.debug(f'invoke {method_name}')
            try:
                request = await self.get_request(object_id, task_type)
            except ClientError as e:
                logger.warning(f'Cannot retrieve object {data.object_id} for task {data.task_id}')
                self.send_exception_response(data, e)
                return
            request_status = request.get('status')
            if request_status not in self.worker.capabilities[task_type]:
                logger.debug('Send skip response since request status is not supported.')
                self.send_skip_response(
                    data,
                    f'The status {request_status} is not supported by the extension.',
                )
                return
        elif data.task_category == TaskCategory.INTERACTIVE:
            self.running_interactive_tasks += 1
            method_name = TASK_TYPE_EXT_METHOD_MAP[task_type]
            method = getattr(extension, method_name)
            logger.debug(f'invoke {method_name}')
            request = data.data
        elif data.task_category == TaskCategory.SCHEDULED:
            self.running_scheduled_tasks += 1
            try:
                request = await self.get_schedule(object_id)
            except ClientError as e:
                logger.warning(f'Cannot retrieve object {data.object_id} for task {data.task_id}')
                self.send_exception_response(data, e)
                return
            method_name = request['method']
            method = getattr(extension, method_name)
            logger.debug(f'invoke {method_name}')

        if inspect.iscoroutinefunction(method):
            future = asyncio.create_task(method(request))
        else:
            future = self.loop.run_in_executor(
                getattr(self, f'{data.task_category}_executor'),
                method,
                request,
            )
        logger.info(f'Task {data.task_id} has been submitted.')
        logger.debug(f'Current running task: {self.running_tasks}')
        asyncio.create_task(self.enqueue_result(data, future))

    async def get_request(self, object_id, task_type):
        """
        Get the request object through the Connect public API
        related to the task that need processing.
        """
        if task_type in ASSET_REQUEST_TASK_TYPES:
            logger.debug(f'get asset request {object_id}')
            return await self.client.requests[object_id].get()
        if task_type in TIER_CONFIG_REQUEST_TASK_TYPES:
            logger.debug(f'get TC request {object_id}')
            return await self.client('tier').config_requests[object_id].get()

    async def get_schedule(self, object_id):
        return (
            await self.client('devops')
            .services[self.worker.service_id]
            .environments[self.worker.environment_id]
            .schedules[object_id]
            .get()
        )

    async def result_sender(self):  # noqa: CCR001
        """
        This coroutine is responsible of dequeueing results from
        the results queue and send it to the EaaS backend.
        """
        while True:
            if self.results_queue.empty():
                if not self.run_event.is_set() and self.running_tasks == 0:
                    logger.info('Worker exiting and no more running tasks: exit!')
                    return
                await asyncio.sleep(.1)
                continue
            if self.worker.ws is None or self.worker.ws.closed:
                if not self.run_event.is_set() and self.running_tasks == 0:
                    return
                logger.debug('wait WS reconnection before resuming result sender')
                await asyncio.sleep(.1)
                continue

            result = await self.results_queue.get()
            logger.debug(f'got result from queue: {result}')
            retries = 0
            while retries < RESULT_SENDER_MAX_RETRIES:
                try:
                    message = Message(
                        message_type=MessageType.TASK,
                        data=result,
                    )
                    await self.worker.send(dataclasses.asdict(message))
                    logger.info(f'Result for task {result.task_id} has been sent.')
                    break
                except Exception:
                    retries += 1
                    await asyncio.sleep(.1)
            else:
                logger.warning(
                    f'max retries exceeded for sending results of task {result.task_id}',
                )

            if not self.run_event.is_set():
                logger.debug(
                    f'Current processing status: running={self.running_tasks} '
                    f'results={self.results_queue.qsize()}',
                )

    async def enqueue_result(self, task_data, future):
        """
        Build a result object for a task and put it in the
        result queue.
        """
        if task_data.task_category == TaskCategory.BACKGROUND:
            self.running_background_tasks -= 1
            await self.results_queue.put(
                await self.build_bg_response(task_data, future),
            )
        elif task_data.task_category == TaskCategory.INTERACTIVE:
            self.running_interactive_tasks -= 1
            await self.results_queue.put(
                await self.build_interactive_response(task_data, future),
            )
        elif task_data.task_category == TaskCategory.SCHEDULED:
            self.running_scheduled_tasks -= 1
            await self.results_queue.put(
                await self.build_scheduled_response(task_data, future),
            )

        logger.debug(f'enqueue results for sender, running tasks: {self.running_tasks}')

    def send_exception_response(self, data, e):
        future = Future()
        future.set_exception(e)
        asyncio.create_task(self.enqueue_result(data, future))

    def send_skip_response(self, data, output):
        future = Future()
        future.set_result(ProcessingResponse.skip(output))
        asyncio.create_task(self.enqueue_result(data, future))

    async def build_bg_response(self, task_data, future):
        """
        Wait for a background task to be completed and than uild the task result message.
        """
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        result = None
        try:
            result = await asyncio.wait_for(future, timeout=self.background_task_max_execution_time)
        except Exception as e:
            logger.warning(f'Got exception during execution of task {task_data.task_id}: {e}')
            self.worker.get_extension(task_data.task_id).logger.exception(
                f'Unhandled exception during execution of task {task_data.task_id}',
            )
            result_message.result = ResultType.RETRY
            result_message.output = traceback.format_exc()[:4000]
            return result_message
        logger.debug(f'result: {result}')
        result_message.result = result.status

        if result.status in (ResultType.SKIP, ResultType.FAIL):
            result_message.output = result.output

        if result.status == ResultType.RESCHEDULE:
            result_message.countdown = result.countdown
        return result_message

    async def build_interactive_response(self, task_data, future):
        """
        Wait for an interactive task to be completed and than uild the task result message.
        """
        result = None
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.interactive_task_max_execution_time,
            )
        except Exception as e:
            logger.warning(f'Got exception during execution of task {task_data.task_id}: {e}')
            self.worker.get_extension(task_data.task_id).logger.exception(
                f'Unhandled exception during execution of task {task_data.task_id}',
            )
            result_message.result = ResultType.FAIL
            result_message.output = traceback.format_exc()[:4000]
            if result_message.task_type in (
                TaskType.PRODUCT_ACTION_EXECUTION,
                TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            ):
                result_message.data = {
                    'http_status': 400,
                    'headers': None,
                    'body': result_message.output,
                }

            return result_message

        result_message.result = result.status
        result_message.data = result.data
        result_message.output = result.output
        return result_message

    async def build_scheduled_response(self, task_data, future):
        """
        Wait for a scheduled task to be completed and than uild the task result message.
        """
        result = None
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        try:
            result = await asyncio.wait_for(future, timeout=self.scheduled_task_max_execution_time)
        except Exception as e:
            logger.warning(f'Got exception during execution of task {task_data.task_id}: {e}')
            self.worker.get_extension(task_data.task_id).logger.exception(
                f'Unhandled exception during execution of task {task_data.task_id}',
            )
            result_message.result = ResultType.RETRY
            result_message.output = traceback.format_exc()[:4000]

            return result_message

        result_message.result = result.status
        result_message.output = result.output
        return result_message
