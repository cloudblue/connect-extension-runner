#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import logging
import time
import traceback

from connect.eaas.core.dataclasses import (
    EventType,
    ResultType,
    TaskPayload,
)
from connect.eaas.extension_runner.constants import EVENT_TYPE_EXT_METHOD_MAP
from connect.eaas.extension_runner.managers.base import TasksManagerBase

logger = logging.getLogger(__name__)


class InteractiveTasksManager(TasksManagerBase):

    def get_method(self, task_data, extension, argument):
        method_name = EVENT_TYPE_EXT_METHOD_MAP[task_data.input.event_type]
        return getattr(extension, method_name, None)

    async def get_argument(self, task_data):
        return task_data.input.data

    async def build_response(self, task_data, future):
        """
        Wait for an interactive task to be completed and then build the task result message.
        """
        result = None
        result_message = TaskPayload(**task_data.dict())
        try:
            begin_ts = time.monotonic()
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('interactive'),
            )
            result_message.options.result = result.status
            result_message.input.data = result.data
            result_message.options.output = result.output
            result_message.options.runtime = time.monotonic() - begin_ts
            logger.info(
                f'interactive task {task_data.options.task_id} result: {result.status}, took:'
                f' {result_message.options.runtime}',
            )
        except Exception as e:
            self.log_exception(task_data, e)
            result_message.options.result = ResultType.FAIL
            result_message.options.output = traceback.format_exc()[:4000]
            if result_message.input.event_type in (
                EventType.PRODUCT_ACTION_EXECUTION,
                EventType.PRODUCT_CUSTOM_EVENT_PROCESSING,
            ):
                result_message.input.data = {
                    'http_status': 400,
                    'headers': None,
                    'body': result_message.options.output,
                }

        return result_message
