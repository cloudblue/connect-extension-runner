#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import logging
import traceback

from connect.eaas.constants import TASK_TYPE_EXT_METHOD_MAP
from connect.eaas.dataclasses import (
    ResultType,
    TaskPayload,
    TaskType,
)
from connect.eaas.managers.base import TasksManagerBase


logger = logging.getLogger(__name__)


class InteractiveTasksManager(TasksManagerBase):

    def get_method(self, task_data, extension, argument):
        method_name = TASK_TYPE_EXT_METHOD_MAP[task_data.task_type]
        return getattr(extension, method_name)

    async def get_argument(self, task_data):
        return task_data.data

    async def build_response(self, task_data, future):
        """
        Wait for an interactive task to be completed and than uild the task result message.
        """
        result = None
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('interactive'),
            )
            result_message.result = result.status
            result_message.data = result.data
            result_message.output = result.output
        except Exception as e:
            self.log_exception(task_data, e)
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
