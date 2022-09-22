#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import logging
import time
import traceback

from connect.eaas.core.enums import ResultType
from connect.eaas.core.proto import (
    Task,
    TaskOutput,
)
from connect.eaas.runner.managers.base import TasksManagerBase


logger = logging.getLogger(__name__)


class ScheduledTasksManager(TasksManagerBase):

    def get_method_name(self, task_data, argument):
        return argument['method']

    async def get_argument(self, task_data):
        return (
            await self.client('devops')
            .services[self.config.service_id]
            .environments[self.config.environment_id]
            .schedules[task_data.input.object_id]
            .get()
        )

    async def build_response(self, task_data, future):
        """
        Wait for a scheduled task to be completed and then build the task result message.
        """
        result = None
        result_message = Task(**task_data.dict())
        try:
            begin_ts = time.monotonic()
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('scheduled'),
            )
            result_message.output = TaskOutput(result=result.status)
            result_message.output.message = result.output
            result_message.output.runtime = time.monotonic() - begin_ts
            logger.info(
                f'interactive task {task_data.options.task_id} result: {result.status}, took:'
                f' {result_message.output.runtime}',
            )
        except Exception as e:
            self.log_exception(task_data, e)
            result_message.output = TaskOutput(result=ResultType.RETRY)
            result_message.output.message = traceback.format_exc()[:4000]

        return result_message
