#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import logging
import traceback

from connect.eaas.dataclasses import (
    ResultType,
    TaskPayload,
)
from connect.eaas.managers.base import TasksManagerBase


logger = logging.getLogger(__name__)


class ScheduledTasksManager(TasksManagerBase):

    def get_method(self, task_data, extension, argument):
        return getattr(extension, argument['method'])

    async def get_argument(self, task_data):
        return (
            await self.client('devops')
            .services[self.config.service_id]
            .environments[self.config.environment_id]
            .schedules[task_data.object_id]
            .get()
        )

    async def build_response(self, task_data, future):
        """
        Wait for a scheduled task to be completed and than uild the task result message.
        """
        result = None
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('scheduled'),
            )
            result_message.result = result.status
            result_message.output = result.output
        except Exception as e:
            self.log_exception(task_data, e)
            result_message.result = ResultType.RETRY
            result_message.output = traceback.format_exc()[:4000]

        return result_message
