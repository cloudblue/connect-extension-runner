#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import logging
import traceback

from connect.eaas.constants import (
    ASSET_REQUEST_TASK_TYPES,
    LISTING_REQUEST_TASK_TYPES,
    TASK_TYPE_EXT_METHOD_MAP,
    TIER_CONFIG_REQUEST_TASK_TYPES,
)
from connect.eaas.dataclasses import (
    ResultType,
    TaskPayload,
    TaskType,
)
from connect.client import R
from connect.eaas.extension import ProcessingResponse
from connect.eaas.managers.base import TasksManagerBase


logger = logging.getLogger(__name__)


class BackgroundTasksManager(TasksManagerBase):

    async def _accept_listing_request(self, task_data, listing_request):
        if self.config.hub_id is None:
            return True
        marketplace_id = listing_request['listing']['contract']['marketplace']['id']
        marketplace = await self.client.marketplaces[marketplace_id].get()
        hubs = [
            marketplace_hub['hub']['id']
            for marketplace_hub in marketplace['hubs']
        ]
        if self.config.hub_id not in hubs:
            self.send_skip_response(
                task_data,
                f'The marketplace {marketplace_id} does not belong '
                f'to hub {self.config.hub_id}.',
            )
            return False

        return True

    async def _accept_tier_account_request(self, task_data, tar):
        tier_account_id = tar['account']['id']
        product_id = tar['product']['id']
        connection_type = (
            'preview' if self.config.environment_type == 'development'
            else self.config.environment_type
        )
        tiers_filter = (
            R().tiers.tier2.id.eq(tier_account_id)
            | R().tiers.tier1.id.eq(tier_account_id)
            | R().tiers.customer.id.eq(tier_account_id)
        )
        query = (
            R().product.id.eq(product_id)
            & R().connection.type.eq(connection_type)
            & tiers_filter
        )
        if await self.client.assets.filter(query).count() == 0:
            self.send_skip_response(
                task_data,
                'The Tier Account related to this request does not '
                f'have assets with a {connection_type} connection.',
            )
            return False

        return True

    def get_method(self, task_data, extension, argument):
        method_name = TASK_TYPE_EXT_METHOD_MAP[task_data.task_type]
        return getattr(extension, method_name)

    async def get_argument(self, task_data):
        """
        Get the request object through the Connect public API
        related to the task that need processing.
        """
        argument = None
        if task_data.task_type in ASSET_REQUEST_TASK_TYPES:
            logger.info(f'get asset request {task_data.object_id}')
            argument = await self.client.requests[task_data.object_id].get()
        if task_data.task_type in TIER_CONFIG_REQUEST_TASK_TYPES:
            logger.info(f'get TC request {task_data.object_id}')
            argument = await self.client('tier').config_requests[task_data.object_id].get()
        if task_data.task_type in LISTING_REQUEST_TASK_TYPES:
            argument = await self.client.listing_requests[task_data.object_id].get()
            if not await self._accept_listing_request(task_data, argument):
                return
        if task_data.task_type == TaskType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING:
            argument = await self.client('tier').account_requests[task_data.object_id].get()
            if not await self._accept_tier_account_request(task_data, argument):
                return
        if task_data.task_type == TaskType.USAGE_FILE_REQUEST_PROCESSING:
            argument = await self.client('usage').files[task_data.object_id].get()
        if task_data.task_type == TaskType.PART_USAGE_FILE_REQUEST_PROCESSING:
            argument = await self.client('usage').chunks[task_data.object_id].get()
        status = argument.get('status', argument.get('state'))
        if status not in self.handler.capabilities[task_data.task_type]:
            logger.info('Send skip response since request status is not supported.')
            self.send_skip_response(
                task_data,
                f'The status {status} is not supported by the extension.',
            )
            return
        return argument

    async def build_response(self, task_data, future):
        """
        Wait for a background task to be completed and than uild the task result message.
        """
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        result = None
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('background'),
            )
            result_message.result = result.status
            logger.info(f'task {task_data.task_id} result: {result.status}')
            if result.status in (ResultType.SKIP, ResultType.FAIL):
                result_message.output = result.output

            if result.status == ResultType.RESCHEDULE:
                result_message.countdown = result.countdown
        except Exception as e:
            self.log_exception(task_data, e)
            result_message.result = ResultType.RETRY
            result_message.output = traceback.format_exc()[:4000]

        return result_message

    def send_skip_response(self, data, output):
        future = asyncio.Future()
        future.set_result(ProcessingResponse.skip(output))
        asyncio.create_task(self.enqueue_result(data, future))
