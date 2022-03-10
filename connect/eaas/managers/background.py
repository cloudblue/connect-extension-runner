#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import asyncio
import dataclasses
import logging
import time
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

    def get_method(self, task_data, extension, argument):
        method_name = TASK_TYPE_EXT_METHOD_MAP[task_data.task_type]
        return getattr(extension, method_name, None)

    async def get_argument(self, task_data):
        """
        Get the request object through the Connect public API
        related to the task that need processing.
        """
        if task_data.task_type in ASSET_REQUEST_TASK_TYPES:
            return await self._get_asset_request(task_data)
        if task_data.task_type in TIER_CONFIG_REQUEST_TASK_TYPES:
            return await self._get_tier_config_request(task_data)
        if task_data.task_type in LISTING_REQUEST_TASK_TYPES:
            return await self._get_listing_request(task_data)
        if task_data.task_type == TaskType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING:
            return await self._get_tier_account_request(task_data)
        if task_data.task_type == TaskType.USAGE_FILE_REQUEST_PROCESSING:
            return await self._get_usage_file(task_data)
        if task_data.task_type == TaskType.PART_USAGE_FILE_REQUEST_PROCESSING:
            return await self._get_usage_file_chunk(task_data)

    async def build_response(self, task_data, future):
        """
        Wait for a background task to be completed and than uild the task result message.
        """
        result_message = TaskPayload(**dataclasses.asdict(task_data))
        result = None
        try:
            begin_ts = time.monotonic()
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('background'),
            )
            result_message.result = result.status
            result_message.runtime = time.monotonic() - begin_ts
            logger.info(
                f'background task {task_data.task_id} result: {result.status}, tooks:'
                f' {result_message.runtime}',
            )
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

    async def _status_has_changed(self, task_data, namespace, collection, status_field):
        supported_statuses = self.handler.capabilities[task_data.task_type]
        client = self.client
        if namespace:
            client = client.ns(namespace)
        client = client.collection(collection)
        filter_expr = {
            'id': task_data.object_id,
            f'{status_field}__in': supported_statuses,
        }
        if await client.filter(**filter_expr).count() == 0:
            logger.info(
                f'Send skip response for {task_data.task_id} since '
                'the current request status is not supported.',
            )
            self.send_skip_response(
                task_data,
                'The request status does not match the '
                f'supported statuses: {",".join(supported_statuses)}.',
            )
            return True

        return False

    async def _get_asset_request(self, task_data):
        if await self._status_has_changed(task_data, None, 'requests', 'status'):
            return

        return await self.client.requests[task_data.object_id].get()

    async def _get_tier_config_request(self, task_data):
        if await self._status_has_changed(task_data, 'tier', 'config-requests', 'status'):
            return

        return await self.client('tier').config_requests[task_data.object_id].get()

    async def _get_listing_request(self, task_data):

        if await self._status_has_changed(task_data, None, 'listing-requests', 'state'):
            return

        listing_request = await self.client.listing_requests[task_data.object_id].get()

        if self.config.hub_id is None:
            return listing_request

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
            return

        return listing_request

    async def _get_tier_account_request(self, task_data):
        if await self._status_has_changed(task_data, 'tier', 'account-requests', 'status'):
            return

        tar = await self.client('tier').account_requests[task_data.object_id].get()
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
            return

        return tar

    async def _get_usage_file(self, task_data):
        if await self._status_has_changed(task_data, 'usage', 'files', 'status'):
            return

        return await self.client('usage').files[task_data.object_id].get()

    async def _get_usage_file_chunk(self, task_data):
        if await self._status_has_changed(task_data, 'usage', 'chunks', 'status'):
            return

        return await self.client('usage').chunks[task_data.object_id].get()
