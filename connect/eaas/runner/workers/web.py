#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import asyncio
import base64
import copy
import json
import logging
import signal

import httpx

from connect.eaas.core.proto import (
    HttpRequest,
    HttpResponse,
    Message,
    MessageType,
    SetupRequest,
    WebTask,
)
from connect.eaas.runner.helpers import (
    configure_logger,
    get_version,
)
from connect.eaas.runner.workers.base import (
    WorkerBase,
)


logger = logging.getLogger(__name__)


class WebWorker(WorkerBase):
    """
    The EventsWorker is responsible to handle the websocket connection
    with the server. It will send the extension capabilities to
    the server and wait for tasks that need to be processed using
    the tasks manager.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._httpx_app_transport = httpx.ASGITransport(app=self.handler.app)

    def get_url(self):
        return self.config.get_webapp_ws_url()

    def get_setup_request(self):
        msg = Message(
            version=2,
            message_type=MessageType.SETUP_REQUEST,
            data=SetupRequest(
                ui_modules=self.handler.ui_modules,
                variables=self.handler.variables,
                repository={
                    'readme_url': self.handler.readme,
                    'changelog_url': self.handler.changelog,
                },
                icon=self.handler.icon,
                audience=self.handler.audience,
                runner_version=get_version(),
                proxied_connect_api=self.handler.proxied_connect_api,
            ),
        )
        logger.debug(f'Sending setup request: {self.prettify(msg)}')
        return msg.dict()

    async def stopping(self):
        pass

    async def process_message(self, data):
        message = Message.deserialize(data)
        logger.debug(f'Received message: {self.prettify(message)}')
        if message.message_type == MessageType.SETUP_RESPONSE:
            await self.process_setup_response(message.data)
        elif message.message_type == MessageType.WEB_TASK:
            asyncio.create_task(self.process_task(message.data))
        elif message.message_type == MessageType.SHUTDOWN:
            await self.shutdown()

    async def shutdown(self):
        await super().shutdown()

    async def process_task(self, task):
        logger.info(f'new webtask received: {task.request.method} {task.request.url}')
        headers = copy.copy(task.request.headers)
        headers.update(self.get_internal_headers(task))
        message = None
        try:
            async with httpx.AsyncClient(
                base_url='http://localhost', transport=self._httpx_app_transport,
            ) as client:
                body = (
                    base64.decodebytes(task.request.content.encode('utf-8'))
                    if task.request.content else b''
                )
                response = await client.request(
                    task.request.method,
                    task.request.url,
                    headers=headers,
                    content=body,
                )

            message = self.build_response(
                task, response.status_code, response.headers, response.content,
            )
        except Exception as e:
            logger.exception('Cannot invoke API endpoint')
            message = self.build_response(
                task, 500, {}, str(e).encode('utf-8'),
            )
        await self.send(message)

    def get_logging_level(self):
        if self.config.logging_level:
            return self.config.logging_level
        current_level = logging.getLogger('eaas').getEffectiveLevel()
        return logging.getLevelName(current_level)

    def get_internal_headers(self, task):
        headers = {}
        headers['X-Connect-Api-Gateway-Url'] = self.config.get_api_url()
        headers['X-Connect-User-Agent'] = self.config.get_user_agent()['User-Agent']
        headers['X-Connect-Extension-Id'] = self.config.service_id
        headers['X-Connect-Environment-Id'] = self.config.environment_id
        headers['X-Connect-Environment-Type'] = self.config.environment_type
        headers['X-Connect-Logging-Level'] = self.get_logging_level()
        headers['X-Connect-Config'] = json.dumps(self.config.variables)

        if task.options.api_key:
            headers['X-Connect-Installation-Api-Key'] = task.options.api_key

        if task.options.installation_id:
            headers['X-Connect-Installation-Id'] = task.options.installation_id

        if task.options.tier_account_id:
            headers['X-Connect-Tier-Account-Id'] = task.options.tier_account_id

        if task.options.connect_correlation_id:
            headers['X-Connect-Correlation-Id'] = task.options.connect_correlation_id

        if task.options.user_id:
            headers['X-Connect-User-Id'] = task.options.user_id

        if task.options.account_id:
            headers['X-Connect-Account-Id'] = task.options.account_id

        if task.options.account_role:
            headers['X-Connect-Account-Role'] = task.options.account_role

        if task.options.call_type:
            headers['X-Connect-Call-Type'] = task.options.call_type

        if task.options.call_source:
            headers['X-Connect-Call-Source'] = task.options.call_source

        if self.config.logging_api_key is not None:
            headers['X-Connect-Logging-Api-Key'] = self.config.logging_api_key
            headers['X-Connect-Logging-Metadata'] = json.dumps(self.config.metadata)

        return headers

    def build_response(self, task, status, headers, body):
        log = logger.info if status < 500 else logger.error
        log(
            f'{task.request.method.upper()} {task.request.url} {status} - {len(body)}',
        )
        task_response = WebTask(
            options=task.options,
            request=HttpRequest(
                method=task.request.method.upper(),
                url=task.request.url,
                headers={},
            ),
            response=HttpResponse(
                status=status,
                headers=headers,
                content=base64.encodebytes(body).decode('utf-8'),
            ),
        )
        message = Message(
            version=2,
            message_type=MessageType.WEB_TASK,
            data=task_response,
        )
        logger.debug(f'Sending message: {self.prettify(message)}')
        return message.serialize()


def start_webapp_worker_process(
    handler_class,
    config,
    lifecycle_lock,
    on_startup_fired,
    on_shutdown_fired,
    debug,
    no_rich,
):
    handler = handler_class(config)
    configure_logger(debug, no_rich)
    worker = WebWorker(
        handler,
        lifecycle_lock,
        on_startup_fired,
        on_shutdown_fired,
    )
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT,
        worker.handle_signal,
    )
    loop.add_signal_handler(
        signal.SIGTERM,
        worker.handle_signal,
    )
    loop.run_until_complete(worker.start())
