#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import logging
import platform

from connect.eaas.runner.helpers import (
    get_environment,
    get_version,
)
from connect.eaas.core.proto import Logging, LogMeta

logger = logging.getLogger(__name__)


class ConfigHelper:
    """
    Provides both static (from env vars) and dynamic (from backend)
    configuration.
    """
    def __init__(self, secure=True):
        self.secure = secure
        self.env = get_environment()
        self.dyn_config = None

    @property
    def api_key(self):
        return self.env['api_key']

    @property
    def service_id(self):
        return self.dyn_config.logging.meta.service_id

    @property
    def products(self):
        return self.dyn_config.logging.meta.products

    @property
    def hub_id(self):
        return self.dyn_config.logging.meta.hub_id

    @property
    def environment_id(self):
        return self.env['environment_id']

    @property
    def instance_id(self):
        return self.env['instance_id']

    @property
    def environment_type(self):
        return self.dyn_config.environment_type

    @property
    def account_id(self):
        return self.dyn_config.logging.meta.account_id

    @property
    def account_name(self):
        return self.dyn_config.logging.meta.account_name

    @property
    def logging_api_key(self):
        return self.dyn_config.logging.logging_api_key if self.dyn_config else None

    @property
    def logging_level(self):
        return self.dyn_config.logging.log_level

    @property
    def variables(self):
        return self.dyn_config.variables if self.dyn_config else {}

    @property
    def metadata(self):
        return {
            'api_address': self.env['api_address'],
            'service_id': self.service_id,
            'environment_id': self.environment_id,
            'environment_type': self.environment_type,
            'instance_id': self.instance_id,
            'account_id': self.account_id,
            'account_name': self.account_name,
        }

    @property
    def event_definitions(self):
        return {
            definition.event_type: definition
            for definition in (self.dyn_config.event_definitions or [])
        }

    def get_events_ws_url(self):
        proto = 'wss' if self.secure else 'ws'
        return (
            f'{proto}://{self.env["ws_address"]}/public/v1/devops/ws'
            f'/{self.env["environment_id"]}/{self.env["instance_id"]}'
        )

    def get_webapp_ws_url(self):
        proto = 'wss' if self.secure else 'ws'
        return (
            f'{proto}://{self.env["ws_address"]}/public/v1/devops/ws'
            f'/{self.env["environment_id"]}/{self.env["instance_id"]}/webapp'
        )

    def get_anvilapp_ws_url(self):
        proto = 'wss' if self.secure else 'ws'
        return (
            f'{proto}://{self.env["ws_address"]}/public/v1/devops/ws'
            f'/{self.env["environment_id"]}/{self.env["instance_id"]}/anvilapp'
        )

    def get_api_url(self):
        return f'https://{self.env["api_address"]}/public/v1'

    def get_user_agent(self):
        version = get_version()
        pimpl = platform.python_implementation()
        pver = platform.python_version()
        sysname = platform.system()
        sysver = platform.release()
        ua = (
            f'connect-extension-runner/{version} {pimpl}/{pver} {sysname}/{sysver}'
            f' {self.env["environment_id"]}/{self.env["instance_id"]}'
        )
        return {'User-Agent': ua}

    def get_headers(self):
        return (
            ('Authorization', self.api_key),
        )

    def get_timeout(self, category):
        return self.env[f'{category}_task_max_execution_time']

    def update_dynamic_config(self, data):
        """Updates the dynamic configuration."""
        if data.logging is None:
            data.logging = Logging()

        if data.logging.meta is None:
            data.logging.meta = LogMeta()

        if not self.dyn_config:
            self.dyn_config = data
        else:
            self.dyn_config.logging.meta.service_id = (
                data.logging.meta.service_id or self.dyn_config.logging.meta.service_id
            )
            self.dyn_config.logging.meta.products = (
                data.logging.meta.products or self.dyn_config.logging.meta.products
            )
            self.dyn_config.logging.meta.hub_id = (
                data.logging.meta.hub_id or self.dyn_config.logging.meta.hub_id
            )
            self.dyn_config.environment_type = (
                data.environment_type or self.dyn_config.environment_type
            )
            self.dyn_config.logging.meta.account_id = (
                data.logging.meta.account_id or self.dyn_config.logging.meta.account_id
            )
            self.dyn_config.logging.meta.account_name = (
                data.logging.meta.account_name or self.dyn_config.logging.meta.account_name
            )
            self.dyn_config.variables = data.variables or self.dyn_config.variables
            self.dyn_config.logging.logging_api_key = (
                data.logging.logging_api_key or self.dyn_config.logging.logging_api_key
            )

            self.dyn_config.logging.logging_api_key = (
                data.logging.logging_api_key or self.dyn_config.logging.logging_api_key
            )

            self.dyn_config.logging.log_level = (
                data.logging.log_level or self.dyn_config.logging.log_level
            )

        logger.debug(f'Runner dynamic config updated {data}')
        if data.logging.log_level:
            logger.info(f'Change extension logger level to {data.logging.log_level}')
            logging.getLogger('eaas.eventsapp').setLevel(
                getattr(logging, data.logging.log_level),
            )
        if data.logging.runner_log_level:
            logging.getLogger('connect.eaas').setLevel(
                getattr(logging, data.logging.runner_log_level),
            )
