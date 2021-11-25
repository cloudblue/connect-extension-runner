#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import logging
import platform

from connect.eaas.helpers import (
    get_environment,
    get_version,
)


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
        return self.dyn_config.service_id

    @property
    def product_id(self):
        return self.dyn_config.product_id

    @property
    def hub_id(self):
        return self.dyn_config.hub_id

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
        return self.dyn_config.account_id

    @property
    def account_name(self):
        return self.dyn_config.account_name

    @property
    def logging_api_key(self):
        return self.dyn_config.logging_api_key

    @property
    def variables(self):
        return self.dyn_config.configuration

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

    def get_ws_url(self):
        proto = 'wss' if self.secure else 'ws'
        return (
            f'{proto}://{self.env["ws_address"]}/public/v1/devops/ws'
            f'/{self.env["environment_id"]}/{self.env["instance_id"]}'
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
        if not self.dyn_config:
            self.dyn_config = data
        else:
            self.dyn_config.service_id = data.service_id or self.dyn_config.service_id
            self.dyn_config.product_id = data.product_id or self.dyn_config.product_id
            self.dyn_config.hub_id = data.hub_id or self.dyn_config.hub_id
            self.dyn_config.environment_type = (
                data.environment_type or self.dyn_config.environment_type
            )
            self.dyn_config.account_id = data.account_id or self.dyn_config.account_id
            self.dyn_config.account_name = (
                data.account_name or self.dyn_config.account_name
            )
            self.dyn_config.configuration = data.configuration or self.dyn_config.configuration
            self.dyn_config.logging_api_key = (
                data.logging_api_key or self.dyn_config.logging_api_key
            )

        logger.info(f'Runner dynamic config updated {data}')
        if data.log_level:
            logger.info(f'Change extesion logger level to {data.log_level}')
            logging.getLogger('eaas.extension').setLevel(
                getattr(logging, data.log_level),
            )
        if data.runner_log_level:
            logging.getLogger('connect.eaas').setLevel(
                getattr(logging, data.runner_log_level),
            )
