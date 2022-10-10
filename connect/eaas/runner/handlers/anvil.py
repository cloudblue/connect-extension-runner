#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import logging

import anvil.server

from connect.client import ConnectClient
from connect.eaas.core.logging import ExtensionLogHandler, RequestLogger
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.helpers import iter_entry_points


logger = logging.getLogger(__name__)


class AnvilApp:
    """
    Handle the lifecycle of an Anvil extension.
    """
    def __init__(self, config: ConfigHelper):
        self._config = config
        self._anvilapp_class = self.get_anvilapp_class()
        self._anvilapp_instance = None
        self._logging_handler = None

    @property
    def config(self):
        return self._config

    @property
    def should_start(self):
        return self._anvilapp_class is not None

    @property
    def variables(self):
        return self._anvilapp_class.get_variables()

    @property
    def callables(self):
        return self._anvilapp_class.get_anvil_callables()

    @property
    def readme(self):
        return self._anvilapp_class.get_descriptor()['readme_url']

    @property
    def changelog(self):
        return self._anvilapp_class.get_descriptor()['changelog_url']

    @property
    def features(self):
        return {
            'callables': self._anvilapp_class.get_anvil_callables(),
        }

    def start(self):
        logger.info('Create anvil connection...')
        var_name = self._anvilapp_class.get_anvil_key_variable()
        logger.info(f'Anvil key variable name: {var_name}')
        anvil_api_key = self._config.variables.get(var_name)
        if not anvil_api_key:
            logger.error(f'Cannot start Anvil application: variable {var_name} not found!')
            return

        logger.info('Starting anvil server...')
        anvil.server.connect(anvil_api_key)
        logger.info('Anvil server started successfully.')
        self.setup_anvilapp()

    def stop(self):
        logger.info('Stopping anvil server...')
        anvil.server.disconnect()
        logger.info('Anvil server stopped successfully.')

    def get_anvilapp_class(self):
        anvil_class = next(iter_entry_points('connect.eaas.ext', 'anvilapp'), None)
        return anvil_class.load() if anvil_class else None

    def setup_anvilapp(self):
        if not self._anvilapp_instance:
            self._anvilapp_instance = self._anvilapp_class(
                self.get_client(),
                self.get_logger(),
                self._config.variables,
            )
            self._anvilapp_instance.setup_anvil_callables()

    def get_client(self):
        return ConnectClient(
            self._config.api_key,
            endpoint=self._config.get_api_url(),
            use_specs=False,
            max_retries=3,
            default_headers=self._config.get_user_agent(),
            logger=RequestLogger(
                self.get_logger(),
            ),
        )

    def get_logger(self):
        """
        Returns a logger instance configured with the LogZ.io handler.
        This logger will be used by the extension to send logging records
        to the Logz.io service.
        """
        logger = logging.getLogger('eaas.anvilapp')
        if self._logging_handler is None and self._config.logging_api_key is not None:
            self._logging_handler = ExtensionLogHandler(
                self._config.logging_api_key,
                default_extra_fields=self._config.metadata,
            )
            logger.addHandler(self._logging_handler)
        return logger
