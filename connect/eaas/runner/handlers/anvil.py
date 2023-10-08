#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import logging

import anvil.server

from connect.client import (
    ConnectClient,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.base import (
    ApplicationHandlerBase,
)
from connect.eaas.runner.logging import (
    RequestLogger,
)


logger = logging.getLogger(__name__)


class AnvilApp(ApplicationHandlerBase):
    """
    Handle the lifecycle of an Anvil extension.
    """

    LOGGER_NAME = 'eaas.anvilapp'

    def __init__(self, config: ConfigHelper):
        super().__init__(config)
        self._anvilapp_instance = None
        self._logging_handler = None

    def get_application(self):
        return self.load_application('anvilapp')

    def get_descriptor(self):
        if application := self.get_application():
            return application.get_descriptor()

    def get_features(self):
        return {
            'callables': self.callables,
        }

    def get_variables(self):
        if application := self.get_application():
            return application.get_variables()

    @property
    def callables(self):
        if application := self.get_application():
            return application.get_anvil_callables()

    def start(self):
        logger.info('Create anvil connection...')
        var_name = self.get_application().get_anvil_key_variable()
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

    def setup_anvilapp(self):
        if not self._anvilapp_instance:
            self._anvilapp_instance = self.get_application()(
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
