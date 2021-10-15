import logging

from connect.client import AsyncConnectClient, ConnectClient
from connect.eaas.helpers import (
    get_extension_class,
    get_extension_type,
)
from connect.eaas.config import ConfigHelper
from connect.eaas.logging import ExtensionLogHandler, RequestLogger


class ExtensionHandler:
    """
    Handle the lifecycle of an extension.
    """
    def __init__(self, config: ConfigHelper):
        self.config = config
        self.extension_class = get_extension_class()
        self.extension_type = get_extension_type(self.extension_class)
        self.descriptor = self.extension_class.get_descriptor()
        self.logging_handler = None

    @property
    def capabilities(self):
        return self.descriptor['capabilities']

    @property
    def variables(self):
        return self.descriptor.get('variables')

    @property
    def schedulables(self):
        return self.descriptor.get('schedulables')

    @property
    def readme(self):
        return self.descriptor['readme_url']

    @property
    def changelog(self):
        return self.descriptor['changelog_url']

    def new_extension(self, task_id):
        return self.extension_class(
            self._create_client(task_id),
            logging.LoggerAdapter(
                self._create_logger(),
                {'task_id': task_id},
            ),
            self.config.variables,
        )

    def _create_logger(self):
        """
        Returns a logger instance configured with the LogZ.io handler.
        This logger will be used by the extension to send logging records
        to the Logz.io service.
        """
        logger = logging.getLogger('eaas.extension')
        if self.logging_handler is None and self.config.logging_api_key is not None:
            self.logging_handler = ExtensionLogHandler(
                self.config.logging_api_key,
                default_extra_fields=self.config.metadata,
            )
            logger.addHandler(self.logging_handler)
        return logger

    def _create_client(self, task_id):
        """
        Get an instance of the Connect Openapi Client. If the extension is asyncrhonous
        it returns an instance of the AsyncConnectClient otherwise the ConnectClient.
        """
        Client = ConnectClient if self.extension_type == 'sync' else AsyncConnectClient
        return Client(
            self.config.api_key,
            endpoint=self.config.get_api_url(),
            use_specs=False,
            logger=RequestLogger(
                logging.LoggerAdapter(
                    self._create_logger(),
                    {'task_id': task_id},
                ),
            ),
        )
