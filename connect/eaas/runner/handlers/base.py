import importlib
import inspect
import logging
import sys
from abc import ABC, abstractmethod

from connect.eaas.core.logging import ExtensionLogHandler
from connect.eaas.runner.helpers import iter_entry_points


class ApplicationHandlerBase(ABC):
    def __init__(self, config):
        self._config = config
        self._logging_handler = None

    @property
    def config(self):
        return self._config

    @property
    def should_start(self):
        return self.get_application() is not None

    @property
    def name(self):
        return self.get_descriptor().get('name', 'Unnamed API')

    @property
    def description(self):
        return self.get_descriptor().get('description', '')

    @property
    def version(self):
        return self.get_descriptor().get('version', '0.0.0')

    @property
    def readme(self):
        return self.get_descriptor()['readme_url']

    @property
    def changelog(self):
        return self.get_descriptor()['changelog_url']

    @property
    def audience(self):
        return self.get_descriptor().get('audience')

    @property
    def icon(self):
        return self.get_descriptor().get('icon')

    @property
    def features(self):
        return self.get_features()

    @property
    def variables(self):
        return self.get_variables()

    def load_application(self, name):
        ep = next(
            iter_entry_points('connect.eaas.ext', name),
            None,
        )
        return ep.load() if ep else None

    def reload(self):
        application = self.get_application()
        if not application:
            return
        if inspect.isclass(application):
            importlib.reload(sys.modules[application.__module__])
        elif inspect.ismodule(application):
            importlib.reload(application)
        else:
            raise RuntimeError('Invalid application object.')

    def get_descriptor(self):
        if application := self.get_application():
            return application.get_descriptor()

    def get_variables(self):
        if application := self.get_application():
            return application.get_variables()

    def get_logger(self, extra=None):
        """
        Returns a logger instance configured with the LogZ.io handler.
        This logger will be used by the extension to send logging records
        to the Logz.io service.
        """
        logger = logging.getLogger(self.LOGGER_NAME)
        if self._logging_handler is None and self._config.logging_api_key is not None:
            self._logging_handler = ExtensionLogHandler(
                self._config.logging_api_key,
                default_extra_fields=self._config.metadata,
            )
            logger.addHandler(self._logging_handler)
        return logging.LoggerAdapter(
            logger,
            extra=extra,
        )

    @abstractmethod
    def get_application(self):
        raise NotImplementedError()

    @abstractmethod
    def get_features(self):
        raise NotImplementedError()
