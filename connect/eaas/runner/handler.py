import inspect
import logging

from pkg_resources import iter_entry_points

from connect.client import AsyncConnectClient, ConnectClient
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.constants import EVENT_TYPE_EXT_METHOD_MAP
from connect.eaas.runner.logging import ExtensionLogHandler, RequestLogger


logger = logging.getLogger(__name__)


class ExtensionHandler:
    """
    Handle the lifecycle of an extension.
    """
    def __init__(self, config: ConfigHelper):
        self._config = config
        self._extension_class = self.get_extension_class()
        if self._extension_class:
            self._descriptor = self._extension_class.get_descriptor()
            self._events = self.get_events()
            self._schedulables = self.get_schedulables()
            self._variables = self.get_variables()
        self._logging_handler = None

    @property
    def should_start(self):
        return self._extension_class is not None

    @property
    def events(self):
        return self._events

    @property
    def variables(self):
        return self._variables

    @property
    def schedulables(self):
        return self._schedulables

    @property
    def readme(self):
        return self._descriptor['readme_url']

    @property
    def changelog(self):
        return self._descriptor['changelog_url']

    def get_method(self, task_id, method_name):
        if not method_name:  # pragma: no cover
            return
        ext = self._extension_class(
            self._create_client(task_id, method_name),
            self.get_logger(task_id),
            self._config.variables,
        )
        return getattr(ext, method_name, None)

    def get_logger(self, task_id):
        """
        Returns a logger instance configured with the LogZ.io handler.
        This logger will be used by the extension to send logging records
        to the Logz.io service.
        """
        logger = logging.getLogger('eaas.extension')
        if self._logging_handler is None and self._config.logging_api_key is not None:
            self._logging_handler = ExtensionLogHandler(
                self._config.logging_api_key,
                default_extra_fields=self._config.metadata,
            )
            logger.addHandler(self._logging_handler)
        return logging.LoggerAdapter(
            logger,
            {'task_id': task_id},
        )

    def _create_client(self, task_id, method_name):
        """
        Get an instance of the Connect Openapi Client. If the extension is asyncrhonous
        it returns an instance of the AsyncConnectClient otherwise the ConnectClient.
        """
        method = getattr(self._extension_class, method_name)

        Client = ConnectClient if not inspect.iscoroutinefunction(method) else AsyncConnectClient

        return Client(
            self._config.api_key,
            endpoint=self._config.get_api_url(),
            use_specs=False,
            logger=RequestLogger(
                self.get_logger(task_id),
            ),
            default_headers=self._config.get_user_agent(),
        )

    def get_events(self):
        if 'capabilities' in self._descriptor:
            logger.warning(
                "The definition of extension's capabilities in extension.json is deprecated.",
            )
            data = {
                event_type: {
                    'method': EVENT_TYPE_EXT_METHOD_MAP[event_type],
                    'event_type': event_type,
                    'statuses': statuses,
                }
                for event_type, statuses in self._descriptor['capabilities'].items()
            }
            return data

        return {
            event['event_type']: event
            for event in self._extension_class.get_events()
        }

    def get_schedulables(self):
        if 'schedulables' in self._descriptor:
            logger.warning(
                "The definition of extension's schedulables in extension.json is deprecated.",
            )
            return self._descriptor['schedulables']
        return self._extension_class.get_schedulables()

    def get_variables(self):
        if 'variables' in self._descriptor:
            logger.warning(
                "The definition of extension's variables in extension.json is deprecated.",
            )
            return self._descriptor['variables']
        return self._extension_class.get_variables()

    def get_extension_class(self):
        ext_class = next(iter_entry_points('connect.eaas.ext', 'extension'), None)
        return ext_class.load() if ext_class else None
