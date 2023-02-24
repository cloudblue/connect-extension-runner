import inspect
import logging

from connect.eaas.core.models import (
    Context,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.constants import (
    EVENT_TYPE_EXT_METHOD_MAP,
)
from connect.eaas.runner.handlers.base import (
    ApplicationHandlerBase,
)


logger = logging.getLogger(__name__)


class EventsApp(ApplicationHandlerBase):
    """
    Handle the lifecycle of an extension.
    """

    LOGGER_NAME = 'eaas.eventsapp'

    def __init__(self, config: ConfigHelper):
        super().__init__(config)
        self._config = config
        self._logging_handler = None

    @property
    def events(self):
        return self.get_events()

    @property
    def schedulables(self):
        return self.get_schedulables()

    def get_features(self):
        return {
            'events': self.events,
            'schedulables': self.schedulables,
        }

    def get_method(
        self,
        event_type,
        task_id,
        method_name,
        installation=None,
        api_key=None,
        connect_correlation_id=None,
    ):
        if not method_name:  # pragma: no cover
            return
        args = (
            self._create_client(
                event_type, task_id, method_name, self._config.api_key, connect_correlation_id,
            ),
            self.get_logger(extra={'task_id': task_id}),
            self._config.variables,
        )
        kwargs = {}
        if installation:
            kwargs['installation'] = installation
            kwargs['installation_client'] = self._create_client(
                event_type,
                task_id,
                method_name,
                api_key,
                connect_correlation_id,
            )

        app_class = self.get_application()

        if 'context' in inspect.signature(app_class.__init__).parameters:
            kwargs['context'] = Context(
                extension_id=self.config.service_id,
                environment_id=self.config.environment_id,
                environment_type=self.config.environment_type,
            )

        ext = app_class(*args, **kwargs)

        return getattr(ext, method_name, None)

    def get_events(self):
        if 'capabilities' in self.get_descriptor():
            logger.warning(
                "The definition of extension's capabilities in extension.json is deprecated.",
            )
            data = {
                event_type: {
                    'method': EVENT_TYPE_EXT_METHOD_MAP[event_type],
                    'event_type': event_type,
                    'statuses': statuses,
                }
                for event_type, statuses in self.get_descriptor()['capabilities'].items()
            }
            return data

        return {
            event['event_type']: event
            for event in self.get_application().get_events()
        }

    def get_schedulables(self):
        if 'schedulables' in self.get_descriptor():
            logger.warning(
                "The definition of extension's schedulables in extension.json is deprecated.",
            )
            return self.get_descriptor()['schedulables']
        return self.get_application().get_schedulables()

    def get_variables(self):
        if 'variables' in self.get_descriptor():
            logger.warning(
                "The definition of extension's variables in extension.json is deprecated.",
            )
            return self.get_descriptor()['variables']
        return self.get_application().get_variables()

    def get_application(self):
        application = self.load_application('eventsapp')
        if not application:
            application = self.load_application('extension')
        return application
