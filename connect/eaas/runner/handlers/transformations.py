#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import inspect
import logging

from connect.eaas.core.models import (
    Context,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.base import (
    ApplicationHandlerBase,
)


logger = logging.getLogger(__name__)


class TfnApp(ApplicationHandlerBase):
    """
    Handle the lifecycle of a Transformation extension.
    """
    LOGGER_NAME = 'eaas.tfnapp'

    def __init__(self, config: ConfigHelper):
        super().__init__(config)
        self._config = config
        self._transformations = None

    def get_application(self):
        return self.load_application('tfnapp')

    @property
    def transformations(self):
        return self.get_application().get_transformations()

    def get_features(self):
        return {
            'transformations': self.transformations,
        }

    def get_method(
            self,
            event_type,
            task_id,
            method_name,
            transformation_request=None,
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
        if transformation_request:
            kwargs['transformation_request'] = transformation_request

        app_class = self.get_application()

        if 'context' in inspect.signature(app_class.__init__).parameters:
            kwargs['context'] = Context(
                extension_id=self.config.service_id,
                environment_id=self.config.environment_id,
                environment_type=self.config.environment_type,
            )

        ext = app_class(*args, **kwargs)

        return getattr(ext, method_name, None)
