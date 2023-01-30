#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import inspect
import json
import logging

import pkg_resources

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.core.extension import TransformationBase
from connect.eaas.runner.handlers.base import ApplicationHandlerBase


logger = logging.getLogger(__name__)


class TfnApp(ApplicationHandlerBase):
    """
    Handle the lifecycle of a Transformation extension.
    """
    def __init__(self, config: ConfigHelper):
        super().__init__(config)
        self._transformations = None

    def get_application(self):
        return self.load_application('tfnapp')

    def get_descriptor(self):  # pragma: no cover
        if application := self.get_application():
            return json.load(
                pkg_resources.resource_stream(
                    application.__name__,
                    'extension.json',
                ),
            )

    @property
    def transformations(self):
        if self._transformations is not None:
            return self._transformations

        result = []

        if application := self.get_application():
            try:
                for _, el in inspect.getmembers(application):
                    if inspect.isclass(el) and issubclass(el, TransformationBase):
                        if el.get_transformation_info():
                            result.append(el.get_transformation_info())
            except AttributeError:  # pragma: no branch
                logger.warning(f'Can not inspect {application}...')

        self._transformations = result
        return result

    def get_features(self):
        return {
            'transformations': self.transformations,
        }
