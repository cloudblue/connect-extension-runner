#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import logging

from connect.eaas.runner.config import ConfigHelper
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

    @property
    def transformations(self):
        return self.get_application().get_transformations()

    def get_features(self):
        return {
            'transformations': self.transformations,
        }
