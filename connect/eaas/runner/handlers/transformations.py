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
from connect.eaas.runner.helpers import iter_entry_points


logger = logging.getLogger(__name__)


class TfnApp:
    """
    Handle the lifecycle of a Transformation extension.
    """
    def __init__(self, config: ConfigHelper):
        self._config = config
        self._logging_handler = None

        self._transformations = None
        self._descriptor = None

    def get_tfn_module(self):
        tfn_module = next(iter_entry_points('connect.eaas.ext', 'tfnapp'), None)
        return tfn_module.load() if tfn_module else None

    def get_descriptor(self):  # pragma: no cover
        if not self._descriptor:
            tfn_module = self.get_tfn_module()
            if tfn_module:
                self._descriptor = json.load(
                    pkg_resources.resource_stream(
                        tfn_module.__name__,
                        'extension.json',
                    ),
                )

        return self._descriptor

    @property
    def should_start(self):
        return self.get_tfn_module() is not None

    @property
    def config(self):
        return self._config

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
    def transformations(self):
        if self._transformations is not None:
            return self._transformations

        result = []

        tfn_module = self.get_tfn_module()
        if tfn_module:
            try:
                for _, el in inspect.getmembers(tfn_module):
                    if inspect.isclass(el) and issubclass(el, TransformationBase):
                        if el.get_transformation_info():
                            result.append(el.get_transformation_info())
            except AttributeError:  # pragma: no branch
                logger.warning(f'Can not inspect {tfn_module}...')

        self._transformations = result
        return result

    @property
    def features(self):
        return {
            'transformations': self.transformations,
        }
