#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import json

import pkg_resources


class _ProcessResult:
    def __init__(self, status):
        self.status = status
        self.data = None

    def __call__(self, data):
        self.data = data
        return self


OK = _ProcessResult('succeeded')

SKIP = _ProcessResult('skip')


class Reschedule(_ProcessResult):
    def __init__(self, countdown=30):
        super().__init__('reschedule')
        self.countdown = countdown


class Extension:
    def __init__(self, client, logger, config):
        self.client = client
        self.logger = logger
        self.config = config

    @classmethod
    def get_descriptor(cls):
        return json.load(
            pkg_resources.resource_stream(
                cls.__module__,
                'extension.json',
            ),
        )

    def process_asset_request(self, request):  # pragma: no cover
        pass

    def validate_asset_request(self, request):  # pragma: no cover
        pass

    def process_tier_config_request(self, request):  # pragma: no cover
        pass

    def validate_tier_config_request(self, request):  # pragma: no cover
        pass
