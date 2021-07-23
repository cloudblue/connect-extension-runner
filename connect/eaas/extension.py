#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import json

import pkg_resources

from connect.eaas.dataclasses import ResultType


class _Response:
    def __init__(self, status):
        self.status = status


class ProcessingResponse(_Response):

    def __init__(self, status, countdown=30, output=None):
        super().__init__(status)
        self.countdown = 30 if countdown < 30 else countdown
        self.output = output

    @classmethod
    def done(cls):
        return cls(ResultType.SUCCESS)

    @classmethod
    def skip(cls, output=None):
        return cls(ResultType.SKIP, output=output)

    @classmethod
    def reschedule(cls, countdown=30):
        return cls(ResultType.RESCHEDULE, countdown=countdown)

    @classmethod
    def slow_process_reschedule(cls, countdown=300):
        return cls(
            ResultType.RESCHEDULE,
            countdown=300 if countdown < 300 else countdown,
        )

    @classmethod
    def fail(cls, output=None):
        return cls(ResultType.FAIL, output=output)


class ValidationResponse(_Response):
    def __init__(self, status, data, output=None):
        super().__init__(status)
        self.data = data
        self.output = output

    @classmethod
    def done(cls, data):
        return cls(ResultType.SUCCESS, data)

    @classmethod
    def fail(cls, data=None, output=None):
        return cls(ResultType.FAIL, data=data, output=output)


class _InteractiveTaskResponse(_Response):
    def __init__(self, status, http_status, headers, body, output):
        super().__init__(status)
        self.http_status = http_status
        self.headers = headers
        self.body = body
        self.output = output

    @property
    def data(self):
        return {
            'http_status': self.http_status,
            'headers': self.headers,
            'body': self.body,
        }

    @classmethod
    def done(cls, http_status=200, headers=None, body=None):
        return cls(ResultType.SUCCESS, http_status, headers, body, None)

    @classmethod
    def fail(cls, http_status=400, headers=None, body=None, output=None):
        return cls(ResultType.FAIL, http_status, headers, body, output)


class CustomEventResponse(_InteractiveTaskResponse):
    pass


class ProductActionResponse(_InteractiveTaskResponse):
    pass


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

    def process_asset_purchase_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_asset_change_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_asset_suspend_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_asset_resume_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_asset_cancel_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_asset_adjustment_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def validate_asset_purchase_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def validate_asset_change_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_tier_config_setup_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_tier_config_change_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_tier_config_adjustment_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def validate_tier_config_setup_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def validate_tier_config_change_request(self, request):  # pragma: no cover
        raise NotImplementedError()

    def execute_product_action(self, request):  # pragma: no cover
        raise NotImplementedError()

    def process_product_custom_event(self, request):  # pragma: no cover
        raise NotImplementedError()
