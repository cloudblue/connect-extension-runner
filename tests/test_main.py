#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import signal
import sys
from collections import namedtuple

from connect.eaas.extension_runner.main import main, start
from connect.eaas.extension_runner.worker import Worker


def test_start(mocker):
    class MyExtension:

        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': {
                    'asset_purchase_request_processing': [],
                    'asset_purchase_request_validation': [],
                },
                'variables': [],
                'schedulables': [],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        def process_asset_purchase_request(self, request):
            pass

        def validate_asset_purchase_request(self, request):
            pass
    start_mock = mocker.AsyncMock()

    mocker.patch(
        'connect.eaas.extension_runner.handler.get_extension_class',
        return_value=MyExtension,
    )
    mocker.patch.object(Worker, 'start', start_mock)
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    start(parsed_args(True, extension_dir='/extension', split=False))
    start_mock.assert_awaited_once()
    signal.raise_signal(signal.SIGTERM)


def test_main(mocker):
    testargs = ['cextrun']
    mocker.patch.object(sys, 'argv', testargs)
    mocked_start = mocker.patch('connect.eaas.extension_runner.main.start')
    mocked_configure_logger = mocker.patch(
        'connect.eaas.extension_runner.main.logging.config.dictConfig',
    )
    main()
    mocked_start.assert_called_once()
    mocked_configure_logger.assert_called_once()
