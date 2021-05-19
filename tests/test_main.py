#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import signal
from collections import namedtuple

from connect.eaas.main import main, start
from connect.eaas.workers import ControlWorker


def test_start(mocker):
    class MyExtension:

        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': {
                    'process_asset_request': [],
                    'validate_asset_request': [],
                },
            }

        def process_asset_request(self, request):
            pass

        def validate_asset_request(self, request):
            pass
    run_mock = mocker.AsyncMock()
    mocker.patch('connect.eaas.main.install_extension')
    mocker.patch('connect.eaas.workers.get_extension_class', return_value=MyExtension)
    mocker.patch.object(ControlWorker, 'run', run_mock)
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir'))
    start(parsed_args(True, extension_dir='/extension'))
    run_mock.assert_awaited_once()
    signal.raise_signal(signal.SIGTERM)


def test_main(mocker):
    mocked_start = mocker.patch('connect.eaas.main.start')
    mocked_configure_logger = mocker.patch('connect.eaas.main.configure_logger')
    main()
    mocked_start.assert_called_once()
    mocked_configure_logger.assert_called_once()
