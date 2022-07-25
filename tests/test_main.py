#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import signal
import sys
import threading
import time
from collections import namedtuple

from connect.eaas.main import main, start, start_worker_process
from connect.eaas.worker import Worker


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

    mocker.patch('connect.eaas.handler.get_extension_class', return_value=MyExtension)
    mocker.patch.object(Worker, 'start', start_mock)
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    start(parsed_args(True, extension_dir='/extension', split=False))
    start_mock.assert_awaited_once()
    signal.raise_signal(signal.SIGTERM)


def test_main(mocker):
    testargs = ['cextrun']
    mocker.patch.object(sys, 'argv', testargs)
    mocked_start = mocker.patch('connect.eaas.main.start')
    mocked_configure_logger = mocker.patch('connect.eaas.main.logging.config.dictConfig')
    main()
    mocked_start.assert_called_once()
    mocked_configure_logger.assert_called_once()


def test_start_split(mocker):
    mocker.patch('connect.eaas.main.PROCESS_CHECK_INTERVAL_SECS', 0.01)
    stop_event = mocker.MagicMock()
    stop_event.is_set.return_value = False
    mocker.patch('connect.eaas.main.threading.Event', return_value=stop_event)
    mocker.patch('connect.eaas.main.signal.signal')
    mocked_process_constr = mocker.patch(
        'connect.eaas.main.Process',
        side_effect=[mocker.MagicMock(), mocker.MagicMock()],
    )
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    t = threading.Thread(
        target=start,
        args=(parsed_args(True, extension_dir='/extension', split=True),),
    )
    t.start()
    time.sleep(.1)
    stop_event.is_set.return_value = True
    t.join()
    assert mocked_process_constr.mock_calls[0].kwargs == dict(
        daemon=True, target=start_worker_process, args=(True, 'interactive'),
    )
    assert mocked_process_constr.mock_calls[1].kwargs == dict(
        daemon=True, target=start_worker_process, args=(True, 'background'),
    )


def test_start_split_process_die(mocker):
    mocker.patch('connect.eaas.main.PROCESS_CHECK_INTERVAL_SECS', 0.01)
    stop_event = mocker.MagicMock()
    stop_event.is_set.return_value = False
    mocker.patch('connect.eaas.main.threading.Event', return_value=stop_event)
    mocker.patch('connect.eaas.main.signal.signal')
    mocked_notify = mocker.patch(
        'connect.eaas.main.notify_process_restarted',
    )
    mocked_process = mocker.MagicMock()
    mocked_process_constr = mocker.patch(
        'connect.eaas.main.Process',
        side_effect=[mocker.MagicMock(), mocked_process, mocker.MagicMock()],
    )
    mocked_process.is_alive.return_value = False
    mocked_process.exitcode = -9
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    t = threading.Thread(
        target=start,
        args=(parsed_args(True, extension_dir='/extension', split=True),),
    )
    t.start()
    time.sleep(.1)
    stop_event.is_set.return_value = True
    t.join()
    assert mocked_process_constr.mock_calls[0].kwargs == dict(
        daemon=True, target=start_worker_process, args=(True, 'interactive'),
    )
    assert mocked_process_constr.mock_calls[1].kwargs == dict(
        daemon=True, target=start_worker_process, args=(True, 'background'),
    )
    assert mocked_process_constr.mock_calls[2].kwargs == dict(
        daemon=True, target=start_worker_process, args=(True, 'background'),
    )
    mocked_notify.assert_called_once_with('background')
