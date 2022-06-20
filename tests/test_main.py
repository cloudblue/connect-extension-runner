#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import signal
import sys
from collections import namedtuple

from connect.eaas.runner.main import (
    main,
    start,
    start_anvilapp_worker,
    start_anvilapp_worker_process,
    start_event_worker,
    start_event_worker_process,
    start_webapp_worker,
    start_webapp_worker_process,
)
from connect.eaas.runner.handlers.anvilapp import AnvilApp
from connect.eaas.runner.handlers.events import ExtensionHandler
from connect.eaas.runner.handlers.webapp import WebApp
from connect.eaas.runner.workers.anvilapp import AnvilWorker
from connect.eaas.runner.workers.webapp import WebWorker
from connect.eaas.runner.workers.events import Worker


def test_start(mocker):
    mocked_event = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.threading.Event', return_value=mocked_event)

    mocked_signal = mocker.patch('connect.eaas.runner.main.signal.signal')
    mocked_start_event_worker = mocker.patch(
        'connect.eaas.runner.main.start_event_worker',
    )
    mocked_start_webapp_worker = mocker.patch(
        'connect.eaas.runner.main.start_webapp_worker',
    )
    mocked_start_anvilapp_worker = mocker.patch(
        'connect.eaas.runner.main.start_anvilapp_worker',
    )

    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    start(data)

    mocked_start_event_worker.assert_called_once_with(data)
    mocked_start_webapp_worker.assert_called_once_with(data)
    mocked_start_anvilapp_worker.assert_called_once_with(data)
    assert mocked_signal.mock_calls[0].args[0] == signal.SIGINT
    assert mocked_signal.mock_calls[1].args[0] == signal.SIGTERM
    mocked_event.wait.assert_called_once()


def test_main(mocker):
    testargs = ['cextrun']
    mocker.patch.object(sys, 'argv', testargs)
    mocked_start = mocker.patch('connect.eaas.runner.main.start')
    mocked_configure_logger = mocker.patch(
        'connect.eaas.runner.main.logging.config.dictConfig',
    )
    main()
    mocked_start.assert_called_once()
    mocked_configure_logger.assert_called_once()


def test_start_event_worker(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    mocker.patch.object(
        ExtensionHandler,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )

    mocked_process = mocker.MagicMock()

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
        return_value=mocked_process,
    )

    mocked_config = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.ConfigHelper', return_value=mocked_config)

    mocked_handler = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.ExtensionHandler', return_value=mocked_handler)

    start_event_worker(data)
    assert mocked_process_cls.mock_calls[0].kwargs['target'] == start_event_worker_process
    assert mocked_process_cls.mock_calls[0].kwargs['args'] == (mocked_config, mocked_handler, None)

    mocked_process.start.assert_called_once()


def test_start_event_worker_split(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=True)
    mocker.patch.object(
        ExtensionHandler,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )

    mocked_process = mocker.MagicMock()

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
        return_value=mocked_process,
    )

    mocked_config = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.ConfigHelper', return_value=mocked_config)

    mocked_handler = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.ExtensionHandler', return_value=mocked_handler)

    start_event_worker(data)

    assert mocked_process_cls.mock_calls[0].kwargs['target'] == start_event_worker_process
    assert mocked_process_cls.mock_calls[0].kwargs['args'] == (
        mocked_config, mocked_handler, 'interactive',
    )

    assert mocked_process_cls.mock_calls[1].kwargs['target'] == start_event_worker_process
    assert mocked_process_cls.mock_calls[1].kwargs['args'] == (
        mocked_config, mocked_handler, 'background',
    )

    assert mocked_process.start.call_count == 2


def test_start_event_worker_should_not_start(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    mocker.patch.object(
        ExtensionHandler,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=False),
    )

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
    )

    start_event_worker(data)

    mocked_process_cls.assert_not_called()


def test_start_webapp_worker(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    mocker.patch.object(
        WebApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )

    mocked_process = mocker.MagicMock()

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
        return_value=mocked_process,
    )

    mocked_config = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.ConfigHelper', return_value=mocked_config)

    mocked_webapp = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.WebApp', return_value=mocked_webapp)

    start_webapp_worker(data)
    assert mocked_process_cls.mock_calls[0].kwargs['target'] == start_webapp_worker_process
    assert mocked_process_cls.mock_calls[0].kwargs['args'] == (mocked_config, mocked_webapp)

    mocked_process.start.assert_called_once()


def test_start_webapp_worker_should_not_start(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    mocker.patch.object(
        WebApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=False),
    )

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
    )

    start_webapp_worker(data)

    mocked_process_cls.assert_not_called()


def test_start_anvilapp_worker(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    mocker.patch.object(
        AnvilApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )

    mocked_process = mocker.MagicMock()

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
        return_value=mocked_process,
    )

    mocked_config = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.ConfigHelper', return_value=mocked_config)

    mocked_webapp = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.main.AnvilApp', return_value=mocked_webapp)

    start_anvilapp_worker(data)
    assert mocked_process_cls.mock_calls[0].kwargs['target'] == start_anvilapp_worker_process
    assert mocked_process_cls.mock_calls[0].kwargs['args'] == (mocked_config, mocked_webapp)

    mocked_process.start.assert_called_once()


def test_start_anvilapp_worker_should_not_start(mocker):
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    data = parsed_args(True, extension_dir='/extension', split=False)
    mocker.patch.object(
        AnvilApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=False),
    )

    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.main.Process',
    )

    start_anvilapp_worker(data)

    mocked_process_cls.assert_not_called()


def test_start_event_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocked_config = mocker.MagicMock()

    mocker.patch.object(
        ExtensionHandler,
        'get_extension_class',
    )
    mocker.patch.object(Worker, 'start', start_mock)

    start_event_worker_process(mocked_config, None, None)

    start_mock.assert_awaited_once()


def test_start_webapp_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocked_config = mocker.MagicMock()
    mocked_handler = mocker.MagicMock()

    mocker.patch.object(
        WebApp,
        'get_webapp_class',
    )
    mocker.patch.object(WebWorker, 'start', start_mock)

    start_webapp_worker_process(mocked_config, mocked_handler)

    start_mock.assert_awaited_once()
    mocked_handler.start.assert_called_once()


def test_start_anvilapp_worker_process(mocker):
    start_mock = mocker.AsyncMock()

    mocked_config = mocker.MagicMock()
    mocked_handler = mocker.MagicMock()

    mocker.patch.object(
        AnvilApp,
        'get_anvilapp_class',
    )
    mocker.patch.object(AnvilWorker, 'start', start_mock)

    start_anvilapp_worker_process(mocked_config, mocked_handler)

    start_mock.assert_awaited_once()
