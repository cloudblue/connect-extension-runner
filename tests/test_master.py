import logging
import signal
import threading
import time
from pathlib import Path

import pytest

from connect.eaas.runner.constants import (
    ANVILAPP_WORKER,
    BACKGROUND_EVENTS_WORKER,
    INTERACTIVE_EVENTS_WORKER,
    WEBAPP_WORKER,
    WORKER_TYPES,
)
from connect.eaas.runner.handlers.anvil import AnvilApp
from connect.eaas.runner.handlers.events import EventsApp
from connect.eaas.runner.handlers.transformations import TfnApp
from connect.eaas.runner.handlers.web import WebApp
from connect.eaas.runner.master import Master
from connect.eaas.runner.workers.anvil import start_anvilapp_worker_process
from connect.eaas.runner.workers.events import (
    start_background_worker_process, start_interactive_worker_process,
)
from connect.eaas.runner.workers.web import start_webapp_worker_process


def test_process_targets():
    assert Master.PROCESS_TARGETS[ANVILAPP_WORKER] == start_anvilapp_worker_process
    assert Master.PROCESS_TARGETS[BACKGROUND_EVENTS_WORKER] == start_background_worker_process
    assert Master.PROCESS_TARGETS[INTERACTIVE_EVENTS_WORKER] == start_interactive_worker_process
    assert Master.PROCESS_TARGETS[WEBAPP_WORKER] == start_webapp_worker_process


def test_handler_classes():
    assert Master.HANDLER_CLASSES[ANVILAPP_WORKER] == AnvilApp
    assert Master.HANDLER_CLASSES[BACKGROUND_EVENTS_WORKER] == EventsApp
    assert Master.HANDLER_CLASSES[INTERACTIVE_EVENTS_WORKER] == EventsApp
    assert Master.HANDLER_CLASSES[WEBAPP_WORKER] == WebApp


@pytest.mark.parametrize('secure', (True, False))
def test_init(mocker, secure):
    mocked_config = mocker.patch('connect.eaas.runner.master.ConfigHelper')
    mocked_signal = mocker.patch('connect.eaas.runner.master.signal.signal')

    mocked_flt_instance = mocker.MagicMock()
    mocked_pyfilter = mocker.patch(
        'connect.eaas.runner.master.PythonFilter',
        return_value=mocked_flt_instance,
    )
    mocked_watch = mocker.patch('connect.eaas.runner.master.watch')

    master = Master(
        secure=secure,
        debug=True,
        no_rich=False,
        reload=True,
    )

    mocked_config.assert_called_once_with(secure=secure)
    assert master.workers == {}
    assert master.debug is True
    assert master.no_rich is False
    assert master.reload is True

    assert isinstance(master.stop_event, threading.Event)
    assert isinstance(master.monitor_event, threading.Event)
    mocked_pyfilter.assert_called_once_with(ignore_paths=None)
    assert master.watch_filter == mocked_flt_instance
    mocked_watch.assert_called_once_with(
        Path.cwd(),
        watch_filter=mocked_flt_instance,
        stop_event=master.stop_event,
        yield_on_timeout=True,
    )

    for worker_type in WORKER_TYPES:
        assert isinstance(master.handlers[worker_type], Master.HANDLER_CLASSES[worker_type])

    assert mocked_signal.mock_calls[0].args == (signal.SIGINT, master.handle_signal)
    assert mocked_signal.mock_calls[1].args == (signal.SIGTERM, master.handle_signal)


@pytest.mark.parametrize('worker_type', WORKER_TYPES)
def test_start_worker_process(mocker, worker_type):
    handler = mocker.MagicMock()
    mocked_process = mocker.MagicMock()
    mocked_start_process = mocker.patch(
        'connect.eaas.runner.master.start_process',
        return_value=mocked_process,
    )
    mocker.patch.dict(Master.HANDLER_CLASSES, {worker_type: handler.__class__})

    master = Master()
    master.start_worker_process(worker_type, handler)
    assert master.workers[worker_type] == mocked_process
    mocked_start_process.assert_called_once_with(
        master.PROCESS_TARGETS[worker_type],
        'function',
        (
            handler.__class__,
            mocker.ANY,
            master.lifecycle_lock,
            master.lifecycle_events[handler.__class__].on_startup,
            master.lifecycle_events[handler.__class__].on_shutdown,
            master.debug,
            master.no_rich,
        ),
        {},
    )


def test_start(mocker):
    mocked_thread = mocker.MagicMock()
    mocked_thread_cls = mocker.patch(
        'connect.eaas.runner.master.threading.Thread',
        return_value=mocked_thread,
    )
    mocker.patch
    for handler_cls in (AnvilApp, WebApp, EventsApp, TfnApp):
        mocker.patch.object(
            handler_cls,
            'should_start',
            new_callable=mocker.PropertyMock(return_value=True),
        )

    mocked_start_process = mocker.patch.object(Master, 'start_worker_process')
    mocker.patch.object(Master, 'check')

    master = Master()
    master.start()

    assert tuple([
        mock_call.args[0] for mock_call in mocked_start_process.mock_calls
    ]) == WORKER_TYPES

    assert master.monitor_event.is_set() is True
    mocked_thread_cls.assert_called_once_with(target=master.monitor_processes)
    mocked_thread.start.assert_called_once()


def test_stop(mocker):
    master = Master()
    master.monitor_event.set()
    master.monitor_thread = mocker.MagicMock()
    master.workers['test'] = mocker.MagicMock()

    master.stop()

    assert master.monitor_event.is_set() is False
    master.monitor_thread.join.assert_called_once()
    master.workers['test'].stop.assert_called_once_with(sigint_timeout=5, sigkill_timeout=1)


def test_restart(mocker):
    mocked_stop = mocker.patch.object(Master, 'stop')
    mocked_start = mocker.patch.object(Master, 'start')

    master = Master()

    master.restart()

    mocked_stop.assert_called_once()
    mocked_start.assert_called_once()


def test_handle_signal():
    master = Master()
    master.handle_signal()
    assert master.stop_event.is_set() is True


def test_next():
    master = Master()
    master.watcher = iter([[(None, '/file1.py')], None])

    assert next(master) == [Path('/file1.py')]
    assert next(master) is None


def test_run(mocker):
    mocked_start = mocker.patch.object(Master, 'start')
    mocked_stop = mocker.patch.object(Master, 'stop')

    master = Master()
    master.stop_event.wait = mocker.MagicMock()

    master.run()
    mocked_start.assert_called_once()
    mocked_stop.assert_called_once()
    master.stop_event.wait.assert_called_once()


def test_run_with_reload(mocker, caplog):
    mocked_start = mocker.patch.object(Master, 'start')
    mocked_stop = mocker.patch.object(Master, 'stop')
    mocked_restart = mocker.patch.object(Master, 'restart')
    mocker.patch.object(
        Master,
        '__next__',
        side_effect=[[Path.cwd() / Path('changed_file.py')], None],
    )

    master = Master(reload=True)
    master.stop_event.wait = mocker.MagicMock()
    with caplog.at_level(logging.WARNING):
        master.run()
    assert 'Detected changes in "changed_file.py"' in caplog.text
    mocked_start.assert_called_once()
    mocked_restart.assert_called_once()
    mocked_stop.assert_called_once()
    master.stop_event.wait.assert_not_called()


def test_monitor_restart_died_process(mocker):
    mocker.patch('connect.eaas.runner.master.PROCESS_CHECK_INTERVAL_SECS', 0.01)
    mocked_start_process = mocker.patch.object(Master, 'start_worker_process')
    mocked_notify = mocker.patch('connect.eaas.runner.master.notify_process_restarted')

    mocked_process = mocker.MagicMock()
    mocked_process.is_alive.return_value = False
    mocked_process.exitcode = -9

    master = Master()
    master.workers['webapp'] = mocked_process
    master.monitor_event.set()

    t = threading.Thread(target=master.monitor_processes)
    t.start()
    time.sleep(.03)
    master.monitor_event.clear()
    t.join()

    mocked_notify.assert_called()
    assert mocked_notify.mock_calls[0].args[0] == 'webapp'
    mocked_start_process.assert_called_with(
        'webapp',
        master.handlers['webapp'],
    )


def test_monitor_process_exited(mocker, caplog):
    mocker.patch('connect.eaas.runner.master.PROCESS_CHECK_INTERVAL_SECS', 0.01)
    mocker.patch('connect.eaas.runner.master.signal.signal')
    mocked_start_process = mocker.patch.object(Master, 'start_worker_process')
    mocked_notify = mocker.patch('connect.eaas.runner.master.notify_process_restarted')

    mocked_process = mocker.MagicMock()
    mocked_process.is_alive.return_value = False
    mocked_process.exitcode = 0

    master = Master()
    master.workers['webapp'] = mocked_process
    master.monitor_event.set()

    with caplog.at_level(logging.INFO):
        t = threading.Thread(target=master.monitor_processes)
        t.start()
        time.sleep(.03)
        master.monitor_event.clear()
        t.join()

    assert 'Webapp worker exited' in caplog.text
    mocked_notify.assert_not_called()
    mocked_start_process.assert_not_called()
    assert master.stop_event.is_set() is True


def test_get_available_features(mocker):
    mocker.patch.object(
        AnvilApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )
    mocker.patch.object(
        AnvilApp,
        'features',
        new_callable=mocker.PropertyMock(return_value={'callables': []}),
    )

    mocker.patch.object(
        EventsApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )
    mocker.patch.object(
        EventsApp,
        'features',
        new_callable=mocker.PropertyMock(return_value={
            'events': {},
            'schedulables': [],
        }),
    )

    mocker.patch.object(
        WebApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )
    mocker.patch.object(
        WebApp,
        'features',
        new_callable=mocker.PropertyMock(return_value={'endpoints': {}}),
    )

    mocker.patch.object(
        TfnApp,
        'should_start',
        new_callable=mocker.PropertyMock(return_value=True),
    )
    mocker.patch.object(
        TfnApp,
        'features',
        new_callable=mocker.PropertyMock(return_value={'transformations': []}),
    )

    master = Master()

    assert master.get_available_features() == (
        True,
        {
            'AnvilApp': {
                'available': True,
                'features': {'callables': []},
            },
            'EventsApp': {
                'available': True,
                'features': {'events': {}, 'schedulables': []},
            },
            'WebApp': {
                'available': True,
                'features': {'endpoints': {}},
            },
            'TfnApp': {
                'available': True,
                'features': {'transformations': []},
            },
        },
    )


def test_check_validate_ok(mocker):
    mocker.patch.object(Master, 'get_available_features', return_value=(True, {}))
    mocker.patch('connect.eaas.runner.master.validate_extension', return_value=('INFO', []))
    master = Master(False, False, False, False, False)
    assert master.check() is True


def test_start_validate_warning(mocker):
    mocker.patch.object(Master, 'get_available_features', return_value=(True, {}))
    mocker.patch('connect.eaas.runner.master.validate_extension', return_value=('WARNING', []))
    master = Master(False, False, False, False, False)
    assert master.check() is True


def test_start_validate_error(mocker):
    mocker.patch.object(Master, 'get_available_features', return_value=(True, {}))
    mocker.patch('connect.eaas.runner.master.validate_extension', return_value=('ERROR', []))
    master = Master(False, False, False, False, False)
    assert master.check() is False


def test_start_no_feature(mocker):
    mocker.patch.object(Master, 'get_available_features', return_value=(False, {}))
    mocker.patch('connect.eaas.runner.master.validate_extension', return_value=('INFO', []))
    master = Master(False, False, False, False, False)
    assert master.check() is False


def test_start_check_fails(mocker):
    mocker.patch.object(Master, 'check', return_value=False)
    master = Master(False, False, False, False, False)
    with pytest.raises(SystemExit):
        master.start()
