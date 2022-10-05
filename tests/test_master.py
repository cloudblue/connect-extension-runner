import signal
import threading
import time

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

    master = Master(secure)

    mocked_config.assert_called_once_with(secure=secure)
    assert master.workers == {}
    assert master.exited_workers == {}
    assert isinstance(master.stop_event, threading.Event)
    for worker_type in WORKER_TYPES:
        assert isinstance(master.handlers[worker_type], Master.HANDLER_CLASSES[worker_type])


@pytest.mark.parametrize('worker_type', WORKER_TYPES)
def test_start_worker_process(mocker, worker_type):
    handler = mocker.MagicMock()
    handler._config = mocker.MagicMock()
    mocked_process = mocker.MagicMock()
    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.master.Process',
        return_value=mocked_process,
    )
    master = Master()
    master.start_worker_process(worker_type, handler)
    assert master.workers[worker_type] == mocked_process
    assert mocked_process_cls.mock_calls[0].kwargs['target'] == Master.PROCESS_TARGETS[worker_type]
    assert mocked_process_cls.mock_calls[0].kwargs['args'] == (handler,)
    mocked_process.start.assert_called_once()


def test_run_start_processes(mocker):
    for handler_cls in (AnvilApp, WebApp, EventsApp):
        mocker.patch.object(
            handler_cls,
            'should_start',
            new_callable=mocker.PropertyMock(return_value=True),
        )

    mocked_start_process = mocker.patch.object(Master, 'start_worker_process')
    mocked_signal = mocker.patch('connect.eaas.runner.master.signal.signal')

    master = Master()
    master.stop_event.set()
    master.run()

    assert tuple([
        mock_call.args[0] for mock_call in mocked_start_process.mock_calls
    ]) == WORKER_TYPES

    assert mocked_signal.mock_calls[0].args[0] == signal.SIGINT
    assert mocked_signal.mock_calls[1].args[0] == signal.SIGTERM


def test_run_restart_died_process(mocker):
    mocker.patch('connect.eaas.runner.master.PROCESS_CHECK_INTERVAL_SECS', 0.01)
    mocker.patch('connect.eaas.runner.master.signal.signal')
    mocked_start_process = mocker.patch.object(Master, 'start_worker_process')
    mocked_notify = mocker.patch('connect.eaas.runner.master.notify_process_restarted')

    mocked_process = mocker.MagicMock()
    mocked_process.is_alive.return_value = False
    mocked_process.exitcode = -9

    master = Master()
    master.workers['webapp'] = mocked_process
    master.exited_workers['webapp'] = False

    t = threading.Thread(target=master.run)
    t.start()
    time.sleep(.03)
    master.stop_event.set()
    t.join()

    mocked_notify.assert_called()
    assert mocked_notify.mock_calls[0].args[0] == 'webapp'
    mocked_start_process.assert_called_with(
        'webapp',
        master.handlers['webapp'],
    )


def test_run_process_exited(mocker):
    mocker.patch('connect.eaas.runner.master.PROCESS_CHECK_INTERVAL_SECS', 0.01)
    mocker.patch('connect.eaas.runner.master.signal.signal')
    mocked_start_process = mocker.patch.object(Master, 'start_worker_process')
    mocked_notify = mocker.patch('connect.eaas.runner.master.notify_process_restarted')

    mocked_process = mocker.MagicMock()
    mocked_process.is_alive.return_value = True

    master = Master()
    master.workers['webapp'] = mocked_process
    master.exited_workers['webapp'] = False

    t = threading.Thread(target=master.run)
    t.start()
    time.sleep(.03)
    mocked_process.is_alive.return_value = False
    mocked_process.exitcode = 0
    time.sleep(.05)
    t.join()

    mocked_notify.assert_not_called()
    mocked_start_process.assert_not_called()
    assert master.exited_workers['webapp'] is True


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
        },
    )
