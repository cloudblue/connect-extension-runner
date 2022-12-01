import logging
import threading
import signal
import time
from pathlib import Path

from watchfiles import watch
from watchfiles.filters import PythonFilter
from watchfiles.run import start_process

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.constants import (
    ANVILAPP_WORKER,
    BACKGROUND_EVENTS_WORKER,
    INTERACTIVE_EVENTS_WORKER,
    PROCESS_CHECK_INTERVAL_SECS,
    TFNAPP_WORKER,
    WEBAPP_WORKER,
    WORKER_TYPES,
)
from connect.eaas.runner.handlers.anvil import AnvilApp
from connect.eaas.runner.handlers.events import EventsApp
from connect.eaas.runner.handlers.transformations import TfnApp
from connect.eaas.runner.handlers.web import WebApp
from connect.eaas.runner.helpers import notify_process_restarted
from connect.eaas.runner.workers.anvil import start_anvilapp_worker_process
from connect.eaas.runner.workers.transformations import start_tfnapp_worker_process
from connect.eaas.runner.workers.web import start_webapp_worker_process
from connect.eaas.runner.workers.events import (
    start_background_worker_process,
    start_interactive_worker_process,
)


logger = logging.getLogger('connect.eaas')


HANDLED_SIGNALS = (signal.SIGINT, signal.SIGTERM)


def _display_path(path):
    try:
        return f'"{path.relative_to(Path.cwd())}"'
    except ValueError:  # pragma: no cover
        return f'"{path}"'


class Master:

    HANDLER_CLASSES = {
        BACKGROUND_EVENTS_WORKER: EventsApp,
        INTERACTIVE_EVENTS_WORKER: EventsApp,
        WEBAPP_WORKER: WebApp,
        ANVILAPP_WORKER: AnvilApp,
        TFNAPP_WORKER: TfnApp,
    }

    PROCESS_TARGETS = {
        BACKGROUND_EVENTS_WORKER: start_background_worker_process,
        INTERACTIVE_EVENTS_WORKER: start_interactive_worker_process,
        WEBAPP_WORKER: start_webapp_worker_process,
        ANVILAPP_WORKER: start_anvilapp_worker_process,
        TFNAPP_WORKER: start_tfnapp_worker_process,
    }

    def __init__(self, secure=True, debug=False, no_rich=False, reload=False):
        self.config = ConfigHelper(secure=secure)
        self.handlers = {
            worker_type: self.HANDLER_CLASSES[worker_type](self.config)
            for worker_type in WORKER_TYPES
        }
        self.reload = reload
        self.debug = debug
        self.no_rich = no_rich
        self.workers = {}
        self.stop_event = threading.Event()
        self.monitor_event = threading.Event()
        self.watch_filter = PythonFilter(ignore_paths=None)
        self.watcher = watch(
            Path.cwd(),
            watch_filter=self.watch_filter,
            stop_event=self.stop_event,
            yield_on_timeout=True,
        )
        self.monitor_thread = None
        self.setup_signals_handler()

    def setup_signals_handler(self):
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.handle_signal)

    def get_available_features(self):
        have_features = False
        features = {}
        for handler in self.handlers.values():
            have_features = have_features or handler.should_start
            worker_info = {
                'available': handler.should_start,
            }
            if handler.should_start:
                worker_info['features'] = handler.features
            features[handler.__class__.__name__] = worker_info
        return have_features, features

    def handle_signal(self, *args, **kwargs):
        self.stop_event.set()

    def start(self):
        for worker_type, handler in self.handlers.items():
            if handler.should_start:
                self.start_worker_process(worker_type, handler)
        self.monitor_thread = threading.Thread(target=self.monitor_processes)
        self.monitor_event.set()
        self.monitor_thread.start()

    def start_worker_process(self, worker_type, handler):
        p = start_process(
            self.PROCESS_TARGETS[worker_type],
            'function',
            (handler, self.debug, self.no_rich),
            {},
        )
        self.workers[worker_type] = p
        logger.info(f'{worker_type.capitalize()} worker pid: {p.pid}')

    def monitor_processes(self):
        while self.monitor_event.is_set():
            exited_workers = []
            for worker_type, p in self.workers.items():
                if not p.is_alive():
                    if p.exitcode != 0:
                        notify_process_restarted(worker_type)
                        logger.info(f'Process of type {worker_type} is dead, restart it')
                        self.start_worker_process(worker_type, self.handlers[worker_type])
                    else:
                        exited_workers.append(worker_type)
                        logger.info(f'{worker_type.capitalize()} worker exited')
            if exited_workers == list(self.workers.keys()):
                self.stop_event.set()

            time.sleep(PROCESS_CHECK_INTERVAL_SECS)

    def stop(self):
        self.monitor_event.clear()
        self.monitor_thread.join()
        for process in self.workers.values():
            process.stop(sigint_timeout=5, sigkill_timeout=1)
            logger.info(f'Consumer process with pid {process.pid} stopped.')

    def restart(self):
        self.stop()
        self.start()

    def __iter__(self):
        return self

    def __next__(self):
        changes = next(self.watcher)
        if changes:
            return list({Path(c[1]) for c in changes})
        return None

    def run(self):
        self.start()
        if self.reload:
            for files_changed in self:
                if files_changed:
                    logger.warning(
                        'Detected changes in %s. Reloading...',
                        ', '.join(map(_display_path, files_changed)),
                    )
                    self.restart()
        else:
            self.stop_event.wait()
        self.stop()
