import logging
import threading
import signal
import time
from multiprocessing import Process

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.constants import (
    ANVILAPP_WORKER,
    BACKGROUND_EVENTS_WORKER,
    INTERACTIVE_EVENTS_WORKER,
    PROCESS_CHECK_INTERVAL_SECS,
    WEBAPP_WORKER,
    WORKER_TYPES,
)
from connect.eaas.runner.handlers.anvil import AnvilApp
from connect.eaas.runner.handlers.events import EventsApp
from connect.eaas.runner.handlers.web import WebApp
from connect.eaas.runner.helpers import notify_process_restarted
from connect.eaas.runner.workers.anvil import start_anvilapp_worker_process
from connect.eaas.runner.workers.web import start_webapp_worker_process
from connect.eaas.runner.workers.events import (
    start_background_worker_process,
    start_interactive_worker_process,
)


logger = logging.getLogger('connect.eaas')


class Master:

    HANDLER_CLASSES = {
        BACKGROUND_EVENTS_WORKER: EventsApp,
        INTERACTIVE_EVENTS_WORKER: EventsApp,
        WEBAPP_WORKER: WebApp,
        ANVILAPP_WORKER: AnvilApp,
    }

    PROCESS_TARGETS = {
        BACKGROUND_EVENTS_WORKER: start_background_worker_process,
        INTERACTIVE_EVENTS_WORKER: start_interactive_worker_process,
        WEBAPP_WORKER: start_webapp_worker_process,
        ANVILAPP_WORKER: start_anvilapp_worker_process,
    }

    def __init__(self, secure=True):
        self.config = ConfigHelper(secure=secure)
        self.handlers = {
            worker_type: self.HANDLER_CLASSES[worker_type](self.config)
            for worker_type in WORKER_TYPES
        }
        self.workers = {}
        self.exited_workers = {}
        self.stop_event = threading.Event()

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

    def start_worker_process(self, worker_type, handler):
        p = Process(
            target=self.PROCESS_TARGETS[worker_type],
            args=(handler,),
        )
        self.workers[worker_type] = p
        p.start()
        logger.info(f'{worker_type.capitalize()} worker pid: {p.pid}')

    def run(self):
        for worker_type, handler in self.handlers.items():
            if handler.should_start:
                self.exited_workers[worker_type] = False
                self.start_worker_process(worker_type, handler)

        def _terminate(*args, **kwargs):  # pragma: no cover
            for p in self.workers.values():
                p.join()
            self.stop_event.set()

        signal.signal(signal.SIGINT, _terminate)
        signal.signal(signal.SIGTERM, _terminate)

        while not (self.stop_event.is_set() or all(self.exited_workers.values())):
            for worker_type, p in self.workers.items():
                if not p.is_alive():
                    if p.exitcode != 0:
                        notify_process_restarted(worker_type)
                        logger.info(f'Process of type {worker_type} is dead, restart it')
                        self.start_worker_process(worker_type, self.handlers[worker_type])
                    else:
                        self.exited_workers[worker_type] = True
                        logger.info(f'{worker_type.capitalize()} worker exited')

            time.sleep(PROCESS_CHECK_INTERVAL_SECS)
