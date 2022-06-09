#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import argparse
import asyncio
import logging
import logging.config
import signal
import threading
from multiprocessing import Process

import uvloop

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handler import ExtensionHandler
from connect.eaas.runner.webworker import WebWorker
from connect.eaas.runner.webapp import WebApp
from connect.eaas.runner.worker import Worker

logger = logging.getLogger('eaas')

workers = []


def configure_logger(debug):
    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'verbose': {
                    'format': '%(asctime)s %(name)s %(levelname)s PID_%(process)d %(message)s',
                },
            },
            'filters': {},
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'verbose',
                },
                'null': {
                    'class': 'logging.NullHandler',
                },
            },
            'loggers': {
                'connect.eaas': {
                    'handlers': ['console'],
                    'level': 'DEBUG' if debug else 'INFO',
                },
                'eaas.extension': {
                    'handlers': ['console'],
                    'level': 'DEBUG' if debug else 'INFO',
                },
            },
        },
    )


def start_event_worker_process(config, handler, runner_type):
    worker = Worker(config, handler, runner_type=runner_type)
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT,
        worker.handle_signal,
    )
    loop.add_signal_handler(
        signal.SIGTERM,
        worker.handle_signal,
    )
    loop.run_until_complete(worker.start())


def start_webapp_worker_process(config, handler):
    worker = WebWorker(config, handler)
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT,
        worker.handle_signal,
    )
    loop.add_signal_handler(
        signal.SIGTERM,
        worker.handle_signal,
    )
    loop.run_until_complete(worker.start())


def start_event_worker(data):
    config = ConfigHelper(secure=not data.unsecure)
    handler = ExtensionHandler(config)

    if not handler.should_start:
        return

    runner_types = ('interactive', 'background') if data.split else (None,)

    for runner_type in runner_types:
        p = Process(
            target=start_event_worker_process,
            args=(config, handler, runner_type),
        )
        workers.append(p)
        p.start()
        logger.info(f'{runner_type or "Events"} tasks worker pid: {p.pid}')


def start_webapp_worker(data):
    config = ConfigHelper(secure=not data.unsecure)
    handler = WebApp(config)
    if not handler.should_start:
        return

    p = Process(
        target=start_webapp_worker_process,
        args=(config, handler),
    )
    workers.append(p)
    p.start()
    logger.info(f'Webapp worker pid: {p.pid}')


def start(data):
    uvloop.install()
    logger.info('Starting Connect EaaS runtime....')
    if data.unsecure:
        logger.warning('Websocket connections will be established using unsecure protocol (ws).')

    stop_event = threading.Event()

    def _terminate(*args, **kwargs):  # pragma: no cover
        for p in workers:
            p.join()
        stop_event.set()

    signal.signal(signal.SIGINT, _terminate)
    signal.signal(signal.SIGTERM, _terminate)

    start_event_worker(data)
    start_webapp_worker(data)

    stop_event.wait()


def main():
    parser = argparse.ArgumentParser(prog='cextrun')
    parser.add_argument('-u', '--unsecure', action='store_true')
    parser.add_argument('-s', '--split', action='store_true', default=False)
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    data = parser.parse_args()
    configure_logger(data.debug)
    start(data)


if __name__ == '__main__':  # pragma: no cover
    main()
