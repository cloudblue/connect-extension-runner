#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import argparse
import asyncio
import logging
import logging.config
import pathlib
import signal

from connect.eaas.helpers import install_extension
from connect.eaas.workers import ControlWorker


logger = logging.getLogger('eaas')


def configure_logger():
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
                'eaas': {
                    'handlers': ['console'],
                    'level': 'INFO',
                },
            },
        },
    )


def start(data):
    logger.info('Starting Connect EaaS runtime....')
    logger.info('Installing the extension package...')
    install_extension(data.extension_dir)
    logger.info('The extension has been installed, starting the control worker...')
    if data.unsecure:
        logger.warning('Websocket connections will be established using unsecure protocol (ws).')
    worker = ControlWorker(secure=not data.unsecure)
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGTERM,
        worker.run_event.clear,
    )
    worker.run_event.set()
    asyncio.run(worker.run())


def main():
    parser = argparse.ArgumentParser(prog='cextrun')
    parser.add_argument('-u', '--unsecure', action='store_true')
    parser.add_argument('-e', '--extension-dir', default='/extension', type=pathlib.Path)
    data = parser.parse_args()
    configure_logger()
    start(data)


if __name__ == '__main__':  # pragma: no cover
    main()
