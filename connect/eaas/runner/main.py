#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import argparse
import logging
import logging.config
import sys

import uvloop

from connect.eaas.runner.helpers import (
    get_connect_version,
    get_pypi_runner_minor_version,
    get_version,
)
from connect.eaas.runner.master import Master


logger = logging.getLogger('connect.eaas')


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


def check_runner_version(skip_check):
    if skip_check:
        return

    connect_full_version = get_connect_version()
    connect_version = connect_full_version.split('.')[0]
    runner_version = get_version()
    latest_minor_version = get_pypi_runner_minor_version(connect_version)

    if (
            connect_version == runner_version.split('.')[0]
            and runner_version.split('.')[1] == latest_minor_version
    ):
        return

    logger.error(
        'Runner is outdated, please, update. '
        f'Required version {connect_version}.{latest_minor_version}, '
        f'current version: {runner_version}.',
    )
    sys.exit(3)


def start(data):
    uvloop.install()
    logger.info('Starting Connect EaaS runtime....')
    if data.unsecure:
        logger.warning('Websocket connections will be established using unsecure protocol (ws).')

    master = Master(not data.unsecure)
    master.run()

    logger.info('Connect EaaS runtime terminated.')


def main():
    parser = argparse.ArgumentParser(prog='cextrun')
    parser.add_argument('-u', '--unsecure', action='store_true')
    parser.add_argument('-s', '--split', action='store_true', default=False)
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('-n', '--no-version-check', action='store_true', default=False)
    data = parser.parse_args()
    configure_logger(data.debug)
    check_runner_version(data.no_version_check)
    start(data)


if __name__ == '__main__':  # pragma: no cover
    main()
