#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import argparse
import logging
import logging.config

import uvloop

from connect.eaas.runner.helpers import (
    configure_logger,
)
from connect.eaas.runner.master import (
    Master,
)


logger = logging.getLogger('connect.eaas')


def start(data):
    uvloop.install()
    master = Master(
        secure=not data.unsecure,
        debug=data.debug,
        no_rich=data.no_rich_logging,
        no_validate=data.no_validate,
        reload=data.reload,
    )

    logger.info('Starting Connect EaaS runtime....')
    if data.unsecure:
        logger.warning('Websocket connections will be established using unsecure protocol (ws).')

    master.run()

    logger.info('Connect EaaS runtime terminated.')


def main():
    parser = argparse.ArgumentParser(prog='cextrun')
    parser.add_argument('-u', '--unsecure', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('-s', '--split', action='store_true', default=False, help=argparse.SUPPRESS)
    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        default=False,
        help='Set the log level to DEBUG.',
    )
    parser.add_argument(
        '-n',
        '--no-validate',
        action='store_true',
        default=False,
        help='Skip extension validations and start the runner.',
    )
    parser.add_argument(
        '-r',
        '--reload',
        action='store_true',
        default=False,
        help='Reload the runner when a python file changes (use only for development).',
    )

    parser.add_argument(
        '--no-rich-logging',
        action='store_true',
        default=False,
        help='Turn the rich console log to off.',
    )
    data = parser.parse_args()
    configure_logger(data.debug, data.no_rich_logging)
    start(data)


if __name__ == '__main__':  # pragma: no cover
    main()
