#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import argparse
import logging
import logging.config
import sys

import uvloop
from rich.console import Console

from connect.eaas.runner.artworks.banner import print_banner
from connect.eaas.runner.helpers import (
    configure_logger,
    get_features_table,
    get_no_features_table,
    validate_extension,
)
from connect.eaas.runner.master import Master


logger = logging.getLogger('connect.eaas')


def start(data):
    uvloop.install()
    console = Console()

    highest_message_level = 'INFO'
    tables = []

    if not data.no_validate:
        highest_message_level, tables = validate_extension()

    master = Master(not data.unsecure)

    have_features, features = master.get_available_features()

    if not have_features:
        highest_message_level = 'ERROR'

    console.print()

    print_banner(highest_message_level)

    if not have_features:
        console.print(get_no_features_table())

    for table in tables:
        console.print(table)

    if highest_message_level == 'ERROR':
        sys.exit(-1)

    console.print()
    console.print(get_features_table(features))

    logger.info('Starting Connect EaaS runtime....')
    if data.unsecure:
        logger.warning('Websocket connections will be established using unsecure protocol (ws).')

    master.run()

    logger.info('Connect EaaS runtime terminated.')


def main():
    parser = argparse.ArgumentParser(prog='cextrun')
    parser.add_argument('-u', '--unsecure', action='store_true')
    parser.add_argument('-s', '--split', action='store_true', default=False)
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('-n', '--no-validate', action='store_true', default=False)
    data = parser.parse_args()
    configure_logger(data.debug)
    if not data.no_validate:
        validate_extension()
    start(data)


if __name__ == '__main__':  # pragma: no cover
    main()
