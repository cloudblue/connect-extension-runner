#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import sys
from collections import namedtuple

import pytest

from connect.eaas.runner.main import (
    main,
    start,
)


parsed_args = namedtuple(
    '_Args',
    ('unsecure', 'extension_dir', 'split', 'no_validate', 'no_rich_logging', 'reload', 'debug'),
)


@pytest.mark.parametrize('unsecure', (True, False))
def test_start(mocker, unsecure):
    mocked_master = mocker.MagicMock()
    mocked_master_cls = mocker.patch(
        'connect.eaas.runner.main.Master',
        return_value=mocked_master,
    )
    start(parsed_args(
        unsecure,
        extension_dir='/extension',
        split=False,
        no_validate=True,
        no_rich_logging=False,
        reload=False,
        debug=False,
    ))
    mocked_master.run.assert_called_once()
    mocked_master_cls.assert_called_once_with(
        secure=not unsecure,
        debug=False,
        no_rich=False,
        no_validate=True,
        reload=False,
    )


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
