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
    mocked_master.get_available_features.return_value = (True, {})
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
        reload=False,
    )


def test_start_validate_ok(mocker):
    mocked_master = mocker.MagicMock()
    mocked_master.get_available_features.return_value = (True, {})
    mocked_master_cls = mocker.patch(
        'connect.eaas.runner.main.Master',
        return_value=mocked_master,
    )
    mocker.patch('connect.eaas.runner.main.validate_extension', return_value=('INFO', []))

    start(parsed_args(
        False,
        extension_dir='/extension',
        split=False,
        no_validate=False,
        no_rich_logging=False,
        reload=False,
        debug=False,
    ))
    mocked_master.run.assert_called_once()
    mocked_master_cls.assert_called_once_with(
        secure=True,
        debug=False,
        no_rich=False,
        reload=False,
    )


def test_start_validate_warning(mocker):
    mocked_master = mocker.MagicMock()
    mocked_master.get_available_features.return_value = (True, {})
    mocked_master_cls = mocker.patch(
        'connect.eaas.runner.main.Master',
        return_value=mocked_master,
    )
    mocker.patch('connect.eaas.runner.main.validate_extension', return_value=('WARNING', ['']))

    start(parsed_args(
        False,
        extension_dir='/extension',
        split=False,
        no_validate=False,
        no_rich_logging=False,
        reload=False,
        debug=False,
    ))
    mocked_master.run.assert_called_once()
    mocked_master_cls.assert_called_once_with(
        secure=True,
        debug=False,
        no_rich=False,
        reload=False,
    )


def test_start_validate_error(mocker):
    mocked_master = mocker.MagicMock()
    mocked_master.get_available_features.return_value = (True, {})
    mocker.patch(
        'connect.eaas.runner.main.Master',
        return_value=mocked_master,
    )
    mocker.patch('connect.eaas.runner.main.validate_extension', return_value=('ERROR', ['']))

    with pytest.raises(SystemExit):
        start(parsed_args(
            False,
            extension_dir='/extension',
            split=False,
            no_validate=False,
            no_rich_logging=False,
            reload=False,
            debug=False,
        ))


def test_start_no_feature(mocker):
    mocked_master = mocker.MagicMock()
    mocked_master.get_available_features.return_value = (False, {})
    mocker.patch(
        'connect.eaas.runner.main.Master',
        return_value=mocked_master,
    )
    mocked_no_feature = mocker.patch(
        'connect.eaas.runner.main.get_no_features_table',
        return_value='',
    )
    mocker.patch('connect.eaas.runner.main.validate_extension', return_value=('INFO', []))

    with pytest.raises(SystemExit):
        start(parsed_args(
            False,
            extension_dir='/extension',
            split=False,
            no_validate=False,
            no_rich_logging=False,
            reload=False,
            debug=False,
        ))
    mocked_no_feature.assert_called()


def test_main(mocker):
    testargs = ['cextrun']
    mocker.patch.object(sys, 'argv', testargs)
    mocked_check = mocker.patch('connect.eaas.runner.main.validate_extension')
    mocked_start = mocker.patch('connect.eaas.runner.main.start')
    mocked_configure_logger = mocker.patch(
        'connect.eaas.runner.main.logging.config.dictConfig',
    )
    main()
    mocked_check.assert_called_once()
    mocked_start.assert_called_once()
    mocked_configure_logger.assert_called_once()
