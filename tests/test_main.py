#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import sys
from collections import namedtuple

import pytest

from connect.eaas.runner.main import (
    check_runner_version,
    main,
    start,
)


@pytest.mark.parametrize('unsecure', (True, False))
def test_start(mocker, unsecure):
    mocked_master = mocker.MagicMock()
    mocked_master_cls = mocker.patch(
        'connect.eaas.runner.main.Master',
        return_value=mocked_master,
    )
    parsed_args = namedtuple('_Args', ('unsecure', 'extension_dir', 'split'))
    start(parsed_args(unsecure, extension_dir='/extension', split=False))
    mocked_master.run.assert_called_once()
    mocked_master_cls.assert_called_once_with(not unsecure)


def test_main(mocker):
    testargs = ['cextrun']
    mocker.patch.object(sys, 'argv', testargs)
    mocked_check = mocker.patch('connect.eaas.runner.main.check_runner_version')
    mocked_start = mocker.patch('connect.eaas.runner.main.start')
    mocked_configure_logger = mocker.patch(
        'connect.eaas.runner.main.logging.config.dictConfig',
    )
    main()
    mocked_check.assert_called_once()
    mocked_start.assert_called_once()
    mocked_configure_logger.assert_called_once()


def test_check_runner_version_ok(mocker, responses):
    mocker.patch(
        'connect.eaas.runner.main.get_connect_version',
        return_value='26.0.26-gjg2312',
    )
    mocker.patch(
        'connect.eaas.runner.main.get_version',
        return_value='26.12.3',
    )
    mocker.patch(
        'connect.eaas.runner.main.get_pypi_runner_minor_version',
        return_value='12',
    )
    mocked_exit_main = mocker.patch('connect.eaas.runner.main.sys.exit')
    mocked_exit_helpers = mocker.patch('connect.eaas.runner.helpers.sys.exit')

    check_runner_version(False)
    mocked_exit_main.assert_not_called()
    mocked_exit_helpers.assert_not_called()


def test_check_runner_version_skip(mocker):
    mocked_exit_main = mocker.patch('connect.eaas.runner.main.sys.exit')
    mocked_exit_helpers = mocker.patch('connect.eaas.runner.helpers.sys.exit')

    check_runner_version(True)
    mocked_exit_main.assert_not_called()
    mocked_exit_helpers.assert_not_called()


def test_check_runner_version_not_matching(mocker):
    mocker.patch(
        'connect.eaas.runner.main.get_connect_version',
        return_value='26.0.26-gjg2312',
    )
    mocker.patch(
        'connect.eaas.runner.main.get_version',
        return_value='25.12',
    )
    mocker.patch(
        'connect.eaas.runner.main.get_pypi_runner_minor_version',
        return_value='12',
    )

    with pytest.raises(SystemExit) as cv:
        check_runner_version(False)
    assert cv.value.code == 3


def test_check_runner_version_outdated(mocker):
    mocker.patch(
        'connect.eaas.runner.main.get_connect_version',
        return_value='26.0.26-gjg2312',
    )
    mocker.patch(
        'connect.eaas.runner.main.get_version',
        return_value='26.8.1',
    )
    mocker.patch(
        'connect.eaas.runner.main.get_pypi_runner_minor_version',
        return_value='17',
    )

    with pytest.raises(SystemExit) as cv:
        check_runner_version(False)
    assert cv.value.code == 3
