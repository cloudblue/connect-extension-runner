#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import os
import subprocess

import pytest
from pkg_resources import EntryPoint

from connect.eaas.constants import (
    BACKGROUND_TASK_MAX_EXECUTION_TIME,
    INTERACTIVE_TASK_MAX_EXECUTION_TIME,
    SCHEDULED_TASK_MAX_EXECUTION_TIME,
)
from connect.eaas.exceptions import EaaSError
from connect.eaas.helpers import (
    get_container_id,
    get_environment,
    get_extension_class,
    get_extension_type,
)


def test_get_container_id(mocker):
    result = mocker.MagicMock()
    result.returnvalue = 0
    result.stdout = (
        '/docker/e724f0291e6b5a3a37b1c0eb83daa6ee1df5c52f40a01d49f481908766251791\n'.encode('utf-8')
    )
    mocked = mocker.patch('connect.eaas.helpers.subprocess.run', return_value=result)
    assert get_container_id() == 'e724f0291e6b5a3a37b1c0eb83daa6ee1df5c52f40a01d49f481908766251791'
    assert mocked.mock_calls[0].args[0] == ['cat', '/proc/1/cpuset']


def test_get_container_id_ko(mocker):
    result = mocker.MagicMock()
    result.check_returncode = mocker.MagicMock(
        side_effect=subprocess.CalledProcessError(128, cmd=[]),
    )
    result.stderr = 'error message'.encode('utf-8')
    mocker.patch('connect.eaas.helpers.subprocess.run', return_value=result)
    expected_result = get_container_id()

    assert isinstance(expected_result, str)
    assert len(expected_result) > 0


def test_get_environment_with_defaults(mocker):
    os.environ['API_KEY'] = 'SU-000:XXXX'
    os.environ['ENVIRONMENT_ID'] = 'ENV-0000-0000-01'
    if 'SERVER_ADDRESS' in os.environ:
        del os.environ['SERVER_ADDRESS']
    if 'INSTANCE_ID' in os.environ:
        del os.environ['INSTANCE_ID']
    mocker.patch('connect.eaas.helpers.get_container_id', return_value='container_id')
    env = get_environment()
    assert env['api_key'] == 'SU-000:XXXX'
    assert env['environment_id'] == 'ENV-0000-0000-01'
    assert env['instance_id'] == 'container_id'
    assert env['ws_address'] == 'api.cnct.info'
    assert env['api_address'] == 'api.cnct.info'
    assert env['background_task_max_execution_time'] == BACKGROUND_TASK_MAX_EXECUTION_TIME
    assert env['interactive_task_max_execution_time'] == INTERACTIVE_TASK_MAX_EXECUTION_TIME
    assert env['scheduled_task_max_execution_time'] == SCHEDULED_TASK_MAX_EXECUTION_TIME


def test_get_environment(mocker):
    os.environ['API_KEY'] = 'SU-000:XXXX'
    os.environ['ENVIRONMENT_ID'] = 'ENV-0000-0000-01'
    os.environ['INSTANCE_ID'] = 'SIN-0000-0000-01'
    os.environ['SERVER_ADDRESS'] = 'api.example.com'
    os.environ['API_ADDRESS'] = 'api.example.com'
    os.environ['BACKGROUND_TASK_MAX_EXECUTION_TIME'] = '1'
    os.environ['INTERACTIVE_TASK_MAX_EXECUTION_TIME'] = '2'
    os.environ['SCHEDULED_TASK_MAX_EXECUTION_TIME'] = '3'

    env = get_environment()
    assert env['api_key'] == 'SU-000:XXXX'
    assert env['environment_id'] == 'ENV-0000-0000-01'
    assert env['instance_id'] == 'SIN-0000-0000-01'
    assert env['ws_address'] == 'api.example.com'
    assert env['api_address'] == 'api.example.com'
    assert env['background_task_max_execution_time'] == 1
    assert env['interactive_task_max_execution_time'] == 2
    assert env['scheduled_task_max_execution_time'] == 3


def test_get_extension_class(mocker):
    class MyExtension:
        pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.helpers.pkg_resources.iter_entry_points',
        return_value=iter([
            EntryPoint('extension', 'connect.eaas.ext'),
        ]),
    )

    extension_class = get_extension_class()

    assert extension_class == MyExtension


def test_get_extension_class_not_found(mocker):
    mocker.patch(
        'connect.eaas.helpers.pkg_resources.iter_entry_points',
        return_value=iter([
        ]),
    )

    assert get_extension_class() is None


def test_get_extension_type_ok_sync():
    class MyExtension:

        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': {
                    'asset_purchase_request_processing': [],
                    'asset_purchase_request_validation': [],
                },
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        def process_asset_purchase_request(self, request):
            pass

        def validate_asset_purchase_request(self, request):
            pass

    assert get_extension_type(MyExtension) == 'sync'


def test_get_extension_type_ok_async():
    class MyExtension:

        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': {
                    'asset_purchase_request_processing': [],
                    'asset_purchase_request_validation': [],
                },
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        async def process_asset_purchase_request(self, request):
            pass

        async def validate_asset_purchase_request(self, request):
            pass

    assert get_extension_type(MyExtension) == 'async'


def test_get_extension_type_ko():
    class MyExtension:

        @classmethod
        def get_descriptor(cls):
            return {
                'capabilities': {
                    'asset_purchase_request_processing': [],
                    'asset_purchase_request_validation': [],
                },
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        def process_asset_purchase_request(self, request):
            pass

        async def validate_asset_purchase_request(self, request):
            pass

    with pytest.raises(EaaSError) as cv:
        get_extension_type(MyExtension)

    assert (
        str(cv.value) == 'An Extension class can only have sync or async methods not a mix of both.'
    )
