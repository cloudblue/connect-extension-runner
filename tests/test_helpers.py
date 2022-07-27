#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import logging
import os
import subprocess

import pytest

from connect.client import ConnectClient

from freezegun import freeze_time
from pkg_resources import (
    DistributionNotFound,
    EntryPoint,
)
from responses import matchers

from connect.eaas.constants import (
    BACKGROUND_TASK_MAX_EXECUTION_TIME,
    INTERACTIVE_TASK_MAX_EXECUTION_TIME,
    SCHEDULED_TASK_MAX_EXECUTION_TIME,
)
from connect.eaas.exceptions import EaaSError
from connect.eaas.helpers import (
    get_client,
    get_container_id,
    get_current_environment,
    get_environment,
    get_extension_class,
    get_extension_type,
    get_version,
    notify_process_restarted,
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


def test_get_container_id_cgroups2(mocker):
    overlay_mount = (
        '776 562 0:178 / / rw,relatime master:163 - overlay overlay rw,'
        'lowerdir=/var/lib/docker/overlay2/l/MVS46I7BSZLIJ4CCFBC2HWYOA6,'
        'upperdir=/var/lib/docker/overlay2/d296001e9df3fe165e465fc7c37dee126d8dc7720f2a451b270cd7c1d4e9ce66/diff,'  # noqa: E501
        'workdir=/var/lib/docker/overlay2/d296001e9df3fe165e465fc7c37dee126d8dc7720f2a451b270cd7c1d4e9ce66/work\n'  # noqa: E501
    )
    result_cpuset = mocker.MagicMock()
    result_cpuset.returnvalue = 0
    result_cpuset.stdout = (
        '/\n'.encode('utf-8')
    )
    result = mocker.MagicMock()
    result.returnvalue = 0
    result.stdout = overlay_mount.encode('utf-8')
    mocked = mocker.patch(
        'connect.eaas.helpers.subprocess.run',
        side_effect=[result_cpuset, result],
    )
    assert get_container_id() == 'd296001e9df3fe165e465fc7c37dee126d8dc7720f2a451b270cd7c1d4e9ce66'
    assert mocked.mock_calls[0].args[0] == ['cat', '/proc/1/cpuset']
    assert mocked.mock_calls[1].args[0] == ['grep', 'overlay', '/proc/self/mountinfo']


def test_get_container_id_cgroups2_fallback(mocker):
    overlay_mount = (
        '776 562 0:178 / / rw,relatime master:163 - overlay overlay rw,'
        'lowerdir=/var/lib/docker/overlay2/l/MVS46I7BSZLIJ4CCFBC2HWYOA6,'
        'workdir=/var/lib/docker/overlay2/d296001e9df3fe165e465fc7c37dee126d8dc7720f2a451b270cd7c1d4e9ce66/work\n'  # noqa: E501
    )
    mocker.patch('connect.eaas.helpers.uuid4', return_value='test_uuid')
    result_cpuset = mocker.MagicMock()
    result_cpuset.returnvalue = 0
    result_cpuset.stdout = (
        '/\n'.encode('utf-8')
    )
    result = mocker.MagicMock()
    result.returnvalue = 0
    result.stdout = overlay_mount.encode('utf-8')
    mocked = mocker.patch(
        'connect.eaas.helpers.subprocess.run',
        side_effect=[result_cpuset, result],
    )
    assert get_container_id() == 'test_uuid'
    assert mocked.mock_calls[0].args[0] == ['cat', '/proc/1/cpuset']
    assert mocked.mock_calls[1].args[0] == ['grep', 'overlay', '/proc/self/mountinfo']


def test_get_container_id_cgroups2_call_error(mocker):
    mocker.patch('connect.eaas.helpers.uuid4', return_value='test_uuid')
    result_cpuset = mocker.MagicMock()
    result_cpuset.returnvalue = 0
    result_cpuset.stdout = (
        '/\n'.encode('utf-8')
    )
    result = mocker.MagicMock()
    result.check_returncode = mocker.MagicMock(
        side_effect=subprocess.CalledProcessError(128, cmd=[]),
    )
    mocked = mocker.patch(
        'connect.eaas.helpers.subprocess.run',
        side_effect=[result_cpuset, result],
    )
    assert get_container_id() == 'test_uuid'
    assert mocked.mock_calls[0].args[0] == ['cat', '/proc/1/cpuset']
    assert mocked.mock_calls[1].args[0] == ['grep', 'overlay', '/proc/self/mountinfo']


def test_get_container_id_invalid_mount_container_id(mocker):
    mocker.patch('connect.eaas.helpers.uuid4', return_value='test_uuid')
    result1 = mocker.MagicMock()
    result1.returnvalue = 0
    result1.stdout.decode = mocker.MagicMock(
        side_effect=['/hello/'],
    )
    result2 = mocker.MagicMock()
    result2.stdout.decode = mocker.MagicMock(
        return_value='upperdir=/hello/you/,you,\n',
    )
    mocker.patch(
        'connect.eaas.helpers.subprocess.run',
        side_effect=[result1, result2],
    )
    assert get_container_id() == 'test_uuid'


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
        'connect.eaas.helpers.iter_entry_points',
        return_value=iter([
            EntryPoint('extension', 'connect.eaas.ext'),
        ]),
    )

    extension_class = get_extension_class()

    assert extension_class == MyExtension


def test_get_extension_class_not_found(mocker):
    mocker.patch(
        'connect.eaas.helpers.iter_entry_points',
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


def test_get_version(mocker):
    mocked = mocker.MagicMock()
    mocked.version = '22.0'
    mocker.patch(
        'connect.eaas.helpers.get_distribution',
        return_value=mocked,
    )
    assert get_version() == '22.0'


def test_get_version_exception(mocker):
    mocker.patch(
        'connect.eaas.helpers.get_distribution',
        side_effect=DistributionNotFound(),
    )
    assert get_version() == '0.0.0'


def test_get_client(mocker):
    env = {
        'api_key': 'ApiKey XXXX:YYYY',
        'api_address': 'api.example.com',
    }
    mocker.patch('connect.eaas.helpers.get_environment', return_value=env)

    client = get_client()

    assert isinstance(client, ConnectClient)
    assert client.api_key == 'ApiKey XXXX:YYYY'
    assert client.endpoint == 'https://api.example.com/public/v1'


def test_get_current_environment(mocker, responses):
    mocker.patch(
        'connect.eaas.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'environment_id': 'ENV-0000-1111-01',
        },
    )

    responses.add(
        'GET',
        (
            'https://api.example.com/public/v1/devops/services'
            '/SRVC-0000-1111/environments/ENV-0000-1111-01'
        ),
        json={
            'id': 'ENV-0000-1111-01',
        },
        status=200,
    )

    assert get_current_environment() == {'id': 'ENV-0000-1111-01'}


def test_get_current_environment_client_error(mocker, responses, caplog):
    mocker.patch(
        'connect.eaas.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'environment_id': 'ENV-0000-1111-01',
        },
    )

    responses.add(
        'GET',
        (
            'https://api.example.com/public/v1/devops/services'
            '/SRVC-0000-1111/environments/ENV-0000-1111-01'
        ),
        status=400,
    )

    with caplog.at_level(logging.WARNING):
        assert get_current_environment() is None

    assert 'Cannot retrieve environment information' in caplog.text


def test_get_current_environment_using_uuid(mocker):
    mocker.patch(
        'connect.eaas.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'environment_id': 'fake_uuid',
        },
    )

    assert get_current_environment() is None


def test_notify_process_restarted(mocker, responses):
    mocker.patch(
        'connect.eaas.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'instance_id': 'instance_id',
        },
    )
    mocker.patch(
        'connect.eaas.helpers.get_current_environment',
        return_value={
            'id': 'ENV-0000-1111-01',
            'runtime': 'cloud',
        },
    )

    responses.add(
        'PUT',
        (
            'https://api.example.com/public/v1/devops/services'
            '/SRVC-0000-1111/environments/ENV-0000-1111-01'
        ),
        match=[
            matchers.json_params_matcher(
                {
                    'runtime': 'cloud',
                    'error_output': (
                        'Process background worker of instance instance_id has been '
                        'restarted at 2022-01-01T12:00:00'
                    ),
                },
            ),
        ],
        json={},
        status=200,
    )
    with freeze_time('2022-01-01 12:00:00'):
        notify_process_restarted('background')


def test_notify_process_restarted_client_error(mocker, responses, caplog):
    mocker.patch(
        'connect.eaas.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'instance_id': 'instance_id',
        },
    )
    mocker.patch(
        'connect.eaas.helpers.get_current_environment',
        return_value={
            'id': 'ENV-0000-1111-01',
            'runtime': 'cloud',
        },
    )

    responses.add(
        'PUT',
        (
            'https://api.example.com/public/v1/devops/services'
            '/SRVC-0000-1111/environments/ENV-0000-1111-01'
        ),
        status=400,
    )
    with caplog.at_level(logging.WARNING):
        notify_process_restarted('background')

    assert 'Cannot notify background process restart' in caplog.text
