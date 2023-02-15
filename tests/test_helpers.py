#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import logging
import os
import subprocess

import pytest
from connect.client import (
    ConnectClient,
)
from freezegun import (
    freeze_time,
)
from responses import (
    matchers,
)
from rich.table import (
    Table,
)

from connect.eaas.core.validation.models import (
    ValidationItem,
    ValidationResult,
)
from connect.eaas.runner.constants import (
    BACKGROUND_TASK_MAX_EXECUTION_TIME,
    INTERACTIVE_TASK_MAX_EXECUTION_TIME,
    SCHEDULED_TASK_MAX_EXECUTION_TIME,
)
from connect.eaas.runner.helpers import (
    check_runner_version,
    get_anvilapp_detail_table,
    get_client,
    get_connect_version,
    get_container_id,
    get_current_environment,
    get_environment,
    get_eventsapp_detail_table,
    get_features_table,
    get_no_features_table,
    get_pypi_runner_latest_version,
    get_tfnapp_detail_table,
    get_version,
    get_webapp_detail_table,
    iter_entry_points,
    notify_process_restarted,
    validate_extension,
)


def test_get_container_id(mocker):
    result = mocker.MagicMock()
    result.returnvalue = 0
    result.stdout = (
        '/docker/e724f0291e6b5a3a37b1c0eb83daa6ee1df5c52f40a01d49f481908766251791\n'.encode('utf-8')
    )
    mocked = mocker.patch(
        'connect.eaas.runner.helpers.subprocess.run',
        return_value=result,
    )
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
        'connect.eaas.runner.helpers.subprocess.run',
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
    mocker.patch('connect.eaas.runner.helpers.uuid4', return_value='test_uuid')
    result_cpuset = mocker.MagicMock()
    result_cpuset.returnvalue = 0
    result_cpuset.stdout = (
        '/\n'.encode('utf-8')
    )
    result = mocker.MagicMock()
    result.returnvalue = 0
    result.stdout = overlay_mount.encode('utf-8')
    mocked = mocker.patch(
        'connect.eaas.runner.helpers.subprocess.run',
        side_effect=[result_cpuset, result],
    )
    assert get_container_id() == 'test_uuid'
    assert mocked.mock_calls[0].args[0] == ['cat', '/proc/1/cpuset']
    assert mocked.mock_calls[1].args[0] == ['grep', 'overlay', '/proc/self/mountinfo']


def test_get_container_id_cgroups2_call_error(mocker):
    mocker.patch('connect.eaas.runner.helpers.uuid4', return_value='test_uuid')
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
        'connect.eaas.runner.helpers.subprocess.run',
        side_effect=[result_cpuset, result],
    )
    assert get_container_id() == 'test_uuid'
    assert mocked.mock_calls[0].args[0] == ['cat', '/proc/1/cpuset']
    assert mocked.mock_calls[1].args[0] == ['grep', 'overlay', '/proc/self/mountinfo']


def test_get_container_id_invalid_mount_container_id(mocker):
    mocker.patch('connect.eaas.runner.helpers.uuid4', return_value='test_uuid')
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
        'connect.eaas.runner.helpers.subprocess.run',
        side_effect=[result1, result2],
    )
    assert get_container_id() == 'test_uuid'


def test_get_container_id_ko(mocker):
    result = mocker.MagicMock()
    result.check_returncode = mocker.MagicMock(
        side_effect=subprocess.CalledProcessError(128, cmd=[]),
    )
    result.stderr = 'error message'.encode('utf-8')
    mocker.patch('connect.eaas.runner.helpers.subprocess.run', return_value=result)
    expected_result = get_container_id()

    assert isinstance(expected_result, str)
    assert len(expected_result) > 0


def test_get_environment_with_defaults(mocker):
    mocker.patch.dict(
        os.environ,
        {
            'API_KEY': 'SU-000:XXXX',
            'ENVIRONMENT_ID': 'ENV-0000-0000-01',
        },
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_container_id',
        return_value='container_id',
    )
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
    mocker.patch.dict(
        os.environ,
        {
            'API_KEY': 'SU-000:XXXX',
            'ENVIRONMENT_ID': 'ENV-0000-0000-01',
            'INSTANCE_ID': 'SIN-0000-0000-01',
            'SERVER_ADDRESS': 'api.example.com',
            'API_ADDRESS': 'api.example.com',
            'BACKGROUND_TASK_MAX_EXECUTION_TIME': '1',
            'INTERACTIVE_TASK_MAX_EXECUTION_TIME': '2',
            'SCHEDULED_TASK_MAX_EXECUTION_TIME': '3',
        },
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_container_id',
        return_value='container_id',
    )

    env = get_environment()
    assert env['api_key'] == 'SU-000:XXXX'
    assert env['environment_id'] == 'ENV-0000-0000-01'
    assert env['instance_id'] == 'SIN-0000-0000-01'
    assert env['ws_address'] == 'api.example.com'
    assert env['api_address'] == 'api.example.com'
    assert env['background_task_max_execution_time'] == 1
    assert env['interactive_task_max_execution_time'] == 2
    assert env['scheduled_task_max_execution_time'] == 3


def test_get_version(mocker):
    mocker.patch(
        'connect.eaas.runner.helpers.version',
        return_value='22.0',
    )
    assert get_version() == '22.0'


def test_get_version_exception(mocker):
    mocker.patch(
        'connect.eaas.runner.helpers.version',
        side_effect=Exception('error'),
    )
    assert get_version() == '0.0.0'


def test_get_connect_version(mocker, responses):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
        return_value={'api_address': 'localhost', 'api_key': 'ApiKey test:key'},
    )
    responses.add(
        'GET',
        'https://localhost/public/v1/accounts',
        json=[{}],
        status=200,
        headers={'Connect-Version': '26.0.12-gjg2312'},
    )

    version = get_connect_version()

    assert version == '26.0.12-gjg2312'


def test_get_connect_version_server_error(mocker, responses):
    mocker.patch(
        'connect.eaas.runner.helpers.get_client',
        return_value=ConnectClient('api', endpoint='https://localhost/public/v1', max_retries=0),
    )
    responses.add(
        'GET',
        'https://localhost/public/v1/accounts',
        status=500,
    )

    with pytest.raises(Exception) as cv:
        get_connect_version()

    assert str(cv.value) == (
        'Cannot check the current EaaS Runner version: 500 Internal Server Error.'
    )


def test_get_connect_version_api_key_error(mocker, responses):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
        return_value={'api_address': 'localhost', 'api_key': 'ApiKey test:key'},
    )
    responses.add(
        'GET',
        'https://localhost/public/v1/accounts',
        json={"error_code": "AUTH_001", "errors": ["API request is unauthorized."]},
        status=401,
    )

    with pytest.raises(Exception) as cv:
        get_connect_version()

    assert (
        'Cannot check the current EaaS Runner version: '
        'API key is not valid'
    ) in str(cv.value)


def test_get_pypi_runner_latest_version(responses):
    responses.add(
        'GET',
        'https://pypi.org/pypi/connect-extension-runner/json',
        json={
            'releases': {
                '26.0': 'v1',
                '26.12': 'v2',
                '25.1': '-v3',
                '26.13': 'v4',
            },
            'info': {'version': '26.13'},
        },
        status=200,
    )

    latest_version = get_pypi_runner_latest_version('26')

    assert latest_version == '26.13'


def test_get_pypi_runner_latest_version_no_releases(responses):
    responses.add(
        'GET',
        'https://pypi.org/pypi/connect-extension-runner/json',
        json={
            'releases': {},
            'info': {'version': '26.13'},
        },
        status=200,
    )

    latest_version = get_pypi_runner_latest_version('26')

    assert latest_version == '26.13'


def test_get_pypi_runner_latest_version_request_error(responses):
    responses.add(
        'GET',
        'https://pypi.org/pypi/connect-extension-runner/json',
        body='error',
        status=400,
    )

    with pytest.raises(Exception) as cv:
        get_pypi_runner_latest_version('26')

    assert str(cv.value) == 'Cannot check the current EaaS Runner version: error.'


def test_get_client(mocker):
    env = {
        'api_key': 'ApiKey XXXX:YYYY',
        'api_address': 'api.example.com',
    }
    mocker.patch('connect.eaas.runner.helpers.get_environment', return_value=env)

    client = get_client()

    assert isinstance(client, ConnectClient)
    assert client.api_key == 'ApiKey XXXX:YYYY'
    assert client.endpoint == 'https://api.example.com/public/v1'


def test_get_current_environment(mocker, responses):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
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
        'connect.eaas.runner.helpers.get_environment',
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
        'connect.eaas.runner.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'environment_id': 'fake_uuid',
        },
    )

    assert get_current_environment() is None


def test_notify_process_restarted(mocker, responses):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'instance_id': 'instance_id',
        },
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_current_environment',
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
        'connect.eaas.runner.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey XXXX:YYYY',
            'api_address': 'api.example.com',
            'instance_id': 'instance_id',
        },
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_current_environment',
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


def test_iter_entry_points(mocker):
    ep1 = mocker.MagicMock()
    ep1.name = 'ep1'
    ep2 = mocker.MagicMock()
    ep2.name = 'ep2'
    mocker.patch(
        'connect.eaas.runner.helpers.entry_points',
        return_value={'ep.group': [ep1, ep2]},
    )

    assert list(iter_entry_points('ep.group', name='ep1')) == [ep1]
    assert list(iter_entry_points('ep.group')) == [ep1, ep2]
    assert list(iter_entry_points('ep.other_group')) == []


def test_check_runner_version(mocker):
    mocker.patch(
        'connect.eaas.runner.helpers.get_connect_version',
        return_value='26.0.0',
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_version',
        return_value='26.13',
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_pypi_runner_latest_version',
        return_value='26.13',
    )

    result = check_runner_version({})
    assert isinstance(result, ValidationResult)
    assert len(result.items) == 0
    assert result.must_exit is False


def test_check_runner_version_outdated(mocker):
    mocker.patch(
        'connect.eaas.runner.helpers.get_connect_version',
        return_value='26.0.0',
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_version',
        return_value='26.13',
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_pypi_runner_latest_version',
        return_value='26.14',
    )

    result = check_runner_version({})
    assert isinstance(result, ValidationResult)
    assert len(result.items) == 1
    assert result.must_exit is True
    assert isinstance(result.items[0], ValidationItem)
    assert result.items[0].level == 'ERROR'
    assert 'Runner is outdated, please, update.' in result.items[0].message


def test_check_runner_version_exception(mocker):
    mocker.patch(
        'connect.eaas.runner.helpers.get_connect_version',
        side_effect=Exception('hello'),
    )

    result = check_runner_version({})
    assert isinstance(result, ValidationResult)
    assert len(result.items) == 1
    assert result.must_exit is True
    assert isinstance(result.items[0], ValidationItem)
    assert result.items[0].level == 'ERROR'
    assert 'hello' in result.items[0].message


def test_validate_extension(mocker, client_mocker_factory):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey',
            'api_address': 'localhost',
        },
    )
    client_mock = client_mocker_factory(base_url='https://localhost/public/v1')
    client_mock('devops').event_definitions.all().mock(return_value=[])
    mocker.patch(
        'connect.eaas.runner.helpers.check_runner_version',
        return_value=ValidationResult(),
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_validators',
        return_value=[],
    )
    higest_message_level, tables = validate_extension()

    assert higest_message_level == 'INFO'
    assert tables == []


def test_validate_extension_warning(mocker, client_mocker_factory):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey',
            'api_address': 'localhost',
        },
    )
    client_mock = client_mocker_factory(base_url='https://localhost/public/v1')
    client_mock('devops').event_definitions.all().mock(return_value=[])
    mocker.patch(
        'connect.eaas.runner.helpers.check_runner_version',
        return_value=ValidationResult(
            items=[
                ValidationItem(level='WARNING', message='A warning'),
            ],
            context={'updated': 'context'},
        ),
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_validators',
        return_value=[],
    )
    higest_message_level, tables = validate_extension()

    assert higest_message_level == 'WARNING'
    assert len(tables) > 0


def test_validate_extension_error(mocker, client_mocker_factory):
    mocker.patch(
        'connect.eaas.runner.helpers.get_environment',
        return_value={
            'api_key': 'ApiKey',
            'api_address': 'localhost',
        },
    )
    client_mock = client_mocker_factory(base_url='https://localhost/public/v1')
    client_mock('devops').event_definitions.all().mock(return_value=[])
    mocker.patch(
        'connect.eaas.runner.helpers.check_runner_version',
        return_value=ValidationResult(
            items=[
                ValidationItem(level='ERROR', message='An error'),
            ],
            context={},
            must_exit=True,
        ),
    )
    mocker.patch(
        'connect.eaas.runner.helpers.get_validators',
        return_value=[],
    )
    higest_message_level, tables = validate_extension()

    assert higest_message_level == 'ERROR'
    assert len(tables) > 0


def test_get_no_features_table():
    assert isinstance(get_no_features_table(), Table)


def test_anvilapp_detail_table():
    features = {
        'callables': [
            {
                'method': 'summary(self, param)',
                'summary': 'Summary',
                'description': 'Description',
            },
        ],
    }
    assert isinstance(get_anvilapp_detail_table({'features': features}), Table)


def test_eventsapp_detail_table():
    features = {
        'events': {
            'test_event': {
                'event_type': 'test_event',
                'statuses': ['pending', 'accepted'],
                'method': 'my_method',
            },
        },
        'schedulables': [
            {
                'method': 'my_schedulable_method',
                'name': 'My schedulable',
                'description': 'description',
            },
        ],
    }

    assert isinstance(get_eventsapp_detail_table({'features': features}), Table)


def test_websapp_detail_table():
    features = {
        'endpoints': {
            'auth': [
                {
                    'method': 'GET',
                    'path': '/auth',
                    'summary': 'Example Auth',
                },
            ],
            'no_auth': [
                {
                    'method': 'GET',
                    'path': '/no_auth',
                    'summary': 'Example No Auth',
                },
            ],
        },
    }

    assert isinstance(get_webapp_detail_table({'features': features}), Table)


def test_tfnapp_detail_table():
    features = {
        'transformations': [
            {
                'name': 'Test transformation',
                'description': 'Description',
                'edit_dialog_ui': '/static/settings.html',
                'method': 'package.tfnapp.TestTransformation',
            },
        ],
    }
    assert isinstance(get_tfnapp_detail_table({'features': features}), Table)


def test_get_features_table():
    features = {
        'AnvilApp': {
            'available': True,
            'features': {
                'callables': [
                    {
                        'method': 'summary(self, param)',
                        'summary': 'Summary',
                        'description': 'Description',
                    },
                ],
            },
        },
        'EventsApp': {
            'available': True,
            'features': {
                'events': {
                    'test_event': {
                        'event_type': 'test_event',
                        'statuses': ['pending', 'accepted'],
                        'method': 'my_method',
                    },
                },
                'schedulables': [
                    {
                        'method': 'my_schedulable_method',
                        'name': 'My schedulable',
                        'description': 'description',
                    },
                ],
            },
        },
        'WebApp': {
            'available': True,
            'features': {
                'endpoints': {
                    'auth': [
                        {
                            'method': 'GET',
                            'path': '/auth',
                            'summary': 'Example Auth',
                        },
                    ],
                    'no_auth': [
                        {
                            'method': 'GET',
                            'path': '/no_auth',
                            'summary': 'Example No Auth',
                        },
                    ],
                },
                'ui_modules': [
                    {
                        'name': 'Name',
                        'url': '/static/name.html',
                        'integration_point': 'Account Settings',
                    },
                ],
            },
        },
        'TfnApp': {
            'available': True,
            'features': {
                'transformations': [
                    {
                        'name': 'Test transformation',
                        'description': 'Description',
                        'edit_dialog_ui': '/static/settings.html',
                        'method': 'package.tfnapp.TestTransformation',
                    },
                ],
            },
        },
    }

    assert isinstance(get_features_table(features), Table)
