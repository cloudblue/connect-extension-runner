#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import inspect
import logging
import os
import subprocess
from datetime import datetime
from uuid import uuid4

from connect.client import ClientError, ConnectClient

from pkg_resources import (
    DistributionNotFound,
    get_distribution,
    iter_entry_points,
)

from connect.eaas.constants import (
    BACKGROUND_TASK_MAX_EXECUTION_TIME,
    INTERACTIVE_TASK_MAX_EXECUTION_TIME,
    ORDINAL_SUFFIX,
    SCHEDULED_TASK_MAX_EXECUTION_TIME,
    TASK_TYPE_EXT_METHOD_MAP,
)
from connect.eaas.exceptions import EaaSError


logger = logging.getLogger('connect.eaas')


def get_container_id():
    result = subprocess.run(
        ['cat', '/proc/1/cpuset'],
        capture_output=True,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        return str(uuid4())

    _, container_id = result.stdout.decode()[:-1].rsplit('/', 1)
    if len(container_id) == 64:
        return container_id

    result = subprocess.run(
        ['grep', 'overlay', '/proc/self/mountinfo'],
        capture_output=True,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        result.check_returncode()
        mount = result.stdout.decode()
        start_idx = mount.index('upperdir=') + len('upperdir=')
        end_idx = mount.index(',', start_idx)
        dir_path = mount[start_idx:end_idx]
        _, container_id, _ = dir_path.rsplit('/', 2)
        if len(container_id) != 64:
            return str(uuid4())
        return container_id
    except (subprocess.CalledProcessError, ValueError):
        return str(uuid4())


def get_environment():
    return {
        'api_key': os.getenv('API_KEY'),
        'environment_id': os.getenv('ENVIRONMENT_ID'),
        'instance_id': os.getenv('INSTANCE_ID', get_container_id()),
        'ws_address': os.getenv('SERVER_ADDRESS', 'api.cnct.info'),
        'api_address': os.getenv('API_ADDRESS', os.getenv('SERVER_ADDRESS', 'api.cnct.info')),
        'background_task_max_execution_time': int(os.getenv(
            'BACKGROUND_TASK_MAX_EXECUTION_TIME', BACKGROUND_TASK_MAX_EXECUTION_TIME,
        )),
        'interactive_task_max_execution_time': int(os.getenv(
            'INTERACTIVE_TASK_MAX_EXECUTION_TIME', INTERACTIVE_TASK_MAX_EXECUTION_TIME,
        )),
        'scheduled_task_max_execution_time': int(os.getenv(
            'SCHEDULED_TASK_MAX_EXECUTION_TIME', SCHEDULED_TASK_MAX_EXECUTION_TIME,
        )),
    }


def get_extension_class():
    ext_class = next(iter_entry_points('connect.eaas.ext', 'extension'), None)
    return ext_class.load() if ext_class else None


def get_extension_type(cls):
    descriptor = cls.get_descriptor()
    guess_async = [
        inspect.iscoroutinefunction(getattr(cls, TASK_TYPE_EXT_METHOD_MAP[name]))
        for name in descriptor['capabilities'].keys()
    ] + [
        inspect.iscoroutinefunction(getattr(cls, schedulable['method']))
        for schedulable in descriptor.get('schedulables', [])
    ]

    if all(guess_async):
        return 'async'
    if not any(guess_async):
        return 'sync'

    raise EaaSError('An Extension class can only have sync or async methods not a mix of both.')


def get_version():
    try:
        return get_distribution('connect-extension-runner').version
    except DistributionNotFound:
        return '0.0.0'


def to_ordinal(val):
    if val > 14:
        return f"{val}{ORDINAL_SUFFIX.get(int(str(val)[-1]), 'th')}"
    return f"{val}{ORDINAL_SUFFIX.get(val, 'th')}"


def get_client():
    env = get_environment()
    api_key = env['api_key']
    api_address = env['api_address']
    return ConnectClient(
        api_key,
        endpoint=f'https://{api_address}/public/v1',
        use_specs=False,
        max_retries=3,
    )


def get_current_environment():
    env = get_environment()
    environment_id = env['environment_id']
    if environment_id.startswith('ENV-'):
        service_id = f'SRVC-{environment_id[4:-3]}'
        client = get_client()
        try:
            return (
                client('devops').services[service_id].environments[environment_id].get()
            )
        except ClientError as ce:
            logger.warning(f'Cannot retrieve environment information: {ce}')


def notify_process_restarted(process_type):
    env = get_environment()
    current_environment = get_current_environment()
    if current_environment and current_environment['runtime'] == 'cloud':
        instance_id = env['instance_id']
        environment_id = current_environment['id']
        service_id = f'SRVC-{environment_id[4:-3]}'
        client = get_client()
        try:
            client('devops').services[service_id].environments[environment_id].update(
                {
                    'runtime': 'cloud',
                    'error_output': (
                        f'Process {process_type} worker of instance '
                        f'{instance_id} has been '
                        f'restarted at {datetime.now().isoformat()}'
                    ),
                },
            )
        except ClientError as ce:
            logger.warning(f'Cannot notify {process_type} process restart: {ce}')
