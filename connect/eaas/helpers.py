#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#
import inspect
import os
import subprocess
from uuid import uuid4

import pkg_resources

from connect.eaas.constants import TASK_TYPE_EXT_METHOD_MAP
from connect.eaas.exceptions import EaaSError


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
    return container_id


def get_environment():
    return {
        'api_key': os.getenv('API_KEY'),
        'environment_id': os.getenv('ENVIRONMENT_ID'),
        'instance_id': os.getenv('INSTANCE_ID', get_container_id()),
        'ws_address': os.getenv('SERVER_ADDRESS', 'api.cnct.info'),
        'api_address': os.getenv('API_ADDRESS', os.getenv('SERVER_ADDRESS', 'api.cnct.info')),
    }


def get_extension_class():
    ext_class = next(pkg_resources.iter_entry_points('connect.eaas.ext', 'extension'), None)
    return ext_class.load() if ext_class else None


def get_extension_type(cls):
    descriptor = cls.get_descriptor()
    guess_async = [
        inspect.iscoroutinefunction(getattr(cls, TASK_TYPE_EXT_METHOD_MAP[name]))
        for name in descriptor['capabilities'].keys()
    ]

    if all(guess_async):
        return 'async'
    if not any(guess_async):
        return 'sync'

    raise EaaSError('An Extension class can only have sync or async methods not a mix of both.')
