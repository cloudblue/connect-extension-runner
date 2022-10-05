#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2022 Ingram Micro. All Rights Reserved.
#
import os
import logging
import subprocess
from datetime import datetime
from importlib.metadata import entry_points, version
from uuid import uuid4

import requests
from rich import box
from rich.align import Align
from rich.markdown import Markdown
from rich.table import Table
from rich.syntax import Syntax

from connect.client import ClientError, ConnectClient
from connect.eaas.core.validation.models import ValidationItem, ValidationResult
from connect.eaas.core.validation.validators import get_validators
from connect.eaas.runner.constants import (
    BACKGROUND_TASK_MAX_EXECUTION_TIME,
    HANDLER_CLASS_TITLE,
    INTERACTIVE_TASK_MAX_EXECUTION_TIME,
    ORDINAL_SUFFIX,
    PYPI_EXTENSION_RUNNER_URL,
    SCHEDULED_TASK_MAX_EXECUTION_TIME,
)

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


def get_version():
    try:
        return version('connect-extension-runner')
    except Exception:
        return '0.0.0'


def to_ordinal(val):
    if val > 14:
        return f"{val}{ORDINAL_SUFFIX.get(int(str(val)[-1]), 'th')}"
    return f"{val}{ORDINAL_SUFFIX.get(val, 'th')}"


def get_connect_version():
    connect_client = get_client()
    try:
        connect_client.accounts.all().first()
    except ClientError as ce:
        if ce.status_code in [401, 403]:
            raise Exception(
                'Cannot check the current EaaS Runner version: '
                f'API key is not valid: {ce}.')
        raise Exception(f'Cannot check the current EaaS Runner version: {ce}.')

    return connect_client.response.headers['Connect-Version']


def get_pypi_runner_minor_version(major_version):
    res = requests.get(PYPI_EXTENSION_RUNNER_URL)
    if res.status_code != 200:
        logger.error(
            f'Cannot check the current EaaS Runner version: {res.text}.',
        )
        raise Exception(f'Cannot check the current EaaS Runner version: {res.text}.')

    content = res.json()
    tags = [
        int(version.split('.')[1])
        for version in content['releases'] if version.startswith(f'{major_version}.')
    ]
    if tags:
        return str(max(tags))
    return content['info']['version'].split('.')[1]


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
                    'error_output': (
                        f'Process {process_type} worker of instance '
                        f'{instance_id} has been '
                        f'restarted at {datetime.now().isoformat()}'
                    ),
                },
            )
        except ClientError as ce:
            logger.warning(f'Cannot notify {process_type} process restart: {ce}')


def iter_entry_points(group, name=None):
    group_entrypoints = entry_points().get(group)
    if not group_entrypoints:
        return
    for ep in group_entrypoints:
        if name:
            if ep.name == name:
                yield ep
        else:
            yield ep


def configure_logger(debug):
    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'verbose': {
                    'format': '%(asctime)s %(name)s %(levelname)s PID_%(process)d %(message)s',
                },
            },
            'filters': {},
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'verbose',
                },
                'null': {
                    'class': 'logging.NullHandler',
                },
            },
            'loggers': {
                'connect.eaas': {
                    'handlers': ['console'],
                    'level': 'DEBUG' if debug else 'INFO',
                },
                'eaas': {
                    'handlers': ['console'],
                    'level': 'DEBUG' if debug else 'INFO',
                },
            },
        },
    )


def check_runner_version(context):
    try:
        connect_full_version = get_connect_version()
        connect_version = connect_full_version.split('.')[0]
        runner_version = get_version()
        latest_minor_version = get_pypi_runner_minor_version(connect_version)

        if not (
                connect_version == runner_version.split('.')[0]
                and runner_version.split('.')[1] == latest_minor_version
        ):
            return ValidationResult(
                items=[
                    ValidationItem(
                        level='ERROR',
                        message=(
                            'Runner is outdated, please, update. '
                            f'Required version {connect_version}.{latest_minor_version}, '
                            f'current version: {runner_version}.'
                        ),
                    ),
                ],
                must_exit=True,
            )
    except Exception as e:
        return ValidationResult(
            items=[
                ValidationItem(level='ERROR', message=str(e)),
            ],
            must_exit=True,
        )
    return ValidationResult()


def validate_extension():
    validation_items = []
    event_definitions = {
        definition['type']: definition
        for definition in get_client()('devops').event_definitions.all()
    }
    context = {
        'runner_version': get_version(),
        'project_dir': os.getcwd(),
        'event_definitions': event_definitions,
    }
    for validator in [check_runner_version, *get_validators()]:
        result = validator(context=context)
        validation_items.extend(result.items)
        if result.must_exit:
            break
        if result.context:
            context.update(result.context)

    higest_message_level = 'INFO'

    tables = []

    if validation_items:
        higest_message_level = 'WARNING'
        for item in validation_items:
            if item.level == 'ERROR':
                higest_message_level = 'ERROR'
            table = Table(
                box=box.ROUNDED,
                show_header=False,
            )
            table.add_column('Field', style='blue')
            table.add_column('Value', overflow='fold')
            level_color = 'red' if item.level == 'ERROR' else 'yellow'
            table.add_row('Level', f'[bold {level_color}]{item.level}[/]')
            table.add_row('Message', Markdown(item.message))
            table.add_row('File', item.file or '-')
            table.add_row(
                'Code',
                Syntax(
                    item.code,
                    'python3',
                    theme='ansi_light',
                    dedent=True,
                    line_numbers=True,
                    start_line=item.start_line,
                    highlight_lines={item.lineno},
                ) if item.code else '-',
            )
            tables.append(table)
    return higest_message_level, tables


def get_no_features_table():
    table = Table(
        box=box.ROUNDED,
        show_header=False,
    )
    table.add_column('Field', style='blue')
    table.add_column('Value')
    table.add_row('Level', '[bold red]ERROR[/]')
    table.add_row(
        'Message',
        (
            'No feature available for current extension. '
            'Did you run poetry install ?'
        ),
    )
    table.add_row('File', '-')
    table.add_row('Code', '-')
    return table


def get_anvilapp_detail_table(details):
    callables_table = None

    callables = details['features'].get('callables')

    if callables:
        callables_table = Table(
            box=box.MINIMAL_HEAVY_HEAD,
            title='Anvil Callables',
            title_style='blue',
            show_header=True,
            expand=True,
            row_styles=['', 'dim'],
        )
        callables_table.add_column('Summary')
        callables_table.add_column('Signature')
        for callable in callables:
            callables_table.add_row(callable['summary'], callable['signature'])

    return callables_table


def get_eventsapp_detail_table(details):
    events_table = None
    schedulables_table = None
    grid = None

    events = details['features'].get('events')
    schedulables = details['features'].get('schedulables')

    if events:
        events_table = Table(
            box=box.MINIMAL_HEAVY_HEAD,
            title='Event Subscriptions',
            title_style='blue',
            show_header=True,
            expand=True,
            row_styles=['', 'dim'],
        )
        events_table.add_column('Method Name')
        events_table.add_column('Event Type')
        events_table.add_column('Statuses')
        for event in events.values():
            events_table.add_row(
                event['method'],
                event['event_type'],
                ', '.join(event['statuses'] or []),
            )

    if schedulables:
        schedulables_table = Table(
            box=box.MINIMAL_HEAVY_HEAD,
            title='Schedulable Methods',
            title_style='blue',
            show_header=True,
            expand=True,
            row_styles=['', 'dim'],
        )
        schedulables_table.add_column('Method Name')
        schedulables_table.add_column('Name')
        schedulables_table.add_column('Description')
        for schedulable in schedulables:
            schedulables_table.add_row(
                schedulable['method'],
                schedulable['name'],
                schedulable['description'],
            )

    if events_table and schedulables_table:
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_row(events_table)
        grid.add_row(schedulables_table)

    return grid or events_table or schedulables_table


def get_webapp_detail_table(details):
    auth_table = None
    no_auth_table = None
    grid = None

    auth_endpoints = details['features']['endpoints'].get('auth')
    no_auth_endpoints = details['features']['endpoints'].get('no_auth')

    if auth_endpoints:
        auth_table = Table(
            box=box.MINIMAL_HEAVY_HEAD,
            title='Authenticated API Endpoints',
            title_style='blue',
            show_header=True,
            expand=True,
            row_styles=['', 'dim'],
        )
        auth_table.add_column('Summary')
        auth_table.add_column('Method')
        auth_table.add_column('Path')
        for endpoint in auth_endpoints:
            auth_table.add_row(
                endpoint['summary'],
                endpoint['method'],
                endpoint['path'],
            )

    if no_auth_endpoints:
        no_auth_table = Table(
            box=box.MINIMAL_HEAVY_HEAD,
            title='Non Authenticated API Endpoints',
            title_style='blue',
            show_header=True,
            expand=True,
            row_styles=['', 'dim'],
        )
        no_auth_table.add_column('Summary')
        no_auth_table.add_column('Method')
        no_auth_table.add_column('Path')
        for endpoint in no_auth_endpoints:
            no_auth_table.add_row(
                endpoint['summary'],
                endpoint['method'],
                endpoint['path'],
            )

    if auth_table and no_auth_table:
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_row(auth_table)
        grid.add_row(no_auth_table)

    return grid or auth_table or no_auth_table


def get_features_table(features):
    table = Table(
        box=box.ROUNDED,
        title='Available Features',
        title_style='blue',
        show_header=True,
        show_lines=True,
        expand=True,
    )
    table.add_column('Feature Name')
    table.add_column('Supported', width=9)
    table.add_column('Details')

    for feature_handler, details in features.items():
        if not details['available']:
            table.add_row(
                HANDLER_CLASS_TITLE[feature_handler],
                Align.center('[red]\u2716[/]'),
                '-',
            )
            continue
        if feature_handler == 'EventsApp':
            table.add_row(
                HANDLER_CLASS_TITLE[feature_handler],
                Align.center('[green]\u2714[/]'),
                Align.center(get_eventsapp_detail_table(details)),
            )
        elif feature_handler == 'WebApp':
            table.add_row(
                HANDLER_CLASS_TITLE[feature_handler],
                Align.center('[green]\u2714[/]'),
                Align.center(get_webapp_detail_table(details)),
            )
        elif feature_handler == 'AnvilApp':
            table.add_row(
                HANDLER_CLASS_TITLE[feature_handler],
                Align.center('[green]\u2714[/]'),
                Align.center(get_anvilapp_detail_table(details)),
            )
    return table
