import dataclasses

from connect.eaas.dataclasses import (
    CapabilitiesPayload,
    ConfigurationPayload,
    from_dict,
    Message,
    MessageType,
    parse_message,
    TaskPayload,
)


def test_from_dict():
    data = {
        'capabilities': {'test': 'data'},
        'variables': [],
        'readme_url': 'https://read.me',
        'changelog_url': 'https://change.log',
        'extra': 'data',
    }
    capabilities = from_dict(CapabilitiesPayload, data)

    assert capabilities.capabilities == data['capabilities']
    assert capabilities.changelog_url == data['changelog_url']
    assert capabilities.readme_url == data['readme_url']


def test_parse_task_message():
    msg_data = {
        'message_type': 'task',
        'data': {
            'task_id': 'task_id',
            'task_category': 'task_category',
            'task_type': 'task_type',
            'object_id': 'object_id',
            'result': 'result',
            'data': {'data': 'value'},
            'countdown': 10,
            'output': 'output',
            'correlation_id': 'correlation_id',
            'reply_to': 'reply_to',
        },
    }

    message = parse_message(msg_data)

    assert isinstance(message, Message)
    assert message.message_type == MessageType.TASK
    assert isinstance(message.data, TaskPayload)

    assert dataclasses.asdict(message) == msg_data


def test_parse_capabilities_message():
    msg_data = {
        'message_type': 'capabilities',
        'data': {
            'capabilities': {'test': 'data'},
            'variables': [],
            'schedulables': [],
            'readme_url': 'https://read.me',
            'changelog_url': 'https://change.log',
        },
    }

    message = parse_message(msg_data)

    assert isinstance(message, Message)
    assert message.message_type == MessageType.CAPABILITIES
    assert isinstance(message.data, CapabilitiesPayload)

    assert dataclasses.asdict(message) == msg_data


def test_parse_configuration_message():
    msg_data = {
        'message_type': 'configuration',
        'data': {
            'configuration': {'conf1': 'val1'},
            'logging_api_key': 'logging-token',
            'environment_type': 'environ-type',
            'log_level': 'log-level',
            'runner_log_level': 'runner-log-level',
            'account_id': 'account_id',
            'account_name': 'account_name',
            'service_id': 'service_id',
        },
    }

    message = parse_message(msg_data)

    assert isinstance(message, Message)
    assert message.message_type == MessageType.CONFIGURATION
    assert isinstance(message.data, ConfigurationPayload)

    assert dataclasses.asdict(message) == msg_data


def test_parse_capabilities_message_with_vars():
    msg_data = {
        'message_type': 'capabilities',
        'data': {
            'capabilities': {'test': 'data'},
            'variables': [{'foo': 'value', 'bar': 'value'}],
            'schedulables': None,
            'readme_url': 'https://read.me',
            'changelog_url': 'https://change.log',
        },
    }

    message = parse_message(msg_data)

    assert isinstance(message, Message)
    assert message.message_type == MessageType.CAPABILITIES
    assert isinstance(message.data, CapabilitiesPayload)

    assert dataclasses.asdict(message) == msg_data
    assert message.data.variables == msg_data['data']['variables']
    assert message.data.variables[0]['foo'] == msg_data['data']['variables'][0]['foo']
    assert message.data.variables[0]['bar'] == msg_data['data']['variables'][0]['bar']


def test_parse_capabilities_message_with_schedulables():
    msg_data = {
        'message_type': 'capabilities',
        'data': {
            'capabilities': {'test': 'data'},
            'variables': None,
            'schedulables': [
                {
                    'method': 'method_name',
                    'name': 'Name',
                    'description': 'Description',
                },
            ],
            'readme_url': 'https://read.me',
            'changelog_url': 'https://change.log',
        },
    }

    message = parse_message(msg_data)

    assert isinstance(message, Message)
    assert message.message_type == MessageType.CAPABILITIES
    assert isinstance(message.data, CapabilitiesPayload)

    assert dataclasses.asdict(message) == msg_data
    assert message.data.schedulables == msg_data['data']['schedulables']
