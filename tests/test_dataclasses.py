from connect.eaas.dataclasses import (
    CapabilitiesPayload,
    ConfigurationPayload,
    Message,
    MessageType,

)


def test_capabilities_payload():
    assert CapabilitiesPayload(
        {'cap1': 'val1'},
        'https://example.com/readme',
        'https://example.com/changelog',
    ).to_json() == {
        'capabilities': {'cap1': 'val1'},
        'readme_url': 'https://example.com/readme',
        'changelog_url': 'https://example.com/changelog',
    }


def test_configuration_payload():
    assert ConfigurationPayload(
        {'conf1': 'val1'},
        'logging-token',
        'environ-type',
        'log-level',
        'runner-log-level',
    ).to_json() == {
        'configuration': {'conf1': 'val1'},
        'logging_api_key': 'logging-token',
        'environment_type': 'environ-type',
        'log_level': 'log-level',
        'runner_log_level': 'runner-log-level',
    }


def test_message_capabilities():
    cap = CapabilitiesPayload(
        {'cap1': 'val1'},
        'https://example.com/readme',
        'https://example.com/changelog',
    )

    msg = Message(
        MessageType.CAPABILITIES,
        cap.to_json(),
    )
    assert msg.data == cap

    assert msg.to_json() == {
        'message_type': MessageType.CAPABILITIES,
        'data': cap.to_json(),
    }
