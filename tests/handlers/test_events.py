import os

import pytest

from connect.eaas.core.proto import (
    Logging,
    LogMeta,
    SetupResponse,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.events import (
    EventsApp,
)


def test_get_method(mocker, settings_payload, extension_cls):
    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.runner.handlers.base.logging.getLogger')
    ext_class = extension_cls('test_method')
    mocker.patch.object(ext_class, 'get_descriptor')
    mocker.patch.object(
        EventsApp,
        'load_application',
        return_value=ext_class,
    )
    mocked_log_handler = mocker.patch(
        'connect.eaas.runner.handlers.base.ExtensionLogHandler',
        autospec=True,
    )
    handler = EventsApp(config)

    method = handler.get_method('event_type', 'TQ-000', 'test_method')
    assert method.__name__ == 'test_method'
    assert method.__self__.__class__ == ext_class

    mocked_log_handler.assert_called_once_with(
        config.logging_api_key,
        default_extra_fields=config.metadata,
    )


@pytest.mark.parametrize(
    'entrypoint_name',
    ('eventsapp', 'extension'),
)
def test_get_extension_class(mocker, settings_payload, entrypoint_name):

    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return {}

        @classmethod
        def get_events(cls):
            return []

        @classmethod
        def get_schedulables(cls):
            return []

        @classmethod
        def get_variables(cls):
            pass

    mocked_load = mocker.patch.object(EventsApp, 'load_application', return_value=MyExtension)

    handler = EventsApp(config)

    assert handler.get_application() == MyExtension
    mocked_load.assert_called_once_with('eventsapp')

    mocked_load = mocker.patch.object(
        EventsApp, 'load_application', side_effect=[None, MyExtension],
    )

    handler = EventsApp(config)

    assert handler.get_application() == MyExtension
    assert mocked_load.call_count == 2
    assert mocked_load.mock_calls[1].args[0] == 'extension'


def test_get_method_multi_account(mocker, settings_payload, extension_cls):
    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.runner.handlers.base.logging.getLogger')
    test_random_bytes = os.urandom(8)
    mocker.patch('connect.eaas.runner.handlers.events.os.urandom', return_value=test_random_bytes)
    ext_class = extension_cls('test_method')
    mocker.patch.object(ext_class, 'get_descriptor')
    mocker.patch.object(
        EventsApp,
        'load_application',
        return_value=ext_class,
    )
    mocked_log_handler = mocker.patch(
        'connect.eaas.runner.handlers.base.ExtensionLogHandler',
        autospec=True,
    )
    handler = EventsApp(config)

    method = handler.get_method(
        'event_type',
        'TQ-000',
        'test_method',
        installation={'installation': 'data'},
        api_key='installation_key',
        connect_correlation_id='correlation_id-for-events-application',
    )
    assert method.__name__ == 'test_method'
    assert method.__self__.__class__ == ext_class

    assert method.__self__.installation == {'installation': 'data'}
    assert method.__self__.installation_client.api_key == 'installation_key'

    default_headers = method.__self__.installation_client.default_headers
    assert 'ext-traceparent' in default_headers
    correlation_id = f'00-relation_id-for-events-applicat-{test_random_bytes.hex()}-01'
    assert default_headers['ext-traceparent'] == correlation_id

    mocked_log_handler.assert_called_once_with(
        config.logging_api_key,
        default_extra_fields=config.metadata,
    )


def test_get_method_multi_account_no_cor_id(mocker, settings_payload, extension_cls):
    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.runner.handlers.base.logging.getLogger')
    ext_class = extension_cls('test_method')
    mocker.patch.object(ext_class, 'get_descriptor')
    mocker.patch.object(
        EventsApp,
        'load_application',
        return_value=ext_class,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.base.ExtensionLogHandler',
        autospec=True,
    )
    handler = EventsApp(config)

    method = handler.get_method(
        'event_type',
        'TQ-000',
        'test_method',
        installation={'installation': 'data'},
        api_key='installation_key',
    )

    assert method.__self__.installation == {'installation': 'data'}
    assert method.__self__.installation_client.api_key == 'installation_key'
    assert 'ext-traceparent' not in method.__self__.installation_client.default_headers


def test_properties(mocker):

    descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
    }

    variables = [
        {
            'name': 'var1',
            'initial_value': 'val1',
        },
    ]

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return descriptor

        @classmethod
        def get_events(cls):
            return [
                {
                    'event_type': 'test_event',
                    'statuses': ['pending', 'accepted'],
                    'method': 'my_method',
                },
            ]

        @classmethod
        def get_schedulables(cls):
            return [
                {
                    'method': 'my_schedulable_method',
                    'name': 'My schedulable',
                    'description': 'description',
                },
            ]

        @classmethod
        def get_variables(cls):
            return variables

    mocker.patch.object(
        EventsApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = EventsApp(config)

    assert handler.get_application() == MyExtension
    assert handler.events == {
        'test_event': {
            'event_type': 'test_event',
            'statuses': ['pending', 'accepted'],
            'method': 'my_method',
        },
    }
    assert handler.variables == variables
    assert handler.config == config
    assert handler.schedulables == MyExtension.get_schedulables()
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.audience is None
    assert handler.should_start is True


def test_properties_legacy_extension(mocker):

    descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'capabilities': {
            'asset_purchase_request_processing': ['pending', 'accepted'],
        },
        'schedulables': [
            {
                'method': 'my_schedulable_method',
                'name': 'My schedulable',
                'description': 'description',
            },
        ],
        'variables': [
            {
                'name': 'var1',
                'initial_value': 'val1',
            },
        ],
    }

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return descriptor

        @classmethod
        def get_events(cls):
            return []

        @classmethod
        def get_schedulables(cls):
            return []

        @classmethod
        def get_variables(cls):
            return []

    mocker.patch.object(EventsApp, 'load_application', return_value=MyExtension)

    handler = EventsApp(config)

    assert handler.get_application() == MyExtension
    assert handler.events == {
        'asset_purchase_request_processing': {
            'event_type': 'asset_purchase_request_processing',
            'statuses': ['pending', 'accepted'],
            'method': 'process_asset_purchase_request',
        },
    }
    assert handler.variables == descriptor['variables']
    assert handler.config == config
    assert handler.schedulables == descriptor['schedulables']
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.should_start is True


def test_get_features(mocker):

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return {}

        @classmethod
        def get_events(cls):
            return [
                {
                    'event_type': 'test_event',
                    'statuses': ['pending', 'accepted'],
                    'method': 'my_method',
                },
            ]

        @classmethod
        def get_schedulables(cls):
            return [
                {
                    'method': 'my_schedulable_method',
                    'name': 'My schedulable',
                    'description': 'description',
                },
            ]

        @classmethod
        def get_variables(cls):
            return []

    mocker.patch.object(
        EventsApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = EventsApp(config)

    assert handler.features == {
        'events': {
            'test_event': {
                'event_type': 'test_event',
                'statuses': ['pending', 'accepted'],
                'method': 'my_method',
            },
        },
        'schedulables': MyExtension.get_schedulables(),
    }


def test_get_features_legacy_extension(mocker):

    descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'capabilities': {
            'asset_purchase_request_processing': ['pending', 'accepted'],
        },
        'schedulables': [
            {
                'method': 'my_schedulable_method',
                'name': 'My schedulable',
                'description': 'description',
            },
        ],
    }

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return descriptor

        @classmethod
        def get_events(cls):
            return []

        @classmethod
        def get_schedulables(cls):
            return

        @classmethod
        def get_variables(cls):
            return []

    mocker.patch.object(
        EventsApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = EventsApp(config)

    assert handler.features == {
        'events': {
            'asset_purchase_request_processing': {
                'event_type': 'asset_purchase_request_processing',
                'statuses': ['pending', 'accepted'],
                'method': 'process_asset_purchase_request',
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
