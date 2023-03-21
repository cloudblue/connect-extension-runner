import os

from connect.eaas.core.decorators import (
    transformation,
)
from connect.eaas.core.extension import (
    TransformationsApplicationBase,
)
from connect.eaas.core.proto import (
    Logging,
    LogMeta,
    SetupResponse,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.transformations import (
    TfnApp,
)


def test_get_tfnapp_module(mocker, settings_payload):

    config = ConfigHelper()

    mocked_load = mocker.patch.object(TfnApp, 'load_application')

    handler = TfnApp(config)

    assert handler.get_application() is not None
    mocked_load.assert_called_once_with('tfnapp')


def test_properties(mocker):

    config = ConfigHelper()

    class MyExtension(TransformationsApplicationBase):
        @transformation(
            name='my transformation',
            description='The my transformation',
            edit_dialog_ui='/static/my_settings.html',
        )
        def my_transformation(self, row):
            pass

    mocker.patch.object(TfnApp, 'load_application', return_value=MyExtension)
    mocker.patch.object(TfnApp, 'get_descriptor', return_value={
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'audience': ['vendor'],
    })

    handler = TfnApp(config)

    tfn = {
        'name': 'my transformation',
        'description': 'The my transformation',
        'edit_dialog_ui': '/static/my_settings.html',
        'method': 'my_transformation',
        'manual': False,
    }

    assert handler.config == config
    assert len(handler.transformations) == 1
    assert handler.transformations == [tfn]
    assert handler.should_start is True
    assert handler.readme == 'https://readme.com'
    assert handler.changelog == 'https://changelog.org'
    assert handler.audience == ['vendor']
    assert handler.features == {'transformations': [tfn]}


def test_get_method(mocker, settings_payload):
    class MyExtension(TransformationsApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'audience': ['vendor'],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @transformation(
            name='my transformation',
            description='The my transformation',
            edit_dialog_ui='/static/my_settings.html',
        )
        def transform_it(self, row):
            return row

    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.runner.handlers.base.logging.getLogger')
    mocker.patch.object(
        TfnApp,
        'load_application',
        return_value=MyExtension,
    )
    mocked_log_handler = mocker.patch(
        'connect.eaas.runner.handlers.base.ExtensionLogHandler',
        autospec=True,
    )
    handler = TfnApp(config)

    method = handler.get_method('event_type', 'TQ-000', 'transform_it')
    assert method.__name__ == 'transform_it'
    assert method.__self__.__class__ == MyExtension

    mocked_log_handler.assert_called_once_with(
        config.logging_api_key,
        default_extra_fields=config.metadata,
    )


def test_get_method_with_extra_attrs(mocker, settings_payload):
    class MyExtension(TransformationsApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'audience': ['vendor'],
                'readme_url': 'https://example.com/README.md',
                'changelog_url': 'https://example.com/CHANGELOG.md',
            }

        @transformation(
            name='my async transformation',
            description='The real masterpiece',
            edit_dialog_ui='/static/async_settings.html',
        )
        async def async_transform_it(self, row):
            return row

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
    mocker.patch('connect.eaas.runner.handlers.base.os.urandom', return_value=test_random_bytes)
    mocker.patch.object(
        TfnApp,
        'load_application',
        return_value=MyExtension,
    )
    mocked_log_handler = mocker.patch(
        'connect.eaas.runner.handlers.base.ExtensionLogHandler',
        autospec=True,
    )
    handler = TfnApp(config)

    method = handler.get_method(
        'event_type',
        'TQ-000',
        'async_transform_it',
        installation={'installation': 'data'},
        api_key='installation_key',
        connect_correlation_id='correlation_id-for-events-application',
        transformation_request='TRF-000-000-000',
    )
    assert method.__name__ == 'async_transform_it'
    assert method.__self__.__class__ == MyExtension
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
