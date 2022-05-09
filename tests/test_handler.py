from pkg_resources import EntryPoint

from connect.eaas.core.proto import Logging, LogMeta, SetupResponse
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handler import ExtensionHandler


def test_get_method(mocker, settings_payload, extension_cls):
    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.runner.handler.logging.getLogger')
    ext_class = extension_cls('test_method')
    mocker.patch.object(ext_class, 'get_descriptor')
    mocker.patch.object(
        ExtensionHandler,
        'get_extension_class',
        return_value=ext_class,
    )
    mocked_log_handler = mocker.patch(
        'connect.eaas.runner.handler.ExtensionLogHandler',
        autospec=True,
    )
    handler = ExtensionHandler(config)

    method = handler.get_method('TQ-000', 'test_method')
    assert method.__name__ == 'test_method'
    assert method.__self__.__class__ == ext_class

    mocked_log_handler.assert_called_once_with(
        config.logging_api_key,
        default_extra_fields=config.metadata,
    )


def test_get_extension_class(mocker, settings_payload):

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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handler.iter_entry_points',
        return_value=iter([
            EntryPoint('extension', 'connect.eaas.ext'),
        ]),
    )

    handler = ExtensionHandler(config)

    assert handler._extension_class == MyExtension
