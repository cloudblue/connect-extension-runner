from connect.eaas.core.proto import Logging, LogMeta, SetupResponse
from connect.eaas.runner.config import ConfigHelper


def test_new_extension(mocker, settings_payload, extension_cls):
    from connect.eaas.runner.handler import ExtensionHandler
    config = ConfigHelper()
    dyn_config = SetupResponse(
        varibles=settings_payload.get('configuration'),
        environment_type=settings_payload.get('environment_type'),
        logging=Logging(**settings_payload, meta=LogMeta(**settings_payload)),
    )
    dyn_config.logging.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.runner.handler.logging.getLogger')
    mocker.patch('connect.eaas.runner.handler.get_extension_class')
    mocker.patch('connect.eaas.runner.handler.get_extension_type')
    mocked_log_handler = mocker.patch(
        'connect.eaas.runner.handler.ExtensionLogHandler',
        autospec=True,
    )
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls('test_method')
    handler.extension_type = 'sync'

    extension = handler.new_extension('TQ-000')
    assert isinstance(extension, handler.extension_class)

    mocked_log_handler.assert_called_once_with(
        config.logging_api_key,
        default_extra_fields=config.metadata,
    )
