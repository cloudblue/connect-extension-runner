from connect.eaas.config import ConfigHelper
from connect.eaas.dataclasses import ConfigurationPayload


def test_new_extension(mocker, config_payload, extension_cls):
    from connect.eaas.handler import ExtensionHandler
    config = ConfigHelper()
    dyn_config = ConfigurationPayload(**config_payload)
    dyn_config.logging_api_key = 'test_key'
    config.update_dynamic_config(dyn_config)
    mocker.patch('connect.eaas.handler.logging.getLogger')
    mocker.patch('connect.eaas.handler.get_extension_class')
    mocker.patch('connect.eaas.handler.get_extension_type')
    mocked_log_handler = mocker.patch('connect.eaas.handler.ExtensionLogHandler', autospec=True)
    handler = ExtensionHandler(config)
    handler.extension_class = extension_cls('test_method')
    handler.extension_type = 'sync'

    extension = handler.new_extension('TQ-000')
    assert isinstance(extension, handler.extension_class)

    mocked_log_handler.assert_called_once_with(
        config.logging_api_key,
        default_extra_fields=config.metadata,
    )
