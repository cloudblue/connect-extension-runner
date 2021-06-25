import logging

from connect.eaas.logging import ExtensionLogHandler


def test_extension_log_handler():

    handler = ExtensionLogHandler('api_key', default_extra_fields={'field': 'value'})
    assert handler.logzio_sender.token == 'api_key'
    extra_fields = handler.extra_fields(logging.LogRecord(
        'name',
        logging.INFO,
        'path',
        10,
        'message',
        None,
        None,
    ))
    assert extra_fields['field'] == 'value'
