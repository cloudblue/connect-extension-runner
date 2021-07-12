import json

from logzio.handler import LogzioHandler


class ExtensionLogHandler(LogzioHandler):
    def __init__(self, *args, **kwargs):
        self.default_extra_fields = kwargs.pop('default_extra_fields')
        super().__init__(*args, **kwargs)

    def extra_fields(self, message):
        extra_fields = super().extra_fields(message)
        extra_fields.update(self.default_extra_fields)
        return extra_fields


class RequestLogger:

    def __init__(self, logger):
        self.logger = logger

    def log_request(self, method, url, kwargs):
        other_args = {k: v for k, v in kwargs.items() if k not in ('headers', 'json', 'params')}

        if 'params' in kwargs:
            url += '&' if '?' in url else '?'
            url += '&'.join([f'{k}={v}' for k, v in kwargs['params'].items()])

        lines = [
            '--- HTTP Request ---',
            f'{method.upper()} {url} {other_args if other_args else ""}',
        ]

        if 'headers' in kwargs:
            for k, v in kwargs['headers'].items():
                lines.append(f'{k}: {v}')

        if 'json' in kwargs:
            lines.append(json.dumps(kwargs['json'], indent=4))

        lines.append('')
        self.logger.debug('\n'.join(lines))

    def log_response(self, response):
        reason = response.raw.reason if getattr(response, 'raw', None) else response.reason_phrase
        lines = [
            '--- HTTP Response ---',
            f'{response.status_code} {reason}',
        ]

        for k, v in response.headers.items():
            lines.append(f'{k}: {v}')

        if response.headers.get('Content-Type', None) == 'application/json':
            lines.append(json.dumps(response.json(), indent=4))

        lines.append('')

        self.logger.debug('\n'.join(lines))
