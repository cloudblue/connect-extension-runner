from logzio.handler import LogzioHandler


class ExtensionLogHandler(LogzioHandler):
    def __init__(self, *args, **kwargs):
        self.default_extra_fields = kwargs.pop('default_extra_fields')
        super().__init__(*args, **kwargs)

    def extra_fields(self, message):
        extra_fields = super().extra_fields(message)
        extra_fields.update(self.default_extra_fields)
        return extra_fields
