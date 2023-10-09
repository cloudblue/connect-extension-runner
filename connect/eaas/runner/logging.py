from connect.eaas.core.logging import (
    RequestLogger as _RequestLogger,
)


class RequestLogger(_RequestLogger):
    def log_request(self, method, url, kwargs):
        if not self.logger.isEnabledFor(self.level):
            return
        super().log_request(method, url, kwargs)

    def log_response(self, response):
        if not self.logger.isEnabledFor(self.level):
            return
        super().log_response(response)
