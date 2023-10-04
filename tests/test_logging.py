import logging

from connect.eaas.core.logging import (
    RequestLogger as RLBase,
)
from connect.eaas.runner.logging import (
    RequestLogger,
)


def test_log_request(mocker):
    mocked_log_req = mocker.patch.object(RLBase, 'log_request')
    logger = logging.getLogger('test')
    logger.setLevel(logging.INFO)

    rl = RequestLogger(logger, logging.DEBUG)

    rl.log_request('method', 'url', {'a': 'b'})

    mocked_log_req.assert_not_called()

    logger.setLevel(logging.DEBUG)

    rl.log_request('method', 'url', {'a': 'b'})

    mocked_log_req.assert_called_once()


def test_log_response(mocker):
    mocked_log_req = mocker.patch.object(RLBase, 'log_response')
    logger = logging.getLogger('test')
    logger.setLevel(logging.INFO)

    rl = RequestLogger(logger, logging.DEBUG)

    rl.log_response('response')

    mocked_log_req.assert_not_called()

    logger.setLevel(logging.DEBUG)

    rl.log_response('response')

    mocked_log_req.assert_called_once()
