import pytest

from connect.eaas.dataclasses import ResultType
from connect.eaas.extension import (
    CustomEventResponse,
    ProcessingResponse,
    ProductActionResponse,
    ValidationResponse,
)


def test_result_ok():
    assert ProcessingResponse.done().status == ResultType.SUCCESS
    data = {'test': 'data'}
    ok = ValidationResponse.done(data)
    assert ok.status == ResultType.SUCCESS
    assert ok.data == data


def test_result_skip():
    assert ProcessingResponse.skip().status == ResultType.SKIP


def test_result_skip_with_output():
    skip = ProcessingResponse.skip('output')
    assert skip.status == ResultType.SKIP
    assert skip.output == 'output'


@pytest.mark.parametrize(
    ('countdown', 'expected'),
    (
        (0, 30),
        (-1, 30),
        (1, 30),
        (30, 30),
        (31, 31),
        (100, 100),
    ),
)
def test_result_reschedule(countdown, expected):
    r = ProcessingResponse.reschedule(countdown)

    assert r.status == ResultType.RESCHEDULE
    assert r.countdown == expected


@pytest.mark.parametrize(
    ('countdown', 'expected'),
    (
        (0, 300),
        (-1, 300),
        (1, 300),
        (30, 300),
        (300, 300),
        (600, 600),
    ),
)
def test_result_slow_reschedule(countdown, expected):
    r = ProcessingResponse.slow_process_reschedule(countdown)

    assert r.status == ResultType.RESCHEDULE
    assert r.countdown == expected


@pytest.mark.parametrize(
    'response_cls',
    (
        ProcessingResponse, ValidationResponse,
        CustomEventResponse, ProductActionResponse,
    ),
)
def test_result_fail(response_cls):
    r = response_cls.fail(output='reason of failure')

    assert r.status == ResultType.FAIL
    assert r.output == 'reason of failure'


def test_custom_event():
    r = CustomEventResponse.done(headers={'X-Custom-Header': 'value'}, body='text')

    assert r.status == ResultType.SUCCESS
    assert r.data == {
        'http_status': 200,
        'headers': {'X-Custom-Header': 'value'},
        'body': 'text',
    }


def test_product_action():
    r = ProductActionResponse.done(headers={'X-Custom-Header': 'value'}, body='text')

    assert r.status == ResultType.SUCCESS
    assert r.data == {
        'http_status': 200,
        'headers': {'X-Custom-Header': 'value'},
        'body': 'text',
    }
