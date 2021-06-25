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


def test_result_reschedule():
    r = ProcessingResponse.reschedule(60)

    assert r.status == ResultType.RESCHEDULE
    assert r.countdown == 60


def test_custom_event():
    r = CustomEventResponse.done(headers={'X-Custom-Header': 'value'}, body='text')

    assert r.status == ResultType.SUCCESS
    assert r.http_status == 200
    assert r.headers == {'X-Custom-Header': 'value'}
    assert r.body == 'text'


def test_product_action():
    r = ProductActionResponse.done(headers={'X-Custom-Header': 'value'}, body='text')

    assert r.status == ResultType.SUCCESS
    assert r.http_status == 200
    assert r.headers == {'X-Custom-Header': 'value'}
    assert r.body == 'text'
