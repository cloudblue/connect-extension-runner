from connect.eaas.extension import OK, Reschedule, SKIP


def test_result_ok():
    assert OK.status == 'succeeded'
    data = {'test': 'data'}
    ok = OK(data)
    assert ok.status == 'succeeded'
    assert ok.data == data


def test_result_skip():
    assert SKIP.status == 'skip'


def test_result_reschedule():
    r = Reschedule(60)

    assert r.status == 'reschedule'
    assert r.countdown == 60
