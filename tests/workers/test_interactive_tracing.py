import asyncio
import threading

import pytest
from opentelemetry import (
    trace,
)
from opentelemetry.sdk.trace import (
    TracerProvider,
)
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from connect.eaas.core.enums import (
    EventType,
    TaskCategory,
)
from connect.eaas.core.proto import (
    SetupResponse,
    Task,
)
from connect.eaas.core.responses import (
    ValidationResponse,
)
from connect.eaas.runner import (
    tracing,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.events import (
    EventsApp,
)
from connect.eaas.runner.workers.events import (
    EventsWorker,
)


@pytest.mark.asyncio
@pytest.mark.parametrize('async_handler', [False, True])
@pytest.mark.parametrize('send_ok', [False, True])
async def test_interactive_trace(mocker, settings_payload, task_payload, async_handler, send_ok):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer('test')
    mocker.patch.object(tracing, 'tracer', tracer)

    config = ConfigHelper()
    config.update_dynamic_config(SetupResponse(**settings_payload))
    mocker.patch.object(EventsApp, 'load_application')
    app = EventsApp(config)
    event = EventType.ASSET_PURCHASE_REQUEST_VALIDATION
    mocker.patch.object(
        type(app), 'events', new_callable=mocker.PropertyMock,
        return_value={event: {'method': 'validate'}},
    )
    worker = EventsWorker(app, mocker.Mock(), mocker.Mock(), mocker.Mock())
    loop_thread = threading.get_ident()
    handler_threads = []

    def validate(arg):
        handler_threads.append(threading.get_ident())
        with tracer.start_as_current_span('extension.lookup'):
            return ValidationResponse.done(arg)

    async def validate_async(arg):
        await asyncio.sleep(0)
        return validate(arg)

    method = validate_async if async_handler else validate
    client_headers = {}

    def get_method(event_type, task_id, method_name, **kwargs):
        client = app._create_client(
            event_type, task_id, method_name, 'test', kwargs['connect_correlation_id'],
        )
        client_headers.update(client.default_headers)
        return method

    mocker.patch.object(app, 'get_application', return_value=mocker.Mock(validate=method))
    mocker.patch.object(app, 'get_method', side_effect=get_method)
    task = Task(**task_payload(TaskCategory.INTERACTIVE, event, 'task-id'))
    task.input.data = {'input': 'sensitive'}
    task.options.connect_correlation_id = '00-' + 'ab' * 16 + '-' + 'cd' * 8 + '-01'
    await worker.process_task(task)

    # Wait for the actual manager to enqueue the result before starting the sender.
    async def queued():
        while worker.results_queue.empty():
            await asyncio.sleep(0)

    await asyncio.wait_for(queued(), timeout=5)

    async def send(message):
        worker.run_event.clear()
        if not send_ok:
            raise RuntimeError('test delivery failure')

    worker.ensure_connection = mocker.AsyncMock()
    worker.send = send
    mocker.patch('connect.eaas.runner.workers.events.RESULT_SENDER_MAX_RETRIES', 1)
    mocker.patch('connect.eaas.runner.workers.events.DELAY_ON_CONNECT_EXCEPTION_SECONDS', 0)
    worker.run_event.set()
    await asyncio.wait_for(worker.result_sender(), timeout=5)
    spans = {s.name: s for s in exporter.get_finished_spans()}
    root = spans['runner.interactive']
    handler = spans['runner.interactive.handler']
    assert root.context.trace_id == int('ab' * 16, 16)
    assert root.parent.span_id == int('cd' * 8, 16)
    assert client_headers['ext-traceparent'] == (
        f'00-{root.context.trace_id:032x}-{root.context.span_id:016x}-01'
    )
    assert handler.parent.span_id == root.context.span_id
    assert spans['extension.lookup'].parent.span_id == handler.context.span_id
    assert (handler_threads[0] == loop_thread) == async_handler
    assert root.attributes == {'eaas.task.id': task.options.task_id}
    assert [e.name for e in root.events] == ['result.queued', 'result.dequeued', 'websocket.send']
    assert root.start_time <= handler.start_time <= handler.end_time <= root.events[0].timestamp
    assert root.events[0].timestamp <= root.events[1].timestamp <= root.end_time
    assert (root.status.status_code == trace.StatusCode.ERROR) != send_ok
    assert not worker._interactive_spans
    provider.shutdown()


@pytest.mark.parametrize('parent', [None, 'invalid', '00-' + 'ab' * 16 + '-' + 'cd' * 8 + '-00'])
def test_missing_or_unsampled_parent(mocker, task_payload, parent):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    mocker.patch.object(tracing, 'tracer', provider.get_tracer('test'))
    task = Task(**task_payload(
        TaskCategory.INTERACTIVE, EventType.ASSET_PURCHASE_REQUEST_VALIDATION, 'task',
        connect_correlation_id=parent,
    ))
    span = tracing.start_interactive_span(task)
    if span is not None:
        assert not span.is_recording()
        span.end()
    assert not exporter.get_finished_spans()
    provider.shutdown()
