#
# This file is part of the CloudBlue Connect EaaS Extension Runner.
#
import inspect
import os
from contextlib import (
    contextmanager,
)
from functools import (
    wraps,
)

from opentelemetry import (
    trace,
)
from opentelemetry.context import (
    Context,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import (
    Resource,
)
from opentelemetry.sdk.trace import (
    TracerProvider,
)
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)


tracer = trace.get_tracer(__name__)


def configure_worker_tracing():
    """Initialize the exporter in the event worker, after multiprocessing starts."""
    if not (
        os.environ.get('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT')
        or os.environ.get('OTEL_EXPORTER_OTLP_ENDPOINT')
    ):
        return
    if os.environ.get('OTEL_SDK_DISABLED', '').lower() == 'true':
        return
    if isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider):
        provider = TracerProvider(resource=Resource.create({
            'service.name': os.environ.get('OTEL_SERVICE_NAME', 'connect-extension-runner'),
        }))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
        return provider


def start_interactive_span(task):
    parent = TraceContextTextMapPropagator().extract(
        {'traceparent': task.options.connect_correlation_id or ''}, context=Context(),
    )
    if not trace.get_current_span(parent).get_span_context().is_valid:
        return
    return tracer.start_span(
        'runner.interactive', context=parent, kind=trace.SpanKind.CONSUMER,
        attributes={'eaas.task.id': task.options.task_id},
    )


@contextmanager
def handler_span(parent):
    with tracer.start_as_current_span(
        'runner.interactive.handler', context=trace.set_span_in_context(parent),
        record_exception=False, set_status_on_exception=False,
    ) as span:
        try:
            yield
        except BaseException:
            span.set_status(trace.StatusCode.ERROR)
            raise


def traced_handler(method):
    parent = trace.get_current_span()
    if not parent.is_recording():
        return method

    if inspect.iscoroutinefunction(method):
        @wraps(method)
        async def call_async(*args):
            with handler_span(parent):
                return await method(*args)
        return call_async

    @wraps(method)
    def call_sync(*args):
        with handler_span(parent):
            return method(*args)
    return call_sync
