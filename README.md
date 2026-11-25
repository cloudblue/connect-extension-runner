# CloudBlue Connect EaaS Extension Runner


![pyversions](https://img.shields.io/pypi/pyversions/connect-extension-runner.svg) [![PyPi Status](https://img.shields.io/pypi/v/connect-extension-runner.svg)](https://pypi.org/project/connect-extension-runner/) [![Build Connect Reports Core](https://github.com/cloudblue/connect-extension-runner/actions/workflows/build.yml/badge.svg)](https://github.com/cloudblue/connect-extension-runner/actions/workflows/build.yml) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=cloudblue_connect-extension-runner&metric=alert_status)](https://sonarcloud.io/dashboard?id=cloudblue_connect-extension-runner) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=cloudblue_connect-extension-runner&metric=coverage)](https://sonarcloud.io/dashboard?id=cloudblue_connect-extension-runner) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=cloudblue_connect-extension-runner&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=cloudblue_connect-extension-runner)![Docker Image Version (latest semver)](https://img.shields.io/docker/v/cloudblueconnect/connect-extension-runner?label=docker%20version&sort=semver)![Docker Image Size (latest semver)](https://img.shields.io/docker/image-size/cloudblueconnect/connect-extension-runner?label=docker%20image%20size&sort=semver)![Docker Pulls](https://img.shields.io/docker/pulls/cloudblueconnect/connect-extension-runner)

## Interactive task tracing

To export interactive task spans from an events worker, set
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to your OTLP/HTTP collector's full traces URL
(for example, `http://collector:4318/v1/traces`). The worker also supports
`OTEL_EXPORTER_OTLP_ENDPOINT` as a base URL. Configure the collector to forward to
the tracing backend used by the EaaS gateway. An existing OpenTelemetry provider
in the worker takes precedence. The runner also works without an exporter.

For a sampled task carrying a W3C traceparent in `connect_correlation_id`, find
`runner.interactive` by `eaas.task.id`. Its start to the start of
`runner.interactive.handler` includes argument preparation and scheduling.
The handler span covers the actual extension call, including synchronous calls
on executor threads. Instrumented dependencies can create children in that context.
The interval from `result.queued` to `result.dequeued` measures result queue
wait. Each `websocket.send` event marks a send attempt. The parent ends after
the result is sent, delivery fails, or the worker stops.
