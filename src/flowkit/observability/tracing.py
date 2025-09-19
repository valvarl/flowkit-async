from __future__ import annotations

"""
flowkit.observability.tracing
=============================

Optional OpenTelemetry instrumentation bootstrap.

- If OpenTelemetry SDK is not installed, all APIs become no-ops.
- Provides `setup_tracing()` to configure a service-wide tracer provider.
- Provides `trace()` decorator/context manager to annotate functions with spans.

Usage:
    setup_tracing(service_name="flowkit-coordinator", otlp_endpoint="http://otelcol:4317")
    @trace("schedule.tick")
    async def tick(): ...
"""

from typing import Any, Callable, Optional, Awaitable, TypeVar, cast
import asyncio
import logging

from ..core.logging import get_logger, warn_once

try:  # pragma: no cover - avoid hard dep at import time
    from opentelemetry import trace as _otel_trace
    from opentelemetry.sdk.resources import Resource as _OTelResource
    from opentelemetry.sdk.trace import TracerProvider as _TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor as _BatchSpanProcessor

    try:
        # OTLP exporter is optional even with OpenTelemetry installed
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as _OTLPExporter  # type: ignore
    except Exception:  # pragma: no cover
        _OTLPExporter = None  # type: ignore
except Exception:  # pragma: no cover
    _otel_trace = None  # type: ignore
    _OTelResource = None  # type: ignore
    _TracerProvider = None  # type: ignore
    _BatchSpanProcessor = None  # type: ignore
    _OTLPExporter = None  # type: ignore

__all__ = ["setup_tracing", "trace"]

_log = get_logger("observability.tracing")
_F = TypeVar("_F", bound=Callable[..., Any])


def setup_tracing(
    *,
    service_name: str,
    otlp_endpoint: str | None = None,
    ratio: float = 1.0,
) -> None:
    """
    Configure OpenTelemetry tracing (global tracer provider).

    Args:
        service_name: logical service name for resources.
        otlp_endpoint: OTLP gRPC endpoint (e.g. "http://otelcol:4317"); if None,
                       tracing is enabled but no exporter is attached.
        ratio: sampling ratio in [0.0..1.0]. Currently relies on environment vars
               for sampler configuration if needed (kept simple here).

    Notes:
        - This function is idempotent: multiple calls reconfigure the provider.
        - If OpenTelemetry SDK is not installed, this is a no-op with a warning.
    """
    if _otel_trace is None or _TracerProvider is None:  # pragma: no cover
        warn_once(_log, "tracing.no_otel", "opentelemetry-sdk is not installed; tracing disabled")
        return

    resource = _OTelResource.create({"service.name": service_name})
    provider = _TracerProvider(resource=resource)
    _otel_trace.set_tracer_provider(provider)

    if otlp_endpoint and _OTLPExporter is not None:
        try:
            exporter = _OTLPExporter(endpoint=otlp_endpoint, insecure=True)  # pragma: no cover
            processor = _BatchSpanProcessor(exporter)  # pragma: no cover
            provider.add_span_processor(processor)  # pragma: no cover
            _log.info("otel tracing configured (otlp)", endpoint=otlp_endpoint)
        except Exception:  # pragma: no cover
            warn_once(_log, "tracing.otlp_init_failed", "Failed to initialize OTLP exporter", level=logging.ERROR)


def trace(name: str) -> Callable[[_F], _F]:
    """
    Decorator to trace function execution with a span named `name`.

    Works with both sync and async callables. No-op if OpenTelemetry is not available.
    """

    def _decorator(func: _F) -> _F:
        if _otel_trace is None:  # pragma: no cover
            return func

        tracer = _otel_trace.get_tracer("flowkit")

        if asyncio.iscoroutinefunction(func):

            async def _aw(*args: Any, **kwargs: Any):  # type: ignore[misc]
                with tracer.start_as_current_span(name):
                    return await func(*args, **kwargs)

            return cast(_F, _aw)
        else:

            def _sw(*args: Any, **kwargs: Any):  # type: ignore[misc]
                with tracer.start_as_current_span(name):
                    return func(*args, **kwargs)

            return cast(_F, _sw)

    return _decorator
