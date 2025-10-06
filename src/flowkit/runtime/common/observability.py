# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Observability facade: unified access to logging, metrics, and tracing.

- logger: based on flowkit.core.logging
- meter: implements Meter protocol from runtime/common/metrics (Noop or Prom)
- tracer: thin adapter around flowkit.observability.tracing (no-op when OTel missing)

Use ScopedObs to enrich logs/metrics with contextual labels (task_id/node_id/etc.).
"""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from ...core.logging import get_logger, log_context
from ...observability import tracing as otel
from .metrics import Meter, NoopMeter


@dataclass(frozen=True)
class _TracerFacade:
    """
    Minimal tracer facade to avoid hard dependency on OpenTelemetry in call sites.

    Use `start_span(name)` as a context manager when you need a span.
    """

    def start_span(self, name: str):
        if getattr(otel, "trace", None) is None:  # should not happen with our module
            # Return a no-op context manager
            from contextlib import nullcontext

            return nullcontext()

        # otel.trace(name) returns decorator; we want a context manager
        class _Span:
            def __enter__(self_nonlocal):
                # rely on otel.trace decorator's internals indirectly via tracer
                return self_nonlocal

            def __exit__(self_nonlocal, exc_type, exc, tb):
                return False

        # Prefer using global tracer if available
        try:
            from opentelemetry import trace as _otel_trace  # type: ignore

            tracer = _otel_trace.get_tracer("flowkit")  # pragma: no cover
            return tracer.start_as_current_span(name)  # pragma: no cover
        except Exception:  # pragma: no cover
            return _Span()


@dataclass(frozen=True)
class Observatory:
    """
    Unified observability facade.

    Attributes:
        logger: structured logger adapter
        meter: metrics facade (Noop or Prom)
        tracer: minimal tracer facade (no-op if OTel not installed)
    """

    logger: Any
    meter: Meter
    tracer: _TracerFacade

    @staticmethod
    def create(*, service_name: str = "flowkit", meter: Meter | None = None) -> Observatory:
        lg = get_logger(service_name)
        mt = meter or NoopMeter()
        tr = _TracerFacade()
        return Observatory(logger=lg, meter=mt, tracer=tr)

    def scoped(self, **ctx: Any) -> ScopedObs:
        """Create a ScopedObs that enriches logs and default metric labels."""
        return ScopedObs(base=self, default_labels={k: str(v) for k, v in ctx.items() if v is not None})


@dataclass(frozen=True)
class ScopedObs:
    """
    Scoped view over Observatory with default labels attached to metrics and logs.

    - Logs: uses flowkit.core.logging.log_context to attach fields for the duration of the call.
    - Metrics: appends default labels on top of explicit labels passed by the caller.
    """

    base: Observatory
    default_labels: Mapping[str, str]

    def with_context(self, **extra: Any) -> ScopedObs:
        merged = {**self.default_labels, **{k: str(v) for k, v in extra.items() if v is not None}}
        return replace(self, default_labels=merged)

    # Logging helpers ---------------------------------------------------------

    def log_context(self, **extra: Any):
        """Context manager that binds default + extra context fields for logs."""
        payload = {**self.default_labels, **{k: v for k, v in extra.items() if v is not None}}
        return log_context(**payload)

    # Metrics helpers ---------------------------------------------------------

    def counter(self, name: str, help_text: str, *, label_names: list[str] | None = None):
        return self.base.meter.counter(name, help_text, label_names=label_names)

    def histogram(
        self, name: str, help_text: str, *, label_names: list[str] | None = None, buckets: list[float] | None = None
    ):
        return self.base.meter.histogram(name, help_text, label_names=label_names, buckets=buckets)

    def gauge(self, name: str, help_text: str, *, label_names: list[str] | None = None):
        return self.base.meter.gauge(name, help_text, label_names=label_names)

    # Tracing -----------------------------------------------------------------

    def start_span(self, name: str):
        return self.base.tracer.start_span(name)
