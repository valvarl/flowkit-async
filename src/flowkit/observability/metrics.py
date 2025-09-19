from __future__ import annotations

"""
flowkit.observability.metrics
=============================

Prometheus metrics bootstrap with **optional dependency** on `prometheus_client`.

Features:
- Global registry accessor (or no-op fallback if the library is not installed).
- Helpers to create low-cardinality, label-validated metrics (SafeCounter/Histogram).
- Tiny HTTP exposition server backed by `prometheus_client.start_http_server()`.

Design notes:
- Importing this module must NOT fail if prometheus_client is absent.
- Starting the server will be a no-op when the client is not available.
"""

from typing import Any, Mapping, Sequence, Iterable, Optional, Dict
import logging

from ..core.logging import get_logger, warn_once

try:
    # type: ignore[assignment]
    import prometheus_client as _prom  # pragma: no cover
except Exception:  # pragma: no cover - any import error -> fallback
    _prom = None  # type: ignore[assignment]

__all__ = [
    "get_registry",
    "MetricsService",
    "SafeCounter",
    "SafeHistogram",
]


_log = get_logger("observability.metrics")


# ---- Fallback (no prometheus_client available) -------------------------------


class _NoopChild:
    def inc(self, amount: float = 1.0) -> None:  # noqa: D401
        return

    def observe(self, amount: float) -> None:  # noqa: D401
        return


class _NoopMetric:
    def labels(self, **labels: str) -> _NoopChild:  # noqa: D401
        return _NoopChild()


class _NoopRegistry:
    def register(self, *_: Any, **__: Any) -> None:  # noqa: D401
        return


_NOOP_REGISTRY = _NoopRegistry()


def get_registry() -> Any:
    """
    Return a prometheus_client CollectorRegistry if available, otherwise a no-op stub.

    Consumers should treat the return type as opaque and pass it back to helpers
    below (SafeCounter/Histogram) or to `MetricsService`.
    """
    if _prom is None:  # pragma: no cover
        return _NOOP_REGISTRY
    return _prom.REGISTRY  # default global registry


# ---- Safe metric wrappers ----------------------------------------------------


class _LabelChecker:
    """Validate label names against an allowlist to keep cardinality under control."""

    __slots__ = ("_allowed",)

    def __init__(self, allowed: Iterable[str] | None) -> None:
        self._allowed = frozenset(allowed or ())

    def validate(self, labels: Mapping[str, str]) -> None:
        if not self._allowed:
            return
        unknown = [k for k in labels.keys() if k not in self._allowed]
        if unknown:
            raise ValueError(f"Unknown label(s) for metric: {unknown}; allowed={sorted(self._allowed)}")


class SafeCounter:
    """
    Counter wrapper that validates label names against an allowlist.

    Example:
        cnt = SafeCounter("flowkit_items_total", "Processed items", label_names=["role", "result"])
        cnt.labels(role="indexer", result="ok").inc()
    """

    def __init__(
        self,
        name: str,
        documentation: str,
        *,
        label_names: Sequence[str] | None = None,
        registry: Any | None = None,
    ) -> None:
        self._checker = _LabelChecker(label_names or [])
        if _prom is None:  # pragma: no cover
            self._metric = _NoopMetric()
            return
        reg = registry or _prom.REGISTRY
        self._metric = _prom.Counter(name, documentation, labelnames=list(label_names or []), registry=reg)

    def labels(self, **labels: str):
        self._checker.validate(labels)
        return self._metric.labels(**labels)


class SafeHistogram:
    """
    Histogram wrapper that validates label names against an allowlist.

    Args:
        buckets: optional custom buckets. If omitted, prometheus_client defaults are used.
    """

    def __init__(
        self,
        name: str,
        documentation: str,
        *,
        label_names: Sequence[str] | None = None,
        registry: Any | None = None,
        buckets: Sequence[float] | None = None,
    ) -> None:
        self._checker = _LabelChecker(label_names or [])
        if _prom is None:  # pragma: no cover
            self._metric = _NoopMetric()
            return
        reg = registry or _prom.REGISTRY
        self._metric = _prom.Histogram(
            name,
            documentation,
            labelnames=list(label_names or []),
            registry=reg,
            buckets=list(buckets) if buckets is not None else _prom.Histogram.DEFAULT_BUCKETS,
        )

    def labels(self, **labels: str):
        self._checker.validate(labels)
        return self._metric.labels(**labels)


# ---- HTTP exposition ---------------------------------------------------------


class MetricsService:
    """
    Minimal Prometheus exposition server.

    Uses `prometheus_client.start_http_server()` under the hood, which starts a
    background HTTP server bound to `/metrics`. There is no official stop API,
    so `stop()` is a no-op (the process termination will close the socket).
    """

    def __init__(self, *, address: str = "0.0.0.0", port: int = 8000, registry: Any | None = None) -> None:
        self.address = address
        self.port = int(port)
        self._registry = registry
        self._started = False

    def start(self) -> None:
        """Start the HTTP server if prometheus_client is available."""
        if self._started:
            return
        if _prom is None:  # pragma: no cover
            warn_once(_log, "metrics.no_prometheus_client", "prometheus_client is not installed; metrics disabled")
            self._started = True  # consider started to avoid repeated warnings
            return
        # Note: start_http_server ignores custom registry; it exposes the default REGISTRY.
        # If you need an isolated registry, run your own WSGI server with `make_wsgi_app(registry=...)`.
        _prom.start_http_server(self.port, addr=self.address)  # pragma: no cover
        _log.info("metrics server started", address=self.address, port=self.port)
        self._started = True

    def stop(self) -> None:
        """No-op: prometheus_client has no stop API for the background server."""
        if self._started:
            _log.info("metrics server stopping (no-op)")
        self._started = False
