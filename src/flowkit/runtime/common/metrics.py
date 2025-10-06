# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Common metrics facade (no external deps by default).

- Defines a minimal `Meter` interface and instrument types (Counter/Gauge/Histogram).
- Provides `NoopMeter` (safe defaults) and `PromMeter` (bridges prometheus_client).

Design goals:
- Zero-allocation fast paths for no-op usage.
- Optional Prometheus wiring without hard dependency.
- Stable protocol used by Coordinator/Worker, easy to swap exporters.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

try:  # optional dependency
    import prometheus_client as _prom  # type: ignore  # pragma: no cover
except Exception:  # pragma: no cover
    _prom = None  # type: ignore


# ---- instrument interfaces ---------------------------------------------------


class _CounterChild(Protocol):
    def inc(self, amount: float = 1.0) -> None: ...


class _GaugeChild(Protocol):
    def set(self, value: float) -> None: ...


class _HistogramChild(Protocol):
    def observe(self, amount: float) -> None: ...


class Counter(Protocol):
    def labels(self, **labels: str) -> _CounterChild: ...


class Gauge(Protocol):
    def labels(self, **labels: str) -> _GaugeChild: ...


class Histogram(Protocol):
    def labels(self, **labels: str) -> _HistogramChild: ...


class Meter(Protocol):
    def counter(self, name: str, help_text: str, *, label_names: Sequence[str] | None = None) -> Counter: ...
    def gauge(self, name: str, help_text: str, *, label_names: Sequence[str] | None = None) -> Gauge: ...
    def histogram(
        self,
        name: str,
        help_text: str,
        *,
        label_names: Sequence[str] | None = None,
        buckets: Sequence[float] | None = None,
    ) -> Histogram: ...


# ---- no-op meter (allocation-minimal) ---------------------------------------


class _NoopCounterChild:
    __slots__ = ()

    def inc(self, amount: float = 1.0) -> None:
        """No-op increment."""
        return


class _NoopGaugeChild:
    __slots__ = ()

    def set(self, value: float) -> None:
        """No-op set."""
        return


class _NoopHistogramChild:
    __slots__ = ()

    def observe(self, amount: float) -> None:
        """No-op observe."""
        return


# Singletons to avoid per-call allocs
_NOOP_COUNTER_CHILD = _NoopCounterChild()
_NOOP_GAUGE_CHILD = _NoopGaugeChild()
_NOOP_HIST_CHILD = _NoopHistogramChild()


class _NoopInstrument:
    __slots__ = ("_kind",)

    def __init__(self, kind: str) -> None:
        self._kind = kind

    def labels(self, **labels: str):
        # Always return the same child singletons — safe & allocation-free.
        if self._kind == "counter":
            return _NOOP_COUNTER_CHILD
        if self._kind == "gauge":
            return _NOOP_GAUGE_CHILD
        return _NOOP_HIST_CHILD


@dataclass
class NoopMeter:
    """No-op meter that safely discards all observations."""

    # Reuse instrument singletons; Prometheus-like API is preserved.
    _COUNTER = _NoopInstrument("counter")
    _GAUGE = _NoopInstrument("gauge")
    _HIST = _NoopInstrument("histogram")

    def counter(self, name: str, help_text: str, *, label_names: Sequence[str] | None = None) -> Counter:
        return self._COUNTER

    def gauge(self, name: str, help_text: str, *, label_names: Sequence[str] | None = None) -> Gauge:
        return self._GAUGE

    def histogram(
        self,
        name: str,
        help_text: str,
        *,
        label_names: Sequence[str] | None = None,
        buckets: Sequence[float] | None = None,
    ) -> Histogram:
        return self._HIST


# ---- prometheus-backed meter -------------------------------------------------

# Prefer our safe wrappers for Counter/Histogram if available (validate labels).
try:
    from ...observability.metrics import (  # type: ignore
        SafeCounter as _SafeCounter,
    )
    from ...observability.metrics import (
        SafeHistogram as _SafeHistogram,
    )
except Exception:  # pragma: no cover
    _SafeCounter = None  # type: ignore
    _SafeHistogram = None  # type: ignore


class _PromGauge:
    """
    Thin gauge wrapper with a tiny label-child cache to cut repeated dict hashing
    inside prometheus_client on hot paths.
    """

    __slots__ = ("_cache", "_inner")

    def __init__(self, name: str, documentation: str, *, label_names: Sequence[str] | None = None) -> None:
        if _prom is None:  # pragma: no cover
            self._inner = None
        else:
            self._inner = _prom.Gauge(name, documentation, labelnames=list(label_names or []), registry=_prom.REGISTRY)
        self._cache: dict[tuple[tuple[str, str], ...], _GaugeChild] = {}

    def labels(self, **labels: str):
        if self._inner is None:  # pragma: no cover
            return _NOOP_GAUGE_CHILD
        key = tuple(sorted(labels.items()))
        child = self._cache.get(key)
        if child is None:
            child = self._inner.labels(**labels)
            self._cache[key] = child
        return child


@dataclass
class PromMeter:
    """
    Prometheus-backed Meter.

    Notes:
      - Requires `prometheus_client`. If it's not installed, falls back to no-op instruments.
      - Label validation for Counter/Histogram is performed by SafeCounter/SafeHistogram wrappers.
      - Gauge uses direct client with a small child cache.
    """

    def counter(self, name: str, help_text: str, *, label_names: Sequence[str] | None = None) -> Counter:
        if _SafeCounter is None:  # pragma: no cover
            return NoopMeter().counter(name, help_text, label_names=label_names)
        return _SafeCounter(name, help_text, label_names=tuple(label_names or ()))

    def gauge(self, name: str, help_text: str, *, label_names: Sequence[str] | None = None) -> Gauge:
        return _PromGauge(name, help_text, label_names=tuple(label_names or ()))

    def histogram(
        self,
        name: str,
        help_text: str,
        *,
        label_names: Sequence[str] | None = None,
        buckets: Sequence[float] | None = None,
    ) -> Histogram:
        if _SafeHistogram is None:  # pragma: no cover
            return NoopMeter().histogram(name, help_text, label_names=label_names, buckets=buckets)
        return _SafeHistogram(
            name,
            help_text,
            label_names=tuple(label_names or ()),
            buckets=tuple(buckets) if buckets is not None else None,
        )
