# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Low-cardinality Prometheus metrics for the coordinator runtime.

We keep labels conservative (role, step_type, result/phase) and avoid IDs.

If `prometheus_client` is not installed, the module degrades to no-op stubs,
so the runtime remains functional in minimal environments.
"""

from dataclasses import dataclass
from typing import Any

try:
    from prometheus_client import Counter, Gauge, Histogram
except Exception:  # pragma: no cover
    # no-op fallbacks to keep imports safe
    class _N:
        def __init__(self, *a: Any, **k: Any) -> None: ...
        def labels(self, *a: Any, **k: Any) -> _N:
            return self

        def inc(self, *a: Any, **k: Any) -> None: ...
        def dec(self, *a: Any, **k: Any) -> None: ...
        def observe(self, *a: Any, **k: Any) -> None: ...
        def set(self, *a: Any, **k: Any) -> None: ...

    Counter = Gauge = Histogram = _N  # type: ignore[misc, assignment]


@dataclass
class CoordinatorMetrics:
    scheduled_total: Any
    dispatched_total: Any
    finished_total: Any
    failed_total: Any
    queue_depth: Any
    schedule_latency_ms: Any

    @classmethod
    def create(cls) -> CoordinatorMetrics:
        scheduled_total = Counter("flowkit_coord_scheduled_total", "Nodes scheduled", ["step_type"])
        dispatched_total = Counter("flowkit_coord_dispatched_total", "Nodes dispatched", ["step_type"])
        finished_total = Counter("flowkit_coord_finished_total", "Nodes finished", ["step_type", "result"])
        failed_total = Counter("flowkit_coord_failed_total", "Nodes failed", ["step_type", "reason"])
        queue_depth = Gauge("flowkit_coord_queue_depth", "Queue depth", ["priority"])
        schedule_latency_ms = Histogram(
            "flowkit_coord_schedule_latency_ms",
            "Latency from eligible to dispatch (ms)",
            buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 5000),
        )
        return cls(
            scheduled_total=scheduled_total,
            dispatched_total=dispatched_total,
            finished_total=finished_total,
            failed_total=failed_total,
            queue_depth=queue_depth,
            schedule_latency_ms=schedule_latency_ms,
        )
