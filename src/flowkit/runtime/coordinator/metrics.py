# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Low-cardinality coordinator metrics built on the common Meter facade.

This avoids a hard dependency on prometheus_client here: the application decides
which Meter to wire (NoopMeter/PromMeter) via Observatory.meter.
"""

from dataclasses import dataclass

from ..common.metrics import Counter, Gauge, Histogram, Meter


@dataclass
class CoordinatorMetrics:
    scheduled_total: Counter
    dispatched_total: Counter
    finished_total: Counter
    failed_total: Counter
    queue_depth: Gauge
    schedule_latency_ms: Histogram

    @classmethod
    def create(cls, meter: Meter) -> CoordinatorMetrics:
        scheduled_total = meter.counter("flowkit_coord_scheduled_total", "Nodes scheduled", label_names=["step_type"])
        dispatched_total = meter.counter(
            "flowkit_coord_dispatched_total", "Nodes dispatched", label_names=["step_type"]
        )
        finished_total = meter.counter(
            "flowkit_coord_finished_total", "Nodes finished", label_names=["step_type", "result"]
        )
        failed_total = meter.counter("flowkit_coord_failed_total", "Nodes failed", label_names=["step_type", "reason"])
        queue_depth = meter.gauge("flowkit_coord_queue_depth", "Queue depth", label_names=["priority"])
        schedule_latency_ms = meter.histogram(
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
