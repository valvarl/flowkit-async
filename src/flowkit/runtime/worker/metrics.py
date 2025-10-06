# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Worker metrics built on the common Meter facade.

Optimizations / design:
- Zero-config no-op by default (safe in tests and local runs).
- Prometheus wiring is handled by passing a PromMeter via Observatory.
- Low-cardinality counters/gauges use pre-bound label-less children to avoid
  repeated `.labels()` allocations on hot paths.
- Operation timing uses a shared histogram with `op` label.
"""

import time

from ..common.metrics import Gauge, Histogram, Meter, NoopMeter


class WorkerMetrics:
    """
    Public attributes are *children*, ready for direct `.inc()` / `.set()`:

        m = WorkerMetrics()  # Noop by default
        m.items_in.inc()
        m.queue_depth.set(42)

    For timing ops, use the helper:

        done = m.op_timer("map.json")
        ... work ...
        done()
    """

    def __init__(self, meter: Meter | None = None) -> None:
        m = meter or NoopMeter()
        # Pre-bind label-less children for counters/gauges
        self.items_in = m.counter("flowkit_worker_items_in_total", "Number of items received").labels()
        self.items_out = m.counter("flowkit_worker_items_out_total", "Number of items delivered").labels()
        self.batches_out = m.counter("flowkit_worker_batches_out_total", "Number of batches delivered").labels()
        self.input_lag_ms = m.gauge("flowkit_worker_input_lag_ms", "Estimated input lag in ms").labels()
        self.queue_depth: Gauge = m.gauge("flowkit_worker_queue_depth", "Internal queue depth").labels()  # type: ignore[assignment]
        # Shared histogram; child is selected per op in op_timer()
        self._op_latency: Histogram = m.histogram(
            "flowkit_worker_op_duration_ms",
            "Operation latency (ms) by op",
            label_names=("op",),
            buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000),
        )

    @classmethod
    def create(cls, meter: Meter) -> WorkerMetrics:
        return cls(meter=meter)

    # ---- helpers -------------------------------------------------------------

    def op_timer(self, op_name: str):
        """
        Return a callable that, when invoked, observes the elapsed time in ms
        into the `op_name` bucket of the histogram.
        """
        start = time.perf_counter()
        child = self._op_latency.labels(op=op_name)

        def _done() -> None:
            child.observe((time.perf_counter() - start) * 1000.0)

        return _done
