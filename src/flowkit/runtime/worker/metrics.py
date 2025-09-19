# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Counter:
    value: int = 0

    def inc(self, n: int = 1) -> None:
        self.value += int(n)


@dataclass
class Gauge:
    value: float = 0.0

    def set(self, v: float) -> None:
        self.value = float(v)


@dataclass
class Timer:
    total_ms: float = 0.0
    count: int = 0

    def observe(self, ms: float) -> None:
        self.total_ms += float(ms)
        self.count += 1


@dataclass
class WorkerMetrics:
    """
    Minimal in-process metrics. Plug your Prometheus/OpenTelemetry exporter
    by adapting this facade as needed.
    """

    items_in: Counter = field(default_factory=Counter)
    items_out: Counter = field(default_factory=Counter)
    batches_out: Counter = field(default_factory=Counter)
    input_lag_ms: Gauge = field(default_factory=Gauge)
    queue_depth: Gauge = field(default_factory=Gauge)
    op_durations_ms: Dict[str, Timer] = field(default_factory=dict)

    def op_timer(self, name: str):
        t = self.op_durations_ms.setdefault(name, Timer())
        start = time.perf_counter()

        def _done():
            t.observe((time.perf_counter() - start) * 1000.0)

        return _done
