# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Runtime common facades used by both Coordinator and Worker.

Exports:
  - Observability facade (logger + meter + tracer) and ScopedObs helper
  - Metrics façade (NoopMeter/PromMeter and instrument interfaces)
  - Durable Outbox dispatcher (at-least-once egress)
  - Best-effort HooksBus (rate-limited)
"""

from .hooks_runner import HooksBus
from .metrics import (
    Counter,
    Gauge,
    Histogram,
    Meter,
    NoopMeter,
    PromMeter,
)
from .observability import Observatory, ScopedObs
from .outbox import OutboxConfig, OutboxDispatcher, OutboxRecord, OutboxStore

__all__ = [
    # observability
    "Observatory",
    "ScopedObs",
    # metrics
    "Meter",
    "Counter",
    "Gauge",
    "Histogram",
    "NoopMeter",
    "PromMeter",
    # outbox
    "OutboxConfig",
    "OutboxDispatcher",
    "OutboxRecord",
    "OutboxStore",
    # hooks
    "HooksBus",
]
