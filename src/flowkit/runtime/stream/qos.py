# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
QoS primitives: backpressure signaling and adaptive chunk sizing.

Everything here is lock-free and allocation-light to live on the hot path.
"""

from dataclasses import dataclass

__all__ = [
    "AdaptiveChunkSizer",
    "BackpressureController",
    "TokenBucket",
]


# ---------------------------------------------------------------------------
# Backpressure
# ---------------------------------------------------------------------------


@dataclass
class BackpressureController:
    """
    Hysteresis-based backpressure controller for a bounded queue.

    When qsize >= high_watermark -> paused=True
    When qsize <= low_watermark  -> paused=False

    This prevents oscillation around a single threshold.
    """

    maxsize: int
    high_ratio: float = 0.8
    low_ratio: float = 0.5

    def __post_init__(self) -> None:
        if not (0.0 < self.low_ratio < self.high_ratio < 1.0):
            raise ValueError("expected 0.0 < low_ratio < high_ratio < 1.0")
        if self.maxsize <= 0:
            raise ValueError("maxsize must be > 0")
        self._paused = False
        self._high = max(1, int(self.maxsize * self.high_ratio))
        self._low = max(0, int(self.maxsize * self.low_ratio))

    @property
    def paused(self) -> bool:
        return self._paused

    def update(self, qsize: int) -> bool:
        """
        Update state from current qsize; return the new paused flag.
        """
        if not self._paused and qsize >= self._high:
            self._paused = True
        elif self._paused and qsize <= self._low:
            self._paused = False
        return self._paused


# ---------------------------------------------------------------------------
# Adaptive chunk sizing
# ---------------------------------------------------------------------------


@dataclass
class AdaptiveChunkSizer:
    """
    Simple AIMD-like (Additive Increase / Multiplicative Decrease) chunk sizer.

    Inputs:
      - observed latency (ms) per chunk
      - whether the producer/consumer was blocked by backpressure
      - optional size_bytes to bias by bandwidth

    Behavior:
      - increase by +inc when things are healthy
      - decrease by *decay when backpressure/latency too high
    """

    min_items: int = 1
    max_items: int = 10_000
    start_items: int = 64
    inc_items: int = 16
    decay: float = 0.5
    latency_target_ms: int = 250

    def __post_init__(self) -> None:
        self._n = min(max(self.start_items, self.min_items), self.max_items)

    def next(self) -> int:
        """Current chunk size to request/accumulate."""
        return self._n

    def on_result(self, *, latency_ms: int, blocked: bool, size_bytes: int | None = None) -> None:
        """
        Update heuristics based on the last chunk execution.
        """
        too_slow = latency_ms > self.latency_target_ms
        if blocked or too_slow:
            # multiplicative decrease
            self._n = max(self.min_items, int(self._n * self.decay) or self.min_items)
            return
        # additive increase
        self._n = min(self.max_items, self._n + self.inc_items)


# ---------------------------------------------------------------------------
# Token bucket (optional rate limiter)
# ---------------------------------------------------------------------------


@dataclass
class TokenBucket:
    """
    Minimal token bucket in discrete steps.

    Call `consume(n)` to try acquiring `n` tokens. Refill periodically using
    `refill(step_tokens)`, typically on a timer in the runtime loop.
    """

    capacity: int
    tokens: int
    # optional floor to avoid stalling forever due to rounding/drift
    min_grant: int = 1

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.tokens = min(max(self.tokens, 0), self.capacity)

    def consume(self, n: int) -> bool:
        if n <= 0:
            return True
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    def refill(self, n: int) -> None:
        if n <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + n)
