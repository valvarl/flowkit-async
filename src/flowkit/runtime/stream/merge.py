# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Merge arbiters and bounded source buffers.

This module provides:
- pluggable merge strategies (interleave, concat, priority)
- a tiny bounded in-memory buffer (deque-based)
- a stateless arbiter that pops items in the chosen order

Everything is synchronous and allocation-light; integration with async
adapters is done by feeding buffers from adapter tasks.
"""

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Generic, Iterable, MutableMapping, Optional, Protocol, TypeVar


__all__ = [
    "SourceState",
    "SourceBuffer",
    "BoundedDequeBuffer",
    "MergeStrategy",
    "InterleaveStrategy",
    "ConcatStrategy",
    "PriorityStrategy",
    "MergeRegistry",
    "get_default_merge_registry",
    "MergeArbiter",
]


T = TypeVar("T")


# ---------------------------------------------------------------------------
# State snapshot
# ---------------------------------------------------------------------------


@dataclass
class SourceState:
    """Lightweight snapshot of a source buffer for a merge strategy.

    Attributes:
        alias: Logical alias of the source.
        queue_size: Current number of items ready to be popped.
        exhausted: True when the source has reached EOF (no more items will arrive).
        priority: Lower number means higher priority (used by PriorityStrategy).
        weight: Relative weight for weighted round-robin (future use).
        paused: Temporarily paused due to backpressure or upstream signal.
        stats: Mutable counters (e.g. {"served": 10}).
    """

    alias: str
    queue_size: int = 0
    exhausted: bool = False
    priority: int = 0
    weight: float = 1.0
    paused: bool = False
    stats: Dict[str, int] = field(default_factory=lambda: {"served": 0})


# ---------------------------------------------------------------------------
# Bounded buffer
# ---------------------------------------------------------------------------


class SourceBuffer(Protocol, Generic[T]):
    """Minimal buffer protocol used by the arbiter."""

    def put_nowait(self, item: T) -> bool: ...
    def get_nowait(self) -> T: ...
    def qsize(self) -> int: ...
    def empty(self) -> bool: ...
    def full(self) -> bool: ...
    def close(self) -> None: ...
    def closed(self) -> bool: ...


class BoundedDequeBuffer(Generic[T]):
    """Fast, GC-friendly deque-based bounded buffer.

    - put_nowait returns False when full (callers may block/backoff upstream).
    - close() marks the buffer as exhausted; get_nowait can still drain remaining items.
    """

    __slots__ = ("_dq", "_maxsize", "_closed")

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0")
        self._dq: Deque[T] = deque()
        self._maxsize = int(maxsize)
        self._closed = False

    def put_nowait(self, item: T) -> bool:
        if self._closed or len(self._dq) >= self._maxsize:
            return False
        self._dq.append(item)
        return True

    def get_nowait(self) -> T:
        return self._dq.popleft()

    def qsize(self) -> int:
        return len(self._dq)

    def empty(self) -> bool:
        return not self._dq

    def full(self) -> bool:
        return len(self._dq) >= self._maxsize

    def close(self) -> None:
        self._closed = True

    def closed(self) -> bool:
        return self._closed


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


class MergeStrategy(Protocol):
    """Strategy that selects which source to pop from next."""

    name: str

    def select_next(self, states: list[SourceState]) -> Optional[str]: ...
    def on_yield(self, alias: str) -> None: ...
    def on_block(self, alias: str) -> None: ...


class InterleaveStrategy:
    """Fair round-robin with light weighting support."""

    name = "interleave"

    def __init__(self) -> None:
        # alias -> debt (lower debt is preferred)
        self._debt: Dict[str, float] = {}

    def select_next(self, states: list[SourceState]) -> Optional[str]:
        ready = [s for s in states if not s.exhausted and not s.paused and s.queue_size > 0]
        if not ready:
            return None

        for s in ready:
            self._debt.setdefault(s.alias, 0.0)

        # Choose the alias with the smallest accumulated debt
        alias = min(ready, key=lambda s: self._debt.get(s.alias, 0.0)).alias
        return alias

    def on_yield(self, alias: str) -> None:
        self._debt[alias] = self._debt.get(alias, 0.0) + 1.0

    def on_block(self, alias: str) -> None:
        # No special penalty for transient blocks
        pass


class ConcatStrategy:
    """Drain one source fully before moving to the next.

    Useful when preserving local order is important.
    """

    name = "concat"

    def __init__(self) -> None:
        self._current: Optional[str] = None

    def select_next(self, states: list[SourceState]) -> Optional[str]:
        by_alias = {s.alias: s for s in states}
        if self._current:
            s = by_alias.get(self._current)
            if s and not s.exhausted and not s.paused and s.queue_size > 0:
                return self._current

        for s in states:
            if not s.exhausted and not s.paused and s.queue_size > 0:
                self._current = s.alias
                return self._current
        return None

    def on_yield(self, alias: str) -> None:
        self._current = alias

    def on_block(self, alias: str) -> None:
        # Could reset _current on persistent block; keep simple for now.
        pass


class PriorityStrategy:
    """Always prefer the highest-priority (numerically smallest) ready source.

    Ties are round-robin among sources of the same priority.
    """

    name = "priority"

    def __init__(self) -> None:
        self._rr_idx: Dict[int, int] = {}  # priority -> next rr index

    def select_next(self, states: list[SourceState]) -> Optional[str]:
        ready = [s for s in states if not s.exhausted and not s.paused and s.queue_size > 0]
        if not ready:
            return None

        by_prio: Dict[int, list[SourceState]] = {}
        for s in ready:
            by_prio.setdefault(s.priority, []).append(s)

        best = min(by_prio.keys())
        pool = by_prio[best]
        idx = self._rr_idx.get(best, 0) % len(pool)
        alias = pool[idx].alias
        self._rr_idx[best] = (idx + 1) % len(pool)
        return alias

    def on_yield(self, alias: str) -> None:
        pass

    def on_block(self, alias: str) -> None:
        pass


# ---------------------------------------------------------------------------
# Strategy registry
# ---------------------------------------------------------------------------


class MergeRegistry:
    """Simple registry of merge strategies (allows plugins/tests to override)."""

    def __init__(self) -> None:
        self._by_name: Dict[str, MergeStrategy] = {}
        self.register(InterleaveStrategy())
        self.register(ConcatStrategy())
        self.register(PriorityStrategy())

    def register(self, strategy: MergeStrategy) -> None:
        self._by_name[strategy.name] = strategy

    def get(self, name: str) -> MergeStrategy:
        try:
            return self._by_name[name]
        except KeyError:
            raise LookupError(f"unknown merge strategy: {name!r}. Known: {sorted(self._by_name.keys())}")


_default_merge_registry: MergeRegistry | None = None


def get_default_merge_registry() -> MergeRegistry:
    global _default_merge_registry
    if _default_merge_registry is None:
        _default_merge_registry = MergeRegistry()
    return _default_merge_registry


# ---------------------------------------------------------------------------
# Arbiter
# ---------------------------------------------------------------------------


class MergeArbiter(Generic[T]):
    """Pop items from multiple bounded buffers according to a merge strategy.

    Typical loop:

        arbiter = MergeArbiter(buffers, strategy=InterleaveStrategy())
        while True:
            out = arbiter.pop()
            if out is None:
                if arbiter.all_exhausted():
                    break
                # nothing ready right now
                continue
            alias, item = out
            ...  # process item

    Pushing items is handled by adapter feeder tasks calling buffer.put_nowait().
    """

    def __init__(
        self,
        buffers: MutableMapping[str, SourceBuffer[T]],
        *,
        strategy: MergeStrategy | None = None,
        priorities: Mapping[str, int] | None = None,
    ) -> None:
        self._buffers = buffers
        self._strategy = strategy or InterleaveStrategy()
        self._paused: Dict[str, bool] = {}
        self._exhausted: Dict[str, bool] = {}
        self._priority: Dict[str, int] = dict(priorities or {})

    # ---- control

    def mark_exhausted(self, alias: str) -> None:
        self._exhausted[alias] = True
        buf = self._buffers.get(alias)
        if buf:
            buf.close()

    def set_paused(self, alias: str, paused: bool) -> None:
        self._paused[alias] = paused

    # ---- queries

    def all_exhausted(self) -> bool:
        if not self._buffers:
            return True
        for a, b in self._buffers.items():
            if not (self._exhausted.get(a) or b.closed()) or not b.empty():
                return False
        return True

    # ---- pop

    def _snapshot(self) -> list[SourceState]:
        out: list[SourceState] = []
        for alias, buf in self._buffers.items():
            out.append(
                SourceState(
                    alias=alias,
                    queue_size=buf.qsize(),
                    exhausted=self._exhausted.get(alias, False) or buf.closed(),
                    priority=self._priority.get(alias, 0),
                    paused=self._paused.get(alias, False),
                )
            )
        return out

    def pop(self) -> tuple[str, T] | None:
        """Return (alias, item) according to the strategy, or None if nothing is ready."""
        states = self._snapshot()
        alias = self._strategy.select_next(states)
        if alias is None:
            return None
        buf = self._buffers.get(alias)
        if buf is None or buf.empty():
            self._strategy.on_block(alias)
            return None
        item = buf.get_nowait()
        self._strategy.on_yield(alias)
        return alias, item
