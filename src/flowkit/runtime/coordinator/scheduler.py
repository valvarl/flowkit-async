# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Priority/concurrency-aware scheduler queues.

This module tracks "runnable" node executions and decides which ones may be
dispatched next, enforcing per-key concurrency limits and providing fair
round-robin between keys of the same priority.

Design notes:
- Higher number => higher priority (20 outranks 10).
- Within the same priority and concurrency availability, we round-robin across
  concurrency keys, then FIFO within each key.
- Each "item" represents a (task_id, node_id) to run once.
"""

from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class QueueItem:
    task_id: str
    node_id: str
    step_type: str
    priority: int = 0
    concurrency_key: str | None = None
    concurrency_limit: int | None = None


class Scheduler:
    """In-memory scheduler core (Coordinator wires persistence/leases externally)."""

    def __init__(self) -> None:
        # priority -> key -> queue[item...]
        self._queues: dict[int, dict[str, deque[QueueItem]]] = defaultdict(lambda: defaultdict(deque))
        # current running counters per key
        self._inflight_by_key: dict[str, int] = defaultdict(int)
        # round-robin cursor per priority over keys
        self._rr_idx: dict[int, int] = defaultdict(int)

    # ---- enqueue / cancel

    def offer(self, item: QueueItem) -> None:
        key = item.concurrency_key or "_none"
        self._queues[item.priority][key].append(item)

    def cancel(self, task_id: str, node_id: str) -> int:
        """Remove queued items; returns the number of removed items."""
        removed = 0
        for prio, by_key in self._queues.items():
            for key, q in by_key.items():
                keep: deque[QueueItem] = deque()
                while q:
                    it = q.popleft()
                    if it.task_id == task_id and it.node_id == node_id:
                        removed += 1
                    else:
                        keep.append(it)
                by_key[key] = keep
        return removed

    # ---- release inflight (should be called by Coordinator on finish/fail/cancel)

    def release(self, concurrency_key: str | None) -> None:
        if not concurrency_key:
            return
        cur = self._inflight_by_key.get(concurrency_key, 0)
        if cur > 0:
            self._inflight_by_key[concurrency_key] = cur - 1

    # ---- scheduling

    def pop_ready(self, max_n: int = 1) -> list[QueueItem]:
        """
        Pick up to max_n runnable items respecting concurrency limits.
        """
        picked: list[QueueItem] = []
        for prio in sorted(self._queues.keys(), reverse=True):
            by_key = self._queues[prio]
            if not by_key:
                continue
            keys = sorted(by_key.keys())
            if not keys:
                continue
            idx = self._rr_idx.get(prio, 0) % len(keys)
            visited = 0
            while visited < len(keys) and len(picked) < max_n:
                key = keys[idx]
                q = by_key[key]
                if q:
                    head = q[0]
                    if self._can_run(head):
                        picked.append(q.popleft())
                        self._mark_running(head)
                idx = (idx + 1) % len(keys)
                visited += 1
            self._rr_idx[prio] = idx
            if len(picked) >= max_n:
                break
        return picked

    # ---- helpers

    def _cur_limit(self, key: str | None, want_limit: int | None) -> tuple[int, int]:
        if not key or not want_limit:
            return (0, 1)  # effectively limitless (treated as available)
        return (self._inflight_by_key.get(key, 0), want_limit)

    def _can_run(self, it: QueueItem) -> bool:
        cur, lim = self._cur_limit(it.concurrency_key, it.concurrency_limit)
        return cur < lim

    def _mark_running(self, it: QueueItem) -> None:
        if it.concurrency_key and it.concurrency_limit:
            self._inflight_by_key[it.concurrency_key] += 1
