"""
Unit tests for PullAdapters.iter_from_artifacts_rechunk() selection rules.

Contract:
  - If meta_list_key is provided and points to a list → chunk that list.
  - Otherwise → treat the entire meta as a single item (wrap into a one-element list), no heuristics.

Notes:
  * We keep these tests unit-level (no Kafka/Coordinator). To let the adapter
    terminate its polling loop, we set `eof_on_task_done=True` and append a
    synthetic `{status: "complete"}` marker for the source node. Our stub
    `count_documents` recognizes such queries.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from flowkit.core.config import WorkerConfig
from flowkit.core.time import SystemClock
from flowkit.worker.input.pull_adapters import PullAdapters

pytestmark = [pytest.mark.unit, pytest.mark.adapters]


class _InMemCol:
    """Minimal collection-like shim backing tests via the adapter's 'rows' fallback path."""

    def __init__(self):
        self.rows = []

    async def count_documents(self, query: dict, *_args, **_kwargs):
        """
        Recognize EOF checks like:
          {"task_id": t, "node_id": src, "status": "complete"}
        Return 1 if there is at least one matching row, else 0.
        """

        def _match(row: dict, q: dict) -> bool:
            return all(row.get(k) == v for k, v in (q or {}).items())

        return sum(1 for r in self.rows if _match(r, query))

    def find(self, *_args, **_kwargs):
        # Force adapter to use fallback branch (raise to enter 'except' path that uses .rows)
        class _Cursor:
            async def __aiter__(self):  # pragma: no cover
                raise RuntimeError("force-fallback")

        return _Cursor()

    def create_index(self, *_args, **_kwargs):  # pragma: no cover
        return None


class _InMemDB:
    def __init__(self):
        self.artifacts = _InMemCol()
        self.stream_progress = SimpleNamespace(insert_one=lambda *a, **k: None)


async def _collect(async_iter):
    return [x async for x in async_iter]


@pytest.mark.asyncio
async def test_rechunk_with_meta_list_key_splits_that_list():
    db = _InMemDB()
    cfg = WorkerConfig()
    clock = SystemClock()
    pa = PullAdapters(db=db, clock=clock, cfg=cfg)

    # Prepare one upstream artifact with list in meta
    db.artifacts.rows.append(
        {
            "task_id": "t",
            "node_id": "src",
            "status": "partial",
            "batch_uid": "b1",
            "meta": {"skus": [1, 2, 3, 4]},
        }
    )
    # Add completion marker so the adapter terminates after yielding
    db.artifacts.rows.append(
        {
            "task_id": "t",
            "node_id": "src",
            "status": "complete",
        }
    )

    it = pa.iter_from_artifacts_rechunk(
        task_id="t",
        consumer_node="dst",
        from_nodes=["src"],
        size=2,
        poll_ms=5,
        eof_on_task_done=True,  # critical to stop after 'complete'
        backoff_max_ms=100,
        cancel_flag=asyncio.Event(),
        meta_list_key="skus",
    )
    batches = await _collect(it)
    # Expect two chunks: [1,2], [3,4]
    sizes = [len((b.payload or {}).get("items") or []) for b in batches]
    assert sizes == [2, 2]


@pytest.mark.asyncio
async def test_rechunk_without_meta_list_key_wraps_meta_as_single_item():
    db = _InMemDB()
    cfg = WorkerConfig()
    clock = SystemClock()
    pa = PullAdapters(db=db, clock=clock, cfg=cfg)

    db.artifacts.rows.extend(
        [
            {"task_id": "t", "node_id": "src", "status": "partial", "batch_uid": "b1", "meta": {"skus": [1, 2, 3]}},
            {"task_id": "t", "node_id": "src", "status": "partial", "batch_uid": "b2", "meta": {"skus": [4, 5, 6]}},
        ]
    )
    # Completion marker to end the loop
    db.artifacts.rows.append({"task_id": "t", "node_id": "src", "status": "complete"})

    it = pa.iter_from_artifacts_rechunk(
        task_id="t",
        consumer_node="dst",
        from_nodes=["src"],
        size=10,  # big enough to keep one chunk per parent artifact
        poll_ms=5,
        eof_on_task_done=True,  # stop after we see 'complete'
        backoff_max_ms=100,
        cancel_flag=asyncio.Event(),
        meta_list_key=None,  # <-- critical: no list key
    )
    batches = await _collect(it)
    # Each parent artifact yields exactly one chunk with a single-item 'items' list (the meta)
    sizes = [len((b.payload or {}).get("items") or []) for b in batches]
    assert sizes == [1, 1]
