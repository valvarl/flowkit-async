from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, List, Optional

from flowkit.worker.handlers.base import RoleHandler, Batch, BatchResult, FinalizeResult  # type: ignore

from tests.helpers.util import dbg, stable_hash


# ───────────────────────── Index-like (graph fan-in/out) ─────────────────────────

class IndexerHandler(RoleHandler):
    """Synthetic indexer that emits SKUs in batches and reports per-batch counts."""
    role = "indexer"

    def __init__(self, *, db: Any) -> None:
        self.db = db
        self._task_id: Optional[str] = None
        self._epoch: Optional[int] = None

    async def init(self, cfg):
        self._task_id, self._epoch = cfg["task_id"], cfg["attempt_epoch"]

    async def load_input(self, ref, inline):
        return inline or {}

    async def iter_batches(self, loaded):
        total = int(loaded.get("total_skus", 12))
        bs = int(loaded.get("batch_size", 5))
        skus = [f"sku-{i}" for i in range(total)]
        for idx in range(0, total, bs):
            chunk = skus[idx: idx + bs]
            uid = stable_hash({"node": "w1", "idx": idx // bs})
            dbg("HNDL.indexer.yield", task_id=self._task_id, epoch=self._epoch, batch_uid=uid, count=len(chunk))
            yield Batch(batch_uid=uid, payload={"skus": chunk})

    async def process_batch(self, batch, ctx):
        # configurable think-time via env to keep old behavior compatible
        import os
        delay = float(os.getenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.25"))
        if delay > 0:
            await asyncio.sleep(delay)
        skus = batch.payload.get("skus") or []
        return BatchResult(success=True, metrics={"skus": skus, "count": len(skus)})


class _PullFromArtifactsMixin:
    """
    Utility for streaming items from artifacts of upstream nodes for the SAME task.
    Deduplication key: (task_id, attempt_epoch, src_node, parent_uid).
    """

    async def _emit_from_artifacts(
        self, *, from_nodes: List[str], size: int, meta_key: str, poll: float, role_tag: str
    ):
        if not hasattr(self, "_emitted"):
            self._emitted: Dict[tuple, set] = {}
        completed_nodes: set[str] = set()

        while True:
            progressed = False
            has_unseen = False

            # NOTE: `self.db` is injected by builder; rows live under db.artifacts.rows
            for doc in list(self.db.artifacts.rows):  # type: ignore[attr-defined]
                if doc.get("task_id") != self._task_id:
                    continue
                node_id = doc.get("node_id")
                if node_id not in from_nodes:
                    continue

                parent_uid = doc.get("batch_uid")
                meta = (doc.get("meta") or {})
                items = list(meta.get(meta_key) or [])

                if doc.get("status") == "complete":
                    completed_nodes.add(node_id)
                    continue

                seen_key = (self._task_id, self._epoch, node_id, parent_uid)
                seen = self._emitted.get(seen_key, set())
                new_items = [x for x in items if x not in seen]

                if new_items:
                    idx_local = 0
                    for i in range(0, len(new_items), size):
                        chunk = new_items[i: i + size]
                        chunk_uid = stable_hash({"src": node_id, "parent": parent_uid, "idx": idx_local})
                        dbg("HNDL.emit",
                            role=role_tag, task_id=self._task_id, epoch=self._epoch,
                            src=node_id, parent_uid=parent_uid, batch_uid=chunk_uid, chunk=len(chunk))
                        yield Batch(
                            batch_uid=chunk_uid,
                            payload={"items": chunk, "parent": {"batch_uid": parent_uid, "list_key": meta_key}},
                        )
                        progressed = True
                        idx_local += 1
                    seen.update(new_items)
                    self._emitted[seen_key] = seen

                if any(x not in seen for x in items):
                    has_unseen = True

            if all(n in completed_nodes for n in from_nodes) and not has_unseen:
                break
            if not progressed:
                await asyncio.sleep(poll)


class EnricherHandler(_PullFromArtifactsMixin, RoleHandler):
    """Takes items from artifacts, tags them with `enriched=True`, tracks counts."""
    role = "enricher"

    def __init__(self, *, db: Any) -> None:
        self.db = db
        self._task_id = None
        self._epoch = None

    async def init(self, cfg):
        self._task_id, self._epoch = cfg["task_id"], cfg["attempt_epoch"]

    async def load_input(self, ref, inline):
        return {"input_inline": inline or {}}

    async def iter_batches(self, loaded):
        ii = (loaded or {}).get("input_inline") or {}
        args = ii.get("input_args", {})
        from_nodes = list(args.get("from_nodes", []))
        size = int(args.get("size", 1))
        meta_key = args.get("meta_list_key", "items")
        poll = float(args.get("poll_ms", 50)) / 1000.0
        async for b in self._emit_from_artifacts(
            from_nodes=from_nodes, size=size, meta_key=meta_key, poll=poll, role_tag="enricher"
        ):
            yield b

    async def process_batch(self, batch, ctx):
        items = batch.payload.get("items") or []
        if not items:
            return BatchResult(success=True, metrics={"noop": 1})
        enriched = [{"sku": (x if isinstance(x, str) else x.get("sku", x)), "enriched": True} for x in items]
        return BatchResult(success=True, metrics={"enriched": enriched, "count": len(enriched)})


class OCRHandler(_PullFromArtifactsMixin, RoleHandler):
    """Turns items into `ocr_ok=True` rows; mirrors Enricher flow."""
    role = "ocr"

    def __init__(self, *, db: Any) -> None:
        self.db = db
        self._task_id = None
        self._epoch = None

    async def init(self, cfg):
        self._task_id, self._epoch = cfg["task_id"], cfg["attempt_epoch"]

    async def load_input(self, ref, inline):
        return {"input_inline": inline or {}}

    async def iter_batches(self, loaded):
        ii = (loaded or {}).get("input_inline") or {}
        args = ii.get("input_args", {})
        from_nodes = list(args.get("from_nodes", []))
        size = int(args.get("size", 1))
        meta_key = args.get("meta_list_key", "items")
        poll = float(args.get("poll_ms", 40)) / 1000.0
        async for b in self._emit_from_artifacts(
            from_nodes=from_nodes, size=size, meta_key=meta_key, poll=poll, role_tag="ocr"
        ):
            yield b

    async def process_batch(self, batch, ctx):
        items = batch.payload.get("items") or []
        if not items:
            return BatchResult(success=True, metrics={"noop": 1})
        ocrd = [{"sku": (it["sku"] if isinstance(it, dict) else it), "ocr_ok": True} for it in items]
        return BatchResult(success=True, metrics={"ocr": ocrd, "count": len(ocrd)})


class AnalyzerHandler(_PullFromArtifactsMixin, RoleHandler):
    """Counts/sinks items from upstream; used as final consumer in tests."""
    role = "analyzer"

    def __init__(self, *, db: Any) -> None:
        self.db = db
        self._task_id = None
        self._epoch = None

    async def init(self, cfg):
        self._task_id, self._epoch = cfg["task_id"], cfg["attempt_epoch"]

    async def load_input(self, ref, inline):
        # Keep inline so worker can still use input adapters if necessary.
        return inline or {}

    async def iter_batches(self, loaded):
        args = (loaded or {}).get("input_args", {}) or {}
        from_nodes = list(args.get("from_nodes") or [])
        meta_key = args.get("meta_list_key") or "skus"
        size = int(args.get("size") or 3)
        poll = float(args.get("poll_ms", 25) or 25) / 1000.0
        async for b in self._emit_from_artifacts(
            from_nodes=from_nodes, size=size, meta_key=meta_key, poll=poll, role_tag="analyzer"
        ):
            yield b

    async def process_batch(self, batch, ctx):
        payload = batch.payload or {}
        items = payload.get("items") or payload.get("skus") or []
        n = len(items)
        return BatchResult(success=True, metrics={"count": n} if n else {"noop": 1})


# ───────────────────────── Source-like (status/lease/heartbeat tests) ─────────────────────────

class _BaseSource(RoleHandler):
    """
    A simple source that generates batches of integer items.
    Used in reliability/cancellation/heartbeat tests.
    """
    role = "source"

    def __init__(self, *, db: Any, total: int = 1, batch: int = 1, delay: float = 0.0) -> None:
        self.db = db
        self.total = int(total)
        self.batch = max(1, int(batch))
        self.delay = float(delay)

    async def load_input(self, ref, inline):
        return {"total": self.total, "batch": self.batch, "delay": self.delay}

    async def iter_batches(self, loaded):
        t, b = int(loaded["total"]), int(loaded["batch"])
        shard = 0
        for i in range(0, t, b):
            lo, hi = i, min(i + b, t)
            payload = {"items": list(range(lo, hi))}
            if self.delay > 0:
                payload["delay"] = self.delay
            yield Batch(batch_uid=f"s-{shard}", payload=payload)
            shard += 1

    async def process_batch(self, batch, ctx):
        d = float(batch.payload.get("delay", self.delay) or 0.0)
        if d > 0:
            await asyncio.sleep(d)
        n = len(batch.payload.get("items") or [])
        return BatchResult(success=True, metrics={"count": n})


class _CancelableSource(_BaseSource):
    """Source whose processing loop cooperatively respects cancellation via ctx."""
    async def process_batch(self, batch, ctx):
        d = float(batch.payload.get("delay", self.delay) or 0.3)
        # sleep in small steps to give the runtime a chance to cancel
        step = 0.05
        remain = d
        while remain > 0:
            if hasattr(ctx, "cancelled") and callable(ctx.cancelled) and ctx.cancelled():
                return BatchResult(success=False, permanent=False, reason_code="cancelled")
            await asyncio.sleep(min(step, remain))
            remain -= step
        return await super().process_batch(batch, ctx)


class _FlakyOnce(RoleHandler):
    """Fails the first batch (transient), then succeeds on retry."""
    role = "flaky"

    def __init__(self, *, db: Any) -> None:
        self.db = db
        self._failed_once = False

    async def load_input(self, ref, inline):
        return inline or {}

    async def iter_batches(self, loaded):
        yield Batch(batch_uid="only", payload={"items": [1, 2, 3]})

    async def process_batch(self, batch, ctx):
        if not self._failed_once:
            self._failed_once = True
            return BatchResult(success=False, permanent=False, error="transient glitch")
        return BatchResult(success=True, metrics={"count": len(batch.payload.get("items", []))})


class _PermanentFail(RoleHandler):
    """Always fails permanently; used to test cascade cancel."""
    role = "a"

    def __init__(self, *, db: Any) -> None:
        self.db = db

    async def load_input(self, ref, inline):
        return {}

    async def iter_batches(self, loaded):
        yield Batch(batch_uid="only", payload={"x": 1})

    async def process_batch(self, batch, ctx):
        # Raise and classify as permanent through classify_error hook.
        raise RuntimeError("hard_fail")

    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        # (reason_code, permanent)
        return ("hard", True)


class _Noop(RoleHandler):
    """Role that produces a single no-op batch and succeeds."""
    def __init__(self, *, db: Any, role: str) -> None:
        self.db = db
        self.role = role  # worker uses handler.role for topic naming

    async def load_input(self, ref, inline):
        return {}

    async def iter_batches(self, loaded):
        yield Batch(batch_uid=f"{self.role}-0", payload={})

    async def process_batch(self, batch, ctx):
        await asyncio.sleep(0.01)
        return BatchResult(success=True, metrics={"count": 1})


class _Sleepy(RoleHandler):
    """Emits N batches and sleeps in process to simulate long CPU/IO work."""
    def __init__(self, *, db: Any, role: str, batches: int = 1, sleep_s: float = 1.2) -> None:
        self.db = db
        self.role = role
        self.batches = int(batches)
        self.sleep_s = float(sleep_s)

    async def load_input(self, ref, inline):
        return {}

    async def iter_batches(self, loaded):
        for i in range(self.batches):
            yield Batch(batch_uid=f"{self.role}-{i}", payload={"i": i})

    async def process_batch(self, batch, ctx):
        await asyncio.sleep(self.sleep_s)
        return BatchResult(success=True, metrics={"count": 1})

    async def finalize(self, ctx):  # not strictly needed, but harmless
        return FinalizeResult(metrics={})


class _NoopQueryOnly(RoleHandler):
    """One-shot batch, mainly for query-path sanity tests."""
    def __init__(self, *, db: Any, role: str) -> None:
        self.db = db
        self.role = role

    async def load_input(self, ref, inline):
        return {}

    async def iter_batches(self, loaded):
        yield Batch(batch_uid=f"{self.role}-0", payload={})

    async def process_batch(self, batch, ctx):
        return BatchResult(success=True, metrics={"noop": 1})


# ───────────────────────── Public builders (factory-friendly) ─────────────────────────

# graph/pipeline handlers
def build_indexer_handler(*, db: Any) -> RoleHandler: return IndexerHandler(db=db)
def build_enricher_handler(*, db: Any) -> RoleHandler: return EnricherHandler(db=db)
def build_ocr_handler(*, db: Any) -> RoleHandler:     return OCRHandler(db=db)
def build_analyzer_handler(*, db: Any) -> RoleHandler: return AnalyzerHandler(db=db)

# source/reliability handlers
def build_counting_source_handler(*, db: Any, total: int, batch: int) -> RoleHandler:
    return _BaseSource(db=db, total=total, batch=batch, delay=0.0)

def build_slow_source_handler(*, db: Any, total: int, batch: int, delay: float) -> RoleHandler:
    return _BaseSource(db=db, total=total, batch=batch, delay=delay)

def build_cancelable_source_handler(*, db: Any, total: int, batch: int, delay: float) -> RoleHandler:
    return _CancelableSource(db=db, total=total, batch=batch, delay=delay)

def build_flaky_once_handler(*, db: Any) -> RoleHandler:
    return _FlakyOnce(db=db)

def build_permanent_fail_handler(*, db: Any, role: str = "a") -> RoleHandler:
    h = _PermanentFail(db=db)
    h.role = role  # allow "a"/custom role for cascade graphs
    return h

def build_noop_handler(*, db: Any, role: str) -> RoleHandler:
    return _Noop(db=db, role=role)

def build_sleepy_handler(*, db: Any, role: str, batches: int = 1, sleep_s: float = 1.2) -> RoleHandler:
    return _Sleepy(db=db, role=role, batches=batches, sleep_s=sleep_s)

def build_noop_query_only_role(*, db: Any, role: str) -> RoleHandler:
    return _NoopQueryOnly(db=db, role=role)
