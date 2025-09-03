from typing import Any, Dict, Iterable, Optional, List

from flowkit.worker.handlers.base import RoleHandler, Batch, BatchResult

from ..util import dbg, stable_hash


class IndexerHandler(RoleHandler):
    role = "indexer"

    async def init(self, cfg):
        # task-aware initialization
        self._task_id, self._epoch = cfg["task_id"], cfg["attempt_epoch"]

    async def load_input(self, ref, inline):
        return inline or {}

    async def iter_batches(self, loaded):
        total = int(loaded.get("total_skus", 12))
        bs = int(loaded.get("batch_size", 5))
        skus = [f"sku-{i}" for i in range(total)]
        for idx in range(0, total, bs):
            chunk = skus[idx : idx + bs]
            uid = stable_hash({"node": "w1", "idx": idx // bs})
            dbg("HNDL.indexer.yield", task_id=self._task_id, epoch=self._epoch, batch_uid=uid, count=len(chunk))
            yield Batch(batch_uid=uid, payload={"skus": chunk})

    async def process_batch(self, batch, ctx):
        import asyncio
        import os

        delay = float(os.getenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.25"))
        await asyncio.sleep(delay)
        return BatchResult(success=True, metrics={"skus": batch.payload["skus"], "count": len(batch.payload["skus"])})


class _PullFromArtifactsMixin:
    async def _emit_from_artifacts(
        self, *, from_nodes: List[str], size: int, meta_key: str, poll: float, role_tag: str
    ):
        """
        Stream items from in-mem artifacts ONLY for current task_id; slice in chunks.
        Dedup key: (task_id, attempt_epoch, src_node, parent_uid).
        """
        if not hasattr(self, "_emitted"):
            self._emitted = {}

        completed_nodes = set()

        import asyncio

        while True:
            progressed = False
            has_unseen = False

            # NOTE: db is injected into handler as `self.db` from test fixture.
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
                        chunk = new_items[i : i + size]
                        chunk_uid = stable_hash({"src": node_id, "parent": parent_uid, "idx": idx_local})
                        dbg(
                            "HNDL.emit",
                            role=role_tag,
                            task_id=self._task_id,
                            epoch=self._epoch,
                            src=node_id,
                            parent_uid=parent_uid,
                            batch_uid=chunk_uid,
                            chunk=len(chunk),
                        )
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
    role = "enricher"

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
        items = batch.payload.get("items")
        if not items:
            return BatchResult(success=True, metrics={"noop": 1})
        enriched = [{"sku": (x if isinstance(x, str) else x.get("sku", x)), "enriched": True} for x in items]
        return BatchResult(success=True, metrics={"enriched": enriched, "count": len(enriched)})


class OCRHandler(_PullFromArtifactsMixin, RoleHandler):
    role = "ocr"

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
        async for b in self._emit_from_artifacts(from_nodes=from_nodes, size=size, meta_key=meta_key, poll=poll, role_tag="ocr"):
            yield b

    async def process_batch(self, batch, ctx):
        items = batch.payload.get("items")
        if not items:
            return BatchResult(success=True, metrics={"noop": 1})
        ocrd = [{"sku": (it["sku"] if isinstance(it, dict) else it), "ocr_ok": True} for it in items]
        return BatchResult(success=True, metrics={"ocr": ocrd, "count": len(ocrd)})


class AnalyzerHandler(_PullFromArtifactsMixin, RoleHandler):
    role = "analyzer"

    async def init(self, cfg):
        self._task_id, self._epoch = cfg["task_id"], cfg["attempt_epoch"]

    async def load_input(self, ref, inline):
        # Return inline as-is so the worker can use an input adapter if needed.
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
        if n:
            return BatchResult(success=True, metrics={"count": n, "sinked": n})
        return BatchResult(success=True, metrics={"noop": 1})


def make_test_handlers(_, db, include: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """
    Instantiate handlers for selected roles and inject `db` into each instance.
    Example: make_test_handlers(wu, db, include=["indexer","analyzer"])
    """
    registry = {
        "indexer": IndexerHandler,
        "enricher": EnricherHandler,
        "ocr": OCRHandler,
        "analyzer": AnalyzerHandler,
    }
    names = list(include) if include else list(registry.keys())
    out: Dict[str, Any] = {}
    for name in names:
        inst = registry[name]()
        inst.db = db
        out[name] = inst
    return out
