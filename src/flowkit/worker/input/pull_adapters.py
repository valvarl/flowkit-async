from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable

from ...core.config import WorkerConfig
from ...core.time import Clock
from ...core.utils import stable_hash
from ..handlers.base import Batch


class PullAdapters:
    """
    pull.from_artifacts (+ rechunk) implemented as instance with injected db/clock.
    """

    def __init__(self, *, db, clock: Clock, cfg: WorkerConfig) -> None:
        self.db = db
        self.clock = clock
        self.cfg = cfg

    async def iter_from_artifacts(
        self,
        *,
        task_id: str,
        consumer_node: str,
        from_nodes: list[str],
        poll_ms: int,
        eof_on_task_done: bool,
        backoff_max_ms: int,
        cancel_flag: asyncio.Event | None,
    ) -> AsyncIterator[Batch]:

        claimed_mem: set[tuple[str, str, str, str]] = set()
        if not from_nodes:
            return

        backoff_ms = poll_ms
        already_warned = False

        while True:
            if cancel_flag and cancel_flag.is_set():
                break

            got_any = False
            try:
                for src in from_nodes:
                    matched = 0
                    try:
                        cur = (
                            self.db.artifacts.find(
                                {"task_id": task_id, "node_id": src, "status": "partial"},
                                {"_id": 0, "task_id": 1, "node_id": 1, "batch_uid": 1, "meta": 1, "created_at": 1},
                            )
                            .sort([("created_at", 1)])
                            .limit(200)
                        )
                        async for a in cur:
                            batch_uid = a.get("batch_uid")
                            if not batch_uid:
                                continue
                            try:
                                await self.db.stream_progress.insert_one(
                                    {
                                        "task_id": task_id,
                                        "consumer_node": consumer_node,
                                        "from_node": src,
                                        "batch_uid": batch_uid,
                                        "claimed_at": self.clock.now_dt(),
                                    }
                                )
                            except Exception:
                                continue
                            matched += 1
                            got_any = True
                            backoff_ms = poll_ms
                            yield Batch(
                                batch_uid=batch_uid,
                                payload={
                                    "from_node": src,
                                    "ref": {"task_id": task_id, "node_id": src, "batch_uid": batch_uid},
                                    "meta": a.get("meta") or {},
                                },
                            )
                    except Exception:
                        rows = getattr(getattr(self.db, "artifacts", None), "rows", None)
                        if isinstance(rows, list):
                            for a in list(rows):
                                if (
                                    a.get("task_id") != task_id
                                    or a.get("node_id") != src
                                    or a.get("status") != "partial"
                                ):
                                    continue
                                batch_uid = a.get("batch_uid") or stable_hash(
                                    {"task_id": task_id, "node_id": src, "meta": a.get("meta", {})}
                                )
                                key = (task_id, consumer_node, src, batch_uid)
                                if key in claimed_mem:
                                    continue
                                try:
                                    await self.db.stream_progress.insert_one(
                                        {
                                            "task_id": task_id,
                                            "consumer_node": consumer_node,
                                            "from_node": src,
                                            "batch_uid": batch_uid,
                                            "claimed_at": self.clock.now_dt(),
                                        }
                                    )
                                except Exception:
                                    pass
                                claimed_mem.add(key)
                                matched += 1
                                got_any = True
                                backoff_ms = poll_ms
                                yield Batch(
                                    batch_uid=batch_uid,
                                    payload={
                                        "from_node": src,
                                        "ref": {"task_id": task_id, "node_id": src, "batch_uid": batch_uid},
                                        "meta": (a.get("meta") or {}),
                                    },
                                )

                if got_any:
                    continue

                if eof_on_task_done:
                    all_complete = True
                    for src in from_nodes:
                        try:
                            c = await self.db.artifacts.count_documents(
                                {"task_id": task_id, "node_id": src, "status": "complete"}
                            )
                            if c <= 0:
                                all_complete = False
                                break
                        except Exception:
                            all_complete = False
                            break
                    if all_complete:
                        break

                await self.clock.sleep_ms(backoff_ms)
                backoff_ms = min(backoff_ms * 2, backoff_max_ms)
                already_warned = False

            except Exception:
                if not already_warned:
                    # one-time warn per backoff window
                    already_warned = True
                await self.clock.sleep_ms(backoff_ms)
                backoff_ms = min(backoff_ms * 2, backoff_max_ms)

    async def iter_from_artifacts_rechunk(
        self,
        *,
        task_id: str,
        consumer_node: str,
        from_nodes: list[str],
        size: int,
        poll_ms: int,
        eof_on_task_done: bool,
        backoff_max_ms: int,
        meta_list_key: str | None,
        cancel_flag: asyncio.Event | None,
    ) -> AsyncIterator[Batch]:
        if size <= 0:
            size = 1

        async def _inner():
            base = self.iter_from_artifacts(
                task_id=task_id,
                consumer_node=consumer_node,
                from_nodes=from_nodes,
                poll_ms=poll_ms,
                eof_on_task_done=eof_on_task_done,
                backoff_max_ms=backoff_max_ms,
                cancel_flag=cancel_flag,
            )
            async for b in base:
                src = (b.payload or {}).get("from_node") or (b.payload or {}).get("ref", {}).get("node_id")
                parent_uid = b.batch_uid or stable_hash({"payload": b.payload})
                meta = (b.payload or {}).get("meta") or {}

                key = meta_list_key
                if key is None:
                    for cand in ("items", "skus", "enriched", "ocr"):
                        if isinstance(meta.get(cand), list):
                            key = cand
                            break
                items = meta.get(key) if key else None
                if not isinstance(items, list):
                    items = [meta]

                idx = 0
                for i in range(0, len(items), size):
                    chunk = items[i : i + size]
                    chunk_uid = stable_hash({"src": src, "parent": parent_uid, "idx": idx})
                    yield Batch(
                        batch_uid=chunk_uid,
                        payload={
                            "from_node": src,
                            "items": chunk,
                            "parent": {"ref": {"batch_uid": parent_uid}, "list_key": key},
                        },
                    )
                    idx += 1

        async for y in _inner():
            if cancel_flag and cancel_flag.is_set():
                break
            yield y


def build_input_adapters(*, db, clock: Clock, cfg: WorkerConfig) -> dict[str, Callable[..., AsyncIterator[Batch]]]:
    impl = PullAdapters(db=db, clock=clock, cfg=cfg)
    return {
        "pull.from_artifacts": impl.iter_from_artifacts,
        "pull.from_artifacts.rechunk:size": impl.iter_from_artifacts_rechunk,
    }
