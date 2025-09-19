# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Any, Union

from ...api.adapters import AdapterContext, SourceAdapter
from ...api.registry import PluginRegistry
from ...api.streams import Batch, Checkpoint, Item
from .checkpoints import CheckpointsStore
from .hooks_runner import WorkerHooksRunner
from .metrics import WorkerMetrics
from .ops_runner import OpsChain
from .outputs_runner import SinksRunner

StreamEl = Union[Item, Batch]


class StreamEngine:
    """
    Source(s) → Transform chain → Merge → Sink(s)

    Responsibilities:
      - lifecycle of sources/transforms/sinks
      - cooperative backpressure (bounded internal queue)
      - checkpointing: commit AFTER durable send
      - cancellation & graceful drain
      - worker-side hooks

    Assumptions about adapters:
      - SourceAdapter provides an async iterator of (element, optional checkpoint, optional idempotency_key).
        Shapes supported per item:
            element
            (element, checkpoint)
            (element, checkpoint, idem_key)
      - SinkAdapter implements `send(element, idempotency_key=None)` and optional `flush()/close()`.
    """

    def __init__(
        self,
        registry: PluginRegistry,
        ctx: AdapterContext,
        *,
        hooks: WorkerHooksRunner | None = None,
        metrics: WorkerMetrics | None = None,
        queue_maxsize: int = 1024,
    ) -> None:
        self._registry = registry
        self._ctx = ctx
        self._hooks = hooks or WorkerHooksRunner(registry=registry)
        self._metrics = metrics or WorkerMetrics()

        self._sources: list[tuple[str, SourceAdapter, str | None]] = []  # (alias, adapter, checkpoint_key)
        self._ops = OpsChain(ctx)
        self._sinks = SinksRunner(ctx)

        self._merge_name: str = "interleave"
        self._queue: asyncio.Queue[tuple[StreamEl, Checkpoint | None, str | None, str | None]] = asyncio.Queue(
            maxsize=max(1, queue_maxsize)
        )
        self._tasks: list[asyncio.Task] = []
        self._running = False

        self._ckpt_store: CheckpointsStore | None = None

    async def build_from_plan(self, plan: Mapping[str, Any]) -> None:
        """
        Plan layout (normalized view):
        {
          "input": {
             "merge": {"name": "interleave"},
             "sources": [
               {
                 "alias": "s1",
                 "adapter": {"name": "pull.kafka.subscribe", "args": {...}, "checkpoint_key": "kafka:topic:p=0"},
                 "ops": [{"name": "map.json.loads", "args": {...}}, ...]
               }, ...
             ]
          },
          "output": {
             "channels": [{"name": "emit.kafka", "args": {...}}, ...],
             "delivery": {...},
             "dlq": {"name": "...", "args": {...}}
          }
        }
        """
        self._merge_name = (plan.get("input", {}).get("merge", {}) or {}).get("name", "interleave")

        # Sources
        self._sources.clear()
        for src in plan.get("input", {}).get("sources") or []:
            alias = src.get("alias") or src.get("name") or "source"
            ad = src.get("adapter") or {}
            name = ad.get("name")
            args = ad.get("args") or {}
            checkpoint_key = ad.get("checkpoint_key")
            sa = _create_source(self._registry, name, self._ctx, args)
            if hasattr(sa, "open"):
                maybe = sa.open(self._ctx, **args)  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe
            self._sources.append((alias, sa, checkpoint_key))

        # Transforms (ops chain)
        ops_union: list[Mapping[str, Any]] = []
        for src in plan.get("input", {}).get("sources") or []:
            for op in src.get("ops") or []:
                ops_union.append(op)
        await self._ops.build(ops_union, registry=self._registry)

        # Sinks
        await self._sinks.build(plan.get("output") or {}, registry=self._registry)

        # Checkpoints store (deferred injection from ctx.extra)
        ckpt_store = getattr(self._ctx, "checkpoints", None)
        if ckpt_store:
            self._ckpt_store = ckpt_store  # expected to be CheckpointsStore-compatible

    async def start(self) -> None:
        """Begin pulling from sources and streaming through transforms to sinks."""
        if self._running:
            return
        self._running = True
        # Producer tasks (one per source)
        for alias, adapter, checkpoint_key in self._sources:
            self._tasks.append(asyncio.create_task(self._run_source(alias, adapter, checkpoint_key)))
        # Consumer task (single sink writer)
        self._tasks.append(asyncio.create_task(self._run_sinks()))

    async def drain(self) -> None:
        """Flush sinks and gracefully stop sources (draining inflight)."""
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        await self._sinks.flush()
        await self._ops.close()
        for _, a, _ in self._sources:
            if hasattr(a, "close"):
                maybe = a.close()  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe

    async def stop(self) -> None:
        """Hard-close all components. Used on cancellation/hard deadline."""
        await self.drain()
        await self._sinks.close()

    # ---- internal ------------------------------------------------------------

    async def _run_source(self, alias: str, adapter: SourceAdapter, checkpoint_key: str | None) -> None:
        """
        Read from a source, pass through ops chain, and enqueue to the central queue.
        Keeps backpressure by awaiting `put` on the bounded queue.
        """
        # Try to read the last checkpoint
        ckpt: Checkpoint | None = None
        if checkpoint_key and self._ckpt_store:
            ckpt = await self._ckpt_store.read(checkpoint_key)

        # Call common entrypoint: adapter.stream(ctx, checkpoint=...)
        stream = _source_stream(adapter, self._ctx, ckpt)
        async for payload in stream:
            element, event_ckpt, idem_key = _unpack_source_payload(payload)
            self._metrics.items_in.inc()

            # Hook: source.item (best-effort)
            await self._hooks.emit_source_item(
                item=element, source_alias=alias, node_id=self._ctx.node_id, task_id=self._ctx.task_id
            )

            # Transform chain
            out = await self._ops.process(element)
            if out is None:
                continue

            # Fan-out or single
            if isinstance(out, (Item, Batch)):
                await self._queue.put((out, event_ckpt, idem_key, checkpoint_key))
                self._metrics.queue_depth.set(self._queue.qsize())
            else:
                for el in out:  # type: ignore[assignment]
                    await self._queue.put((el, event_ckpt, idem_key, checkpoint_key))
                    self._metrics.queue_depth.set(self._queue.qsize())

    async def _run_sinks(self) -> None:
        """
        Dequeue elements, push to sinks, commit checkpoints.
        Single consumer makes idempotent delivery and commit ordering trivial.
        """
        while True:
            el, ckpt, idem_key, ckpt_key = await self._queue.get()
            try:
                await self._sinks.deliver(el, idempotency_key=idem_key)
                self._metrics.items_out.inc()
                if isinstance(el, Batch):
                    self._metrics.batches_out.inc()

                # Commit checkpoint AFTER durable delivery
                if ckpt and ckpt_key and self._ckpt_store:
                    await self._ckpt_store.save(ckpt_key, ckpt)
            finally:
                self._queue.task_done()
                self._metrics.queue_depth.set(self._queue.qsize())

    # -------------------------------------------------------------------------


def _create_source(registry: PluginRegistry, name: str, ctx: AdapterContext, args: Mapping[str, Any]) -> SourceAdapter:
    if hasattr(registry, "create"):
        try:
            src = registry.create(kind="source", name=name, ctx=ctx, **args)
            return src  # type: ignore[return-value]
        except Exception:
            pass
    if hasattr(registry, "get_source"):
        factory = registry.get_source(name)  # type: ignore[attr-defined]
        return factory(ctx, **args)  # type: ignore[return-value]
    raise LookupError(f"source '{name}' is not registered")


def _source_stream(adapter: SourceAdapter, ctx: AdapterContext, ckpt: Checkpoint | None) -> AsyncIterator[Any]:
    """
    Resolve a common streaming interface across adapters:

      - adapter.stream(ctx, checkpoint=ckpt) → AsyncIterator
      - adapter.records(ctx, **opts)         → AsyncIterator
      - __aiter__                            → AsyncIterator
    """
    if hasattr(adapter, "stream"):
        return adapter.stream(ctx, checkpoint=ckpt)  # type: ignore[return-value, attr-defined]
    if hasattr(adapter, "records"):
        return adapter.records(ctx, checkpoint=ckpt)  # type: ignore[return-value, attr-defined]
    if hasattr(adapter, "__aiter__"):
        return adapter  # type: ignore[return-value]
    raise TypeError(f"source adapter {adapter!r} does not provide a streaming method")


def _unpack_source_payload(x: Any) -> tuple[StreamEl, Checkpoint | None, str | None]:
    """
    Normalize source output into (element, checkpoint, idempotency_key).
    Supported shapes:
      - element
      - (element, checkpoint)
      - (element, checkpoint, idem_key)
    """
    if isinstance(x, (Item, Batch)):
        return x, None, None
    if isinstance(x, (list, tuple)):
        if len(x) == 1:
            return x[0], None, None  # type: ignore[return-value]
        if len(x) == 2:
            return x[0], x[1], None  # type: ignore[return-value]
        if len(x) >= 3:
            return x[0], x[1], x[2]  # type: ignore[return-value]
    # As a last resort, wrap into Item-like payload
    raise TypeError("source yielded unsupported payload shape")
