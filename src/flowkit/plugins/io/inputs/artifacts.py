# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Artifacts input adapter.

This source is a thin facade intended to read upstream node outputs from
the execution storage (e.g., "artifacts" store). Because storage wiring
differs by deployment, the adapter tries two strategies:

1) If `ctx.externals["artifacts"]` provides a provider with a coroutine
   `iter_upstream(task_id: str, node_id: str, from_nodes: list[str], **kwargs)`
   yielding `Item`/`Batch` (or tuples with Checkpoint/idempotency_key),
   the adapter delegates to it.

2) Otherwise, it raises a clear runtime error telling the user to supply
   an artifacts provider via externals or to replace the adapter.

This keeps the source self-contained for now, while allowing real storage
integration in your environment without baking hard dependencies here.
"""

from collections.abc import AsyncIterator, Mapping
from typing import Any

from ....api.adapters import AdapterContext, SourceAdapter
from ....api.streams import Batch, Checkpoint, Item


class ArtifactsSource(SourceAdapter):
    """
    A minimal "upstream artifacts" source delegating to an external provider.

    Args:
        from_nodes: list of parent node_ids to read from.
        select: "batches" or "final" (forwarded to the provider).
        poll_ms: optional poll interval for streaming reads.
        eof_on_task_done: optional flag forwarded to provider.
        backoff_max_ms: optional, forwarded to provider.

    The exact semantics depend on your artifacts provider. See the module
    docstring for the expected provider interface.
    """

    name = "pull.from_artifacts"
    # Emits items or framed batches depending on provider; we declare no strict content caps.
    capabilities = None  # protocol allows absence in practice; registry may ignore

    def __init__(self) -> None:
        self._ctx: AdapterContext | None = None
        self._args: Mapping[str, Any] = {}
        self._provider: Any = None

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        self._ctx = ctx
        self._args = spec or {}
        self._provider = (ctx.externals or {}).get("artifacts")

    # Engine will call stream(ctx, checkpoint=...) if available.
    def stream(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None = None) -> AsyncIterator[Any]:
        return self._stream_impl(ctx, checkpoint=checkpoint)

    async def ack(self, checkpoint: Checkpoint | None) -> None:  # pragma: no cover - not used by engine
        return

    async def nack(self, error: BaseException | None = None) -> None:  # pragma: no cover - not used by engine
        return

    async def close(self) -> None:
        self._provider = None
        self._ctx = None

    # ---- internals -----------------------------------------------------------

    async def _no_provider(self) -> AsyncIterator[Item | Batch]:
        raise RuntimeError(
            "ArtifactsSource requires an artifacts provider exposed via ctx.externals['artifacts'] "
            "with a coroutine 'iter_upstream(...)'. Provide one in your environment or replace this adapter."
        )
        yield  # pragma: no cover

    async def _stream_impl(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None) -> AsyncIterator[Any]:
        if not self._provider or not hasattr(self._provider, "iter_upstream"):
            async for _ in self._no_provider():
                yield _
            return

        # Extract common args
        from_nodes = self._args.get("from_nodes") or []
        select = self._args.get("select", "batches")
        poll_ms = int(self._args.get("poll_ms", 200))
        eof_on_task_done = bool(self._args.get("eof_on_task_done", False))
        backoff_max_ms = int(self._args.get("backoff_max_ms", 5000))

        agen = self._provider.iter_upstream(
            task_id=ctx.task_id,
            node_id=ctx.node_id,
            from_nodes=from_nodes,
            select=select,
            checkpoint=checkpoint,
            poll_ms=poll_ms,
            eof_on_task_done=eof_on_task_done,
            backoff_max_ms=backoff_max_ms,
        )

        async for el in agen:
            # Provider may already yield (Item|Batch) or tuples
            yield el
