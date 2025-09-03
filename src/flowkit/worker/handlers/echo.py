from __future__ import annotations

import asyncio

from .base import Batch, BatchResult, RoleHandler


class EchoHandler(RoleHandler):
    role = "echo"

    async def process_batch(self, batch: Batch, ctx) -> BatchResult:
        # Cooperative cancellation + periodic checkpoints as example
        await ctx.raise_if_cancelled()
        steps = 5
        for i in range(steps):
            await ctx.raise_if_cancelled()
            await ctx.cancellable(asyncio.sleep(0.05))
            if i and i % 2 == 0 and batch.batch_uid:
                try:
                    await ctx.artifacts.upsert_partial(batch.batch_uid, {"echoed": i})
                except Exception:
                    pass
        return BatchResult(success=True, metrics={"echoed": steps})
