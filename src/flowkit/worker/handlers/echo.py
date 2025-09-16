from __future__ import annotations

import asyncio
import logging
from functools import lru_cache

from ...core.log import get_logger, swallow
from .base import Batch, BatchResult, RoleHandler


@lru_cache(maxsize=1)
def _log():
    return get_logger("worker.handler.echo")


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
                with swallow(
                    logger=_log(),
                    code="echo.upsert_partial",
                    msg="artifacts upsert_partial failed",
                    level=logging.DEBUG,
                    expected=True,
                ):
                    await ctx.artifacts.upsert_partial(batch.batch_uid, {"echoed": i})
        return BatchResult(success=True, metrics={"echoed": steps})
