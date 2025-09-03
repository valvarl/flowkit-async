from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from pydantic import BaseModel, Field


class Batch(BaseModel):
    batch_uid: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class BatchResult(BaseModel):
    success: bool
    metrics: dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: dict[str, Any] | None = None
    reason_code: str | None = None
    permanent: bool = False
    error: str | None = None


class FinalizeResult(BaseModel):
    metrics: dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: dict[str, Any] | None = None


class RoleHandler:
    role: str

    async def init(self, cfg: dict[str, Any]) -> None:
        pass

    async def load_input(self, input_ref: dict[str, Any] | None, input_inline: dict[str, Any] | None) -> Any:
        return {"input_ref": input_ref or {}, "input_inline": input_inline or {}}

    async def iter_batches(self, loaded: Any) -> AsyncIterator[Batch]:
        yield Batch(batch_uid=None, payload=loaded or {})

    async def process_batch(self, batch: Batch, ctx) -> BatchResult:
        await asyncio.sleep(0)
        return BatchResult(success=True, metrics={"processed": 1})

    async def finalize(self, ctx) -> FinalizeResult | None:
        return FinalizeResult(metrics={})

    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        return ("unexpected_error", False)
