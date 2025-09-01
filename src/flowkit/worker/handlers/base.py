from __future__ import annotations
import asyncio
from typing import Any, AsyncIterator, Dict, Optional
from pydantic import BaseModel, Field


class Batch(BaseModel):
    batch_uid: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class BatchResult(BaseModel):
    success: bool
    metrics: Dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None
    reason_code: Optional[str] = None
    permanent: bool = False
    error: Optional[str] = None


class FinalizeResult(BaseModel):
    metrics: Dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None


class RoleHandler:
    role: str

    async def init(self, cfg: Dict[str, Any]) -> None:
        pass

    async def load_input(self, input_ref: Optional[Dict[str, Any]], input_inline: Optional[Dict[str, Any]]) -> Any:
        return {"input_ref": input_ref or {}, "input_inline": input_inline or {}}

    async def iter_batches(self, loaded: Any) -> AsyncIterator[Batch]:
        yield Batch(batch_uid=None, payload=loaded or {})

    async def process_batch(self, batch: Batch, ctx) -> BatchResult:
        await asyncio.sleep(0)
        return BatchResult(success=True, metrics={"processed": 1})

    async def finalize(self, ctx) -> Optional[FinalizeResult]:
        return FinalizeResult(metrics={})

    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        return ("unexpected_error", False)
