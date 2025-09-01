import asyncio
from typing import Any, Dict, Optional


def _base_role(wu):
    try:
        return wu.RoleHandler
    except Exception:
        from flowkit.worker.handlers.base import RoleHandler as Base
        return Base

def build_sleepy_handler(wu, role: str, *, batches=1, sleep_s=1.2):
    Base = _base_role(wu)
    class Sleepy(Base):
        def __init__(self, role): self.role = role
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            for i in range(batches):
                yield wu.Batch(batch_uid=f"{self.role}-{i}", payload={"i": i})
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(float(sleep_s))
            return wu.BatchResult(success=True, metrics={"count": 1})
        async def finalize(self, ctx):
            return wu.FinalizeResult(metrics={})
    return Sleepy(role=role)

def build_noop_query_only_role(wu, role: str):
    Base = _base_role(wu)
    class Noop(Base):
        def __init__(self, role): self.role = role
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(batch_uid=f"{self.role}-0", payload={})
        async def process_batch(self, batch, ctx):
            return wu.BatchResult(success=True, metrics={"noop": 1})
    return Noop(role=role)

def build_flaky_once_handler(wu, role: str = "flaky"):
    Base = _base_role(wu)
    class Flaky(Base):
        def __init__(self, role):
            self.role = role
            self.failed_once = False
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(batch_uid=f"{self.role}-0", payload={})
        async def process_batch(self, batch, ctx):
            if not self.failed_once:
                self.failed_once = True
                return wu.BatchResult(success=False, reason_code="transient", permanent=False, error="boom")
            return wu.BatchResult(success=True, metrics={"count": 1})
        async def finalize(self, ctx):
            return wu.FinalizeResult(metrics={})
    return Flaky(role=role)

def build_counting_source_handler(wu, *, total=9, batch=3):
    Base = _base_role(wu)
    class Source(Base):
        role = "source"
        async def load_input(self, ref, inline): return {"total": total, "batch": batch}
        async def iter_batches(self, loaded):
            t, b = loaded["total"], loaded["batch"]
            shard = 0
            for i in range(0, t, b):
                yield wu.Batch(batch_uid=f"s-{shard}", payload={"items": list(range(i, min(i+b, t)))})
                shard += 1
        async def process_batch(self, batch, ctx):
            n = len(batch.payload.get("items") or [])
            return wu.BatchResult(success=True, metrics={"count": n})
        async def finalize(self, ctx):
            return wu.FinalizeResult(metrics={})
    return Source()

def build_permanent_fail_handler(wu, role_name: str = "a"):
    Base = _base_role(wu)
    class AFail(Base):
        role = role_name
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(batch_uid=f"{role_name}-0", payload={"x": 1})
        async def process_batch(self, batch, ctx):
            raise RuntimeError("hard_fail")
        def classify_error(self, exc: BaseException) -> tuple[str, bool]:
            return ("hard", True)
    return AFail()

def build_noop_handler(wu, role_name: str):
    Base = _base_role(wu)
    class Noop(Base):
        role = role_name
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(batch_uid=f"{role_name}-0", payload={})
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(0.01)
            return wu.BatchResult(success=True, metrics={"count": 1})
    return Noop()

def build_slow_source_handler(wu, *, total=50, batch=5, delay=0.15):
    Base = _base_role(wu)
    class SlowSource(Base):
        role = "source"
        async def load_input(self, ref, inline): return {"total": total, "batch": batch, "delay": delay}
        async def iter_batches(self, loaded):
            t, b = loaded["total"], loaded["batch"]
            shard = 0
            for i in range(0, t, b):
                yield wu.Batch(batch_uid=f"s-{shard}", payload={"items": list(range(i, min(i+b, t))), "delay": loaded["delay"]})
                shard += 1
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(batch.payload.get("delay", 0.1))
            return wu.BatchResult(success=True, metrics={"count": len(batch.payload.get("items") or [])})
    return SlowSource()

def build_cancelable_source_handler(wu, *, total=100, batch=10, delay=0.3):
    Base = _base_role(wu)
    class Cancellable(Base):
        role = "source"
        async def load_input(self, ref, inline): return {"total": total, "batch": batch}
        async def iter_batches(self, loaded):
            t, b = loaded["total"], loaded["batch"]
            shard = 0
            for i in range(0, t, b):
                yield wu.Batch(batch_uid=f"s-{shard}", payload={"i": i})
                shard += 1
        async def process_batch(self, batch, ctx):
            for _ in range(int(delay / 0.05)):
                if ctx.cancelled():
                    return wu.BatchResult(success=False, reason_code="cancelled", permanent=False)
                await asyncio.sleep(0.05)
            return wu.BatchResult(success=True, metrics={"count": 1})
    return Cancellable()
