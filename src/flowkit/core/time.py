from __future__ import annotations
import time
from datetime import datetime, timezone

def now_dt() -> datetime:
    return datetime.now(timezone.utc)

def now_ms() -> int:
    return time.time_ns() // 1_000_000

def mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000
