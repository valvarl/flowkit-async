from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_hash(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(data.encode("utf-8")).hexdigest()


def dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def jitter_ms(base_ms: int) -> int:
    import random

    delta = int(base_ms * 0.2)
    return max(0, base_ms + random.randint(-delta, +delta))
