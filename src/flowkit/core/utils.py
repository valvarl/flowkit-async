from __future__ import annotations

import hashlib
import json
from secrets import randbelow
from typing import Any


def stable_hash(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.blake2b(data, digest_size=20).hexdigest()


def dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def jitter_ms(base_ms: int) -> int:
    delta = max(0, int(base_ms * 0.2))
    if delta == 0:
        return max(0, base_ms)
    # rand in [-delta, +delta]
    return max(0, base_ms + (randbelow(2 * delta + 1) - delta))
