import hashlib
import json
from datetime import UTC, datetime
from typing import Any


def _ts() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]


def dbg(tag: str, **kv):
    kvs = " ".join(f"{k}={kv[k]!r}" for k in kv)
    print(f"[{_ts()}] {tag}: {kvs}", flush=True)


def stable_hash(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(data.encode("utf-8")).hexdigest()
