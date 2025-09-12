from __future__ import annotations

from typing import Any

BAD_KEYS = [
    "",  # empty
    "$bad",  # segment starts with $
    "a..b",  # empty segment
    "bad\0seg",  # NUL in segment
    "a." + ("s" * 129),  # segment longer than limit
]


def too_long_key(limit: int, prefix_len: int) -> str:
    """
    Construct a key whose total path length (including 'coordinator.vars.' prefix)
    exceeds the adapter limit by exactly 1 char.
    """
    return "k" * (limit - prefix_len + 1)


def make_deep_obj(depth_segments: int) -> dict[str, Any]:
    """
    Build nested dict structure of a given depth, like {"seg": {"seg": {...}}}.
    """
    root: dict[str, Any] = {}
    cur = root
    for _ in range(depth_segments):
        nxt: dict[str, Any] = {}
        cur["seg"] = nxt
        cur = nxt
    return root


async def mk_task(db, tid: str) -> str:
    """
    Insert a minimal task document for unit tests and return its id.
    """
    await db.tasks.insert_one(
        {"id": tid, "pipeline_id": tid, "status": "queued", "graph": {"nodes": [], "edges": [], "edges_ex": []}}
    )
    return tid


def get_record_by_event(caplog, event_name: str):
    """
    Return the first log record whose 'event' attribute equals event_name.
    Raise StopIteration if not found to make failures explicit.
    """
    return next(r for r in caplog.records if getattr(r, "event", "") == event_name)
