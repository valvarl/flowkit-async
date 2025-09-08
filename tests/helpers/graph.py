from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any

from flowkit.protocol.messages import RunState

from .util import dbg


def prime_graph(cd, graph: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize graph nodes before submitting to the coordinator:
    - ensure status is set to queued (supports both enum and raw string)
    - ensure attempt_epoch/stats/lease are present
    """
    for n in graph.get("nodes", []):
        st = n.get("status")
        if st is None or (isinstance(st, str) and not st.strip()):
            # Support tests where RunState may be an enum on `cd`, or just strings.
            n["status"] = getattr(cd, "RunState", type("RS", (), {"queued": "queued"})).queued
        n.setdefault("attempt_epoch", 0)
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph


async def wait_task_finished(db, task_id: str, timeout: float = 10.0) -> dict[str, Any]:  # noqa: ASYNC109
    """
    Wait until the whole task reaches RunState.finished.
    Logs periodic progress for debugging.
    """
    t0 = time.monotonic()
    last_log = 0.0
    while time.monotonic() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        now = time.monotonic()
        if now - last_log >= 0.5:
            if t:
                st = t.get("status")
                nodes = {n["node_id"]: n.get("status") for n in (t.get("graph", {}).get("nodes") or [])}
                dbg("WAIT.PROGRESS", task_status=st, nodes=nodes)
            else:
                dbg("WAIT.PROGRESS", info="task_not_found_yet")
            last_log = now
        if t and (t.get("status") == RunState.finished or str(t.get("status", "")).endswith("finished")):
            dbg("WAIT.DONE")
            return t
        await asyncio.sleep(0.03)
    raise AssertionError("task not finished in time")


async def wait_node_running(db, task_id: str, node_id: str, timeout: float = 8.0):  # noqa: ASYNC109
    """
    Wait until a node has *actually* started:
      - `started_at` is present OR
      - `attempt_epoch` > 0 OR
      - status is 'running' OR the node already finished (fast path).

    This avoids flakes when nodes run and finish between polling intervals.
    """
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        doc = await db.tasks.find_one({"id": task_id})
        if doc:
            nodes = (doc.get("graph", {}) or {}).get("nodes") or []
            n = next((x for x in nodes if x.get("node_id") == node_id), None)
            if n:
                st = str(n.get("status") or "")
                started = bool(n.get("started_at")) or int(n.get("attempt_epoch") or 0) > 0
                if started or st.endswith("running") or st.endswith("finished"):
                    return doc
        await asyncio.sleep(0.01)  # tighter polling to catch short running windows
    raise AssertionError(f"node {node_id} not running in time")


async def wait_node_not_running_for(db, task_id: str, node_id: str, hold: float = 0.6) -> None:
    """
    Ensure that a node does NOT start within a small hold window.

    We treat the node as "started" if *any* of these is true:
      - status is 'running' or 'finished'
      - `started_at` is present
      - `attempt_epoch` > 0
    """
    t0 = time.monotonic()
    seen_started = False
    while time.monotonic() - t0 < hold:
        doc = await db.tasks.find_one({"id": task_id})
        n = node_by_id(doc or {}, node_id)
        if n:
            st = str(n.get("status") or "")
            started = (
                st.endswith("running")
                or st.endswith("finished")
                or bool(n.get("started_at"))
                or int(n.get("attempt_epoch") or 0) > 0
            )
            if started:
                seen_started = True
                break
        await asyncio.sleep(0.02)
    assert not seen_started, f"{node_id} unexpectedly started during hold window"


async def wait_node_finished(db, task_id: str, node_id: str, timeout: float = 8.0):  # noqa: ASYNC109
    """
    Wait until a specific node reaches the 'finished' state.
    Returns the latest task snapshot. Raises on timeout (with last observed status).
    """
    t0 = time.monotonic()
    last_doc = None
    while time.monotonic() - t0 < timeout:
        doc = await db.tasks.find_one({"id": task_id})
        if doc:
            last_doc = doc
            st = (node_by_id(doc, node_id) or {}).get("status")
            if str(st).endswith("finished"):
                return doc
        await asyncio.sleep(0.02)
    last_status = (node_by_id(last_doc or {}, node_id) or {}).get("status") if last_doc else None
    raise AssertionError(f"node {node_id} not finished in time (last_status={last_status})")


def node_by_id(doc: dict[str, Any] | None, node_id: str) -> dict[str, Any]:
    """
    Return node document from task snapshot by node_id, or {} if not found.
    """
    if not doc:
        return {}
    for n in doc.get("graph", {}).get("nodes") or []:
        if n.get("node_id") == node_id:
            return n
    return {}


async def wait_task_status(db, task_id: str, want: str, timeout: float = 6.0):  # noqa: ASYNC109
    """
    Wait until task status equals `want` (string-endswith compatible).
    """
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        if t:
            st = str(t.get("status") or "")
            if st == want or st.endswith(want):
                return t
        await asyncio.sleep(0.02)
    raise AssertionError(f"task not reached status={want} in time")


def make_graph(
    *,
    nodes: list[dict[str, Any]],
    edges: Iterable[tuple[str, str]] | list[list[str]] = (),
    agg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build a graph document with schema_version and optional aggregation node.

    Example:
      g = make_graph(
        nodes=[
          {"node_id":"s","type":"source","depends_on":[],"fan_in":"all","io":{"input_inline":{}}},
        ],
        edges=[],
        agg={"after": "s", "node_id": "agg", "mode": "sum"}  # optional
      )
    """
    graph = {"schema_version": "1.0", "nodes": list(nodes), "edges": [list(e) for e in edges]}
    if agg:
        name = agg.get("node_id") or f"agg_{agg['after']}"
        graph["nodes"].append(
            {
                "node_id": name,
                "type": "coordinator_fn",
                "depends_on": [agg["after"]],
                "fan_in": "all",
                "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": agg["after"], "mode": agg.get("mode", "sum")}},
                "status": None,
                "attempt_epoch": 0,
            }
        )
        graph["edges"].append([agg["after"], name])
    return graph
