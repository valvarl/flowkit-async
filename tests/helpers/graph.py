from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any

from flowkit.protocol.messages import RunState

from .util import dbg


def prime_graph(cd, graph: dict[str, Any]) -> dict[str, Any]:
    for n in graph.get("nodes", []):
        st = n.get("status")
        if st is None or (isinstance(st, str) and not st.strip()):
            n["status"] = getattr(cd, "RunState", type("RS", (), {"queued": "queued"})).queued
        n.setdefault("attempt_epoch", 0)
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph


async def wait_task_finished(db, task_id: str, timeout: float = 10.0) -> dict[str, Any]:  # noqa: ASYNC109
    t0 = time.time()
    last_log = 0.0
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        now = time.time()
        if now - last_log >= 0.5:
            if t:
                st = t.get("status")
                nodes = {n["node_id"]: n.get("status") for n in (t.get("graph", {}).get("nodes") or [])}
                dbg("WAIT.PROGRESS", task_status=st, nodes=nodes)
            else:
                dbg("WAIT.PROGRESS", info="task_not_found_yet")
            last_log = now
        if t and t.get("status") == RunState.finished:
            dbg("WAIT.DONE")
            return t
        await asyncio.sleep(0.05)
    raise AssertionError("task not finished in time")


async def wait_node_running(db, task_id: str, node_id: str, timeout: float = 8.0):  # noqa: ASYNC109
    """
    Poll the task document until a specific node reaches the 'running' state.
    Returns the latest task snapshot. Raises on timeout.
    """
    from time import time

    t0 = time()
    while time() - t0 < timeout:
        doc = await db.tasks.find_one({"id": task_id})
        if doc:
            st = (node_by_id(doc, node_id) or {}).get("status")
            if str(st).endswith("running"):
                return doc
        await asyncio.sleep(0.02)
    raise AssertionError(f"node {node_id} not running in time")


async def wait_node_not_running_for(db, task_id: str, node_id: str, hold: float = 0.6) -> None:
    """
    Ensure that a node does NOT enter 'running' within a small hold window.
    Useful for negative checks when downstream must wait for upstream completion.
    """
    from time import time

    t0 = time()
    seen_running = False
    while time() - t0 < hold:
        doc = await db.tasks.find_one({"id": task_id})
        n = node_by_id(doc or {}, node_id)
        st = n.get("status") if n else None
        if str(st).endswith("running"):
            seen_running = True
            break
        await asyncio.sleep(0.03)
    assert not seen_running, f"{node_id} unexpectedly started during hold window"


async def wait_node_finished(db, task_id: str, node_id: str, timeout: float = 8.0):  # noqa: ASYNC109
    """
    Poll the task document until a specific node reaches the 'finished' state.
    Returns the latest task snapshot. Raises on timeout.
    """
    from time import time

    t0 = time()
    last_doc = None
    while time() - t0 < timeout:
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
    Poll task doc until its status equals `want`, or raise if deadline exceeded.
    """
    from time import time

    t0 = time()
    while time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        if t and str(t.get("status")) == want:
            return t
        await asyncio.sleep(0.03)
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
