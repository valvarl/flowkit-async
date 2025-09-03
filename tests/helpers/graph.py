from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flowkit.protocol.messages import RunState

from .util import dbg


def prime_graph(cd, graph: Dict[str, Any]) -> Dict[str, Any]:
    for n in graph.get("nodes", []):
        st = n.get("status")
        if st is None or (isinstance(st, str) and not st.strip()):
            n["status"] = getattr(cd, "RunState", type("RS", (), {"queued":"queued"})).queued
        n.setdefault("attempt_epoch", 0)
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph

async def wait_task_finished(db, task_id: str, timeout: float = 10.0) -> Dict[str, Any]:
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

async def wait_node_running(db, task_id: str, node_id: str, timeout: float = 8.0):
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

def node_by_id(doc: Dict[str, Any] | None, node_id: str) -> Dict[str, Any]:
    """
    Return node document from task snapshot by node_id, or {} if not found.
    """
    if not doc:
        return {}
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n
    return {}

async def wait_task_status(db, task_id: str, want: str, timeout: float = 6.0):
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
    nodes: List[Dict[str, Any]], 
    edges: Iterable[Tuple[str, str]] | List[List[str]] = (),
    agg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
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
        graph["nodes"].append({
            "node_id": name,
            "type": "coordinator_fn",
            "depends_on": [agg["after"]],
            "fan_in": "all",
            "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": agg["after"], "mode": agg.get("mode", "sum")}},
            "status": None,
            "attempt_epoch": 0,
        })
        graph["edges"].append([agg["after"], name])
    return graph
