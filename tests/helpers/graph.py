import asyncio
import time
from typing import Any, Dict

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
        if t and str(t.get("status")) == "finished":
            dbg("WAIT.DONE")
            return t
        await asyncio.sleep(0.05)
    raise AssertionError("task not finished in time")
