import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .util import dbg


def _now_dt(): return datetime.now(timezone.utc)

class InMemCollection:
    def __init__(self, name: str):
        self.name = name
        self.rows: List[Dict[str, Any]] = []

    @staticmethod
    def _get_path(doc, path):
        cur = doc
        for p in path.split("."):
            if isinstance(cur, list) and p.isdigit():
                idx = int(p)
                if idx >= len(cur): return None
                cur = cur[idx]
            elif isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
            if cur is None:
                return None
        return cur

    def _match(self, doc, flt):
        from enum import Enum
        def _norm(x): return x.value if isinstance(x, Enum) else x

        for k, v in (flt or {}).items():
            if k == "graph.nodes" and isinstance(v, dict) and "$elemMatch" in v:
                cond = v["$elemMatch"] or {}
                nodes = (((doc.get("graph") or {}).get("nodes")) or [])
                node_ok = False
                for n in nodes:
                    ok = True
                    for ck, cv in cond.items():
                        val = self._get_path(n, ck)
                        val = _norm(val)
                        if isinstance(cv, dict):
                            if "$in" in cv:
                                in_list = [_norm(x) for x in cv["$in"]]
                                if val not in in_list: ok = False; break
                            if "$ne" in cv:
                                if val == _norm(cv["$ne"]): ok = False; break
                            if "$lte" in cv:
                                if not (val is not None and val <= _norm(cv["$lte"])): ok = False; break
                            if "$lt" in cv:
                                if not (val is not None and val < _norm(cv["$lt"])): ok = False; break
                            if "$gte" in cv:
                                if not (val is not None and val >= _norm(cv["$gte"])): ok = False; break
                            if "$gt" in cv:
                                if not (val is not None and val > _norm(cv["$gt"])): ok = False; break
                        else:
                            if _norm(cv) != val: ok = False; break
                    if ok:
                        node_ok = True
                        break
                if not node_ok:
                    return False
                continue

            val = self._get_path(doc, k)
            val = _norm(val)

            if isinstance(v, dict):
                if "$in" in v:
                    in_list = [_norm(x) for x in v["$in"]]
                    if val not in in_list: return False
                if "$ne" in v:
                    if val == _norm(v["$ne"]): return False
                if "$lte" in v:
                    if not (val is not None and val <= _norm(v["$lte"])): return False
                if "$lt" in v:
                    if not (val is not None and val < _norm(v["$lt"])): return False
                if "$gte" in v:
                    if not (val is not None and val >= _norm(v["$gte"])): return False
                if "$gt" in v:
                    if not (val is not None and val > _norm(v["$gt"])): return False
                continue

            if k == "graph.nodes.node_id":
                nodes = (((doc.get("graph") or {}).get("nodes")) or [])
                if not any((n.get("node_id") == v) for n in nodes):
                    return False
                continue

            if _norm(v) != val:
                return False

        return True

    async def insert_one(self, doc):
        if self.name == "stream_progress":
            tk = (doc.get("task_id"), doc.get("consumer_node"), doc.get("from_node"), doc.get("batch_uid"))
            for r in self.rows:
                if (r.get("task_id"), r.get("consumer_node"), r.get("from_node"), r.get("batch_uid")) == tk:
                    raise Exception("duplicate key: stream_progress")
        elif self.name == "metrics_raw":
            tk = (doc.get("task_id"), doc.get("node_id"), doc.get("batch_uid"))
            for r in self.rows:
                if (r.get("task_id"), r.get("node_id"), r.get("batch_uid")) == tk:
                    raise Exception("duplicate key: metrics_raw")

        self.rows.append(dict(doc))
        if self.name == "outbox":
            dbg("DB.OUTBOX.INSERT", size=len(self.rows), doc_keys=list(doc.keys()))

    async def find_one(self, flt, proj=None):
        for d in self.rows:
            if self._match(d, flt): return d
        return None

    async def count_documents(self, flt):
        return sum(1 for d in self.rows if self._match(d, flt))

    def find(self, flt, proj=None):
        rows = [d for d in self.rows if self._match(d, flt)]
        class _Cur:
            def __init__(self, rows): self._rows = rows
            def sort(self, *_): return self
            def limit(self, n): self._rows = self._rows[:n]; return self
            async def __aiter__(self):
                for r in list(self._rows): yield r
        return _Cur(rows)

    async def update_one(self, flt, upd, upsert=False):
        doc = None
        for d in self.rows:
            if self._match(d, flt): doc = d; break

        created = False
        if not doc:
            if not upsert: return
            doc = {}
            self.rows.append(doc)
            created = True

        node_idx = None
        if "graph.nodes.node_id" in flt:
            target = flt["graph.nodes.node_id"]
            nodes = (((doc.get("graph") or {}).get("nodes")) or [])
            for i, n in enumerate(nodes):
                if n.get("node_id") == target:
                    node_idx = i
                    break

        def set_path(m, path, val):
            parts = path.split(".")
            cur = m
            for i, p in enumerate(parts):
                if p == "$": p = str(node_idx)
                last = i == len(parts) - 1
                if p.isdigit() and isinstance(cur, list):
                    idx = int(p)
                    while len(cur) <= idx: cur.append({})
                    if last: cur[idx] = val
                    else:
                        if not isinstance(cur[idx], dict): cur[idx] = {}
                        cur = cur[idx]
                else:
                    if last: cur[p] = val
                    else:
                        if p not in cur or not isinstance(cur[p], (dict, list)):
                            cur[p] = [] if parts[i + 1].isdigit() else {}
                        cur = cur[p]

        if "$setOnInsert" in upd and created:
            for k, v in upd["$setOnInsert"].items():
                set_path(doc, k, v)
            if self.name == "artifacts" and "status" in upd["$setOnInsert"]:
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$setOnInsert"]["status"]))

        if "$set" in upd:
            if self.name == "tasks":
                for k, v in upd["$set"].items():
                    if k.endswith("graph.nodes.$.status"):
                        dbg("DB.TASK.STATUS", filter=flt, new_status=str(v))
            if self.name == "artifacts" and "status" in upd["$set"]:
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$set"]["status"]))
            for k, v in upd["$set"].items():
                set_path(doc, k, v)

        if "$inc" in upd:
            for k, v in upd["$inc"].items():
                parts = k.split(".")
                cur = doc
                for i, p in enumerate(parts):
                    if p == "$": p = str(node_idx)
                    last = (i == len(parts) - 1)

                    if isinstance(cur, list) and p.isdigit():
                        idx = int(p)
                        while len(cur) <= idx: cur.append({})
                        if last:
                            cur[idx] = int((cur[idx] or 0)) + int(v) if isinstance(cur[idx], (int, float)) else int(v)
                        else:
                            if not isinstance(cur[idx], (dict, list)):
                                cur[idx] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[idx]
                    else:
                        if last:
                            cur[p] = int((cur.get(p, 0) or 0)) + int(v)
                        else:
                            if p not in cur or not isinstance(cur[p], (dict, list)):
                                cur[p] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[p]

        if "$max" in upd:
            for k, v in upd["$max"].items():
                parts = k.split(".")
                cur = doc
                for i, p in enumerate(parts):
                    if p == "$": p = str(node_idx)
                    last = (i == len(parts) - 1)

                    if isinstance(cur, list) and p.isdigit():
                        idx = int(p)
                        while len(cur) <= idx: cur.append({})
                        if last:
                            cur[idx] = max(int(cur[idx] or 0), int(v)) if isinstance(cur[idx], (int, float)) else int(v)
                        else:
                            if not isinstance(cur[idx], (dict, list)):
                                cur[idx] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[idx]
                    else:
                        if last:
                            cur[p] = max(int((cur.get(p) or 0)), int(v))
                        else:
                            if p not in cur or not isinstance(cur[p], (dict, list)):
                                cur[p] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[p]

        if "$currentDate" in upd:
            for k, _ in upd["$currentDate"].items():
                set_path(doc, k, _now_dt())

    async def find_one_and_update(self, flt, upd):
        doc = await self.find_one(flt)
        await self.update_one(flt, upd)
        return doc

    async def create_index(self, *a, **k): return "ok"

class InMemDB:
    """
    Dynamic collections map. No predefined attributes.
    Access via attribute (db.tasks) or db.collection('tasks').
    """
    def __init__(self):
        self._collections: Dict[str, InMemCollection] = {}

    def collection(self, name: str) -> InMemCollection:
        return self._collections.setdefault(name, InMemCollection(name))

    def __getattr__(self, name: str) -> InMemCollection:
        # Lazily create any requested collection
        return self.collection(name)

    async def create_index(self, collection: str, *args, **kwargs):
        # Forward to collection-level create_index
        return await self.collection(collection).create_index(*args, **kwargs)
