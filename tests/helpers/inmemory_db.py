from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from .util import dbg


def _now_dt():
    return datetime.now(UTC)


def _norm_enum(v):
    try:
        from enum import Enum as _E

        if isinstance(v, _E):
            return v.value
    except Exception:
        pass
    return v


class _Index:
    """Minimal Mongo-like index descriptor."""

    def __init__(
        self,
        keys: list[tuple[str, int]],
        *,
        unique: bool = False,
        sparse: bool = False,
        name: str | None = None,
        expireAfterSeconds: int | None = None,
    ) -> None:
        self.keys = list(keys)
        self.unique = bool(unique)
        self.sparse = bool(sparse)
        self.name = name
        self.expireAfterSeconds = expireAfterSeconds  # stored only

    def signature(self) -> tuple:
        """Used to de-dupe identical create_index calls."""
        return (
            tuple(self.keys),
            self.unique,
            self.sparse,
            self.expireAfterSeconds,
        )

    def extract_tuple(self, doc: dict[str, Any], getter) -> tuple | None:
        """
        Build an index tuple from doc. If sparse and any field is missing, return None.
        """
        vals = []
        for path, _order in self.keys:
            v = getter(doc, path)
            v = _norm_enum(v)
            if v is None and self.sparse:
                return None  # not indexed
            vals.append(v)
        return tuple(vals)


class InMemCollection:
    def __init__(self, name: str):
        self.name = name
        self.rows: list[dict[str, Any]] = []
        self._indexes: list[_Index] = []
        # Built-in defaults (to mirror prod behavior even if create_index is not called)
        if self.name == "stream_progress":
            self._indexes.append(
                _Index(
                    [("task_id", 1), ("consumer_node", 1), ("from_node", 1), ("batch_uid", 1)],
                    unique=True,
                    name="uniq_stream_progress",
                )
            )

    # ───────────────────────── Path utils ─────────────────────────

    @staticmethod
    def _get_path(doc, path: str):
        cur = doc
        for p in path.split("."):
            if isinstance(cur, list) and p.isdigit():
                idx = int(p)
                if idx >= len(cur):
                    return None
                cur = cur[idx]
            elif isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
            if cur is None:
                return None
        return cur

    @staticmethod
    def _has_path(doc, path: str) -> bool:
        cur = doc
        for p in path.split("."):
            if isinstance(cur, list) and p.isdigit():
                idx = int(p)
                if idx >= len(cur):
                    return False
                cur = cur[idx]
            elif isinstance(cur, dict):
                if p not in cur:
                    return False
                cur = cur[p]
            else:
                return False
        return True

    # ───────────────────────── Query match ─────────────────────────


    def _match(self, doc, flt):
        from enum import Enum

        def _norm(x):
            return x.value if isinstance(x, Enum) else x

        for k, v in (flt or {}).items():
            # $elemMatch for graph.nodes
            if k == "graph.nodes" and isinstance(v, dict) and "$elemMatch" in v:
                cond = v["$elemMatch"] or {}
                nodes = ((doc.get("graph") or {}).get("nodes")) or []
                node_ok = False
                for n in nodes:
                    ok = True
                    for ck, cv in cond.items():
                        val = self._get_path(n, ck)
                        val = _norm(val)
                        if isinstance(cv, dict):
                            if "$in" in cv:
                                in_list = [_norm(x) for x in cv["$in"]]
                                if val not in in_list:
                                    ok = False
                                    break
                            if "$ne" in cv:
                                if val == _norm(cv["$ne"]):
                                    ok = False
                                    break
                            if "$lte" in cv:
                                if not (val is not None and val <= _norm(cv["$lte"])):
                                    ok = False
                                    break
                            if "$lt" in cv:
                                if not (val is not None and val < _norm(cv["$lt"])):
                                    ok = False
                                    break
                            if "$gte" in cv:
                                if not (val is not None and val >= _norm(cv["$gte"])):
                                    ok = False
                                    break
                            if "$gt" in cv:
                                if not (val is not None and val > _norm(cv["$gt"])):
                                    ok = False
                                    break
                        else:
                            if _norm(cv) != val:
                                ok = False
                                break
                    if ok:
                        node_ok = True
                        break
                if not node_ok:
                    return False
                continue

            val = self._get_path(doc, k)
            val = _norm(val)

            if isinstance(v, dict):
                if "$exists" in v:
                    want = bool(v["$exists"])
                    present = self._has_path(doc, k)
                    if present != want:
                        return False
                if "$in" in v:
                    in_list = [_norm(x) for x in v["$in"]]
                    if val not in in_list:
                        return False
                if "$ne" in v:
                    if val == _norm(v["$ne"]):
                        return False
                if "$lte" in v:
                    if not (val is not None and val <= _norm(v["$lte"])):
                        return False
                if "$lt" in v:
                    if not (val is not None and val < _norm(v["$lt"])):
                        return False
                if "$gte" in v:
                    if not (val is not None and val >= _norm(v["$gte"])):
                        return False
                if "$gt" in v:
                    if not (val is not None and val > _norm(v["$gt"])):
                        return False
                continue

            if k == "graph.nodes.node_id":
                nodes = ((doc.get("graph") or {}).get("nodes")) or []
                if not any((n.get("node_id") == v) for n in nodes):
                    return False
                continue

            if v != val:
                return False

        return True

    # ───────────────────────── Index helpers ─────────────────────────

    def _ensure_unique_ok(self, candidate: dict[str, Any], *, ignore_doc: dict[str, Any] | None = None) -> None:
        """Validate all unique indexes for a candidate doc."""
        for idx in self._indexes:
            if not idx.unique:
                continue
            tup = idx.extract_tuple(candidate, self._get_path)
            if tup is None:  # sparse doc not indexed
                continue
            # Compare with every other doc's index tuple
            for other in self.rows:
                if other is ignore_doc:
                    continue
                other_tup = idx.extract_tuple(other, self._get_path)
                if other_tup is None:
                    continue
                if other_tup == tup:
                    # Mirror Mongo-ish error signal
                    raise Exception(f"duplicate key: {self.name}.{idx.name or 'unique'} -> {tup}")

    def _apply_update_doc(self, base: dict[str, Any], flt: dict, upd: dict, created: bool) -> dict[str, Any]:
        """Apply update operators to a copy and return it."""
        from enum import Enum

        def _norm(x):
            return x.value if isinstance(x, Enum) else x

        # resolve positional '$' for 'graph.nodes'
        node_idx = None
        if "graph.nodes.node_id" in flt:
            target = flt["graph.nodes.node_id"]
            nodes = ((base.get("graph") or {}).get("nodes")) or []
            for i, n in enumerate(nodes):
                if n.get("node_id") == target:
                    node_idx = i
                    break
        if (
            node_idx is None
            and "graph.nodes" in flt
            and isinstance(flt["graph.nodes"], dict)
            and "$elemMatch" in flt["graph.nodes"]
        ):
            cond = flt["graph.nodes"]["$elemMatch"] or {}
            nodes = ((base.get("graph") or {}).get("nodes")) or []
            for i, n in enumerate(nodes):
                ok = True
                for ck, cv in cond.items():
                    val = self._get_path(n, ck)
                    val = _norm(val)
                    if isinstance(cv, dict):
                        if "$in" in cv:
                            in_list = [_norm(x) for x in cv["$in"]]
                            if val not in in_list:
                                ok = False
                                break
                        if "$ne" in cv and val == _norm(cv["$ne"]):
                            ok = False
                            break
                        if "$lte" in cv and not (val is not None and val <= _norm(cv["$lte"])):
                            ok = False
                            break
                        if "$lt" in cv and not (val is not None and val < _norm(cv["$lt"])):
                            ok = False
                            break
                        if "$gte" in cv and not (val is not None and val >= _norm(cv["$gte"])):
                            ok = False
                            break
                        if "$gt" in cv and not (val is not None and val > _norm(cv["$gt"])):
                            ok = False
                            break
                    else:
                        if _norm(cv) != val:
                            ok = False
                            break
                if ok:
                    node_idx = i
                    break

        def _resolve(tok: str) -> str:
            return str(node_idx) if tok == "$" else tok

        def _set_path(m: dict[str, Any], path: str, val: Any):
            parts = path.split(".")
            cur: Any = m
            for i in range(len(parts)):
                p = _resolve(parts[i])
                last = i == len(parts) - 1
                next_tok = _resolve(parts[i + 1]) if i + 1 < len(parts) else None
                next_is_index = bool(next_tok and next_tok.isdigit())
                if isinstance(cur, list):
                    if not p.isdigit():
                        p = "0"
                    idx = int(p)
                    while len(cur) <= idx:
                        cur.append({} if not next_is_index else [])
                    if last:
                        cur[idx] = val
                    else:
                        if not isinstance(cur[idx], dict | list):
                            cur[idx] = [] if next_is_index else {}
                        cur = cur[idx]
                else:
                    if last:
                        cur[p] = val
                    else:
                        if p not in cur or not isinstance(cur[p], dict | list):
                            cur[p] = [] if next_is_index else {}
                        cur = cur[p]

        def _inc_path(m: dict[str, Any], path: str, delta: int):
            parts = path.split(".")
            cur: Any = m
            for i in range(len(parts)):
                p = _resolve(parts[i])
                last = i == len(parts) - 1
                next_tok = _resolve(parts[i + 1]) if i + 1 < len(parts) else None
                next_is_index = bool(next_tok and next_tok.isdigit())
                if isinstance(cur, list):
                    if not p.isdigit():
                        p = "0"
                    idx = int(p)
                    while len(cur) <= idx:
                        cur.append({} if not next_is_index else [])
                    if last:
                        cur[idx] = int(cur[idx] or 0) + int(delta) if isinstance(cur[idx], int | float) else int(delta)
                    else:
                        if not isinstance(cur[idx], dict | list):
                            cur[idx] = [] if next_is_index else {}
                        cur = cur[idx]
                else:
                    if last:
                        cur[p] = int(cur.get(p, 0) or 0) + int(delta)
                    else:
                        if p not in cur or not isinstance(cur[p], dict | list):
                            cur[p] = [] if next_is_index else {}
                        cur = cur[p]

        def _max_path(m: dict[str, Any], path: str, val: int):
            parts = path.split(".")
            cur: Any = m
            for i in range(len(parts)):
                p = _resolve(parts[i])
                last = i == len(parts) - 1
                next_tok = _resolve(parts[i + 1]) if i + 1 < len(parts) else None
                next_is_index = bool(next_tok and next_tok.isdigit())
                if isinstance(cur, list):
                    if not p.isdigit():
                        p = "0"
                    idx = int(p)
                    while len(cur) <= idx:
                        cur.append({} if not next_is_index else [])
                    if last:
                        existing = cur[idx]
                        cur[idx] = max(int(existing or 0), int(val)) if isinstance(existing, int | float) else int(val)
                    else:
                        if not isinstance(cur[idx], dict | list):
                            cur[idx] = [] if next_is_index else {}
                        cur = cur[idx]
                else:
                    if last:
                        existing = cur.get(p)
                        cur[p] = max(int(existing or 0), int(val)) if isinstance(existing, int | float) else int(val)
                    else:
                        if p not in cur or not isinstance(cur[p], dict | list):
                            cur[p] = [] if next_is_index else {}
                        cur = cur[p]

        # Apply operators to a copy
        out = {} if created else {**base}

        if "$setOnInsert" in upd and created:
            for k, v in (upd["$setOnInsert"] or {}).items():
                _set_path(out, k, v)
            if self.name == "artifacts" and "status" in (upd["$setOnInsert"] or {}):
                dbg("DB.ARTIFACTS.STATUS", filter={}, new_status=str(upd["$setOnInsert"]["status"]))

        if "$set" in upd:
            if self.name == "tasks":
                for k, v in (upd["$set"] or {}).items():
                    if k == "graph.nodes.$.status":
                        dbg("DB.TASK.STATUS", filter={}, new_status=str(v))
            if self.name == "artifacts" and "status" in (upd["$set"] or {}):
                dbg("DB.ARTIFACTS.STATUS", filter={}, new_status=str(upd["$set"]["status"]))
            for k, v in (upd["$set"] or {}).items():
                _set_path(out, k, v)

        if "$inc" in upd:
            for k, v in (upd["$inc"] or {}).items():
                _inc_path(out, k, v)

        if "$max" in upd:
            for k, v in (upd["$max"] or {}).items():
                _max_path(out, k, v)

        if "$currentDate" in upd:
            for k in (upd["$currentDate"] or {}).keys():
                _set_path(out, k, _now_dt())

        return out

    # ───────────────────────── CRUD ─────────────────────────

    async def insert_one(self, doc):
        cand = dict(doc)
        self._ensure_unique_ok(cand)
        self.rows.append(cand)
        if self.name == "outbox":
            dbg("DB.OUTBOX.INSERT", size=len(self.rows), doc_keys=list(doc.keys()))

    async def find_one(self, flt, proj=None):
        for d in self.rows:
            if self._match(d, flt):
                return d
        return None

    async def count_documents(self, flt):
        return sum(1 for d in self.rows if self._match(d, flt))

    def find(self, flt, proj=None):
        rows = [d for d in self.rows if self._match(d, flt)]

        class _Cur:
            def __init__(self, rows: list[dict[str, Any]], getter):
                self._rows = rows
                self._getter = getter

            def sort(self, spec: Iterable[tuple[str, int]]):
                # Supports .sort([("field.path", 1|-1), ...])
                for field, order in reversed(list(spec or [])):
                    rev = int(order or 1) < 0

                    # Bind loop variable to avoid ruff B023
                    def _key(doc, _field=field):
                        v = self._getter(doc, _field)
                        # place None at the end for asc, start for desc
                        missing_flag = 1 if v is None else 0
                        return (missing_flag, v)

                    self._rows.sort(key=_key, reverse=rev)
                return self

            def limit(self, n):
                self._rows = self._rows[: int(n)]
                return self

            async def __aiter__(self):
                for r in list(self._rows):
                    yield r

        return _Cur(rows, InMemCollection._get_path)

    async def update_one(self, flt, upd, upsert=False):
        # find doc
        doc = None
        for d in self.rows:
            if self._match(d, flt):
                doc = d
                break

        created = False
        if not doc:
            if not upsert:
                return
            created = True
            doc = {}

        # build candidate with updates applied, then validate unique indexes
        candidate = self._apply_update_doc(doc, flt, upd or {}, created)
        self._ensure_unique_ok(candidate, ignore_doc=None if created else doc)

        if created:
            self.rows.append(candidate)
        else:
            # mutate in place
            doc.clear()
            doc.update(candidate)

    async def find_one_and_update(self, flt, upd):
        doc = await self.find_one(flt)
        await self.update_one(flt, upd, upsert=False)
        return doc

    async def create_index(self, keys, **kwargs):
        # Normalize keys input: list[tuple[str,int]]
        if isinstance(keys, dict):
            keys = list(keys.items())
        if not isinstance(keys, list):
            keys = [tuple(keys)]
        idx = _Index(list(map(lambda kv: (str(kv[0]), int(kv[1])), keys)), **kwargs)
        sig = idx.signature()
        for existing in self._indexes:
            if existing.signature() == sig:
                return idx.name or "ok"
        self._indexes.append(idx)
        return idx.name or "ok"


class InMemDB:
    """
    Dynamic collections map with Mongo-like indices.
    Access via attribute: db.tasks, or db.collection('tasks').
    """

    def __init__(self):
        self._collections: dict[str, InMemCollection] = {}

    def collection(self, name: str) -> InMemCollection:
        coll = self._collections.get(name)
        if coll is None:
            coll = InMemCollection(name)
            self._collections[name] = coll
        return coll

    def __getattr__(self, name: str) -> InMemCollection:
        return self.collection(name)

    async def create_index(self, collection: str, *args, **kwargs):
        return await self.collection(collection).create_index(*args, **kwargs)
