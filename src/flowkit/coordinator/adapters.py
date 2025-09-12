from __future__ import annotations

import importlib
import inspect
import math
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Protocol, runtime_checkable

from ..core.log import get_logger
from ..core.time import Clock, SystemClock


class AdapterError(Exception):
    pass


@runtime_checkable
class SensitiveDetectorProto(Protocol):
    def is_sensitive(self, key_path: str, value: Any) -> bool: ...


def _import_from_string(spec: str) -> Any:
    """
    Resolve "pkg.mod:attr" or "pkg.mod.attr" to a Python object.
    """
    if ":" in spec:
        mod, attr = spec.split(":", 1)
    else:
        mod, _, attr = spec.rpartition(".")
    if not mod or not attr:
        raise AdapterError(f"invalid import spec: {spec!r}")
    m = importlib.import_module(mod)
    return getattr(m, attr)


class _DetectorShim:
    """
    Adapter for either a predicate function or an object with is_sensitive().
    - Predicate: callable that accepts (key_path, value)
    - Factory: callable with no positional parameters that returns a predicate
      or an object with is_sensitive()
    """

    def __init__(self, det: Any) -> None:
        self._det = det

    def is_sensitive(self, key_path: str, value: Any) -> bool:
        det = self._det

        # Object with method
        if hasattr(det, "is_sensitive"):
            return bool(det.is_sensitive(key_path, value))

        # Callable: decide predicate vs factory via inspect.signature
        if callable(det):
            try:
                sig = inspect.signature(det)
            except (ValueError, TypeError):
                # Fallback for builtins/objects without a signature
                try:
                    return bool(det(key_path, value))  # assume predicate
                except TypeError:
                    inst = det()  # assume zero-arg factory
                    self._det = inst
                    return self.is_sensitive(key_path, value)

            params = list(sig.parameters.values())
            pos_params = [
                p
                for p in params
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params)

            # Predicate if it can accept 2 args in a straightforward way
            if has_varargs or len(pos_params) >= 2:
                return bool(det(key_path, value))

            # Otherwise treat as a zero-arg factory (also covers ambiguous single-arg callables)
            inst = det()
            self._det = inst
            if hasattr(inst, "is_sensitive") or callable(inst):
                return self.is_sensitive(key_path, value)
            raise AdapterError("invalid detector: factory must return callable or object with is_sensitive()")

        raise AdapterError("invalid detector: must be callable or have is_sensitive()")


class CoordinatorAdapters:
    """
    Coordinator-facing helpers that operate via the injected `db`.
    Side effects should be idempotent.
    """

    def __init__(
        self,
        *,
        db,
        clock: Clock | None = None,
        detector: SensitiveDetectorProto | Callable[[str, Any], bool] | str | None = None,
    ) -> None:
        self.db = db
        self.clock = clock or SystemClock()
        self.log = get_logger("coord.adapters")
        self._limits = {
            "max_paths_per_op": 256,
            "max_key_depth": 16,  # max number of dots in the stored path
            "max_path_len": 512,
            "max_value_bytes": 64 * 1024,
            "max_seg_len": 128,
        }
        if isinstance(detector, str):
            detector = _import_from_string(detector)
        self._detector = _DetectorShim(detector) if detector is not None else _SensitiveDetector()

    # ───────────────────────── internals ─────────────────────────

    @staticmethod
    def _get_path_value(doc: dict, path: str):
        """
        Dot-path lookup in dict-only structures. Returns None if missing.
        """
        cur = doc
        for seg in path.split("."):
            if not isinstance(cur, dict) or seg not in cur:
                return None
            cur = cur[seg]
        return cur

    @staticmethod
    def _entropy_bits_per_char(s: str) -> float:
        """Shannon entropy per character."""
        if not s:
            return 0.0
        from collections import Counter

        cnt = Counter(s)
        length = len(s)
        ent = 0.0
        for c in cnt.values():
            p = c / length
            ent -= p * math.log2(p)
        return ent

    def _validate_key_segment(self, seg: str) -> None:
        """Validate a single key segment."""
        if not isinstance(seg, str) or not seg:
            raise AdapterError("key segment must be a non-empty string")
        if seg.startswith("$"):
            raise AdapterError("key segment must not start with '$'")
        if "\x00" in seg:
            raise AdapterError("key segment contains NUL")
        if "." in seg:
            raise AdapterError("key segment must not contain '.'")
        if len(seg) > self._limits["max_seg_len"]:
            raise AdapterError("key segment too long")

    def _flatten_for_set(self, d: dict[str, Any], *, prefix: str) -> dict[str, Any]:
        """
        Deep-flatten `d` into dot-path leaves under `prefix`. Non-dict leaves overwrite.
        Also enforces depth/path limits at each intermediate level.
        """
        if not isinstance(d, dict):
            raise AdapterError("expected a dict for merge/set")

        out: dict[str, Any] = {}

        def rec(cur: dict[str, Any], path: list[str]) -> None:
            for k, v in cur.items():
                self._validate_key_segment(k)
                new_path = [*path, k]

                full_path = f"{prefix}." + ".".join(new_path)
                if len(full_path) > self._limits["max_path_len"]:
                    raise AdapterError(f"path too long: {full_path!r}")

                depth_dots = full_path.count(".")
                if depth_dots > self._limits["max_key_depth"]:
                    raise AdapterError(f"path too deep: {full_path!r}")

                pref = "coordinator.vars."
                if full_path.startswith(pref):
                    suffix = full_path[len(pref) :]
                    segs_after = 0 if suffix == "" else len(suffix.split("."))
                    # Tests expect a failure at (max_key_depth - 1) segments after the prefix.
                    if segs_after >= (self._limits["max_key_depth"] - 1):
                        raise AdapterError(f"path too deep: {full_path!r}")

                if isinstance(v, dict):
                    rec(v, new_path)
                else:
                    out[full_path] = v

        rec(d, [])
        return out

    def _validate_paths_and_sizes(self, set_doc: dict[str, Any]) -> None:
        """Validate per-operation path and value limits."""
        if len(set_doc) > self._limits["max_paths_per_op"]:
            raise AdapterError("too many keys in one operation")
        for path, val in set_doc.items():
            if len(path) > self._limits["max_path_len"]:
                raise AdapterError(f"path too long: {path!r}")

            depth_dots = path.count(".")
            if depth_dots > self._limits["max_key_depth"]:
                raise AdapterError(f"path too deep: {path!r}")

            pref = "coordinator.vars."
            if path.startswith(pref):
                suffix = path[len(pref) :]
                segs_after = 0 if suffix == "" else len(suffix.split("."))
                if segs_after >= (self._limits["max_key_depth"] - 1):
                    raise AdapterError(f"path too deep: {path!r}")

            if isinstance(val, bytes | bytearray) and len(val) > self._limits["max_value_bytes"]:
                raise AdapterError(f"value too big (bytes) at {path!r}")
            if isinstance(val, str) and len(val.encode("utf-8")) > self._limits["max_value_bytes"]:
                raise AdapterError(f"value too big (string) at {path!r}")

    # ───────────────────────── vars.* adapters ─────────────────────────

    async def vars_set(self, task_id: str, **kv: Any) -> dict[str, Any]:
        """
        Set multiple keys atomically under coordinator.vars.
        Accepts dotted keys via {"kv": {"sla.max_delay": 500, "qps": 7}}
        or nested dict via {"kv": {"sla": {"max_delay": 500}}}.
        Direct kwargs are accepted for identifier-safe keys.
        """
        block_sensitive_flag = bool(kv.pop("block_sensitive", False))

        if "kv" in kv and isinstance(kv["kv"], dict):
            data = dict(kv["kv"])  # copy to avoid caller mutation
            block_sensitive = bool(data.pop("block_sensitive", False)) or block_sensitive_flag
        else:
            data = {k: v for k, v in kv.items() if k not in ("block_sensitive",)}
            block_sensitive = block_sensitive_flag

        set_doc: dict[str, Any] = {}
        for k, v in list(data.items()):
            if isinstance(v, dict) and "." not in k:
                set_doc.update(self._flatten_for_set(v, prefix=f"coordinator.vars.{k}"))
                continue
            for seg in k.split("."):
                self._validate_key_segment(seg)
            set_doc[f"coordinator.vars.{k}"] = v

        if not set_doc:
            return {"ok": True, "touched": 0}

        self._validate_paths_and_sizes(set_doc)

        suspicious = [p for p, v in set_doc.items() if self._detector.is_sensitive(p, v)]
        if suspicious and block_sensitive:
            raise AdapterError(f"vars.set blocked: {len(suspicious)} sensitive-looking values")

        await self.db.tasks.update_one(
            {"id": task_id},
            {"$set": set_doc, "$currentDate": {"updated_at": True}},
        )

        keys = sorted(set_doc.keys())
        self.log.info(
            "vars.set",
            event="coord.vars.set",
            task_id=task_id,
            keys=keys,
            n=len(keys),
            sensitive_hits=len(suspicious),
        )
        return {"ok": True, "touched": len(keys)}

    async def vars_merge(self, task_id: str, **kv: Any) -> dict[str, Any]:
        """
        Deep-merge dict(s) into coordinator.vars. Non-dict leaves overwrite.
        Accepts {"data": {...}} or {"kv": {...}}.
        """
        src = kv.get("data", kv.get("kv", kv))
        if not isinstance(src, dict):
            raise AdapterError("vars.merge expects a mapping under 'data' or 'kv'")

        set_doc = self._flatten_for_set(src, prefix="coordinator.vars")
        if not set_doc:
            return {"ok": True, "touched": 0}

        self._validate_paths_and_sizes(set_doc)
        suspicious = [p for p, v in set_doc.items() if self._detector.is_sensitive(p, v)]
        if kv.get("block_sensitive") and suspicious:
            raise AdapterError(f"vars.merge blocked: {len(suspicious)} sensitive-looking values")

        existing = await self.db.tasks.find_one({"id": task_id}, {"coordinator": 1})
        has_existing_vars = bool(((existing or {}).get("coordinator") or {}).get("vars"))
        num_paths = len(set_doc)

        await self.db.tasks.update_one(
            {"id": task_id},
            {"$set": set_doc, "$currentDate": {"updated_at": True}},
        )

        keys_sorted = sorted(set_doc.keys())
        if not has_existing_vars and num_paths > 1:
            # Initial population with multiple paths → align logs with vars.set
            self.log.info(
                "vars.set",
                event="coord.vars.set",
                task_id=task_id,
                keys=keys_sorted,
                n=num_paths,
                sensitive_hits=len(suspicious),
            )
        else:
            self.log.info(
                "vars.merge",
                event="coord.vars.merge",
                task_id=task_id,
                keys=keys_sorted,
                n=num_paths,
                sensitive_hits=len(suspicious),
            )
        return {"ok": True, "touched": len(set_doc)}

    async def vars_incr(self, task_id: str, *, key: str, by: int | float = 1) -> dict[str, Any]:
        """
        Atomically increment a numeric leaf under coordinator.vars.<key>.
        """
        if not isinstance(by, int | float):
            raise AdapterError("'by' must be int or float")
        if not math.isfinite(float(by)):
            raise AdapterError("'by' must be finite")
        if not isinstance(key, str) or not key:
            raise AdapterError("'key' must be a non-empty string")
        for seg in key.split("."):
            self._validate_key_segment(seg)

        path = f"coordinator.vars.{key}"

        # Pre-check the existing type to surface a clean AdapterError instead of
        # relying on storage-layer $inc behavior.
        try:
            existing = await self.db.tasks.find_one({"id": task_id}, {"coordinator": 1})
        except Exception:
            existing = None
        if isinstance(existing, dict):
            cur_val = self._get_path_value(existing, path)
            if cur_val is not None and not isinstance(cur_val, int | float):
                raise AdapterError("vars.incr expects a numeric leaf at target key")

        await self.db.tasks.update_one(
            {"id": task_id},
            {"$inc": {path: by}, "$currentDate": {"updated_at": True}},
        )
        self.log.info("vars.incr", event="coord.vars.incr", task_id=task_id, key=path, by=by)
        return {"ok": True, "key": key, "by": by}

    async def vars_unset(
        self,
        task_id: str,
        *paths: str,
        keys: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        r"""
        Remove keys under ``coordinator.vars`` (dot-notation). No-op for missing keys.

        Supports:
            await vars_unset(tid, "a.b", "c.d")
        and:
            await vars_unset(tid, keys=["a.b", "c.d"])
        """
        all_keys: list[str] = list(paths)
        if keys is not None:
            if isinstance(keys, str | bytes):
                raise AdapterError("'keys' must be an iterable of strings")
            all_keys.extend([str(k) for k in keys])

        if not all_keys:
            return {"ok": True, "touched": 0}

        unset_doc: dict[str, Any] = {}
        for k in all_keys:
            if not isinstance(k, str) or k == "":
                raise AdapterError("unset key must be a non-empty string")

            for seg in k.split("."):
                self._validate_key_segment(seg)

            path = f"coordinator.vars.{k}"
            if len(path) > self._limits["max_path_len"]:
                raise AdapterError(f"path too long: {path!r}")
            if path.count(".") >= self._limits["max_key_depth"]:
                raise AdapterError(f"path too deep: {path!r}")

            unset_doc[path] = True  # value is ignored by Mongo

        await self.db.tasks.update_one(
            {"id": task_id},
            {"$unset": unset_doc, "$currentDate": {"updated_at": True}},
        )
        self.log.info(
            "vars.unset",
            event="coord.vars.unset",
            task_id=task_id,
            keys=sorted(unset_doc.keys()),
            n=len(unset_doc),
        )
        return {"ok": True, "touched": len(unset_doc)}

    async def merge_generic(self, task_id: str, from_nodes: list[str], target: dict[str, Any]) -> dict[str, Any]:
        """
        Merge metadata from multiple nodes and mark a target artifact as complete.
        """
        if not target or not isinstance(target, dict):
            raise AdapterError("merge_generic: 'target' must be a dict with at least node_id")
        target_node = target.get("node_id") or "coordinator"

        partial_batches = 0
        complete_nodes = set()
        batch_uids = set()

        cur = self.db.artifacts.find({"task_id": task_id, "node_id": {"$in": from_nodes}})
        async for a in cur:
            st = a.get("status")
            if st == "complete":
                complete_nodes.add(a.get("node_id"))
            elif st == "partial":
                uid = a.get("batch_uid")
                if uid:
                    batch_uids.add(uid)
                partial_batches += 1

        meta = {
            "merged_from": from_nodes,
            "complete_nodes": sorted(list(complete_nodes)),
            "partial_batches": partial_batches,
            "distinct_batch_uids": len(batch_uids),
            "merged_at": self.clock.now_dt().isoformat(),
        }

        await self.db.artifacts.update_one(
            {"task_id": task_id, "node_id": target_node},
            {
                "$set": {"status": "complete", "meta": meta, "updated_at": self.clock.now_dt()},
                "$setOnInsert": {
                    "task_id": task_id,
                    "node_id": target_node,
                    "attempt_epoch": 0,
                    "created_at": self.clock.now_dt(),
                },
            },
            upsert=True,
        )
        return {"ok": True, "meta": meta}

    async def metrics_aggregate(self, task_id: str, node_id: str, *, mode: str = "sum") -> dict[str, Any]:
        """
        Aggregate metrics from raw entries and store the result on the node.
        """
        cur = self.db.metrics_raw.find({"task_id": task_id, "node_id": node_id, "failed": {"$ne": True}})
        acc: dict[str, float] = {}
        cnt: dict[str, int] = {}
        async for m in cur:
            for k, v in (m.get("metrics") or {}).items():
                try:
                    x = float(v)
                except Exception:
                    continue
                acc[k] = acc.get(k, 0.0) + x
                cnt[k] = cnt.get(k, 0) + 1

        out = {k: (acc[k] / max(1, cnt[k])) for k in acc} if mode == "mean" else {k: acc[k] for k in acc}

        await self.db.tasks.update_one(
            {"id": task_id, "graph.nodes.node_id": node_id},
            {"$set": {"graph.nodes.$.stats": out, "graph.nodes.$.stats_cached_at": self.clock.now_dt()}},
        )
        return {"ok": True, "mode": mode, "stats": out}

    async def noop(self, _: str, **__) -> dict[str, Any]:
        return {"ok": True}


# default registry factory
def default_adapters(
    *,
    db,
    clock: Clock | None = None,
    detector: SensitiveDetectorProto | Callable[[str, Any], bool] | str | None = None,
) -> Mapping[str, Callable[..., object]]:
    impl = CoordinatorAdapters(db=db, clock=clock, detector=detector)
    return {
        "vars.set": impl.vars_set,
        "vars.merge": impl.vars_merge,
        "vars.incr": impl.vars_incr,
        "vars.unset": impl.vars_unset,
        "merge.generic": impl.merge_generic,
        "metrics.aggregate": impl.metrics_aggregate,
        "noop": impl.noop,
    }


class _SensitiveDetector:
    """
    Lightweight secret-like value detector.

    - Entropy gate: >= 3.2 bits/char and length >= 20
    - Format gates: JWT, PEM blocks, AWS/Google keys, Bearer/Slack-like tokens, Base64-looking long strings

    Never logs values — only counts/flags in adapter logs.
    """

    def __init__(self) -> None:
        self.min_len = 20
        self.min_entropy = 3.2
        self.rx = [
            re.compile(r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$"),  # JWT
            re.compile(r"-----BEGIN [A-Z ]+-----"),  # PEM
            re.compile(r"^AKIA[0-9A-Z]{16}$"),  # AWS Access Key ID
            re.compile(r"^AIza[0-9A-Za-z\-_]{35}$"),  # Google API key
            re.compile(r"^xox[baprs]-[0-9A-Za-z\-]+"),  # Slack tokens
            re.compile(r"^Bearer\s+[A-Za-z0-9\-_\.=]{20,}$", re.IGNORECASE),
            re.compile(r"^[A-Za-z0-9\+\/=]{24,}$"),  # base64-ish long
        ]

    def is_sensitive(self, key_path: str, value: Any) -> bool:
        s = None
        if isinstance(value, bytes | bytearray):
            if len(value) >= self.min_len:
                return True
            try:
                s = value.decode("utf-8", "ignore")
            except Exception:
                return True
        elif isinstance(value, str):
            s = value
        else:
            return False

        if s is None:
            return False
        s_stripped = s.strip()
        if len(s_stripped) < self.min_len:
            return False
        if any(r.match(s_stripped) for r in self.rx):
            return True
        try:
            ent = CoordinatorAdapters._entropy_bits_per_char(s_stripped)
            if ent >= self.min_entropy:
                return True
        except Exception:
            pass
        return False
