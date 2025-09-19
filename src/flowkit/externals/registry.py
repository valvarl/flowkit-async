# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Runtime resolver for declared externals (graph-level and node-level).

Responsibilities:
- Merge global and local declarations with clear override semantics.
- Resolve each declaration into a live `ExternalProvider` instance using a factory map keyed by `kind`.
- Manage provider lifecycle (open/close) with instance reuse via configuration fingerprinting.
- Provide simple reference counting so multiple nodes can share the same provider instance safely.

Design notes:
- Factories are pure callables: (name, kind, config) -> ExternalProvider
- Unknown kinds can either raise (strict mode) or fall back to a generic "static" provider.
- To maximize reuse, provider instances are cached by (kind, stable_hash(config-with-kind)).
- Resolver is DB/bus-agnostic and does not import heavy deps.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple

from ..core.logging import get_logger
from ..core.utils import stable_hash
from ..api.externals import ExternalProvider, ExternalReady, ExternalError


# -----------------------------
# Factory type & built-ins
# -----------------------------

ExternalFactory = Callable[[str, str, Mapping[str, Any]], ExternalProvider]


class _GenericExternal(ExternalProvider):
    """
    A generic, no-op external provider.

    Useful as a default for unknown kinds in non-strict mode, and for tests.
    `ready()` returns True unless config explicitly sets {"ready": false}.
    """

    name: str

    def __init__(self, name: str, kind: str, config: Mapping[str, Any]) -> None:
        self.name = name
        self.kind = kind
        self.config = dict(config)
        self._open = False
        self._ready = bool(config.get("ready", True))

    async def open(self, config: Mapping[str, Any]) -> None:  # type: ignore[override]
        self._open = True

    async def ready(self) -> ExternalReady:  # type: ignore[override]
        return ExternalReady(bool(self._ready))

    async def close(self) -> None:  # type: ignore[override]
        self._open = False

    def source_factory(self) -> Any:  # type: ignore[override]
        return None


# -----------------------------
# Merge & normalization helpers
# -----------------------------


def merge_declared(
    graph_level: Mapping[str, Mapping[str, Any]] | None,
    node_level: Mapping[str, Mapping[str, Any]] | None,
) -> Dict[str, Dict[str, Any]]:
    """
    Merge graph-level and node-level externals with override semantics.

    Rules:
      - Keys (names) from node-level overshadow graph-level entirely.
      - Within the same name, we shallow-merge dicts with node-level fields winning.
      - `kind`:
          * if node-level specifies a kind -> use it;
          * else inherit graph-level kind;
          * if neither provides kind -> raise.

    Returns: new dict suitable for resolution (JSON-serializable).
    """
    out: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in (graph_level or {}).items()}
    for name, local_cfg in (node_level or {}).items():
        base = out.get(name, {})
        merged = dict(base)
        merged.update(dict(local_cfg))
        if not merged.get("kind"):
            raise ValueError(f"external '{name}' must have a 'kind'")
        out[name] = merged
    # sanity: ensure every entry has kind
    for n, cfg in out.items():
        if not cfg.get("kind"):
            raise ValueError(f"external '{n}' must have a 'kind'")
    return out


def _fingerprint(kind: str, cfg: Mapping[str, Any]) -> str:
    """
    Compute a stable fingerprint for caching provider instances.
    Includes `kind` to prevent collisions between different kinds with the same body.
    """
    body = dict(cfg)
    # retain kind as part of the hash to be explicit
    body["__kind__"] = kind
    return stable_hash(body)


# -----------------------------
# Resolver & lifecycle manager
# -----------------------------


@dataclass
class _Cached:
    instance: ExternalProvider
    refcnt: int


class ExternalsResolver:
    """
    Registry + lifecycle manager for external providers.

    Typical usage:

        resolver = ExternalsResolver()
        resolver.register_factory("kafka.topic", KafkaExternal)
        ...
        declared = merge_declared(spec.externals, node.externals)
        providers = await resolver.acquire(declared)
        try:
            ready = await providers["kafka.analytics"].ready()
            ...
        finally:
            await resolver.release(providers)

    You can reuse a single resolver across the whole Coordinator/Worker process.
    Instances are reference-counted and re-used by configuration fingerprint.
    """

    def __init__(
        self,
        *,
        factories: Mapping[str, ExternalFactory] | None = None,
        strict_kinds: bool = False,
    ) -> None:
        self._log = get_logger("externals.resolver")
        self._strict = bool(strict_kinds)
        # factories by kind
        self._factories: Dict[str, ExternalFactory] = dict(factories or {})
        # cache: fp -> _Cached
        self._cache: Dict[str, _Cached] = {}
        # reverse name index (name -> fp) to support targeted release
        self._name_to_fp: Dict[str, str] = {}

        # register a generic fallback (only used when not strict)
        self._generic_factory: ExternalFactory = lambda name, kind, cfg: _GenericExternal(name, kind, cfg)

    # ---- registration API

    def register_factory(self, kind: str, factory: ExternalFactory) -> None:
        """
        Register or override a factory for a given external kind.

        The factory must be a callable `(name, kind, config) -> ExternalProvider`.
        """
        if not kind or not callable(factory):
            raise ValueError("invalid external factory registration")
        self._factories[kind] = factory

    def has_kind(self, kind: str) -> bool:
        return kind in self._factories

    def kinds(self) -> list[str]:
        return sorted(self._factories.keys())

    # ---- acquire / release

    async def acquire(self, declared: Mapping[str, Mapping[str, Any]]) -> Dict[str, ExternalProvider]:
        """
        Resolve and open providers for the given declaration map.

        Returns a mapping name -> ExternalProvider. Reuses cached instances if configs match.
        """
        result: Dict[str, ExternalProvider] = {}
        for name, cfg in declared.items():
            kind = str(cfg.get("kind", "")).strip()
            if not kind:
                raise ValueError(f"external '{name}' has no 'kind'")

            fp = _fingerprint(kind, cfg)
            cached = self._cache.get(fp)
            if cached is None:
                # create new instance
                factory = self._factories.get(kind)
                if factory is None:
                    if self._strict:
                        raise ValueError(f"unknown external kind: {kind!r}")
                    factory = self._generic_factory
                    self._log.debug("using generic external for unknown kind", kind=kind, name=name)
                inst = factory(name, kind, cfg)
                await inst.open(cfg)  # provider receives the raw config map
                self._cache[fp] = _Cached(instance=inst, refcnt=1)
                self._name_to_fp[name] = fp
                result[name] = inst
            else:
                cached.refcnt += 1
                self._name_to_fp[name] = fp
                result[name] = cached.instance
        return result

    async def release(self, providers_by_name: Mapping[str, ExternalProvider] | Iterable[str]) -> None:
        """
        Release previously acquired providers (by names or by mapping).

        When the reference count of an instance reaches zero, it is closed and evicted.
        """
        if isinstance(providers_by_name, Mapping):
            names = list(providers_by_name.keys())
        else:
            names = list(providers_by_name)

        for name in names:
            fp = self._name_to_fp.pop(name, None)
            if fp is None:
                continue
            cached = self._cache.get(fp)
            if cached is None:
                continue
            cached.refcnt -= 1
            if cached.refcnt <= 0:
                # last reference -> close and evict
                try:
                    await cached.instance.close()
                finally:
                    self._cache.pop(fp, None)

    async def close_all(self) -> None:
        """
        Close and clear all cached provider instances regardless of reference counts.
        """
        for fp, cached in list(self._cache.items()):
            try:
                await cached.instance.close()
            finally:
                self._cache.pop(fp, None)
        self._name_to_fp.clear()

    # ---- convenience helpers

    async def resolve_for_node(
        self,
        graph_level: Mapping[str, Mapping[str, Any]] | None,
        node_level: Mapping[str, Mapping[str, Any]] | None,
    ) -> Dict[str, ExternalProvider]:
        """
        Merge + acquire providers for a node overlaying graph-level externals.
        """
        merged = merge_declared(graph_level, node_level)
        return await self.acquire(merged)
