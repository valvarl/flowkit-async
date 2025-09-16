from __future__ import annotations

from typing import Any

# Canonical adapter registry with aliases and minimal schema.
# Extend here to add new adapters and argument rules.
ADAPTER_SPECS: dict[str, dict[str, Any]] = {
    "pull.from_artifacts": {
        "aliases": {"pull.artifacts", "pull.from.upstream"},
        "required": set(),  # no required args
        "optional": {"from_nodes", "poll_ms", "eof_on_task_done", "backoff_max_ms"},
    },
    "pull.from_artifacts.rechunk:size": {
        "aliases": {"pull.from_artifacts.rechunk", "pull.rechunk"},
        "required": {"size"},
        "optional": {"meta_list_key", "from_nodes", "poll_ms", "eof_on_task_done", "backoff_max_ms"},
        # strict mode requires meta_list_key to be a string
        "strict_requires": {"meta_list_key"},
    },
}

# Argument alias map (extendable). Keys are input aliases, values are canonical names.
ARG_ALIAS: dict[str, str] = {
    "from_node": "from_nodes",
    # add future aliases here if needed
}


def resolve_adapter_name(name: str) -> str | None:
    """Return the canonical adapter name or None if unknown (considers aliases)."""
    if name in ADAPTER_SPECS:
        return name
    for canon, spec in ADAPTER_SPECS.items():
        if name in spec.get("aliases", set()):
            return canon
    return None


def adapter_aliases() -> dict[str, str]:
    """Return {alias -> canonical} mapping for all registered adapters."""
    out: dict[str, str] = {}
    for canon, spec in ADAPTER_SPECS.items():
        for a in spec.get("aliases", set()):
            out[a] = canon
    return out


def normalize_adapter_args(args: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    """Normalize argument aliases without applying defaults.

    Returns:
      (from_nodes, kwargs_without_from_nodes)

    Notes:
      Defaults (poll/backoff/etc.) are intentionally NOT set here; the worker
      supplies them from its WorkerConfig.
    """
    # normalize argument name aliases
    norm: dict[str, Any] = {}
    for k, v in (args or {}).items():
        norm[ARG_ALIAS.get(k, k)] = v

    # normalize from_nodes to a list[str]
    fn = norm.get("from_nodes", [])
    if isinstance(fn, list | tuple | set):
        from_nodes: list[str] = list(fn)
    elif isinstance(fn, str):
        from_nodes = [fn]
    elif fn is None:
        from_nodes = []
    else:
        # be conservative: reject unexpected types early
        raise TypeError("from_nodes must be a list/tuple/set, string, or None")

    kwargs = dict(norm)
    kwargs.pop("from_nodes", None)
    return from_nodes, kwargs


def validate_input_adapter(name: str, all_args: dict[str, Any], *, strict: bool = False) -> tuple[bool, str | None]:
    """Shared validation for coordinator and worker (canonicalizes names and checks minimal schema)."""
    canon = resolve_adapter_name(name)
    if not canon:
        return False, f"unknown adapter '{name}'"

    spec = ADAPTER_SPECS[canon]
    required: set[str] = set(spec.get("required", set()))
    # optional args exist but we intentionally don't enforce/whitelist them here
    strict_requires: set[str] = set(spec.get("strict_requires", set())) if strict else set()

    # required args presence
    missing = [r for r in required if r not in all_args]
    if missing:
        return False, f"missing required args: {missing}"

    # strict-only required args
    if strict:
        missing_strict = [r for r in strict_requires if r not in all_args]
        if missing_strict:
            return False, f"strict mode: missing required args: {missing_strict}"

    # adapter-specific checks
    if canon == "pull.from_artifacts.rechunk:size":
        size = all_args.get("size")
        if not isinstance(size, int) or size <= 0:
            return False, "missing or invalid 'size' (positive int required)"
        mlk = all_args.get("meta_list_key", None)
        if mlk is not None and not isinstance(mlk, str):
            return False, "'meta_list_key' must be a string if provided"
        if strict and not isinstance(mlk, str):
            return False, "strict mode: 'meta_list_key' is required and must be a string"

    # unknown args are allowed here (they may be ignored by adapters), but
    # you can tighten this later by whitelisting optional+required only.

    return True, None
