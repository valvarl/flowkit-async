# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Adapter name/args normalization and capability checks.

Design goals:
- Keep the validation permissive for userland adapters (unknown names allowed).
- Provide helpful messages for common mistakes (missing required args, select mismatch).
- Do not apply runtime defaults here: workers supply adapter defaults via config.
"""

from typing import Any

from .capabilities import get_caps

__all__ = [
    "ADAPTER_SPECS",
    "ARG_ALIAS",
    "adapter_aliases",
    "check_select_supported",
    "normalize_adapter_args",
    "resolve_adapter_name",
    "validate_input_adapter",
    "validate_io_source",
    "validate_output_adapter",
]


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
    "pull.kafka.subscribe": {
        "aliases": {"pull.kafka"},
        "required": set(),
        "optional": {"group", "client", "topics", "assign", "poll_ms"},
    },
    # outputs/sinks can be listed here for name canonicalization (args usually adapter-specific)
    "push.to_artifacts": {
        "aliases": {"push.artifacts"},
        "required": set(),
        "optional": {"bucket", "prefix", "codec"},
    },
    "emit.kafka": {
        "aliases": {"push.kafka"},
        "required": set(),
        "optional": {"external", "topic", "key", "headers"},
    },
    "emit.s3": {
        "aliases": {"push.s3"},
        "required": set(),
        "optional": {"bucket", "key_template", "acl"},
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
    if isinstance(fn, (list, tuple, set)):
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
        # Unknown adapters are allowed; upstream may provide own validation.
        return True, None

    spec = ADAPTER_SPECS[canon]
    required: set[str] = set(spec.get("required", set()))
    strict_requires: set[str] = set(spec.get("strict_requires", set())) if strict else set()

    missing = [r for r in required if r not in all_args]
    if missing:
        return False, f"missing required args: {missing}"

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

    return True, None


def check_select_supported(adapter_name: str, select: str) -> tuple[bool, str | None]:
    """Check that the adapter supports the requested select mode."""
    caps = get_caps(adapter_name)
    if not caps:
        # unknown adapter — skip strict validation
        return True, None
    supported = set(caps.get("supports_select", []) or [])
    if supported and select not in supported:
        return False, f"adapter {adapter_name!r} does not support select={select!r} (supports: {sorted(supported)})"
    return True, None


def validate_io_source(
    adapter_name: str, select: str, args: dict[str, Any], *, strict: bool = False
) -> tuple[bool, str | None]:
    """Validate a single input source tuple (adapter/select/args)."""
    ok, err = validate_input_adapter(adapter_name, args, strict=strict)
    if not ok:
        return ok, err
    ok, err = check_select_supported(adapter_name, select)
    return ok, err


def validate_output_adapter(name: str, args: dict[str, Any] | None = None) -> tuple[bool, str | None]:
    """Validate output adapter reference (name exists or is alias).

    For userland adapters (unknown), we return True to avoid blocking compilation.
    """
    canon = resolve_adapter_name(name)
    if canon:
        return True, None
    # Unknown outputs are allowed; coordinator will treat them as userland.
    return True, None
