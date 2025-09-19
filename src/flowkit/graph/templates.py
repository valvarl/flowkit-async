# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Template support for foreach/spawn.

The goal is to validate and normalize node templates defined under
graph.templates (free-form dicts) and provide a helper to instantiate a
template into a concrete NodeSpecV2 with a specific node_id and overrides.

This module is intentionally lightweight: the compiler currently does not
materialize foreach templates; coordinator logic may call `instantiate_template`
when spawning dynamic nodes.
"""

from copy import deepcopy
from typing import Any, Dict, Mapping, Optional

from .spec import NodeSpecV2


class TemplateError(ValueError):
    """User-facing template error."""


def compile_templates(raw: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Shallow-validate template records and return a normalized copy.

    Rules:
      - each template must be a mapping
      - `node_id` must NOT be present (it will be assigned at instantiation)
      - minimal required fields for a node: 'type' (others are optional)
    """
    out: Dict[str, Dict[str, Any]] = {}
    for name, data in raw.items():
        if not isinstance(data, dict):
            raise TemplateError(f"template '{name}' must be an object")
        if "node_id" in data:
            raise TemplateError(f"template '{name}' must not include 'node_id'")
        if "type" not in data or not data.get("type"):
            raise TemplateError(f"template '{name}' must include non-empty 'type'")
        # keep as-is (NodeSpecV2 validation will happen at instantiation)
        out[name] = deepcopy(data)
    return out


def instantiate_template(
    templates: Mapping[str, Dict[str, Any]],
    name: str,
    *,
    node_id: str,
    parents: Optional[list[str]] = None,
    overrides: Optional[Mapping[str, Any]] = None,
) -> NodeSpecV2:
    """
    Create a NodeSpecV2 from a named template with optional field overrides.

    Merge strategy (shallow, last-wins):
      result = {**template, **overrides, "node_id": node_id, "parents": parents?}

    Deep merging of nested dicts is not performed here to keep behavior
    predictable; callers can pre-merge if needed.
    """
    try:
        base = deepcopy(templates[name])
    except KeyError as e:
        raise TemplateError(f"unknown template '{name}'") from e

    merged: Dict[str, Any] = {**base}
    if overrides:
        merged.update(dict(overrides))
    merged["node_id"] = node_id
    if parents is not None:
        merged["parents"] = list(parents)

    # Let NodeSpecV2 run full validation
    return NodeSpecV2.model_validate(merged)
