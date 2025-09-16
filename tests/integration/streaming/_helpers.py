"""Small helpers shared by streaming tests."""

from __future__ import annotations

from typing import Any

from tests.helpers.graph import node_by_id


def _node_status(doc: dict[str, Any], node_id: str) -> Any:
    """Get node status by node_id from a task doc."""
    n = node_by_id(doc, node_id)
    return n.get("status") if n else None


def _make_indexer(node_id: str, total: int, batch: int) -> dict[str, Any]:
    """
    Build a simple indexer node:
      - emits `total` items in batches of `batch`.
      - uses inline input to avoid external dependencies.
    """
    return {
        "node_id": node_id,
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": batch, "total_skus": total}},
    }


def _get_count(doc: dict[str, Any], node_id: str) -> int:
    """Return the aggregated 'count' metric from a node's stats (0 if absent)."""
    node = node_by_id(doc or {}, node_id)
    return int((node.get("stats") or {}).get("count") or 0)
