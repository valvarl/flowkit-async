# Complex Workflows

This page shows composable patterns for larger pipelines. Below is a compact example that uses **task variables** to set SLA knobs, stream processing early, and collect metrics — without touching worker code.

## Vars-driven SLA with streaming fan-in

```json
{
  "nodes": [
    {"node_id": "indexer", "type": "indexer"},
    {"node_id": "proc", "type": "processor", "depends_on": ["indexer"],
      "io": {"start_when": "first_batch"}},
    {"node_id": "update_sla", "type": "coordinator_fn", "depends_on": ["indexer"],
      "io": {"fn": "vars.merge", "fn_args": {"data": {"sla": {"max_delay": 500}, "flags": {"ab": true}}}}},
    {"node_id": "agg_metrics", "type": "coordinator_fn", "depends_on": ["proc"],
      "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": "proc", "mode": "sum"}}}
  ],
  "edges": [
    ["indexer", "proc"],
    ["indexer", "update_sla"],
    ["proc", "agg_metrics"]
  ],
  "edges_ex": [
    {"from": "indexer", "to": "proc", "mode": "async", "trigger": "on_batch"}
  ]
}
```

**What happens**

- `update_sla` seeds `coordinator.vars.sla.max_delay` and a feature flag
- `proc` can start as soon as `indexer` produces the first batch
- `agg_metrics` aggregates raw metrics from `proc` into `graph.nodes[].stats`

## Inspecting `coordinator.vars`

A coordinator function (or an external diagnostic tool) can read the task document to act on `vars`:

```python
task = await db.tasks.find_one({"id": task_id}, {"coordinator": 1})
vars = ((task or {}).get("coordinator") or {}).get("vars") or {}
assert isinstance(vars, dict)
```

## Cleaning up vars

You can remove keys and prune empty objects with `vars.unset`:

```json
{
  "node_id": "cleanup_vars",
  "type": "coordinator_fn",
  "io": {
    "fn": "vars.unset",
    "fn_args": {"keys": ["flags.ab", "sla.max_delay"]}
  }
}
```

> Empty parent objects are removed in a second pass by the adapter.
