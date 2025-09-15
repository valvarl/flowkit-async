# Coordinator API Reference

This page combines API introspection with a practical overview of coordinator adapters and task variables.

## Task Variables & Adapters (overview)

Task variables live under `tasks.coordinator.vars`. Adapters are exposed as coordinator functions and can also be imported and called directly in Python.

### Quick reference

| Adapter | Signature (simplified) | Semantics |
|---|---|---|
| `vars.set` | `vars_set(task_id, **kv)` | Set multiple dotted/nested keys atomically (`$set`). Optional `block_sensitive=True`. |
| `vars.merge` | `vars_merge(task_id, **kv)` | Deep-merge dicts into `coordinator.vars`; non-dict leaves overwrite. Optional `block_sensitive=True`. |
| `vars.incr` | `vars_incr(task_id, *, key, by=1)` | Atomic numeric increment on `coordinator.vars.<key>` (`$inc`). Creates the leaf if missing. |
| `vars.unset` | `vars_unset(task_id, *paths, keys=None)` | Remove one or more keys; prunes empty subtrees afterwards. |
| `merge.generic` | `merge_generic(task_id, from_nodes, target)` | Merge metadata across nodes, mark target artifact complete. |
| `metrics.aggregate` | `metrics_aggregate(task_id, node_id, mode="sum")` | Aggregate raw metrics, write to node stats. |
| `noop` | `noop(task_id, **_)` | No-op helper useful for tests. |

### Limits & validation

- Max 256 keys per operation
- Max depth ≈ 16 dot segments across the full path (including `coordinator.vars.`)
- Max path length 512 chars; max segment length 128 chars
- Max string/bytes value size: 64 KiB (UTF‑8 for strings)
- Key segments cannot start with `$`, contain `.` or NUL (`\x00`)

### Sensitive value detection

Adapters can block or flag sensitive-looking values (tokens, PEM/JWT, high-entropy blobs). Pass `block_sensitive=True` to make the adapter fail fast, otherwise the write succeeds and logs `sensitive_hits` > 0.

---

## Coordinator Class

::: flowkit.coordinator.runner.Coordinator
    options:
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - start
        - stop
        - create_task

## Adapters (API via mkdocstrings)

::: flowkit.coordinator.adapters
    options:
      show_root_heading: true
      show_source: true
