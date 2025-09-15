# Task Graphs

This page describes FlowKit DAGs: nodes, edges, fan‑in modes, streaming rules — and how to use **task variables** (`coordinator.vars`) to enable data‑driven behavior.

## Nodes

A graph consists of **nodes** with unique `node_id` and a `type`:

- Worker nodes (`type` = your worker type, e.g. `indexer`, `processor`, `analyzer`)
- Coordinator functions (`type` = `coordinator_fn`) which run small orchestration steps inside the coordinator process

Minimal node example:

```json
{
  "node_id": "processor",
  "type": "processor",
  "depends_on": ["indexer"]
}
```

## Edges

Use `edges: [["src", "dst"], ...]` to express simple dependencies. A node becomes **ready** when its fan‑in condition is satisfied (see below).

### Extended edges (`edges_ex`)

You can add additional routing metadata with `edges_ex` entries:

```json
{
  "edges": [["indexer", "processor"]],
  "edges_ex": [
    {"from": "indexer", "to": "processor", "mode": "async", "trigger": "on_batch"}
  ]
}
```

- `mode: "async"` — allows downstream to start before upstream completes
- `trigger: "on_batch"` — pair with `io.start_when = "first_batch"` on the child for streaming start

> The coordinator still enforces normal readiness checks; `edges_ex` is a **hint** that complements them.

## Fan‑in modes

A node can delay start until enough parents finish:

```json
{ "node_id": "combiner", "depends_on": ["a", "b", "c"], "fan_in": "all" }   // default
{ "node_id": "processor", "depends_on": ["a", "b"], "fan_in": "any" }
{ "node_id": "analyzer", "depends_on": ["a", "b", "c"], "fan_in": "count:2" }
```

## Streaming

Start a node as soon as the upstream emits its **first batch**:

```json
{ "node_id": "proc", "depends_on": ["indexer"], "io": {"start_when": "first_batch"} }
```

If omitted, the default is `ready` (start after parents finish).

## Coordinator functions in graphs

Coordinator functions are nodes with `type: "coordinator_fn"`. They execute one of the registered **adapters** by name via `io.fn` with arguments in `io.fn_args`.

```json
{
  "node_id": "seed_vars",
  "type": "coordinator_fn",
  "io": {
    "fn": "vars.set",
    "fn_args": { "kv": { "flags.ab": true, "sla.max_delay": 500 } }
  }
}
```

See [Coordinators → Task Variables](coordinators.md#task-variables-coordinatorvars) for all adapters.

## Using `coordinator.vars` for data‑driven routing

`coordinator.vars` is a task‑scoped key–value bag stored on the task document. Typical patterns:

- **Feature flags / AB routing**: `flags.ab = True` to route lighter branch in later coordinator steps
- **SLA knobs**: `sla.max_delay`, `sla.quality_threshold`
- **Counters and volumes**: `counters.pages += 1`

### Pattern: gate a branch with a coordinator function

1. Seed routing signals:

```json
{ "node_id": "seed_vars", "type": "coordinator_fn",
  "io": {"fn": "vars.merge", "fn_args": {"data": {"sla": {"max_delay": 500}, "flags": {"ab": true}}}} }
```

2. Downstream coordinator function checks `tasks.coordinator.vars` and decides what to do (e.g., set additional flags, update metrics, or schedule light‑weight work).

> The base scheduler reacts to **dependencies, fan‑in**, and **streaming** rules. Using `coordinator.vars` lets your coordinator‑side logic remain pure data. Upcoming features (see EPIC: Dynamic DAG) make conditional edges first‑class.

## Example: streaming with early processing

```json
{
  "nodes": [
    {"node_id": "indexer", "type": "indexer"},
    {"node_id": "proc", "type": "processor", "depends_on": ["indexer"],
      "io": {"start_when": "first_batch"}},
    {"node_id": "seed_vars", "type": "coordinator_fn", "depends_on": ["indexer"],
      "io": {"fn": "vars.incr", "fn_args": {"key": "counters.pages", "by": 2}}}
  ],
  "edges": [["indexer", "proc"], ["indexer", "seed_vars"]],
  "edges_ex": [
    {"from": "indexer", "to": "proc", "mode": "async", "trigger": "on_batch"}
  ]
}
```

- `proc` can start on the first upstream batch
- `seed_vars` adjusts a task counter side‑by‑side
