# FlowKit Graph Specification v2.2

> **Status:** Draft, feature-complete for schema v2.2 (static + dynamic).
> **Scope:** Declarative graph model used by FlowKit Coordinator/Worker.
> **Non-goals:** This document does not describe internal DB schemas, Kafka topics, or handler SDK details.

---

## Table of Contents

1. [Overview](#overview)
2. [Versioning & Compatibility](#versioning--compatibility)
3. [Top-level Document](#top-level-document)
4. [NodeSpec](#nodespec)
5. [Start Semantics](#start-semantics)
6. [Input: Merge & Ports](#input-merge--ports)
7. [Adapters & Ops](#adapters--ops)
8. [Output & Delivery](#output--delivery)
9. [Variables (`vars`), Params (`params`) & Expressions (`expr`)](#variables-vars-params-params--expressions-expr)
10. [Externals](#externals)
11. [Hooks](#hooks)
12. [Dynamic Graph Features](#dynamic-graph-features)
    - [edges_ex (conditional edges)](#edges_ex-conditional-edges)
    - [foreach / scatter-gather](#foreach--scatter-gather)
    - [barriers & gateways](#barriers--gateways)
13. [Policies & Hints](#policies--hints)
    - [Retry Policy](#retry-policy)
    - [SLA & Deadlines](#sla--deadlines)
    - [Concurrency](#concurrency)
    - [Resources](#resources)
    - [Worker Hints & Checkpointing](#worker-hints--checkpointing)
14. [Observability & Metrics](#observability--metrics)
15. [Defaults & Templates](#defaults--templates)
16. [Feature Flags](#feature-flags)
17. [Compilation to ExecutionPlan](#compilation-to-executionplan)
18. [Validation Rules & Errors](#validation-rules--errors)
19. [Security Notes](#security-notes)
20. [Reserved Names](#reserved-names)
21. [Examples](#examples)
    - [Static Graph Example](#static-graph-example)
    - [Dynamic Graph Example](#dynamic-graph-example)
22. [Appendix A: Expression Language](#appendix-a-expression-language)
23. [Appendix B: Content Descriptor](#appendix-b-content-descriptor)
24. [Appendix C: Built-in Adapters & Actions (Catalog)](#appendix-c-built-in-adapters--actions-catalog)

---

## Overview

The **FlowKit Graph Schema v2.2** is a declarative model that separates:
- **Topology & start conditions** (when a node is allowed to run),
- **Data plane** (inputs, transforms, merge/ports, outputs),
- **Dynamic behavior** (conditional edges, foreach/scatter, barriers/gateways),
- **Policies** (retry, SLA, concurrency, resources),
- **Externals & Hooks** (integrations and lifecycle events),
- **Variables & Expressions** (data-driven orchestration with safe evaluation).

The Coordinator validates and **compiles** the graph into an **ExecutionPlan**. Workers never parse the full graph: they receive pre-validated, normalized `CmdTaskStart` with concrete input/output plans.

---

## Versioning & Compatibility

- `schema_version` is a **string** (`"2.2"` in this document).
- v2.x is **not** backward compatible with v1.x.
- Minor increments (2.1 → 2.2) may add optional fields. A `schema_version_policy` may be introduced in future to allow `warn|fail` on mismatch.

---

## Top-level Document

```json
{
  "schema_version": "2.2",
  "meta": {
    "name": "pipeline-name",
    "description": "Human readable description",
    "owner": "team-x",
    "tags": ["prod", "realtime"]
  },

  "params": { /* runtime parameters available to expressions */ },

  "externals": {
    /* Global externals visible to all nodes (can be overridden locally) */
    "kafka.analytics": { "kind": "kafka.topic", "topic": "analytics.events", "connection": "kafka.default" },
    "webhook.orders": { "kind": "webhook", "path": "/orders/callback", "method": "POST" },
    "secrets.main":  { "kind": "secrets", "provider": "vault", "path": "kv/app" }
  },

  "hooks": [ /* Optional global hooks (see Hooks) */ ],

  "defaults": {
    "start": { "when": "all" },
    "merge": { "strategy": "interleave", "fair": true },
    "retry_policy": { "max": 2, "backoff_sec": 300, "jitter": "50%" },
    "output_delivery": { "semantics": "at_least_once", "idempotency_key": "{{item.meta.uid}}" },
    "concurrency": { "priority": 0, "concurrency_key": null, "limit": null },
    "sla": { "node_timeout_ms": null, "batch_deadline_ms": null }
  },

  "edges_ex": [ /* Optional conditional edges; see Dynamic Graph Features */ ],
  "foreach":  [ /* Optional scatter-gather specifications */ ],

  "templates": {
    /* Optional reusable node templates, referenced by foreach.spawn.template_node */
  },

  "feature_flags": {
    /* Toggle advanced features explicitly, e.g., dynamic_edges, foreach */
    "dynamic_edges": true,
    "foreach": true
  },

  "obs": {
    /* Optional instrumentation policy (sampling, label allowlists) */
    "sampling": {
      "output.sent": 0.1,
      "source.item": 0.01
    }
  },

  "nodes": [ /* Array of NodeSpec objects (see below) */ ]
}
```

---

## NodeSpec

```json
{
  "node_id": "analyze",
  "type": "analyzer",

  "parents": ["index", "enrich"],

  "start": {
    "when": "expr",                           // "all" | "any" | "count:k" | "expr" | "true" | "false"
    "expr": "index.batch || (enrich.done && external.webhook.orders.ready)",
    "gate": { "expr": "vars.ok_to_run == true" },  // optional additional latch (idempotent)
    "approval": {                             // optional human gate
      "required": false,
      "external": "notify.slack.ops",
      "timeout_ms": 900000,
      "on_timeout": "fail"                    // "fail" | "skip" | "auto_approve"
    }
  },

  "io": {
    "input": { /* see Input */ },
    "output": { /* see Output */ }
  },

  "vars": [
    { "op": "set",   "key": "stage",  "value": "online", "when": "node.started" },
    { "op": "merge", "key": "stats",  "value": {"batches":"{{metrics.batches}}"}, "when": "node.finished" },
    { "op": "incr",  "key": "retries","by": 1,           "when": "on_retry" }
  ],

  "retry_policy": {
    "max": 3,
    "backoff_sec": 120,
    "jitter": "30%",
    "permanent_on": ["bad_input","schema_mismatch"],
    "compensate": [
      { "type": "emit.kafka", "args": {"external":"kafka.analytics","topic":"saga","value":{"event":"compensate","node":"{{node.node_id}}"}} }
    ]
  },

  "sla": {
    "node_timeout_ms": 1800000,
    "batch_deadline_ms": 20000,
    "on_violation": [{ "type":"emit.event", "args":{"kind":"SLA_VIOLATION","node":"{{node.node_id}}"}}]
  },

  "concurrency": {
    "priority": 10,
    "concurrency_key": "type:analyzer",
    "limit": 8
  },

  "resources": {
    "cpu_quota": "200ms/batch",
    "mem_watermark_mb": 512
  },

  "externals": {
    /* Local externals that override/extend graph-level externals */
    "notify.slack.ops": { "kind": "slack.webhook", "url": "https://hooks.slack..." }
  },

  "hooks": [
    { "when": { "on": "node.started" },  "actions": [ { "type":"emit.metrics", "args":{"counter":"node_started"} } ] },
    { "when": { "on": "output.sent", "output":"analytics", "every_n": 1000 }, "actions": [
      { "type":"emit.metrics", "args":{"counter":"analytics_sent","n":1000} }
    ]},
    { "when": { "on": "source.error", "source":"from_index" }, "actions": [
      { "type":"emit.kafka", "args": {"external":"kafka.analytics","topic":"alerts","value":{"src":"from_index","err":"{{error.code}}"}} }
    ]}
  ],

  "worker": {
    "prefetch": 2,
    "inflight_queue": 4,
    "autotune_chunk": true,
    "checkpointing": { "scope":"task+node", "retention":"7d" },
    "tracing": { "enabled": false }
  }
}
```

---

## Start Semantics

- `parents`: **array of node_ids**. May be empty when using pure externals or notify streams.
- `start.when`:
  - `"all"`: all parents **done**.
  - `"any"`: any parent **done**.
  - `"count:k"`: at least *k* parents **done**.
  - `"expr"`: boolean expression (see Expression Language) using predicates:
    - `{parent}.done` | `{parent}.failed` | `{parent}.batch` | `{parent}.exists`
    - `external.{name}.ready`
    - `vars.*` and `params.*`
  - `"true"` / `"false"`: explicit always/never (useful with externals-only nodes).
- `start.gate.expr` (optional): extra boolean latch; **must** be idempotent.
- `start.approval` (optional): a human gate with timeout and behavior on timeout.

**Coordinator compiles** `start.when`/`gate.expr` into a normalized AST and tracks required dependencies (`needs[]`).

---

## Input: Merge & Ports

Two modes for `io.input`:

### 1) **Merge mode** (default)

```json
"io": {
  "input": {
    "mode": "merge",
    "merge": { "strategy": "interleave", "fair": true, "max_buffer_per_source": 100 },
    "sources": [ /* homogeneous content within this array */ ]
  }
}
```

- `sources[]`: each source must declare `origin`, `select`, optional `content`, `adapter`, and `ops`.
- All sources **MUST** be content-compatible (see [Content Descriptor](#appendix-b-content-descriptor)); otherwise validation fails.
- `merge.strategy`: `"interleave"` | `"concat"` | `"priority"` (with `priority: ["alias1","alias2"]`).

### 2) **Ports mode** (heterogeneous inputs)

```json
"io": {
  "input": {
    "mode": "ports",
    "selection": {
      "policy": "priority",                 // "priority" | "weighted_fair" | "demand"
      "priority": ["control", "data"],
      "weights": { "data": 3, "control": 1 },
      "max_inflight_per_port": 2
    },
    "ports": {
      "data": {
        "merge": { "strategy": "interleave" },
        "sources": [ /* homogeneous to data port */ ]
      },
      "control": {
        "merge": { "strategy": "concat" },
        "sources": [ /* homogeneous to control port */ ]
      }
    }
  }
}
```

- **Within each port**, sources must be content-compatible.
- **Across ports**, content may differ (e.g., data: JSON records; control: bytes).
- Worker receives `Batch` with `port` and `source_alias` tags; routing can differ per port.

### Source object

```json
{
  "alias": "from_index",
  "origin": { "parent": "index" },             // exactly one of {parent} OR {external}
  "select": "batches",                          // "batches" | "final"
  "content": { "frame": "record", "type": "json", "schema_ref": "doc.v1" },
  "adapter": { "name": "pull.from_artifacts", "args": { "poll_ms": 200 } },
  "ops": [
    { "op": "filter", "args": { "expr": "meta.score >= 0.7" } },
    { "op": "json.map", "args": { "expr": "{id: meta.id, text: payload.text}" } },
    { "op": "dedupe", "args": { "key": "id", "window": "10m" } }
  ]
}
```

- `origin`: **exactly one** of:
  - `{ "parent": "<node_id>" }`
  - `{ "external": "<external-name>" }`
- `select`:
  - `"batches"`: reads partial batches/stream.
  - `"final"`: reads final complete artifacts.
- `content` (recommended): used to validate compatibility and drive decode ops; see [Appendix B](#appendix-b-content-descriptor).
- `adapter`: input adapter + arguments (validated by adapter’s schema).
- `ops[]`: transform pipeline (validated against content).

---

## Adapters & Ops

- **Adapters** are named like `pull.from_artifacts`, `pull.kafka.subscribe`, `push.to_artifacts`, `emit.kafka`, etc.
- Each adapter **declares**:
  - capability flags (e.g., `supports_select: ["batches","final"]`),
  - JSON-Schema for `args`,
  - content compatibility hints (if applicable).

**Transform ops** (`ops[]`) are applied per-source **before** merge/ports selection materializes a batch:

Examples:
- `filter:expr`
- `json.map` (JMESPath/JSONata-lite)
- `map` with field projection
- `dedupe:key`
- `sort:bounded` (requires bounded buffer)
- `compress`, `decompress`, `json.decode`, `csv.to_parquet`

Validation **fails fast** if an op is incompatible with the source `content` or requires missing buffers.

---

## Output & Delivery

```json
"io": {
  "output": {
    "channels": [
      { "name": "artifacts", "adapter": { "name": "push.to_artifacts", "args": {} } },
      { "name": "analytics", "adapter": { "name": "emit.kafka", "args": { "external": "kafka.analytics", "key": "{{item.meta.id}}" } } }
    ],
    "delivery": {
      "semantics": "at_least_once",                   // future: "exactly_once"
      "idempotency_key": "{{item.meta.uid}}",
      "retry": { "max": 5, "backoff_sec": 30, "jitter": "25%" },
      "dlq": { "channel": "artifacts", "bucket": "dlq", "reason_key": "{{error.code}}" }
    },
    "routing": [
      { "when": "item.port == 'control'", "to": ["analytics"] },
      { "when": "item.port == 'data'",    "to": ["artifacts"] }
    ]
  }
}
```

- `channels[]`: named sinks with output adapters. Names referenced by `routing[].to` and `delivery.dlq.channel`.
- `delivery`: delivery semantics, retries, DLQ behavior.
- `routing[]` (optional): expression-based per-batch routing. If omitted, batches go to **all** channels.

---

## Variables (`vars`), Params (`params`) & Expressions (`expr`)

- **`params`**: top-level runtime parameters (immutable), available in expressions as `params.*`.
- **`vars`**: task-level mutable store managed by Coordinator via **safe operations** (idempotent by design).
  - `set`, `merge`, `incr` with a `when` selector (`node.*`, `on_retry`, etc.).
- **Expressions**:
  - Use a constrained language (see [Appendix A](#appendix-a-expression-language)).
  - Can reference `params.*`, `vars.*`, `external.*.ready`, parent predicates (`{parent}.done|failed|batch`), current `item.*` (for routing/ops).

**Best practice:** Keep `vars` low-cardinality and deterministic. Avoid storing large objects.

---

## Externals

Externals declare named integration points that **can be referenced** by adapters, hooks, or expressions:

Examples:
- `kafka.topic` (with `topic`, `connection`)
- `webhook` (with `path`, `method`)
- `http.endpoint` (with `base_url`, `headers_ref`)
- `secrets` (with `provider`, `path`)
- `lock` (distributed locks)
- `ratelimit` (rate limiting windows)
- `slack.webhook`, `telegram.bot`, `email.smtp`
- `cache` (redis/mem)
- `search` (es/opensearch)

**Scope & override**:
- Graph-level (`graph.externals`) act as defaults.
- Node-level (`node.externals`) can override or add entries by the same names.

Validation: every `external` reference in adapters/hooks must exist.

---

## Hooks

Hooks emit side effects or metrics on lifecycle events. They can be defined **globally** and/or **per node**.

**Events** (non-exhaustive):
- `node.scheduled`, `node.started`, `node.finished`, `node.failed`, `on_retry`, `gate.open`
- `source.opened`, `source.item`, `source.eof`, `source.error`
- `merge.started`, `merge.progress`, `merge.finished`
- `output.started`, `output.sent`, `output.error`, `output.finished`

**Hook spec**:
```json
{
  "when": { "on": "<event>", "source": "alias?", "output": "name?", "every_n": 1000?, "throttle": {"seconds": 5}? },
  "actions": [ { "type": "<action>", "args": { /* adapter-like args */ } }, ... ]
}
```

Actions (built-in): `emit.metrics`, `emit.kafka`, `emit.event`, `http.post`, `vars.set|merge|incr`, `secrets.fetch`, `lock.acquire|release`, `ratelimit.enforce`, `audit.log`, `notify.slack|telegram|email`.

**Cardinality policy**: metrics hooks **must** adhere to a fixed label allowlist; IDs are not allowed as labels.

---

## Dynamic Graph Features

### edges_ex (conditional edges)

```json
{
  "from": "A",
  "to": "B",
  "on": "on_done",                 // "on_done" | "on_failed" | "on_batch"
  "when": "vars.stage == 'online'",
  "gateway": "inclusive"           // "inclusive" | "exclusive"
}
```

- Evaluated by Coordinator. If multiple `to` are enabled and `gateway="exclusive"`, **first true** wins (stable order).

### foreach / scatter-gather

```json
{
  "from": "fanout-source",
  "select": "final",                                  // "batches" | "final"
  "items_expr": "$payload.shards[*]",                 // JSONata-lite/JMESPath-like
  "spawn": {
    "template_node": "shard-worker",                  // must exist in templates
    "id_expr": "concat('shard-', $.id)"               // unique node_id
  },
  "gather": {
    "barrier": "all",                                 // "all" | "any" | "count:k"
    "to": "reducer"
  }
}
```

- Coordinator materializes child nodes based on items and links them into the DAG.
- Gathering barrier defines when the reducer may start.

### Barriers & Gateways

- Modeled via `start.when` or `edges_ex.gateway` as inclusive/exclusive branching.
- For more complex control, create explicit “barrier” nodes with no IO.

---

## Policies & Hints

### Retry Policy

```json
"retry_policy": {
  "max": 3,
  "backoff_sec": 120,
  "jitter": "30%",
  "permanent_on": ["bad_input","schema_mismatch"],
  "compensate": [ { "type": "emit.kafka", "args": { /* ... */ } } ]
}
```

- `permanent_on`: reason codes that make failures non-retriable.
- `compensate[]`: optional saga-like compensations (best-effort).

### SLA & Deadlines

```json
"sla": {
  "node_timeout_ms": 1800000,
  "batch_deadline_ms": 20000,
  "on_violation": [ { "type": "emit.event", "args": { /* ... */ } } ]
}
```

- Worker enforces batch deadlines (soft→hard cancel); Coordinator enforces node timeouts.

### Concurrency

```json
"concurrency": {
  "priority": 10,                     // lower = lower priority
  "concurrency_key": "type:analyzer", // shared queue key
  "limit": 8
}
```

### Resources

```json
"resources": {
  "cpu_quota": "200ms/batch",
  "mem_watermark_mb": 512
}
```

### Worker Hints & Checkpointing

```json
"worker": {
  "prefetch": 2,
  "inflight_queue": 4,
  "autotune_chunk": true,
  "checkpointing": { "scope":"task+node", "retention":"7d" },
  "tracing": { "enabled": false }
}
```

---

## Observability & Metrics

- `/metrics` endpoints exposed by Coordinator/Worker (Prometheus).
- Low-cardinality labels only: e.g., `role`, `step_type`, `result` — **no IDs**.
- Hooks provide fine-grained events (`emit.metrics`) with sampling and throttling.
- Alerting targets (examples): stalled scheduling, CAS-skip rate, outbox lag, DLQ growth.

---

## Defaults & Templates

- `defaults`: global fallbacks for nodes (start, merge, retry, delivery, concurrency, SLA).
- `templates`: reusable node skeletons referenced by `foreach.spawn.template_node`.

---

## Feature Flags

- `feature_flags.dynamic_edges`, `feature_flags.foreach`, etc., allow incremental rollout and safer deployments.

---

## Compilation to ExecutionPlan

Coordinator compiles the declarative graph into an explicit **ExecutionPlan** stored in `TaskDoc.execution_plan`:

- `parents_by_child` / `children_by_parent`
- `start_policy[child]`: `{ ast, needs[] }` (normalized start/gate AST + dependency set)
- `inputs[child]`: normalized input plan (merge/ports, sources with compiled ops)
- `outputs[child]`: channels, routing, delivery
- `vars_hooks[child]`: normalized var operations
- `hooks[child]`: merged global+local, partially compiled templates
- `policies[child]`: retry/sla/concurrency/resources/worker
- Optional `dynamic` section for `edges_ex`/`foreach`

Workers do **not** read `ExecutionPlan` directly; they receive a `CmdTaskStart` with baked `input_inline`/`batching` and output plan snippets.

---

## Validation Rules & Errors

**Fast-fail** validation (Pydantic or equivalent):

- `node_id` uniqueness across `nodes[]` and derived `foreach` spawns.
- `parents[]` must reference known nodes.
- `start.when` ∈ {`all`,`any`,`count:k`,`expr`,`true`,`false`}. `count:k` requires `k≥1`.
- `expr` parsing: valid AST; referenced symbols must exist (`parents`, `externals`, `vars`, `params`).
- DAG acyclicity check (Kahn’s algorithm) across static `parents` and materialized dynamic edges known at compile time.
- **Input**:
  - In `merge` mode: all `sources[]` content-compatible.
  - In `ports` mode: sources **within a port** content-compatible; names unique.
  - Exactly one of `origin.parent` or `origin.external` must be set.
  - Adapters known and args schema-validated.
  - `ops[]` supported and content-compatible.
- **Output**:
  - Channel names unique; adapters known; routing references existing channels.
  - `delivery.dlq.channel` exists if `dlq` specified.
- **Externals**: referenced names exist (after node-level override).

**Typical messages** (examples):
- `E_NODE_DUPLICATE_ID: duplicate node_id "index"`
- `E_PARENT_UNKNOWN: node "analyze" depends_on unknown: ["enrich2"]`
- `E_EXPR_PARSE: start.expr at node "analyze": unexpected token near "&&"`
- `E_INPUT_CONTENT_MISMATCH: port "data" mixes types ["json","bytes"]`
- `E_ADAPTER_UNKNOWN: input.adapter "pull.s3.list" not registered`
- `E_OUTPUT_ROUTING_CHANNEL: unknown channel "analytics2" in routing rule #1`

---

## Security Notes

- Secrets must flow via `externals` + `secrets.fetch` hooks; never embed secrets in plain args.
- Logs/metrics must redact sensitive fields (worker/coordinator policy).
- HTTP/Kafka adapters include timeouts, retries, and idempotency hooks by default.

---

## Reserved Names

- Node IDs, source aliases, port names, channel names **must not** start with `_` (reserved for internal use).
- `external.*` and `vars.*` namespaces are reserved in expressions.
- Built-in event names (hooks) are reserved (see [Hooks](#hooks)).

---

## Examples

### Static Graph Example

```json
{
  "schema_version": "2.2",
  "meta": { "name": "simple-pipeline", "owner": "search-team" },
  "params": { "min_score": 0.7 },

  "nodes": [
    { "node_id": "index", "type": "indexer" },

    {
      "node_id": "analyze",
      "type": "analyzer",
      "parents": ["index"],
      "start": { "when": "all" },
      "io": {
        "input": {
          "mode": "merge",
          "merge": { "strategy": "interleave" },
          "sources": [{
            "alias": "from_index",
            "origin": { "parent": "index" },
            "select": "batches",
            "content": { "frame": "record", "type": "json", "schema_ref": "doc.v1" },
            "adapter": { "name": "pull.from_artifacts", "args": { "poll_ms": 200 } },
            "ops": [
              { "op": "filter", "args": { "expr": "meta.score >= params.min_score" } },
              { "op": "json.map", "args": { "expr": "{id: meta.id, text: payload.text}" } }
            ]
          }]
        },
        "output": {
          "channels": [
            { "name": "artifacts", "adapter": { "name": "push.to_artifacts", "args": {} } }
          ],
          "delivery": { "semantics": "at_least_once", "idempotency_key": "{{item.meta.uid}}" }
        }
      }
    }
  ]
}
```

### Dynamic Graph Example

```json
{
  "schema_version": "2.2",
  "meta": { "name": "dynamic-pipeline", "owner": "ml-platform" },
  "params": { "stage": "online" },

  "externals": {
    "kafka.analytics": { "kind": "kafka.topic", "topic": "analytics.events", "connection": "kafka.default" },
    "kafka.reloads":   { "kind": "kafka.topic", "topic": "model.reloads",   "connection": "kafka.default" }
  },

  "edges_ex": [
    { "from": "index", "to": "rank", "on": "on_done", "when": "params.stage == 'online'", "gateway": "inclusive" },
    { "from": "index", "to": "validate", "on": "on_done", "when": "params.stage != 'online'", "gateway": "inclusive" }
  ],

  "foreach": [{
    "from": "fanout-source",
    "select": "final",
    "items_expr": "$payload.shards[*]",
    "spawn": { "template_node": "shard-worker", "id_expr": "concat('shard-', $.id)" },
    "gather": { "barrier": "all", "to": "reducer" }
  }],

  "templates": {
    "shard-worker": {
      "type": "worker",
      "io": {
        "input": {
          "mode": "merge",
          "merge": { "strategy": "interleave" },
          "sources": [{
            "alias": "shard-input",
            "origin": { "external": "kafka.analytics" },
            "select": "batches",
            "content": { "frame": "record", "type": "json" },
            "adapter": { "name": "pull.kafka.subscribe", "args": { "group": "flowkit-shard" } }
          }]
        },
        "output": {
          "channels": [{ "name": "artifacts", "adapter": { "name": "push.to_artifacts", "args": {} } }],
          "delivery": { "semantics": "at_least_once" }
        }
      }
    }
  },

  "nodes": [
    { "node_id": "index", "type": "indexer" },

    {
      "node_id": "rank",
      "type": "ranker",
      "parents": ["index"],
      "start": { "when": "expr", "expr": "index.done || external.kafka.reloads.ready" },
      "io": {
        "input": {
          "mode": "ports",
          "selection": { "policy": "priority", "priority": ["control","data"], "max_inflight_per_port": 1 },
          "ports": {
            "data": {
              "merge": { "strategy": "interleave" },
              "sources": [{
                "alias": "from_index",
                "origin": { "parent": "index" },
                "select": "batches",
                "content": { "frame":"record", "type":"json", "schema_ref":"doc.v1" },
                "adapter": { "name": "pull.from_artifacts", "args": { "poll_ms": 200 } }
              }]
            },
            "control": {
              "merge": { "strategy": "concat" },
              "sources": [{
                "alias": "ext_kafka_raw",
                "origin": { "external": "kafka.reloads" },
                "select": "batches",
                "content": { "frame":"bytes", "type":"bytes" },
                "adapter": { "name": "pull.kafka.subscribe", "args": { "group":"flowkit-ctl" } },
                "ops": [{ "op":"json.decode" }, { "op":"filter", "args":{"expr":"payload.kind=='MODEL_RELOAD'"}}]
              }]
            }
          }
        },
        "output": {
          "channels": [
            { "name": "artifacts", "adapter": { "name": "push.to_artifacts", "args": {} } },
            { "name": "events",    "adapter": { "name": "emit.kafka", "args": { "external": "kafka.analytics" } } }
          ],
          "routing": [
            { "when": "item.port == 'control'", "to": ["events"] },
            { "when": "item.port == 'data'",    "to": ["artifacts"] }
          ],
          "delivery": { "semantics": "at_least_once", "idempotency_key": "{{item.meta.uid}}" }
        }
      }
    },

    { "node_id": "validate", "type": "validator", "parents": ["index"], "start": { "when": "all" } },

    { "node_id": "fanout-source", "type": "collector", "parents": ["index"] },

    { "node_id": "reducer", "type": "reduce", "parents": [] }
  ]
}
```

---

## Appendix A: Expression Language

**Goal:** minimal, safe, deterministic; no arbitrary code execution.

### Syntax (EBNF-like)

```
expr       := or_expr
or_expr    := and_expr { "||" and_expr }*
and_expr   := not_expr { "&&" not_expr }*
not_expr   := [ "!" ] primary
primary    := literal
            | "(" expr ")"
            | comparison
            | function_call
comparison := value ( "==" | "!=" | ">" | "<" | ">=" | "<=" ) value
value      := identifier | literal | template
literal    := "true" | "false" | number | string | null
identifier := [a-zA-Z_][a-zA-Z0-9_\.]*
template   := "{{" [^}]+"}}"     // handlebars-like, resolved before eval
function_call := name "(" [arglist] ")"
arglist    := expr { "," expr }*
```

### Namespaces

- `params.*`, `vars.*`
- `external.<name>.ready`
- `<parent>.done|failed|batch|exists`
- For routing/ops: `item.*`, `meta.*`, `payload.*`

### Functions (whitelist)

- `in(x, list...)`
- `len(x)`
- `starts_with(s, prefix)`
- `regex_match(s, pattern)`
- `coalesce(a, b, ...)`

Unsupported references or types → **validation error**.

---

## Appendix B: Content Descriptor

```json
{ "frame": "record" | "bytes" | "file" | "kv", "type": "json"|"avro"|"bytes"|"csv"|"parquet"|"...", "schema_ref": "optional-id" }
```

- **frame** describes the envelope at batch item level.
- **type** refines codec/format.
- `schema_ref` is an opaque identifier for downstream validation/transforms.

Adapters & ops can define which content they **support**. Validation ensures compatibility upfront.

---

## Appendix C: Built-in Adapters & Actions (Catalog)

**Input (pull/notify):**
- `pull.from_artifacts`, `pull.from_artifacts.rechunk:size|bytes`
- `pull.kafka.subscribe`
- `pull.s3.list+get`, `pull.gcs.list+get`
- `pull.http.paginate`
- `pull.sql.cursor`, `pull.mongo.cursor`
- `notify.webhook`

**Transform ops:**
- `filter:expr`, `json.map`, `map` (projection), `dedupe:key`
- `sort:bounded`, `compress`, `decompress`, `json.decode`, `csv.to_parquet`

**Output/Sink:**
- `push.to_artifacts`, `emit.kafka`, `emit.s3`, `emit.gcs`, `upsert.merged`

**Coordinator/Ops:**
- `artifacts.merge.docs`, `artifacts.compact`, `artifacts.validate.schema`
- `metrics.thresholds.check`, `dq.checks.run`
- `artifacts.route.by_meta`, `graph.spawn_children`, `vars.set|merge|incr`

**Hook actions:**
- `emit.metrics`, `emit.kafka`, `emit.event`, `http.post`
- `vars.*`, `secrets.fetch`, `lock.*`, `ratelimit.enforce`, `audit.log`
- `notify.slack|telegram|email`
