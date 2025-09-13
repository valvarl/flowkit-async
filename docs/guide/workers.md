# Workers

Workers are the execution engine of FlowKit. They execute handler code, stream batches, and report status back to the Coordinator.

This page documents the **orchestrator-first contract**, input adapter precedence, deterministic rechunking, and error visibility that make worker behavior predictable and easy to operate.

## Overview

A Worker:

- **Executes tasks** via user-provided `RoleHandler`s.
- **Streams input** either through **input adapters** (pulling from upstream artifacts) or via the handler’s own `iter_batches` fallback.
- **Reports progress** to Kafka status topics.
- **Handles cancellation** quickly and cooperatively.
- **Persists artifacts** and emits metrics.

## Quick start

```python
from flowkit.worker.runner import Worker, WorkerConfig
from flowkit.worker.handlers.base import RoleHandler, Batch, BatchResult

class MyHandler(RoleHandler):
    role = "processor"

    async def iter_batches(self, loaded):
        yield Batch(batch_uid="b1", payload={"items": [1, 2, 3]})

    async def process_batch(self, batch, ctx):
        return BatchResult(success=True, metrics={"count": len(batch.payload.get("items", []))})

worker = Worker(db=my_db, cfg=WorkerConfig(roles=["processor"]), handlers={"processor": MyHandler()})
await worker.start()
```

## Input routing: the orchestrator-first contract

There is **one source of truth** for the input route: the Coordinator.

**Adapter selection precedence**

1. **If the Coordinator provides** `cmd.input_inline.input_adapter` → the worker **must use it**.
2. **Else**, the worker **may** use a handler-proposed adapter from `load_input()` **if it is known**.
3. **Else**, the worker **falls back** to `handler.iter_batches()`.

Additional rules:

- Unknown adapter in cmd → **permanent `TASK_FAILED`** with reason `bad_input_adapter`.
- Missing/invalid adapter args → **permanent `TASK_FAILED`** with reason `bad_input_args` (no hidden `TypeError`).
- We accept the alias **`from_node`** and normalize it to `from_nodes=[...]`.
- The core is **domain-agnostic**: it never guesses schema keys like `skus` or `items`.

### Example: Coordinator wins over handler

```json
{
  "io": {
    "start_when": "first_batch",
    "input_inline": {
      "input_adapter": "pull.from_artifacts.rechunk:size",
      "input_args": {"from_nodes": ["u"], "size": 2, "meta_list_key": "skus"}
    }
  }
}
```
If a handler returns a different route from `load_input()`, it is **ignored** and the route above is used.

## Adapters

### `pull.from_artifacts`
Pull partial upstream artifacts as they are produced.

```json
{
  "io": {
    "input_inline": {
      "input_adapter": "pull.from_artifacts",
      "input_args": {"from_nodes": ["u"], "poll_ms": 50}
    }
  }
}
```

### `pull.from_artifacts.rechunk:size`
Deterministically reshape upstream artifact **meta** to fixed-size slices.

#### Key semantics (deterministic, schema-agnostic)

- `meta_list_key` is **optional**.
  - If provided **and** `meta[meta_list_key]` is a list → chunk that list.
  - If not provided (or key is not a list) → **treat each artifact meta as a single logical item** (`items=[meta]`).
- No heuristics over domain keys. No guessing `skus|items|…`.
- In strict installations, you may require `meta_list_key` via a product policy flag (outside of worker core).

```json
{
  "io": {
    "input_inline": {
      "input_adapter": "pull.from_artifacts.rechunk:size",
      "input_args": {
        "from_node": "u",
        "size": 3,
        "poll_ms": 25,
        "meta_list_key": "skus"
      }
    }
  }
}
```

> **Empty upstream is not an error.** If the route/args are valid but there is no data, the node finishes normally (`count=0`).

## Handler interface

```python
from typing import AsyncIterator
from flowkit.worker.handlers.base import RoleHandler, Batch, BatchResult, FinalizeResult

class MyRole(RoleHandler):
    role = "probe"

    async def init(self, run_info: dict):  # optional
        pass

    async def load_input(self, input_ref, input_inline):
        # May return a *suggestion* for adapter when cmd doesn't provide one
        return {"input_ref": input_ref or {}, "input_inline": input_inline or {}}

    async def iter_batches(self, loaded) -> AsyncIterator[Batch]:
        # Used only when no adapter is selected
        yield Batch(batch_uid="b1", payload={"items": [1, 2]})

    async def process_batch(self, batch: Batch, ctx) -> BatchResult:
        return BatchResult(success=True, metrics={"count": len(batch.payload.get("items", []))})

    async def finalize(self, ctx) -> FinalizeResult | None:
        return FinalizeResult(metrics={})

    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        # Default behavior in core:
        #   TypeError/ValueError/KeyError -> permanent ("bad_input")
        #   otherwise -> transient ("unexpected_error")
        if isinstance(exc, (TypeError, ValueError, KeyError)):
            return ("bad_input", True)
        return ("unexpected_error", False)
```

## Validation & observability

The worker **validates** adapter names and arguments before streaming:

- Unknown adapter → `TASK_FAILED` (`bad_input_adapter`, permanent).
- Bad/missing args → `TASK_FAILED` (`bad_input_args`, permanent).
- Structured logs include:
  - `adapter.selected` (name, args, source=`cmd|handler`)
  - `handler_conflict_ignored` when cmd overrides handler suggestion

## Cancellation

Workers check `ctx.cancel_flag` between batches and within long operations and transition to `cancelling` quickly on:

1. Coordinator **CANCEL** signal
2. DB cancel flags
3. Lease/heartbeat policy
4. Graceful shutdown

## Metrics & artifacts

Handlers return metrics with each `BatchResult`. The worker upserts partial artifact rows and marks completion at the end, attaching final metrics.

## Configuration (essentials)

```python
from flowkit.worker.runner import WorkerConfig

cfg = WorkerConfig(
    kafka_bootstrap="kafka:9092",
    roles=["indexer", "probe"],
    lease_ttl_sec=60,
    hb_interval_sec=20,
    pull_poll_ms_default=300,
    pull_empty_backoff_ms_max=4000,
)
```

Load with overrides:

```python
WorkerConfig.load(path="configs/worker.default.json", overrides={"roles": ["probe"]})
```

## Best practices

- Keep handlers stateless; put state/offsets in artifacts or DB.
- Classify errors carefully; return **permanent** for configuration mistakes.
- Don’t encode domain semantics in core—use `meta_list_key` where needed.
- Prefer `start_when=first_batch` for true streaming, otherwise wait for upstream completion.

## See also

- [Reference → Worker](../reference/worker.md)
- [Guide → Error handling](error-handling.md)
- [Getting started → Concepts](../getting-started/concepts.md)
