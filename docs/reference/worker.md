# Worker API Reference

## Worker Class

::: flowkit.worker.runner.Worker
    options:
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - start
        - stop

## Handlers

::: flowkit.worker.handlers.base.RoleHandler
    options:
      show_root_heading: true
      show_source: true
      docstring_section_style: list

::: flowkit.worker.handlers.base.Batch
    options:
      show_root_heading: true
      show_source: true

::: flowkit.worker.handlers.base.BatchResult
    options:
      show_root_heading: true
      show_source: true

::: flowkit.worker.handlers.base.FinalizeResult
    options:
      show_root_heading: true
      show_source: true

## Worker Context

::: flowkit.worker.context
    options:
      show_root_heading: true
      show_source: true

## Worker State

::: flowkit.worker.state
    options:
      show_root_heading: true
      show_source: true

## Artifacts

::: flowkit.worker.artifacts
    options:
      show_root_heading: true
      show_source: true

---

## Behavior contract (summary)

- **Adapter precedence**: `cmd.input_inline.input_adapter` (Coordinator) → handler suggestion → `iter_batches` fallback.
- **Validation**: unknown adapter → `bad_input_adapter` (permanent); bad args → `bad_input_args` (permanent).
- **Aliases**: `from_node` is accepted and normalized to `from_nodes=[...]`.
- **Rechunk**: with `meta_list_key` (list) chunk that list; otherwise each artifact meta is a single logical item.
