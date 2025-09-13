# Error handling & retries

FlowKit separates **configuration/programming errors (permanent)** from **operational errors (transient)** so the Coordinator can retry only when it makes sense.

## Classification rules

### Worker-side normalization

- **Unknown input adapter** → `TASK_FAILED`, `reason_code="bad_input_adapter"`, `permanent=True`.
- **Missing/invalid adapter args** → `TASK_FAILED`, `reason_code="bad_input_args"`, `permanent=True`.

Handlers can also classify errors using `RoleHandler.classify_error(exc)`:

```python
def classify_error(self, exc: BaseException) -> tuple[str, bool]:
    if isinstance(exc, (TypeError, ValueError, KeyError)):
        return ("bad_input", True)         # permanent
    return ("unexpected_error", False)     # transient
```

Use this default or refine it for your domain.

### Coordinator retry policy

Coordinator retries **only transient** failures according to per-node policy (max attempts, backoff, etc.). Permanent failures finalize the node immediately.

## Visibility & observability

Workers emit structured logs for input routing and failures:

- `worker.adapter.selected` → adapter name, normalized args, and `io_source` (`cmd|handler`).
- `worker.adapter.handler_conflict_ignored` → when cmd overrides handler suggestion.
- `TASK_FAILED` envelopes include `reason_code`, `permanent`, and `error` message.

This makes misconfiguration obvious during pre-flight instead of failing deep inside background tasks.

## Empty upstream is not an error

If routes/args are valid but upstream has no data, the worker completes normally (`count=0`). This prevents flapping retries on empty streams.

## Examples

**Unknown adapter in cmd**
```json
{
    "io": {"input_inline": {"input_adapter": "does.not.exist", "input_args": {}}}
}
```
→ `TASK_FAILED` / `bad_input_adapter` (permanent).

**Missing args for rechunk**
```json
{
    "io": {
    "input_inline": {
        "input_adapter": "pull.from_artifacts.rechunk:size",
        "input_args": {"from_nodes": ["u"]}
    }
    }
}
```
If your product policy requires `meta_list_key`, enforce it at the coordinator (strict mode). The worker core is lenient by default and will treat each artifact meta as a single logical item.

## Tips

- Keep classification **stable**: don’t flip an error from transient to permanent across releases without a migration plan.
- Attach **actionable text** in `error` field for operator runbooks.
