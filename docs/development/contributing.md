# Contributing to FlowKit‑Async

Thank you for taking the time to contribute! This guide sets the ground rules for code, tests, docs, and CI so that changes stay reliable and fast.

> Short on time? See the **TL;DR checklist** below.

---

## TL;DR checklist

- Use **Python 3.11 or 3.12**.
- Install dev deps: `pip install -e .[test]`.
- Run locally before pushing:
  - `ruff check src tests` (lint)
  - `black --check src tests` (format)
  - `mypy src` (types)
  - `pytest -n auto -q` (tests, parallel)
- Write **Google‑style docstrings** for public APIs; don’t duplicate types already in annotations.
- Prefer **precise typing** (`collections.abc`, `dict[str, Any]` only at edges).
- Async code must be **cancellation‑safe** and non‑blocking.
- Keep PRs **small and focused**; link issues; add tests & docs.

---

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .[test]
# optional but recommended
pip install pre-commit && pre-commit install
```

### Useful commands

```bash
ruff check src tests
black --check src tests
mypy src
pytest -n auto -q
pytest -n auto --count=10 -q  # stress/flakiness check
mkdocs serve                   # docs preview on http://localhost:8000
```

---

## Branching, commits, and PRs

- Branch names: `feature/<slug>`, `fix/<slug>`, `chore/<slug>`.
- Keep PRs < ~400 lines of diff when possible; split large changes.
- Reference issues (`Fixes #123`) and **include tests**.
- Commit messages: Conventional Commits **recommended** (not strictly required):
  - `feat(worker): cooperative cancellation for adapter`
  - `fix(coordinator): fence duplicate lease renewals`
  - `test(chaos): restart coordinator mid-stream`
  - `docs(getting-started): clarify quickstart prerequisites`

---

## Code style

### Docstrings: Google style

Use Google‑style docstrings for public modules, classes, and functions. Don’t repeat types already expressed in annotations.

```python
from typing import Iterable

def stable_hash(items: Iterable[str]) -> str:
    """Return a content‑stable hex digest for an iterable of strings.

    Args:
        items: Strings to hash in order.

    Returns:
        Hexadecimal SHA1 digest.
    """
    ...
```

Sections you may use: **Args**, **Returns**, **Raises**, **Yields**, **Examples**, **Notes**. Keep docstrings concise and focused on behavior and invariants rather than implementation.

### Typing policy

- Target **Python 3.11+** syntax (`list[str]`, `X | None`, `typing.Self`).
- Prefer **`collections.abc`** for abstract containers: `Iterable`, `Mapping`, `Sequence`.
- Use `dict[str, Any]` only at **system boundaries** (I/O, JSON, DB). Convert to typed structures internally.
- For protocol payloads and wire messages use **Pydantic v2** models (e.g., `Envelope`) to validate and serialize.
- For internal immutable data consider `@dataclass(frozen=True)`.
- Avoid `Any` unless there is a hard boundary. If used, document why.
- Always annotate **async** functions and **return types** explicitly.

### Formatting & linting

- **Black** enforces formatting; **Ruff** enforces lint rules.
- Ruff is the source of truth for style checks; follow `ruff.toml` in the repo.
- Keep imports sorted (Ruff’s `I` rules) and avoid unused code (`F401`, etc.).
- Lines should generally be ≤ 120 characters (CI enforces lints; docs may be stricter).

### Logging

- Prefer **structured logs**. Workers use `_json_log(clock, **kv)` for consistent, parseable output.
- Tests may use `tests.helpers.dbg()` for human‑readable traces.

### Async & cancellation

- Never block the event loop. Use `await` for I/O and `asyncio.to_thread` for CPU‑bound work (sparingly).
- Handle `asyncio.CancelledError` by **propagating** (re‑raise) unless you are performing cleanup.
- Long‑running loops must check **cancel flags** (`ctx.cancel_flag.is_set()`) and yield (`await asyncio.sleep(…)`).
- When catching broad exceptions, **classify** and surface them via handler error APIs (see `RoleHandler.classify_error`).

---

## Tests

We use **pytest** with asyncio, randomized order, and parallelism.

### Quick start

```bash
pytest -n auto -q
```

### Conventions

- Name files `test_*.py` and mark async tests with `@pytest.mark.asyncio`.
- Use the helpers in `tests.helpers`:
  - **Graph helpers**: `prime_graph`, `wait_task_finished`, `wait_node_running`, `node_by_id`.
  - **In-memory DB & bus** fixtures for isolated tests.
- Chaos/resiliency scenarios are marked with `@pytest.mark.chaos` and may use small sleeps; keep them deterministic otherwise.
- Every new feature/change should include **unit tests**; integration/chaos tests as applicable.

### Coverage

Coverage is reported in CI. Try to keep coverage **steady or improving**; focus on critical paths (coordinator scheduling, worker heartbeats, adapters, and cancellation).

---

## CI (what must pass)

The repository ships a CI workflow that runs on Linux with Python **3.11** and **3.12**:

- **Lint**: `ruff`, `black --check`, `mypy`.
- **Tests**: `pytest -n auto --count=10` with timeouts and JUnit + coverage reports.
- Optional **flake-hunt** mode can repeat tests in random order to chase flakes.
- Docs build must succeed (`mkdocs build --strict`) in the docs workflow.

If CI is red, please reproduce locally and push a fix or mark a flaky test with `@pytest.mark.flaky(reruns=...)` as a **temporary** measure with a tracking issue.

---

## Documentation

Docs live under `docs/` and are built with **MkDocs** (Material theme).

- Keep reference pages in sync when changing public APIs (protocol messages, coordinator/worker interfaces).
- Prefer short, task‑oriented pages. Put long rationale into `development/architecture.md`.
- Run `mkdocs serve` locally to preview. Do **not** commit the built `site/` folder; GitHub Actions deploys Pages automatically.

---

## Security & secrets

- Never commit credentials or tokens. Use environment variables in examples.
- Treat logs as potentially sensitive; avoid dumping full payloads unless sanitized.

---

## Design principles (project‑specific)

- **Explicit states**: transitions go through `RunState` and are visible in the task document.
- **Exactly‑once intent** for outbox/bus: deduplicate via `fp` (fingerprint), idempotent Kafka producers where applicable.
- **Cooperative cancellation**: make workers stop promptly and safely when signaled (signals topic, DB flags).
- **Resilient batching**: partial artifacts per batch, final “complete” artifact on finalize.

---

## Getting help

- Open a **Draft PR** early to discuss design.
- Use Discussions/Issues for proposals and bug reports.
- Ping maintainers in the PR if you need a review on a deadline.

Thanks again for contributing ❤️
