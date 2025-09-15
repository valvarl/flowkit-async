# flowkit-async *(temporary name)*

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Status: Alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#status)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-brightgreen.svg)](./LICENSE)
[![AsyncIO](https://img.shields.io/badge/runtime-asyncio-4b8bbe.svg)](https://docs.python.org/3/library/asyncio.html)
[![Docs: MkDocs](https://img.shields.io/badge/docs-MkDocs-0052cc.svg)](./docs/)
[![Kafka](https://img.shields.io/badge/infra-Kafka-black.svg)](https://kafka.apache.org/)
[![MongoDB](https://img.shields.io/badge/storage-MongoDB-47A248.svg)](https://www.mongodb.com/)

**flowkit-async** is a tiny async orchestration backbone that grew out of another pet project.
Fun fact: it started with **zero prior exposure to orchestration frameworks** — built from first principles.
It speaks Kafka, stores state in MongoDB, and does **streaming-by-batches**.

## Why this exists

- I needed a pragmatic, async-first backbone to run small DAGs and stream processing without committing to a massive platform.
- Requirements were simple but strict: **batch streaming**, **resilient retries**, **cooperative cancel**, and **observable execution**.
- The project grew from an internal need; if it helps you too — great!

> **Temporary name:** `flowkit-async`. The naming may change later.

---

## Highlights

- **Async DAG** over Kafka topics: coordinator + workers.
- **Streaming batches** with *partial artifacts* → early downstream starts.
- **Leases & adopt**: discover running work and continue without restart.
- **Cooperative cancel** and **retries** with backoff.
- **Start-on-first-batch** and per-edge streaming triggers.
- **Simple contracts**: `RoleHandler` (your work) + `ArtifactsWriter` (partial/complete).
- A couple of coordinator-side helpers out of the box: `merge.generic`, `metrics.aggregate`.
- Built with **Python 3.11+**, `asyncio`, Motor, aiokafka.

> Docs are pinned in the repo (MkDocs). See `./docs/` while we shape the API.

---

## Try it (hello, echo)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
# make sure Kafka & MongoDB are reachable (docker-compose or local)
```

```python
# minimal pseudocode
import asyncio
from flowkit.worker import Worker
from flowkit.coordinator import Coordinator

async def main():
    worker = Worker(db=<mongo>, roles=["echo"])
    coord = Coordinator(db=<mongo>, worker_types=["echo"])
    await coord.start(); await worker.start()

    task_id = await coord.create_task(params={}, graph={
        "nodes": [{"node_id":"n1","type":"echo","status":"queued"}], "edges":[]
    })

    await asyncio.sleep(3)
    await worker.stop(); await coord.stop()

asyncio.run(main())
```

---

## Status & roadmap

**Alpha.** Expect rapid changes. Tracked in GitHub issues/epics: concurrency & backpressure, reliability & DLQ, deadlines & resource control, observability, checkpointing/progress, input/transform/output adapters, shutdown/resume, security & ops controls, handler SDK ergonomics, testing/benchmarks, config & feature flags.

---

## Contributing

Issues and PRs welcome! Please keep labels consistent (`type:*`, `area:*`, `priority:*`). Small proposals first are appreciated.

---

## License

Licensed under the **Apache License, Version 2.0**. See [`LICENSE`](./LICENSE).

---

## Backstory

This project began as the orchestration backbone for another pet project. I’d never used an orchestration framework before — so this is a fresh take with a focus on **small surface area**, **observable behavior**, and **streaming-by-batches**.
