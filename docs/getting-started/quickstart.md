# Quick Start

This guide will walk you through creating your first FlowKit pipeline in just a few minutes.

## Prerequisites

- FlowKit installed ([Installation Guide](installation.md))
- Kafka running on `localhost:9092`
- MongoDB running on `localhost:27017`

## Step 1: Create a Simple Handler

First, create a custom handler that will process your data:

```python
# handlers.py
import asyncio
from typing import AsyncIterator
from flowkit.worker.handlers.base import Handler, Batch, BatchResult, RunContext

class EchoHandler(Handler):
    """A simple handler that echoes input data."""

    async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
        """Generate batches from input data."""
        data = input_data.get("input_inline", {}).get("message", "Hello World")
        yield Batch(
            batch_uid="batch_1",
            payload={"message": data, "processed_at": "now"}
        )

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        """Process a single batch."""
        message = batch.payload.get("message", "")
        processed_message = f"Echo: {message.upper()}"

        print(f"Processing: {processed_message}")

        # Simulate some work
        await asyncio.sleep(0.1)

        return BatchResult(
            success=True,
            metrics={"messages_processed": 1},
            artifacts_ref={"output": processed_message}
        )
```

## Step 2: Set Up Database Connection

```python
# database.py
from motor.motor_asyncio import AsyncIOMotorClient

async def get_database():
    """Get MongoDB database connection."""
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    return client.flowkit_db
```

## Step 3: Create Your First Pipeline

```python
# pipeline.py
import asyncio
from flowkit import Coordinator, CoordinatorConfig, WorkerConfig
from flowkit.worker import Worker
from handlers import EchoHandler
from database import get_database

async def run_pipeline():
    # Get database connection
    db = await get_database()

    # Configure coordinator
    coord_config = CoordinatorConfig(
        kafka_bootstrap="localhost:9092",
        worker_types=["echo"]
    )

    # Configure worker
    worker_config = WorkerConfig(
        kafka_bootstrap="localhost:9092",
        roles=["echo"]
    )

    # Start coordinator
    coordinator = Coordinator(db=db, cfg=coord_config)
    await coordinator.start()
    print("✅ Coordinator started")

    # Start worker
    worker = Worker(
        db=db,
        cfg=worker_config,
        handlers={"echo": EchoHandler()}
    )
    await worker.start()
    print("✅ Worker started")

    try:
        # Create a simple task graph
        graph = {
            "nodes": [
                {
                    "node_id": "echo_task",
                    "type": "echo",
                    "depends_on": [],
                    "io": {
                        "input_inline": {
                            "message": "Hello FlowKit!"
                        }
                    }
                }
            ],
            "edges": []
        }

        # Create and execute task
        task_id = await coordinator.create_task(
            params={"pipeline_name": "quickstart"},
            graph=graph
        )
        print(f"✅ Created task: {task_id}")

        # Wait for completion (in real apps, you'd monitor differently)
        await asyncio.sleep(5)

        # Check task status
        task_doc = await db.tasks.find_one({"id": task_id})
        print(f"✅ Task status: {task_doc['status']}")

        # Check artifacts
        artifacts = await db.artifacts.find({"task_id": task_id}).to_list(10)
        print(f"✅ Artifacts created: {len(artifacts)}")

    finally:
        # Clean shutdown
        await worker.stop()
        await coordinator.stop()
        print("✅ Pipeline shutdown complete")

if __name__ == "__main__":
    asyncio.run(run_pipeline())
```

## Step 4: Run Your Pipeline

```bash
python pipeline.py
```

Expected output:
```
✅ Coordinator started
✅ Worker started
✅ Created task: 12345678-1234-1234-1234-123456789abc
Processing: Echo: HELLO FLOWKIT!
✅ Task status: finished
✅ Artifacts created: 1
✅ Pipeline shutdown complete
```

## Step 5: Create a Multi-Stage Pipeline

Now let's create a more complex pipeline with multiple stages:

```python
# complex_pipeline.py
import asyncio
from flowkit import Coordinator, CoordinatorConfig, WorkerConfig
from flowkit.worker import Worker
from handlers import EchoHandler
from database import get_database

class ProcessorHandler(Handler):
    """Processes data from previous stage."""

    async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
        # Use input adapter to pull from artifacts
        input_adapter = input_data.get("input_inline", {}).get("input_adapter")
        if input_adapter == "pull.from_artifacts":
            # This would pull from upstream artifacts
            # For simplicity, we'll simulate
            yield Batch(
                batch_uid="process_batch_1",
                payload={"data": "processed_data", "stage": "processor"}
            )

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        data = batch.payload.get("data", "")
        result = f"Processed: {data}"

        print(f"Stage 2 - {result}")
        await asyncio.sleep(0.1)

        return BatchResult(
            success=True,
            metrics={"items_processed": 1},
            artifacts_ref={"result": result}
        )

async def run_complex_pipeline():
    db = await get_database()

    coord_config = CoordinatorConfig(
        kafka_bootstrap="localhost:9092",
        worker_types=["echo", "processor"]
    )

    worker_config = WorkerConfig(
        kafka_bootstrap="localhost:9092",
        roles=["echo", "processor"]
    )

    coordinator = Coordinator(db=db, cfg=coord_config)
    await coordinator.start()

    worker = Worker(
        db=db,
        cfg=worker_config,
        handlers={
            "echo": EchoHandler(),
            "processor": ProcessorHandler()
        }
    )
    await worker.start()

    try:
        # Multi-stage graph
        graph = {
            "nodes": [
                {
                    "node_id": "stage1",
                    "type": "echo",
                    "depends_on": [],
                    "io": {"input_inline": {"message": "Input data"}}
                },
                {
                    "node_id": "stage2",
                    "type": "processor",
                    "depends_on": ["stage1"],
                    "io": {
                        "input_inline": {
                            "input_adapter": "pull.from_artifacts",
                            "input_args": {"from_nodes": ["stage1"]}
                        }
                    }
                }
            ],
            "edges": [["stage1", "stage2"]]  # stage1 -> stage2
        }

        task_id = await coordinator.create_task(params={}, graph=graph)
        print(f"✅ Multi-stage task created: {task_id}")

        await asyncio.sleep(10)  # Wait for completion

        task_doc = await db.tasks.find_one({"id": task_id})
        print(f"✅ Final status: {task_doc['status']}")

    finally:
        await worker.stop()
        await coordinator.stop()

if __name__ == "__main__":
    asyncio.run(run_complex_pipeline())
```

## Next Steps

- Learn about [Basic Concepts](concepts.md)
- Explore [Task Graphs](../guide/graphs.md) in detail
- Check out more [Examples](../examples/simple-pipeline.md)
- Read about [Error Handling](../guide/error-handling.md)
