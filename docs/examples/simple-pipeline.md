# Simple Pipeline Example

This example demonstrates a basic three-stage ETL pipeline: Extract → Transform → Load.

## Complete Example

```python
import asyncio
from typing import AsyncIterator
from motor.motor_asyncio import AsyncIOMotorClient

from flowkit import Coordinator, CoordinatorConfig, WorkerConfig
from flowkit.worker import Worker
from flowkit.worker.handlers.base import Handler, Batch, BatchResult, RunContext

# Mock data source
SAMPLE_DATA = [
    {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
    {"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
    {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
    {"id": 4, "name": "Diana", "age": 28, "city": "Seattle"},
    {"id": 5, "name": "Eve", "age": 32, "city": "Boston"},
]

class ExtractHandler(Handler):
    """Extract data from source."""

    async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
        batch_size = input_data.get("input_inline", {}).get("batch_size", 2)

        for i in range(0, len(SAMPLE_DATA), batch_size):
            batch_data = SAMPLE_DATA[i:i + batch_size]
            yield Batch(
                batch_uid=f"extract_batch_{i // batch_size}",
                payload={"records": batch_data}
            )

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        records = batch.payload["records"]
        print(f"📥 Extracted {len(records)} records")

        # Simulate extraction work
        await asyncio.sleep(0.1)

        return BatchResult(
            success=True,
            metrics={"records_extracted": len(records)},
            artifacts_ref={"data": records, "stage": "extract"}
        )

class TransformHandler(Handler):
    """Transform extracted data."""

    async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
        # Use input adapter to pull from extract stage
        input_args = input_data.get("input_inline", {}).get("input_args", {})
        from_nodes = input_args.get("from_nodes", [])

        # In real implementation, this would pull from artifacts DB
        # For demo, we'll simulate pulling transformed data
        if "extract" in from_nodes:
            # Simulate pulling from artifacts
            for i in range(3):  # Mock 3 batches from extract
                yield Batch(
                    batch_uid=f"transform_batch_{i}",
                    payload={"batch_id": i, "from_extract": True}
                )

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        print(f"🔄 Transforming batch {batch.batch_uid}")

        # Simulate transformation: add computed fields
        transformed_data = {
            "batch_id": batch.payload.get("batch_id"),
            "transformation": "added_full_name_and_category",
            "processed_at": ctx.clock.now_dt().isoformat()
        }

        await asyncio.sleep(0.15)  # Simulate work

        return BatchResult(
            success=True,
            metrics={"records_transformed": 2},  # Simulated
            artifacts_ref={"transformed_data": transformed_data, "stage": "transform"}
        )

class LoadHandler(Handler):
    """Load transformed data to destination."""

    async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
        # Pull from transform stage
        input_args = input_data.get("input_inline", {}).get("input_args", {})
        from_nodes = input_args.get("from_nodes", [])

        if "transform" in from_nodes:
            # Simulate final loading batch
            yield Batch(
                batch_uid="load_final",
                payload={"load_all": True, "from_transform": True}
            )

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        print(f"💾 Loading data to destination")

        # Simulate loading to database/warehouse
        await asyncio.sleep(0.2)

        return BatchResult(
            success=True,
            metrics={"records_loaded": 5, "load_time_ms": 200},
            artifacts_ref={"load_status": "completed", "stage": "load"}
        )

async def run_etl_pipeline():
    """Run the complete ETL pipeline."""

    # Database setup
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.flowkit_examples

    # Configuration
    coord_config = CoordinatorConfig(
        kafka_bootstrap="localhost:9092",
        worker_types=["extractor", "transformer", "loader"],
        scheduler_tick_sec=0.1  # Fast scheduling for demo
    )

    worker_config = WorkerConfig(
        kafka_bootstrap="localhost:9092",
        roles=["extractor", "transformer", "loader"]
    )

    # Start coordinator
    coordinator = Coordinator(db=db, cfg=coord_config)
    await coordinator.start()
    print("🚀 Coordinator started")

    # Start worker with all handlers
    worker = Worker(
        db=db,
        cfg=worker_config,
        handlers={
            "extractor": ExtractHandler(),
            "transformer": TransformHandler(),
            "loader": LoadHandler()
        }
    )
    await worker.start()
    print("🚀 Worker started")

    try:
        # Define ETL graph
        graph = {
            "nodes": [
                {
                    "node_id": "extract",
                    "type": "extractor",
                    "depends_on": [],
                    "io": {
                        "input_inline": {"batch_size": 2}
                    }
                },
                {
                    "node_id": "transform",
                    "type": "transformer",
                    "depends_on": ["extract"],
                    "io": {
                        "start_when": "first_batch",  # Start as soon as extract produces data
                        "input_inline": {
                            "input_adapter": "pull.from_artifacts",
                            "input_args": {"from_nodes": ["extract"]}
                        }
                    }
                },
                {
                    "node_id": "load",
                    "type": "loader",
                    "depends_on": ["transform"],
                    "fan_in": "all",  # Wait for transform to complete
                    "io": {
                        "input_inline": {
                            "input_adapter": "pull.from_artifacts",
                            "input_args": {"from_nodes": ["transform"]}
                        }
                    }
                }
            ],
            "edges": [
                ["extract", "transform"],
                ["transform", "load"]
            ]
        }

        # Create and run task
        task_id = await coordinator.create_task(
            params={"pipeline_type": "etl_demo"},
            graph=graph
        )
        print(f"📋 Created ETL task: {task_id}")

        # Monitor progress
        print("\
⏳ Waiting for pipeline completion...")
        for i in range(30):  # Wait up to 30 seconds
            task_doc = await db.tasks.find_one({"id": task_id})
            status = task_doc.get("status")

            if status == "finished":
                print(f"✅ Pipeline completed successfully!")
                break
            elif status == "failed":
                print(f"❌ Pipeline failed")
                break

            await asyncio.sleep(1)

        # Show final results
        task_doc = await db.tasks.find_one({"id": task_id})
        nodes = {n["node_id"]: n["status"] for n in task_doc["graph"]["nodes"]}
        print(f"\
📊 Final node statuses: {nodes}")

        # Show artifacts
        artifacts = await db.artifacts.find({"task_id": task_id}).to_list(10)
        print(f"📦 Total artifacts created: {len(artifacts)}")

        # Show metrics
        metrics = await db.metrics_raw.find({"task_id": task_id}).to_list(20)
        total_extracted = sum(m.get("metrics", {}).get("records_extracted", 0) for m in metrics)
        total_transformed = sum(m.get("metrics", {}).get("records_transformed", 0) for m in metrics)
        total_loaded = sum(m.get("metrics", {}).get("records_loaded", 0) for m in metrics)

        print(f"\
📈 Pipeline Metrics:")
        print(f"  Records extracted: {total_extracted}")
        print(f"  Records transformed: {total_transformed}")
        print(f"  Records loaded: {total_loaded}")

    finally:
        # Cleanup
        await worker.stop()
        await coordinator.stop()
        client.close()
        print("\
✅ Pipeline shutdown complete")

if __name__ == "__main__":
    asyncio.run(run_etl_pipeline())
```

## Key Concepts Demonstrated

### 1. Handler Implementation
Each stage implements the `Handler` interface with:
- `iter_batches()`: Generates batches for processing
- `process_batch()`: Processes individual batches
- Returns `BatchResult` with success status and metrics

### 2. Input Adapters
The transform and load stages use `pull.from_artifacts` to get data from upstream stages:

```python
"input_inline": {
    "input_adapter": "pull.from_artifacts",
    "input_args": {"from_nodes": ["extract"]}
}
```

### 3. Streaming Processing
The transform stage starts as soon as extract produces its first batch:

```python
"io": {
    "start_when": "first_batch"
}
```

### 4. DAG Dependencies
The graph clearly defines the flow: extract → transform → load

### 5. Metrics Collection
Each handler reports metrics that are aggregated for monitoring

## Running the Example

1. Start Kafka and MongoDB:
```bash
# Terminal 1: Kafka
docker run -d --name kafka -p 9092:9092 confluentinc/cp-kafka:latest

# Terminal 2: MongoDB
docker run -d --name mongodb -p 27017:27017 mongo:6.0
```

2. Run the pipeline:
```bash
python simple_pipeline.py
```

Expected output:
```
🚀 Coordinator started
🚀 Worker started
📋 Created ETL task: abc123...
⏳ Waiting for pipeline completion...
📥 Extracted 2 records
📥 Extracted 2 records
📥 Extracted 1 records
🔄 Transforming batch transform_batch_0
🔄 Transforming batch transform_batch_1
🔄 Transforming batch transform_batch_2
💾 Loading data to destination
✅ Pipeline completed successfully!
📊 Final node statuses: {'extract': 'finished', 'transform': 'finished', 'load': 'finished'}
📦 Total artifacts created: 7
📈 Pipeline Metrics:
  Records extracted: 5
  Records transformed: 6
  Records loaded: 5
✅ Pipeline shutdown complete
```

## Next Steps

- [Complex Workflows](complex-workflows.md) - Multi-path DAGs with fan-in/fan-out
- [Testing Scenarios](testing.md) - How to test your pipelines
- [Error Handling](../guide/error-handling.md) - Robust error handling patterns
