# Workers

Workers are the execution engine of FlowKit, responsible for processing individual tasks and reporting results back to coordinators.

## Overview

A Worker:

- **Executes Tasks**: Runs custom handler logic for specific task types
- **Manages State**: Maintains local state for reliability and recovery
- **Communicates Progress**: Reports status updates via Kafka
- **Handles Cancellation**: Responds to cancellation signals gracefully
- **Supports Multiple Roles**: Can handle different task types simultaneously

## Basic Setup

```python
from flowkit.worker import Worker, WorkerConfig
from flowkit.worker.handlers.base import Handler
from motor.motor_asyncio import AsyncIOMotorClient

class MyHandler(Handler):
    async def iter_batches(self, input_data):
        # Generate batches from input
        yield Batch(batch_uid="batch_1", payload={"data": "example"})

    async def process_batch(self, batch, ctx):
        # Process the batch
        return BatchResult(success=True, metrics={"processed": 1})

async def setup_worker():
    # Database connection
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.flowkit

    # Configuration
    config = WorkerConfig(
        kafka_bootstrap="localhost:9092",
        roles=["processor", "analyzer"]
    )

    # Create worker with handlers
    worker = Worker(
        db=db,
        cfg=config,
        handlers={
            "processor": MyHandler(),
            "analyzer": MyHandler()
        }
    )

    await worker.start()
    return worker
```

## Configuration Options

### Core Settings

```python
config = WorkerConfig(
    # Kafka configuration
    kafka_bootstrap="localhost:9092",

    # Worker identity
    roles=["processor", "analyzer"],    # Task types this worker handles
    worker_id=None,                     # Auto-generated if None
    worker_version="2.0.0",            # Worker version for tracking

    # Timing settings
    lease_ttl_sec=60,                  # How long to hold task leases
    hb_interval_sec=20,                # Heartbeat frequency
    announce_interval_sec=60,          # Worker announcement frequency

    # Performance tuning
    dedup_cache_size=10000,           # Deduplication cache size
    dedup_ttl_ms=3600_000,            # Dedup entry TTL
    pull_poll_ms_default=300,         # Default polling interval
    db_cancel_poll_ms=500,            # Cancellation check frequency
)
```

### Loading from File

```python
# Load from JSON file
config = WorkerConfig.load("configs/worker.json")

# With overrides
config = WorkerConfig.load(
    "configs/worker.json",
    overrides={"roles": ["processor"]}
)
```

## Handler Implementation

### Handler Interface

All handlers must implement the `Handler` base class:

```python
from flowkit.worker.handlers.base import Handler, Batch, BatchResult, RunContext
from typing import AsyncIterator

class CustomHandler(Handler):

    async def init(self, run_info: dict):
        """Initialize handler for a new task run."""
        self.task_id = run_info["task_id"]
        self.node_id = run_info["node_id"]
        # Setup resources, connections, etc.

    async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
        """Generate batches from input data."""
        # Extract batch information from input_data
        batch_size = input_data.get("input_inline", {}).get("batch_size", 10)

        for i in range(0, 100, batch_size):
            yield Batch(
                batch_uid=f"batch_{i}",
                payload={"start": i, "end": min(i + batch_size, 100)}
            )

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        """Process a single batch."""
        start = batch.payload["start"]
        end = batch.payload["end"]

        # Check for cancellation
        if ctx.cancel_flag.is_set():
            raise asyncio.CancelledError()

        # Do actual processing
        result = await self.process_range(start, end)

        # Save artifacts if needed
        artifacts_ref = await ctx.artifacts_writer.upsert_partial(
            batch.batch_uid,
            {"items_processed": end - start}
        )

        return BatchResult(
            success=True,
            metrics={"items_processed": end - start},
            artifacts_ref=artifacts_ref
        )

    async def finalize(self, ctx: RunContext) -> FinalizationResult:
        """Clean up after all batches are processed."""
        # Final processing, cleanup, etc.
        return FinalizationResult(
            metrics={"total_items": ctx.total_processed}
        )

    def classify_error(self, error: Exception) -> tuple[str, bool]:
        """Classify errors as permanent or transient."""
        if isinstance(error, ValueError):
            return "validation_error", True  # Permanent
        elif isinstance(error, ConnectionError):
            return "connection_error", False  # Transient
        else:
            return "unknown_error", False
```

### Batch Processing Patterns

#### Simple Iterator

```python
async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
    items = input_data.get("input_inline", {}).get("items", [])

    for i, item in enumerate(items):
        yield Batch(
            batch_uid=f"item_{i}",
            payload={"item": item, "index": i}
        )
```

#### Database Cursor

```python
async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
    batch_size = input_data.get("input_inline", {}).get("batch_size", 100)

    async for batch in self.db.collection.find().batch_size(batch_size):
        yield Batch(
            batch_uid=f"db_batch_{batch[0]['_id']}",
            payload={"records": batch}
        )
```

#### File Processing

```python
async def iter_batches(self, input_data) -> AsyncIterator[Batch]:
    file_path = input_data.get("input_inline", {}).get("file_path")

    with open(file_path, 'r') as f:
        batch = []
        for i, line in enumerate(f):
            batch.append(line.strip())

            if len(batch) >= 1000:  # Process in chunks of 1000
                yield Batch(
                    batch_uid=f"file_batch_{i//1000}",
                    payload={"lines": batch}
                )
                batch = []

        # Final batch
        if batch:
            yield Batch(
                batch_uid=f"file_batch_final",
                payload={"lines": batch}
            )
```

## Input Adapters

Workers can use input adapters to pull data from previous stages:

### Pull from Artifacts

```python
# In task definition
"io": {
    "input_inline": {
        "input_adapter": "pull.from_artifacts",
        "input_args": {
            "from_nodes": ["upstream_node"],
            "poll_ms": 500,
            "eof_on_task_done": True
        }
    }
}
```

### Rechunk Data

```python
# Rechunk upstream data into different batch sizes
"io": {
    "input_inline": {
        "input_adapter": "pull.from_artifacts.rechunk:size",
        "input_args": {
            "from_nodes": ["upstream_node"],
            "size": 50,  # New batch size
            "meta_list_key": "items"
        }
    }
}
```

## State Management

### Worker State Architecture

**问题分析**: 当前的 worker_state 文件系统有以下问题：

1. **文件系统依赖**: 需要创建目录，可能在容器环境中有权限问题
2. **单点故障**: 本地文件丢失会导致状态丢失
3. **扩展性限制**: 不适合多实例部署

**建议的更好架构解决方案**:

```python
# 新的基于数据库的状态管理
class DatabaseWorkerState:
    """Database-backed worker state for production reliability."""

    def __init__(self, db, worker_id: str):
        self.db = db
        self.worker_id = worker_id
        self.collection = db.worker_states

    async def read_active(self) -> ActiveRun | None:
        """Read active task state from database."""
        doc = await self.collection.find_one({"worker_id": self.worker_id})
        if doc and doc.get("active_run"):
            return ActiveRun(**doc["active_run"])
        return None

    async def write_active(self, active_run: ActiveRun | None):
        """Write active task state to database."""
        await self.collection.update_one(
            {"worker_id": self.worker_id},
            {
                "$set": {
                    "worker_id": self.worker_id,
                    "active_run": active_run.__dict__ if active_run else None,
                    "updated_at": datetime.utcnow()
                }
            },
            upsert=True
        )

    async def write_checkpoint(self, checkpoint: dict):
        """Write processing checkpoint."""
        await self.collection.update_one(
            {"worker_id": self.worker_id},
            {"$set": {"active_run.checkpoint": checkpoint}},
            upsert=True
        )
```

**优势**:
- ✅ **无文件系统依赖**: 完全基于数据库
- ✅ **高可用**: 数据库集群提供冗余
- ✅ **扩展性**: 支持多实例部署
- ✅ **监控友好**: 可查询所有 worker 状态
- ✅ **事务性**: 原子性状态更新

### State Recovery

Workers automatically recover from failures:

```python
# On worker startup, check for existing state
active_run = await worker.state.read_active()
if active_run:
    print(f"Recovering task: {active_run.task_id}")
    # Worker will resume the task automatically
```

### Checkpointing

```python
async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
    # Save progress checkpoint
    checkpoint = {
        "processed_items": batch.payload["count"],
        "last_id": batch.payload["last_id"]
    }
    await ctx.save_checkpoint(checkpoint)

    # Continue processing...
```

## Cancellation Handling

### Graceful Cancellation

```python
async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
    # Check cancellation at key points
    if ctx.cancel_flag.is_set():
        reason = ctx.cancel_meta.get("reason", "unknown")
        print(f"Task cancelled: {reason}")
        raise asyncio.CancelledError()

    # Long-running operation with periodic checks
    for item in batch.payload["items"]:
        if ctx.cancel_flag.is_set():
            raise asyncio.CancelledError()

        await process_item(item)

    return BatchResult(success=True)
```

### Cancellation Sources

Cancellation can come from:

1. **Coordinator signals**: Explicit cancellation commands
2. **Database flags**: Task marked as cancelled in DB
3. **Timeout**: Lease expiration
4. **Worker shutdown**: Graceful shutdown process

## Monitoring and Health

### Worker Announcements

Workers periodically announce their presence:

```python
# Automatic announcements include:
{
    "worker_id": "worker-abc123",
    "type": "processor,analyzer",  # Comma-separated roles
    "capabilities": {"roles": ["processor", "analyzer"]},
    "version": "2.0.0",
    "capacity": {"tasks": 1},
    "status": "online"
}
```

### Health Checks

```python
async def check_worker_health(worker):
    return {
        "worker_id": worker.worker_id,
        "roles": worker.cfg.roles,
        "busy": worker._busy,
        "active_task": worker.active.task_id if worker.active else None,
        "last_heartbeat": worker.last_heartbeat_time
    }
```

### Metrics Collection

```python
async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
    start_time = time.time()

    # Do processing...

    processing_time = time.time() - start_time

    return BatchResult(
        success=True,
        metrics={
            "items_processed": len(batch.payload["items"]),
            "processing_time_ms": int(processing_time * 1000),
            "memory_used_mb": get_memory_usage()
        }
    )
```

## Scaling Workers

### Horizontal Scaling

```python
# Run multiple workers with same roles
workers = []
for i in range(5):
    worker = Worker(
        db=db,
        cfg=WorkerConfig(roles=["processor"]),
        handlers={"processor": ProcessorHandler()}
    )
    await worker.start()
    workers.append(worker)

# Tasks are automatically distributed across workers
```

### Role Specialization

```python
# Specialized workers for different task types
cpu_worker = Worker(cfg=WorkerConfig(roles=["cpu_intensive"]))
io_worker = Worker(cfg=WorkerConfig(roles=["io_intensive"]))
gpu_worker = Worker(cfg=WorkerConfig(roles=["gpu_processing"]))
```

## Best Practices

### Handler Design

1. **Keep handlers stateless** where possible
2. **Implement proper error classification**
3. **Use checkpointing** for long-running tasks
4. **Handle cancellation gracefully**
5. **Report meaningful metrics**

### Performance

1. **Tune batch sizes** for your workload
2. **Use async I/O** for external calls
3. **Implement connection pooling** for databases
4. **Monitor memory usage** in long-running tasks

### Reliability

1. **Implement retry logic** for transient failures
2. **Use idempotent operations** where possible
3. **Save state frequently** for recovery
4. **Test failure scenarios** thoroughly

### Resource Management

```python
class DatabaseHandler(Handler):
    async def init(self, run_info):
        # Create connection pool
        self.pool = await create_connection_pool()

    async def cleanup(self):
        # Clean up resources
        await self.pool.close()

    async def process_batch(self, batch, ctx):
        async with self.pool.acquire() as conn:
            # Use connection for processing
            pass
```

## Troubleshooting

### Common Issues

**Worker not receiving tasks**:
- Check Kafka topic subscription
- Verify worker roles match task types
- Check for consumer group conflicts

**Tasks timing out**:
- Increase `lease_ttl_sec`
- Check heartbeat frequency
- Monitor processing times

**Memory leaks**:
- Implement proper cleanup in handlers
- Monitor batch processing memory usage
- Use memory profiling tools

**State corruption**:
- Check database connectivity
- Verify state serialization/deserialization
- Implement state validation

## Next Steps

- [Design Task Graphs](graphs.md) for your workflows
- [Configure Error Handling](error-handling.md) policies
- [Monitor Performance](../development/architecture.md) in production
- [Test Workers](../examples/testing.md) thoroughly
