# Coordinators

Coordinators are the orchestration layer of FlowKit, responsible for managing task execution, scheduling DAG nodes, and monitoring worker health.

## Overview

A Coordinator:

- **Schedules Tasks**: Determines when nodes in a DAG are ready to execute
- **Manages Workers**: Tracks worker health and capacity
- **Handles Failures**: Implements retry policies and failure recovery
- **Coordinates State**: Maintains task state in MongoDB
- **Routes Messages**: Uses Kafka for reliable communication with workers

## Basic Setup

```python
from flowkit import Coordinator, CoordinatorConfig
from motor.motor_asyncio import AsyncIOMotorClient

async def setup_coordinator():
    # Database connection
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.flowkit

    # Configuration
    config = CoordinatorConfig(
        kafka_bootstrap="localhost:9092",
        worker_types=["processor", "analyzer", "loader"]
    )

    # Create and start coordinator
    coordinator = Coordinator(db=db, cfg=config)
    await coordinator.start()

    return coordinator
```

## Configuration Options

### Core Settings

```python
config = CoordinatorConfig(
    # Kafka configuration
    kafka_bootstrap="localhost:9092",
    worker_types=["indexer", "processor", "analyzer"],

    # Topic naming patterns
    topic_cmd_fmt="cmd.{type}.v1",
    topic_status_fmt="status.{type}.v1",

    # Timing settings (seconds)
    heartbeat_soft_sec=300,      # Soft heartbeat timeout
    heartbeat_hard_sec=3600,     # Hard heartbeat timeout
    lease_ttl_sec=45,            # Worker lease duration
    scheduler_tick_sec=1.0,      # Scheduling frequency

    # Retry and backoff
    cancel_grace_sec=30,         # Grace period for cancellation
    outbox_max_retry=12,         # Max retry attempts
    outbox_backoff_min_ms=250,   # Min backoff time
    outbox_backoff_max_ms=60000, # Max backoff time
)
```

### Loading from File

```python
# Load from JSON file
config = CoordinatorConfig.load("configs/coordinator.json")

# With overrides
config = CoordinatorConfig.load(
    "configs/coordinator.json",
    overrides={"scheduler_tick_sec": 0.5}
)

# Environment variables
# KAFKA_BOOTSTRAP_SERVERS, WORKER_TYPES are automatically loaded
```

## Task Management

### Creating Tasks

```python
async def create_pipeline_task(coordinator):
    graph = {
        "nodes": [
            {
                "node_id": "extract",
                "type": "extractor",
                "depends_on": [],
                "io": {"input_inline": {"source": "database"}}
            },
            {
                "node_id": "transform",
                "type": "processor",
                "depends_on": ["extract"],
                "io": {
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["extract"]}
                    }
                }
            }
        ],
        "edges": [["extract", "transform"]]
    }

    task_id = await coordinator.create_task(
        params={"pipeline_name": "etl_demo"},
        graph=graph
    )

    return task_id
```

### Task Lifecycle

Tasks progress through these states:

1. **queued** → Task created, waiting to be scheduled
2. **running** → At least one node is executing
3. **finished** → All nodes completed successfully
4. **failed** → Task failed permanently
5. **deferred** → Temporarily paused for retry

### Monitoring Tasks

```python
async def monitor_task(coordinator, task_id):
    # Get task document from database
    task_doc = await coordinator.db.tasks.find_one({"id": task_id})

    # Check overall status
    print(f"Task status: {task_doc['status']}")

    # Check individual node statuses
    for node in task_doc["graph"]["nodes"]:
        print(f"Node {node['node_id']}: {node['status']}")

    # Check artifacts
    artifacts = await coordinator.db.artifacts.find(
        {"task_id": task_id}
    ).to_list(100)
    print(f"Artifacts created: {len(artifacts)}")
```

## Scheduling Behavior

### Node Readiness

The coordinator schedules nodes when:

1. **Dependencies satisfied**: All `depends_on` nodes are finished
2. **Fan-in condition met**: Based on `fan_in` strategy
3. **Not already running**: Node isn't currently being processed
4. **Retry conditions met**: If deferred, retry time has passed

### Fan-in Strategies

```python
# Wait for all dependencies (default)
{
    "node_id": "combiner",
    "depends_on": ["node1", "node2", "node3"],
    "fan_in": "all"
}

# Start when any dependency completes
{
    "node_id": "processor",
    "depends_on": ["node1", "node2"],
    "fan_in": "any"
}

# Start when N dependencies complete
{
    "node_id": "analyzer",
    "depends_on": ["node1", "node2", "node3"],
    "fan_in": "count:2"
}
```

### Streaming Execution

```python
# Start as soon as upstream produces first batch
{
    "node_id": "processor",
    "depends_on": ["extractor"],
    "io": {"start_when": "first_batch"}
}

# Wait for upstream to completely finish (default)
{
    "node_id": "loader",
    "depends_on": ["processor"],
    "io": {"start_when": "ready"}
}
```

## Coordinator Functions

Coordinators can execute logic directly without workers:

```python
{
    "node_id": "merger",
    "type": "coordinator_fn",
    "depends_on": ["extract1", "extract2"],
    "io": {
        "fn": "merge.generic",
        "fn_args": {
            "from_nodes": ["extract1", "extract2"],
            "target": {"key": "merged_data"}
        }
    }
}
```

Built-in coordinator functions:

- `merge.generic`: Merge artifacts from multiple nodes
- `copy.artifacts`: Copy artifacts between nodes
- `transform.metadata`: Transform artifact metadata

## Error Handling

### Retry Policies

```python
{
    "node_id": "flaky_processor",
    "retry_policy": {
        "max": 3,                    # Maximum retry attempts
        "backoff_sec": 300,          # Backoff between retries
        "permanent_on": [            # Errors that shouldn't retry
            "bad_input",
            "schema_mismatch"
        ]
    }
}
```

### Cascade Cancellation

When a node fails permanently, the coordinator can cancel downstream nodes:

```python
# This happens automatically for permanent failures
# You can also trigger manual cancellation:

await coordinator.cascade_cancel(
    task_id="abc123",
    reason="upstream_failure"
)
```

## Scaling Coordinators

### Multiple Coordinators

You can run multiple coordinators for high availability:

```python
# Coordinator 1
coordinator1 = Coordinator(db=db, cfg=config, worker_types=["processor"])

# Coordinator 2
coordinator2 = Coordinator(db=db, cfg=config, worker_types=["analyzer"])

# They coordinate through MongoDB and don't conflict
await coordinator1.start()
await coordinator2.start()
```

### Load Balancing

Coordinators automatically distribute work:

- Task scheduling is coordinated through MongoDB
- Workers are discovered dynamically
- No single point of failure

## Monitoring and Observability

### Health Checks

```python
async def check_coordinator_health(coordinator):
    # Check if coordinator is running
    if not coordinator._running:
        return "stopped"

    # Check active workers
    workers = await coordinator.db.worker_registry.find(
        {"status": "online"}
    ).to_list(100)

    # Check pending tasks
    pending = await coordinator.db.tasks.count_documents(
        {"status": {"$in": ["queued", "running"]}}
    )

    return {
        "status": "healthy",
        "active_workers": len(workers),
        "pending_tasks": pending
    }
```

### Metrics Collection

```python
async def collect_coordinator_metrics(coordinator):
    # Task metrics
    tasks_by_status = await coordinator.db.tasks.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ]).to_list(10)

    # Worker metrics
    workers_by_status = await coordinator.db.worker_registry.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ]).to_list(10)

    # Recent events
    recent_events = await coordinator.db.worker_events.count_documents({
        "created_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}
    })

    return {
        "tasks": dict(tasks_by_status),
        "workers": dict(workers_by_status),
        "recent_events": recent_events
    }
```

## Best Practices

### Configuration Management

1. **Use environment-specific configs**:
   ```python
   config = CoordinatorConfig.load(f"configs/coordinator.{env}.json")
   ```

2. **Tune timing parameters** based on your workload:
   - Short `scheduler_tick_sec` for low-latency
   - Longer `heartbeat_soft_sec` for stable workloads

3. **Set appropriate worker types** to match your pipeline needs

### Task Design

1. **Keep DAGs focused**: Don't create overly complex graphs
2. **Use streaming** for large datasets
3. **Implement proper retry policies** for reliability
4. **Add monitoring** for long-running tasks

### Error Handling

1. **Classify errors properly** in handlers
2. **Set reasonable retry limits** to avoid infinite loops
3. **Monitor failure patterns** to identify systemic issues
4. **Implement circuit breakers** for external dependencies

### Performance

1. **Scale coordinators horizontally** for high throughput
2. **Tune Kafka settings** for your message volume
3. **Monitor MongoDB performance** under load
4. **Use indexing** for large task collections

## Troubleshooting

### Common Issues

**Coordinator not starting**:
- Check Kafka connectivity
- Verify MongoDB connection
- Ensure topics are created

**Tasks not being scheduled**:
- Check worker availability
- Verify DAG dependencies are correct
- Look for failed discovery queries

**High memory usage**:
- Tune outbox retention settings
- Clean up old task documents
- Monitor worker event collection size

**Slow scheduling**:
- Reduce `scheduler_tick_sec`
- Add MongoDB indexes
- Optimize DAG complexity

## Next Steps

- [Configure Workers](workers.md) to process your tasks
- [Design Task Graphs](graphs.md) for complex workflows
- [Handle Errors](error-handling.md) gracefully
- [Monitor Performance](../development/architecture.md) in production
