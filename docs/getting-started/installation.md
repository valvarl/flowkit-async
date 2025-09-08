# Installation

## Requirements

- Python 3.11+
- Apache Kafka
- MongoDB
- Docker (optional, for development)

## Install FlowKit

### From PyPI

```bash
pip install flowkit
```

### From Source

```bash
git clone https://github.com/your-org/flowkit.git
cd flowkit
pip install -e .
```

### Development Installation

For development with all dependencies:

```bash
git clone https://github.com/your-org/flowkit.git
cd flowkit
pip install -e ".[dev,test]"
```

## Infrastructure Setup

### Apache Kafka

FlowKit requires Kafka for message coordination. You can run Kafka locally using Docker:

```bash
# Start Zookeeper and Kafka
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.8
docker run -d --name kafka -p 9092:9092 \
    --link zookeeper:zookeeper \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:latest
```

Or use the provided Docker Compose file:

```bash
cd flowkit
docker-compose up -d kafka
```

### MongoDB

FlowKit uses MongoDB for task state and artifact persistence:

```bash
# Run MongoDB with Docker
docker run -d --name mongodb -p 27017:27017 mongo:6.0
```

Or with Docker Compose:

```bash
cd flowkit
docker-compose up -d mongodb
```

## Configuration

Create configuration files for your coordinator and workers:

### Coordinator Configuration (`coordinator.json`)

```json
{
    "kafka_bootstrap": "localhost:9092",
    "worker_types": ["indexer", "processor", "analyzer"],
    "heartbeat_soft_sec": 300,
    "heartbeat_hard_sec": 3600,
    "lease_ttl_sec": 60
}
```

### Worker Configuration (`worker.json`)

```json
{
    "kafka_bootstrap": "localhost:9092",
    "roles": ["processor"],
    "worker_id": null,
    "lease_ttl_sec": 60,
    "hb_interval_sec": 20
}
```

## Verification

Test your installation:

```python
import flowkit
print(f"FlowKit version: {flowkit.__version__}")

# Test coordinator and worker imports
from flowkit import Coordinator, CoordinatorConfig, WorkerConfig
from flowkit.worker import Worker
print("FlowKit installed successfully!")
```
