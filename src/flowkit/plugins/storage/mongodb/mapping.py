# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Collection names and index creation for the MongoDB storage provider.
"""

from motor.motor_asyncio import AsyncIOMotorDatabase

# Collection constants
COL_KV = "kv"
COL_LOCKS = "locks"
COL_CHECKPOINTS = "checkpoints"
COL_TASKS = "tasks"
COL_ARTIFACTS = "artifacts"
COL_OUTBOX = "outbox"


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:
    # KV: TTL on expires_at
    await db[COL_KV].create_index("expires_at", expireAfterSeconds=0, name="kv_ttl")

    # Locks
    await db[COL_LOCKS].create_index("deadline_ms", name="locks_deadline")

    # Checkpoints
    await db[COL_CHECKPOINTS].create_index([("scope", 1), ("key", 1)], name="cp_scope_key")

    # Tasks
    await db[COL_TASKS].create_index("state", name="tasks_state")
    await db[COL_TASKS].create_index("worker_id", name="tasks_worker")
    await db[COL_TASKS].create_index("lease_deadline_ms", name="tasks_lease_deadline")
    await db[COL_TASKS].create_index("created_ms", name="tasks_created")
    await db[COL_TASKS].create_index("meta.owner", name="tasks_owner")

    # Artifacts
    await db[COL_ARTIFACTS].create_index(
        [("task_id", 1), ("node_id", 1), ("bucket", 1), ("key", 1)],
        unique=True,
        name="artifacts_scope_key_unique",
    )
    await db[COL_ARTIFACTS].create_index(
        [("task_id", 1), ("node_id", 1), ("bucket", 1), ("idempotency_key", 1)],
        unique=True,
        name="artifacts_idem_unique",
        partialFilterExpression={"idempotency_key": {"$exists": True}},
    )
    await db[COL_ARTIFACTS].create_index([("created_ms", -1)], name="artifacts_created")
    await db[COL_ARTIFACTS].create_index([("key", 1)], name="artifacts_key")

    # Outbox
    await db[COL_OUTBOX].create_index(
        [("status", 1), ("next_attempt_at_ms", 1), ("locked_until_ms", 1), ("_id", 1)],
        name="outbox_dispatch_order",
    )
    await db[COL_OUTBOX].create_index(
        [("fp", 1)],
        name="outbox_fp_unique",
        unique=True,
        partialFilterExpression={"fp": {"$exists": True}},
    )
    await db[COL_OUTBOX].create_index([("topic", 1)], name="outbox_topic")
