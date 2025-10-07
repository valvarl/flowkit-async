# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Sink adapters bundle.

This module exposes a Plugin that registers sink implementations under
canonical names used by the Graph Spec and runtime.

Registered sinks:
  - "emit.kafka"        → KafkaProducerSink
  - "push.to_artifacts" → ArtifactsSink (simple local/artifacts store writer)
  - "emit.s3"           → ObjectStorageSink (S3 via aioboto3)
"""

from collections.abc import Mapping

from ....api.adapters import Plugin, SinkAdapter
from .artifacts import ArtifactsSink
from .kafka import KafkaProducerSink
from .object_storage import ObjectStorageSink


class IOSinksPlugin(Plugin):
    """Plugin bundle for IO sink adapters."""

    def sources(self) -> Mapping[str, type]:
        return {}

    def sinks(self) -> Mapping[str, type[SinkAdapter]]:
        return {
            "emit.kafka": KafkaProducerSink,
            "push.to_artifacts": ArtifactsSink,
            "emit.s3": ObjectStorageSink,
        }

    def transforms(self) -> Mapping[str, type]:
        return {}


__all__ = [
    "ArtifactsSink",
    "IOSinksPlugin",
    "KafkaProducerSink",
    "ObjectStorageSink",
]
