# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
FlowKit built-in input adapters (sources).

This module exposes a Plugin that registers minimal, production-ready
sources: HTTP polling/pagination, Kafka subscribe/assign, S3 list+get,
and a thin "artifacts" source that relies on an external provider.

Downstream code can import this plugin and register it with the central
registry (via entry points or manual registration).
"""

from collections.abc import Mapping
from typing import Any

from ....api.adapters import Plugin
from .artifacts import ArtifactsSource
from .http import HttpPollingSource
from .kafka import KafkaSubscribeSource
from .s3 import S3ListGetSource


class IOInputsPlugin(Plugin):
    """
    Bundle of input adapters (sources).
    """

    api_version = "3.0"

    def sources(self) -> Mapping[str, type]:
        # Canonical names that appear in graph spec / validation.
        return {
            # Artifacts (upstream node outputs) — requires runtime/storage integration
            "pull.from_artifacts": ArtifactsSource,
            "pull.from_artifacts.rechunk:size": ArtifactsSource,  # simple alias to the same impl (size handled in args)
            # HTTP polling / pagination
            "pull.http.paginate": HttpPollingSource,
            "pull.http": HttpPollingSource,
            # Kafka subscribe/assign (aiokafka)
            "pull.kafka.subscribe": KafkaSubscribeSource,
            "pull.kafka": KafkaSubscribeSource,
            # Object storage (S3-compatible) list+get
            "pull.s3.list+get": S3ListGetSource,
            "pull.s3": S3ListGetSource,
        }

    def sinks(self) -> Mapping[str, type]:
        # No sinks from inputs plugin.
        return {}

    def transforms(self) -> Mapping[str, type]:
        # No transforms from inputs plugin.
        return {}
