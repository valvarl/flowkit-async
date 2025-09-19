# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Public API for resolving and managing lifecycle of externals.
"""

from .registry import (
    ExternalsResolver,
    ExternalFactory,
    merge_declared,
)

# Re-export protocol types for convenience of integrators
from ..api.externals import ExternalProvider, ExternalReady, ExternalError

__all__ = [
    "ExternalsResolver",
    "ExternalFactory",
    "merge_declared",
    "ExternalProvider",
    "ExternalReady",
    "ExternalError",
]
