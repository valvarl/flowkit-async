from __future__ import annotations

# Runtime package version, provided by setuptools_scm during build.
try:
    # created at build time by setuptools_scm (see [tool.setuptools_scm].write_to)
    from ._version import __version__
except Exception:  # pragma: no cover
    # fallback for editable installs / missing file
    try:
        from importlib.metadata import version as _pkg_version

        __version__ = _pkg_version("flowkit")
    except Exception:
        __version__ = "0.0.0"

from .coordinator.runner import Coordinator
from .core.config import CoordinatorConfig, WorkerConfig

__all__ = [
    "Coordinator",
    "CoordinatorConfig",
    "WorkerConfig",
    "__version__",
]
