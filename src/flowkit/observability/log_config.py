from __future__ import annotations

"""
flowkit.observability.log_config
================================

High-level logging configuration helpers for applications and CLIs.

This module is a thin facade over `flowkit.core.logging` that:
- exposes context binding utilities,
- wires a redaction filter (via `flowkit.security.redaction`),
- provides one-call setup driven by environment variables.

Design goals:
- Keep imports lightweight: importing this module must NOT start threads or
  mutate global logging handlers on its own.
- Make redaction pluggable and **fail-open** (i.e., if no redactor provided,
  nothing is redacted).
"""

from typing import Any, Mapping, MutableMapping, Iterable, Tuple, Union, cast
import logging

from ..core import logging as corelog
from ..security.redaction import Redactor, redact_obj


__all__ = [
    "setup_logging",
    "install_redaction_filter",
    "bind_context",
    "log_context",
    "get_logger",
    "set_level",
    "configure_from_env",
]


class _RedactionFilter(logging.Filter):
    """
    Logging filter that redacts sensitive fields in LogRecord.

    Responsibilities:
    - Redact `record.msg` if it's a str (via redactor.redact_text()) or mapping (via redact_obj()).
    - Redact `record.args` (tuple or dict) in-place to avoid leaking secrets when the
      formatter interpolates the message.
    - Redact extra attributes injected by adapters (anything not in std LogRecord attrs).

    Note:
        We rely on Filter stage (before formatting) to sanitize data.
        This does not parse arbitrary structured payloads embedded into strings,
        so prefer logging dicts/kwargs over concatenated strings when handling secrets.
    """

    _STD_ATTRS = corelog._STD_ATTRS  # reuse list from core logging
    _REDACTED_KEY = "***"

    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        # 1) redact msg
        if isinstance(record.msg, str):
            record.msg = self._redactor.redact_text(record.msg)
        elif isinstance(record.msg, Mapping):
            # make a shallow redacted copy to avoid mutating caller structures
            record.msg = redact_obj(record.msg, redactor=self._redactor, in_place=False)

        # 2) redact args (tuple or dict used for %-style formatting)
        if isinstance(record.args, tuple):
            record.args = tuple(
                redact_obj(a, redactor=self._redactor, in_place=False) for a in cast(Tuple[Any, ...], record.args)
            )
        elif isinstance(record.args, Mapping):
            record.args = redact_obj(cast(Mapping[str, Any], record.args), redactor=self._redactor, in_place=False)

        # 3) redact "extra" fields already attached to the record
        #    (we avoid touching standard attributes)
        for k, v in list(record.__dict__.items()):
            if k in self._STD_ATTRS:
                continue
            # msg/args handled already
            if k in ("msg", "args"):
                continue
            record.__dict__[k] = redact_obj(v, redactor=self._redactor, in_place=False)

        return True


def install_redaction_filter(redactor: Redactor) -> None:
    """
    Install a redaction filter on the FlowKit logger tree.

    Safe to call multiple times; re-installs the filter with the new redactor.
    """
    base = logging.getLogger("flowkit")
    # Remove previous redaction filters if any
    to_remove: list[logging.Filter] = []
    for f in base.filters:
        if isinstance(f, _RedactionFilter):
            to_remove.append(f)
    for f in to_remove:
        base.removeFilter(f)

    base.addFilter(_RedactionFilter(redactor))


# --- Facade to core logging ---------------------------------------------------

bind_context = corelog.bind_context
log_context = corelog.log_context
get_logger = corelog.get_logger
set_level = corelog.set_level
configure_from_env = corelog.configure_from_env


def setup_logging(
    *,
    level: int | str = logging.INFO,
    json_output: bool = True,
    pretty: bool = False,
    include_stack: bool = False,
    route_errors_to_stderr: bool = False,
    redactor: Redactor | None = None,
) -> None:
    """
    Configure FlowKit logging for an application/CLI.

    Args:
        level: base log level.
        json_output: emit structured JSON logs when True (recommended for prod).
        pretty: human-readable logs (overrides json_output when True).
        include_stack: include exception stack traces in JSON logs.
        route_errors_to_stderr: split WARNING- and ERROR+ streams.
        redactor: optional Redactor; when provided, sensitive data is scrubbed.

    Notes:
        - This is a convenience wrapper. Advanced setups can call
          `corelog.enable_stdout_logging()` and `install_redaction_filter()` directly.
    """
    corelog.enable_stdout_logging(
        level=level,
        json_output=not pretty and json_output,
        include_stack=include_stack,
        pretty=pretty,
        route_errors_to_stderr=route_errors_to_stderr,
    )
    if redactor:
        install_redaction_filter(redactor)
