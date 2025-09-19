from __future__ import annotations

"""
flowkit.core.logging
====================

A tiny, dependency-free structured logging helper:
- Context propagation via contextvars (task_id, node_id, etc.).
- JSON formatter for production; human formatter for local debugging.
- Safe LoggerAdapter that accepts arbitrary keyword fields.
- Utilities to enable/disable stdout logging and set levels.
"""

import contextvars
import json
import logging
import os
import sys
import threading
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar, Final

__all__ = [
    "bind_context",
    "configure_from_env",
    "disable_stdout_logging",
    "enable_stdout_logging",
    "get_logger",
    "log_context",
    "set_level",
    "swallow",
    "warn_once",
]

# ---------- Context ----------

_log_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("flowkit_log_ctx", default=None)


def _ctx_copy() -> dict[str, Any]:
    ctx = _log_context.get()
    return dict(ctx) if ctx else {}


def bind_context(**fields: Any) -> None:
    """
    Merge fields into the current structured log context.
    Use from long-lived code (e.g., at worker init or per task).
    """
    ctx = _ctx_copy()
    ctx.update({k: v for k, v in fields.items() if v is not None})
    _log_context.set(ctx)


@contextmanager
def log_context(**fields: Any):
    """
    Temporarily add fields to the structured log context.
    Restores the previous context on exit.
    """
    token = _log_context.set({**_ctx_copy(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _log_context.reset(token)


# ---------- JSON formatter ----------

_STD_ATTRS: Final[frozenset[str]] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "asctime",
    }
)


def _iso_utc_ms(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonFormatter(logging.Formatter):
    """
    Minimal JSON formatter. Merges:
      - ts, level, logger
      - message
      - contextvars (task_id, node_id, worker_id, etc.)
      - extra=... fields (non-standard LogRecord attributes)
      - exception info when present
    """

    def __init__(self, *, include_stack: bool = False) -> None:
        super().__init__()
        self.include_stack = include_stack

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": _iso_utc_ms(record.created),
            "level": record.levelname,
            "logger": record.name,
        }

        msg = record.getMessage()
        if msg:
            out["message"] = msg

        ctx = _log_context.get()
        if ctx:
            out.update(ctx)

        for k, v in record.__dict__.items():
            if k in _STD_ATTRS:
                continue
            if k not in out:
                out[k] = v

        exc = None
        if record.exc_info:
            if isinstance(record.exc_info, BaseException):
                e = record.exc_info
                exc = (type(e), e, e.__traceback__)
            elif record.exc_info is True:
                exc = sys.exc_info()
            elif isinstance(record.exc_info, tuple):
                exc = record.exc_info
        if exc:
            exc_type = exc[0].__name__ if exc[0] else "Exception"
            out.setdefault("error", {})
            out["error"].update(
                {
                    "type": exc_type,
                    "message": str(exc[1]) if exc[1] else None,
                }
            )
            if self.include_stack:
                out["error"]["stack"] = self.formatException(exc)
        elif record.exc_text:
            out.setdefault("error", {})
            out["error"]["stack"] = record.exc_text

        return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


class HumanFormatter(logging.Formatter):
    """Compact human-friendly formatter for local debugging."""

    default_time_format = "%Y-%m-%d %H:%M:%S"
    default_msec_format = "%s.%03d"

    def format(self, record: logging.LogRecord) -> str:
        s = f"{self.formatTime(record)} {record.levelname:<5} {record.name}: {record.getMessage()}"
        ctx = _log_context.get()
        if ctx:
            keys = ("task_id", "node_id", "attempt_epoch", "worker_id")
            compact = {k: ctx.get(k) for k in keys if ctx.get(k) is not None}
            if compact:
                parts = ", ".join(f"{k}={v}" for k, v in compact.items())
                s += f"  [{parts}]"
        if record.exc_info:
            if isinstance(record.exc_info, BaseException):
                e = record.exc_info
                exc = (type(e), e, e.__traceback__)
            elif record.exc_info is True:
                exc = sys.exc_info()
            else:
                exc = record.exc_info
            s += "\n" + self.formatException(exc)
        return s


# ---------- Filters / helpers ----------


class ContextFilter(logging.Filter):
    """Inject current log context into LogRecord for downstream handlers."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _log_context.get()
        if ctx:
            for k, v in ctx.items():
                if k not in record.__dict__:
                    record.__dict__[k] = v
        return True


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


class _MinLevelFilter(logging.Filter):
    def __init__(self, min_level: int) -> None:
        self.min_level = min_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= self.min_level


class _KwExtraAdapter(logging.LoggerAdapter):
    """
    Logger adapter that moves any unknown kwargs into `extra={...}` so you can write:

        log.info("msg", event="...", task_id=..., node_id=...)

    without TypeError from the logging module.
    """

    _allowed_passthrough: ClassVar[frozenset[str]] = frozenset({"exc_info", "stack_info", "stacklevel", "extra"})
    _logrecord_attrs: ClassVar[frozenset[str]] = _STD_ATTRS

    def process(self, msg, kwargs):
        extra = kwargs.get("extra")
        if extra is None or not isinstance(extra, dict):
            extra = {}

        moved = {}
        for k in list(kwargs.keys()):
            if k in self._allowed_passthrough:
                continue
            moved[k] = kwargs.pop(k)

        for k, v in moved.items():
            key = k
            if key in self._logrecord_attrs:
                key = f"field_{key}"
            if key not in extra:
                extra[key] = v

        kwargs["extra"] = extra
        return msg, kwargs


_WARN_ONCE_SEEN: set[str] = set()
_WARN_ONCE_LOCK = threading.Lock()


def warn_once(
    logger: logging.Logger | logging.LoggerAdapter,
    code: str,
    msg: str,
    *,
    level: int = logging.WARNING,
    **extra: Any,
) -> None:
    """Log a message only once per process for the given code."""
    with _WARN_ONCE_LOCK:
        if code in _WARN_ONCE_SEEN:
            return
        _WARN_ONCE_SEEN.add(code)
    adapter: logging.LoggerAdapter
    if isinstance(logger, logging.LoggerAdapter):
        adapter = logger
    else:
        adapter = _KwExtraAdapter(logger, {})
    adapter.log(level, msg, code=code, **extra)


# ---------- Public configuration API ----------

_FLOWKIT_LOGGER_NAME = "flowkit"
_configured = False
_stdout_handler_key = "_flowkit_stdout_handler"
_stderr_handler_key = "_flowkit_stderr_handler"


def _bootstrap_minimal() -> None:
    """Install a NullHandler and a context filter to keep library silent by default."""
    global _configured
    if _configured:
        return
    lg = logging.getLogger(_FLOWKIT_LOGGER_NAME)
    lg.setLevel(logging.DEBUG)
    if not any(isinstance(h, logging.NullHandler) for h in lg.handlers):
        lg.addHandler(logging.NullHandler())
    if not any(isinstance(f, ContextFilter) for f in lg.filters):
        lg.addFilter(ContextFilter())
    _configured = True


def get_logger(name: str | None = None) -> logging.LoggerAdapter:
    """
    Return a namespaced logger adapter that accepts arbitrary keyword fields.
    The base logger is silent by default; call `enable_stdout_logging()` in apps/tests.
    """
    _bootstrap_minimal()
    base = logging.getLogger(_FLOWKIT_LOGGER_NAME)
    target = base.getChild(name) if name else base
    return _KwExtraAdapter(target, {})


def _resolve_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        val = getattr(logging, level.upper(), None)
        if isinstance(val, int):
            return val
    raise ValueError("Invalid level name")


def set_level(level: int | str) -> None:
    """Change library logger level at runtime (affects all children)."""
    logging.getLogger(_FLOWKIT_LOGGER_NAME).setLevel(_resolve_level(level))


def enable_stdout_logging(
    *,
    level: int | str = logging.DEBUG,
    json_output: bool = True,
    include_stack: bool = False,
    pretty: bool = False,
    route_errors_to_stderr: bool = False,
) -> None:
    """
    Attach stream handlers for tests/local/containers.

    - json_output=True -> JsonFormatter; pretty=True -> HumanFormatter
    - route_errors_to_stderr=True -> ERROR+ to stderr, others to stdout
    """
    lvl = _resolve_level(level)
    _bootstrap_minimal()
    lg = logging.getLogger(_FLOWKIT_LOGGER_NAME)

    disable_stdout_logging()  # clear previous

    fmt: logging.Formatter
    if pretty:
        fmt = HumanFormatter()
    elif json_output:
        fmt = JsonFormatter(include_stack=include_stack)
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    if route_errors_to_stderr:
        # stdout: <= WARNING
        h_out = logging.StreamHandler(sys.stdout)
        h_out.set_name(_stdout_handler_key)
        h_out.setLevel(lvl)
        h_out.addFilter(_MaxLevelFilter(logging.WARNING))
        h_out.setFormatter(fmt)
        lg.addHandler(h_out)

        # stderr: >= ERROR
        h_err = logging.StreamHandler(sys.stderr)
        h_err.set_name(_stderr_handler_key)
        h_err.setLevel(max(lvl, logging.ERROR))
        h_err.addFilter(_MinLevelFilter(logging.ERROR))
        h_err.setFormatter(fmt)
        lg.addHandler(h_err)
    else:
        h = logging.StreamHandler(sys.stdout)
        h.set_name(_stdout_handler_key)
        h.setLevel(lvl)
        h.setFormatter(fmt)
        lg.addHandler(h)


def disable_stdout_logging() -> None:
    """Detach previously installed stdout/stderr handlers, if present."""
    lg = logging.getLogger(_FLOWKIT_LOGGER_NAME)
    for h in list(lg.handlers):
        if h.get_name() in (_stdout_handler_key, _stderr_handler_key):
            lg.removeHandler(h)


def configure_from_env() -> None:
    """
    Optional convenience config for apps/tests.

    Env:
      - FLOWKIT_LOG_STDOUT=1|true
      - FLOWKIT_LOG_LEVEL=DEBUG|INFO|...
      - FLOWKIT_LOG_PRETTY=1
      - FLOWKIT_LOG_STACK=1
    """
    want_stdout = os.getenv("FLOWKIT_LOG_STDOUT", "").lower() in ("1", "true", "yes", "on")
    level = os.getenv("FLOWKIT_LOG_LEVEL", "DEBUG")
    pretty = os.getenv("FLOWKIT_LOG_PRETTY", "").lower() in ("1", "true", "yes", "on")
    with_stack = os.getenv("FLOWKIT_LOG_STACK", "").lower() in ("1", "true", "yes", "on")

    _bootstrap_minimal()
    set_level(level)

    if want_stdout:
        enable_stdout_logging(level=level, json_output=not pretty, include_stack=with_stack, pretty=pretty)
    else:
        disable_stdout_logging()


@contextmanager
def swallow(
    *,
    logger: logging.Logger | logging.LoggerAdapter | None = None,
    level: int = logging.DEBUG,
    code: str,
    msg: str | None = None,
    reraise: bool = False,
    return_value: Any = None,
    extra: Mapping[str, Any] | None = None,
    expected: bool = True,
):
    """
    Replace `try/except: pass` with structured logging.

    Example:
        with swallow(logger=log, code="db.index.ensure", msg="Index creation failed"):
            await db.create_index(...)
    """
    base_logger = logger or get_logger("swallow")
    adapter = base_logger if isinstance(base_logger, logging.LoggerAdapter) else _KwExtraAdapter(base_logger, {})
    try:
        yield
    except Exception:
        payload: dict[str, Any] = {"code": code, "expected": expected}
        if extra:
            payload.update(dict(extra))
        adapter.log(level, msg or "Suppressed exception", exc_info=True, **payload)
        if reraise:
            raise
        return return_value


# Initialize minimal config on import (silent by default).
_bootstrap_minimal()
