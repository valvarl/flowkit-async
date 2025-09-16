from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import subprocess as subproc  # nosec B404 - used only for CREATE_NEW_PROCESS_GROUP; no shell execution
from collections.abc import Callable
from typing import Any

from ..core.log import get_logger, swallow
from ..core.time import Clock


class RunContext:
    """
    Cancellation-aware runtime utilities for handlers:
    - shared cancel flag + meta (reason, deadline ts)
    - cancellable awaits
    - subprocess group termination with escalation
    - resource cleanup registry
    """

    def __init__(
        self,
        *,
        cancel_flag: asyncio.Event,
        cancel_meta: dict[str, Any],
        artifacts_writer,
        clock: Clock,
        task_id: str,
        node_id: str,
        attempt_epoch: int,
        worker_id: str,
    ):
        self._cancel_flag = cancel_flag
        self._cancel_meta = cancel_meta
        self.artifacts = artifacts_writer
        self.clock = clock

        self.task_id = task_id
        self.node_id = node_id
        self.attempt_epoch = attempt_epoch
        self.worker_id = worker_id

        self.kv: dict[str, Any] = {}
        self.log = get_logger("worker.ctx")
        self._cleanup_callbacks: list[Callable[[], Any]] = []
        self._subprocesses: list[Any] = []
        self._temp_paths: list[str] = []

    # ---- cancellation
    def cancelled(self) -> bool:
        return self._cancel_flag.is_set()

    @property
    def cancel_reason(self) -> str | None:
        return self._cancel_meta.get("reason")

    @property
    def cancel_deadline_ts_ms(self) -> int | None:
        return self._cancel_meta.get("deadline_ts_ms")

    def remaining_ms(self) -> float | None:
        dl = self.cancel_deadline_ts_ms
        if not dl:
            return None
        return max(0.0, float(dl - self.clock.now_ms()))

    async def raise_if_cancelled(self) -> None:
        if self._cancel_flag.is_set():
            raise asyncio.CancelledError()

    async def cancellable(self, coro: Any) -> Any:
        if self._cancel_flag.is_set():
            raise asyncio.CancelledError()
        task = asyncio.create_task(coro)
        done, pending = await asyncio.wait(
            {task, asyncio.create_task(self._cancel_flag.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            try:
                return await task
            finally:
                for p in pending:
                    p.cancel()
        else:
            task.cancel()
            try:
                await task  # drain cancellation
            except asyncio.CancelledError:
                # expected when our own cancellation wins the race
                self.log.debug("ctx.cancellable.await_cancelled", event="ctx.cancellable.await_cancelled")
            except Exception as e:
                # task could have raced to a different exception; cancellation still wins
                self.log.debug("ctx.cancellable.await_exc", event="ctx.cancellable.await_exc", error=str(e))
            raise asyncio.CancelledError()

    # ---- subprocess helpers
    async def run_subprocess(self, *cmd: str, grace_ms: int = 5000) -> int:
        kwargs: dict[str, Any] = {}
        if os.name == "posix":
            kwargs["preexec_fn"] = os.setsid
        else:
            kwargs["creationflags"] = getattr(subproc, "CREATE_NEW_PROCESS_GROUP", 0)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs
        )
        self.register_subprocess(proc)

        try:
            await self.cancellable(proc.wait())
            return proc.returncode or 0
        except asyncio.CancelledError:
            try:
                await self._terminate_process(proc, grace_ms=grace_ms)
            finally:
                raise

    def register_subprocess(self, proc: Any) -> None:
        self._subprocesses.append(proc)

    def register_temp_path(self, path: str) -> None:
        self._temp_paths.append(path)

    def on_cleanup(self, cb: Callable[[], Any]) -> None:
        self._cleanup_callbacks.append(cb)

    async def _terminate_process(self, proc: Any, *, grace_ms: int = 5000) -> None:
        if proc.returncode is not None:
            return
        if os.name == "posix":
            with swallow(
                logger=self.log, code="ctx.proc.killpg", msg="killpg SIGTERM failed", level=logging.DEBUG, expected=True
            ):
                os.killpg(proc.pid, signal.SIGTERM)
            with swallow(
                logger=self.log,
                code="ctx.proc.terminate",
                msg="proc.terminate failed",
                level=logging.DEBUG,
                expected=True,
            ):
                proc.terminate()
        else:
            with swallow(
                logger=self.log,
                code="ctx.proc.ctrl_break",
                msg="CTRL_BREAK_EVENT failed",
                level=logging.DEBUG,
                expected=True,
            ):
                proc.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
            with swallow(
                logger=self.log,
                code="ctx.proc.terminate.win",
                msg="proc.terminate failed (win)",
                level=logging.DEBUG,
                expected=True,
            ):
                proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=max(0.001, grace_ms / 1000.0))
        except TimeoutError:
            with swallow(
                logger=self.log,
                code="ctx.proc.kill",
                msg="proc.kill failed after timeout",
                level=logging.DEBUG,
                expected=True,
            ):
                proc.kill()

    async def _cleanup_resources(self) -> None:
        for cb in reversed(self._cleanup_callbacks):
            try:
                res = cb()
                if asyncio.iscoroutine(res):
                    await res
            except asyncio.CancelledError:
                # Cleanup during shutdown should not propagate cancellation.
                self.log.debug("ctx.cleanup.cb.cancelled", event="ctx.cleanup.cb.cancelled")
            except Exception as e:
                self.log.debug("ctx.cleanup.cb.error", event="ctx.cleanup.cb.error", error=str(e))
        for proc in list(self._subprocesses):
            with swallow(
                logger=self.log,
                code="ctx.cleanup.proc_terminate",
                msg="terminate subprocess failed",
                level=logging.DEBUG,
                expected=True,
            ):
                await self._terminate_process(proc, grace_ms=1000)
        self._subprocesses.clear()

        for p in list(self._temp_paths):
            if os.path.isdir(p):
                # Best-effort recursive removal
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                with swallow(
                    logger=self.log,
                    code="ctx.cleanup.unlink",
                    msg="remove temp file failed",
                    level=logging.DEBUG,
                    expected=True,
                ):
                    os.remove(p)
        self._temp_paths.clear()
