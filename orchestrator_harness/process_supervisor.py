"""Exact, reusable child cleanup for coding lane controllers."""
from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping

from .models import ProcessInfo, ProcessQuery, iso_utc, parse_utc
from .processes import targeted_process_query


PROCESS_CLEANUP_SCHEMA = "orchestrator-process-cleanup/v1"


def _identity_from(value: ProcessInfo | Mapping[str, Any] | None) -> ProcessInfo | None:
    if isinstance(value, ProcessInfo):
        return value if value.pid > 0 and value.created_utc is not None else None
    if not isinstance(value, Mapping):
        return None
    pid = value.get("pid")
    ppid = value.get("ppid", 0)
    created = parse_utc(value.get("created_utc"))
    if (
        not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0
        or not isinstance(ppid, int) or isinstance(ppid, bool)
        or created is None
    ):
        return None
    return ProcessInfo(pid, ppid, str(value.get("name") or ""), str(value.get("command_line") or ""), created)


@dataclass(frozen=True)
class CleanupResult:
    """Typed evidence for every cleanup stage and the final reap proof."""

    pid: int
    expected_created_utc: str | None
    status: str
    stages: tuple[str, ...] = ()
    terminate_attempted: bool = False
    kill_attempted: bool = False
    final_reap: bool = False
    cleanup_confirmed: bool = False
    identity_verified: bool = False
    identity_uncertain: bool = False
    exit_code: int | None = None
    reaped_after: str | None = None
    errors: tuple[str, ...] = ()

    @property
    def proved_reap(self) -> bool:
        return self.cleanup_confirmed and self.final_reap and not self.identity_uncertain

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema": PROCESS_CLEANUP_SCHEMA,
            "pid": self.pid,
            "provider_pid": self.pid,
            "codex_pid": self.pid,
            "expected_created_utc": self.expected_created_utc,
            "status": self.status,
            "stages": list(self.stages),
            "terminate_attempted": self.terminate_attempted,
            "kill_attempted": self.kill_attempted,
            "final_reap": self.final_reap,
            "cleanup_confirmed": self.cleanup_confirmed,
            "identity_verified": self.identity_verified,
            "identity_uncertain": self.identity_uncertain,
            "exit_code": self.exit_code,
            "reaped_after": self.reaped_after,
            "errors": list(self.errors),
        }
        return record


@dataclass
class ProcessSupervisor:
    """Own one Popen handle and prove reap before a caller releases resources."""

    process: Any
    identity: ProcessInfo | Mapping[str, Any] | None
    graceful_timeout_seconds: float = 5.0
    force_timeout_seconds: float | None = None
    parent_pid: int | None = None
    observer: Callable[..., ProcessQuery] | None = targeted_process_query
    _identity_record: ProcessInfo | None = field(init=False, repr=False)
    _reaped: bool = field(init=False, default=False, repr=False)
    _exit_code: int | None = field(init=False, default=None, repr=False)
    _result: CleanupResult | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        if self.graceful_timeout_seconds < 0:
            raise ValueError("graceful timeout cannot be negative")
        if self.force_timeout_seconds is None:
            self.force_timeout_seconds = self.graceful_timeout_seconds
        if self.force_timeout_seconds < 0:
            raise ValueError("force timeout cannot be negative")
        self._identity_record = _identity_from(self.identity)

    @property
    def pid(self) -> int:
        value = getattr(self.process, "pid", 0)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else 0

    @property
    def creation_identity(self) -> str | None:
        return iso_utc(self._identity_record.created_utc) if self._identity_record is not None else None

    def _observation(self) -> tuple[bool, str | None]:
        """Return whether external exact observation corroborates the bound child."""
        identity = self._identity_record
        if identity is None or identity.pid != self.pid:
            return False, "owned process creation identity is unavailable"
        if self.observer is None:
            return True, None
        try:
            query = self.observer(self.pid, expected_parent_pid=self.parent_pid)
        except TypeError:
            query = self.observer(self.pid)
        except BaseException as exc:
            return False, f"exact process observation failed: {type(exc).__name__}: {exc}"
        if not isinstance(query, ProcessQuery):
            return False, "exact process observation returned an invalid result"
        if not query.complete:
            # An incomplete inventory is uncertainty, not safe absence.  The
            # owned handle is not enough to release an external claim when
            # creation identity cannot be independently revalidated.
            return False, "exact process observation is incomplete"
        if query.process is None:
            try:
                if self.process.poll() is None:
                    return False, "exact observation says child is absent while owned handle is live"
            except BaseException as exc:
                return False, f"cannot reconcile absent child observation: {type(exc).__name__}: {exc}"
            return True, None
        if query.process.pid != identity.pid:
            return False, "observed child PID does not match owned Popen PID"
        if query.process.created_utc != identity.created_utc:
            return False, "observed child creation identity does not match owned Popen identity"
        if self.parent_pid is not None and query.process.ppid != self.parent_pid:
            return False, "observed child parent does not match controller identity"
        return True, None

    def _result_for(
        self,
        *,
        status: str,
        stages: list[str],
        terminate_attempted: bool,
        kill_attempted: bool,
        final_reap: bool,
        identity_verified: bool,
        identity_uncertain: bool,
        reaped_after: str | None,
        errors: list[str],
    ) -> CleanupResult:
        return CleanupResult(
            pid=self.pid,
            expected_created_utc=self.creation_identity,
            status=status,
            stages=tuple(stages),
            terminate_attempted=terminate_attempted,
            kill_attempted=kill_attempted,
            final_reap=final_reap,
            cleanup_confirmed=final_reap and not identity_uncertain,
            identity_verified=identity_verified,
            identity_uncertain=identity_uncertain,
            exit_code=self._exit_code,
            reaped_after=reaped_after,
            errors=tuple(errors),
        )

    def wait_for_exit(self) -> int:
        """Wait normally and bind the observed exit to the owned handle."""
        if self._reaped:
            return int(self._exit_code or 0)
        identity_ok, reason = self._observation()
        if not identity_ok:
            raise RuntimeError(reason or "child identity is uncertain")
        result = self.process.wait()
        self._exit_code = int(result) if isinstance(result, int) else None
        self._reaped = True
        return int(result)

    def cleanup(self) -> CleanupResult:
        """Gracefully stop, force stop if needed, then perform a final reap."""
        if self._result is not None:
            return self._result
        stages: list[str] = []
        errors: list[str] = []
        terminate_attempted = False
        kill_attempted = False
        identity_ok, observation_note = self._observation()
        identity_uncertain = not identity_ok or self._identity_record is None
        identity_verified = identity_ok and self._identity_record is not None
        if observation_note is not None:
            errors.append(observation_note)
        if self._identity_record is None or self.pid <= 0:
            self._result = self._result_for(
                status="IDENTITY_UNCERTAIN", stages=stages,
                terminate_attempted=False, kill_attempted=False, final_reap=False,
                identity_verified=False, identity_uncertain=True, reaped_after=None, errors=errors,
            )
            return self._result
        if not identity_ok:
            self._result = self._result_for(
                status="IDENTITY_UNCERTAIN", stages=stages,
                terminate_attempted=False, kill_attempted=False, final_reap=False,
                identity_verified=False, identity_uncertain=True, reaped_after=None, errors=errors,
            )
            return self._result
        try:
            already_exited = self.process.poll()
        except BaseException as exc:
            already_exited = None
            errors.append(f"initial poll failed: {type(exc).__name__}: {exc}")
        if already_exited is not None:
            stages.append("ALREADY_EXITED")
            try:
                self._exit_code = self.process.wait(timeout=0)
                self._reaped = True
                stages.append("FINAL_REAP")
                self._result = self._result_for(
                    status="REAPED", stages=stages,
                    terminate_attempted=False, kill_attempted=False, final_reap=True,
                    identity_verified=identity_verified, identity_uncertain=identity_uncertain,
                    reaped_after="already_exited", errors=errors,
                )
                return self._result
            except BaseException as exc:
                errors.append(f"already-exited reap failed: {type(exc).__name__}: {exc}")

        stages.append("GRACEFUL_STOP_REQUESTED")
        terminate_attempted = True
        try:
            self.process.terminate()
        except BaseException as exc:
            errors.append(f"graceful stop failed: {type(exc).__name__}: {exc}")
        try:
            self._exit_code = self.process.wait(timeout=self.graceful_timeout_seconds)
            self._reaped = True
            stages.append("GRACEFUL_WAIT")
            stages.append("FINAL_REAP")
            self._result = self._result_for(
                status="REAPED", stages=stages,
                terminate_attempted=terminate_attempted, kill_attempted=False, final_reap=True,
                identity_verified=identity_verified, identity_uncertain=identity_uncertain,
                reaped_after="graceful_stop", errors=errors,
            )
            return self._result
        except subprocess.TimeoutExpired:
            stages.append("GRACEFUL_WAIT_TIMEOUT")
        except BaseException as exc:
            errors.append(f"graceful wait failed: {type(exc).__name__}: {exc}")

        stages.append("FORCE_STOP_REQUESTED")
        kill_attempted = True
        try:
            self.process.kill()
        except BaseException as exc:
            errors.append(f"force stop failed: {type(exc).__name__}: {exc}")
        try:
            self._exit_code = self.process.wait(timeout=self.force_timeout_seconds)
            self._reaped = True
            stages.append("FINAL_REAP")
            self._result = self._result_for(
                status="REAPED", stages=stages,
                terminate_attempted=terminate_attempted, kill_attempted=kill_attempted, final_reap=True,
                identity_verified=identity_verified, identity_uncertain=identity_uncertain,
                reaped_after="force_stop", errors=errors,
            )
            return self._result
        except subprocess.TimeoutExpired:
            stages.append("FINAL_REAP_TIMEOUT")
        except BaseException as exc:
            errors.append(f"final reap failed: {type(exc).__name__}: {exc}")
        self._result = self._result_for(
            status="IDENTITY_UNCERTAIN" if identity_uncertain else "UNREAPED",
            stages=stages, terminate_attempted=terminate_attempted, kill_attempted=kill_attempted,
            final_reap=False, identity_verified=identity_verified, identity_uncertain=identity_uncertain,
            reaped_after=None, errors=errors,
        )
        return self._result


def supervise_process(
    process: Any,
    identity: ProcessInfo | Mapping[str, Any] | None,
    *,
    graceful_timeout_seconds: float = 5.0,
    observer: Callable[..., ProcessQuery] | None = targeted_process_query,
    parent_pid: int | None = None,
) -> CleanupResult:
    return ProcessSupervisor(
        process,
        identity,
        graceful_timeout_seconds=graceful_timeout_seconds,
        observer=observer,
        parent_pid=parent_pid,
    ).cleanup()


__all__ = [
    "CleanupResult",
    "PROCESS_CLEANUP_SCHEMA",
    "ProcessSupervisor",
    "supervise_process",
]
