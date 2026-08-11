from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return iso_utc(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [jsonable(item) for item in value]
    return value


@dataclass(frozen=True)
class ProcessInfo:
    pid: int
    ppid: int
    name: str
    command_line: str
    created_utc: datetime | None
    process_group_id: int | None = None
    session_id: int | None = None
    boundary_id: str | None = None


@dataclass(frozen=True)
class ProcessBoundaryInventory:
    """One complete or explicitly incomplete ownership-boundary observation."""

    complete: bool
    boundary_kind: str
    boundary_identity: str | None
    processes: tuple[ProcessInfo, ...] = ()
    observed_processes: tuple[ProcessInfo, ...] = ()
    errors: tuple[str, ...] = ()
    source: str = "unknown"
    cleanup: str | None = None

    @property
    def live_processes(self) -> tuple[ProcessInfo, ...]:
        return self.processes

    def to_record(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "boundary_kind": self.boundary_kind,
            "boundary_identity": self.boundary_identity,
            "processes": [jsonable(item) for item in self.processes],
            "observed_processes": [jsonable(item) for item in self.observed_processes],
            "errors": list(self.errors),
            "source": self.source,
            "cleanup": self.cleanup,
        }


@dataclass(frozen=True)
class ProcessQuery:
    """Result of a targeted query for one known process identity."""

    complete: bool
    process: ProcessInfo | None
    errors: tuple[str, ...] = ()

    def parent_matches(self, parent_pid: int) -> bool | None:
        if not self.complete or self.process is None:
            return None
        return self.process.ppid == parent_pid


@dataclass(frozen=True)
class ProcessSnapshot:
    complete: bool
    processes: tuple[ProcessInfo, ...]
    errors: tuple[str, ...] = ()
    provider: str = "unknown"

    @cached_property
    def by_pid(self) -> dict[int, ProcessInfo]:
        return {process.pid: process for process in self.processes}

    @property
    def pid_index(self) -> dict[int, ProcessInfo]:
        return self.by_pid

    def process_for(self, pid: int | None) -> ProcessInfo | None:
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return None
        return self.by_pid.get(pid)

    def parent_matches(self, child_pid: int, parent_pid: int) -> bool | None:
        process = self.process_for(child_pid)
        if process is None:
            return None if not self.complete else False
        return process.ppid == parent_pid


@dataclass(frozen=True)
class RequestFacts:
    """Independent observations from which request state is derived."""

    lifetime_state: str = "UNKNOWN"
    relay_state: str = "ABSENT"
    mcp_lifetime_state: str = "NOT_DECLARED"
    expiry_bucket: str = "UNKNOWN"
    mcp_declared: bool = False
    explicit_mcp_lifetime: bool = False
    sidecar_matches: bool | None = None
    process_states: tuple[str, ...] = ()
    resource_ambiguity: tuple[str, ...] = ()

    @property
    def operator_summary(self) -> str:
        if self.relay_state == "BOUND_EXPIRED":
            return "RELAYED_EXPIRED"
        if self.mcp_declared and not self.explicit_mcp_lifetime:
            return "REQUEST_AMBIGUOUS"
        if self.sidecar_matches is False:
            return "REQUEST_AMBIGUOUS"
        if self.relay_state == "BOUND" and self.lifetime_state == "LIVE":
            return "RELAYED"
        if self.relay_state == "BOUND" and self.lifetime_state == "ABSENT":
            return "RELAYED_INACTIVE"
        if self.relay_state == "BOUND":
            return "RELAYED_AMBIGUOUS"
        if self.relay_state == "UNBOUND":
            return "RELAY_UNBOUND"
        if self.lifetime_state == "LIVE":
            return "RELAY_READY"
        if self.lifetime_state == "ABSENT":
            return "REQUEST_STALE"
        return "REQUEST_AMBIGUOUS"


@dataclass(frozen=True)
class StableBytes:
    path: Path
    data: bytes
    sha256: str
    size: int
    mtime_ns: int
    file_id: tuple[int, int]


@dataclass(frozen=True)
class ObservationError:
    path: str
    code: str
    detail: str
