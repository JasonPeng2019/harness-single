from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
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


@dataclass(frozen=True)
class ProcessSnapshot:
    complete: bool
    processes: tuple[ProcessInfo, ...]
    errors: tuple[str, ...] = ()
    provider: str = "unknown"

    @property
    def by_pid(self) -> dict[int, ProcessInfo]:
        return {process.pid: process for process in self.processes}


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
