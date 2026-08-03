from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import HarnessConfig
from .models import ObservationError, StableBytes
from .models import parse_utc
from .stable_io import read_stable, read_tail_stable


@dataclass(frozen=True)
class JsonRecord:
    path: Path
    stable: StableBytes
    value: dict[str, Any]
    sidecar_sha256: str | None = None
    sidecar_matches: bool | None = None


@dataclass(frozen=True)
class ControllerRecord:
    run_root: Path
    workspace: Path
    status: JsonRecord
    label: str
    jsonl_path: Path | None
    terminal_event: str | None
    terminal_mtime_ns: int | None


@dataclass(frozen=True)
class ManagerSignalRecord:
    path: Path
    stable: StableBytes
    value: dict[str, Any]


@dataclass(frozen=True)
class RunRecords:
    run_root: Path
    workspace: Path
    controllers: tuple[ControllerRecord, ...]
    requests: tuple[JsonRecord, ...]
    relays: tuple[JsonRecord, ...]
    helper_records: tuple[JsonRecord, ...]
    mcp_records: tuple[JsonRecord, ...]
    checkpoint: StableBytes | None
    result: JsonRecord | None
    manager_signals: tuple[ManagerSignalRecord, ...]
    errors: tuple[ObservationError, ...]


def _json_record(
    path: Path, config: HarnessConfig, *, check_sidecar: bool = False
) -> JsonRecord:
    stable = read_stable(
        path,
        max_bytes=config.max_json_bytes,
        retries=config.stable_read_retries,
        delay_seconds=config.stable_read_delay_seconds,
    )
    value = json.loads(stable.data.decode("utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError("JSON root must be an object")
    sidecar_sha = None
    sidecar_matches = None
    sidecar = Path(str(path) + ".sha256")
    if check_sidecar and sidecar.exists():
        sidecar_stable = read_stable(
            sidecar,
            max_bytes=4096,
            retries=config.stable_read_retries,
            delay_seconds=config.stable_read_delay_seconds,
        )
        text = sidecar_stable.data.decode("ascii", errors="replace").strip()
        token = text.split()[0].lower() if text else ""
        sidecar_sha = token if len(token) == 64 else None
        sidecar_matches = sidecar_sha == stable.sha256
    return JsonRecord(path, stable, value, sidecar_sha, sidecar_matches)


def _terminal_event(path: Path, config: HarnessConfig) -> tuple[str | None, int | None]:
    stable = read_tail_stable(
        path,
        max_bytes=config.max_jsonl_tail_bytes,
        retries=config.stable_read_retries,
        delay_seconds=config.stable_read_delay_seconds,
    )
    terminal = None
    for raw_line in stable.data.decode("utf-8", errors="replace").splitlines():
        try:
            item = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        kind = item.get("type") if isinstance(item, dict) else None
        if kind in {"turn.completed", "turn.failed", "turn.cancelled"}:
            terminal = str(kind)
    return terminal, stable.mtime_ns


def _looks_like_relay(path: Path, value: dict[str, Any]) -> bool:
    name = path.name.lower()
    if ".relay." in name or "-relay-" in name or "_relay_" in name:
        return True
    return str(value.get("decision", "")).lower() in {
        "approved",
        "rejected",
        "denied",
    } and any(key in value for key in ("request_sha256", "request_hash"))


_SIGNAL_KINDS = {"HELP", "FEEDBACK", "INSTRUCTION", "PASS", "CHECKPOINT"}


def _is_hidden_or_atomic_signal_file(path: Path) -> bool:
    """Return whether a JSON path is a publisher's non-final signal file.

    Signal publishers write an ordinary final ``*.json`` file.  Hidden names and
    ``*.tmp.json`` names are atomic-write staging files, not independently
    deliverable manager signals.
    """
    return path.name.startswith(".") or path.name.endswith(".tmp.json")


def _is_reparse_or_link(path: Path) -> bool:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode):
        return True
    return bool(getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _is_utc_timestamp(value: object) -> bool:
    return (
        isinstance(value, str)
        and (value.endswith("Z") or value.endswith("+00:00"))
        and parse_utc(value) is not None
    )


def _manager_signal(path: Path, signals_root: Path, config: HarnessConfig) -> ManagerSignalRecord:
    """Read one signal without following a link outside the signal directory."""
    if _is_reparse_or_link(path) or path.resolve().parent != signals_root.resolve():
        raise ValueError("signal path is not a regular file directly under manager-signals")
    record = _json_record(path, config)
    value = record.value
    required = ("schema", "signal_id", "kind", "created_utc", "lane_id", "summary")
    if any(not isinstance(value.get(key), str) or not value[key].strip() for key in required):
        raise ValueError("signal has missing or invalid required fields")
    if value["schema"] != "manager-signal/v1":
        raise ValueError("unsupported signal schema")
    if value["kind"] not in _SIGNAL_KINDS:
        raise ValueError("unsupported signal kind")
    for key in ("created_utc", "deadline_utc"):
        if key in value and not _is_utc_timestamp(value[key]):
            raise ValueError(f"invalid {key}")
    if "delivery_deadline_utc" in value:
        delivery_deadline = value["delivery_deadline_utc"]
        if not _is_utc_timestamp(delivery_deadline):
            raise ValueError("invalid delivery_deadline_utc")
        if parse_utc(delivery_deadline) < parse_utc(value["created_utc"]):
            raise ValueError("delivery_deadline_utc precedes created_utc")
    if "agent_blocked" in value and not isinstance(value["agent_blocked"], bool):
        raise ValueError("agent_blocked must be a boolean")
    if "attention_epoch_id" in value and (
        not isinstance(value["attention_epoch_id"], str)
        or not value["attention_epoch_id"].strip()
    ):
        raise ValueError("attention_epoch_id must be a non-empty string")
    evidence = value.get("evidence_paths", [])
    if not isinstance(evidence, list) or any(not isinstance(item, str) for item in evidence):
        raise ValueError("evidence_paths must be a list of strings")
    return ManagerSignalRecord(path, record.stable, value)


def _manager_root_records(
    root: Path,
    config: HarnessConfig,
    *,
    error_code: str,
    seen_paths: set[Path],
) -> tuple[list[JsonRecord], list[ObservationError]]:
    """Read current manager records without relaxing legacy request semantics."""
    records: list[JsonRecord] = []
    errors: list[ObservationError] = []
    if not root.exists():
        return records, errors
    try:
        if not root.is_dir() or _is_reparse_or_link(root):
            raise ValueError("manager record root is not a safe directory")
        resolved_root = root.resolve()
        for path in sorted(root.glob("*.json")):
            try:
                resolved = path.resolve()
                if (
                    resolved in seen_paths
                    or _is_reparse_or_link(path)
                    or resolved.parent != resolved_root
                ):
                    raise ValueError("manager record path is not a regular file directly under its root")
                sidecar = Path(str(path) + ".sha256")
                if sidecar.exists() and (
                    _is_reparse_or_link(sidecar)
                    or sidecar.resolve().parent != resolved_root
                ):
                    raise ValueError("manager record sidecar is not confined to its root")
                record = _json_record(path, config, check_sidecar=True)
                if sidecar.exists() and record.sidecar_matches is not True:
                    raise ValueError("manager record sidecar is invalid or does not match record bytes")
                seen_paths.add(resolved)
                records.append(record)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(ObservationError(str(path), error_code, str(exc)))
    except (OSError, ValueError) as exc:
        errors.append(ObservationError(str(root), error_code, str(exc)))
    return records, errors


def discover_run(
    run_root: Path,
    workspace: Path,
    config: HarnessConfig,
) -> RunRecords:
    errors: list[ObservationError] = []
    controllers: list[ControllerRecord] = []
    for path in sorted(workspace.glob("*_controller.status.json")):
        try:
            status = _json_record(path, config)
            label = path.name[: -len("_controller.status.json")]
            candidates = [
                workspace / f"{label}_codex.jsonl",
                workspace / "test_agent_codex.jsonl",
            ]
            jsonl = next((item for item in candidates if item.exists()), None)
            terminal = None
            terminal_mtime = None
            if jsonl:
                terminal, terminal_mtime = _terminal_event(jsonl, config)
            controllers.append(
                ControllerRecord(
                    run_root,
                    workspace,
                    status,
                    label,
                    jsonl,
                    terminal,
                    terminal_mtime,
                )
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                ObservationError(str(path), "CONTROLLER_READ_ERROR", str(exc))
            )

    requests: list[JsonRecord] = []
    relays: list[JsonRecord] = []
    request_dir = workspace / "permission-requests"
    if request_dir.is_dir():
        for path in sorted(request_dir.glob("*.json")):
            try:
                record = _json_record(path, config, check_sidecar=True)
                (relays if _looks_like_relay(path, record.value) else requests).append(
                    record
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(
                    ObservationError(str(path), "REQUEST_READ_ERROR", str(exc))
                )

    # Current suite lanes keep requests and relays in separate manager-owned roots.
    # Their sidecars are optional for atomic relays, but never ignored when present.
    seen_manager_paths: set[Path] = set()
    manager_requests, manager_request_errors = _manager_root_records(
        workspace / "manager-requests",
        config,
        error_code="REQUEST_READ_ERROR",
        seen_paths=seen_manager_paths,
    )
    manager_relays, manager_relay_errors = _manager_root_records(
        workspace / "manager-relays",
        config,
        error_code="RELAY_READ_ERROR",
        seen_paths=seen_manager_paths,
    )
    requests.extend(manager_requests)
    relays.extend(manager_relays)
    errors.extend(manager_request_errors)
    errors.extend(manager_relay_errors)

    helper_records: list[JsonRecord] = []
    helper_names = {"helper_process.json", "live-context.json"}
    mcp_records: list[JsonRecord] = []
    mcp_names = {"mcp_process.json", "mcp_processes.json", "mcp-lifetime.json"}
    record_names = helper_names | mcp_names
    recursive_candidates = sorted(
        Path(root) / name
        for root, directories, files in os.walk(workspace)
        for names in (directories, files)
        for name in names
        if name in record_names
    )
    for path in recursive_candidates:
        if path.name in helper_names:
            try:
                helper_records.append(_json_record(path, config))
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(ObservationError(str(path), "HELPER_READ_ERROR", str(exc)))
        if path.name in mcp_names:
            try:
                mcp_records.append(_json_record(path, config))
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(ObservationError(str(path), "MCP_READ_ERROR", str(exc)))

    checkpoint = None
    checkpoint_path = workspace / "PARALLEL_CHECKPOINT.md"
    if checkpoint_path.exists():
        try:
            checkpoint = read_stable(
                checkpoint_path,
                max_bytes=config.max_json_bytes,
                retries=config.stable_read_retries,
                delay_seconds=config.stable_read_delay_seconds,
            )
        except OSError as exc:
            errors.append(
                ObservationError(
                    str(checkpoint_path), "CHECKPOINT_READ_ERROR", str(exc)
                )
            )

    result = None
    result_path = workspace / "RESULT.json"
    if result_path.exists():
        try:
            result = _json_record(result_path, config)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                ObservationError(str(result_path), "RESULT_READ_ERROR", str(exc))
            )

    signals_root = workspace / "manager-signals"
    signal_ids: dict[str, list[ManagerSignalRecord]] = {}
    if signals_root.is_dir() and not _is_reparse_or_link(signals_root):
        for path in sorted(signals_root.glob("*.json")):
            if _is_hidden_or_atomic_signal_file(path):
                continue
            try:
                signal = _manager_signal(path, signals_root, config)
                signal_id = str(signal.value["signal_id"])
                signal_ids.setdefault(signal_id, []).append(signal)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(ObservationError(str(path), "MANAGER_SIGNAL_READ_ERROR", str(exc)))
    elif signals_root.exists():
        errors.append(ObservationError(str(signals_root), "MANAGER_SIGNAL_READ_ERROR", "manager-signals is not a safe directory"))

    manager_signals: list[ManagerSignalRecord] = []
    for signal_id, records in sorted(signal_ids.items()):
        if len({record.stable.sha256 for record in records}) == 1:
            manager_signals.append(records[0])
            continue
        for record in records:
            errors.append(
                ObservationError(
                    str(record.path),
                    "MANAGER_SIGNAL_READ_ERROR",
                    f"conflicting duplicate signal_id {signal_id}",
                )
            )

    return RunRecords(
        run_root=run_root,
        workspace=workspace,
        controllers=tuple(controllers),
        requests=tuple(requests),
        relays=tuple(relays),
        helper_records=tuple(helper_records),
        mcp_records=tuple(mcp_records),
        checkpoint=checkpoint,
        result=result,
        manager_signals=tuple(manager_signals),
        errors=tuple(errors),
    )


def discover_suite(config: HarnessConfig) -> tuple[RunRecords, ...]:
    roots: dict[str, Path] = {}
    for pattern in config.run_globs:
        for candidate in config.suite_root.glob(pattern):
            if candidate.is_dir():
                roots[str(candidate.resolve()).lower()] = candidate.resolve()
    runs = []
    for run_root in sorted(roots.values(), key=lambda item: str(item).lower()):
        workspace = run_root / config.workspace_relpath
        if workspace.is_dir():
            runs.append(discover_run(run_root, workspace, config))
    return tuple(runs)
