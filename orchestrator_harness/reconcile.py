from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import HarnessConfig, path_identity, same_path
from .discovery import ControllerRecord, JsonRecord, RunRecords
from .models import (
    ProcessInfo,
    ProcessSnapshot,
    RequestFacts,
    iso_utc,
    jsonable,
    parse_utc,
)


_WINDOWS_DATE_MILLISECONDS = re.compile(r"^/Date\((-?\d+)\)/$")


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _controller_role(controller: ControllerRecord) -> str:
    status = controller.status.value
    doer = status.get("doer")
    if isinstance(doer, str) and doer.strip():
        return doer.strip()
    thread = status.get("thread_id")
    if isinstance(thread, str) and thread.strip():
        return f"session:{thread.strip()}"
    return controller.label


def _lane_id(run: RunRecords, controller: ControllerRecord) -> str:
    status = controller.status.value
    declared = status.get("declared_lane_id")
    if isinstance(declared, str) and declared.strip():
        return declared.strip()
    doer = _controller_role(controller)
    task = str(status.get("task") or run.run_root.name.split("_", 1)[0])
    return f"{run.run_root.name}:{doer}:{task}"


def _attempt_group_id(run: RunRecords, controller: ControllerRecord) -> str:
    """Identify persistent attempts before choosing the public lane label."""
    status = controller.status.value
    thread_id = status.get("thread_id")
    task = str(status.get("task") or run.run_root.name.split("_", 1)[0])
    if isinstance(thread_id, str) and thread_id.strip():
        return f"thread:{run.run_root.name}:{task}:{thread_id.strip()}"
    return _lane_id(run, controller)


def _process_identity(
    snapshot: ProcessSnapshot,
    *,
    pid: int | None,
    expected_started: datetime | None,
    tolerance_seconds: int,
) -> tuple[str, ProcessInfo | None, str]:
    if not pid or pid <= 0:
        return "unknown", None, "PID absent from observation"
    process = snapshot.process_for(pid)
    if not snapshot.complete and snapshot.provider != "linux-proc":
        return "unknown", process, "process snapshot is incomplete"
    if process is None:
        if not snapshot.complete:
            return "unknown", None, f"PID {pid} is not observed in a partial snapshot"
        return "absent", None, f"PID {pid} is absent"
    if process.created_utc is None or expected_started is None:
        return "unknown", process, "creation time cannot be compared"
    delta = abs((process.created_utc - expected_started).total_seconds())
    if delta > tolerance_seconds:
        return (
            "mismatch",
            process,
            f"PID {pid} creation differs from recorded start by {delta:.3f}s",
        )
    return "live", process, "PID and creation time match"


def _controller_observation(
    controller: ControllerRecord,
    snapshot: ProcessSnapshot,
    config: HarnessConfig,
) -> dict[str, Any]:
    raw = controller.status.value
    started = parse_utc(raw.get("started_utc"))
    controller_started = parse_utc(raw.get("controller_started_utc"))
    codex_started = parse_utc(raw.get("codex_started_utc"))
    controller_pid_value = raw.get("controller_pid")
    try:
        controller_pid = (
            int(controller_pid_value) if controller_pid_value is not None else None
        )
    except (TypeError, ValueError):
        controller_pid = None
    codex_pid_value = raw.get("codex_pid")
    try:
        codex_pid = int(codex_pid_value) if codex_pid_value is not None else None
    except (TypeError, ValueError):
        codex_pid = None

    controller_identity, controller_process, controller_reason = _process_identity(
        snapshot,
        pid=controller_pid,
        expected_started=controller_started,
        tolerance_seconds=config.process_start_tolerance_seconds,
    )
    codex_identity, codex_process, codex_reason = _process_identity(
        snapshot,
        pid=codex_pid,
        expected_started=codex_started,
        tolerance_seconds=config.process_start_tolerance_seconds,
    )
    parent_state = "unknown"
    parent_reason = "process identity unavailable"
    if controller_identity == "live" and codex_identity == "live":
        assert controller_process is not None and codex_process is not None
        if codex_process.ppid != controller_process.pid:
            parent_state = "mismatch"
            parent_reason = f"Codex parent {codex_process.ppid} != controller {controller_process.pid}"
        elif (
            controller_process.created_utc is None or codex_process.created_utc is None
        ):
            parent_state = "unknown"
            parent_reason = "parent creation time unavailable"
        elif controller_process.created_utc > codex_process.created_utc:
            parent_state = "mismatch"
            parent_reason = "controller incarnation began after Codex child"
        else:
            parent_state = "match"
            parent_reason = "PID and creation-time parent identity match"

    declared = str(raw.get("state") or "unknown").lower()
    terminal = controller.terminal_event
    if declared == "waiting_resource":
        if controller_identity == "live":
            operational = "WAITING_RESOURCE"
            reason = "controller is waiting before Codex launch"
        elif controller_identity == "unknown":
            operational = "PROCESS_STATE_UNKNOWN"
            reason = "resource-wait controller identity cannot be proved"
        else:
            operational = "STALE_STATUS"
            reason = "resource-wait declaration contradicts controller identity"
    elif declared in {"running", "running_codex"}:
        if terminal:
            operational = "STALE_STATUS"
            reason = f"terminal JSONL event {terminal} contradicts declared running"
        elif (
            controller_identity == "live"
            and codex_identity == "live"
            and parent_state == "match"
        ):
            operational = "RUNNING_CODEX"
            reason = "controller and Codex identities are live"
        elif (
            controller_identity == "unknown"
            or codex_identity == "unknown"
            or (
                controller_identity == "live"
                and codex_identity == "live"
                and parent_state == "unknown"
            )
        ):
            operational = "PROCESS_STATE_UNKNOWN"
            reason = "running declaration cannot be reconciled completely"
        else:
            operational = "STALE_STATUS"
            reason = "running declaration contradicts process identity"
    elif declared in {
        "exited",
        "codex_exited",
        "controller_interrupted",
        "controller_failed",
        "coordination_failed",
        "launch_failed",
        "completed",
        "done",
        "error",
        "failed",
        "terminated",
        "killed",
        "cancelled",
    }:
        identities = {controller_identity, codex_identity}
        if "live" in identities:
            operational = "STALE_STATUS"
            reason = f"declared {declared} contradicts a live recorded process"
        elif "unknown" in identities:
            operational = "PROCESS_STATE_UNKNOWN"
            reason = f"declared {declared} cannot prove recorded processes absent"
        elif identities.issubset({"absent", "mismatch"}):
            operational = "EXITED"
            reason = (
                f"controller declared {declared} and recorded identities are absent"
            )
        else:
            operational = "PROCESS_STATE_UNKNOWN"
            reason = f"declared {declared} has incomplete process identity evidence"
    elif (
        controller_identity == "live"
        and codex_identity == "live"
        and parent_state == "match"
    ):
        operational = "RUNNING_CODEX"
        reason = "live identities override unknown declaration"
    elif (
        controller_identity == "unknown"
        or codex_identity == "unknown"
        or (
            controller_identity == "live"
            and codex_identity == "live"
            and parent_state == "unknown"
        )
    ):
        operational = "PROCESS_STATE_UNKNOWN"
        reason = "process identity cannot be proved"
    else:
        operational = "UNKNOWN"
        reason = "no authoritative operational state"

    return {
        "label": controller.label,
        "status_path": str(controller.status.path),
        "status_sha256": controller.status.stable.sha256,
        "declared_state": declared,
        "operational_state": operational,
        "reason": reason,
        "doer": _controller_role(controller),
        "task": str(raw.get("task") or ""),
        "phase": str(raw.get("phase") or ""),
        "thread_id": raw.get("thread_id"),
        "started_utc": iso_utc(started),
        "ended_utc": raw.get("ended_utc"),
        "controller_pid": controller_pid,
        "controller_identity": controller_identity,
        "controller_reason": controller_reason,
        "controller_created_utc": iso_utc(
            controller_process.created_utc if controller_process else None
        ),
        "controller_started_utc": iso_utc(controller_started),
        "codex_pid": codex_pid,
        "codex_identity": codex_identity,
        "codex_reason": codex_reason,
        "codex_created_utc": iso_utc(
            codex_process.created_utc if codex_process else None
        ),
        "codex_started_utc": iso_utc(codex_started),
        "parent_state": parent_state,
        "parent_reason": parent_reason,
        "terminal_event": terminal,
        "invocation_schema": raw.get("invocation_schema"),
        "worker_invocation_id": raw.get("worker_invocation_id"),
        "repository": raw.get("repository"),
        "declared_resources": list(raw.get("resources", []))
        if isinstance(raw.get("resources"), list)
        else [],
        "resource_lock_root": raw.get("resource_lock_root"),
        "held_resource_claims": list(raw.get("held_resource_claims", []))
        if isinstance(raw.get("held_resource_claims"), list)
        else [],
        "waiting_resource_claim": raw.get("waiting_resource_claim")
        if isinstance(raw.get("waiting_resource_claim"), dict)
        else None,
        "resource_claim_findings": list(raw.get("resource_claim_findings", []))
        if isinstance(raw.get("resource_claim_findings"), list)
        else [],
        "result_validation": raw.get("result_validation")
        if isinstance(raw.get("result_validation"), dict)
        else None,
        "result_valid": raw.get("result_valid")
        if isinstance(raw.get("result_valid"), bool)
        else None,
        "coordination_failure": raw.get("coordination_failure")
        if isinstance(raw.get("coordination_failure"), dict)
        else None,
        "jsonl_path": str(controller.jsonl_path) if controller.jsonl_path else None,
        "board_tokens": sorted(
            str(item) for item in raw.get("board_tokens", []) if item is not None
        )
        if isinstance(raw.get("board_tokens", []), list)
        else [],
        "mcp_servers": sorted(
            str(item) for item in raw.get("mcp_servers", []) if item is not None
        )
        if isinstance(raw.get("mcp_servers", []), list)
        else [],
    }


def _nested_values(value: Any, keys: set[str]) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized in keys and item not in (None, "", [], {}):
                if isinstance(item, (str, int, float)):
                    found.append(str(item))
            found.extend(_nested_values(item, keys))
    elif isinstance(value, list):
        for item in value:
            found.extend(_nested_values(item, keys))
    return found


def _windows_date_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    match = _WINDOWS_DATE_MILLISECONDS.fullmatch(value)
    if match is None:
        return None
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            milliseconds=int(match.group(1))
        )
    except OverflowError:
        return None


def _local_creation_utc(value: dict[str, Any]) -> datetime | None:
    for key in ("creation_time_utc", "creation_utc", "started_utc", "created_utc"):
        parsed = parse_utc(value.get(key))
        if parsed is not None:
            return parsed
    return _windows_date_utc(value.get("creation_time_raw"))


def _local_pid_identities(value: Any, prefix: str = "") -> list[dict[str, Any]]:
    """Read only declared process shapes, with schema-less legacy compatibility."""

    found: list[dict[str, Any]] = []
    seen: set[tuple[int, str, str | None]] = set()

    def add_process(item: object, role: str, *, field: str = "pid") -> None:
        if not isinstance(item, dict):
            return
        pid = item.get(field)
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return
        expected = iso_utc(_local_creation_utc(item))
        key = (pid, role, expected)
        if key in seen:
            return
        seen.add(key)
        found.append(
            {
                "pid": pid,
                "role": role,
                "expected_started_utc": expected,
            }
        )

    def add_direct_fields(item: object, role: str) -> None:
        if not isinstance(item, dict):
            return
        add_process(item, role)
        for field in ("launcher_pid", "server_pid", "provider_pid", "helper_pid"):
            if field in item:
                add_process(item, f"{role}.{field}", field=field)

    def add_container(item: object, role: str) -> None:
        if not isinstance(item, dict):
            return
        add_direct_fields(item, role)
        for child_name in (
            "process",
            "launcher",
            "server",
            "provider",
            "required_helper",
            "helper_process",
            "mcp_process",
        ):
            child = item.get(child_name)
            if isinstance(child, dict):
                add_direct_fields(child, f"{role}.{child_name}")
        processes = item.get("processes")
        if isinstance(processes, list):
            for index, child in enumerate(processes):
                if isinstance(child, dict):
                    declared_role = child.get("role")
                    suffix = (
                        str(declared_role).strip()
                        if isinstance(declared_role, str) and declared_role.strip()
                        else str(index)
                    )
                    add_direct_fields(child, f"{role}.processes[{suffix}]")

    if not isinstance(value, dict):
        return found

    # Schema-less helper and MCP records historically use a direct ``pid``;
    # the field is accepted only at this record boundary.
    add_direct_fields(value, prefix or "record")
    for field in ("process", "mcp_process", "helper_process"):
        child = value.get(field)
        if isinstance(child, dict):
            add_direct_fields(child, f"{prefix + '.' if prefix else ''}{field}")
    processes = value.get("processes")
    if isinstance(processes, list):
        add_container(
            {"processes": processes}, f"{prefix + '.' if prefix else ''}processes"
        )
    for container_name in ("live_lifetime", "lifetime_binding"):
        container = value.get(container_name)
        if isinstance(container, dict):
            add_container(container, f"{prefix + '.' if prefix else ''}{container_name}")
    return found


def _helper_identity_map(run: RunRecords) -> dict[int, datetime]:
    result: dict[int, datetime] = {}
    for record in run.helper_records:
        for item in _local_pid_identities(record.value):
            started = parse_utc(item.get("expected_started_utc"))
            if started is not None:
                result[int(item["pid"])] = started
    return result


def _record_lifetime_observations(
    records: Iterable[JsonRecord],
    *,
    run_root: Path,
    snapshot: ProcessSnapshot,
    config: HarnessConfig,
    running_state: str,
    exited_state: str,
    unknown_state: str,
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for record in records:
        processes: list[dict[str, Any]] = []
        states: list[str] = []
        for item in _local_pid_identities(record.value):
            expected = parse_utc(item.get("expected_started_utc"))
            state, process, reason = _process_identity(
                snapshot,
                pid=int(item["pid"]),
                expected_started=expected,
                tolerance_seconds=config.process_start_tolerance_seconds,
            )
            states.append(state)
            processes.append(
                {
                    **item,
                    "state": state,
                    "reason": reason,
                    "actual_created_utc": iso_utc(
                        process.created_utc if process else None
                    ),
                }
            )
        if not states:
            operational = unknown_state
        elif all(state == "live" for state in states) and (
            snapshot.complete or snapshot.provider == "linux-proc"
        ):
            operational = running_state
        elif snapshot.complete and all(state in {"absent", "mismatch"} for state in states):
            operational = exited_state
        else:
            operational = unknown_state
        observations.append(
            {
                "path": str(record.path),
                "sha256": record.stable.sha256,
                "run_root": str(run_root),
                "operational_state": operational,
                "processes": processes,
                "server_name": next(
                    iter(
                        _nested_values(
                            record.value,
                            {"mcp_server", "mcp_name", "server_name"},
                        )
                    ),
                    None,
                ),
                "session_id": _request_identity_fields(record.value).get("session_id"),
                "declared_lane_id": next(
                    iter(_nested_values(record.value, {"lane_id", "declared_lane_id"})), None
                ),
            }
        )
    return observations


def _helper_observations(
    run: RunRecords,
    snapshot: ProcessSnapshot,
    config: HarnessConfig,
) -> list[dict[str, Any]]:
    return _record_lifetime_observations(
        run.helper_records,
        run_root=run.run_root,
        snapshot=snapshot,
        config=config,
        running_state="HELPER_RUNNING",
        exited_state="HELPER_EXITED",
        unknown_state="HELPER_STATE_UNKNOWN",
    )


def _mcp_record_observations(
    run: RunRecords,
    snapshot: ProcessSnapshot,
    config: HarnessConfig,
) -> list[dict[str, Any]]:
    observations = _record_lifetime_observations(
        run.mcp_records,
        run_root=run.run_root,
        snapshot=snapshot,
        config=config,
        running_state="MCP_RUNNING",
        exited_state="MCP_EXITED",
        unknown_state="MCP_STATE_UNKNOWN",
    )
    for record, observation in zip(run.mcp_records, observations):
        if _explicit_terminal_mcp_lifetime(record.value):
            observation["operational_state"] = "MCP_EXITED"
    return observations


def _explicit_terminal_mcp_lifetime(value: dict[str, Any]) -> bool:
    return (
        value.get("lifetime_status") == "closed_before_board_action"
        or value.get("terminal_state") == "exact_pid_absent_after_cleanup"
    )


def _request_identity_fields(value: dict[str, Any]) -> dict[str, str]:
    aliases = {
        "run_id": {"run_id"},
        "session_id": {"session_id", "controller_session", "thread_id"},
        "request_id": {"request_id"},
    }
    result: dict[str, str] = {}
    for canonical, keys in aliases.items():
        values = _nested_values(value, keys)
        if values:
            result[canonical] = values[0]
    return result


def _mcp_server_names(value: Any) -> list[str]:
    """Extract declared MCP server names, including object-shaped declarations."""
    names: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized in {"mcp_server", "mcp_name", "server_name"}:
                if isinstance(item, str) and item.strip():
                    names.append(item.strip())
                elif isinstance(item, dict):
                    for name_key in ("name", "mcp_server", "mcp_name", "server_name"):
                        candidate = item.get(name_key)
                        if isinstance(candidate, str) and candidate.strip():
                            names.append(candidate.strip())
            names.extend(_mcp_server_names(item))
    elif isinstance(value, list):
        for item in value:
            names.extend(_mcp_server_names(item))
    return sorted(set(names))


def _mcp_lifetime_proven(
    value: dict[str, Any],
    identities: dict[str, str],
    reconciled: list[dict[str, Any]],
    server_names: list[str],
) -> bool:
    """Require a live, identity-correlated MCP lifetime for MCP-bearing relays."""
    binding = value.get("lifetime_binding")
    if isinstance(binding, dict):
        server = binding.get("server_name")
        try:
            pid = int(binding.get("pid"))
        except (TypeError, ValueError):
            pid = None
        expected = parse_utc(binding.get("creation_utc"))
        declared_value = value.get("mcp_server")
        if isinstance(declared_value, str):
            declared = {declared_value.strip().lower()} if declared_value.strip() else set()
        elif isinstance(declared_value, dict):
            name = declared_value.get("name") or declared_value.get("server_name")
            declared = {name.strip().lower()} if isinstance(name, str) and name.strip() else set()
        else:
            declared = set()
        normalized_server = server.strip().lower() if isinstance(server, str) else ""
        return bool(
            normalized_server
            and (not declared or normalized_server in declared)
            and pid
            and expected
            and any(
                item.get("state") == "live"
                and item.get("pid") == pid
                and parse_utc(item.get("expected_started_utc")) == expected
                and str(item.get("role") or "").startswith("lifetime_binding")
                for item in reconciled
            )
        )
    lifetime = value.get("live_lifetime")
    if not isinstance(lifetime, dict) or not server_names:
        return False
    roles = {
        role.strip().lower().replace("_", "-")
        for role in _nested_values(
            lifetime, {"role", "process_role", "producer_role", "lifetime_role"}
        )
    }
    is_mcp_role = any(
        role in {"mcp", "mcp-server", "provider", "mcp-provider"}
        or role.startswith("mcp-")
        for role in roles
    )
    lifetime_identities = _request_identity_fields(lifetime)
    same_identity = all(
        identities.get(key)
        and lifetime_identities.get(key) == identities.get(key)
        for key in ("run_id", "session_id")
    )
    lifetime_servers = {
        name.strip().lower() for name in _mcp_server_names(lifetime) if name.strip()
    }
    same_server = bool(lifetime_servers.intersection(
        {name.strip().lower() for name in server_names}
    ))
    live_process = any(
        item.get("state") == "live"
        and str(item.get("role") or "").startswith("live_lifetime")
        for item in reconciled
    )
    return is_mcp_role and same_identity and same_server and live_process


def _resolve_explicit_relay(request: JsonRecord, run: RunRecords) -> Path | None:
    value = request.value.get("relay_path")
    if not isinstance(value, str) or not value:
        relay = request.value.get("relay")
        if isinstance(relay, dict):
            value = relay.get("path")
    if not isinstance(value, str) or not value:
        return None
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = run.run_root / candidate
    return candidate.resolve(strict=False)


def _relay_state(
    request: JsonRecord,
    run: RunRecords,
    request_identities: dict[str, str],
    now: datetime,
) -> tuple[str, str | None, str, str | None, str | None]:
    explicit = _resolve_explicit_relay(request, run)
    candidates = list(run.relays)
    if explicit is not None:
        candidates.sort(
            key=lambda item: 0 if same_path(item.path, explicit) else 1
        )
    saw_candidate = False
    candidate_observed_utc: str | None = None
    candidate_sha256: str | None = None
    for relay in candidates:
        if explicit is not None and same_path(relay.path, explicit):
            saw_candidate = True
            candidate_observed_utc = iso_utc(
                datetime.fromtimestamp(relay.stable.mtime_ns / 1_000_000_000, tz=timezone.utc)
            )
            candidate_sha256 = relay.stable.sha256
        hashes = {
            str(relay.value.get("request_sha256") or "").lower(),
            str(relay.value.get("request_hash") or "").lower(),
        }
        hashes.discard("")
        if request.stable.sha256.lower() not in hashes:
            continue
        saw_candidate = True
        candidate_observed_utc = iso_utc(
            datetime.fromtimestamp(relay.stable.mtime_ns / 1_000_000_000, tz=timezone.utc)
        )
        candidate_sha256 = relay.stable.sha256
        manager_binding = _manager_relay_binding_state(request, relay, now)
        if manager_binding is not None:
            return (
                manager_binding,
                str(relay.path),
                "manager request and exact approved relay binding match"
                if manager_binding == "BOUND"
                else "manager request and exact approved relay binding is expired",
                candidate_observed_utc,
                candidate_sha256,
            )
        relay_identities = _request_identity_fields(relay.value)
        comparable = 0
        mismatch = False
        for key in ("run_id", "session_id"):
            expected = request_identities.get(key)
            if expected is None:
                continue
            actual = relay_identities.get(key)
            if actual is None:
                mismatch = True
                break
            comparable += 1
            if actual != expected:
                mismatch = True
                break
        if mismatch or comparable == 0:
            continue
        return (
            "BOUND",
            str(relay.path),
            "hash and live producer identity match",
            candidate_observed_utc,
            candidate_sha256,
        )
    if saw_candidate:
        return (
            "UNBOUND",
            str(explicit) if explicit else None,
            "relay candidate lacks exact binding",
            candidate_observed_utc,
            candidate_sha256,
        )
    return "ABSENT", str(explicit) if explicit else None, "no relay candidate", None, None


def _manager_relay_binding_state(
    request: JsonRecord, relay: JsonRecord, now: datetime
) -> str | None:
    """Return exact manager binding separately from the relay freshness lease."""
    value = request.value
    relay_value = relay.value
    if value.get("schema") != "suite-manager-request/v1":
        return None
    lane_id = value.get("declared_lane_id") or value.get("lane_id")
    exact_call = relay_value.get("exact_approved_call")
    expires = parse_utc(relay_value.get("expires_utc"))
    exact = (
        relay_value.get("decision") == "approved"
        and relay_value.get("request_id") == value.get("request_id")
        and relay_value.get("request_sha256") == request.stable.sha256
        and relay_value.get("declared_lane_id", relay_value.get("lane_id")) == lane_id
        and (relay_value.get("tool_argument_sha256") or relay_value.get("tool_arguments_sha256"))
        == (value.get("tool_argument_sha256") or value.get("tool_arguments_sha256"))
        and relay_value.get("server_snapshot") == value.get("server_snapshot")
        and isinstance(exact_call, dict)
        and exact_call.get("tool") == value.get("tool")
        and exact_call.get("arguments") == value.get("arguments")
    )
    if not exact or expires is None:
        return None
    return "BOUND" if expires > now else "BOUND_EXPIRED"


def _deadline(
    value: dict[str, Any], request_created: datetime | None
) -> datetime | None:
    for key in ("expires_utc", "deadline_utc", "expiry_utc"):
        values = _nested_values(value, {key})
        for item in values:
            parsed = parse_utc(item)
            if parsed:
                return parsed
    seconds_values = _nested_values(
        value, {"expires_in_seconds", "wait_seconds", "timeout_seconds"}
    )
    if request_created and seconds_values:
        try:
            seconds = float(seconds_values[0])
        except ValueError:
            return None
        from datetime import timedelta

        return request_created + timedelta(seconds=seconds)
    return None


def _token(value: str) -> str:
    return "-".join(value.strip().lower().replace("_", "-").split())


def _normalized_resource_values(
    value: dict[str, Any], *, run_root: Path
) -> tuple[list[str], list[str]]:
    resources: set[str] = set()
    ambiguity: list[str] = []
    categories = {
        "board": {"board_id", "friendly_name", "board_token"},
        "probe": {"probe_uid", "probe", "stable_uid", "connection_id"},
        "serial": {"vcom", "port", "serial_id", "serial_port"},
        "radio": {"radio_peer", "peer", "channel", "frequency"},
    }
    for category, keys in categories.items():
        for item in _nested_values(value, keys):
            normalized = _token(item)
            if normalized:
                resources.add(f"{category}:{normalized}")
    roots = value.get("roots")
    if isinstance(roots, dict):
        for key, item in roots.items():
            if isinstance(item, str) and item.strip():
                root = Path(item)
                if not root.is_absolute():
                    root = run_root / root
                normalized = path_identity(root)
                resources.add(f"root:{key.lower()}:{normalized}")
    for run_id in _nested_values(value, {"run_id"}):
        resources.add(f"producer-lifetime:{run_id}")
    if any(item.startswith("board:") for item in resources):
        for category in ("probe:", "root:", "producer-lifetime:"):
            if not any(item.startswith(category) for item in resources):
                ambiguity.append(f"request lacks canonical {category[:-1]} ownership")
    return sorted(resources), sorted(ambiguity)


def _request_observation(
    request: JsonRecord,
    run: RunRecords,
    snapshot: ProcessSnapshot,
    config: HarnessConfig,
    now: datetime,
) -> dict[str, Any]:
    helper_starts = _helper_identity_map(run)
    pid_items = _local_pid_identities(request.value)
    reconciled = []
    states = []
    for item in pid_items:
        pid = int(item["pid"])
        expected = parse_utc(item.get("expected_started_utc")) or helper_starts.get(pid)
        state, process, reason = _process_identity(
            snapshot,
            pid=pid,
            expected_started=expected,
            tolerance_seconds=config.process_start_tolerance_seconds,
        )
        states.append(state)
        reconciled.append(
            {
                **item,
                "expected_started_utc": iso_utc(expected),
                "state": state,
                "reason": reason,
                "actual_created_utc": iso_utc(process.created_utc if process else None),
            }
        )
    if states and all(state == "live" for state in states) and (
        snapshot.complete or snapshot.provider == "linux-proc"
    ):
        lifetime = "LIVE"
    elif snapshot.complete and states and all(
        state in {"absent", "mismatch"} for state in states
    ):
        lifetime = "ABSENT"
    else:
        lifetime = "UNKNOWN"

    identities = _request_identity_fields(request.value)
    mcp_servers = _mcp_server_names(request.value)
    mcp_declared = bool(mcp_servers)
    explicit_mcp = _mcp_lifetime_proven(
        request.value, identities, reconciled, mcp_servers
    )
    resources, resource_ambiguity = _normalized_resource_values(
        request.value, run_root=run.run_root
    )
    if explicit_mcp and identities.get("run_id"):
        resources.append(f"mcp-lifetime:{identities['run_id']}")
        resources = sorted(set(resources))
    (
        relay_state,
        relay_path,
        relay_reason,
        relay_observed_utc,
        relay_sha256,
    ) = _relay_state(request, run, identities, now)
    created = parse_utc(request.value.get("created_utc"))
    deadline = _deadline(request.value, created)
    remaining = (deadline - now).total_seconds() if deadline else None
    if remaining is None:
        expiry_bucket = "UNKNOWN"
    elif remaining <= 0:
        expiry_bucket = "EXPIRED"
    elif remaining <= config.request_critical_seconds:
        expiry_bucket = "CRITICAL"
    elif remaining <= config.request_warning_seconds:
        expiry_bucket = "WARNING"
    else:
        expiry_bucket = "OK"

    facts = RequestFacts(
        lifetime_state=lifetime,
        relay_state=relay_state,
        mcp_lifetime_state=(
            "PROVEN" if explicit_mcp else "UNPROVEN" if mcp_declared else "NOT_DECLARED"
        ),
        expiry_bucket=expiry_bucket,
        mcp_declared=mcp_declared,
        explicit_mcp_lifetime=explicit_mcp,
        sidecar_matches=request.sidecar_matches,
        process_states=tuple(states),
        resource_ambiguity=tuple(resource_ambiguity),
    )
    # Keep this as a derived operator summary.  The individual facts remain
    # visible and are not collapsed into an actionability decision.
    operational = facts.operator_summary
    manager_actionable = operational in {
        "RELAY_READY",
        "RELAY_UNBOUND",
        "REQUEST_AMBIGUOUS",
    } and (
        lifetime == "LIVE" or (lifetime == "UNKNOWN" and expiry_bucket != "EXPIRED")
    )

    return {
        "path": str(request.path),
        "sha256": request.stable.sha256,
        "schema": request.value.get("schema"),
        "request_kind": request.value.get("request_kind"),
        "request_id": identities.get("request_id") or request.value.get("boundary"),
        "created_utc": iso_utc(created),
        "deadline_utc": iso_utc(deadline),
        "expiry_bucket": expiry_bucket,
        "remaining_seconds": round(remaining, 3) if remaining is not None else None,
        "producer_identities": identities,
        "declared_lane_id": request.value.get("declared_lane_id") or next(
            iter(_nested_values(request.value, {"lane_id"})), None
        ),
        "explicit_mcp_lifetime": {
            "is_explicit": explicit_mcp,
            "server_name": mcp_servers[0] if mcp_declared else None,
            "session_id": identities.get("session_id") if explicit_mcp else None,
        },
        "mcp_lifetime_state": (
            "PROVEN" if explicit_mcp else "UNPROVEN" if mcp_declared else "NOT_DECLARED"
        ),
        "request_facts": jsonable(facts),
        "operator_summary": operational,
        "lifetime_state": lifetime,
        "processes": reconciled,
        "relay_state": relay_state,
        "relay_path": relay_path,
        "relay_reason": relay_reason,
        "relay_observed_utc": relay_observed_utc,
        "relay_sha256": relay_sha256,
        "operational_state": operational,
        "manager_actionable": manager_actionable,
        "resources": resources,
        "resource_ambiguity": resource_ambiguity,
        "sidecar_sha256": request.sidecar_sha256,
        "sidecar_matches": request.sidecar_matches,
    }


def _signal_request_match(raw: dict[str, Any], request: dict[str, Any], run: RunRecords) -> bool:
    """Correlate only durable manager-request IDs or exact request file paths."""
    if raw.get("request_id") == request.get("request_id") and request.get("request_id"):
        return True
    request_path = Path(str(request["path"])).resolve(strict=False)
    candidates = [raw.get("request_path"), *raw.get("evidence_paths", [])]
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate:
            continue
        path = Path(candidate)
        if not path.is_absolute():
            path = run.run_root / path
        if same_path(path, request_path):
            return True
    return False


def _latest_by_lane(
    run: RunRecords,
    observations: list[tuple[ControllerRecord, dict[str, Any]]],
) -> dict[str, tuple[ControllerRecord, dict[str, Any], list[dict[str, Any]]]]:
    grouped: dict[str, list[tuple[ControllerRecord, dict[str, Any]]]] = defaultdict(
        list
    )
    for controller, observation in observations:
        grouped[_attempt_group_id(run, controller)].append((controller, observation))
    selected: list[tuple[ControllerRecord, dict[str, Any], list[dict[str, Any]]]] = []
    for items in grouped.values():
        items.sort(
            key=lambda pair: (
                parse_utc(pair[1].get("started_utc"))
                or datetime.min.replace(tzinfo=timezone.utc),
                pair[0].status.stable.mtime_ns,
            ),
            reverse=True,
        )
        live_attempts = [
            item for _, item in items if item["operational_state"] == "RUNNING_CODEX"
        ]
        selected.append((items[0][0], items[0][1], live_attempts))

    public_ids: dict[str, int] = defaultdict(int)
    for controller, _, _ in selected:
        public_ids[_lane_id(run, controller)] += 1
    result = {}
    for controller, latest, live_attempts in selected:
        lane_id = _lane_id(run, controller)
        if public_ids[lane_id] > 1:
            thread_id = str(latest.get("thread_id") or "").strip()
            if thread_id:
                lane_id = f"{lane_id}:thread:{thread_id}"
        result[lane_id] = (controller, latest, live_attempts)
    return result


def _resource_set(
    lane: dict[str, Any], requests: Iterable[dict[str, Any]]
) -> tuple[list[str], list[str]]:
    resources: set[str] = set()
    ambiguity: list[str] = []
    active_lane = _lane_is_active_or_unknown(lane)
    if active_lane and lane.get("invocation_schema") == "orchestrator-coding-invocation/v1":
        resources.update(
            item for item in lane.get("declared_resources", [])
            if isinstance(item, str) and item
        )
    if active_lane:
        for board in lane.get("board_tokens", []):
            resources.add(f"board:{_token(str(board))}")
    for server in lane.get("mcp_servers", []):
        resources.add(f"mcp-name:{str(server).strip().lower()}")

    related = [
        request
        for request in requests
        if request.get("operational_state") in {"RELAY_READY", "RELAY_UNBOUND"}
    ]
    for request in related:
        resources.update(request.get("resources", []))
        ambiguity.extend(request.get("resource_ambiguity", []))
        for process in request.get("processes", []):
            if process.get("state") == "live":
                resources.add(
                    f"process:{process['pid']}@{process.get('actual_created_utc')}"
                )
    if active_lane and lane.get("board_tokens") and not related:
        ambiguity.append(
            "active hardware lane has no current request identity for probe/serial/root audit"
        )
    return sorted(resources), sorted(set(ambiguity))


def _lane_is_active_or_unknown(lane: dict[str, Any]) -> bool:
    return lane.get("operational_state", lane.get("process_state")) in {
        "RUNNING_CODEX",
        "WAITING_RESOURCE",
        "WAITING_RELAY",
        "HELPER_RUNNING",
        "PROCESS_STATE_UNKNOWN",
        "UNKNOWN",
    }


def _record_belongs_to_lane(record: dict[str, Any], lane: dict[str, Any]) -> bool:
    """Require an explicit lane identity when present, otherwise an exact session.

    A persistent session may have been reused for an older role in the same run root.
    Therefore an explicit lane declaration is authoritative and must not be overridden
    by a matching reused session identity.
    """
    declared_lane_id = record.get("declared_lane_id")
    if isinstance(declared_lane_id, str) and declared_lane_id:
        return declared_lane_id == lane.get("lane_id")
    session_id = record.get("producer_identities", {}).get(
        "session_id"
    ) or record.get("session_id")
    thread_id = lane.get("thread_id")
    return (
        isinstance(session_id, str)
        and bool(session_id)
        and session_id == thread_id
    )


def _request_belongs_to_lane(request: dict[str, Any], lane: dict[str, Any]) -> bool:
    """Keep unowned historical requests observable but out of lane state/resources."""
    return _record_belongs_to_lane(request, lane)


def reconcile(
    runs: tuple[RunRecords, ...],
    snapshot: ProcessSnapshot,
    config: HarnessConfig,
    *,
    now: datetime,
) -> dict[str, Any]:
    lanes: list[dict[str, Any]] = []
    requests: list[dict[str, Any]] = []
    helpers: list[dict[str, Any]] = []
    mcps: list[dict[str, Any]] = []
    manager_signals: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for run in runs:
        controller_observations = [
            (controller, _controller_observation(controller, snapshot, config))
            for controller in run.controllers
        ]
        run_requests = [
            _request_observation(request, run, snapshot, config, now)
            for request in run.requests
        ]
        requests.extend(run_requests)
        run_helpers = _helper_observations(run, snapshot, config)
        helpers.extend(run_helpers)
        run_mcps = _mcp_record_observations(run, snapshot, config)
        for request in run_requests:
            explicit = request.get("explicit_mcp_lifetime", {})
            if not explicit.get("is_explicit"):
                continue
            run_id = request.get("producer_identities", {}).get("run_id")
            lifetime = str(request.get("lifetime_state") or "")
            state = {
                "LIVE": "MCP_RUNNING",
                "ABSENT": "MCP_EXITED",
            }.get(lifetime, "MCP_STATE_UNKNOWN")
            run_mcps.append(
                {
                    "path": f"{request['path']}#mcp-lifetime:{run_id}",
                    "source_path": request["path"],
                    "sha256": request.get("sha256"),
                    "run_root": str(run.run_root),
                    "run_id": run_id,
                    "server_name": explicit.get("server_name"),
                    "session_id": explicit.get("session_id"),
                    "declared_lane_id": request.get("declared_lane_id"),
                    "operational_state": state,
                    "processes": request.get("processes", []),
                }
            )
        mcps.extend(run_mcps)
        for signal in run.manager_signals:
            raw = signal.value
            correlated = next(
                (item for item in run_requests if _signal_request_match(raw, item, run)),
                None,
            )
            manager_signals.append(
                {
                    "path": str(signal.path),
                    "sha256": signal.stable.sha256,
                    "run_root": str(run.run_root),
                    "signal_id": raw["signal_id"],
                    "kind": raw["kind"],
                    "created_utc": iso_utc(parse_utc(raw["created_utc"])),
                    "deadline_utc": iso_utc(parse_utc(raw.get("deadline_utc"))),
                    **({"delivery_deadline_utc": iso_utc(parse_utc(raw["delivery_deadline_utc"]))} if "delivery_deadline_utc" in raw else {}),
                    **({"agent_blocked": raw["agent_blocked"]} if "agent_blocked" in raw else {}),
                    **({"attention_epoch_id": raw["attention_epoch_id"]} if "attention_epoch_id" in raw else {}),
                    "lane_id": raw["lane_id"],
                    "task": raw.get("task"),
                    "phase": raw.get("phase"),
                    "summary": raw["summary"],
                    "evidence_paths": list(raw.get("evidence_paths", [])),
                    "correlated_request_id": correlated.get("request_id") if correlated else None,
                    "correlated_request_path": correlated.get("path") if correlated else None,
                    "correlated_request_answered": bool(
                        correlated
                        and correlated.get("relay_state") in {"BOUND", "BOUND_EXPIRED"}
                    ),
                }
            )
        for lane_id, (_, latest, live_attempts) in _latest_by_lane(
            run, controller_observations
        ).items():
            lane = {
                **latest,
                "process_state": latest["operational_state"],
                "process_reason": latest["reason"],
                "lane_id": lane_id,
                "run_root": str(run.run_root),
                "workspace": str(run.workspace),
                "duplicate_live_attempts": len(live_attempts),
                "live_attempt_labels": [item["label"] for item in live_attempts],
            }
            phase_text = str(lane.get("phase") or "").lower()
            lane["provider_wait"] = any(
                marker in phase_text
                for marker in (
                    "provider wait",
                    "waiting_for_provider",
                    "provider unavailable",
                    "backend unavailable",
                )
            )
            owns_invalid_result = (
                run.invalid_result is not None
                and run.invalid_result_status_path is not None
                and lane["status_path"] == str(run.invalid_result_status_path)
            )
            owns_result = (
                run.result is not None
                and run.result_status_path is not None
                and lane["status_path"] == str(run.result_status_path)
            )
            if owns_invalid_result:
                assert run.invalid_result is not None
                lane["invalid_result"] = dict(run.invalid_result)
            if owns_result:
                assert run.result is not None
                lane["operational_state"] = "TERMINAL_RESULT"
                lane["result_path"] = str(run.result.path)
                lane["result_sha256"] = run.result.stable.sha256
                lane["result_observed_utc"] = iso_utc(
                    datetime.fromtimestamp(
                        run.result.stable.mtime_ns / 1_000_000_000, tz=timezone.utc
                    )
                )
            elif run.checkpoint is not None:
                started = parse_utc(lane.get("started_utc"))
                checkpoint_time = datetime.fromtimestamp(
                    run.checkpoint.mtime_ns / 1_000_000_000, tz=timezone.utc
                )
                if started is None or checkpoint_time >= started:
                    if lane["operational_state"] in {"EXITED", "UNKNOWN"}:
                        lane["operational_state"] = "CHECKPOINTED"
                    lane["checkpoint_path"] = str(run.checkpoint.path)
                    lane["checkpoint_sha256"] = run.checkpoint.sha256
                    lane["checkpoint_observed_utc"] = iso_utc(checkpoint_time)
                    checkpoint_preamble = (
                        run.checkpoint.data.decode("utf-8", errors="replace")
                        .lower()
                        .split("\n## ", 1)[0]
                    )
                    if (
                        "waiting_for_provider" in checkpoint_preamble
                        or "provider wait" in checkpoint_preamble
                    ):
                        lane["provider_wait"] = True
            thread = lane.get("thread_id")
            matching_requests = [
                item
                for item in run_requests
                if _request_belongs_to_lane(item, lane)
            ]
            if any(
                item["operational_state"] == "RELAY_READY" for item in matching_requests
            ):
                lane["operational_state"] = "WAITING_RELAY"
            resources, ambiguity = _resource_set(lane, matching_requests)
            declared_mcp_values = lane.get("mcp_servers")
            if not isinstance(declared_mcp_values, (list, tuple, set, frozenset)):
                declared_mcp_values = []
            declared_mcp_names = {
                str(name).strip().lower() for name in declared_mcp_values
            }
            correlated_mcps = [
                item
                for item in run_mcps
                if _record_belongs_to_lane(item, lane)
            ]
            matching_mcps = [
                item
                for item in correlated_mcps
                if str(item.get("server_name") or "").strip().lower()
                in declared_mcp_names
            ]
            evidenced_names = {
                str(item.get("server_name") or "").strip().lower()
                for item in matching_mcps
            }
            missing_mcp_names = declared_mcp_names - evidenced_names
            if _lane_is_active_or_unknown(lane) and missing_mcp_names:
                ambiguity.append(
                    "declared MCP server has no lane/session-correlated PID lifetime evidence"
                )
                for normalized_name in sorted(missing_mcp_names):
                    server_name = next(
                        name
                        for name in declared_mcp_values
                        if str(name).strip().lower() == normalized_name
                    )
                    unknown_mcp = {
                        "path": (
                            f"{lane['status_path']}#mcp-name:"
                            f"{normalized_name}:session:{thread or 'unknown'}"
                        ),
                        "source_path": lane["status_path"],
                        "sha256": lane.get("status_sha256"),
                        "run_root": str(run.run_root),
                        "server_name": server_name,
                        "session_id": thread,
                        "declared_lane_id": lane_id,
                        "operational_state": "MCP_STATE_UNKNOWN",
                        "processes": [],
                    }
                    run_mcps.append(unknown_mcp)
                    mcps.append(unknown_mcp)
                    correlated_mcps.append(unknown_mcp)
                    matching_mcps.append(unknown_mcp)
            lane["resources"] = resources
            lane["resource_ambiguity"] = sorted(set(ambiguity))
            related_helpers = [
                item
                for item in run_helpers
                if _record_belongs_to_lane(item, lane)
                or not item.get("declared_lane_id")
                and not item.get("session_id")
            ]
            request_lifetimes = {
                item.get("lifetime_state") for item in matching_requests
            }
            helper_states = {item.get("operational_state") for item in related_helpers}
            mcp_states = {item.get("operational_state") for item in correlated_mcps}
            lane["resource_release_possible"] = (
                lane["process_state"] == "EXITED"
                and not lane["resource_ambiguity"]
                and not request_lifetimes.intersection({"LIVE", "UNKNOWN"})
                and not helper_states.intersection(
                    {"HELPER_RUNNING", "HELPER_STATE_UNKNOWN"}
                )
                and not mcp_states.intersection({"MCP_RUNNING", "MCP_STATE_UNKNOWN"})
            )
            lanes.append(lane)
        errors.extend(
            {"path": item.path, "code": item.code, "detail": item.detail}
            for item in run.errors
        )

    owners: dict[str, list[str]] = defaultdict(list)
    for lane in lanes:
        if not _lane_is_active_or_unknown(lane):
            continue
        for resource in lane.get("resources", []):
            owners[resource].append(lane["lane_id"])
    conflicts = [
        {"resource": resource, "owners": sorted(set(lane_ids))}
        for resource, lane_ids in owners.items()
        if len(set(lane_ids)) > 1
    ]

    coding_conflicts: list[dict[str, Any]] = []
    active_coding = [
        lane for lane in lanes
        if _lane_is_active_or_unknown(lane)
        and lane.get("invocation_schema") == "orchestrator-coding-invocation/v1"
        and isinstance(lane.get("repository"), dict)
    ]
    for field, kind in (("worktree_root", "DUPLICATE_CODING_WORKTREE"), ("branch", "DUPLICATE_CODING_BRANCH")):
        groups: dict[str, list[str]] = defaultdict(list)
        for lane in active_coding:
            repository = lane["repository"]
            value = repository.get(field)
            if field == "branch":
                common = repository.get("common_dir")
                key = f"{common}::{value}" if isinstance(common, str) and isinstance(value, str) else ""
            else:
                key = path_identity(value) if isinstance(value, str) and value else ""
            if key:
                groups[key].append(lane["lane_id"])
        for key, lane_ids in groups.items():
            if len(set(lane_ids)) > 1:
                coding_conflicts.append({
                    "type": kind,
                    "identity": key,
                    "lanes": sorted(set(lane_ids)),
                })

    return {
        "schema": "orchestrator-watcher-snapshot/v1",
        "observed_utc": iso_utc(now),
        "process_provider": snapshot.provider,
        "process_snapshot_complete": snapshot.complete,
        "process_errors": list(snapshot.errors),
        "runs_discovered": len(runs),
        "lanes": sorted(lanes, key=lambda item: item["lane_id"]),
        "requests": sorted(requests, key=lambda item: item["path"]),
        "helpers": sorted(helpers, key=lambda item: item["path"]),
        "mcps": sorted(mcps, key=lambda item: item["path"]),
        "manager_signals": sorted(manager_signals, key=lambda item: (item["signal_id"], item["path"])),
        "resource_conflicts": sorted(conflicts, key=lambda item: item["resource"]),
        "coding_conflicts": sorted(
            coding_conflicts, key=lambda item: (item["type"], item["identity"])
        ),
        "observation_errors": sorted(
            errors, key=lambda item: (item["path"], item["code"])
        ),
    }
