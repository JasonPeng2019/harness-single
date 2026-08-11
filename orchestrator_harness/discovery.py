from __future__ import annotations

import json
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .config import HarnessConfig, path_identity
from .git_safety import (
    GitSafetyError,
    declaration_from_status,
    invalid_result_evidence,
    _git_inspection_env,
    validate_coding_result,
    validate_task_result_repository,
)
from .models import ObservationError, StableBytes
from .models import parse_utc
from .stable_io import read_stable, read_tail_stable
from .task import TaskValidationError, read_task_advancement, task_card_from_identity, validate_task_result
from .provider import ProviderAdapterError, provider_adapter


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
    result_status_path: Path | None
    invalid_result: dict[str, Any] | None
    invalid_result_status_path: Path | None
    manager_signals: tuple[ManagerSignalRecord, ...]
    errors: tuple[ObservationError, ...]
    result_acceptance_state: str | None = None


_HELPER_RECORD_NAMES = {"helper_process.json", "live-context.json"}
_MCP_RECORD_NAMES = {"mcp_process.json", "mcp_processes.json", "mcp-lifetime.json"}
_RECORD_MANIFEST_NAMES = (
    "record-manifest.json",
    "record_manifest.json",
    "observation-manifest.json",
)
_RECORD_CACHE_MAX = 4096
_JSON_RECORD_CACHE: dict[tuple[Any, ...], JsonRecord] = {}
_TERMINAL_EVENT_CACHE: dict[tuple[Any, ...], tuple[str | None, int | None]] = {}
_RESULT_VALIDATION_CACHE: dict[tuple[Any, ...], tuple[bool, Any]] = {}


def _clear_observation_caches() -> None:
    """Clear process-local observation caches; intended for bounded tests."""

    _JSON_RECORD_CACHE.clear()
    _TERMINAL_EVENT_CACHE.clear()
    _RESULT_VALIDATION_CACHE.clear()


def _file_signature(path: Path) -> tuple[tuple[int, int], int, int] | None:
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    return (int(info.st_dev), int(info.st_ino)), int(info.st_size), int(info.st_mtime_ns)


def _cache_put(cache: dict[tuple[Any, ...], Any], key: tuple[Any, ...], value: Any) -> None:
    if len(cache) >= _RECORD_CACHE_MAX:
        cache.pop(next(iter(cache)))
    cache[key] = value


def _json_record(
    path: Path, config: HarnessConfig, *, check_sidecar: bool = False
) -> JsonRecord:
    sidecar = Path(str(path) + ".sha256")
    file_signature = _file_signature(path)
    if file_signature is None:
        raise FileNotFoundError(path)
    sidecar_signature = _file_signature(sidecar) if check_sidecar else None
    cache_key = (
        path_identity(path),
        file_signature,
        check_sidecar,
        sidecar_signature,
        config.max_json_bytes,
    )
    cached = _JSON_RECORD_CACHE.get(cache_key)
    if cached is not None:
        return cached
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
    record = JsonRecord(path, stable, value, sidecar_sha, sidecar_matches)
    _cache_put(_JSON_RECORD_CACHE, cache_key, record)
    return record


def _terminal_event(
    path: Path,
    config: HarnessConfig,
    *,
    provider_id: str = "codex",
) -> tuple[str | None, int | None]:
    signature = _file_signature(path)
    if signature is None:
        raise FileNotFoundError(path)
    cache_key = (path_identity(path), signature, config.max_jsonl_tail_bytes, provider_id)
    cached = _TERMINAL_EVENT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    stable = read_tail_stable(
        path,
        max_bytes=config.max_jsonl_tail_bytes,
        retries=config.stable_read_retries,
        delay_seconds=config.stable_read_delay_seconds,
    )
    try:
        adapter = provider_adapter(provider_id or "codex")
    except ProviderAdapterError as exc:
        raise ValueError(f"unsupported provider in transcript status: {provider_id}") from exc
    terminal = None
    for raw_line in stable.data.decode("utf-8", errors="replace").splitlines():
        event = adapter.parse_transcript_line((raw_line + "\n").encode("utf-8"))
        if event is not None and event.is_terminal:
            terminal = event.raw_type or event.kind
    result = (terminal, stable.mtime_ns)
    _cache_put(_TERMINAL_EVENT_CACHE, cache_key, result)
    return result


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


def _relative_to_root(path: Path, root: Path) -> tuple[str, ...]:
    try:
        return path.resolve(strict=False).relative_to(root.resolve(strict=False)).parts
    except ValueError as exc:
        raise ValueError("declared record path escapes its workspace") from exc


def _declared_record_path(value: object, workspace: Path) -> Path:
    if isinstance(value, Path):
        candidate = value
    elif isinstance(value, str) and value.strip():
        candidate = Path(value.strip())
    else:
        raise ValueError("record path must be a non-empty string")
    if not candidate.is_absolute():
        if ".." in candidate.parts:
            raise ValueError("record path may not contain '..'")
        candidate = workspace / candidate
    resolved = candidate.resolve(strict=False)
    relative = _relative_to_root(resolved, workspace)
    current = workspace.resolve(strict=False)
    for part in relative:
        current = current / part
        if current.exists() and _is_reparse_or_link(current):
            raise ValueError("record path contains a link or reparse point")
    return resolved


def _declared_values(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    return []


def _manifest_declarations(value: Mapping[str, Any]) -> list[tuple[str, str]]:
    schema = value.get("schema")
    if schema not in (None, "orchestrator-record-manifest/v1"):
        raise ValueError("unsupported record manifest schema")
    declarations: list[tuple[str, str]] = []
    for kind, keys in (
        ("helper", ("helper_paths", "helper_records", "helpers")),
        ("mcp", ("mcp_paths", "mcp_records", "mcps")),
        ("unknown", ("paths", "record_paths", "records")),
    ):
        for key in keys:
            items = value.get(key)
            if isinstance(items, Mapping) and key in {"records", "record_paths"}:
                for path, declared_kind in items.items():
                    declarations.append((str(path), str(declared_kind)))
                break
            if key == "records" and isinstance(items, list):
                break
            if items is not None and not (
                isinstance(items, str)
                or isinstance(items, list)
                and all(isinstance(item, str) for item in items)
            ):
                raise ValueError(f"manifest {key} must contain record paths")
            for path in _declared_values(items):
                declarations.append((path, kind))
            if items is not None:
                break
    records = value.get("records")
    if isinstance(records, list):
        for item in records:
            if not isinstance(item, Mapping):
                raise ValueError("manifest records must be objects")
            path = item.get("path") or item.get("record_path")
            kind = item.get("kind") or item.get("record_kind") or "unknown"
            if not isinstance(path, str) or not isinstance(kind, str):
                raise ValueError("manifest record path/kind is invalid")
            declarations.append((path, kind))
    return declarations


def _status_record_declarations(
    controllers: list[ControllerRecord],
) -> tuple[list[tuple[object, str]], list[object]]:
    records: list[tuple[object, str]] = []
    manifests: list[object] = []
    for controller in controllers:
        raw = controller.status.value
        for key in ("record_manifest", "observation_manifest"):
            if key in raw:
                manifests.extend(_declared_values(raw[key]))
        for key in ("record_manifests", "observation_manifests"):
            if key in raw:
                manifests.extend(_declared_values(raw[key]))
        for key in ("record_paths", "observation_paths"):
            value = raw.get(key)
            if isinstance(value, Mapping):
                for kind, paths in value.items():
                    for path in _declared_values(paths):
                        records.append((path, str(kind)))
            else:
                for path in _declared_values(value):
                    records.append((path, "unknown"))
    return records, manifests


def _record_kind(path: Path, value: Mapping[str, Any], declared: set[str]) -> str:
    normalized = {item.strip().lower().replace("_", "-") for item in declared}
    if "helper" in normalized and "mcp" not in normalized:
        return "helper"
    if "mcp" in normalized and "helper" not in normalized:
        return "mcp"
    if path.name.lower() in _HELPER_RECORD_NAMES:
        return "helper"
    if path.name.lower() in _MCP_RECORD_NAMES:
        return "mcp"
    kind = value.get("record_kind") or value.get("kind")
    if isinstance(kind, str) and kind.strip().lower().replace("_", "-") in {
        "helper",
        "helper-process",
    }:
        return "helper"
    if isinstance(kind, str) and "mcp" in kind.strip().lower():
        return "mcp"
    schema = str(value.get("schema") or "").lower()
    if "mcp" in schema or any(
        key in value for key in ("mcp_server", "mcp_name", "mcp_process", "mcp_processes")
    ):
        return "mcp"
    return "helper"


def _declared_lifetime_records(
    workspace: Path,
    config: HarnessConfig,
    controllers: list[ControllerRecord],
) -> tuple[dict[str, tuple[Path, set[str]]], list[ObservationError]]:
    declarations: dict[str, tuple[Path, set[str]]] = {}
    errors: list[ObservationError] = []

    def add(value: object, kind: str, *, source: object = workspace) -> None:
        try:
            path = _declared_record_path(value, workspace)
        except (OSError, ValueError) as exc:
            errors.append(ObservationError(str(source), "RECORD_PATH_ERROR", str(exc)))
            return
        key = path_identity(path)
        if key not in declarations:
            declarations[key] = (path, set())
        declarations[key][1].add(kind)

    for name in sorted(_HELPER_RECORD_NAMES):
        path = workspace / name
        if path.exists() or path.is_symlink():
            add(str(path), "helper")
    for name in sorted(_MCP_RECORD_NAMES):
        path = workspace / name
        if path.exists() or path.is_symlink():
            add(str(path), "mcp")
    for path in config.record_paths:
        add(path, "unknown", source="config.record_paths")
    status_records, status_manifests = _status_record_declarations(controllers)
    for path, kind in status_records:
        add(path, kind, source="controller record_paths")

    manifest_values: list[object] = [
        *config.record_manifests,
        *status_manifests,
        *[workspace / name for name in _RECORD_MANIFEST_NAMES],
    ]
    seen_manifests: set[str] = set()
    for value in manifest_values:
        try:
            manifest_path = _declared_record_path(value, workspace)
        except (OSError, ValueError) as exc:
            errors.append(
                ObservationError(str(value), "RECORD_MANIFEST_READ_ERROR", str(exc))
            )
            continue
        manifest_key = path_identity(manifest_path)
        if manifest_key in seen_manifests:
            continue
        seen_manifests.add(manifest_key)
        try:
            manifest = _json_record(manifest_path, config).value
            for path, kind in _manifest_declarations(manifest):
                add(path, kind, source=str(manifest_path))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            # Optional conventional manifests are silent when absent.  A
            # declared manifest remains an explicit observation error.
            if manifest_path.exists() or value not in {
                workspace / name for name in _RECORD_MANIFEST_NAMES
            }:
                errors.append(
                    ObservationError(
                        str(manifest_path), "RECORD_MANIFEST_READ_ERROR", str(exc)
                    )
                )
    return declarations, errors


def _git_state_fingerprint(worktree: Path) -> tuple[str, ...]:
    """Capture only the Git state that can affect result acceptance."""

    outputs: list[str] = []
    for args in (
        ("rev-parse", "--verify", "HEAD^{commit}"),
        ("symbolic-ref", "--quiet", "--short", "HEAD"),
        ("rev-parse", "--show-toplevel"),
        ("rev-parse", "--git-common-dir"),
        ("status", "--porcelain=v1", "-z", "--untracked-files=all", "--", "."),
    ):
        try:
            completed = subprocess.run(
                ["git", "-C", str(worktree), *args],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
                timeout=10,
                env=_git_inspection_env(),
                shell=False,
            )
            outputs.append(
                f"{completed.returncode}:"
                f"{completed.stdout if completed.returncode == 0 else completed.stderr}"
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            outputs.append(f"error:{exc}")
    return tuple(outputs)


def _validate_coding_result_cached(
    candidate: JsonRecord,
    *,
    coding_status: ControllerRecord,
    run_root: Path,
    revalidate: bool,
) -> None:
    declaration = declaration_from_status(coding_status.status.value, run_root)
    git_fingerprint = _git_state_fingerprint(declaration.worktree_root)
    key = (
        path_identity(candidate.path),
        candidate.stable.file_id,
        candidate.stable.size,
        candidate.stable.mtime_ns,
        candidate.stable.sha256,
        git_fingerprint,
        coding_status.status.value.get("declared_lane_id"),
        coding_status.status.value.get("worker_invocation_id"),
        declaration.common_dir,
        declaration.worktree_root,
        declaration.branch,
        declaration.base_commit,
    )
    if not revalidate:
        cached = _RESULT_VALIDATION_CACHE.get(key)
        if cached is not None:
            valid, value = cached
            if valid:
                return
            raise GitSafetyError(str(value))
    try:
        validate_coding_result(
            candidate.value,
            lane_id=str(coding_status.status.value["declared_lane_id"]),
            worker_invocation_id=str(coding_status.status.value["worker_invocation_id"]),
            declaration=declaration,
        )
    except (GitSafetyError, OSError, ValueError) as exc:
        _RESULT_VALIDATION_CACHE[key] = (False, str(exc))
        raise
    _RESULT_VALIDATION_CACHE[key] = (True, None)


def _validate_task_result_cached(
    candidate: JsonRecord,
    *,
    coding_status: ControllerRecord,
    run_root: Path,
    revalidate: bool,
) -> str:
    """Validate canonical result shape and return its semantic advancement state."""

    raw_card = coding_status.status.value.get("task_card")
    if not isinstance(raw_card, Mapping):
        raise TaskValidationError("canonical status is missing task_card identity")
    card = task_card_from_identity(
        card_id=raw_card.get("id"),
        lane_id=coding_status.status.value.get("declared_lane_id"),
        worker_invocation_id=coding_status.status.value.get("worker_invocation_id"),
        cohort_id=coding_status.status.value.get("cohort_id"),
        revision=raw_card.get("revision"),
        content_sha256=raw_card.get("sha256"),
        completion_review_owner=coding_status.status.value.get("completion_review_owner", "ROOT-IM"),
    )
    prompt_bundle_sha = coding_status.status.value.get("prompt_bundle_sha256")
    prompt_content_sha = coding_status.status.value.get("prompt_content_sha256")
    declaration = None
    if isinstance(coding_status.status.value.get("repository"), Mapping):
        declaration = declaration_from_status(coding_status.status.value, run_root)
    result = (
        validate_task_result_repository(
            candidate.value,
            card=card,
            declaration=declaration,
            raw_bytes=candidate.stable.data,
        )
        if declaration is not None
        else validate_task_result(candidate.value, card=card, raw_bytes=candidate.stable.data)
    )
    if candidate.value.get("prompt_bundle_sha256") != prompt_bundle_sha:
        raise TaskValidationError("canonical result prompt bundle identity does not match status")
    if candidate.value.get("prompt_content_sha256") != prompt_content_sha:
        raise TaskValidationError("canonical result prompt content identity does not match status")
    advancement = read_task_advancement(
        coding_status.workspace,
        card=card,
        result=result,
    )
    return advancement.state


def discover_run(
    run_root: Path,
    workspace: Path,
    config: HarnessConfig,
    *,
    revalidate_results: bool = False,
    revalidate: bool = False,
) -> RunRecords:
    errors: list[ObservationError] = []
    controllers: list[ControllerRecord] = []
    conventional_statuses = set(workspace.glob("*_controller.status.json"))
    coding_candidates = set(workspace.glob("*.json")) - conventional_statuses
    for path in sorted(conventional_statuses | coding_candidates):
        try:
            status = _json_record(path, config)
            conventional = path in conventional_statuses
            if not conventional and not (
                status.value.get("schema") == "orchestrator-lane-controller/v1"
                and status.value.get("invocation_schema")
                in {
                    "orchestrator-coding-invocation/v1",
                    "orchestrator-worker-invocation/v1",
                }
            ):
                continue
            label = (
                path.name[: -len("_controller.status.json")]
                if conventional
                else str(
                    status.value.get("label")
                    or status.value.get("worker_invocation_id")
                    or path.stem
                )
            )
            configured_jsonl = status.value.get("jsonl_path")
            configured_path = Path(configured_jsonl).resolve(strict=False) if isinstance(configured_jsonl, str) else None
            safe_configured = (
                configured_path
                if configured_path is not None and configured_path.parent == workspace
                else None
            )
            candidates = ([safe_configured] if safe_configured is not None else []) + [
                workspace / f"{label}_codex.jsonl",
                workspace / "test_agent_codex.jsonl",
            ]
            jsonl = next((item for item in candidates if item.exists()), None)
            terminal = None
            terminal_mtime = None
            if jsonl:
                terminal, terminal_mtime = _terminal_event(
                    jsonl,
                    config,
                    provider_id=str(status.value.get("provider_id") or "codex"),
                )
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
            if path not in conventional_statuses:
                continue
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
    mcp_records: list[JsonRecord] = []
    declared_records, record_errors = _declared_lifetime_records(
        workspace, config, controllers
    )
    errors.extend(record_errors)
    for path, declared_kinds in sorted(
        declared_records.values(), key=lambda item: path_identity(item[0])
    ):
        try:
            record = _json_record(path, config)
            kind = _record_kind(path, record.value, declared_kinds)
            if kind == "mcp":
                mcp_records.append(record)
            else:
                helper_records.append(record)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            kind = _record_kind(path, {}, declared_kinds)
            errors.append(
                ObservationError(
                    str(path),
                    "MCP_READ_ERROR" if kind == "mcp" else "HELPER_READ_ERROR",
                    str(exc),
                )
            )

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
    result_status_path = None
    invalid_result = None
    invalid_result_status_path = None
    result_acceptance_state: str | None = None
    result_path = workspace / "RESULT.json"
    coding_controllers = [
        item
        for item in controllers
        if item.status.value.get("invocation_schema")
        in {
            "orchestrator-coding-invocation/v1",
            "orchestrator-worker-invocation/v1",
        }
    ]
    firmware_controllers = [
        item
        for item in controllers
        if item.status.value.get("invocation_schema") is None
    ]
    coding_route = bool(coding_controllers) and not firmware_controllers
    coding_owner: ControllerRecord | None = None
    candidate: JsonRecord | None = None
    if result_path.exists():
        try:
            candidate = _json_record(result_path, config)
            coding_route = (
                candidate.value.get("schema") is not None
                or "worker_invocation_id" in candidate.value
            )
            if coding_route:
                lane_id = candidate.value.get("lane_id")
                worker_id = candidate.value.get("worker_invocation_id")
                lane_matches = [
                    item
                    for item in coding_controllers
                    if item.status.value.get("declared_lane_id") == lane_id
                ]
                if lane_matches:
                    coding_owner = max(
                        lane_matches,
                        key=lambda item: item.status.stable.mtime_ns,
                    )
                if (
                    coding_owner is None
                    or coding_owner.status.value.get("worker_invocation_id") != worker_id
                ):
                    raise GitSafetyError(
                        "coding result does not match a current coding lane and worker invocation"
                    )
                if (
                    candidate.value.get("schema") == "orchestrator-task-result/v1"
                    or coding_owner.status.value.get("invocation_schema")
                    == "orchestrator-worker-invocation/v1"
                ):
                    result_acceptance_state = _validate_task_result_cached(
                        candidate,
                        coding_status=coding_owner,
                        run_root=run_root,
                        revalidate=revalidate_results or revalidate,
                    )
                else:
                    _validate_coding_result_cached(
                        candidate,
                        coding_status=coding_owner,
                        run_root=run_root,
                        revalidate=revalidate_results or revalidate,
                    )
                    result_acceptance_state = "ACCEPTED" if candidate.value.get("acceptance_state") == "ACCEPTED" else None
                result = candidate
                result_status_path = coding_owner.status.path
            else:
                if len(firmware_controllers) > 1:
                    raise ValueError("legacy firmware result has ambiguous controller ownership")
                if not firmware_controllers and coding_controllers:
                    coding_route = True
                    if len(coding_controllers) == 1:
                        coding_owner = coding_controllers[0]
                    raise GitSafetyError(
                        "firmware-shaped result is not valid for a coding lane"
                    )
                result = candidate
                if firmware_controllers:
                    result_status_path = firmware_controllers[0].status.path
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            code = "CODING_RESULT_INVALID" if coding_route else "RESULT_READ_ERROR"
            errors.append(
                ObservationError(str(result_path), code, str(exc)[:500])
            )
            if coding_route:
                sha256 = candidate.stable.sha256 if candidate is not None else None
                if sha256 is None:
                    try:
                        stable = read_stable(
                            result_path,
                            max_bytes=config.max_json_bytes,
                            retries=config.stable_read_retries,
                            delay_seconds=config.stable_read_delay_seconds,
                        )
                        sha256 = stable.sha256
                    except OSError:
                        pass
                invalid_result = invalid_result_evidence(result_path, str(exc), sha256=sha256)
                invalid_result_status_path = (
                    coding_owner.status.path if coding_owner is not None else None
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
        result_status_path=result_status_path,
        invalid_result=invalid_result,
        invalid_result_status_path=invalid_result_status_path,
        manager_signals=tuple(manager_signals),
        errors=tuple(errors),
        result_acceptance_state=result_acceptance_state,
    )


def discover_suite(
    config: HarnessConfig,
    *,
    revalidate_results: bool = False,
    revalidate: bool = False,
) -> tuple[RunRecords, ...]:
    roots: dict[str, Path] = {}
    for pattern in config.run_globs:
        for candidate in config.suite_root.glob(pattern):
            if candidate.is_dir():
                resolved = candidate.resolve()
                roots[path_identity(resolved)] = resolved
    runs = []
    for run_root in sorted(roots.values(), key=path_identity):
        workspace = run_root / config.workspace_relpath
        if workspace.is_dir():
            runs.append(
                discover_run(
                    run_root,
                    workspace,
                    config,
                    revalidate_results=revalidate_results,
                    revalidate=revalidate,
                )
            )
    return tuple(runs)
