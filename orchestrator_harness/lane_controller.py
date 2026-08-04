"""Small, externally invoked controller for one persistent Codex lane turn.

The controller accepts either the original firmware/policy-bound invocation or the explicit
general-coding schema. It does not schedule lanes: it validates one manager-written invocation,
launches one child, and leaves factual process/output records for the read-only harness to observe.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .git_safety import (
    GitDeclaration,
    GitSafetyError,
    active_declaration_conflicts,
    declaration_from_invocation,
    inspect_repository,
    invalid_result_evidence,
    repository_status,
    validate_coding_result,
    validate_findings,
)
from .models import ProcessInfo, iso_utc
from .processes import process_snapshot
from .resource_locks import ResourceClaims, ResourceLockError


class InvocationError(ValueError):
    pass


CODING_INVOCATION_SCHEMA = "orchestrator-coding-invocation/v1"

# These keys are route discriminators, rather than optional aliases.  Silently
# ignoring one on the other route would make a hand-written mixed invocation
# appear valid while dropping its safety contract.
_FIRMWARE_ONLY_FIELDS = frozenset({
    "policy_sha256",
    "leases",
    "board_tokens",
    "mcp_servers",
    "server_snapshot",
})
_CODING_ONLY_FIELDS = frozenset({
    "worker_invocation_id",
    "runtime_root",
    "resource_lock_root",
    "event_log_path",
    "event_log",
    "lane_id",
    "resources",
    "exclusive_resources",
    "repository",
    "git",
    "resume_identity",
    "resume",
    "codex",
    "codex_settings",
    "finding_gate",
    "child_environment_isolation",
})
# ``model_settings`` is retained by both routes, but coding accepts these
# nested settings only as a compatibility fallback.  They must not be silently
# discarded when a schema-less firmware invocation is parsed.
_CODING_ONLY_MODEL_SETTINGS_FIELDS = frozenset({
    "command",
    "config_overrides",
    "sandbox",
    "approval_policy",
})


def _utc() -> str:
    return iso_utc(datetime.now(timezone.utc)) or ""


_PHYSICAL_CHILD_PREFIXES = ("MCP_", "PYOCD_", "BYO_MCP_", "FIRMWARE_MCP_", "PROBE_", "TARGET_", "SERIAL_", "OPENOCD_", "JLINK_", "CREDENTIAL_", "SECRET_", "TOKEN_", "FIRMWARE_CONFIG_")
_CHILD_ENV_ALLOW = frozenset({"PATH", "PATHEXT", "SYSTEMROOT", "COMSPEC", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "LANG", "LC_ALL", "PYTHONUTF8", "PYTHONIOENCODING", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"})


def isolated_coding_child_environment(inherited: Mapping[str, str] | None = None) -> tuple[dict[str, str], list[str]]:
    """Return the optional Codex-only child environment without physical lane capability."""
    source = dict(os.environ if inherited is None else inherited)
    cleared = sorted(key for key in source if key.upper().startswith(_PHYSICAL_CHILD_PREFIXES))
    allowed = {key: value for key, value in source.items() if key.upper() in _CHILD_ENV_ALLOW and not key.upper().startswith(_PHYSICAL_CHILD_PREFIXES)}
    return allowed, cleared


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _append_event(path: Path, value: dict[str, Any]) -> None:
    data = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "ab", closefd=True) as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _safe_path(value: object, *, root: Path, name: str, must_exist: bool = False) -> Path:
    if not isinstance(value, str) or not value:
        raise InvocationError(f"{name} must be a non-empty path string")
    candidate = Path(value).expanduser().resolve(strict=False)
    if not _inside(candidate, root):
        raise InvocationError(f"{name} escapes its allowed root")
    if must_exist and not candidate.is_file():
        raise InvocationError(f"{name} is not an existing regular file")
    return candidate


def _string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise InvocationError(f"{key} must be a non-empty string")
    return value.strip()


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise InvocationError(f"{name} must be a list of non-empty strings")
    return list(value)


def _reject_foreign_fields(
    raw: Mapping[str, Any], *, route: str, fields: frozenset[str],
) -> None:
    present = sorted(field for field in fields if field in raw)
    if present:
        raise InvocationError(
            f"{route} invocation contains fields reserved for the other route: "
            + ", ".join(present)
        )


def _reject_firmware_coding_model_settings(raw: Mapping[str, Any]) -> None:
    settings = raw.get("model_settings")
    if not isinstance(settings, Mapping):
        return
    _reject_foreign_fields(
        settings,
        route="schema-less firmware model_settings",
        fields=_CODING_ONLY_MODEL_SETTINGS_FIELDS,
    )


@dataclass(frozen=True)
class Invocation:
    invocation_schema: str | None
    worker_invocation_id: str | None
    action: str
    run_root: Path
    workspace: Path
    prompt_path: Path
    prompt_sha256: str
    prompt_bytes: bytes
    policy_path: Path | None
    policy_sha256: str | None
    label: str
    doer: str
    task: str
    phase: str
    lane_id: str
    leases: list[str]
    board_tokens: list[str]
    mcp_servers: list[str]
    server_snapshot: dict[str, Any]
    resources: list[str]
    resource_lock_root: Path | None
    model: str
    reasoning_effort: str
    service_tier: str
    codex_command: list[str]
    config_overrides: list[str]
    sandbox: str
    approval_policy: str
    requested_thread_id: str | None
    repository: GitDeclaration | None
    status_path: Path
    jsonl_path: Path
    stderr_path: Path
    last_message_path: Path
    event_log: Path
    finding_gate: dict[str, str] | None = None
    child_environment_isolation: bool = False


def _common_paths(raw: dict[str, Any]) -> tuple[str, Path, Path, Path, str, bytes, dict[str, Path]]:
    action = _string(raw, "action").lower()
    if action not in {"start", "resume"}:
        raise InvocationError("action must be start or resume")
    run_root_value = raw.get("run_root")
    if not isinstance(run_root_value, str) or not run_root_value:
        raise InvocationError("run_root must be a path")
    try:
        run_root = Path(run_root_value).resolve(strict=True)
    except OSError as exc:
        raise InvocationError(f"run_root cannot be resolved: {exc}") from exc
    if not run_root.is_dir():
        raise InvocationError("run_root must be an existing directory")
    workspace = (run_root / ".agent-workspace").resolve(strict=False)
    if workspace.parent != run_root:
        raise InvocationError("invalid run workspace")
    try:
        workspace.mkdir(exist_ok=True)
    except OSError as exc:
        raise InvocationError(f"cannot create run workspace: {exc}") from exc
    prompt_path = _safe_path(raw.get("prompt_path"), root=run_root, name="prompt_path", must_exist=True)
    outputs = raw.get("output_paths")
    if not isinstance(outputs, dict):
        raise InvocationError("output_paths must be an object")
    status_path = _safe_path(outputs.get("status"), root=workspace, name="status")
    jsonl_path = _safe_path(outputs.get("jsonl"), root=workspace, name="jsonl")
    stderr_path = _safe_path(outputs.get("stderr"), root=workspace, name="stderr")
    last_message_path = _safe_path(outputs.get("last_message"), root=workspace, name="last_message")
    prompt_sha256 = _string(raw, "prompt_sha256").lower()
    if len(prompt_sha256) != 64 or any(char not in "0123456789abcdef" for char in prompt_sha256):
        raise InvocationError("prompt_sha256 must be a SHA-256 hex digest")
    try:
        prompt_bytes = prompt_path.read_bytes()
    except OSError as exc:
        raise InvocationError(f"cannot verify prompt: {exc}") from exc
    if hashlib.sha256(prompt_bytes).hexdigest() != prompt_sha256:
        raise InvocationError("prompt bytes do not match prompt_sha256")
    return action, run_root, workspace, prompt_path, prompt_sha256, prompt_bytes, {
        "status": status_path,
        "jsonl": jsonl_path,
        "stderr": stderr_path,
        "last_message": last_message_path,
    }


def _resume_thread(raw: dict[str, Any]) -> str | None:
    requested_thread = raw.get("resume_thread_id")
    if requested_thread is not None and (not isinstance(requested_thread, str) or not requested_thread.strip()):
        raise InvocationError("resume_thread_id must be a non-empty string when supplied")
    return requested_thread.strip() if isinstance(requested_thread, str) else None


def _load_firmware_invocation(raw: dict[str, Any]) -> Invocation:
    _reject_foreign_fields(raw, route="schema-less firmware", fields=_CODING_ONLY_FIELDS)
    _reject_firmware_coding_model_settings(raw)
    action, run_root, workspace, prompt_path, prompt_sha256, prompt_bytes, outputs = _common_paths(raw)
    label = _string(raw, "label")
    expected = {
        "status": f"{label}_controller.status.json",
        "jsonl": f"{label}_codex.jsonl",
    }
    if outputs["status"].name != expected["status"] or outputs["jsonl"].name != expected["jsonl"]:
        raise InvocationError("controller status/JSONL names must match the lane label")
    server_snapshot = raw.get("server_snapshot")
    if not isinstance(server_snapshot, dict):
        raise InvocationError("server_snapshot must be an object")
    model_settings = raw.get("model_settings")
    if not isinstance(model_settings, dict):
        raise InvocationError("model_settings must be an object")
    command = raw.get("codex_command", ["codex"])
    command = _string_list(command, "codex_command")
    overrides = _string_list(raw.get("config_overrides", []), "config_overrides")
    requested_thread = _resume_thread(raw)
    suite_root = Path(__file__).resolve().parent.parent
    policy_path = suite_root / ".agent-workspace" / "AUTONOMOUS_EXECUTION_POLICY.md"
    sidecar_path = suite_root / ".agent-workspace" / "AUTONOMOUS_EXECUTION_POLICY.sha256"
    policy_sha256 = _string(raw, "policy_sha256").lower()
    if len(policy_sha256) != 64 or any(char not in "0123456789abcdef" for char in policy_sha256):
        raise InvocationError("policy_sha256 must be a SHA-256 hex digest")
    try:
        policy_bytes = policy_path.read_bytes()
        sidecar = sidecar_path.read_text(encoding="utf-8").split()[0].lower()
    except (OSError, IndexError) as exc:
        raise InvocationError(f"cannot verify policy-bound prompt: {exc}") from exc
    if hashlib.sha256(policy_bytes).hexdigest() != policy_sha256 or sidecar != policy_sha256:
        raise InvocationError("canonical policy file or sidecar does not match policy_sha256")
    try:
        prompt_text = prompt_bytes.decode("utf-8-sig")
        policy_text = policy_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InvocationError("policy-bound prompt must be UTF-8") from exc
    markers = (
        "## AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
        f"Policy SHA-256: `{policy_sha256}`",
        "## END AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
        "## FINAL PRECEDENCE REMINDER",
        f"Policy `{policy_sha256}` and the latest signed run amendment control.",
    )
    if any(marker not in prompt_text for marker in markers) or policy_text not in prompt_text:
        raise InvocationError("prompt is not the required policy-bound prompt composition")
    log_root = suite_root / "multi-agent-logs" / "orchestrator-harness"
    event_log = _safe_path(raw.get("lane_event_log"), root=log_root, name="lane_event_log")
    if event_log.name != "LANE_EVENTS.jsonl":
        raise InvocationError("lane_event_log must be named LANE_EVENTS.jsonl")
    event_log.parent.mkdir(parents=True, exist_ok=True)
    return Invocation(
        None, None, action, run_root, workspace, prompt_path,
        prompt_sha256, prompt_bytes, policy_path, policy_sha256, label, _string(raw, "doer"),
        _string(raw, "task"),
        _string(raw, "phase"), _string(raw, "declared_lane_id"), _string_list(raw.get("leases", []), "leases"),
        _string_list(raw.get("board_tokens", []), "board_tokens"), _string_list(raw.get("mcp_servers", []), "mcp_servers"),
        server_snapshot, [], None, _string(model_settings, "model"), _string(model_settings, "reasoning_effort"),
        _string(model_settings, "service_tier"), command, overrides, "danger-full-access", "never", requested_thread, None,
        outputs["status"], outputs["jsonl"], outputs["stderr"], outputs["last_message"], event_log,
    )


def _coding_settings(raw: dict[str, Any]) -> tuple[str, str, str, list[str], list[str], str, str]:
    codex = raw.get("codex", raw.get("codex_settings"))
    if codex is not None and not isinstance(codex, dict):
        raise InvocationError("codex must be an object")
    settings = codex if isinstance(codex, dict) else raw.get("model_settings")
    if not isinstance(settings, dict):
        raise InvocationError("codex or model_settings must be an object")
    command_value = settings.get("command", raw.get("codex_command", ["codex"]))
    overrides_value = settings.get("config_overrides", raw.get("config_overrides", []))
    sandbox = _string(settings, "sandbox")
    if sandbox not in {"read-only", "workspace-write", "danger-full-access"}:
        raise InvocationError("sandbox must be read-only, workspace-write, or danger-full-access")
    approval_policy = _string(settings, "approval_policy")
    if approval_policy not in {"untrusted", "on-failure", "on-request", "never"}:
        raise InvocationError("approval_policy is not a supported Codex approval policy")
    return (
        _string(settings, "model"),
        _string(settings, "reasoning_effort"),
        _string(settings, "service_tier"),
        _string_list(command_value, "codex command"),
        _string_list(overrides_value, "Codex config_overrides"),
        sandbox,
        approval_policy,
    )


def _load_coding_invocation(raw: dict[str, Any]) -> Invocation:
    _reject_foreign_fields(raw, route="coding", fields=_FIRMWARE_ONLY_FIELDS)
    action, run_root, workspace, prompt_path, prompt_sha256, prompt_bytes, outputs = _common_paths(raw)
    if (
        outputs["status"].parent != workspace
        or outputs["status"].suffix != ".json"
        or outputs["status"].name.startswith(".")
        or outputs["status"].name == "RESULT.json"
    ):
        raise InvocationError(
            "coding controller status must be a direct, non-hidden *.json file under .agent-workspace"
        )
    runtime_value = raw.get("runtime_root")
    if not isinstance(runtime_value, str) or not runtime_value:
        raise InvocationError("runtime_root must be a non-empty path string")
    runtime_root = Path(runtime_value).expanduser().resolve(strict=False)
    if not runtime_root.is_dir():
        raise InvocationError("runtime_root must be an existing directory")
    lock_value = raw.get("resource_lock_root", str(runtime_root / "coding-resource-locks"))
    resource_lock_root = _safe_path(
        lock_value, root=runtime_root, name="resource_lock_root"
    )
    if resource_lock_root == runtime_root:
        raise InvocationError("resource_lock_root must be below runtime_root")
    event_value = raw.get("event_log_path", raw.get("event_log", raw.get("lane_event_log")))
    event_log = _safe_path(event_value, root=runtime_root, name="event_log_path")
    event_log.parent.mkdir(parents=True, exist_ok=True)
    worker_invocation_id = _string(raw, "worker_invocation_id")
    lane_id_value = raw.get("lane_id", raw.get("declared_lane_id"))
    if not isinstance(lane_id_value, str) or not lane_id_value.strip():
        raise InvocationError("lane_id must be a non-empty string")
    lane_id = lane_id_value.strip()
    legacy_resources = raw.get("resources")
    exclusive_resources = raw.get("exclusive_resources")
    if (
        legacy_resources is not None
        and exclusive_resources is not None
        and legacy_resources != exclusive_resources
    ):
        raise InvocationError(
            "resources and exclusive_resources must match when both are supplied"
        )
    resources = _string_list(
        exclusive_resources if exclusive_resources is not None else legacy_resources or [],
        "exclusive_resources",
    )
    try:
        repository = declaration_from_invocation(raw, run_root)
    except GitSafetyError as exc:
        raise InvocationError(str(exc)) from exc
    model, reasoning, tier, command, overrides, sandbox, approval = _coding_settings(raw)
    requested_thread = _resume_thread(raw)
    resume_identity = raw.get("resume_identity", raw.get("resume"))
    if resume_identity is not None:
        if not isinstance(resume_identity, dict):
            raise InvocationError("resume_identity must be an object")
        identity_worker = resume_identity.get("worker_invocation_id")
        identity_thread = resume_identity.get("thread_id")
        if identity_worker is not None and identity_worker != worker_invocation_id:
            raise InvocationError("resume identity worker_invocation_id mismatch")
        if identity_thread is not None and (not isinstance(identity_thread, str) or not identity_thread.strip()):
            raise InvocationError("resume identity thread_id must be a non-empty string")
        normalized_identity_thread = identity_thread.strip() if isinstance(identity_thread, str) else None
        if requested_thread and normalized_identity_thread and requested_thread != normalized_identity_thread:
            raise InvocationError("conflicting requested resume thread IDs")
        requested_thread = requested_thread or normalized_identity_thread
    label_value = raw.get("label", worker_invocation_id)
    if not isinstance(label_value, str) or not label_value.strip():
        raise InvocationError("label must be a non-empty string when supplied")
    doer_value = raw.get("doer", lane_id)
    if not isinstance(doer_value, str) or not doer_value.strip():
        raise InvocationError("doer must be a non-empty string when supplied")
    finding_gate = raw.get("finding_gate")
    normalized_gate = None
    if finding_gate is not None:
        if not isinstance(finding_gate, dict) or set(finding_gate) != {"role", "path"}:
            raise InvocationError("finding_gate must contain only role and path")
        if finding_gate.get("role") not in {"reviewer", "test_writer", "test_executor"}:
            raise InvocationError("finding_gate role is invalid")
        finding_path = _safe_path(finding_gate.get("path"), root=workspace, name="finding_gate.path")
        if finding_path.parent != workspace or finding_path.name != "FINDINGS.json":
            raise InvocationError("finding_gate.path must be workspace/FINDINGS.json")
        normalized_gate = {"role": finding_gate["role"], "path": str(finding_path)}
    isolation = raw.get("child_environment_isolation", False)
    if not isinstance(isolation, bool):
        raise InvocationError("child_environment_isolation must be boolean")
    return Invocation(
        CODING_INVOCATION_SCHEMA, worker_invocation_id, action, run_root, workspace, prompt_path,
        prompt_sha256, prompt_bytes, None, None, label_value.strip(), doer_value.strip(), _string(raw, "task"),
        _string(raw, "phase"), lane_id, [], [], [], {}, resources, resource_lock_root,
        model, reasoning, tier, command,
        overrides, sandbox, approval, requested_thread, repository, outputs["status"], outputs["jsonl"],
        outputs["stderr"], outputs["last_message"], event_log, normalized_gate, isolation,
    )


def load_invocation(path: Path) -> Invocation:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvocationError(f"cannot read invocation: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvocationError("invocation root must be an object")
    schema = raw.get("schema")
    if schema is not None and (not isinstance(schema, str) or not schema):
        raise InvocationError("schema must be a non-empty string when supplied")
    if schema == CODING_INVOCATION_SCHEMA:
        return _load_coding_invocation(raw)
    if schema is None:
        return _load_firmware_invocation(raw)
    raise InvocationError(f"unsupported invocation schema: {schema}")


def _identity(pid: int, *, parent: int | None = None, timeout: float = 5.0) -> ProcessInfo:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = process_snapshot()
        item = snapshot.by_pid.get(pid) if snapshot.complete else None
        if item is not None and item.created_utc is not None and (parent is None or item.ppid == parent):
            return item
        time.sleep(0.05)
    raise RuntimeError(f"cannot establish exact process identity for PID {pid}")


def _thread_id(line: bytes) -> str | None:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("type") != "thread.started":
        return None
    for key in ("thread_id", "threadId"):
        item = value.get(key)
        if isinstance(item, str) and item:
            return item
    return None


def _shutdown_exact_child(
    process: subprocess.Popen[bytes], *, timeout_seconds: float = 5.0,
) -> tuple[bool, dict[str, Any]]:
    """Bound shutdown to the captured Popen handle and prove that it was reaped."""
    evidence: dict[str, Any] = {
        "codex_pid": process.pid,
        "cleanup_confirmed": False,
        "terminate_attempted": False,
        "kill_attempted": False,
    }

    def reap(stage: str, timeout: float) -> bool:
        try:
            evidence["exit_code"] = process.wait(timeout=timeout)
            evidence["reaped_after"] = stage
            evidence["cleanup_confirmed"] = True
            return True
        except subprocess.TimeoutExpired:
            evidence[f"{stage}_wait_timed_out"] = True
        except BaseException as exc:
            evidence[f"{stage}_wait_error"] = f"{type(exc).__name__}: {exc}"
        return False

    try:
        already_exited = process.poll()
    except BaseException as exc:
        evidence["initial_poll_error"] = f"{type(exc).__name__}: {exc}"
    else:
        if already_exited is not None and reap("observed_exit", 0):
            return True, evidence

    evidence["terminate_attempted"] = True
    try:
        process.terminate()
    except BaseException as exc:
        evidence["terminate_error"] = f"{type(exc).__name__}: {exc}"
    if reap("terminate", timeout_seconds):
        return True, evidence

    evidence["kill_attempted"] = True
    try:
        process.kill()
    except BaseException as exc:
        evidence["kill_error"] = f"{type(exc).__name__}: {exc}"
    if reap("kill", timeout_seconds):
        return True, evidence

    evidence["error"] = "exact Codex child exit and reap could not be proven"
    return False, evidence


def _read_prior_status(invocation: Invocation) -> dict[str, Any] | None:
    if not invocation.status_path.is_file():
        return None
    try:
        value = json.loads(invocation.status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _event(invocation: Invocation, event: str, **fields: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "utc": _utc(),
        "event": event,
        "label": invocation.label,
        "declared_lane_id": invocation.lane_id,
        "invocation_schema": invocation.invocation_schema,
        "worker_invocation_id": invocation.worker_invocation_id,
    }
    value.update(fields)
    return value


def _persisted_repository(invocation: Invocation, prior_status: Mapping[str, Any]) -> str:
    assert invocation.repository is not None
    nested = prior_status.get("repository")
    if not isinstance(nested, Mapping):
        raise InvocationError("coding resume requires persisted repository identity")
    expected = {
        "common_dir": str(invocation.repository.common_dir),
        "worktree_root": str(invocation.repository.worktree_root),
        "branch": invocation.repository.branch,
    }
    for key, value in expected.items():
        persisted = nested.get(key)
        if key.endswith("dir") or key.endswith("root"):
            try:
                if not isinstance(persisted, str) or Path(persisted).resolve(strict=False) != Path(value).resolve(strict=False):
                    raise InvocationError(f"resume repository {key} does not match persisted status")
            except OSError as exc:
                raise InvocationError(f"cannot compare persisted repository {key}: {exc}") from exc
        elif persisted != value:
            raise InvocationError(f"resume repository {key} does not match persisted status")
    if nested.get("base_commit") != invocation.repository.base_commit:
        raise InvocationError("resume base_commit does not match persisted status")
    starting_commit = nested.get("starting_commit")
    if not isinstance(starting_commit, str) or not starting_commit:
        raise InvocationError("coding resume requires persisted starting_commit")
    return starting_commit


def _coding_result_validation(invocation: Invocation) -> tuple[dict[str, Any], bool]:
    assert invocation.repository is not None and invocation.worker_invocation_id is not None
    path = invocation.workspace / "RESULT.json"
    if not path.exists():
        return {"state": "MISSING", "path": str(path)}, True
    sha256 = None
    try:
        data = path.read_bytes()
        sha256 = hashlib.sha256(data).hexdigest()
        if len(data) > 1024 * 1024:
            raise GitSafetyError("coding result exceeds the 1 MiB controller limit")
        value = json.loads(data.decode("utf-8-sig"))
        if not isinstance(value, Mapping):
            raise GitSafetyError("coding result root must be an object")
        identity = validate_coding_result(
            value,
            lane_id=invocation.lane_id,
            worker_invocation_id=invocation.worker_invocation_id,
            declaration=invocation.repository,
        )
        findings: dict[str, Any] | None = None
        if invocation.finding_gate is not None:
            findings_path = Path(invocation.finding_gate["path"])
            findings = validate_findings(
                findings_path, lane_id=invocation.lane_id, worker_invocation_id=invocation.worker_invocation_id,
                role=invocation.finding_gate["role"], commit=identity.head_commit, outcome=value["outcome"],
            )
        return {
            "state": "VALID",
            "path": str(path),
            "sha256": sha256,
            "commit": identity.head_commit,
            "findings": findings,
        }, True
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, GitSafetyError) as exc:
        return invalid_result_evidence(path, str(exc), sha256=sha256), False


def run(invocation: Invocation) -> int:
    prompt = invocation.prompt_bytes
    if not prompt:
        raise InvocationError("prompt is empty")
    prior_status = _read_prior_status(invocation)
    prior_thread_value = prior_status.get("thread_id") if prior_status else None
    prior_thread = prior_thread_value if isinstance(prior_thread_value, str) and prior_thread_value else None
    starting_commit = None
    git_identity = None
    if invocation.repository is not None:
        try:
            git_identity = inspect_repository(invocation.repository)
        except GitSafetyError as exc:
            raise InvocationError(str(exc)) from exc
    if invocation.action == "resume":
        if invocation.worker_invocation_id is not None:
            if prior_status is None:
                raise InvocationError("coding resume requires persisted controller status")
            if prior_status.get("worker_invocation_id") != invocation.worker_invocation_id:
                raise InvocationError("resume worker_invocation_id does not match persisted status")
            if prior_status.get("invocation_schema") != invocation.invocation_schema:
                raise InvocationError("resume invocation schema does not match persisted status")
            if prior_thread is None:
                raise InvocationError("coding resume requires a persisted lane thread ID")
            starting_commit = _persisted_repository(invocation, prior_status)
        thread = invocation.requested_thread_id or prior_thread
        if not thread or (prior_thread and invocation.requested_thread_id and prior_thread != invocation.requested_thread_id):
            raise InvocationError("resume requires the persisted lane thread ID")
    else:
        thread = None
        if git_identity is not None:
            starting_commit = git_identity.head_commit
    if invocation.repository is not None:
        try:
            conflicts = active_declaration_conflicts(
                invocation.repository,
                current_status_path=invocation.status_path,
            )
        except GitSafetyError as exc:
            raise InvocationError(str(exc)) from exc
        if conflicts:
            raise InvocationError("duplicate ACTIVE coding declaration: " + "; ".join(conflicts))
        try:
            launch_identity = inspect_repository(invocation.repository)
        except GitSafetyError as exc:
            raise InvocationError(str(exc)) from exc
        if git_identity is None or launch_identity.head_commit != git_identity.head_commit:
            raise InvocationError("coding worktree HEAD changed during pre-launch validation")
    controller = _identity(os.getpid())
    argv = [*invocation.codex_command, "exec"]
    if invocation.action == "resume":
        argv.extend(["resume", thread])  # type: ignore[arg-type]
    argv.append("--dangerously-bypass-approvals-and-sandbox")
    argv.extend([
        "--ignore-user-config", "--skip-git-repo-check",
        "-c", f'approval_policy="{invocation.approval_policy}"', "-m", invocation.model,
        "-c", f'model_reasoning_effort="{invocation.reasoning_effort}"',
        "-c", f'service_tier="{invocation.service_tier}"',
    ])
    if invocation.worker_invocation_id is None:
        argv.extend(["-c", 'approvals_reviewer="user"'])
    for override in invocation.config_overrides:
        argv.extend(["-c", override])
    argv.extend(["--json", "--output-last-message", str(invocation.last_message_path)])
    if invocation.action == "start":
        argv.extend(["--cd", str(invocation.run_root)])
    argv.append("-")
    state: dict[str, Any] = {
        "schema": "orchestrator-lane-controller/v1", "state": "LAUNCH_FAILED", "started_utc": _utc(),
        "invocation_schema": invocation.invocation_schema,
        "worker_invocation_id": invocation.worker_invocation_id,
        "controller_pid": controller.pid, "controller_started_utc": iso_utc(controller.created_utc),
        "controller_created_utc": iso_utc(controller.created_utc), "codex_pid": None, "codex_started_utc": None,
        "doer": invocation.doer, "task": invocation.task, "phase": invocation.phase,
        "declared_lane_id": invocation.lane_id, "thread_id": thread, "leases": invocation.leases,
        "board_tokens": invocation.board_tokens, "mcp_servers": invocation.mcp_servers,
        "server_snapshot": invocation.server_snapshot, "resources": invocation.resources,
        "exclusive_resources": invocation.resources,
        "resource_lock_root": str(invocation.resource_lock_root) if invocation.resource_lock_root else None,
        "held_resource_claims": [], "waiting_resource_claim": None,
        "resource_claim_findings": [],
        "jsonl_path": str(invocation.jsonl_path),
        "stderr_path": str(invocation.stderr_path), "last_message_path": str(invocation.last_message_path),
        "prompt_path": str(invocation.prompt_path), "prompt_sha256": invocation.prompt_sha256,
        "policy_path": str(invocation.policy_path) if invocation.policy_path is not None else None,
        "policy_sha256": invocation.policy_sha256,
        "launcher_settings": {"model": invocation.model, "model_reasoning_effort": invocation.reasoning_effort,
            "service_tier": invocation.service_tier, "sandbox": invocation.sandbox,
            "approval_policy": invocation.approval_policy,
            "approvals_reviewer": "user" if invocation.worker_invocation_id is None else None,
            "config_overrides": invocation.config_overrides,
            "jsonl": True, "ephemeral": False, "action": invocation.action, "argv": argv[:-1]},
    }
    if git_identity is not None:
        repository = repository_status(git_identity, starting_commit=starting_commit)
        state.update({
            "repository": repository,
            "repository_common_dir": repository["common_dir"],
            "worktree_root": repository["worktree_root"],
            "branch": repository["branch"],
            "base_commit": repository["base_commit"],
            "starting_commit": repository["starting_commit"],
        })
    lock = threading.Lock()
    process: subprocess.Popen[bytes] | None = None
    resource_claims: ResourceClaims | None = None
    child_exit_confirmed = True
    try:
        if invocation.worker_invocation_id is not None and invocation.resources:
            assert invocation.resource_lock_root is not None
            resource_claims = ResourceClaims(
                invocation.resource_lock_root,
                invocation.lane_id,
                invocation.worker_invocation_id,
                controller,
            )

            def record_wait(wait: dict[str, Any]) -> None:
                state.update({
                    "state": "WAITING_RESOURCE",
                    "held_resource_claims": resource_claims.held,
                    "waiting_resource_claim": wait,
                    "resource_claim_findings": list(resource_claims.findings),
                })
                _atomic_json(invocation.status_path, state)

            resource_claims.acquire_all(invocation.resources, on_wait=record_wait)
            state.update({
                "state": "LAUNCH_FAILED",
                "held_resource_claims": resource_claims.held,
                "waiting_resource_claim": None,
                "resource_claim_findings": list(resource_claims.findings),
            })
            _atomic_json(invocation.status_path, state)
        with invocation.jsonl_path.open("wb") as jsonl, invocation.stderr_path.open("wb") as stderr:
            child_env = None
            if invocation.child_environment_isolation:
                child_env, cleared = isolated_coding_child_environment()
                state["child_environment_isolation"] = {"enabled": True, "cleared_variable_names": cleared}
            process = subprocess.Popen(argv, cwd=invocation.run_root, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=child_env)
            child_exit_confirmed = False
            child = _identity(process.pid, parent=controller.pid)
            state.update({"state": "RUNNING_CODEX", "codex_pid": child.pid, "codex_started_utc": iso_utc(child.created_utc), "codex_created_utc": iso_utc(child.created_utc)})
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, _event(
                invocation, "CODEX_STARTED", controller_pid=controller.pid, codex_pid=child.pid,
            ))
            assert process.stdin is not None and process.stdout is not None and process.stderr is not None
            process.stdin.write(prompt); process.stdin.close()
            def drain(source: Any, destination: Any, parse: bool) -> None:
                nonlocal state
                for line in iter(source.readline, b""):
                    destination.write(line); destination.flush(); os.fsync(destination.fileno())
                    if parse:
                        found = _thread_id(line)
                        if found:
                            with lock:
                                if invocation.action == "resume" and found != thread:
                                    state["thread_identity_error"] = (
                                        f"child thread.started identity {found!r} does not match "
                                        f"validated resume thread {thread!r}"
                                    )
                                else:
                                    state["thread_id"] = found
                                _atomic_json(invocation.status_path, state)
            out_thread = threading.Thread(target=drain, args=(process.stdout, jsonl, True), daemon=True)
            err_thread = threading.Thread(target=drain, args=(process.stderr, stderr, False), daemon=True)
            out_thread.start(); err_thread.start()
            exit_code = process.wait()
            child_exit_confirmed = True
            out_thread.join(); err_thread.join()
            process.stdout.close(); process.stderr.close()
        thread_identity_error = state.get("thread_identity_error")
        if isinstance(thread_identity_error, str):
            state.update({
                "state": "CONTROLLER_FAILED", "exit_code": exit_code, "ended_utc": _utc(),
                "error": thread_identity_error,
            })
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, _event(
                invocation, "CONTROLLER_FAILED", exit_code=exit_code, error=thread_identity_error,
                thread_id=state.get("thread_id"),
            ))
            return 1
        if invocation.action == "start" and not state.get("thread_id"):
            state.update({"state": "LAUNCH_FAILED", "exit_code": exit_code, "ended_utc": _utc(), "error": "Codex exited without thread.started/thread_id; inspect stderr_path"})
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, _event(invocation, "LAUNCH_FAILED", exit_code=exit_code))
            return 1
        result_valid = True
        if invocation.repository is not None:
            result_validation, result_valid = _coding_result_validation(invocation)
            state["result_validation"] = result_validation
            state["result_valid"] = result_valid
        state.update({"state": "CODEX_EXITED", "exit_code": exit_code, "ended_utc": _utc()})
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, _event(
            invocation, "CODEX_EXITED", exit_code=exit_code, thread_id=state.get("thread_id"),
            result_validation=state.get("result_validation"),
        ))
        return exit_code if result_valid else 1
    except KeyboardInterrupt:
        cleanup_evidence: dict[str, Any] | None = None
        if process is not None and not child_exit_confirmed:
            child_exit_confirmed, cleanup_evidence = _shutdown_exact_child(process)
        if not child_exit_confirmed:
            retained_claims = resource_claims.held if resource_claims is not None else []
            error = "controller interrupted; exact Codex child shutdown could not be proven"
            state.update({
                "state": "COORDINATION_FAILED",
                "ended_utc": _utc(),
                "error": error,
                "coordination_failure": {
                    "error": error,
                    "trigger": "KeyboardInterrupt",
                    "child_shutdown": cleanup_evidence,
                    "retained_claims": retained_claims,
                },
                "held_resource_claims": retained_claims,
            })
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, _event(
                invocation, "COORDINATION_FAILED", error=error,
                child_shutdown=cleanup_evidence, retained_claims=retained_claims,
            ))
            return 130
        state.update({"state": "CONTROLLER_INTERRUPTED", "ended_utc": _utc()})
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, _event(invocation, "CONTROLLER_INTERRUPTED"))
        return 130
    except ResourceLockError as exc:
        retained_claims = resource_claims.held if resource_claims is not None else []
        state.update({
            "state": "COORDINATION_FAILED",
            "ended_utc": _utc(),
            "error": str(exc),
            "coordination_failure": {
                "error": str(exc),
                "retained_claims": retained_claims,
            },
            "held_resource_claims": retained_claims,
        })
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, _event(invocation, "COORDINATION_FAILED", error=str(exc)))
        return 1
    except Exception as exc:
        cleanup_evidence = None
        if process is not None and not child_exit_confirmed:
            child_exit_confirmed, cleanup_evidence = _shutdown_exact_child(process)
        if not child_exit_confirmed:
            retained_claims = resource_claims.held if resource_claims is not None else []
            error = f"{exc}; exact Codex child shutdown could not be proven"
            state.update({
                "state": "COORDINATION_FAILED",
                "ended_utc": _utc(),
                "error": error,
                "coordination_failure": {
                    "error": error,
                    "trigger": type(exc).__name__,
                    "trigger_error": str(exc),
                    "child_shutdown": cleanup_evidence,
                    "retained_claims": retained_claims,
                },
                "held_resource_claims": retained_claims,
            })
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, _event(
                invocation, "COORDINATION_FAILED", error=error,
                child_shutdown=cleanup_evidence, retained_claims=retained_claims,
            ))
            return 1
        state.update({"state": "CONTROLLER_FAILED" if state.get("codex_pid") else "LAUNCH_FAILED", "ended_utc": _utc(), "error": str(exc)})
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, _event(invocation, state["state"], error=str(exc)))
        return 1
    finally:
        if resource_claims is not None and child_exit_confirmed:
            release_failures = resource_claims.release_all()
            state["held_resource_claims"] = resource_claims.held
            state["waiting_resource_claim"] = None
            if release_failures:
                state["resource_release_errors"] = release_failures
            try:
                _atomic_json(invocation.status_path, state)
            except OSError:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Launch one observable Codex lane turn")
    parser.add_argument("invocation", type=Path)
    args = parser.parse_args(argv)
    try:
        return run(load_invocation(args.invocation))
    except InvocationError as exc:
        print(f"lane-controller invocation error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
