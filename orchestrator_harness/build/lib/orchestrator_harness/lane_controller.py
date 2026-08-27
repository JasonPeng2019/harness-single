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
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
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
    validate_task_result_repository,
    validate_coding_result,
    validate_findings,
)
from .invocation import (
    CANONICAL_INVOCATION_SCHEMA,
    CODING_V1_FIRMWARE_ONLY_FIELDS,
    CanonicalInvocation,
    InvocationValidationError,
    parse_canonical_invocation,
    validate_coding_v1_fields,
)
from .models import ProcessInfo, iso_utc
from .profile import ProfileError, RuntimeProfile, build_child_environment
from .process_supervisor import (
    CleanupResult,
    ProcessBoundary,
    ProcessBoundaryUnsupported,
    ProcessSupervisor,
)
from .prompt_bundle import PromptBundle, PromptBundleError, bundle_from_record
from .provider import (
    PROVIDER_OPERATION_NAMES,
    PROVIDER_TERMINAL_OUTCOMES,
    ProviderAdapterError,
    ProviderEvent,
    ProviderHandoff,
    ProviderLaunchSpec,
    ProviderResumeDecision,
    build_provider_evidence,
    classify_operation,
    claude_config_override_env,
    decide_resume_or_handoff,
    provider_adapter,
    structured_handoff,
    provider_default_command,
    unsupported_operation_result,
)
from .processes import process_snapshot
from .resource_locks import ResourceClaims, ResourceLockError
from .workspace_overlay import verify_overlay_receipt
from .stable_io import append_jsonl_record
from .lane_lifecycle import (
    LaneLifecycleError,
    _LifecycleAdmission,
    _admit_lifecycle_registry,
    _update_lifecycle_registry,
)
from .mutation import (
    MutationConflict,
    MutationUnsupported,
    capture_target,
    replace as mutation_replace,
)
from .resume import (
    ResumeAdmissionError,
    require_resume_admission,
)
from .task import (
    COMPLETION_REVIEW_FILENAME,
    ORCHESTRATOR_ACCEPTANCE_FILENAME,
    TaskResult,
    TaskValidationError,
    read_task_advancement,
    task_card_from_identity,
    validate_task_result,
)


class InvocationError(ValueError):
    pass


CODING_INVOCATION_SCHEMA = "orchestrator-coding-invocation/v1"

# These keys are route discriminators, rather than optional aliases.  Silently
# ignoring one on the other route would make a hand-written mixed invocation
# appear valid while dropping its safety contract.
_FIRMWARE_ONLY_FIELDS = CODING_V1_FIRMWARE_ONLY_FIELDS
_CODING_ONLY_FIELDS = frozenset(
    {
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
    }
)
# ``model_settings`` is retained by both routes, but coding accepts these
# nested settings only as a compatibility fallback.  They must not be silently
# discarded when a schema-less firmware invocation is parsed.
_CODING_ONLY_MODEL_SETTINGS_FIELDS = frozenset(
    {
        "command",
        "config_overrides",
        "sandbox",
        "approval_policy",
    }
)


def _utc() -> str:
    return iso_utc(datetime.now(timezone.utc)) or ""


_PHYSICAL_CHILD_PREFIXES = (
    "MCP_",
    "PYOCD_",
    "BYO_MCP_",
    "FIRMWARE_MCP_",
    "PROBE_",
    "TARGET_",
    "SERIAL_",
    "OPENOCD_",
    "JLINK_",
    "CREDENTIAL_",
    "SECRET_",
    "TOKEN_",
    "FIRMWARE_CONFIG_",
)
_CHILD_ENV_ALLOW = frozenset(
    {
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "COMSPEC",
        "WINDIR",
        "TEMP",
        "TMP",
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "LANG",
        "LC_ALL",
        "PYTHONUTF8",
        "PYTHONIOENCODING",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    }
)


def isolated_coding_child_environment(
    inherited: Mapping[str, str] | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Return the optional Codex-only child environment without physical lane capability."""
    source = dict(os.environ if inherited is None else inherited)
    cleared = sorted(
        key for key in source if key.upper().startswith(_PHYSICAL_CHILD_PREFIXES)
    )
    allowed = {
        key: value
        for key, value in source.items()
        if key.upper() in _CHILD_ENV_ALLOW
        and not key.upper().startswith(_PHYSICAL_CHILD_PREFIXES)
    }
    return allowed, cleared


def apply_provider_env_overrides(
    child_env: dict[str, str] | None,
    env_overrides: Mapping[str, str],
) -> dict[str, str] | None:
    """Merge declared provider overrides after child isolation filtering."""
    if not env_overrides:
        return child_env
    merged = dict(os.environ) if child_env is None else dict(child_env)
    merged.update(env_overrides)
    return merged


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        mutation_replace(
            path.parent,
            path.name,
            data,
            expected=capture_target(path.parent, path.name),
        )
    except (MutationConflict, MutationUnsupported) as exc:
        raise OSError(str(exc)) from exc


def _append_event(path: Path, value: dict[str, Any]) -> None:
    append_jsonl_record(path, value)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _safe_path(
    value: object, *, root: Path, name: str, must_exist: bool = False
) -> Path:
    if not isinstance(value, str) or not value:
        raise InvocationError(f"{name} must be a non-empty path string")
    candidate = Path(value).expanduser().resolve(strict=False)
    if not _inside(candidate, root):
        raise InvocationError(f"{name} escapes its allowed root")
    if must_exist and not candidate.is_file():
        raise InvocationError(f"{name} is not an existing regular file")
    return candidate


def _canonical_output_path_key(path: Path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def _validate_canonical_output_paths(
    output_paths: Mapping[str, Path],
    *,
    workspace: Path,
) -> None:
    """Reject canonical output aliases before any workspace/output mutation."""

    path_keys = {
        key: _canonical_output_path_key(path) for key, path in output_paths.items()
    }
    if len(set(path_keys.values())) != len(path_keys):
        raise InvocationValidationError(
            "canonical output paths must be pairwise distinct"
        )

    reserved_paths = {
        _canonical_output_path_key(workspace / "RESULT.json"): "RESULT.json",
        _canonical_output_path_key(
            workspace / COMPLETION_REVIEW_FILENAME
        ): COMPLETION_REVIEW_FILENAME,
        _canonical_output_path_key(
            workspace / ORCHESTRATOR_ACCEPTANCE_FILENAME
        ): ORCHESTRATOR_ACCEPTANCE_FILENAME,
    }
    collisions = sorted(
        f"{key}={reserved_paths[path_key]}"
        for key, path_key in path_keys.items()
        if path_key in reserved_paths
    )
    if collisions:
        raise InvocationValidationError(
            "canonical output paths are reserved task artifacts: "
            + ", ".join(collisions)
        )


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
    raw: Mapping[str, Any],
    *,
    route: str,
    fields: frozenset[str],
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


def _reject_ambiguous_coding_aliases(raw: Mapping[str, Any]) -> None:
    for aliases, name in (
        (("codex", "codex_settings", "model_settings"), "provider settings"),
        (("repository", "git"), "repository"),
        (("event_log_path", "event_log", "lane_event_log"), "event log"),
        (("resume_identity", "resume"), "resume identity"),
    ):
        present = [alias for alias in aliases if alias in raw]
        if len(present) > 1:
            raise InvocationError(
                f"coding invocation contains ambiguous {name} aliases: {', '.join(present)}"
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
    canonical: CanonicalInvocation | None = None
    provider_id: str = "codex"
    provider_options: Mapping[str, Any] = field(default_factory=dict)
    prompt_bundle: PromptBundle | None = None
    runtime_profile: RuntimeProfile | None = None
    runtime_root: Path | None = None
    invocation_path: Path | None = None
    overlay_receipt: Path | None = None


def _common_paths(
    raw: dict[str, Any],
) -> tuple[str, Path, Path, Path, str, bytes, dict[str, Path]]:
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
    prompt_path = _safe_path(
        raw.get("prompt_path"), root=run_root, name="prompt_path", must_exist=True
    )
    outputs = raw.get("output_paths")
    if not isinstance(outputs, dict):
        raise InvocationError("output_paths must be an object")
    status_path = _safe_path(outputs.get("status"), root=workspace, name="status")
    jsonl_path = _safe_path(outputs.get("jsonl"), root=workspace, name="jsonl")
    stderr_path = _safe_path(outputs.get("stderr"), root=workspace, name="stderr")
    last_message_path = _safe_path(
        outputs.get("last_message"), root=workspace, name="last_message"
    )
    prompt_sha256 = _string(raw, "prompt_sha256").lower()
    if len(prompt_sha256) != 64 or any(
        char not in "0123456789abcdef" for char in prompt_sha256
    ):
        raise InvocationError("prompt_sha256 must be a SHA-256 hex digest")
    try:
        prompt_bytes = prompt_path.read_bytes()
    except OSError as exc:
        raise InvocationError(f"cannot verify prompt: {exc}") from exc
    if hashlib.sha256(prompt_bytes).hexdigest() != prompt_sha256:
        raise InvocationError("prompt bytes do not match prompt_sha256")
    return (
        action,
        run_root,
        workspace,
        prompt_path,
        prompt_sha256,
        prompt_bytes,
        {
            "status": status_path,
            "jsonl": jsonl_path,
            "stderr": stderr_path,
            "last_message": last_message_path,
        },
    )


def _resume_thread(raw: dict[str, Any]) -> str | None:
    requested_thread = raw.get("resume_thread_id")
    if requested_thread is not None and (
        not isinstance(requested_thread, str) or not requested_thread.strip()
    ):
        raise InvocationError(
            "resume_thread_id must be a non-empty string when supplied"
        )
    return requested_thread.strip() if isinstance(requested_thread, str) else None


def _load_firmware_invocation(raw: dict[str, Any]) -> Invocation:
    _reject_foreign_fields(
        raw, route="schema-less firmware", fields=_CODING_ONLY_FIELDS
    )
    _reject_firmware_coding_model_settings(raw)
    action, run_root, workspace, prompt_path, prompt_sha256, prompt_bytes, outputs = (
        _common_paths(raw)
    )
    label = _string(raw, "label")
    expected = {
        "status": f"{label}_controller.status.json",
        "jsonl": f"{label}_codex.jsonl",
    }
    if (
        outputs["status"].name != expected["status"]
        or outputs["jsonl"].name != expected["jsonl"]
    ):
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
    sidecar_path = (
        suite_root / ".agent-workspace" / "AUTONOMOUS_EXECUTION_POLICY.sha256"
    )
    policy_sha256 = _string(raw, "policy_sha256").lower()
    if len(policy_sha256) != 64 or any(
        char not in "0123456789abcdef" for char in policy_sha256
    ):
        raise InvocationError("policy_sha256 must be a SHA-256 hex digest")
    try:
        policy_bytes = policy_path.read_bytes()
        sidecar = sidecar_path.read_text(encoding="utf-8").split()[0].lower()
    except (OSError, IndexError) as exc:
        raise InvocationError(f"cannot verify policy-bound prompt: {exc}") from exc
    if (
        hashlib.sha256(policy_bytes).hexdigest() != policy_sha256
        or sidecar != policy_sha256
    ):
        raise InvocationError(
            "canonical policy file or sidecar does not match policy_sha256"
        )
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
    if (
        any(marker not in prompt_text for marker in markers)
        or policy_text not in prompt_text
    ):
        raise InvocationError(
            "prompt is not the required policy-bound prompt composition"
        )
    log_root = suite_root / "multi-agent-logs" / "orchestrator-harness"
    event_log = _safe_path(
        raw.get("lane_event_log"), root=log_root, name="lane_event_log"
    )
    if event_log.name != "LANE_EVENTS.jsonl":
        raise InvocationError("lane_event_log must be named LANE_EVENTS.jsonl")
    event_log.parent.mkdir(parents=True, exist_ok=True)
    return Invocation(
        None,
        None,
        action,
        run_root,
        workspace,
        prompt_path,
        prompt_sha256,
        prompt_bytes,
        policy_path,
        policy_sha256,
        label,
        _string(raw, "doer"),
        _string(raw, "task"),
        _string(raw, "phase"),
        _string(raw, "declared_lane_id"),
        _string_list(raw.get("leases", []), "leases"),
        _string_list(raw.get("board_tokens", []), "board_tokens"),
        _string_list(raw.get("mcp_servers", []), "mcp_servers"),
        server_snapshot,
        [],
        None,
        _string(model_settings, "model"),
        _string(model_settings, "reasoning_effort"),
        _string(model_settings, "service_tier"),
        command,
        overrides,
        "danger-full-access",
        "never",
        requested_thread,
        None,
        outputs["status"],
        outputs["jsonl"],
        outputs["stderr"],
        outputs["last_message"],
        event_log,
    )


def _coding_settings(
    raw: dict[str, Any],
) -> tuple[str, str, str, list[str], list[str], str, str]:
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
        raise InvocationError(
            "sandbox must be read-only, workspace-write, or danger-full-access"
        )
    approval_policy = _string(settings, "approval_policy")
    if approval_policy not in {"untrusted", "on-failure", "on-request", "never"}:
        raise InvocationError(
            "approval_policy is not a supported Codex approval policy"
        )
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
    # This must remain the first coding-v1 operation: the shared closed
    # contract rejects mixed/canonical/unknown fields before path resolution,
    # workspace creation, or event-log mutation.
    try:
        validate_coding_v1_fields(raw)
    except InvocationValidationError as exc:
        raise InvocationError(str(exc)) from exc
    _reject_foreign_fields(raw, route="coding", fields=_FIRMWARE_ONLY_FIELDS)
    _reject_ambiguous_coding_aliases(raw)
    action, run_root, workspace, prompt_path, prompt_sha256, prompt_bytes, outputs = (
        _common_paths(raw)
    )
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
    if _inside(runtime_root, run_root) or _inside(run_root, runtime_root):
        raise InvocationError("runtime_root must be separate from run_root")
    lock_value = raw.get(
        "resource_lock_root", str(runtime_root / "coding-resource-locks")
    )
    resource_lock_root = _safe_path(
        lock_value, root=runtime_root, name="resource_lock_root"
    )
    if resource_lock_root == runtime_root:
        raise InvocationError("resource_lock_root must be below runtime_root")
    event_value = raw.get(
        "event_log_path", raw.get("event_log", raw.get("lane_event_log"))
    )
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
        exclusive_resources
        if exclusive_resources is not None
        else legacy_resources or [],
        "exclusive_resources",
    )
    try:
        repository = declaration_from_invocation(raw, run_root)
    except GitSafetyError as exc:
        raise InvocationError(str(exc)) from exc
    model, reasoning, tier, command, overrides, sandbox, approval = _coding_settings(
        raw
    )
    requested_thread = _resume_thread(raw)
    resume_identity = raw.get("resume_identity", raw.get("resume"))
    if resume_identity is not None:
        if not isinstance(resume_identity, dict):
            raise InvocationError("resume_identity must be an object")
        identity_worker = resume_identity.get("worker_invocation_id")
        identity_thread = resume_identity.get("thread_id")
        if identity_worker is not None and identity_worker != worker_invocation_id:
            raise InvocationError("resume identity worker_invocation_id mismatch")
        if identity_thread is not None and (
            not isinstance(identity_thread, str) or not identity_thread.strip()
        ):
            raise InvocationError(
                "resume identity thread_id must be a non-empty string"
            )
        normalized_identity_thread = (
            identity_thread.strip() if isinstance(identity_thread, str) else None
        )
        if (
            requested_thread
            and normalized_identity_thread
            and requested_thread != normalized_identity_thread
        ):
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
        finding_path = _safe_path(
            finding_gate.get("path"), root=workspace, name="finding_gate.path"
        )
        if finding_path.parent != workspace or finding_path.name != "FINDINGS.json":
            raise InvocationError("finding_gate.path must be workspace/FINDINGS.json")
        normalized_gate = {"role": finding_gate["role"], "path": str(finding_path)}
    isolation = raw.get("child_environment_isolation", False)
    if not isinstance(isolation, bool):
        raise InvocationError("child_environment_isolation must be boolean")
    overlay_value = raw.get("overlay_receipt")
    overlay_receipt = None
    if overlay_value is not None:
        if not isinstance(overlay_value, str) or not overlay_value.strip():
            raise InvocationError("overlay_receipt must be a non-empty path string")
        overlay_receipt = _safe_path(
            overlay_value, root=workspace, name="overlay_receipt"
        )
    return Invocation(
        CODING_INVOCATION_SCHEMA,
        worker_invocation_id,
        action,
        run_root,
        workspace,
        prompt_path,
        prompt_sha256,
        prompt_bytes,
        None,
        None,
        label_value.strip(),
        doer_value.strip(),
        _string(raw, "task"),
        _string(raw, "phase"),
        lane_id,
        [],
        [],
        [],
        {},
        resources,
        resource_lock_root,
        model,
        reasoning,
        tier,
        command,
        overrides,
        sandbox,
        approval,
        requested_thread,
        repository,
        outputs["status"],
        outputs["jsonl"],
        outputs["stderr"],
        outputs["last_message"],
        event_log,
        normalized_gate,
        isolation,
        overlay_receipt=overlay_receipt,
    )


def _load_canonical_invocation(raw: dict[str, Any]) -> Invocation:
    """Load the strict provider-neutral route and its exact prompt bytes."""

    try:
        canonical = parse_canonical_invocation(raw)
        run_root = canonical.run_root.expanduser().resolve(strict=True)
        runtime_root = canonical.runtime_root.expanduser().resolve(strict=True)
        if not run_root.is_dir() or not runtime_root.is_dir():
            raise InvocationValidationError(
                "canonical run_root and runtime_root must be directories"
            )
        if _inside(runtime_root, run_root) or _inside(run_root, runtime_root):
            raise InvocationValidationError(
                "canonical runtime_root must be separate from run_root"
            )
        workspace = (run_root / ".agent-workspace").resolve(strict=False)
        if workspace.parent != run_root:
            raise InvocationValidationError(
                "canonical workspace must be a direct child of run_root"
            )
        if canonical.resume_admission_path is not None:
            review_path = canonical.resume_admission_path
            if not review_path.is_absolute():
                review_path = workspace / review_path
            _safe_path(review_path, root=workspace, name="resume_admission_path")

        def rooted(value: Path, base: Path) -> str:
            return str(value if value.is_absolute() else base / value)

        output_paths = {
            key: _safe_path(
                rooted(value, workspace), root=workspace, name=f"output_paths.{key}"
            )
            for key, value in canonical.output_paths.items()
        }
        _validate_canonical_output_paths(output_paths, workspace=workspace)
        if (
            output_paths["status"].parent != workspace
            or output_paths["status"].suffix != ".json"
        ):
            raise InvocationValidationError(
                "canonical status must be a direct *.json file under .agent-workspace"
            )
        event_log = _safe_path(
            rooted(canonical.event_log_path, runtime_root),
            root=runtime_root,
            name="event_log_path",
        )
        bundle = bundle_from_record(canonical.prompt_bundle, run_root=run_root)
        if not bundle.final_bytes:
            raise InvocationValidationError("canonical prompt bundle is empty")
        profile_record = dict(canonical.profile)
        profile_record.setdefault("schema", "orchestrator-runtime-profile/v1")
        profile = RuntimeProfile.from_mapping(profile_record)
        if tuple(canonical.resources) != profile.resources:
            raise InvocationValidationError(
                "invocation resources must match profile resources"
            )
        repository = None
        if canonical.repository is not None:
            try:
                repository = declaration_from_invocation(
                    {"repository": dict(canonical.repository)}, run_root
                )
            except GitSafetyError as exc:
                raise InvocationValidationError(str(exc)) from exc
        provider_options = dict(canonical.provider_options)
        command = provider_options.get(
            "command", provider_default_command(canonical.provider_id)
        )
        command_list = _string_list(command, "provider.command")
        overrides = _string_list(
            provider_options.get("config_overrides", []), "provider.config_overrides"
        )
        reasoning = provider_options.get("reasoning_effort", "medium")
        tier = provider_options.get("service_tier", "priority")
        permission_mode = provider_options.get("permission_mode")
        sandbox = provider_options.get("sandbox", "workspace-write")
        approval = provider_options.get("approval_policy", "never")
        if (
            not isinstance(reasoning, str)
            or not reasoning.strip()
            or not isinstance(tier, str)
            or not tier.strip()
        ):
            raise InvocationValidationError(
                "provider reasoning_effort and service_tier must be non-empty strings"
            )
        if not isinstance(sandbox, str) or sandbox not in {
            "read-only",
            "workspace-write",
            "danger-full-access",
        }:
            raise InvocationValidationError("provider sandbox is invalid")
        if not isinstance(approval, str) or approval not in {
            "untrusted",
            "on-failure",
            "on-request",
            "never",
        }:
            raise InvocationValidationError("provider approval_policy is invalid")
        _string_list(
            provider_options.get("allowed_tools", []), "provider.allowed_tools"
        )
        _string_list(
            provider_options.get("disallowed_tools", []), "provider.disallowed_tools"
        )
        if permission_mode is not None and (
            not isinstance(permission_mode, str) or not permission_mode.strip()
        ):
            raise InvocationValidationError(
                "provider.permission_mode must be a non-empty string"
            )
        component_path = bundle.components[0].path
        if component_path is None:
            raise InvocationValidationError(
                "canonical prompt components must be path-bound"
            )
        overlay_receipt = None
        if canonical.overlay_receipt is not None:
            overlay_receipt = _safe_path(
                str(canonical.overlay_receipt), root=workspace, name="overlay_receipt"
            )
        return Invocation(
            canonical.schema,
            canonical.worker_invocation_id,
            canonical.action,
            run_root,
            workspace,
            component_path,
            bundle.final_sha256,
            bundle.final_bytes,
            None,
            None,
            canonical.label,
            canonical.role,
            canonical.task,
            canonical.phase,
            canonical.lane_id,
            [],
            [],
            [],
            {},
            list(canonical.resources),
            runtime_root / "canonical-resource-locks",
            canonical.provider_model,
            str(reasoning).strip(),
            str(tier).strip(),
            command_list,
            overrides,
            sandbox,
            approval,
            canonical.requested_session_id,
            repository,
            output_paths["status"],
            output_paths["jsonl"],
            output_paths["stderr"],
            output_paths["last_message"],
            event_log,
            None,
            True,
            canonical,
            canonical.provider_id,
            provider_options,
            bundle,
            profile,
            overlay_receipt=overlay_receipt,
        )
    except (InvocationValidationError, PromptBundleError, ProfileError, OSError) as exc:
        raise InvocationError(str(exc)) from exc


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
    if schema == CANONICAL_INVOCATION_SCHEMA:
        invocation = _load_canonical_invocation(raw)
    elif schema == CODING_INVOCATION_SCHEMA:
        invocation = _load_coding_invocation(raw)
    elif schema is None:
        invocation = _load_firmware_invocation(raw)
    else:
        raise InvocationError(f"unsupported invocation schema: {schema}")
    object.__setattr__(
        invocation, "invocation_path", path.expanduser().resolve(strict=False)
    )
    runtime_value = raw.get("runtime_root")
    if isinstance(runtime_value, str) and runtime_value:
        object.__setattr__(
            invocation,
            "runtime_root",
            Path(runtime_value).expanduser().resolve(strict=False),
        )
    else:
        object.__setattr__(invocation, "runtime_root", invocation.run_root.parent)
    return invocation


def _identity(
    pid: int, *, parent: int | None = None, timeout: float = 5.0
) -> ProcessInfo:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = process_snapshot()
        item = snapshot.by_pid.get(pid) if snapshot.complete else None
        if (
            item is not None
            and item.created_utc is not None
            and (parent is None or item.ppid == parent)
        ):
            return item
        time.sleep(0.05)
    raise RuntimeError(f"cannot establish exact process identity for PID {pid}")


def _command_tail(command_line: str) -> str:
    """Return argv after the executable for launcher-chain comparison."""

    value = command_line.strip()
    if not value:
        return ""
    if value.startswith('"'):
        end = value.find('"', 1)
        return " ".join(value[end + 1 :].split()) if end > 0 else ""
    parts = value.split(None, 1)
    return " ".join(parts[1].split()) if len(parts) == 2 else ""


def _is_launcher_descendant(
    item: ProcessInfo,
    parent: ProcessInfo,
    *,
    provider_root_pid: int | None = None,
) -> bool:
    """Exclude an OS launcher re-exec without hiding a real helper root.

    Some Windows Python launch shims expose an empty command line for the
    first child below the Popen PID.  That one direct child is still part of
    the provider launch chain; later descendants are inventoried as helpers.
    """

    return (
        item.pid != parent.pid
        and item.ppid == parent.pid
        and (
            (
                bool(_command_tail(item.command_line))
                and _command_tail(item.command_line)
                == _command_tail(parent.command_line)
            )
            or (
                provider_root_pid is not None
                and parent.pid == provider_root_pid
                and not item.command_line.strip()
            )
        )
    )


def _shutdown_exact_child(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float = 5.0,
    identity: ProcessInfo | None = None,
    boundary: ProcessBoundary | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Compatibility shim around the shared exact supervisor."""
    supervisor = ProcessSupervisor(
        process,
        identity,
        graceful_timeout_seconds=timeout_seconds,
        force_timeout_seconds=timeout_seconds,
        observer=None,
        boundary=boundary,
    )
    result = supervisor.cleanup()
    evidence = result.to_record()
    if "GRACEFUL_WAIT_TIMEOUT" in result.stages:
        evidence["terminate_wait_timed_out"] = True
    if "FINAL_REAP_TIMEOUT" in result.stages:
        evidence["kill_wait_timed_out"] = True
    if result.errors:
        evidence["error"] = "; ".join(result.errors)
    return result.proved_reap, evidence


def _read_prior_status(invocation: Invocation) -> dict[str, Any] | None:
    if not invocation.status_path.is_file():
        return None
    try:
        value = json.loads(invocation.status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _read_canonical_prior_status(
    invocation: Invocation,
) -> tuple[Path, dict[str, Any]] | None:
    """Find the one persisted controller status owned by this canonical workspace."""

    workspace = invocation.workspace
    if not workspace.is_dir():
        return None
    candidates: list[tuple[Path, dict[str, Any]]] = []
    try:
        paths = sorted(workspace.glob("*.json"), key=lambda path: path.name)
    except OSError as exc:
        raise InvocationError(
            f"cannot inspect canonical workspace status: {exc}"
        ) from exc
    for path in paths:
        if path.name.startswith("."):
            continue
        if path == invocation.status_path:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise InvocationError(
                    f"canonical persisted status is invalid: {exc}"
                ) from exc
            if not isinstance(value, dict):
                raise InvocationError("canonical persisted status must be an object")
        else:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(value, dict):
                continue
        if value.get("schema") == "orchestrator-lane-controller/v1":
            candidates.append((path, value))
    if len(candidates) > 1:
        raise InvocationError(
            "canonical workspace contains multiple persisted controller statuses"
        )
    return candidates[0] if candidates else None


def _canonical_repository_identity(
    value: object, *, include_starting_commit: bool
) -> object:
    if not isinstance(value, Mapping):
        return value
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        key_text = str(key)
        if not include_starting_commit and key_text == "starting_commit":
            continue
        if key_text in {"common_dir", "worktree_root"} and isinstance(item, str):
            normalized[key_text] = os.path.normcase(os.path.abspath(item))
        elif key_text in {"base_commit", "starting_commit"} and isinstance(item, str):
            normalized[key_text] = item.lower()
        else:
            normalized[key_text] = item
    return normalized


def _canonical_prior_identity_check(
    invocation: Invocation,
    prior_status: Mapping[str, Any],
    *,
    prior_status_path: Path,
    thread: str | None,
    starting_commit: str | None,
    git_identity: Any,
    allowed_identity_fields: frozenset[str] = frozenset(),
) -> None:
    assert invocation.canonical is not None
    if prior_status_path != invocation.status_path:
        raise InvocationError(
            "canonical workspace already has a persisted status at a different path"
        )

    canonical = invocation.canonical
    expected_identity = canonical.identity(
        session_id=thread if invocation.action == "resume" else None,
        starting_commit=starting_commit if invocation.action == "resume" else None,
    )
    if git_identity is not None:
        expected_identity["repository"] = repository_status(
            git_identity,
            starting_commit=starting_commit if invocation.action == "resume" else None,
        )
    persisted_identity = prior_status.get("resume_identity")
    if not isinstance(persisted_identity, Mapping):
        raise InvocationError("canonical persisted status has no resume_identity")

    identity_fields = (
        "schema",
        "lane_id",
        "worker_invocation_id",
        "cohort_id",
        "workflow_id",
        "workflow_version",
        "task_card_id",
        "task_card_revision",
        "task_card_sha256",
        "provider_id",
        "provider_launch_sha256",
        "session_id",
        "repository",
        "prompt_bundle_sha256",
        "prompt_content_sha256",
        "resources",
    )
    for identity_field in identity_fields:
        if invocation.action == "start" and identity_field == "session_id":
            continue
        if identity_field not in persisted_identity or identity_field not in expected_identity:
            raise InvocationError(
                f"canonical persisted status identity is missing {identity_field}"
            )
        persisted_value = persisted_identity[identity_field]
        expected_value = expected_identity[identity_field]
        if identity_field == "repository":
            include_starting_commit = invocation.action == "resume"
            persisted_value = _canonical_repository_identity(
                persisted_value, include_starting_commit=include_starting_commit
            )
            expected_value = _canonical_repository_identity(
                expected_value, include_starting_commit=include_starting_commit
            )
        if persisted_value != expected_value and identity_field not in allowed_identity_fields:
            raise InvocationError(
                "canonical prior task identity does not match persisted status"
            )

    expected_status_identity = {
        "invocation_schema": canonical.schema,
        "worker_invocation_id": canonical.worker_invocation_id,
        "cohort_id": canonical.cohort_id,
        "workflow": {
            "id": canonical.workflow_id,
            "version": canonical.workflow_version,
        },
        "task_card": {
            "id": canonical.task_card_id,
            "revision": canonical.task_card_revision,
            "sha256": canonical.task_card_sha256,
        },
        "doer": canonical.role,
        "task": canonical.task,
        "phase": canonical.phase,
        "provider_id": canonical.provider_id,
        "prompt_bundle_sha256": canonical.prompt_bundle_sha256,
        "prompt_content_sha256": canonical.prompt_content_sha256,
        "resources": list(canonical.resources),
        "profile": invocation.runtime_profile.to_record()
        if invocation.runtime_profile is not None
        else None,
    }
    for status_field, expected in expected_status_identity.items():
        if status_field not in prior_status:
            raise InvocationError(
                "canonical prior task identity does not match persisted status"
            )
        if status_field == "task_card" and "task_card_sha256" in allowed_identity_fields:
            persisted_card = prior_status.get(status_field)
            if not isinstance(persisted_card, Mapping) or not isinstance(
                expected, Mapping
            ):
                raise InvocationError(
                    "canonical prior task identity does not match persisted status"
                )
            if any(
                persisted_card.get(key) != expected.get(key)
                for key in ("id", "revision")
            ):
                raise InvocationError(
                    "canonical prior task identity does not match persisted status"
                )
            continue
        if (
            status_field in {"prompt_bundle_sha256", "prompt_content_sha256"}
            and status_field in allowed_identity_fields
        ):
            continue
        if prior_status[status_field] != expected:
            raise InvocationError(
                "canonical prior task identity does not match persisted status"
            )
    if git_identity is not None:
        include_starting_commit = invocation.action == "resume"
        persisted_repository = _canonical_repository_identity(
            prior_status.get("repository"),
            include_starting_commit=include_starting_commit,
        )
        expected_repository = _canonical_repository_identity(
            expected_identity["repository"],
            include_starting_commit=include_starting_commit,
        )
        if persisted_repository != expected_repository:
            raise InvocationError(
                "canonical prior task repository identity does not match persisted status"
            )


def _canonical_acceptance_status(
    status: Mapping[str, Any],
    acceptance_identity: Mapping[str, Any],
) -> dict[str, Any]:
    updated_status = dict(status)
    updated_status["terminal_acceptance_state"] = "ACCEPTED"
    updated_status["task_advancement_state"] = "ACCEPTED"
    updated_status["acceptance_identity"] = dict(acceptance_identity)
    persisted_resume_identity = updated_status.get("resume_identity")
    if not isinstance(persisted_resume_identity, Mapping):
        raise InvocationError(
            "canonical accepted task requires persisted resume_identity"
        )
    accepted_resume_identity = dict(persisted_resume_identity)
    accepted_resume_identity["terminal_acceptance_state"] = "ACCEPTED"
    accepted_resume_identity["task_advancement_state"] = "ACCEPTED"
    accepted_resume_identity["acceptance_identity"] = dict(acceptance_identity)
    updated_status["resume_identity"] = accepted_resume_identity
    return updated_status


def _persist_canonical_acceptance(
    path: Path,
    status: Mapping[str, Any],
    acceptance_identity: Mapping[str, Any],
) -> dict[str, Any]:
    updated_status = _canonical_acceptance_status(status, acceptance_identity)
    _atomic_json(path, updated_status)
    return updated_status


def _canonical_prior_task_preflight(
    invocation: Invocation,
    prior_status: Mapping[str, Any] | None,
    *,
    prior_status_path: Path,
    thread: str | None,
    starting_commit: str | None,
    git_identity: Any,
    allowed_identity_fields: frozenset[str] = frozenset(),
) -> None:
    """Admit only a fresh canonical task or a validated continuation."""

    fixed_artifact_paths = (
        invocation.workspace / "RESULT.json",
        invocation.workspace / COMPLETION_REVIEW_FILENAME,
        invocation.workspace / ORCHESTRATOR_ACCEPTANCE_FILENAME,
    )
    fixed_artifacts_present = any(
        path.exists() or path.is_symlink() for path in fixed_artifact_paths
    )
    if prior_status is None and not fixed_artifacts_present:
        return
    if prior_status is not None:
        _canonical_prior_identity_check(
            invocation,
            prior_status,
            prior_status_path=prior_status_path,
            thread=thread,
            starting_commit=starting_commit,
            git_identity=git_identity,
            allowed_identity_fields=allowed_identity_fields,
        )
    result_validation, result_valid, task_result = _canonical_result_validation(
        invocation
    )
    if not result_valid:
        detail = result_validation.get("detail", "canonical task result is invalid")
        raise InvocationError(
            f"canonical {invocation.action} requires a valid task result: {detail}"
        )

    advancement = None
    if task_result is not None:
        try:
            advancement = read_task_advancement(
                invocation.workspace,
                card=task_result.card,
                result=task_result,
            )
        except TaskValidationError as exc:
            raise InvocationError(
                f"canonical {invocation.action} task advancement rejected: {exc}"
            ) from exc
    elif (invocation.workspace / COMPLETION_REVIEW_FILENAME).exists() or (
        invocation.workspace / ORCHESTRATOR_ACCEPTANCE_FILENAME
    ).exists():
        raise InvocationError(
            "canonical task advancement artifacts exist without a valid RESULT.json"
        )

    claimed_terminal = prior_status is not None and any(
        prior_status.get(field) == "ACCEPTED"
        for field in ("terminal_acceptance_state", "task_advancement_state")
    )
    persisted_resume_identity = (
        prior_status.get("resume_identity") if prior_status is not None else None
    )
    if isinstance(persisted_resume_identity, Mapping):
        claimed_terminal = claimed_terminal or any(
            persisted_resume_identity.get(field) == "ACCEPTED"
            for field in ("terminal_acceptance_state", "task_advancement_state")
        )
    if claimed_terminal and (advancement is None or advancement.state != "ACCEPTED"):
        raise InvocationError(
            "canonical persisted status claims acceptance without an accepted task advancement chain"
        )
    if advancement is not None and advancement.state == "ACCEPTED":
        acceptance_identity = advancement.acceptance_identity
        if acceptance_identity is None:
            raise InvocationError("canonical task acceptance is missing its identity")
        if prior_status is not None:
            _persist_canonical_acceptance(
                prior_status_path,
                prior_status,
                acceptance_identity,
            )
        raise InvocationError(
            f"canonical task is already accepted; {invocation.action} rejected before provider launch"
        )
    if invocation.action == "start":
        if prior_status is None:
            raise InvocationError(
                "canonical task has retained fixed artifacts; action:start rejected before provider launch"
            )
        raise InvocationError(
            "canonical task already has persisted work; action:start rejected; use action:resume"
        )


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


def _persisted_repository(
    invocation: Invocation, prior_status: Mapping[str, Any]
) -> str:
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
                if not isinstance(persisted, str) or Path(persisted).resolve(
                    strict=False
                ) != Path(value).resolve(strict=False):
                    raise InvocationError(
                        f"resume repository {key} does not match persisted status"
                    )
            except OSError as exc:
                raise InvocationError(
                    f"cannot compare persisted repository {key}: {exc}"
                ) from exc
        elif persisted != value:
            raise InvocationError(
                f"resume repository {key} does not match persisted status"
            )
    if nested.get("base_commit") != invocation.repository.base_commit:
        raise InvocationError("resume base_commit does not match persisted status")
    starting_commit = nested.get("starting_commit")
    if not isinstance(starting_commit, str) or not starting_commit:
        raise InvocationError("coding resume requires persisted starting_commit")
    return starting_commit


def _coding_result_validation(invocation: Invocation) -> tuple[dict[str, Any], bool]:
    assert (
        invocation.repository is not None
        and invocation.worker_invocation_id is not None
    )
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
                findings_path,
                lane_id=invocation.lane_id,
                worker_invocation_id=invocation.worker_invocation_id,
                role=invocation.finding_gate["role"],
                commit=identity.head_commit,
                outcome=value["outcome"],
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


def _canonical_result_validation(
    invocation: Invocation,
) -> tuple[dict[str, Any], bool, TaskResult | None]:
    """Validate canonical result shape while leaving semantic acceptance pending."""

    assert invocation.canonical is not None
    path = invocation.workspace / "RESULT.json"
    if not path.exists():
        return (
            {"state": "MISSING", "path": str(path), "acceptance_state": "PENDING"},
            True,
            None,
        )
    raw_bytes: bytes | None = None
    try:
        raw_bytes = path.read_bytes()
        if len(raw_bytes) > 1024 * 1024:
            raise TaskValidationError(
                "canonical task result exceeds the 1 MiB controller limit"
            )
        value = json.loads(raw_bytes.decode("utf-8-sig"))
        if not isinstance(value, Mapping):
            raise TaskValidationError("canonical task result root must be an object")
        card = task_card_from_identity(
            card_id=invocation.canonical.task_card_id,
            lane_id=invocation.canonical.lane_id,
            worker_invocation_id=invocation.canonical.worker_invocation_id,
            cohort_id=invocation.canonical.cohort_id,
            revision=invocation.canonical.task_card_revision,
            content_sha256=invocation.canonical.task_card_sha256,
        )
        if invocation.repository is not None:
            result = validate_task_result_repository(
                value,
                card=card,
                declaration=invocation.repository,
                raw_bytes=raw_bytes,
            )
        else:
            result = validate_task_result(value, card=card, raw_bytes=raw_bytes)
        if (
            value.get("prompt_bundle_sha256")
            != invocation.canonical.prompt_bundle_sha256
        ):
            raise TaskValidationError(
                "canonical task result prompt bundle identity does not match"
            )
        if (
            value.get("prompt_content_sha256")
            != invocation.canonical.prompt_content_sha256
        ):
            raise TaskValidationError(
                "canonical task result prompt content identity does not match"
            )
        return (
            {
                "state": "SHAPE_VALID",
                "path": str(path),
                "sha256": hashlib.sha256(raw_bytes).hexdigest(),
                "result_sha256": result.content_sha256,
                "prompt_bundle_sha256": invocation.canonical.prompt_bundle_sha256,
                "prompt_content_sha256": invocation.canonical.prompt_content_sha256,
                "commit": result.commit,
                "acceptance_state": "PENDING",
            },
            True,
            result,
        )
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TaskValidationError,
        GitSafetyError,
    ) as exc:
        return (
            invalid_result_evidence(
                path,
                str(exc),
                sha256=hashlib.sha256(raw_bytes).hexdigest()
                if raw_bytes is not None
                else None,
            ),
            False,
            None,
        )


def _provider_launch_spec(
    invocation: Invocation, session_id: str | None
) -> ProviderLaunchSpec:
    options = dict(invocation.provider_options)
    try:
        env_overrides: dict[str, str] = {}
        if invocation.provider_id == "claude-code":
            for override in invocation.config_overrides:
                env_overrides.update(claude_config_override_env(override))
        return ProviderLaunchSpec(
            action=invocation.action,
            command=tuple(invocation.codex_command),
            model=invocation.model,
            reasoning_effort=invocation.reasoning_effort,
            service_tier=invocation.service_tier,
            session_id=session_id,
            run_root=invocation.run_root,
            last_message_path=invocation.last_message_path,
            config_overrides=tuple(invocation.config_overrides),
            sandbox=invocation.sandbox,
            approval_policy=invocation.approval_policy,
            worker_invocation_id=invocation.worker_invocation_id,
            permission_mode=options.get("permission_mode")
            if isinstance(options.get("permission_mode"), str)
            else None,
            allowed_tools=tuple(
                item
                for item in options.get("allowed_tools", [])
                if isinstance(item, str)
            ),
            disallowed_tools=tuple(
                item
                for item in options.get("disallowed_tools", [])
                if isinstance(item, str)
            ),
            mcp_config=(
                options.get("mcp_config")
                if isinstance(options.get("mcp_config"), (str, Mapping, list))
                else None
            ),
            provider_options=options,
            env_overrides=env_overrides,
            prepared_worktree=invocation.overlay_receipt is not None,
        )
    except (TypeError, ValueError) as exc:
        raise InvocationError(f"provider launch settings are invalid: {exc}") from exc


def _prepared_overlay_role(invocation: Invocation) -> str:
    """Map a controller lane to the one cache receipt role it owns."""

    return invocation.doer if invocation.doer in {"orchestrator", "subagent"} else "subagent"


def _verify_prepared_overlay(invocation: Invocation) -> dict[str, Any]:
    """Require a completed overlay receipt before any provider launch work."""

    if invocation.overlay_receipt is None:
        raise InvocationError("a completed overlay receipt is required before provider launch")
    verification = verify_overlay_receipt(
        receipt_path=invocation.overlay_receipt,
        expected_target_worktree_id=invocation.run_root,
        role=_prepared_overlay_role(invocation),
    )
    if not verification.get("verified"):
        raise InvocationError(
            "overlay receipt is not completed for this prepared worktree: "
            + str(verification.get("reason"))
        )
    return verification


def _run_prepared_stop_hook(invocation: Invocation, *, boundary: str) -> dict[str, Any]:
    """Run the cache-provided finite verifier and reject its blocking result."""

    script = invocation.run_root / ".agent" / "stop-verify.ps1"
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not script.is_file() or shell is None:
        raise InvocationError("prepared worktree is missing its executable Stop verifier")
    environment = os.environ.copy()
    environment["AGENT_STOP_GATE_ENABLED"] = "1"
    environment["AGENT_STOP_GATE_BOUNDARY"] = boundary
    try:
        completed = subprocess.run(
            [shell, "-NoProfile", "-NonInteractive", "-File", str(script)],
            cwd=invocation.run_root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise InvocationError(f"prepared Stop verifier could not run: {exc}") from exc
    output = completed.stdout.strip().splitlines()
    try:
        result = json.loads(output[-1]) if output else None
    except json.JSONDecodeError as exc:
        raise InvocationError("prepared Stop verifier did not emit JSON") from exc
    if completed.returncode != 0 or not isinstance(result, dict):
        raise InvocationError("prepared Stop verifier failed")
    if result.get("continue") is not True:
        raise InvocationError(str(result.get("reason") or "prepared Stop verifier blocked"))
    return result


def _requested_provider_operations(
    invocation: Invocation, spec: ProviderLaunchSpec
) -> list[str]:
    """The concrete operations this controller requests from the selected adapter.

    Permission and configuration are requested whenever the controller
    supplies their launch semantics (sandbox/approval and
    model/reasoning/tier/configuration), even when optional override lists
    are empty.  Classification therefore precedes any adapter work.
    """
    operations = [
        "launch",
        "prompt",
        "event_result",
        "session",
        "permission",
        "configuration",
    ]
    if invocation.action == "resume":
        operations.append("resume")
    if spec.provider_options.get("notification") is True:
        operations.append("notification")
    return operations


def _classify_provider_operations(
    provider_id: str, operations: list[str]
) -> list[dict[str, Any]]:
    """Classify every requested operation before any provider work is fabricated."""
    results: list[dict[str, Any]] = []
    for operation in operations:
        if operation not in PROVIDER_OPERATION_NAMES:
            results.append(
                unsupported_operation_result(
                    provider_id,
                    operation,
                    "operation is not part of the provider contract",
                ).as_record()
            )
            continue
        results.append(classify_operation(provider_id, operation).as_record())
    return results


def _provider_handoff_identity(invocation: Invocation) -> tuple[str, str]:
    """The preserved workflow role and logical task identity for a handoff."""
    role = (
        invocation.doer
        or (invocation.canonical.role if invocation.canonical is not None else None)
        or "coder-main"
    )
    logical_task_id = (
        invocation.canonical.task_card_id
        if invocation.canonical is not None and invocation.canonical.task_card_id
        else invocation.lane_id
    )
    return role, logical_task_id


def _canonical_resume_admission_path(invocation: Invocation) -> Path:
    """Return the one confined ROOT review path for a canonical continuation."""
    assert invocation.canonical is not None
    candidate = invocation.canonical.resume_admission_path
    path = candidate if candidate is not None else Path("RESUME_ADMISSION.json")
    if not path.is_absolute():
        path = invocation.workspace / path
    path = path.expanduser().resolve(strict=False)
    if not _inside(path, invocation.workspace):
        raise InvocationError("resume amendment review escapes the canonical workspace")
    return path


def _read_canonical_resume_amendment(
    invocation: Invocation,
) -> Mapping[str, Any] | None:
    """Read the optional confined review; missing means an unreviewed pair."""
    path = _canonical_resume_admission_path(invocation)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise InvocationError("RESUME_ADMISSION.json is not a regular file")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except InvocationError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvocationError(f"cannot read RESUME_ADMISSION.json: {exc}") from exc
    if not isinstance(value, Mapping):
        raise InvocationError("RESUME_ADMISSION.json must contain an object")
    return value


def _canonical_amendment_job_identity(
    invocation: Invocation,
    requested_identity: Mapping[str, Any],
    persisted_identity: Mapping[str, Any],
) -> dict[str, Any]:
    requested_repository = requested_identity.get("repository")
    persisted_repository = persisted_identity.get("repository")
    repository = (
        requested_repository if isinstance(requested_repository, Mapping) else {}
    )
    prior_repository = (
        persisted_repository if isinstance(persisted_repository, Mapping) else {}
    )
    assert invocation.canonical is not None
    expected = {
        "card_id": requested_identity.get("task_card_id"),
        "stage_cohort_id": requested_identity.get("cohort_id"),
        "worker_invocation_id": requested_identity.get("worker_invocation_id"),
        "lane_id": requested_identity.get("lane_id"),
        "provider_session_id": requested_identity.get("session_id"),
        "exclusive_resources": list(invocation.canonical.resources),
    }
    for status_key, value in (
        ("repository_common_dir", repository.get("common_dir")),
        ("worktree_root", repository.get("worktree_root")),
        ("branch", repository.get("branch")),
        ("original_base_commit", repository.get("base_commit")),
        ("continuation_start_commit", prior_repository.get("starting_commit")),
    ):
        if value is not None:
            expected[status_key] = value
    return expected


def _canonical_resume_claims_accepted(
    invocation: Invocation, prior_status: Mapping[str, Any]
) -> bool:
    """Inspect acceptance artifacts without publishing or changing status."""
    if any(
        prior_status.get(field) == "ACCEPTED"
        for field in ("terminal_acceptance_state", "task_advancement_state")
    ):
        return True
    persisted = prior_status.get("resume_identity")
    if isinstance(persisted, Mapping) and any(
        persisted.get(field) == "ACCEPTED"
        for field in ("terminal_acceptance_state", "task_advancement_state")
    ):
        return True
    result_evidence, result_valid, task_result = _canonical_result_validation(
        invocation
    )
    del result_evidence
    if not result_valid or task_result is None:
        return False
    try:
        advancement = read_task_advancement(
            invocation.workspace,
            card=task_result.card,
            result=task_result,
        )
    except TaskValidationError:
        return False
    return advancement.state == "ACCEPTED"


def run(invocation: Invocation) -> int:
    prompt = invocation.prompt_bytes
    if not prompt:
        raise InvocationError("prompt is empty")
    prior_status_path = invocation.status_path
    if invocation.canonical is not None:
        canonical_prior = _read_canonical_prior_status(invocation)
        prior_status = canonical_prior[1] if canonical_prior is not None else None
        if canonical_prior is not None:
            prior_status_path = canonical_prior[0]
    else:
        prior_status = _read_prior_status(invocation)
    prior_thread_value = prior_status.get("thread_id") if prior_status else None
    prior_thread = (
        prior_thread_value
        if isinstance(prior_thread_value, str) and prior_thread_value
        else None
    )
    starting_commit = None
    git_identity = None
    if invocation.repository is not None:
        try:
            git_identity = inspect_repository(invocation.repository)
        except GitSafetyError as exc:
            raise InvocationError(str(exc)) from exc
    resume_admission_record: dict[str, Any] | None = None
    provider_resume_handoff: ProviderHandoff | None = None
    provider_resume_decision: ProviderResumeDecision | None = None

    def _resume_handoff(
        reason: str,
        *,
        requested_session_id: str | None,
        persisted_session_id: str | None,
        identity_mismatch: bool = False,
    ) -> ProviderHandoff:
        """REQ-O35: one declared same-role handoff with no fabricated continuity."""
        role, logical_task_id = _provider_handoff_identity(invocation)
        decision = decide_resume_or_handoff(
            invocation.provider_id,
            role=role,
            logical_task_id=logical_task_id,
            worker_invocation_id=invocation.worker_invocation_id,
            requested_session_id=requested_session_id,
            persisted_session_id=persisted_session_id,
            identity_mismatch=identity_mismatch,
        )
        handoff = decision.handoff
        if handoff is None:
            handoff = structured_handoff(
                role=role,
                logical_task_id=logical_task_id,
                worker_invocation_id=invocation.worker_invocation_id,
                provider_id=invocation.provider_id,
                prior_session_id=persisted_session_id,
                requested_session_id=requested_session_id,
                reason=reason,
            )
        elif identity_mismatch:
            handoff = replace(handoff, reason=reason)
        return handoff

    if invocation.action == "resume":
        if invocation.canonical is not None:
            if prior_status is None:
                raise InvocationError(
                    "canonical resume requires persisted controller status"
                )
            canonical_resume_mismatch: str | None = None
            if (
                prior_status.get("worker_invocation_id")
                != invocation.worker_invocation_id
            ):
                canonical_resume_mismatch = (
                    "resume worker_invocation_id does not match persisted status"
                )
            elif prior_status.get("invocation_schema") != invocation.invocation_schema:
                canonical_resume_mismatch = (
                    "resume invocation schema does not match persisted status"
                )
            if prior_thread is None:
                prior_thread_value = prior_status.get(
                    "provider_session_id", prior_status.get("session_id")
                )
                prior_thread = (
                    prior_thread_value
                    if isinstance(prior_thread_value, str) and prior_thread_value
                    else None
                )
            if prior_thread is None:
                raise InvocationError(
                    "canonical resume requires a persisted provider session ID"
                )
            requested_session_id = (
                invocation.canonical.requested_session_id
                if invocation.canonical is not None
                else None
            )
            if canonical_resume_mismatch is None and (
                requested_session_id is not None
                and requested_session_id != prior_thread
            ):
                canonical_resume_mismatch = "resume requested session does not match the persisted provider session"
            if canonical_resume_mismatch is None and (
                invocation.requested_thread_id is not None
                and invocation.requested_thread_id != prior_thread
            ):
                canonical_resume_mismatch = (
                    "resume requested thread does not match the persisted lane thread"
                )
            if canonical_resume_mismatch is not None:
                thread = invocation.requested_thread_id or prior_thread
                provider_resume_handoff = _resume_handoff(
                    canonical_resume_mismatch,
                    requested_session_id=(
                        requested_session_id
                        or invocation.requested_thread_id
                        or prior_thread
                    ),
                    persisted_session_id=prior_thread,
                    identity_mismatch=True,
                )
                provider_resume_decision = ProviderResumeDecision(
                    "HANDOFF", canonical_resume_mismatch, provider_resume_handoff
                )
            else:
                starting_commit = (
                    _persisted_repository(invocation, prior_status)
                    if invocation.repository is not None
                    else None
                )
                thread = invocation.requested_thread_id or prior_thread
                if not thread:
                    raise InvocationError(
                        "resume requires the persisted provider session ID"
                    )
                requested_identity = invocation.canonical.identity(
                    session_id=thread,
                    starting_commit=starting_commit,
                )
                requested_identity["live_identity"] = {
                    "provider_id": invocation.provider_id,
                    "session_id": thread,
                }
                persisted_identity = prior_status.get("resume_identity")
                if not isinstance(persisted_identity, Mapping):
                    raise InvocationError(
                        "canonical resume requires persisted resume_identity"
                    )
                raw_identity_fields = frozenset(
                    {
                        "task_card_sha256",
                        "prompt_bundle_sha256",
                        "prompt_content_sha256",
                    }
                )
                raw_identity_changed = any(
                    requested_identity.get(field) != persisted_identity.get(field)
                    for field in raw_identity_fields
                )
                if not raw_identity_changed:
                    _canonical_prior_identity_check(
                        invocation,
                        prior_status,
                        prior_status_path=prior_status_path,
                        thread=thread,
                        starting_commit=starting_commit,
                        git_identity=git_identity,
                    )
                if _canonical_resume_claims_accepted(invocation, prior_status):
                    raise InvocationError(
                        "canonical accepted task cannot be resumed or amended before provider launch"
                    )
                amendment_review = _read_canonical_resume_amendment(invocation)
                live_identity = {
                    "live_identity": {
                        "provider_id": invocation.provider_id,
                        "session_id": thread,
                    },
                    "repository": (
                        repository_status(git_identity, starting_commit=starting_commit)
                        if git_identity is not None
                        else requested_identity.get("repository")
                    ),
                }
                try:
                    admission = require_resume_admission(
                        requested_identity,
                        persisted_identity,
                        live_identity,
                        amendment_review=amendment_review,
                        expected_job_identity=_canonical_amendment_job_identity(
                            invocation, requested_identity, persisted_identity
                        ),
                    )
                except ResumeAdmissionError as exc:
                    raise InvocationError(str(exc)) from exc
                _canonical_prior_task_preflight(
                    invocation,
                    prior_status,
                    prior_status_path=prior_status_path,
                    thread=thread,
                    starting_commit=starting_commit,
                    git_identity=git_identity,
                    allowed_identity_fields=(
                        raw_identity_fields if raw_identity_changed else frozenset()
                    ),
                )
                resume_admission_record = admission.to_record()
        elif invocation.worker_invocation_id is not None:
            if prior_status is None:
                raise InvocationError(
                    "coding resume requires persisted controller status"
                )
            coding_resume_mismatch: str | None = None
            if (
                prior_status.get("worker_invocation_id")
                != invocation.worker_invocation_id
            ):
                coding_resume_mismatch = (
                    "resume worker_invocation_id does not match persisted status"
                )
            elif prior_status.get("invocation_schema") != invocation.invocation_schema:
                coding_resume_mismatch = (
                    "resume invocation schema does not match persisted status"
                )
            elif prior_thread is None:
                raise InvocationError(
                    "coding resume requires a persisted lane thread ID"
                )
            else:
                try:
                    starting_commit = _persisted_repository(invocation, prior_status)
                except InvocationError as exc:
                    coding_resume_mismatch = str(exc)
            if coding_resume_mismatch is None:
                thread = invocation.requested_thread_id or prior_thread
                if not thread or (
                    prior_thread
                    and invocation.requested_thread_id
                    and prior_thread != invocation.requested_thread_id
                ):
                    coding_resume_mismatch = (
                        "resume requires the persisted lane thread ID"
                    )
            if coding_resume_mismatch is not None:
                thread = invocation.requested_thread_id or prior_thread
                provider_resume_handoff = _resume_handoff(
                    coding_resume_mismatch,
                    requested_session_id=thread,
                    persisted_session_id=prior_thread,
                    identity_mismatch=True,
                )
                provider_resume_decision = ProviderResumeDecision(
                    "HANDOFF", coding_resume_mismatch, provider_resume_handoff
                )
        else:
            thread = invocation.requested_thread_id or prior_thread
            if not thread:
                raise InvocationError("resume requires the persisted lane thread ID")
    else:
        thread = None
        if git_identity is not None:
            starting_commit = git_identity.head_commit
        if invocation.canonical is not None:
            _canonical_prior_task_preflight(
                invocation,
                prior_status,
                prior_status_path=prior_status_path,
                thread=thread,
                starting_commit=starting_commit,
                git_identity=git_identity,
            )
    if (
        invocation.action == "resume"
        and provider_resume_handoff is None
        and thread is not None
    ):
        # REQ-O35: run the real adapter resume decision before any adapter
        # construction.  An adapter that does not declare resume (or is not
        # registered) preserves the logical task, workflow role, and state in
        # a declared same-role handoff with fabricated_continuity=false and
        # zero adapter construction or launch.  Identity mismatches were
        # already decided above; this path covers the exact persisted identity
        # with an unsupported resume capability.
        role, logical_task_id = _provider_handoff_identity(invocation)
        resume_decision = decide_resume_or_handoff(
            invocation.provider_id,
            role=role,
            logical_task_id=logical_task_id,
            worker_invocation_id=invocation.worker_invocation_id,
            requested_session_id=thread,
            persisted_session_id=prior_thread,
        )
        if resume_decision.mode == "HANDOFF":
            provider_resume_handoff = resume_decision.handoff
            provider_resume_decision = resume_decision
    if invocation.repository is not None:
        try:
            conflicts = active_declaration_conflicts(
                invocation.repository,
                current_status_path=invocation.status_path,
            )
        except GitSafetyError as exc:
            raise InvocationError(str(exc)) from exc
        if conflicts:
            raise InvocationError(
                "duplicate ACTIVE coding declaration: " + "; ".join(conflicts)
            )
        try:
            launch_identity = inspect_repository(invocation.repository)
        except GitSafetyError as exc:
            raise InvocationError(str(exc)) from exc
        if (
            git_identity is None
            or launch_identity.head_commit != git_identity.head_commit
        ):
            raise InvocationError(
                "coding worktree HEAD changed during pre-launch validation"
            )
    overlay_verification = _verify_prepared_overlay(invocation)
    prepared_stop_baseline = _run_prepared_stop_hook(invocation, boundary="baseline")
    controller = _identity(os.getpid())
    if provider_resume_handoff is not None:
        # A declared handoff never fabricates provider work: no adapter
        # selection, no argv construction, no launch.
        adapter = None
        launch_spec = None
        argv = None
        provider_operation_results: list[dict[str, Any]] = []
        unsupported_operations: list[dict[str, Any]] = []
        provider_evidence = None
    else:
        # REQ-O33: construct only the provider-neutral launch specification
        # before enforcement.  Every requested operation is classified before
        # adapter selection, adapter.build_argv, or any other provider work.
        launch_spec = _provider_launch_spec(invocation, thread)
        requested_operations = _requested_provider_operations(invocation, launch_spec)
        provider_operation_results = _classify_provider_operations(
            invocation.provider_id, requested_operations
        )
        unsupported_operations = [
            result
            for result in provider_operation_results
            if not result.get("supported")
        ]
        if unsupported_operations:
            # An unsupported requested operation returns a closed actionable
            # result with zero adapter construction and zero provider launch.
            adapter = None
            argv = None
            provider_evidence = None
        else:
            try:
                adapter = provider_adapter(invocation.provider_id)
                argv = adapter.build_argv(launch_spec)
            except ProviderAdapterError as exc:
                raise InvocationError(str(exc)) from exc
            provider_evidence = build_provider_evidence(
                invocation.provider_id,
                spec=launch_spec,
                argv=argv,
                worker_invocation_id=(
                    invocation.worker_invocation_id
                    or invocation.label
                    or "unidentified-worker"
                ),
                session_id=thread,
            )
    if invocation.canonical is not None:
        try:
            invocation.workspace.mkdir(exist_ok=True)
            invocation.event_log.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise InvocationError(
                f"cannot create canonical launch parents: {exc}"
            ) from exc
    legacy_status_names = invocation.canonical is None
    running_state = "RUNNING_CODEX" if legacy_status_names else "RUNNING_PROVIDER"
    exited_state = "CODEX_EXITED" if legacy_status_names else "PROVIDER_EXITED"
    started_event = "CODEX_STARTED" if legacy_status_names else "PROVIDER_STARTED"
    exited_event = "CODEX_EXITED" if legacy_status_names else "PROVIDER_EXITED"
    state: dict[str, Any] = {
        "schema": "orchestrator-lane-controller/v1",
        "state": "LAUNCH_FAILED",
        "started_utc": _utc(),
        "invocation_schema": invocation.invocation_schema,
        "worker_invocation_id": invocation.worker_invocation_id,
        "controller_pid": controller.pid,
        "controller_started_utc": iso_utc(controller.created_utc),
        "controller_created_utc": iso_utc(controller.created_utc),
        "provider_id": invocation.provider_id,
        "provider_pid": None,
        "provider_started_utc": None,
        "provider_created_utc": None,
        "provider_session_id": thread,
        "cohort_id": invocation.canonical.cohort_id
        if invocation.canonical is not None
        else None,
        "workflow": (
            {
                "id": invocation.canonical.workflow_id,
                "version": invocation.canonical.workflow_version,
            }
            if invocation.canonical is not None
            else None
        ),
        "task_card": (
            {
                "id": invocation.canonical.task_card_id,
                "revision": invocation.canonical.task_card_revision,
                "sha256": invocation.canonical.task_card_sha256,
            }
            if invocation.canonical is not None
            else None
        ),
        "completion_review_owner": "ROOT-IM"
        if invocation.canonical is not None
        else None,
        "codex_pid": None,
        "codex_started_utc": None,
        "doer": invocation.doer,
        "task": invocation.task,
        "phase": invocation.phase,
        "declared_lane_id": invocation.lane_id,
        "lane_id": invocation.lane_id,
        "thread_id": thread,
        "session_id": thread,
        "leases": invocation.leases,
        "board_tokens": invocation.board_tokens,
        "mcp_servers": invocation.mcp_servers,
        "server_snapshot": invocation.server_snapshot,
        "resources": invocation.resources,
        "exclusive_resources": invocation.resources,
        "resource_lock_root": str(invocation.resource_lock_root)
        if invocation.resource_lock_root
        else None,
        "held_resource_claims": [],
        "waiting_resource_claim": None,
        "resource_claim_findings": [],
        "overlay_receipt": str(invocation.overlay_receipt),
        "overlay_receipt_verified": bool(overlay_verification.get("verified")),
        "prepared_stop": {"baseline": prepared_stop_baseline},
        "jsonl_path": str(invocation.jsonl_path),
        "stderr_path": str(invocation.stderr_path),
        "last_message_path": str(invocation.last_message_path),
        "prompt_path": str(invocation.prompt_path),
        "prompt_sha256": invocation.prompt_sha256,
        "prompt_content_sha256": invocation.prompt_sha256,
        "prompt_bundle": invocation.prompt_bundle.to_record()
        if invocation.prompt_bundle is not None
        else None,
        "prompt_bundle_sha256": invocation.prompt_bundle.bundle_sha256
        if invocation.prompt_bundle is not None
        else None,
        "policy_path": str(invocation.policy_path)
        if invocation.policy_path is not None
        else None,
        "policy_sha256": invocation.policy_sha256,
        "resume_identity": invocation.canonical.identity(
            session_id=thread, starting_commit=starting_commit
        )
        if invocation.canonical is not None
        else None,
        "resume_admission": resume_admission_record,
        "provider_evidence": (
            provider_evidence.as_record() if provider_evidence is not None else None
        ),
        "provider_operation_results": provider_operation_results,
        "provider_handoff": (
            provider_resume_handoff.as_record()
            if provider_resume_handoff is not None
            else None
        ),
        "provider_resume_decision": (
            provider_resume_decision.as_record()
            if provider_resume_decision is not None
            else None
        ),
        "terminal_acceptance_state": "PENDING"
        if invocation.canonical is not None
        else None,
        "profile": invocation.runtime_profile.to_record()
        if invocation.runtime_profile is not None
        else None,
        "launcher_settings": {
            "model": invocation.model,
            "model_reasoning_effort": invocation.reasoning_effort,
            "service_tier": invocation.service_tier,
            "sandbox": invocation.sandbox,
            "approval_policy": invocation.approval_policy,
            "approvals_reviewer": "user"
            if invocation.worker_invocation_id is None
            else None,
            "configuration_digest": (
                provider_evidence.configuration_digest
                if provider_evidence is not None
                else None
            ),
            "jsonl": True,
            "ephemeral": False,
            "action": invocation.action,
            "provider_id": invocation.provider_id,
            "argv": list(provider_evidence.command_provenance)
            if provider_evidence is not None
            else [],
        },
    }
    registry_generation = uuid.uuid4().hex
    state["lifecycle_registry_generation"] = registry_generation
    state["owned_helpers"] = []
    state["process_boundary"] = None
    state["helpers_complete"] = False
    state["direct_child_reaped"] = False
    state["resource_claim_release_safe"] = False
    admission: _LifecycleAdmission | None = None
    boundary: ProcessBoundary | None = None
    if git_identity is not None:
        repository = repository_status(git_identity, starting_commit=starting_commit)
        expected_head = str(
            repository.get("actual_head") or git_identity.head_commit
        ).lower()
        branch_name = str(repository.get("branch") or git_identity.branch)
        retained_ref = f"refs/heads/{branch_name}"
        state.update(
            {
                "repository": repository,
                "repository_common_dir": repository["common_dir"],
                "worktree_root": repository["worktree_root"],
                "branch": repository["branch"],
                "base_commit": repository["base_commit"],
                "starting_commit": repository["starting_commit"],
                "lifecycle_retained_ref": retained_ref,
                "lifecycle_target_revision": expected_head,
            }
        )
        state["repository"]["expected_head"] = expected_head
        state["repository"]["starting_head"] = str(
            repository.get("starting_head")
            or repository.get("starting_commit")
            or expected_head
        ).lower()
        state["lifecycle_registry_path"] = "canonical-common-git-coordinate"
        if invocation.canonical is not None:
            resume_identity = invocation.canonical.identity(
                session_id=thread,
                starting_commit=repository["starting_commit"],
            )
            resume_identity["repository"] = repository
            resume_identity["live_identity"] = {
                "provider_id": invocation.provider_id,
                "session_id": thread,
            }
            state["resume_identity"] = resume_identity

    def _publish_registry() -> None:
        nonlocal admission, registry_generation
        if invocation.repository is None:
            return
        if invocation.invocation_path is None:
            raise InvocationError(
                "controller lifecycle registry requires its source invocation path"
            )
        worker_identity = None
        provider_pid = state.get("provider_pid")
        provider_created = state.get("provider_created_utc")
        if isinstance(provider_pid, int) and isinstance(provider_created, str):
            worker_identity = {"pid": provider_pid, "created_utc": provider_created}
        if admission is None:
            admission = _admit_lifecycle_registry(
                invocation.run_root,
                lane_id=invocation.lane_id,
                run_root=invocation.run_root,
                invocation_path=invocation.invocation_path,
                status_path=invocation.status_path,
                invocation_schema=invocation.invocation_schema,
                worker_invocation_id=invocation.worker_invocation_id or "",
                generation=registry_generation,
                state=str(state.get("state") or "LAUNCH_FAILED"),
                repository=state["repository"],
                controller=controller,
                worker=worker_identity,
                helpers=state.get("owned_helpers", []),
                boundary=state.get("process_boundary"),
                retained_ref=state.get("lifecycle_retained_ref"),
                target_revision=state.get("lifecycle_target_revision"),
                resume=invocation.action == "resume",
            )
            # A resume is admitted from the fixed owner before its resumed
            # status is published.  The owner generation is authoritative for
            # every subsequent status, update, terminal record, and retire.
            registry_generation = admission.generation
            state["lifecycle_registry_generation"] = registry_generation
            state["resume_admission"] = admission.record
        else:
            admission = _update_lifecycle_registry(
                admission,
                lane_id=invocation.lane_id,
                run_root=invocation.run_root,
                invocation_path=invocation.invocation_path,
                status_path=invocation.status_path,
                invocation_schema=invocation.invocation_schema,
                worker_invocation_id=invocation.worker_invocation_id or "",
                generation=admission.generation,
                state=str(state.get("state") or "LAUNCH_FAILED"),
                repository=state["repository"],
                controller=controller,
                worker=worker_identity,
                helpers=state.get("owned_helpers", []),
                boundary=state.get("process_boundary"),
                retained_ref=state.get("lifecycle_retained_ref"),
                target_revision=state.get("lifecycle_target_revision"),
            )

    if provider_resume_handoff is not None:
        # REQ-O35: an unsupported or identity-mismatched resume preserves the
        # logical task, workflow role, and state in a declared same-role
        # structured handoff with fabricated_continuity=false.  No provider is
        # launched and no successor is scheduled by this controller.
        state.update(
            {
                "state": "PROVIDER_HANDOFF",
                "ended_utc": _utc(),
                "error": provider_resume_handoff.reason,
            }
        )
        _atomic_json(invocation.status_path, state)
        _append_event(
            invocation.event_log,
            _event(
                invocation,
                "PROVIDER_HANDOFF",
                provider_id=invocation.provider_id,
                reason=provider_resume_handoff.reason,
                thread_id=state.get("thread_id"),
                session_id=state.get("provider_session_id"),
            ),
        )
        return 1
    if unsupported_operations:
        # REQ-O33: an unsupported requested operation returns an actionable
        # classified result before any provider work is fabricated.
        state.update(
            {
                "state": "PROVIDER_OPERATION_UNSUPPORTED",
                "ended_utc": _utc(),
                "error": "selected provider adapter does not support a requested operation",
            }
        )
        _atomic_json(invocation.status_path, state)
        _append_event(
            invocation.event_log,
            _event(
                invocation,
                "LAUNCH_FAILED",
                provider_id=invocation.provider_id,
                error="unsupported provider operation",
            ),
        )
        return 1
    assert adapter is not None and argv is not None

    if invocation.repository is not None:
        # Admission must precede both initial and resumed status publication.
        # This makes the fixed owner the first durable lifecycle authority and
        # prevents a resumed status from advertising a fresh generation.
        _publish_registry()
        _atomic_json(invocation.status_path, state)
    assert adapter is not None and argv is not None
    lock = threading.Lock()
    process: subprocess.Popen[bytes] | None = None
    child: ProcessInfo | None = None
    supervisor: ProcessSupervisor | None = None
    resource_claims: ResourceClaims | None = None
    direct_child_reaped = False
    release_safe = False
    cleanup_result: CleanupResult | None = None
    cleanup_attempted = False
    direct_handle_cleanup_attempted = False
    final_boundary_recorded = False

    def _incomplete_boundary_record(error: str) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema": "orchestrator-process-boundary/v1",
            "kind": boundary.kind if boundary is not None else "none",
            "identity": boundary.identity if boundary is not None else None,
            "complete": False,
            "inventory_source": "controller",
            "errors": [error],
            "members": [],
            "live_members": [],
        }
        if boundary is not None:
            record.update(
                {
                    "root_pid": getattr(boundary, "_root_pid", None),
                    "group_id": getattr(boundary, "_group_id", None),
                    "session_id": getattr(boundary, "_session_id", None),
                }
            )
        return record

    def _record_final_boundary(cleanup_result: CleanupResult | None) -> Any:
        """Take the one explicit post-cleanup inventory used for release safety."""

        nonlocal final_boundary_recorded, release_safe
        final_boundary_recorded = True
        release_safe = False
        inventory: Any = None
        inventory_error: str | None = None
        try:
            if supervisor is not None:
                inventory = supervisor.boundary_inventory()
            elif boundary is not None:
                inventory = boundary.inventory()
            else:
                inventory_error = "owned process boundary is unavailable"
        except BaseException as exc:
            inventory_error = (
                f"final boundary inventory failed: {type(exc).__name__}: {exc}"
            )
        if inventory_error is not None or inventory is None:
            state["process_boundary"] = _incomplete_boundary_record(
                inventory_error or "final boundary inventory was unavailable"
            )
            state["owned_boundary_empty"] = False
            state["helpers_complete"] = False
            state["resource_claim_release_safe"] = False
            return None
        complete = bool(getattr(inventory, "complete", False))
        live_members = tuple(getattr(inventory, "processes", ()) or ())
        try:
            state["process_boundary"] = (
                boundary.to_record(inventory)
                if boundary is not None
                else _incomplete_boundary_record(
                    "no controller-owned process boundary is attached"
                )
            )
        except BaseException as exc:
            state["process_boundary"] = _incomplete_boundary_record(
                f"final boundary evidence serialization failed: {type(exc).__name__}: {exc}"
            )
            complete = False
            live_members = ()
        state["owned_boundary_empty"] = complete and not live_members
        state["helpers_complete"] = bool(
            cleanup_result is not None
            and cleanup_result.proved_reap
            and complete
            and not live_members
        )
        release_safe = bool(
            process is not None
            and direct_child_reaped
            and cleanup_result is not None
            and cleanup_result.proved_reap
            and complete
            and not live_members
        )
        state["resource_claim_release_safe"] = release_safe
        return inventory

    def _retain_unresolved_claims() -> list[str]:
        if resource_claims is None or process is None or release_safe:
            return []
        boundary_evidence = state.get("process_boundary")
        retained_identities: list[dict[str, Any]] = []
        if isinstance(boundary_evidence, Mapping):
            raw_members = boundary_evidence.get("live_members")
            if isinstance(raw_members, list):
                retained_identities = [
                    dict(item) for item in raw_members if isinstance(item, Mapping)
                ]
            boundary_value = dict(boundary_evidence)
        else:
            boundary_value = {
                "complete": False,
                "errors": ["owned boundary evidence was not published"],
            }
        if not direct_child_reaped:
            boundary_value["complete"] = False
        failures = resource_claims.retain_boundary(
            boundary=boundary_value,
            identities=retained_identities,
        )
        state["held_resource_claims"] = resource_claims.held
        state["waiting_resource_claim"] = None
        if failures:
            state["resource_retention_errors"] = failures
        return failures

    def _claims_can_release() -> bool:
        # Before Popen there is no provider boundary to retain.  Once Popen
        # returns, this is the sole permission used by finally: the explicit
        # post-cleanup release proof above must have completed successfully.
        return process is None or release_safe

    def _direct_handle_cleanup() -> dict[str, Any]:
        """Bounded cleanup for the exact Popen handle when identity is uncertain.

        This is deliberately independent from the supervisor's evidence.  A
        successful ``wait`` proves only that this handle was reaped; it does
        not prove the child creation identity, the owned boundary, or claim
        release safety.
        """

        nonlocal direct_handle_cleanup_attempted, direct_child_reaped
        if direct_handle_cleanup_attempted:
            previous = state.get("direct_handle_cleanup")
            return (
                dict(previous)
                if isinstance(previous, Mapping)
                else {
                    "status": "HANDLE_CLEANUP_ALREADY_ATTEMPTED",
                    "final_reap": direct_child_reaped,
                    "identity_verified": False,
                    "identity_uncertain": True,
                }
            )
        direct_handle_cleanup_attempted = True
        record: dict[str, Any] = {
            "status": "HANDLE_CLEANUP",
            "pid": getattr(process, "pid", None),
            "stages": [],
            "errors": [],
            "final_reap": False,
            "identity_verified": False,
            "identity_uncertain": True,
        }
        stages = record["stages"]
        errors = record["errors"]

        def failed(stage: str, exc: BaseException) -> None:
            assert isinstance(stages, list)
            assert isinstance(errors, list)
            stages.append(f"{stage}_FAILED")
            errors.append(f"{stage.lower()} failed: {type(exc).__name__}: {exc}")

        reaped = False
        poll_value: object = None
        poll_failed = False
        try:
            poll_value = process.poll()
            assert isinstance(stages, list)
            stages.append("POLL")
        except BaseException as exc:
            poll_failed = True
            failed("POLL", exc)

        if not poll_failed and poll_value is not None:
            assert isinstance(stages, list)
            stages.append("ALREADY_EXITED")
            try:
                process.wait(timeout=0)
                stages.append("DIRECT_REAP")
                reaped = True
            except BaseException as exc:
                failed("REAP", exc)
        else:
            assert isinstance(stages, list)
            stages.append("TERMINATE_REQUESTED")
            try:
                process.terminate()
            except BaseException as exc:
                failed("TERMINATE", exc)
            try:
                process.wait(timeout=5.0)
                stages.extend(("TERMINATE_WAIT", "DIRECT_REAP"))
                reaped = True
            except subprocess.TimeoutExpired as exc:
                stages.append("TERMINATE_WAIT_TIMEOUT")
                errors.append(f"terminate wait timed out: {type(exc).__name__}: {exc}")
            except BaseException as exc:
                failed("TERMINATE_WAIT", exc)
            if not reaped:
                stages.append("KILL_REQUESTED")
                try:
                    process.kill()
                except BaseException as exc:
                    failed("KILL", exc)
                try:
                    process.wait(timeout=5.0)
                    stages.extend(("KILL_WAIT", "DIRECT_REAP"))
                    reaped = True
                except BaseException as exc:
                    failed("KILL_WAIT", exc)

        if reaped:
            direct_child_reaped = True
            state["direct_child_reaped"] = True
            record["final_reap"] = True
        state["direct_handle_cleanup"] = record
        return record

    def _postlaunch_cleanup() -> dict[str, Any] | None:
        """Attempt truthful cleanup for every process-nonnull failure route."""

        nonlocal cleanup_attempted, cleanup_result, direct_child_reaped, supervisor
        if process is None:
            return None
        if cleanup_attempted:
            if not final_boundary_recorded:
                _record_final_boundary(cleanup_result)
            return None
        cleanup_attempted = True
        cleanup_evidence: dict[str, Any] | None = None
        if supervisor is None:
            try:
                supervisor = ProcessSupervisor(
                    process,
                    child,
                    graceful_timeout_seconds=5.0,
                    force_timeout_seconds=5.0,
                    observer=None,
                    boundary=boundary,
                )
            except BaseException as exc:
                state.setdefault("postlaunch_cleanup_errors", []).append(
                    f"supervisor construction failed: {type(exc).__name__}: {exc}"
                )
        if supervisor is not None and cleanup_result is None:
            try:
                cleanup_result = supervisor.cleanup()
                direct_child_reaped = cleanup_result.final_reap
                state["direct_child_reaped"] = direct_child_reaped
                cleanup_evidence = cleanup_result.to_record()
                if "GRACEFUL_WAIT_TIMEOUT" in cleanup_result.stages:
                    cleanup_evidence["terminate_wait_timed_out"] = True
                if "FINAL_REAP_TIMEOUT" in cleanup_result.stages:
                    cleanup_evidence["kill_wait_timed_out"] = True
            except BaseException as exc:
                state.setdefault("postlaunch_cleanup_errors", []).append(
                    f"supervisor cleanup failed: {type(exc).__name__}: {exc}"
                )
        if supervisor is None and boundary is not None:
            try:
                inventory = boundary.cleanup_owned(
                    graceful_timeout_seconds=5.0,
                    force_timeout_seconds=5.0,
                )
                state["process_boundary"] = boundary.to_record(inventory)
                cleanup_evidence = {
                    "status": "BOUNDARY_FALLBACK",
                    "boundary_complete": inventory.complete,
                    "owned_boundary_empty": not inventory.processes,
                    "errors": list(inventory.errors),
                }
            except BaseException as exc:
                state.setdefault("postlaunch_cleanup_errors", []).append(
                    f"boundary cleanup failed: {type(exc).__name__}: {exc}"
                )
        if supervisor is None:
            direct_evidence = _direct_handle_cleanup()
            if cleanup_evidence is None:
                cleanup_evidence = direct_evidence
            else:
                cleanup_evidence["direct_handle_cleanup"] = direct_evidence
        elif cleanup_result is None:
            # Construction or cleanup may have left a supervisor object but
            # no truthful result.  The exact Popen handle remains the only
            # bounded cleanup mechanism available in that case.
            cleanup_evidence = cleanup_evidence or _direct_handle_cleanup()
        elif not cleanup_result.final_reap:
            # Identity-uncertain and otherwise unproven supervisor results
            # require the same direct-handle attempt.  Its result is evidence
            # only; _record_final_boundary still requires proved_reap.
            cleanup_evidence = cleanup_evidence or cleanup_result.to_record()
            direct_evidence = _direct_handle_cleanup()
            if isinstance(cleanup_evidence, dict):
                cleanup_evidence["direct_handle_cleanup"] = direct_evidence
        if (
            supervisor is not None
            and cleanup_result is not None
            and cleanup_result.final_reap
        ):
            state["direct_child_reaped"] = direct_child_reaped
        if not final_boundary_recorded:
            _record_final_boundary(cleanup_result)
        return cleanup_evidence

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
                state.update(
                    {
                        "state": "WAITING_RESOURCE",
                        "held_resource_claims": resource_claims.held,
                        "waiting_resource_claim": wait,
                        "resource_claim_findings": list(resource_claims.findings),
                    }
                )
                _atomic_json(invocation.status_path, state)

            resource_claims.acquire_all(invocation.resources, on_wait=record_wait)
            state.update(
                {
                    "held_resource_claims": resource_claims.held,
                    "waiting_resource_claim": None,
                    "resource_claim_findings": list(resource_claims.findings),
                }
            )
        if resource_claims is not None:
            arming_failures = resource_claims.arm_boundary()
            state.update(
                {
                    "held_resource_claims": resource_claims.held,
                    "resource_claim_findings": list(resource_claims.findings),
                }
            )
            if arming_failures:
                state.update(
                    {
                        "state": "LAUNCH_FAILED",
                        "ended_utc": _utc(),
                        "error": "cannot durably arm every resource claim before provider launch",
                        "resource_arming_errors": arming_failures,
                    }
                )
                _atomic_json(invocation.status_path, state)
                return 1
        with (
            invocation.jsonl_path.open("wb") as jsonl,
            invocation.stderr_path.open("wb") as stderr,
        ):
            child_env = None
            if invocation.child_environment_isolation:
                if invocation.runtime_profile is not None:
                    child_env, cleared = build_child_environment(
                        invocation.runtime_profile
                    )
                    state["child_environment_isolation"] = {
                        "enabled": True,
                        "profile_id": invocation.runtime_profile.profile_id,
                        "provider_needs": list(
                            invocation.runtime_profile.provider_needs
                        ),
                        "workflow_grants": list(
                            invocation.runtime_profile.workflow_grants
                        ),
                        "cleared_variable_names": cleared,
                    }
                else:
                    child_env, cleared = isolated_coding_child_environment()
                    state["child_environment_isolation"] = {
                        "enabled": True,
                        "cleared_variable_names": cleared,
                    }
            if launch_spec is not None:
                child_env = apply_provider_env_overrides(
                    child_env, launch_spec.env_overrides
                )
            boundary = ProcessBoundary.prepare()
            process = subprocess.Popen(
                argv,
                cwd=invocation.run_root,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=child_env,
                **boundary.popen_kwargs,
            )
            child = _identity(process.pid, parent=controller.pid)
            state.update(
                {
                    "provider_pid": child.pid,
                    "provider_started_utc": iso_utc(child.created_utc),
                    "provider_created_utc": iso_utc(child.created_utc),
                    "codex_pid": child.pid,
                    "codex_started_utc": iso_utc(child.created_utc),
                    "codex_created_utc": iso_utc(child.created_utc),
                }
            )
            # The captured OS boundary is established before the provider is
            # resumed.  The shared supervisor owns every later exact cleanup,
            # inventory, and final-reap stage.
            supervisor = ProcessSupervisor(
                process,
                child,
                parent_pid=controller.pid,
                observer=None,
                boundary=boundary,
            )
            # The supervisor is bound before attach can partially establish or
            # reject the OS boundary, so every post-Popen exception retains a
            # shared cleanup owner rather than falling back to direct-child
            # evidence alone.
            boundary.attach(process, child)
            state["process_boundary"] = boundary.to_record(boundary.inventory())
            state.update(
                {
                    "state": running_state,
                }
            )
            _atomic_json(invocation.status_path, state)
            _append_event(
                invocation.event_log,
                _event(
                    invocation,
                    started_event,
                    controller_pid=controller.pid,
                    provider_id=invocation.provider_id,
                    provider_pid=child.pid,
                    codex_pid=child.pid,
                ),
            )
            assert (
                process.stdin is not None
                and process.stdout is not None
                and process.stderr is not None
            )
            process.stdin.write(adapter.encode_prompt(prompt))
            process.stdin.close()
            provider_terminal_event: ProviderEvent | None = None

            def drain(source: Any, destination: Any, parse: bool) -> None:
                nonlocal state, provider_terminal_event
                for line in iter(source.readline, b""):
                    destination.write(line)
                    destination.flush()
                    os.fsync(destination.fileno())
                    if parse:
                        event = adapter.parse_transcript_line(line)
                        if event is not None:
                            with lock:
                                if event.session_id:
                                    expected_session = (
                                        thread
                                        if invocation.action == "resume"
                                        else state.get("provider_session_id")
                                    )
                                    if (
                                        expected_session
                                        and event.session_id != expected_session
                                    ):
                                        state["thread_identity_error"] = (
                                            f"provider session identity {event.session_id!r} does not match "
                                            f"validated resume session {expected_session!r}"
                                        )
                                    else:
                                        state["provider_session_id"] = event.session_id
                                        state["session_id"] = event.session_id
                                        state["thread_id"] = event.session_id
                                        evidence = state.get("provider_evidence")
                                        if isinstance(evidence, Mapping):
                                            evidence = dict(evidence)
                                            evidence["session_id"] = event.session_id
                                            state["provider_evidence"] = evidence
                                        if invocation.canonical is not None:
                                            resume_identity = dict(
                                                state.get("resume_identity") or {}
                                            )
                                            resume_identity["session_id"] = (
                                                event.session_id
                                            )
                                            resume_identity["live_identity"] = {
                                                "provider_id": invocation.provider_id,
                                                "session_id": event.session_id,
                                            }
                                            state["resume_identity"] = resume_identity
                                if event.is_terminal:
                                    provider_terminal_event = event
                                _atomic_json(invocation.status_path, state)

            out_thread = threading.Thread(
                target=drain, args=(process.stdout, jsonl, True), daemon=True
            )
            err_thread = threading.Thread(
                target=drain, args=(process.stderr, stderr, False), daemon=True
            )
            out_thread.start()
            err_thread.start()
            assert supervisor is not None
            exit_code = supervisor.wait_for_exit()
            out_thread.join()
            err_thread.join()
            process.stdout.close()
            process.stderr.close()
        if supervisor is None:
            raise ProcessBoundaryUnsupported(
                "provider completed without an ownership boundary"
            )
        # Direct Popen reap and complete owned-boundary emptiness are separate
        # facts.  ``cleanup`` performs a fresh inventory after any termination
        # request, targets escaped exact identities, and reaps adopted members
        # before it can prove the claim-release predicate.
        cleanup_attempted = True
        cleanup_result = supervisor.cleanup()
        direct_child_reaped = cleanup_result.final_reap
        state["cleanup"] = cleanup_result.to_record()
        state["direct_child_reaped"] = direct_child_reaped
        boundary_inventory = _record_final_boundary(cleanup_result)
        if boundary_inventory is None:
            state.update(
                {
                    "state": "CONTROLLER_FAILED",
                    "ended_utc": _utc(),
                    "error": "final owned process boundary inventory failed",
                    "boundary_failure": {
                        "complete": False,
                        "live_helpers": [],
                        "errors": list(
                            (state.get("process_boundary") or {}).get("errors", [])
                        ),
                        "cleanup": cleanup_result.to_record(),
                    },
                }
            )
            _atomic_json(invocation.status_path, state)
            return 1
        observed_by_pid = {
            item.pid: item for item in boundary_inventory.observed_processes
        }
        owned_helpers: list[dict[str, Any]] = []
        for item in boundary_inventory.observed_processes:
            if item.pid == state.get("provider_pid") or item.created_utc is None:
                continue
            parent = observed_by_pid.get(item.ppid)
            if parent is not None and _is_launcher_descendant(
                item,
                parent,
                provider_root_pid=state.get("provider_pid"),
            ):
                continue
            owned_helpers.append(
                {
                    "name": f"helper-{item.pid}",
                    "identity": {
                        "pid": item.pid,
                        "created_utc": iso_utc(item.created_utc),
                    },
                }
            )
        state["owned_helpers"] = owned_helpers
        if not release_safe:
            state.update(
                {
                    "state": "CONTROLLER_FAILED",
                    "ended_utc": _utc(),
                    "error": "owned process boundary is incomplete or still contains live helpers",
                    "boundary_failure": {
                        "complete": boundary_inventory.complete,
                        "live_helpers": [
                            item.pid for item in boundary_inventory.processes
                        ],
                        "errors": list(boundary_inventory.errors),
                        "cleanup": cleanup_result.to_record(),
                    },
                }
            )
            _atomic_json(invocation.status_path, state)
            return 1
        thread_identity_error = state.get("thread_identity_error")
        if isinstance(thread_identity_error, str):
            state.update(
                {
                    "state": "CONTROLLER_FAILED",
                    "exit_code": exit_code,
                    "ended_utc": _utc(),
                    "error": thread_identity_error,
                }
            )
            _atomic_json(invocation.status_path, state)
            _append_event(
                invocation.event_log,
                _event(
                    invocation,
                    "CONTROLLER_FAILED",
                    exit_code=exit_code,
                    error=thread_identity_error,
                    thread_id=state.get("thread_id"),
                ),
            )
            return 1
        if invocation.action == "start" and not state.get("provider_session_id"):
            state.update(
                {
                    "state": "LAUNCH_FAILED",
                    "exit_code": exit_code,
                    "ended_utc": _utc(),
                    "error": f"{invocation.provider_id} exited without a session initialization record; inspect stderr_path",
                }
            )
            _atomic_json(invocation.status_path, state)
            _append_event(
                invocation.event_log,
                _event(invocation, "LAUNCH_FAILED", exit_code=exit_code),
            )
            return 1
        result_valid = True
        if invocation.canonical is not None:
            result_validation, result_valid, task_result = _canonical_result_validation(
                invocation
            )
            advancement = None
            if result_valid and task_result is not None:
                try:
                    advancement = read_task_advancement(
                        invocation.workspace,
                        card=task_result.card,
                        result=task_result,
                    )
                except TaskValidationError as exc:
                    result_validation = {
                        "state": "ADVANCEMENT_INVALID",
                        "path": str(invocation.workspace),
                        "detail": str(exc),
                        "acceptance_state": "PENDING",
                    }
                    result_valid = False
            state["result_validation"] = result_validation
            state["result_valid"] = result_valid
            state["task_advancement_state"] = (
                advancement.state if advancement is not None else "PENDING"
            )
            state["terminal_acceptance_state"] = "PENDING"
            if advancement is not None:
                result_validation["acceptance_state"] = advancement.state
            if advancement is not None and advancement.state == "ACCEPTED":
                acceptance_identity = advancement.acceptance_identity
                if acceptance_identity is None:
                    result_validation = {
                        "state": "ADVANCEMENT_INVALID",
                        "path": str(invocation.workspace),
                        "detail": "accepted task advancement has no acceptance identity",
                        "acceptance_state": "PENDING",
                    }
                    state["result_validation"] = result_validation
                    state["result_valid"] = False
                else:
                    state = _canonical_acceptance_status(state, acceptance_identity)
        elif invocation.repository is not None:
            result_validation, result_valid = _coding_result_validation(invocation)
            state["result_validation"] = result_validation
            state["result_valid"] = result_valid
        terminal_outcome = adapter.terminal_outcome(provider_terminal_event, exit_code)
        if terminal_outcome not in PROVIDER_TERMINAL_OUTCOMES:
            # REL.R1-001: the selected adapter's provider-neutral terminal
            # outcome must be in the closed COMPLETED/FAILED/CANCELLED
            # vocabulary before the generic controller may publish
            # PROVIDER_EXITED or return success.  An unknown outcome is a
            # truthful controller failure even when the child exited 0 and
            # the result is shape-valid.
            terminal_error = (
                f"provider adapter {invocation.provider_id!r} returned terminal outcome "
                f"{terminal_outcome!r} outside the closed provider-neutral vocabulary "
                "COMPLETED/FAILED/CANCELLED"
            )
            state.update(
                {
                    "state": "CONTROLLER_FAILED",
                    "provider_terminal_outcome": terminal_outcome,
                    "exit_code": exit_code,
                    "ended_utc": _utc(),
                    "error": terminal_error,
                    "terminal_outcome_invalid": {
                        "returned": terminal_outcome,
                        "allowed": sorted(PROVIDER_TERMINAL_OUTCOMES),
                    },
                }
            )
            _atomic_json(invocation.status_path, state)
            _append_event(
                invocation.event_log,
                _event(
                    invocation,
                    "CONTROLLER_FAILED",
                    provider_id=invocation.provider_id,
                    provider_terminal_outcome=terminal_outcome,
                    exit_code=exit_code,
                    thread_id=state.get("thread_id"),
                    session_id=state.get("provider_session_id"),
                    result_validation=state.get("result_validation"),
                    error=terminal_error,
                ),
            )
            return 1
        state["prepared_stop"]["final"] = _run_prepared_stop_hook(
            invocation, boundary="final"
        )
        state.update(
            {
                "state": exited_state,
                "provider_terminal_outcome": terminal_outcome,
                "exit_code": exit_code,
                "ended_utc": _utc(),
            }
        )
        _atomic_json(invocation.status_path, state)
        _append_event(
            invocation.event_log,
            _event(
                invocation,
                exited_event,
                provider_id=invocation.provider_id,
                provider_terminal_outcome=terminal_outcome,
                exit_code=exit_code,
                thread_id=state.get("thread_id"),
                session_id=state.get("provider_session_id"),
                result_validation=state.get("result_validation"),
            ),
        )
        return (
            1
            if terminal_outcome in {"FAILED", "CANCELLED"} or not result_valid
            else exit_code
        )
    except KeyboardInterrupt:
        cleanup_evidence = _postlaunch_cleanup() if process is not None else None
        if process is not None and not release_safe:
            _ = _retain_unresolved_claims()
            retained_claims = (
                resource_claims.held if resource_claims is not None else []
            )
            error = "controller interrupted; exact provider child shutdown could not be proven"
            state.update(
                {
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
                }
            )
            _atomic_json(invocation.status_path, state)
            _append_event(
                invocation.event_log,
                _event(
                    invocation,
                    "COORDINATION_FAILED",
                    error=error,
                    child_shutdown=cleanup_evidence,
                    retained_claims=retained_claims,
                ),
            )
            return 130
        state.update({"state": "CONTROLLER_INTERRUPTED", "ended_utc": _utc()})
        _atomic_json(invocation.status_path, state)
        _append_event(
            invocation.event_log, _event(invocation, "CONTROLLER_INTERRUPTED")
        )
        return 130
    except ResourceLockError as exc:
        cleanup_evidence = _postlaunch_cleanup() if process is not None else None
        retained_claims = resource_claims.held if resource_claims is not None else []
        state.update(
            {
                "state": "COORDINATION_FAILED",
                "ended_utc": _utc(),
                "error": str(exc),
                "coordination_failure": {
                    "error": str(exc),
                    "child_shutdown": cleanup_evidence,
                    "retained_claims": retained_claims,
                },
                "held_resource_claims": retained_claims,
            }
        )
        _atomic_json(invocation.status_path, state)
        _append_event(
            invocation.event_log,
            _event(invocation, "COORDINATION_FAILED", error=str(exc)),
        )
        return 1
    except Exception as exc:
        cleanup_evidence = _postlaunch_cleanup() if process is not None else None
        if process is not None and not release_safe:
            _ = _retain_unresolved_claims()
            retained_claims = (
                resource_claims.held if resource_claims is not None else []
            )
            error = f"{exc}; exact provider child shutdown could not be proven"
            state.update(
                {
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
                }
            )
            _atomic_json(invocation.status_path, state)
            _append_event(
                invocation.event_log,
                _event(
                    invocation,
                    "COORDINATION_FAILED",
                    error=error,
                    child_shutdown=cleanup_evidence,
                    retained_claims=retained_claims,
                ),
            )
            return 1
        state.update(
            {
                "state": "CONTROLLER_FAILED"
                if state.get("provider_pid", state.get("codex_pid"))
                else "LAUNCH_FAILED",
                "ended_utc": _utc(),
                "error": str(exc),
            }
        )
        _atomic_json(invocation.status_path, state)
        _append_event(
            invocation.event_log, _event(invocation, state["state"], error=str(exc))
        )
        return 1
    finally:
        if process is not None and not final_boundary_recorded:
            _postlaunch_cleanup()
        if resource_claims is not None:
            if _claims_can_release():
                release_failures = resource_claims.release_all()
                state["held_resource_claims"] = resource_claims.held
                state["waiting_resource_claim"] = None
                if release_failures:
                    state["resource_release_errors"] = release_failures
            else:
                boundary_evidence = state.get("process_boundary")
                retained_identities: list[dict[str, Any]] = []
                if isinstance(boundary_evidence, Mapping):
                    raw_members = boundary_evidence.get("live_members")
                    if isinstance(raw_members, list):
                        retained_identities = [
                            dict(item)
                            for item in raw_members
                            if isinstance(item, Mapping)
                        ]
                if not isinstance(boundary_evidence, Mapping):
                    boundary_evidence = {
                        "complete": False,
                        "errors": ["owned boundary evidence was not published"],
                    }
                else:
                    boundary_evidence = dict(boundary_evidence)
                    if not state.get("direct_child_reaped"):
                        boundary_evidence["complete"] = False
                retention_failures = resource_claims.retain_boundary(
                    boundary=boundary_evidence,
                    identities=retained_identities,
                )
                state["held_resource_claims"] = resource_claims.held
                state["waiting_resource_claim"] = None
                if retention_failures:
                    state["resource_retention_errors"] = retention_failures
            try:
                _atomic_json(invocation.status_path, state)
            except OSError:
                pass
        if (
            invocation.repository is not None
            and provider_resume_handoff is None
            and not unsupported_operations
        ):
            try:
                _publish_registry()
            except Exception as exc:
                state.update(
                    {
                        "state": "CONTROLLER_FAILED",
                        "ended_utc": state.get("ended_utc") or _utc(),
                        "error": f"final lifecycle registry publication failed: {exc}",
                        "lifecycle_publication_failure": {
                            "type": type(exc).__name__,
                            "detail": str(exc),
                            "terminal_authority": False,
                        },
                        "helpers_complete": False,
                    }
                )
                try:
                    _atomic_json(invocation.status_path, state)
                except Exception:
                    pass
                if boundary is not None:
                    boundary.close()
                raise InvocationError(
                    f"final lifecycle registry publication failed: {exc}"
                ) from exc
        if boundary is not None:
            boundary.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Launch one observable Codex lane turn"
    )
    parser.add_argument("invocation", type=Path)
    args = parser.parse_args(argv)
    try:
        return run(load_invocation(args.invocation))
    except (InvocationError, LaneLifecycleError, ProcessBoundaryUnsupported) as exc:
        print(f"lane-controller invocation error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
