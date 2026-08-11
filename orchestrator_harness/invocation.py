"""Strict invocation records and compatibility adapters.

The canonical record is intentionally small and provider-neutral.  The two
older routes are parsed by their existing contracts and can be represented as
an internal :class:`CanonicalInvocation` for shared checks, but their input
schemas are never silently merged with the canonical record.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .prompt_bundle import PROMPT_BUNDLE_SCHEMA, PromptBundleError, bundle_from_record
from .stable_io import canonical_json


CANONICAL_INVOCATION_SCHEMA = "orchestrator-worker-invocation/v1"
CODING_INVOCATION_SCHEMA = "orchestrator-coding-invocation/v1"

# This is the single public top-level coding-v1 contract.  Keep the aliases
# here, rather than in either consumer, so adapters cannot silently diverge.
CODING_V1_CANONICAL_ONLY_FIELDS = frozenset({
    "provider",
    "profile",
    "prompt_bundle",
    "workflow",
    "task_card",
    "cohort_id",
})
CODING_V1_FIRMWARE_ONLY_FIELDS = frozenset({
    "policy_sha256",
    "leases",
    "board_tokens",
    "mcp_servers",
    "server_snapshot",
})
CODING_V1_ALIAS_GROUPS = (
    ("codex", "codex_settings", "model_settings"),
    ("repository", "git"),
    ("event_log_path", "event_log", "lane_event_log"),
    ("resume_identity", "resume"),
)
CODING_V1_ALLOWED_FIELDS = frozenset({
    "schema",
    "action",
    "run_root",
    "runtime_root",
    "prompt_path",
    "prompt_sha256",
    "output_paths",
    "event_log_path",
    "event_log",
    "lane_event_log",
    "worker_invocation_id",
    "lane_id",
    "declared_lane_id",
    "task",
    "phase",
    "repository",
    "git",
    "resources",
    "exclusive_resources",
    "resource_lock_root",
    "codex",
    "codex_settings",
    "model_settings",
    "codex_command",
    "config_overrides",
    "resume_thread_id",
    "resume_identity",
    "resume",
    "label",
    "doer",
    "finding_gate",
    "child_environment_isolation",
})


class InvocationValidationError(ValueError):
    """Raised when an invocation cannot be adapted without ambiguity."""


def validate_coding_v1_fields(raw: Mapping[str, Any]) -> None:
    """Validate coding-v1's closed top-level shape before any filesystem work."""

    if not isinstance(raw, Mapping):
        raise InvocationValidationError("coding v1 invocation must be an object")
    if raw.get("schema") != CODING_INVOCATION_SCHEMA:
        raise InvocationValidationError("record is not coding v1")
    canonical = sorted(CODING_V1_CANONICAL_ONLY_FIELDS & set(raw), key=str)
    if canonical:
        raise InvocationValidationError(
            "coding v1 record contains canonical-only fields: " + ", ".join(map(str, canonical))
        )
    firmware = sorted(CODING_V1_FIRMWARE_ONLY_FIELDS & set(raw), key=str)
    if firmware:
        raise InvocationValidationError(
            "coding v1 record contains fields reserved for the other route (firmware-only): "
            + ", ".join(map(str, firmware))
        )
    unknown = sorted(set(raw) - CODING_V1_ALLOWED_FIELDS, key=str)
    if unknown:
        raise InvocationValidationError(
            "coding v1 record contains unknown top-level fields: " + ", ".join(map(str, unknown))
        )
    for aliases in CODING_V1_ALIAS_GROUPS:
        present = [alias for alias in aliases if alias in raw]
        if len(present) > 1:
            raise InvocationValidationError(
                "coding v1 contains ambiguous aliases: " + ", ".join(present)
            )
    if "lane_id" in raw and "declared_lane_id" in raw and raw["lane_id"] != raw["declared_lane_id"]:
        raise InvocationValidationError("coding v1 lane_id and declared_lane_id conflict")
    if "resources" in raw and "exclusive_resources" in raw and raw["resources"] != raw["exclusive_resources"]:
        raise InvocationValidationError("coding v1 resources and exclusive_resources conflict")


_LEGACY_FIELDS = frozenset(
    {
        "prompt_path",
        "prompt_sha256",
        "model_settings",
        "codex_command",
        "config_overrides",
        "policy_sha256",
        "leases",
        "board_tokens",
        "mcp_servers",
        "server_snapshot",
        "doer",
        "declared_lane_id",
        "resource_lock_root",
        "event_log",
        "lane_event_log",
        "exclusive_resources",
        "git",
        "resume_identity",
        "resume_thread_id",
        "codex",
        "codex_settings",
        "finding_gate",
        "child_environment_isolation",
    }
)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvocationValidationError(f"{name} must be a non-empty string")
    return value.strip()


def _digest(value: object, name: str) -> str:
    result = _text(value, name).lower()
    if len(result) != 64 or any(char not in "0123456789abcdef" for char in result):
        raise InvocationValidationError(f"{name} must be a SHA-256 hex digest")
    return result


def _strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise InvocationValidationError(f"{name} must be a list of non-empty strings")
    if len(set(value)) != len(value):
        raise InvocationValidationError(f"{name} must not contain duplicates")
    return tuple(item.strip() for item in value)


def _closed(value: object, required: set[str], optional: set[str], name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise InvocationValidationError(f"{name} must be an object")
    keys = set(value)
    if not required.issubset(keys) or not keys.issubset(required | optional):
        missing = sorted(required - keys)
        extra = sorted(keys - required - optional)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if extra:
            detail.append("unknown " + ", ".join(extra))
        raise InvocationValidationError(f"{name} has an invalid closed shape ({'; '.join(detail)})")
    return value


def _immutable(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True)
class CanonicalInvocation:
    """The provider-neutral launch/continuation identity."""

    schema: str
    action: str
    run_root: Path
    runtime_root: Path
    lane_id: str
    worker_invocation_id: str
    cohort_id: str
    workflow_id: str
    workflow_version: str
    task_card_id: str
    task_card_revision: str
    task_card_sha256: str
    role: str
    provider_id: str
    provider_model: str
    provider_options: Mapping[str, Any]
    profile: Mapping[str, Any]
    prompt_bundle: Mapping[str, Any]
    output_paths: Mapping[str, Path]
    event_log_path: Path
    resources: tuple[str, ...]
    repository: Mapping[str, Any] | None
    requested_session_id: str | None
    label: str
    task: str
    phase: str
    legacy_route: str | None = None
    legacy_policy_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.schema != CANONICAL_INVOCATION_SCHEMA:
            raise InvocationValidationError("canonical invocation schema is invalid")
        if self.action not in {"start", "resume"}:
            raise InvocationValidationError("action must be start or resume")
        if self.requested_session_id is not None and not self.requested_session_id.strip():
            raise InvocationValidationError("requested session ID cannot be empty")

    @property
    def profile_id(self) -> str:
        return str(self.profile.get("id"))

    @property
    def prompt_bundle_sha256(self) -> str:
        return str(self.prompt_bundle.get("bundle_sha256"))

    @property
    def prompt_content_sha256(self) -> str:
        return str(self.prompt_bundle.get("final_sha256"))

    def provider_launch_record(self) -> dict[str, Any]:
        """Return the validated, effective provider settings used for launch."""

        options = dict(self.provider_options)
        record: dict[str, Any] = {
            "provider_id": self.provider_id,
            "model": self.provider_model,
            "command": list(options.get("command", [self.provider_id])),
            "reasoning_effort": options.get("reasoning_effort", "medium"),
            "service_tier": options.get("service_tier", "priority"),
            "permission_mode": options.get("permission_mode"),
            "allowed_tools": list(options.get("allowed_tools", [])),
            "disallowed_tools": list(options.get("disallowed_tools", [])),
            "mcp_config": options.get("mcp_config"),
            "config_overrides": list(options.get("config_overrides", [])),
            "sandbox": options.get("sandbox", "workspace-write"),
            "approval_policy": options.get("approval_policy", "never"),
        }
        if self.provider_id == "codex":
            workspace = self.run_root.expanduser().resolve(strict=False) / ".agent-workspace"
            last_message = self.output_paths["last_message"]
            effective_path = last_message if last_message.is_absolute() else workspace / last_message
            record["last_message_path"] = os.path.normcase(
                str(effective_path.expanduser().resolve(strict=False))
            )
        return record

    @property
    def provider_launch_sha256(self) -> str:
        return hashlib.sha256(
            canonical_json(self.provider_launch_record()).encode("utf-8")
        ).hexdigest()

    def identity(self, *, session_id: str | None = None, starting_commit: str | None = None) -> dict[str, Any]:
        repository = dict(self.repository or {})
        if starting_commit is not None:
            repository["starting_commit"] = starting_commit
        return {
            "schema": CANONICAL_INVOCATION_SCHEMA,
            "lane_id": self.lane_id,
            "worker_invocation_id": self.worker_invocation_id,
            "cohort_id": self.cohort_id,
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
            "task_card_id": self.task_card_id,
            "task_card_revision": self.task_card_revision,
            "task_card_sha256": self.task_card_sha256,
            "provider_id": self.provider_id,
            "provider_launch_sha256": self.provider_launch_sha256,
            "session_id": session_id or self.requested_session_id,
            "repository": repository,
            "prompt_bundle_sha256": self.prompt_bundle_sha256,
            "prompt_content_sha256": self.prompt_content_sha256,
            "terminal_acceptance_state": "PENDING",
        }

    def to_record(self) -> dict[str, Any]:
        workflow = {"id": self.workflow_id, "version": self.workflow_version}
        task_card = {
            "id": self.task_card_id,
            "revision": self.task_card_revision,
            "sha256": self.task_card_sha256,
        }
        provider = {"id": self.provider_id, "model": self.provider_model, **dict(self.provider_options)}
        record: dict[str, Any] = {
            "schema": self.schema,
            "action": self.action,
            "run_root": str(self.run_root),
            "runtime_root": str(self.runtime_root),
            "lane_id": self.lane_id,
            "worker_invocation_id": self.worker_invocation_id,
            "cohort_id": self.cohort_id,
            "workflow": workflow,
            "task_card": task_card,
            "role": self.role,
            "provider": provider,
            "profile": dict(self.profile),
            "prompt_bundle": dict(self.prompt_bundle),
            "output_paths": {key: str(path) for key, path in self.output_paths.items()},
            "event_log_path": str(self.event_log_path),
            "resources": list(self.resources),
        }
        if self.repository is not None:
            record["repository"] = dict(self.repository)
        if self.requested_session_id is not None:
            record["resume"] = {"session_id": self.requested_session_id}
        return record


def _provider(value: object) -> tuple[str, str, Mapping[str, Any]]:
    provider = _closed(
        value,
        {"id", "model"},
        {
            "command",
            "reasoning_effort",
            "service_tier",
            "permission_mode",
            "allowed_tools",
            "disallowed_tools",
            "mcp_config",
            "config_overrides",
            "sandbox",
            "approval_policy",
        },
        "provider",
    )
    provider_id = _text(provider.get("id"), "provider.id")
    if provider_id not in {"codex", "claude-code"}:
        raise InvocationValidationError("provider.id must be codex or claude-code")
    model = _text(provider.get("model"), "provider.model")
    if "command" in provider:
        _strings(provider.get("command"), "provider.command")
    for key in ("allowed_tools", "disallowed_tools", "config_overrides"):
        if key in provider:
            _strings(provider.get(key), f"provider.{key}")
    if "mcp_config" in provider and not isinstance(provider.get("mcp_config"), (str, dict, list)):
        raise InvocationValidationError("provider.mcp_config must be a path or JSON value")
    return provider_id, model, _immutable(provider)


def parse_canonical_invocation(raw: Mapping[str, Any]) -> CanonicalInvocation:
    """Parse only the canonical schema; legacy aliases are rejected."""

    if not isinstance(raw, Mapping):
        raise InvocationValidationError("invocation root must be an object")
    if raw.get("schema") != CANONICAL_INVOCATION_SCHEMA:
        raise InvocationValidationError("invocation is not the canonical schema")
    foreign = sorted(set(raw) & _LEGACY_FIELDS)
    if foreign:
        raise InvocationValidationError(
            "canonical invocation contains legacy aliases: " + ", ".join(foreign)
        )
    required = {
        "schema",
        "action",
        "run_root",
        "runtime_root",
        "lane_id",
        "worker_invocation_id",
        "cohort_id",
        "workflow",
        "task_card",
        "role",
        "provider",
        "profile",
        "prompt_bundle",
        "output_paths",
        "event_log_path",
        "resources",
    }
    optional = {"repository", "resume", "label", "task", "phase"}
    _closed(raw, required, optional, "canonical invocation")
    action = _text(raw.get("action"), "action").lower()
    if action not in {"start", "resume"}:
        raise InvocationValidationError("action must be start or resume")
    workflow = _closed(raw.get("workflow"), {"id", "version"}, set(), "workflow")
    task_card = _closed(raw.get("task_card"), {"id", "revision", "sha256"}, set(), "task_card")
    provider_id, provider_model, provider_options = _provider(raw.get("provider"))
    profile = _closed(
        raw.get("profile"),
        {"schema", "id", "role", "provider", "model", "tools", "capabilities", "resources"},
        {"provider_needs", "workflow_grants"},
        "profile",
    )
    if profile.get("schema") != "orchestrator-runtime-profile/v1":
        raise InvocationValidationError("profile.schema is invalid")
    profile_id = _text(profile.get("id"), "profile.id")
    if _text(profile.get("role"), "profile.role") != _text(raw.get("role"), "role"):
        raise InvocationValidationError("profile.role does not match invocation role")
    if _text(profile.get("provider"), "profile.provider") != provider_id:
        raise InvocationValidationError("profile.provider does not match provider.id")
    if _text(profile.get("model"), "profile.model") != provider_model:
        raise InvocationValidationError("profile.model does not match provider.model")
    _strings(profile.get("tools"), "profile.tools")
    _strings(profile.get("capabilities"), "profile.capabilities")
    _strings(profile.get("resources"), "profile.resources")
    for key in ("provider_needs", "workflow_grants"):
        if key in profile:
            _strings(profile.get(key), f"profile.{key}")
    prompt_bundle = _closed(
        raw.get("prompt_bundle"),
        {
            "schema",
            "version",
            "workflow_id",
            "task_card_id",
            "profile_id",
            "components",
            "final_sha256",
            "final_size",
            "bundle_sha256",
        },
        set(),
        "prompt_bundle",
    )
    if prompt_bundle.get("schema") != PROMPT_BUNDLE_SCHEMA or prompt_bundle.get("version") != 1:
        raise InvocationValidationError("prompt_bundle schema/version is invalid")
    if prompt_bundle.get("workflow_id") != workflow.get("id"):
        raise InvocationValidationError("prompt_bundle.workflow_id does not match workflow")
    if prompt_bundle.get("task_card_id") != task_card.get("id"):
        raise InvocationValidationError("prompt_bundle.task_card_id does not match task_card")
    if prompt_bundle.get("profile_id") != profile_id:
        raise InvocationValidationError("prompt_bundle.profile_id does not match profile")
    _digest(task_card.get("sha256"), "task_card.sha256")
    output_paths_value = raw.get("output_paths")
    output_paths = _closed(
        output_paths_value,
        {"status", "jsonl", "stderr", "last_message"},
        set(),
        "output_paths",
    )
    for key in output_paths:
        _text(output_paths.get(key), f"output_paths.{key}")
    resources = _strings(raw.get("resources"), "resources")
    repository = raw.get("repository")
    if repository is not None:
        _closed(repository, {"common_dir", "worktree_root", "branch", "base_commit"}, {"starting_commit"}, "repository")
    requested_session_id: str | None = None
    if action == "resume":
        resume = _closed(raw.get("resume"), {"session_id"}, set(), "resume")
        requested_session_id = _text(resume.get("session_id"), "resume.session_id")
    elif "resume" in raw:
        raise InvocationValidationError("start invocation cannot contain resume")
    return CanonicalInvocation(
        schema=CANONICAL_INVOCATION_SCHEMA,
        action=action,
        run_root=Path(_text(raw.get("run_root"), "run_root")),
        runtime_root=Path(_text(raw.get("runtime_root"), "runtime_root")),
        lane_id=_text(raw.get("lane_id"), "lane_id"),
        worker_invocation_id=_text(raw.get("worker_invocation_id"), "worker_invocation_id"),
        cohort_id=_text(raw.get("cohort_id"), "cohort_id"),
        workflow_id=_text(workflow.get("id"), "workflow.id"),
        workflow_version=_text(workflow.get("version"), "workflow.version"),
        task_card_id=_text(task_card.get("id"), "task_card.id"),
        task_card_revision=_text(task_card.get("revision"), "task_card.revision"),
        task_card_sha256=_digest(task_card.get("sha256"), "task_card.sha256"),
        role=_text(raw.get("role"), "role"),
        provider_id=provider_id,
        provider_model=provider_model,
        provider_options=provider_options,
        profile=_immutable(profile),
        prompt_bundle=_immutable(prompt_bundle),
        output_paths={key: Path(str(value)) for key, value in output_paths.items()},
        event_log_path=Path(_text(raw.get("event_log_path"), "event_log_path")),
        resources=resources,
        repository=dict(repository) if isinstance(repository, Mapping) else None,
        requested_session_id=requested_session_id,
        label=_text(raw.get("label", raw.get("worker_invocation_id")), "label"),
        task=_text(raw.get("task", task_card.get("id")), "task"),
        phase=_text(raw.get("phase", "implementation"), "phase"),
    )


def _legacy_bundle_record(
    *, prompt_path: str, prompt_sha256: str, workflow_id: str, task_card_id: str, profile_id: str
) -> dict[str, Any]:
    """Create an identity-only record for a legacy adapter.

    Legacy routes do not read this record to change their prompt policy.  It is
    only used when the route is represented in a provider-neutral diagnostic
    identity, and the controller continues to verify the original prompt
    contract through its legacy loader.
    """

    manifest = {
        "schema": PROMPT_BUNDLE_SCHEMA,
        "version": 1,
        "workflow_id": workflow_id,
        "task_card_id": task_card_id,
        "profile_id": profile_id,
        "components": [
            {
                "id": "legacy-prompt",
                "ordinal": 0,
                "path": prompt_path,
                "sha256": prompt_sha256,
                "size": -1,
            }
        ],
        "final_sha256": prompt_sha256,
        "final_size": -1,
    }
    return {
        **manifest,
        "bundle_sha256": hashlib.sha256(canonical_json(manifest).encode("utf-8")).hexdigest(),
    }


def adapt_coding_v1(raw: Mapping[str, Any], *, run_root: Path | None = None) -> CanonicalInvocation:
    """Adapt coding v1 without accepting firmware-only fields."""

    validate_coding_v1_fields(raw)
    settings = raw.get("codex", raw.get("codex_settings", raw.get("model_settings")))
    if not isinstance(settings, Mapping):
        raise InvocationValidationError("coding v1 provider settings are missing")
    provider_id = "codex"
    model = _text(settings.get("model"), "codex.model")
    prompt_path = _text(raw.get("prompt_path"), "prompt_path")
    prompt_sha256 = _digest(raw.get("prompt_sha256"), "prompt_sha256")
    worker_id = _text(raw.get("worker_invocation_id"), "worker_invocation_id")
    lane_id = _text(raw.get("lane_id", raw.get("declared_lane_id")), "lane_id")
    card_id = _text(raw.get("task", "coding-v1-task"), "task")
    profile_id = f"legacy-coding-v1:{lane_id}"
    options = dict(settings)
    options["command"] = list(settings.get("command", raw.get("codex_command", ["codex"])))
    options["config_overrides"] = list(settings.get("config_overrides", raw.get("config_overrides", [])))
    profile = {
        "id": profile_id,
        "role": _text(raw.get("doer", lane_id), "doer"),
        "provider": provider_id,
        "model": model,
        "tools": [],
        "capabilities": [],
        "resources": list(raw.get("resources", raw.get("exclusive_resources", []))),
    }
    run_path = Path(_text(raw.get("run_root"), "run_root"))
    runtime_path = Path(_text(raw.get("runtime_root"), "runtime_root"))
    output = raw.get("output_paths")
    if not isinstance(output, Mapping):
        raise InvocationValidationError("coding v1 output_paths are missing")
    output_paths = {key: Path(_text(output.get(key), f"output_paths.{key}")) for key in ("status", "jsonl", "stderr", "last_message")}
    repository = raw.get("repository", raw.get("git"))
    requested_thread = _text(raw.get("resume_thread_id"), "resume_thread_id") if raw.get("resume_thread_id") is not None else None
    resume_identity = raw.get("resume_identity", raw.get("resume"))
    if resume_identity is not None:
        if not isinstance(resume_identity, Mapping):
            raise InvocationValidationError("resume_identity must be an object")
        identity_worker = resume_identity.get("worker_invocation_id")
        if identity_worker is not None and identity_worker != worker_id:
            raise InvocationValidationError("resume identity worker_invocation_id mismatch")
        identity_thread = resume_identity.get("thread_id")
        if identity_thread is not None:
            identity_thread_text = _text(identity_thread, "resume_identity.thread_id")
            if requested_thread is not None and requested_thread != identity_thread_text:
                raise InvocationValidationError("conflicting requested resume thread IDs")
            requested_thread = requested_thread or identity_thread_text
    return CanonicalInvocation(
        CANONICAL_INVOCATION_SCHEMA,
        _text(raw.get("action"), "action").lower(),
        run_path if run_root is None else run_root,
        runtime_path,
        lane_id,
        worker_id,
        "legacy-coding-v1",
        "coding-v1",
        "1",
        card_id,
        "legacy",
        _digest(raw.get("prompt_sha256"), "prompt_sha256"),
        _text(raw.get("doer", lane_id), "doer"),
        provider_id,
        model,
        _immutable(options),
        _immutable(profile),
        _immutable(_legacy_bundle_record(prompt_path=prompt_path, prompt_sha256=prompt_sha256, workflow_id="coding-v1", task_card_id=card_id, profile_id=profile_id)),
        output_paths,
        Path(_text(raw.get("event_log_path", raw.get("event_log", raw.get("lane_event_log"))), "event_log_path")),
        tuple(profile["resources"]),
        dict(repository) if isinstance(repository, Mapping) else None,
        requested_thread,
        _text(raw.get("label", worker_id), "label"),
        _text(raw.get("task", card_id), "task"),
        _text(raw.get("phase", "implementation"), "phase"),
        "coding-v1",
        None,
    )


def adapt_legacy_firmware(raw: Mapping[str, Any]) -> CanonicalInvocation:
    """Represent the schema-less firmware route without migrating its policy."""

    if "schema" in raw:
        raise InvocationValidationError("legacy firmware adapter requires a schema-less record")
    forbidden = {
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
    present = sorted(forbidden & set(raw))
    if present:
        raise InvocationValidationError("schema-less firmware record contains coding aliases: " + ", ".join(present))
    prompt_sha256 = _digest(raw.get("prompt_sha256"), "prompt_sha256")
    lane_id = _text(raw.get("declared_lane_id"), "declared_lane_id")
    label = _text(raw.get("label"), "label")
    settings = raw.get("model_settings")
    if not isinstance(settings, Mapping):
        raise InvocationValidationError("schema-less firmware model_settings are missing")
    profile_id = f"legacy-firmware:{lane_id}"
    profile = {
        "id": profile_id,
        "role": _text(raw.get("doer"), "doer"),
        "provider": "codex",
        "model": _text(settings.get("model"), "model_settings.model"),
        "tools": [],
        "capabilities": ["legacy-firmware-policy"],
        "resources": list(raw.get("leases", [])),
    }
    return CanonicalInvocation(
        CANONICAL_INVOCATION_SCHEMA,
        _text(raw.get("action"), "action").lower(),
        Path(_text(raw.get("run_root"), "run_root")),
        Path(_text(raw.get("run_root"), "run_root")),
        lane_id,
        f"legacy-firmware:{label}",
        "legacy-firmware",
        "legacy-firmware",
        "1",
        _text(raw.get("task"), "task"),
        "legacy",
        prompt_sha256,
        _text(raw.get("doer"), "doer"),
        "codex",
        _text(settings.get("model"), "model_settings.model"),
        _immutable({
            "command": list(raw.get("codex_command", ["codex"])),
            "config_overrides": list(raw.get("config_overrides", [])),
            "reasoning_effort": _text(settings.get("reasoning_effort"), "reasoning_effort"),
            "service_tier": _text(settings.get("service_tier"), "service_tier"),
            "sandbox": "danger-full-access",
            "approval_policy": "never",
        }),
        _immutable(profile),
        _immutable(_legacy_bundle_record(prompt_path=_text(raw.get("prompt_path"), "prompt_path"), prompt_sha256=prompt_sha256, workflow_id="legacy-firmware", task_card_id=_text(raw.get("task"), "task"), profile_id=profile_id)),
        {key: Path(_text((raw.get("output_paths") or {}).get(key), f"output_paths.{key}")) for key in ("status", "jsonl", "stderr", "last_message")},
        Path(_text(raw.get("lane_event_log"), "lane_event_log")),
        tuple(profile["resources"]),
        None,
        _text(raw.get("resume_thread_id"), "resume_thread_id") if raw.get("resume_thread_id") is not None else None,
        label,
        _text(raw.get("task"), "task"),
        _text(raw.get("phase"), "phase"),
        "legacy-firmware",
        prompt_sha256,
    )


def adapt_invocation(raw: Mapping[str, Any]) -> CanonicalInvocation:
    """Select exactly one canonical, coding-v1, or schema-less firmware adapter."""

    schema = raw.get("schema") if isinstance(raw, Mapping) else None
    if schema == CANONICAL_INVOCATION_SCHEMA:
        return parse_canonical_invocation(raw)
    if schema == CODING_INVOCATION_SCHEMA:
        return adapt_coding_v1(raw)
    if schema is None:
        return adapt_legacy_firmware(raw)
    raise InvocationValidationError(f"unsupported invocation schema: {schema}")


WorkerInvocation = CanonicalInvocation
parse_invocation = adapt_invocation


__all__ = [
    "CANONICAL_INVOCATION_SCHEMA",
    "CODING_INVOCATION_SCHEMA",
    "CanonicalInvocation",
    "CODING_V1_ALLOWED_FIELDS",
    "CODING_V1_ALIAS_GROUPS",
    "CODING_V1_CANONICAL_ONLY_FIELDS",
    "CODING_V1_FIRMWARE_ONLY_FIELDS",
    "WorkerInvocation",
    "InvocationValidationError",
    "adapt_coding_v1",
    "adapt_invocation",
    "adapt_legacy_firmware",
    "parse_invocation",
    "parse_canonical_invocation",
    "validate_coding_v1_fields",
]
