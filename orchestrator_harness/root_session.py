"""One prepared logical ROOT scope shared by hooks and short-lived commands."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import ConfigError, HarnessConfig, compute_config_identity, load_resource_manifest, path_identity
from .core import content_hash, iso_utc, new_id
from .records import RecordLock, atomic_write_json, read_record, runtime_admission_lock

ROOT_SESSION_SCHEMA = "root-session/v1"
_FIELDS = frozenset({"schema", "run_id", "session_id", "provider_id", "native_session_id",
                     "config_identity", "manager_workspace", "root_workspace", "runtime_root",
                     "prepared_at", "content_hash"})
_PROVIDERS = frozenset({"codex", "claude-code", "qwen-code"})


def root_session_path(config: HarnessConfig) -> Path:
    if config.manager_workspace is None:
        raise ConfigError("ROOT_SESSION_REQUIRED: configure and explicitly prepare ROOT")
    return config.manager_workspace / ".agent-workspace" / "ROOT_SESSION.json"


def read_root_session(config: HarnessConfig) -> dict[str, Any]:
    """Read a current prepared binding; missing/unknown records require preparation."""
    path = root_session_path(config)
    try:
        record = read_record(path, ROOT_SESSION_SCHEMA)
    except FileNotFoundError as exc:
        raise ConfigError("ROOT_SESSION_REQUIRED: explicitly prepare this managed ROOT before ACK/watch") from exc
    except (OSError, ValueError) as exc:
        raise ConfigError(f"ROOT_SESSION_INVALID: preserve and reconcile the prepared binding: {exc}") from exc
    if (set(record) != _FIELDS or record.get("content_hash") != content_hash(record)
            or any(not isinstance(record.get(key), str) or not record[key]
                   for key in ("run_id", "session_id", "prepared_at"))
            or not isinstance(record.get("provider_id"), str)
            or record["provider_id"] not in _PROVIDERS
            or (record.get("native_session_id") is not None
                and (not isinstance(record["native_session_id"], str) or not record["native_session_id"]))):
        raise ConfigError("ROOT_SESSION_INVALID: prepared binding shape or digest is invalid")
    try:
        prepared = datetime.fromisoformat(record["prepared_at"].replace("Z", "+00:00"))
        if prepared.tzinfo is None or prepared.utcoffset().total_seconds() != 0 or prepared > datetime.now(timezone.utc):
            raise ValueError("invalid preparation timestamp")
        for key in ("manager_workspace", "root_workspace", "runtime_root"):
            if not isinstance(record.get(key), str) or not Path(record[key]).is_absolute():
                raise ValueError(f"invalid {key}")
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"ROOT_SESSION_INVALID: {exc}") from exc
    identity = compute_config_identity(config, load_resource_manifest(config.harness_root))
    expected = {"manager_workspace": path_identity(config.manager_workspace),
                "root_workspace": path_identity(config.root_workspace),
                "runtime_root": path_identity(config.runtime_root), "config_identity": identity}
    if any(record.get(key) != value for key, value in expected.items()):
        raise ConfigError("ROOT_SESSION_STALE: manager/product/configuration changed; explicitly prepare ROOT")
    return record


def publish_root_session(config: HarnessConfig, *, run_id: str, provider_id: str,
                         native_session_id: str | None = None) -> dict[str, Any]:
    """Publish once after readiness, or reuse the exact same prepared run."""
    if (not isinstance(run_id, str) or not run_id.strip()
            or not isinstance(provider_id, str) or provider_id not in _PROVIDERS
            or (native_session_id is not None and (not isinstance(native_session_id, str) or not native_session_id))):
        raise ConfigError("ROOT_SESSION_INVALID: preparation requires a run and registered provider")
    path = root_session_path(config)
    with runtime_admission_lock(config.runtime_root):
        from .setup import require_monitor_ready
        identity = compute_config_identity(config, load_resource_manifest(config.harness_root))
        # Admission excludes lifecycle changes. Release the monitor record lock
        # before acquiring the session record lock, including on reuse.
        require_monitor_ready(config.runtime_root, identity)
        return _publish_ready_session(config, path, run_id, provider_id, native_session_id, identity)


def _publish_ready_session(config: HarnessConfig, path: Path, run_id: str,
                           provider_id: str, native_session_id: str | None,
                           identity: str) -> dict[str, Any]:
    with RecordLock(path):
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        else:
            existing = read_root_session(config)
            if existing["run_id"] != run_id or existing["provider_id"] != provider_id:
                raise ConfigError("ROOT_SESSION_STALE: preserve the prior preparation before a fresh ROOT run")
            if native_session_id is not None and existing["native_session_id"] != native_session_id:
                raise ConfigError("ROOT_SESSION_MISMATCH: native binding requires explicit one-time binding")
            return existing
        record = {"schema": ROOT_SESSION_SCHEMA, "run_id": run_id, "session_id": new_id(),
                  "provider_id": provider_id, "native_session_id": native_session_id,
                  "config_identity": identity, "manager_workspace": path_identity(config.manager_workspace),
                  "root_workspace": path_identity(config.root_workspace),
                  "runtime_root": path_identity(config.runtime_root), "prepared_at": iso_utc()}
        record["content_hash"] = content_hash(record)
        atomic_write_json(path, record)
        return record


def resolve_root_session(config: HarnessConfig, *, session_id: str | None = None,
                         binding_id: str | None = None, provider_id: str | None = None) -> tuple[str, str]:
    """Resolve the prepared scope; never derive it from a command's PID."""
    record = read_root_session(config)
    explicit = session_id
    if explicit is None:
        explicit = os.environ.get("HARNESS_ROOT_SESSION_ID", os.environ.get("HARNESS_SESSION_ID"))
    binding = binding_id if binding_id is not None else os.environ.get("HARNESS_BINDING_ID")
    if ((explicit is not None and explicit != record["session_id"])
            or (binding is not None and binding != record["provider_id"])
            or (provider_id is not None and provider_id != record["provider_id"])):
        raise ConfigError("ROOT_SESSION_MISMATCH: explicit ROOT session/provider differs from prepared binding")
    native_name = {"codex": "CODEX_THREAD_ID", "claude-code": "CLAUDE_SESSION_ID", "qwen-code": "QWEN_SESSION_ID"}[record["provider_id"]]
    native = os.environ.get(native_name)
    if native and record["native_session_id"] is not None and native != record["native_session_id"]:
        raise ConfigError("ROOT_SESSION_MISMATCH: native provider session differs from prepared binding")
    return record["session_id"], record["provider_id"]


def bind_native_root_session(config: HarnessConfig, *, run_id: str, session_id: str,
                             native_session_id: str) -> dict[str, Any]:
    """Bind an observed owned native thread once without changing logical scope."""
    if not isinstance(native_session_id, str) or not native_session_id:
        raise ConfigError("ROOT_SESSION_INVALID: native session is empty")
    with runtime_admission_lock(config.runtime_root), RecordLock(root_session_path(config)):
        record = read_root_session(config)
        if record["run_id"] != run_id or record["session_id"] != session_id:
            raise ConfigError("ROOT_SESSION_MISMATCH: native observation belongs to another ROOT run")
        if record["native_session_id"] not in (None, native_session_id):
            raise ConfigError("ROOT_SESSION_MISMATCH: native observation cannot replace a known thread")
        if record["native_session_id"] is None:
            record["native_session_id"] = native_session_id
            record["content_hash"] = content_hash(record)
            atomic_write_json(root_session_path(config), record)
        return record
