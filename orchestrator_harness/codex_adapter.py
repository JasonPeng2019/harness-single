"""Implemented Codex host profile, safe-boundary fixtures, and owned installer.

Codex command hooks are synchronous lifecycle hooks.  The project-local assets
only inspect the already-bound coordinator at PostToolUse/Stop boundaries; the
persistent coordinator owns the wake subscription and replay.  No hook in this
module claims arbitrary file-change subscription or implicit project trust.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any, Mapping, Sequence

from .host_adapters import (
    AdapterCapabilities,
    DELIVERY_NOTICE_SCHEMA,
    DELIVERY_RECEIPT_SCHEMA,
    DeliveryCoordinator,
    DeliveryNotice,
    DeliveryReceipt,
    FutureHostFixture,
    HostAdapter,
    HostAdapterError,
    HostProfile,
    UnsupportedHostAdapterError,
)
from .models import iso_utc, parse_utc, utc_now
from .notifications import ManagerEventRouter
from .stable_io import canonical_json


CODEX_ADAPTER_SCHEMA = "orchestrator-codex-adapter/v1"
CODEX_INSTALL_MANIFEST_SCHEMA = "orchestrator-codex-installation/v1"
CODEX_ADAPTER_VERSION = "codex-v1"
CODEX_PACKAGE_REVISION = "codex-assets-v1"
INSTALL_MANIFEST_RELATIVE = Path(".codex") / "orchestrator-harness-adapter.json"
HOOKS_RELATIVE = Path(".codex") / "hooks.json"
_HOOK_RELATIVES = (
    Path(".codex") / "hooks" / "orchestrator_harness_post_tool_use.py",
    Path(".codex") / "hooks" / "orchestrator_harness_stop.py",
)
_HOOK_EVENT_NAMES = ("PostToolUse", "Stop")
_MAX_MANIFEST_BYTES = 512_000


class CodexAdapterError(ValueError):
    """A Codex boundary or installation transaction failed closed."""


class CodexInstallConflict(CodexAdapterError):
    """An unmanaged or user-modified destination cannot be claimed safely."""


class CodexInstallRollback(CodexAdapterError):
    """The bounded installer transaction failed and was restored."""


class CodexTransport(ABC):
    """Small synthetic-friendly surface for documented Codex boundaries."""

    @abstractmethod
    def post_tool_result_context(self, notice: Mapping[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def inject_items(self, items: Sequence[Mapping[str, Any]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def turn_completed(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def start_turn(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def request_continuation(self) -> bool:
        raise NotImplementedError


class SyntheticCodexTransport(CodexTransport):
    """In-memory App Server/hook transport used by deterministic tests only."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.active_turn = False
        self.continuation_requested = False

    @staticmethod
    def _assert_sparse_notice(notice: Mapping[str, Any]) -> None:
        if notice.get("schema") != DELIVERY_NOTICE_SCHEMA:
            raise CodexAdapterError("synthetic transport received a non-notice payload")
        forbidden = {
            "event_id", "event_ids", "data", "payload", "raw_output", "source_event",
            "queue_records",
        }
        if forbidden.intersection(notice):
            raise CodexAdapterError("delivery notice contains event payload material")

    def post_tool_result_context(self, notice: Mapping[str, Any]) -> None:
        self._assert_sparse_notice(notice)
        self.calls.append({"method": "PostToolUse", "notice": dict(notice)})

    def inject_items(self, items: Sequence[Mapping[str, Any]]) -> None:
        if any(not isinstance(item, Mapping) for item in items):
            raise CodexAdapterError("synthetic injected items must be objects")
        self.calls.append({"method": "thread/inject_items", "items": [dict(item) for item in items]})

    def turn_completed(self) -> None:
        self.active_turn = False
        self.calls.append({"method": "turn/completed"})

    def start_turn(self) -> None:
        if self.active_turn:
            raise CodexAdapterError("synthetic transport cannot start an active turn")
        self.active_turn = True
        self.calls.append({"method": "turn/start"})

    def request_continuation(self) -> bool:
        self.continuation_requested = True
        self.calls.append({"method": "Stop", "continue": True})
        return True


class RecordingCodexTransport(SyntheticCodexTransport):
    """Alias with an explicit test-oriented name."""


class CodexAdapter(HostAdapter):
    """The implemented current host profile."""

    def __init__(self, transport: CodexTransport, coordinator: DeliveryCoordinator) -> None:
        if not isinstance(transport, CodexTransport):
            raise CodexAdapterError("CodexAdapter requires a CodexTransport")
        if coordinator.adapter is not self:
            # The coordinator may be constructed before the adapter.  The
            # identity check is intentionally relaxed for that construction
            # order; binding and profile checks still happen at delivery time.
            coordinator.adapter = self
        self.transport = transport
        self.coordinator = coordinator
        self._profile = HostProfile(
            kind="codex",
            version=CODEX_ADAPTER_VERSION,
            capabilities=AdapterCapabilities.codex(),
            implemented=True,
        )

    @property
    def profile(self) -> HostProfile:
        return self._profile

    def deliver_notice(self, notice: DeliveryNotice, *, boundary: str) -> DeliveryReceipt:
        if notice.adapter_profile != self.profile.profile_id:
            raise CodexAdapterError("notice was created for another adapter profile")
        if boundary in {"post_tool_use", "tool_result"}:
            self.transport.post_tool_result_context(notice.as_record())
        elif boundary in {"turn_completed", "idle"}:
            self.transport.inject_items([
                {"type": "orchestrator_delivery_notice", "notice": notice.as_record()}
            ])
        elif boundary == "finalization":
            self.transport.inject_items([
                {"type": "orchestrator_delivery_notice", "notice": notice.as_record()}
            ])
        else:
            raise CodexAdapterError(f"unsupported Codex safe boundary: {boundary}")
        return DeliveryReceipt(
            receipt_id="codex-receipt-" + uuid.uuid4().hex,
            notice_id=notice.notice_id,
            run_id=notice.run_id,
            queue_id=notice.queue_id,
            manager_session_id=notice.manager_session_id,
            manager_thread_id=notice.manager_thread_id,
            registration_id=notice.registration_id,
            registration_generation=notice.registration_generation,
            observed_queue_revision=notice.observed_queue_revision,
            boundary=boundary,
            outcome="DELIVERED",
            delivered_utc=iso_utc(utc_now()) or notice.observed_utc,
            adapter_profile=self.profile.profile_id,
        )

    def post_tool_use(self, *, task_label: str = "tool") -> DeliveryReceipt | None:
        """Complete the current bounded tool before exposing a queue notice."""
        if self.coordinator.task_active:
            self.coordinator.complete_bounded_task(task_label)
        notice = self.coordinator.notice_for_wake()
        return self.coordinator.deliver_at_boundary(notice, boundary="post_tool_use")

    on_post_tool_use = post_tool_use
    tool_result_boundary = post_tool_use

    def turn_completed(self, *, task_label: str = "turn") -> DeliveryReceipt | None:
        """Inject one bounded next-request notice only after turn completion."""
        if self.coordinator.task_active:
            self.coordinator.complete_bounded_task(task_label)
        self.transport.turn_completed()
        notice = self.coordinator.notice_for_wake()
        receipt = self.coordinator.deliver_at_boundary(notice, boundary="turn_completed")
        if notice is not None and receipt is not None and receipt.outcome == "DELIVERED":
            self.transport.start_turn()
        return receipt

    on_turn_completed = turn_completed
    idle_boundary = turn_completed

    def stop_boundary(self) -> bool:
        """Use the Stop hook only as a finalization continuation backstop."""
        if self.coordinator.task_active:
            return False
        notice = self.coordinator.notice_for_wake()
        if notice is None:
            return False
        receipt = self.coordinator.deliver_at_boundary(notice, boundary="finalization")
        if receipt is None or receipt.outcome != "DELIVERED":
            return False
        return self.transport.request_continuation()

    on_stop = stop_boundary
    finalization_gate = stop_boundary

    def synthetic_self_test(self) -> dict[str, Any]:
        return run_synthetic_wake_self_test(self.coordinator)


def codex_profile() -> HostProfile:
    return HostProfile(
        kind="codex",
        version=CODEX_ADAPTER_VERSION,
        capabilities=AdapterCapabilities.codex(),
        implemented=True,
    )


def create_codex_adapter(
    router: ManagerEventRouter,
    *,
    transport: CodexTransport | None = None,
    state_root: str | Path | None = None,
    registration_generation: int | None = None,
) -> CodexAdapter:
    """Compose the implemented Codex profile with one exact S3 binding."""

    placeholder = FutureHostFixture("codex-bootstrap")
    coordinator = DeliveryCoordinator(
        router=router,
        adapter=placeholder,
        state_root=Path(state_root) if state_root is not None else None,
        registration_generation=registration_generation,
    )
    adapter = CodexAdapter(transport or SyntheticCodexTransport(), coordinator)
    return adapter


def select_host_adapter(
    kind: str,
    router: ManagerEventRouter,
    *,
    transport: CodexTransport | None = None,
    state_root: str | Path | None = None,
    registration_generation: int | None = None,
) -> HostAdapter:
    """Select by capability profile; unsupported hosts are honest fixtures."""

    if kind == "codex":
        return create_codex_adapter(
            router,
            transport=transport,
            state_root=state_root,
            registration_generation=registration_generation,
        )
    return FutureHostFixture(kind)


def _package_resource(name: str) -> bytes:
    try:
        return resources.files("orchestrator_harness.assets.codex").joinpath(name).read_bytes()
    except (FileNotFoundError, ModuleNotFoundError, OSError) as exc:
        raise CodexAdapterError(f"packaged Codex asset is unavailable: {name}") from exc


def packaged_codex_manifest() -> dict[str, Any]:
    try:
        raw = json.loads(_package_resource("manifest.json").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexAdapterError("packaged Codex manifest is invalid") from exc
    if not isinstance(raw, dict) or raw.get("schema") != CODEX_ADAPTER_SCHEMA:
        raise CodexAdapterError("packaged Codex manifest has an invalid schema")
    if raw.get("package_revision") != CODEX_PACKAGE_REVISION:
        raise CodexAdapterError("packaged Codex manifest revision is inconsistent")
    files = raw.get("files")
    if not isinstance(files, list) or not files:
        raise CodexAdapterError("packaged Codex manifest has no assets")
    return raw


def packaged_codex_assets() -> dict[Path, bytes]:
    manifest = packaged_codex_manifest()
    assets: dict[Path, bytes] = {}
    for item in manifest["files"]:
        if not isinstance(item, Mapping):
            raise CodexAdapterError("packaged Codex asset entry is invalid")
        relative = Path(str(item.get("destination", "")))
        resource_name = item.get("resource")
        if (
            not str(relative)
            or relative.is_absolute()
            or ".." in relative.parts
            or not isinstance(resource_name, str)
            or not resource_name
        ):
            raise CodexAdapterError("packaged Codex asset destination is unsafe")
        data = _package_resource(resource_name)
        expected = item.get("sha256")
        actual = hashlib.sha256(data).hexdigest()
        if expected != actual:
            raise CodexAdapterError(f"packaged Codex asset hash mismatch: {relative}")
        assets[relative] = data
    return assets


def _project_root(value: str | Path, *, prepare_codex: bool = False) -> Path:
    original = Path(value).expanduser()
    if original.exists() and original.is_symlink():
        raise CodexAdapterError("project-root must be an existing regular directory")
    path = original.resolve(strict=False)
    if not path.is_dir():
        raise CodexAdapterError("project-root must be an existing regular directory")
    codex = path / ".codex"
    if not codex.exists() and not prepare_codex:
        return path
    if codex.exists() and (codex.is_symlink() or not codex.is_dir()):
        raise CodexAdapterError("project .codex must be a regular directory")
    codex.mkdir(exist_ok=True)
    hooks = codex / "hooks"
    if hooks.exists() and (hooks.is_symlink() or not hooks.is_dir()):
        raise CodexAdapterError("project .codex/hooks must be a regular directory")
    hooks.mkdir(exist_ok=True)
    return path


def _safe_project_relative(project: Path, relative: Path) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise CodexAdapterError("installer destination is not project-relative")
    result = (project / relative).resolve(strict=False)
    try:
        result.relative_to(project.resolve())
    except ValueError as exc:
        raise CodexAdapterError("installer destination escapes the project") from exc
    return result


def _read_bytes(path: Path) -> bytes | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise CodexInstallConflict(f"managed destination is not a regular file: {path}")
    data = path.read_bytes()
    if len(data) > _MAX_MANIFEST_BYTES:
        raise CodexInstallConflict(f"managed destination is oversized: {path}")
    return data


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _manifest_content_digest(value: Mapping[str, Any]) -> str:
    normalized = dict(value)
    normalized["manifest_content_sha256"] = ""
    return hashlib.sha256(_json_bytes(normalized)).hexdigest()


def _hash(data: bytes | None) -> str | None:
    return hashlib.sha256(data).hexdigest() if data is not None else None


def _relative_text(path: Path, project: Path) -> str:
    return path.relative_to(project).as_posix()


def _atomic_replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        descriptor, raw_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        temporary = Path(raw_name)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def _load_install_manifest(path: Path) -> dict[str, Any] | None:
    data = _read_bytes(path)
    if data is None:
        return None
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict("installation manifest is malformed") from exc
    if not isinstance(value, dict) or value.get("schema") != CODEX_INSTALL_MANIFEST_SCHEMA:
        raise CodexInstallConflict("installation manifest is not owned by this adapter")
    return value


def _manifest_is_unchanged(path: Path, manifest: Mapping[str, Any]) -> bool:
    """Prove that the owned manifest still has its canonical installed bytes."""

    try:
        data = _read_bytes(path)
        return (
            data is not None
            and data == _json_bytes(manifest)
            and _manifest_content_digest(manifest) == manifest.get("manifest_content_sha256")
        )
    except (CodexAdapterError, TypeError, ValueError):
        return False


def _validate_manifest_identity(manifest: Mapping[str, Any], project: Path) -> None:
    if manifest.get("adapter") != "codex":
        raise CodexInstallConflict("installation manifest belongs to another host")
    if manifest.get("project_root") != str(project):
        raise CodexInstallConflict("installation manifest project identity differs")
    owned = manifest.get("owned_paths")
    if not isinstance(owned, list) or any(not isinstance(item, str) for item in owned):
        raise CodexInstallConflict("installation manifest ownership is invalid")


def _managed_hook_fragment() -> dict[str, list[dict[str, str]]]:
    raw = json.loads(_package_resource("hooks.fragment.json").decode("utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("hooks"), dict):
        raise CodexAdapterError("packaged Codex hook fragment is invalid")
    result: dict[str, list[dict[str, str]]] = {}
    for event_name in _HOOK_EVENT_NAMES:
        entries = raw["hooks"].get(event_name)
        if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
            raise CodexAdapterError("packaged Codex hook entries are invalid")
        result[event_name] = [dict(item) for item in entries]
    return result


def _merge_hooks(existing: bytes | None) -> bytes:
    if existing is None:
        value: dict[str, Any] = {"hooks": {}}
    else:
        try:
            value = json.loads(existing.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CodexInstallConflict(".codex/hooks.json is not valid JSON") from exc
        if not isinstance(value, dict) or ("hooks" in value and not isinstance(value["hooks"], dict)):
            raise CodexInstallConflict(".codex/hooks.json has an unsupported shape")
        value = dict(value)
        value.setdefault("hooks", {})
    hooks = dict(value["hooks"])
    fragment = _managed_hook_fragment()
    for event_name, entries in fragment.items():
        prior = hooks.get(event_name, [])
        if not isinstance(prior, list):
            raise CodexInstallConflict(f".codex/hooks.json {event_name} is not a list")
        merged = [item for item in prior if isinstance(item, dict)]
        if len(merged) != len(prior):
            raise CodexInstallConflict(f".codex/hooks.json {event_name} contains invalid entries")
        for entry in entries:
            marker = entry.get("id")
            clashes = [item for item in merged if item.get("id") == marker]
            if clashes and any(item != entry for item in clashes):
                raise CodexInstallConflict(f".codex/hooks.json contains an ambiguous owned hook: {marker}")
            if not clashes:
                merged.append(entry)
        hooks[event_name] = merged
    value["hooks"] = hooks
    return _json_bytes(value)


def _prior_entry(data: bytes | None) -> dict[str, Any]:
    return {
        "present": data is not None,
        "sha256": _hash(data),
        "content_b64": base64.b64encode(data).decode("ascii") if data is not None else None,
    }


def _restore_bytes(entry: Mapping[str, Any]) -> bytes | None:
    if entry.get("present") is not True:
        return None
    encoded = entry.get("content_b64")
    if not isinstance(encoded, str):
        raise CodexInstallConflict("installation manifest prior content is incomplete")
    try:
        data = base64.b64decode(encoded.encode("ascii"), validate=True)
    except (ValueError, UnicodeEncodeError) as exc:
        raise CodexInstallConflict("installation manifest prior content is invalid") from exc
    if _hash(data) != entry.get("sha256"):
        raise CodexInstallConflict("installation manifest prior content hash is invalid")
    return data


def _current_hashes(project: Path, paths: Sequence[Path]) -> dict[str, str | None]:
    return {str(path.relative_to(project)): _hash(_read_bytes(path)) for path in paths}


def check_codex_adapter(project_root: str | Path) -> dict[str, Any]:
    project = _project_root(project_root)
    manifest_path = _safe_project_relative(project, INSTALL_MANIFEST_RELATIVE)
    manifest = _load_install_manifest(manifest_path)
    result: dict[str, Any] = {
        "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
        "adapter": "codex",
        "adapter_version": CODEX_ADAPTER_VERSION,
        "package_revision": CODEX_PACKAGE_REVISION,
        "project_root": str(project),
        "installed": manifest is not None,
        "current": False,
        "owned_paths": [],
        "project_trust": "unverified",
        "synthetic_self_test": "not_run",
        "hook_review": "not-reviewed",
        "global_install": False,
    }
    if manifest is None:
        return result
    _validate_manifest_identity(manifest, project)
    manifest_unchanged = _manifest_is_unchanged(manifest_path, manifest)
    result["synthetic_self_test"] = (manifest.get("trust") or {}).get("synthetic_self_test", "not_run")
    result["hook_review"] = (manifest.get("trust") or {}).get("hook_review", "not-reviewed")
    owned = manifest.get("owned_paths", [])
    installed_hashes = manifest.get("installed_content_sha256", {})
    current = True
    rows: list[dict[str, Any]] = []
    for relative in owned:
        path = _safe_project_relative(project, Path(relative))
        data = _read_bytes(path)
        if relative == INSTALL_MANIFEST_RELATIVE.as_posix():
            actual = _manifest_content_digest(manifest) if manifest_unchanged else None
            expected = manifest.get("manifest_content_sha256")
        else:
            actual = _hash(data)
            expected = installed_hashes.get(relative) if isinstance(installed_hashes, Mapping) else None
        same = actual == expected
        if not same:
            current = False
        rows.append({"path": relative, "installed_sha256": expected, "current_sha256": actual, "unchanged": same})
    result["owned_paths"] = rows
    result["current"] = current
    result["manifest_current"] = manifest_unchanged
    result["manifest_revision"] = manifest.get("package_revision")
    return result


def install_codex_adapter(project_root: str | Path, *, upgrade: bool = False) -> dict[str, Any]:
    project = _project_root(project_root, prepare_codex=True)
    packaged = packaged_codex_assets()
    manifest_path = _safe_project_relative(project, INSTALL_MANIFEST_RELATIVE)
    hooks_path = _safe_project_relative(project, HOOKS_RELATIVE)
    existing_manifest = _load_install_manifest(manifest_path)
    if existing_manifest is not None:
        _validate_manifest_identity(existing_manifest, project)
        if not _manifest_is_unchanged(manifest_path, existing_manifest):
            raise CodexInstallConflict("owned installation manifest was modified; refusing overwrite")
        current = check_codex_adapter(project)
        if current["current"] and existing_manifest.get("package_revision") == CODEX_PACKAGE_REVISION:
            current["operation"] = "upgrade" if upgrade else "install"
            current["changed"] = []
            current["idempotent"] = True
            return current
        if not upgrade and existing_manifest.get("package_revision") != CODEX_PACKAGE_REVISION:
            raise CodexInstallConflict("an older owned Codex adapter requires explicit upgrade")
        for relative in existing_manifest.get("owned_paths", []):
            path = _safe_project_relative(project, Path(relative))
            expected = (existing_manifest.get("installed_content_sha256") or {}).get(relative)
            if relative != INSTALL_MANIFEST_RELATIVE.as_posix() and _hash(_read_bytes(path)) != expected:
                raise CodexInstallConflict(f"owned file was modified; refusing overwrite: {relative}")
    else:
        # A foreign manifest or a pre-existing managed hook is an ambiguity, not
        # permission to overwrite.  _load_install_manifest already rejects a
        # foreign manifest with a closed error; here we handle the hook paths.
        if manifest_path.exists():
            raise CodexInstallConflict(".codex/orchestrator-harness-adapter.json is not owned by Codex adapter")
        for relative in _HOOK_RELATIVES:
            path = _safe_project_relative(project, relative)
            if path.exists():
                raise CodexInstallConflict(f"unmanaged hook destination already exists: {relative}")

    target_paths = [_safe_project_relative(project, relative) for relative in packaged]
    target_paths.append(hooks_path)
    target_paths.append(manifest_path)
    originals = {path: _read_bytes(path) for path in target_paths}
    prior: dict[str, Any] = {}
    if existing_manifest is not None and isinstance(existing_manifest.get("prior_content"), Mapping):
        prior.update({str(key): value for key, value in existing_manifest["prior_content"].items()})
    for path, data in originals.items():
        relative = _relative_text(path, project)
        prior.setdefault(relative, _prior_entry(data))
    changed: list[str] = []
    try:
        for relative, data in packaged.items():
            path = _safe_project_relative(project, relative)
            if originals[path] != data:
                _atomic_replace(path, data)
                changed.append(str(relative))
        hooks_data = _merge_hooks(originals[hooks_path])
        if originals[hooks_path] != hooks_data:
            _atomic_replace(hooks_path, hooks_data)
            changed.append(HOOKS_RELATIVE.as_posix())
        installed_hashes: dict[str, str | None] = {}
        for relative, value in packaged.items():
            path = _safe_project_relative(project, relative)
            installed_hashes[_relative_text(path, project)] = _hash(value)
        installed_hashes[HOOKS_RELATIVE.as_posix()] = _hash(hooks_data)
        manifest = {
            "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
            "adapter": "codex",
            "adapter_version": CODEX_ADAPTER_VERSION,
            "package_revision": CODEX_PACKAGE_REVISION,
            "project_root": str(project),
            "owned_paths": sorted([*installed_hashes, INSTALL_MANIFEST_RELATIVE.as_posix()]),
            "prior_managed_revision": existing_manifest.get("package_revision") if existing_manifest else None,
            "prior_content": prior,
            "installed_content_sha256": installed_hashes,
            "trust": {
                "project_layer": "unverified",
                "synthetic_self_test": "not_run",
                "hook_review": "not-reviewed",
            },
            "installed_utc": iso_utc(utc_now()),
        }
        manifest["manifest_content_sha256"] = _manifest_content_digest(manifest)
        _atomic_replace(manifest_path, _json_bytes(manifest))
        changed.append(INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for path, data in originals.items():
            try:
                if data is None:
                    path.unlink(missing_ok=True)
                else:
                    _atomic_replace(path, data)
            except Exception as rollback_exc:  # pragma: no cover - defensive evidence
                rollback_errors.append(f"{path}: {type(rollback_exc).__name__}")
        detail = f"Codex install rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise CodexInstallRollback(detail) from exc
    result = check_codex_adapter(project)
    result.update({"operation": "upgrade" if upgrade else "install", "changed": changed, "idempotent": not bool(changed)})
    return result


def upgrade_codex_adapter(project_root: str | Path) -> dict[str, Any]:
    return install_codex_adapter(project_root, upgrade=True)


def uninstall_codex_adapter(project_root: str | Path) -> dict[str, Any]:
    project = _project_root(project_root)
    manifest_path = _safe_project_relative(project, INSTALL_MANIFEST_RELATIVE)
    manifest = _load_install_manifest(manifest_path)
    if manifest is None:
        return {
            "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
            "adapter": "codex",
            "project_root": str(project),
            "operation": "uninstall",
            "installed": False,
            "removed": [],
            "preserved_modified": [],
            "project_trust": "unverified",
        }
    _validate_manifest_identity(manifest, project)
    installed_hashes = manifest.get("installed_content_sha256")
    prior_content = manifest.get("prior_content")
    if not isinstance(installed_hashes, Mapping) or not isinstance(prior_content, Mapping):
        raise CodexInstallConflict("installation manifest lacks ownership evidence")
    removed: list[str] = []
    preserved: list[str] = []
    manifest_current = _manifest_is_unchanged(manifest_path, manifest)
    for relative in manifest.get("owned_paths", []):
        path = _safe_project_relative(project, Path(relative))
        actual = _hash(_read_bytes(path))
        expected = installed_hashes.get(relative)
        if relative == INSTALL_MANIFEST_RELATIVE.as_posix():
            # The manifest itself is handled last and is removed only when its
            # managed content is still unchanged.
            if manifest_current:
                path.unlink(missing_ok=True)
                removed.append(relative)
            else:
                preserved.append(relative)
            continue
        if actual != expected:
            preserved.append(relative)
            continue
        prior = _restore_bytes(prior_content.get(relative, {"present": False}))
        if prior is None:
            path.unlink(missing_ok=True)
        else:
            _atomic_replace(path, prior)
        removed.append(relative)
    return {
        "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
        "adapter": "codex",
        "project_root": str(project),
        "operation": "uninstall",
        "installed": True,
        "removed": sorted(removed),
        "preserved_modified": sorted(preserved),
        "project_trust": "unverified",
        "ownership_safe": True,
    }


def _set_self_test_state(project: Path, outcome: str) -> None:
    manifest_path = _safe_project_relative(project, INSTALL_MANIFEST_RELATIVE)
    manifest = _load_install_manifest(manifest_path)
    if manifest is None:
        raise CodexInstallConflict("synthetic self-test requires an installed adapter")
    current = check_codex_adapter(project)
    if not current.get("current"):
        raise CodexInstallConflict("synthetic self-test refuses a modified installation")
    trust = dict(manifest.get("trust") or {})
    trust["synthetic_self_test"] = outcome
    manifest["trust"] = trust
    manifest["manifest_content_sha256"] = _manifest_content_digest(manifest)
    _atomic_replace(manifest_path, _json_bytes(manifest))


def run_synthetic_wake_self_test(coordinator: DeliveryCoordinator) -> dict[str, Any]:
    """Exercise one queue revision using only a synthetic Codex transport."""
    if not isinstance(coordinator.adapter, CodexAdapter):
        raise CodexAdapterError("wake self-test requires the implemented Codex adapter")
    coordinator.register()
    binding = coordinator.binding
    event_id = "s4-synthetic-wake-" + uuid.uuid4().hex
    event = {
        "event_id": event_id,
        "type": "MANAGER_SIGNAL",
        "identity": "synthetic:s4:wake",
        "data": {
            "signal_id": event_id,
            "lane_id": "synthetic:s4",
            "manager_actionable": True,
            "severity": "warning",
        },
        "binding": binding,
    }
    admitted = coordinator.router.admit(event, priority=2, binding=binding)
    if admitted is None:
        raise CodexAdapterError("synthetic wake event was not admitted")
    notice = coordinator.notice_for_wake()
    if notice is None:
        raise CodexAdapterError("synthetic queue revision did not produce a notice")
    notice_record = notice.as_record()
    if any(key in notice_record for key in ("event_id", "event_ids", "payload", "data")):
        raise CodexAdapterError("synthetic notice contains event payload")
    receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
    if receipt is None or receipt.outcome != "DELIVERED":
        raise CodexAdapterError("synthetic Codex delivery did not complete")
    pending_after = coordinator.router.pending_events()
    if event_id not in {item.get("event_id") for item in pending_after}:
        raise CodexAdapterError("synthetic transport receipt acknowledged queue work")
    return {
        "schema": "orchestrator-codex-synthetic-wake/v1",
        "adapter_profile": coordinator.adapter.profile.as_record(),
        "notice": notice_record,
        "receipt": receipt.as_record(),
        "pending_event_ids_for_test": [event_id],
        "pending_after_delivery": len(pending_after),
        "acknowledged_by_delivery": False,
        "transport_calls": list(getattr(coordinator.adapter.transport, "calls", [])),
    }


def synthetic_wake_self_test(project_root: str | Path, *, queue_root: str | Path | None = None) -> dict[str, Any]:
    project = _project_root(project_root)
    if queue_root is None:
        queue = project / ".codex" / ".synthetic-manager-binding"
    else:
        queue = Path(queue_root).expanduser().resolve(strict=False)
    router = ManagerEventRouter(
        queue,
        run_id="synthetic-s4-run",
        manager_session_id="synthetic-s4-session",
        manager_thread_id="synthetic-s4-thread",
        registration_id="synthetic-s4-registration",
    )
    transport = SyntheticCodexTransport()
    # Construct a small adapter shell first, then bind the coordinator to its
    # exact profile.  The public constructor also supports direct composition.
    coordinator = DeliveryCoordinator(router=router, adapter=FutureHostFixture("codex-bootstrap"))
    adapter = CodexAdapter(transport, coordinator)
    coordinator.adapter = adapter
    evidence = run_synthetic_wake_self_test(coordinator)
    _set_self_test_state(project, "passed")
    evidence["project_root"] = str(project)
    evidence["project_trust"] = "unverified"
    evidence["hook_review"] = "not-reviewed"
    return evidence


def run_codex_hook(boundary: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Validate a synchronous project hook input without claiming a subscription."""
    if boundary not in {"post_tool_use", "stop"}:
        raise CodexAdapterError("supported Codex hooks are post_tool_use and stop")
    if payload is not None and not isinstance(payload, Mapping):
        raise CodexAdapterError("hook payload must be an object")
    return {
        "schema": "orchestrator-codex-hook-result/v1",
        "boundary": boundary,
        "synchronous": True,
        "persistent_subscription_owner": "harness-delivery-coordinator",
        "project_trust": "unverified",
    }


__all__ = [
    "CODEX_ADAPTER_SCHEMA",
    "CODEX_ADAPTER_VERSION",
    "CODEX_INSTALL_MANIFEST_SCHEMA",
    "CODEX_PACKAGE_REVISION",
    "CodexAdapter",
    "CodexAdapterError",
    "CodexInstallConflict",
    "CodexInstallRollback",
    "CodexTransport",
    "RecordingCodexTransport",
    "SyntheticCodexTransport",
    "check_codex_adapter",
    "codex_profile",
    "create_codex_adapter",
    "install_codex_adapter",
    "packaged_codex_assets",
    "packaged_codex_manifest",
    "run_codex_hook",
    "run_synthetic_wake_self_test",
    "select_host_adapter",
    "synthetic_wake_self_test",
    "uninstall_codex_adapter",
    "upgrade_codex_adapter",
]
