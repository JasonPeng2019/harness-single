"""Implemented Codex host profile, safe-boundary fixtures, and owned installer.

Codex command hooks are synchronous lifecycle hooks.  The project-local assets
only inspect the already-bound coordinator at PostToolUse/Stop boundaries; the
persistent coordinator owns the wake subscription and replay.  No hook in this
module claims arbitrary file-change subscription or implicit project trust.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
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
from .mutation import (
    MutationConflict,
    MutationReceipt,
    MutationUnsupported,
    TargetState,
    capture_target,
    delete as mutation_delete,
    ensure_directory_path,
    replace as mutation_replace,
)
from .notifications import ManagerEventRouter
from .stable_io import canonical_json


CODEX_ADAPTER_SCHEMA = "orchestrator-codex-adapter/v1"
CODEX_INSTALL_MANIFEST_SCHEMA = "orchestrator-codex-installation/v1"
CODEX_BINDING_SCHEMA = "orchestrator-codex-binding/v1"
CODEX_ADAPTER_VERSION = "codex-v1"
CODEX_PACKAGE_REVISION = "codex-assets-v2"
CODEX_SUPPORTED_LEGACY_REVISIONS = frozenset({"codex-assets-v1"})
INSTALL_MANIFEST_RELATIVE = Path(".codex") / "orchestrator-harness-adapter.json"
CODEX_BINDING_RELATIVE = Path(".codex") / "orchestrator-harness-binding.json"
HOOKS_RELATIVE = Path(".codex") / "hooks.json"
_HOOK_RELATIVES = (
    Path(".codex") / "hooks" / "orchestrator_harness_post_tool_use.py",
    Path(".codex") / "hooks" / "orchestrator_harness_stop.py",
)
_HOOK_EVENT_NAMES = ("PostToolUse", "Stop")
_MAX_MANIFEST_BYTES = 512_000
_MAX_BINDING_BYTES = 128_000
_MANAGED_HOOK_IDS = {
    "orchestrator-harness-post-tool-use",
    "orchestrator-harness-stop",
}


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


def _lexical_path(value: str | Path) -> Path:
    return Path(os.path.abspath(str(Path(value).expanduser())))


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _identity(path: Path) -> tuple[int, int]:
    info = path.lstat()
    return info.st_dev, info.st_ino


class _ProjectMutationGuard:
    """One no-follow, identity-revalidated boundary for project mutations."""

    def __init__(self, value: str | Path, *, prepare_codex: bool = False) -> None:
        project = _lexical_path(value)
        self.project = project
        self._validate_project_chain(project)
        if not project.is_dir() or _is_reparse(project):
            raise CodexAdapterError("project-root must be an existing regular directory")
        self._project_identity = _identity(project)
        self._last_receipts: dict[str, MutationReceipt] = {}
        if prepare_codex:
            self.ensure_directory(Path(".codex"))
            self.ensure_directory(Path(".codex") / "hooks")

    @staticmethod
    def _validate_project_chain(project: Path) -> None:
        current = Path(project.anchor)
        for part in project.parts[1:]:
            current = current / part
            if os.path.lexists(current) and _is_reparse(current):
                raise CodexAdapterError(f"project path contains a symlink or reparse point: {current}")

    def _check_project_identity(self) -> None:
        try:
            if _identity(self.project) != self._project_identity or _is_reparse(self.project):
                raise CodexAdapterError("project-root identity changed")
        except FileNotFoundError as exc:
            raise CodexAdapterError("project-root disappeared") from exc

    def path(self, relative: str | Path, *, require_parent: bool = False) -> Path:
        candidate = Path(relative)
        if candidate.is_absolute():
            try:
                candidate = candidate.relative_to(self.project)
            except ValueError as exc:
                raise CodexAdapterError("installer destination is outside the project") from exc
        if ".." in candidate.parts:
            raise CodexAdapterError("installer destination is not project-relative")
        self._check_project_identity()
        target = self.project / candidate
        current = self.project
        parts = candidate.parts
        for index, part in enumerate(parts):
            current = current / part
            if os.path.lexists(current):
                if _is_reparse(current):
                    raise CodexInstallConflict(f"project mutation component is a reparse point: {current}")
                if index < len(parts) - 1 and not current.is_dir():
                    raise CodexInstallConflict(f"project mutation parent is not a directory: {current}")
            elif index < len(parts) - 1 and require_parent:
                raise CodexInstallConflict(f"project mutation parent is missing: {current}")
        if require_parent:
            parent = target.parent
            if not parent.is_dir() or _is_reparse(parent):
                raise CodexInstallConflict(f"project mutation parent is unsafe: {parent}")
        return target

    def ensure_directory(self, relative: str | Path) -> Path:
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise CodexAdapterError("project directory is not project-relative")
        self._check_project_identity()
        try:
            result = ensure_directory_path(self.project / candidate)
        except (MutationConflict, MutationUnsupported) as exc:
            raise CodexInstallConflict(str(exc)) from exc
        self._check_project_identity()
        return result

    def _relative(self, relative_or_path: str | Path) -> Path:
        candidate = Path(relative_or_path)
        if candidate.is_absolute():
            try:
                candidate = candidate.relative_to(self.project)
            except ValueError as exc:
                raise CodexAdapterError("installer destination is outside the project") from exc
        if not candidate.parts or ".." in candidate.parts:
            raise CodexAdapterError("installer destination is not project-relative")
        return candidate

    def snapshot(self, relative_or_path: str | Path) -> TargetState:
        relative = self._relative(relative_or_path)
        self._check_project_identity()
        try:
            state = capture_target(self.project, relative)
        except (MutationConflict, MutationUnsupported) as exc:
            raise CodexInstallConflict(str(exc)) from exc
        if state.present and state.kind != "file":
            raise CodexInstallConflict(f"managed destination is not a regular file: {self.project / relative}")
        self._check_project_identity()
        return state

    def read(self, relative_or_path: str | Path) -> bytes | None:
        relative = self._relative(relative_or_path)
        state = self.snapshot(relative)
        if not state.present:
            return None
        if state.size is not None and state.size > _MAX_MANIFEST_BYTES:
            raise CodexInstallConflict(f"managed destination is oversized: {self.project / relative}")
        return state.content

    def atomic_replace(
        self,
        relative_or_path: str | Path,
        data: bytes,
        *,
        expected: TargetState | None = None,
    ) -> MutationReceipt:
        relative = self._relative(relative_or_path)
        self.path(relative, require_parent=True)
        authorized = expected if expected is not None else self.snapshot(relative)
        try:
            receipt = mutation_replace(self.project, relative, data, expected=authorized)
        except (MutationConflict, MutationUnsupported) as exc:
            raise CodexInstallConflict(str(exc)) from exc
        self._last_receipts[relative.as_posix()] = receipt
        self._check_project_identity()
        return receipt

    def delete(
        self,
        relative_or_path: str | Path,
        *,
        expected: TargetState | None = None,
    ) -> MutationReceipt:
        relative = self._relative(relative_or_path)
        self.path(relative, require_parent=True)
        authorized = expected if expected is not None else self.snapshot(relative)
        try:
            receipt = mutation_delete(self.project, relative, expected=authorized)
        except (MutationConflict, MutationUnsupported) as exc:
            raise CodexInstallConflict(str(exc)) from exc
        self._last_receipts[relative.as_posix()] = receipt
        self._check_project_identity()
        return receipt

    def restore(self, relative_or_path: str | Path, data: bytes | None) -> None:
        relative = self._relative(relative_or_path)
        prior = self._last_receipts.get(relative.as_posix())
        current = self.snapshot(relative)
        if prior is None:
            if (data is None and not current.present) or (data is not None and current.content == data):
                return
            raise CodexInstallConflict(f"rollback target changed outside this transaction: {relative}")
        expected = prior.resulting
        if data is None:
            if not expected.present:
                return
            self.delete(relative, expected=expected)
        else:
            self.atomic_replace(relative, data, expected=expected)


def _project_guard(value: str | Path, *, prepare_codex: bool = False) -> _ProjectMutationGuard:
    return _ProjectMutationGuard(value, prepare_codex=prepare_codex)


def _project_root(value: str | Path, *, prepare_codex: bool = False) -> Path:
    return _project_guard(value, prepare_codex=prepare_codex).project


def _safe_project_relative(project: Path, relative: Path) -> Path:
    return _project_guard(project).path(relative)


def _read_bytes(path: Path, guard: _ProjectMutationGuard | None = None) -> bytes | None:
    if guard is not None:
        return guard.read(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode) or _is_reparse(path) or not stat.S_ISREG(info.st_mode):
        raise CodexInstallConflict(f"managed destination is not a regular file: {path}")
    if info.st_size > _MAX_MANIFEST_BYTES:
        raise CodexInstallConflict(f"managed destination is oversized: {path}")
    return path.read_bytes()


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


def _atomic_replace(path: Path, data: bytes, *, guard: _ProjectMutationGuard | None = None) -> None:
    if guard is None:
        raise CodexInstallConflict("project mutation requires an identity-bound guard")
    guard.atomic_replace(path, data)


def _load_install_manifest(path: Path, *, guard: _ProjectMutationGuard | None = None) -> dict[str, Any] | None:
    data = _read_bytes(path, guard)
    if data is None:
        return None
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict("installation manifest is malformed") from exc
    if not isinstance(value, dict) or value.get("schema") != CODEX_INSTALL_MANIFEST_SCHEMA:
        raise CodexInstallConflict("installation manifest is not owned by this adapter")
    return value


def _manifest_is_unchanged(
    path: Path,
    manifest: Mapping[str, Any],
    *,
    guard: _ProjectMutationGuard | None = None,
) -> bool:
    """Prove that the owned manifest still has its canonical installed bytes."""

    try:
        data = _read_bytes(path, guard)
        return (
            data is not None
            and data == _json_bytes(manifest)
            and _manifest_content_digest(manifest) == manifest.get("manifest_content_sha256")
        )
    except (CodexAdapterError, TypeError, ValueError):
        return False


def _fragment_digest(fragment: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(fragment).encode("utf-8")).hexdigest()


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
        for entry in result[event_name]:
            if set(entry) != {"id", "type", "command"}:
                raise CodexAdapterError("packaged Codex hook entries are not closed")
            if not all(isinstance(entry.get(key), str) and entry[key].strip() for key in ("id", "type", "command")):
                raise CodexAdapterError("packaged Codex hook entry identity is invalid")
    return result


def _managed_paths(packaged: Mapping[Path, bytes]) -> list[str]:
    return sorted({path.as_posix() for path in packaged} | {
        HOOKS_RELATIVE.as_posix(), INSTALL_MANIFEST_RELATIVE.as_posix(),
    })


def _legacy_asset_hashes() -> dict[str, str]:
    # These are the only prior packaged assets admitted for an explicit
    # upgrade.  They are identities of shipped bytes, never restoration data.
    return {
        ".codex/hooks/orchestrator_harness_post_tool_use.py": "bfcfb4d7cdac51fabfe961bd08aa534beaae1564a324a37bace92b2731808592",
        ".codex/hooks/orchestrator_harness_stop.py": "c575b7f78f01dbef1782a140d1a7dcbf857b0400362b7ee72d0f8834f5297244",
    }


def _validate_manifest_identity(
    manifest: Mapping[str, Any],
    project: Path,
    *,
    guard: _ProjectMutationGuard | None = None,
) -> str:
    """Validate a closed supported manifest and return its managed revision."""

    if not isinstance(manifest, Mapping) or manifest.get("schema") != CODEX_INSTALL_MANIFEST_SCHEMA:
        raise CodexInstallConflict("installation manifest is foreign or malformed")
    if manifest.get("adapter") != "codex" or manifest.get("adapter_version") != CODEX_ADAPTER_VERSION:
        raise CodexInstallConflict("installation manifest belongs to another adapter revision")
    if manifest.get("project_root") != str(project):
        raise CodexInstallConflict("installation manifest project identity differs")
    if guard is not None and manifest.get("project_identity") != f"{guard._project_identity[0]}:{guard._project_identity[1]}":
        raise CodexInstallConflict("installation manifest directory identity differs")

    revision = manifest.get("package_revision")
    if revision not in {CODEX_PACKAGE_REVISION, *CODEX_SUPPORTED_LEGACY_REVISIONS}:
        raise CodexInstallConflict("installation manifest revision is unsupported")
    expected_paths = _managed_paths(packaged_codex_assets())
    if revision == CODEX_PACKAGE_REVISION:
        expected_fragment = _managed_hook_fragment()
        if manifest.get("managed_paths") != expected_paths:
            raise CodexInstallConflict("installation manifest managed path set is not supported")
        assets = manifest.get("managed_assets")
        packaged = packaged_codex_assets()
        expected_assets = {path.as_posix(): _hash(data) for path, data in packaged.items()}
        if assets != expected_assets:
            raise CodexInstallConflict("installation manifest managed asset identities are not supported")
        if manifest.get("managed_hook_fragment") != expected_fragment:
            raise CodexInstallConflict("installation manifest hook fragment is not supported")
        if manifest.get("managed_hook_fragment_sha256") != _fragment_digest(expected_fragment):
            raise CodexInstallConflict("installation manifest hook fragment identity is invalid")
        if manifest.get("hooks_path") != HOOKS_RELATIVE.as_posix() or manifest.get("manifest_path") != INSTALL_MANIFEST_RELATIVE.as_posix():
            raise CodexInstallConflict("installation manifest destination contract is invalid")
        if not isinstance(manifest.get("hooks_preexisting"), bool):
            raise CodexInstallConflict("installation manifest pre-existing hook fact is invalid")
        installed = manifest.get("installed_content_sha256")
        if not isinstance(installed, Mapping) or set(installed) != set(expected_assets) | {HOOKS_RELATIVE.as_posix()}:
            raise CodexInstallConflict("installation manifest installed content set is invalid")
        prior_revision = manifest.get("prior_managed_revision")
        if prior_revision is not None and prior_revision not in CODEX_SUPPORTED_LEGACY_REVISIONS | {CODEX_PACKAGE_REVISION}:
            raise CodexInstallConflict("installation manifest prior revision is unsupported")
        trust = manifest.get("trust")
        if not isinstance(trust, Mapping) or set(trust) != {"project_layer", "synthetic_self_test", "hook_review"}:
            raise CodexInstallConflict("installation manifest trust state is invalid")
        if set(manifest) != {
            "schema", "adapter", "adapter_version", "package_revision", "project_root",
            "project_identity", "managed_paths", "managed_assets", "managed_hook_fragment",
            "managed_hook_fragment_sha256", "hooks_path", "manifest_path", "hooks_preexisting",
            "prior_managed_revision", "installed_content_sha256", "trust", "installed_utc",
            "manifest_content_sha256",
        }:
            raise CodexInstallConflict("installation manifest has an unsupported field")
        if _manifest_content_digest(manifest) != manifest.get("manifest_content_sha256"):
            raise CodexInstallConflict("installation manifest content identity is invalid")
        return revision

    # The only accepted legacy revision is the exact v1 packaged path set.  Its
    # historical byte map is opaque metadata and never becomes an authority for
    # uninstall or rollback.
    legacy_fields = {
        "schema", "adapter", "adapter_version", "package_revision", "project_root",
        "owned_paths", "prior_managed_revision", "prior_content",
        "installed_content_sha256", "trust", "installed_utc", "manifest_content_sha256",
    }
    if set(manifest) != legacy_fields:
        raise CodexInstallConflict("legacy installation manifest has an unsupported field")
    if manifest.get("owned_paths") != expected_paths:
        raise CodexInstallConflict("legacy installation manifest managed path set is not supported")
    installed = manifest.get("installed_content_sha256")
    old_assets = _legacy_asset_hashes()
    if not isinstance(installed, Mapping) or any(installed.get(path) != digest for path, digest in old_assets.items()):
        raise CodexInstallConflict("legacy installation manifest asset identities are not supported")
    if manifest.get("hooks_path", HOOKS_RELATIVE.as_posix()) != HOOKS_RELATIVE.as_posix():
        raise CodexInstallConflict("legacy installation manifest hook path is not supported")
    if not isinstance(manifest.get("manifest_content_sha256"), str) or _manifest_content_digest(manifest) != manifest.get("manifest_content_sha256"):
        raise CodexInstallConflict("legacy installation manifest content identity is invalid")
    return revision


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


def _hook_state(existing: bytes | None) -> tuple[dict[str, Any], bool, set[str]]:
    if existing is None:
        return {"hooks": {}}, False, set()
    try:
        value = json.loads(existing.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict(".codex/hooks.json is not valid JSON") from exc
    if not isinstance(value, dict) or ("hooks" in value and not isinstance(value["hooks"], dict)):
        raise CodexInstallConflict(".codex/hooks.json has an unsupported shape")
    value = dict(value)
    value.setdefault("hooks", {})
    fragment = _managed_hook_fragment()
    conflicts: set[str] = set()
    for event_name, entries in fragment.items():
        rows = value["hooks"].get(event_name, [])
        if not isinstance(rows, list) or any(not isinstance(item, dict) for item in rows):
            raise CodexInstallConflict(f".codex/hooks.json {event_name} is not a list of objects")
        for entry in entries:
            marker = entry["id"]
            for row in rows:
                if row.get("id") == marker and row != entry:
                    conflicts.add(marker)
    return value, True, conflicts


def _subtract_hooks(existing: bytes | None) -> tuple[bytes | None, list[str], list[str]]:
    value, present, conflicts = _hook_state(existing)
    if not present:
        return None, [], []
    fragment = _managed_hook_fragment()
    removed: list[str] = []
    for event_name, entries in fragment.items():
        rows = value["hooks"].get(event_name, [])
        kept: list[dict[str, Any]] = []
        for row in rows:
            exact = any(row == entry for entry in entries)
            if exact and row.get("id") not in conflicts:
                removed.append(str(row.get("id")))
            else:
                kept.append(row)
        value["hooks"][event_name] = kept
    if not removed:
        return existing, [], sorted(conflicts)
    return _json_bytes(value), sorted(set(removed)), sorted(conflicts)


def _manifest_result(project: Path) -> dict[str, Any]:
    return {
        "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
        "adapter": "codex",
        "adapter_version": CODEX_ADAPTER_VERSION,
        "package_revision": CODEX_PACKAGE_REVISION,
        "project_root": str(project),
        "installed": False,
        "current": False,
        "managed_paths": [],
        "owned_paths": [],
        "ownership": "unverified",
        "project_trust": "unverified",
        "synthetic_self_test": "not_run",
        "hook_review": "not-reviewed",
        "global_install": False,
    }


def check_codex_adapter(project_root: str | Path) -> dict[str, Any]:
    guard = _project_guard(project_root)
    project = guard.project
    result = _manifest_result(project)
    manifest_path = guard.path(INSTALL_MANIFEST_RELATIVE)
    try:
        manifest = _load_install_manifest(manifest_path, guard=guard)
        if manifest is None:
            return result
        revision = _validate_manifest_identity(manifest, project, guard=guard)
    except CodexInstallConflict as exc:
        result.update({"installed": True, "ownership": "foreign_or_ambiguous", "reason": str(exc)})
        return result
    result.update({
        "installed": True,
        "ownership": "owned",
        "manifest_revision": revision,
        "synthetic_self_test": (manifest.get("trust") or {}).get("synthetic_self_test", "not_run"),
        "hook_review": (manifest.get("trust") or {}).get("hook_review", "not-reviewed"),
    })
    manifest_current = _manifest_is_unchanged(manifest_path, manifest, guard=guard)
    packaged = packaged_codex_assets()
    expected_assets = (
        {path.as_posix(): _hash(data) for path, data in packaged.items()}
        if revision == CODEX_PACKAGE_REVISION else _legacy_asset_hashes()
    )
    installed_hashes = manifest.get("installed_content_sha256", {})
    rows: list[dict[str, Any]] = []
    current = manifest_current and revision == CODEX_PACKAGE_REVISION
    for relative in _managed_paths(packaged):
        data = guard.read(relative)
        if relative == INSTALL_MANIFEST_RELATIVE.as_posix():
            actual = _manifest_content_digest(manifest) if manifest_current else None
            expected = manifest.get("manifest_content_sha256")
        else:
            actual = _hash(data)
            expected = expected_assets.get(relative) if relative in expected_assets else installed_hashes.get(relative)
        same = actual == expected
        if relative in expected_assets and not same:
            current = False
        rows.append({"path": relative, "installed_sha256": expected, "current_sha256": actual, "unchanged": same})
    try:
        _, hook_present, hook_conflicts = _hook_state(guard.read(HOOKS_RELATIVE))
        managed_hooks_present = hook_present and not hook_conflicts and _subtract_hooks(guard.read(HOOKS_RELATIVE))[1] == sorted(_MANAGED_HOOK_IDS)
    except CodexInstallConflict:
        managed_hooks_present = False
    current = current and managed_hooks_present
    result.update({"managed_paths": rows, "owned_paths": rows, "current": current, "manifest_current": manifest_current})
    return result


def install_codex_adapter(project_root: str | Path, *, upgrade: bool = False) -> dict[str, Any]:
    guard = _project_guard(project_root, prepare_codex=True)
    project = guard.project
    packaged = packaged_codex_assets()
    manifest_path = guard.path(INSTALL_MANIFEST_RELATIVE, require_parent=True)
    hooks_path = guard.path(HOOKS_RELATIVE, require_parent=True)
    existing_manifest = _load_install_manifest(manifest_path, guard=guard)
    existing_revision: str | None = None
    if existing_manifest is not None:
        existing_revision = _validate_manifest_identity(existing_manifest, project, guard=guard)
        if not _manifest_is_unchanged(manifest_path, existing_manifest, guard=guard):
            raise CodexInstallConflict("owned installation manifest was modified; refusing overwrite")
        if existing_revision == CODEX_PACKAGE_REVISION and check_codex_adapter(project)["current"]:
            current = check_codex_adapter(project)
            current["operation"] = "upgrade" if upgrade else "install"
            current["changed"] = []
            current["idempotent"] = True
            return current
        if existing_revision != CODEX_PACKAGE_REVISION and not upgrade:
            raise CodexInstallConflict("an older owned Codex adapter requires explicit upgrade")
        old_assets = _legacy_asset_hashes() if existing_revision != CODEX_PACKAGE_REVISION else {
            path.as_posix(): _hash(data) for path, data in packaged.items()
        }
        for relative, expected in old_assets.items():
            if _hash(guard.read(relative)) != expected:
                raise CodexInstallConflict(f"owned file was modified; refusing overwrite: {relative}")
    else:
        # _load_install_manifest rejects any regular foreign manifest before
        # this branch.  Packaged hook destinations may not already be claimed.
        for relative in packaged:
            if guard.read(relative) is not None:
                raise CodexInstallConflict(f"unmanaged hook destination already exists: {relative}")

    target_relatives = [*packaged, HOOKS_RELATIVE, INSTALL_MANIFEST_RELATIVE]
    original_states = {relative: guard.snapshot(relative) for relative in target_relatives}
    originals = {relative: state.content for relative, state in original_states.items()}
    changed: list[str] = []
    try:
        for relative, data in packaged.items():
            if originals[relative] != data:
                guard.atomic_replace(relative, data, expected=original_states[relative])
                changed.append(str(relative))
        hooks_data = _merge_hooks(originals[HOOKS_RELATIVE])
        if originals[HOOKS_RELATIVE] != hooks_data:
            guard.atomic_replace(HOOKS_RELATIVE, hooks_data, expected=original_states[HOOKS_RELATIVE])
            changed.append(HOOKS_RELATIVE.as_posix())
        expected_fragment = _managed_hook_fragment()
        managed_assets = {relative.as_posix(): _hash(value) for relative, value in packaged.items()}
        installed_hashes: dict[str, str | None] = dict(managed_assets)
        installed_hashes[HOOKS_RELATIVE.as_posix()] = _hash(hooks_data)
        manifest = {
            "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
            "adapter": "codex",
            "adapter_version": CODEX_ADAPTER_VERSION,
            "package_revision": CODEX_PACKAGE_REVISION,
            "project_root": str(project),
            "project_identity": f"{guard._project_identity[0]}:{guard._project_identity[1]}",
            "managed_paths": _managed_paths(packaged),
            "managed_assets": managed_assets,
            "managed_hook_fragment": expected_fragment,
            "managed_hook_fragment_sha256": _fragment_digest(expected_fragment),
            "hooks_path": HOOKS_RELATIVE.as_posix(),
            "manifest_path": INSTALL_MANIFEST_RELATIVE.as_posix(),
            "hooks_preexisting": originals[HOOKS_RELATIVE] is not None,
            "prior_managed_revision": existing_revision,
            "installed_content_sha256": installed_hashes,
            "trust": {
                "project_layer": "unverified",
                "synthetic_self_test": "not_run",
                "hook_review": "not-reviewed",
            },
            "installed_utc": iso_utc(utc_now()),
        }
        manifest["manifest_content_sha256"] = _manifest_content_digest(manifest)
        guard.atomic_replace(
            INSTALL_MANIFEST_RELATIVE,
            _json_bytes(manifest),
            expected=original_states[INSTALL_MANIFEST_RELATIVE],
        )
        changed.append(INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for relative, data in originals.items():
            try:
                guard.restore(relative, data)
            except Exception as rollback_exc:  # pragma: no cover - defensive evidence
                rollback_errors.append(f"{relative}: {type(rollback_exc).__name__}")
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
    guard = _project_guard(project_root)
    project = guard.project
    manifest = _load_install_manifest(guard.path(INSTALL_MANIFEST_RELATIVE), guard=guard)
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
    revision = _validate_manifest_identity(manifest, project, guard=guard)
    manifest_path = guard.path(INSTALL_MANIFEST_RELATIVE, require_parent=True)
    if not _manifest_is_unchanged(manifest_path, manifest, guard=guard):
        raise CodexInstallConflict("installation manifest was modified; refusing uninstall")
    packaged = packaged_codex_assets()
    expected_assets = ({path.as_posix(): _hash(data) for path, data in packaged.items()} if revision == CODEX_PACKAGE_REVISION else _legacy_asset_hashes())
    removed: list[str] = []
    preserved: list[str] = []
    original_states = {
        relative: guard.snapshot(relative)
        for relative in [*packaged, HOOKS_RELATIVE, INSTALL_MANIFEST_RELATIVE, CODEX_BINDING_RELATIVE]
    }
    originals = {relative: state.content for relative, state in original_states.items()}
    try:
        for relative, data in packaged.items():
            expected = expected_assets.get(relative.as_posix())
            if _hash(originals[relative]) == expected:
                guard.delete(relative, expected=original_states[relative])
                removed.append(relative.as_posix())
            elif originals[relative] is not None:
                preserved.append(relative.as_posix())
        hooks_data, hook_removed, hook_conflicts = _subtract_hooks(originals[HOOKS_RELATIVE])
        if hook_removed:
            if hooks_data is not None:
                parsed, _, _ = _hook_state(hooks_data)
                no_unrelated = set(parsed) == {"hooks"} and all(not parsed["hooks"].get(name, []) for name in parsed["hooks"])
                if no_unrelated and manifest.get("hooks_preexisting") is False:
                    guard.delete(HOOKS_RELATIVE, expected=original_states[HOOKS_RELATIVE])
                else:
                    guard.atomic_replace(
                        HOOKS_RELATIVE,
                        hooks_data,
                        expected=original_states[HOOKS_RELATIVE],
                    )
            removed.append(HOOKS_RELATIVE.as_posix())
        if hook_conflicts:
            preserved.append(HOOKS_RELATIVE.as_posix())
        if originals[CODEX_BINDING_RELATIVE] is not None:
            try:
                binding = _load_binding_for_hook(project, guard)
                if binding.get("schema") == CODEX_BINDING_SCHEMA and binding.get("project_root") == str(project):
                    guard.delete(CODEX_BINDING_RELATIVE, expected=original_states[CODEX_BINDING_RELATIVE])
                    removed.append(CODEX_BINDING_RELATIVE.as_posix())
                else:
                    preserved.append(CODEX_BINDING_RELATIVE.as_posix())
            except (CodexAdapterError, OSError):
                preserved.append(CODEX_BINDING_RELATIVE.as_posix())
        guard.delete(INSTALL_MANIFEST_RELATIVE, expected=original_states[INSTALL_MANIFEST_RELATIVE])
        removed.append(INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for relative, data in originals.items():
            try:
                guard.restore(relative, data)
            except Exception as rollback_exc:
                rollback_errors.append(f"{relative}: {type(rollback_exc).__name__}")
        detail = f"Codex uninstall rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise CodexInstallRollback(detail) from exc
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


class InstalledCodexHookTransport(CodexTransport):
    """Synchronous project-hook transport for the real installed route.

    The command hook has no active-turn control surface.  It returns only the
    sparse, payload-free context that Codex documents for a completed hook;
    App Server continuation is represented as a separate safe-boundary fact.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._notice: dict[str, Any] | None = None

    @staticmethod
    def _assert_notice(notice: Mapping[str, Any]) -> None:
        if notice.get("schema") != DELIVERY_NOTICE_SCHEMA or any(
            key in notice for key in ("event_id", "event_ids", "data", "payload", "queue_records")
        ):
            raise CodexAdapterError("installed hook received a non-sparse delivery notice")

    def post_tool_result_context(self, notice: Mapping[str, Any]) -> None:
        self._assert_notice(notice)
        self._notice = dict(notice)
        self.calls.append({"method": "PostToolUse", "notice": dict(notice)})

    def inject_items(self, items: Sequence[Mapping[str, Any]]) -> None:
        if any(not isinstance(item, Mapping) for item in items):
            raise CodexAdapterError("installed Codex continuation items are invalid")
        self.calls.append({"method": "thread/inject_items", "items": [dict(item) for item in items]})

    def turn_completed(self) -> None:
        self.calls.append({"method": "turn/completed"})

    def start_turn(self) -> None:
        self.calls.append({"method": "turn/start"})

    def request_continuation(self) -> bool:
        self.calls.append({"method": "Stop.continue", "continue": True})
        return True


def _external_directory(value: str | Path, *, name: str) -> Path:
    path = _lexical_path(value)
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if os.path.lexists(current) and _is_reparse(current):
            raise CodexAdapterError(f"{name} contains a symlink or reparse point: {current}")
    if not path.is_dir() or _is_reparse(path):
        raise CodexAdapterError(f"{name} must be an existing regular directory")
    return path


def _read_external_json(path: Path, *, name: str) -> dict[str, Any]:
    """Read one bound external record without following a substituted file."""

    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise CodexInstallConflict(f"{name} is unavailable") from exc
    if _is_reparse(path) or not stat.S_ISREG(info.st_mode):
        raise CodexInstallConflict(f"{name} is not a regular file")
    if info.st_size > _MAX_BINDING_BYTES:
        raise CodexInstallConflict(f"{name} is oversized")
    parent = path.parent
    try:
        parent_identity = _identity(parent)
        raw = path.read_bytes()
        if _identity(parent) != parent_identity or _is_reparse(parent):
            raise CodexInstallConflict(f"{name} parent identity changed")
    except OSError as exc:
        raise CodexInstallConflict(f"{name} is unavailable") from exc
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict(f"{name} is malformed") from exc
    if not isinstance(value, dict):
        raise CodexInstallConflict(f"{name} is malformed")
    return value


def _directory_identity(path: Path) -> str:
    return f"{_identity(path)[0]}:{_identity(path)[1]}"


def _binding_record(project: Path, router: ManagerEventRouter, coordinator_root: Path) -> dict[str, Any]:
    registration = router.registration
    router.validate_binding(registration)
    queue = _external_directory(router.root, name="manager queue root")
    coordinator = _external_directory(coordinator_root, name="coordinator root")
    return {
        "schema": CODEX_BINDING_SCHEMA,
        "adapter": "codex",
        "adapter_version": CODEX_ADAPTER_VERSION,
        "package_revision": CODEX_PACKAGE_REVISION,
        "project_root": str(project),
        "project_identity": _directory_identity(project),
        "queue_root": str(queue),
        "queue_identity": _directory_identity(queue),
        "coordinator_root": str(coordinator),
        "coordinator_identity": _directory_identity(coordinator),
        "run_id": router.binding.run_id,
        "queue_id": router.binding.queue_id,
        "manager_session_id": router.binding.manager_session_id,
        "manager_thread_id": router.binding.manager_thread_id,
        "manager_invocation_id": router.binding.manager_invocation_id,
        "registration_id": router.binding.registration_id,
        "registration_generation": router.registration_generation,
        "adapter_profile": codex_profile().profile_id,
    }


def activate_codex_binding(
    project_root: str | Path,
    router: ManagerEventRouter,
    *,
    coordinator_root: str | Path | None = None,
) -> dict[str, Any]:
    """Persist one exact S3 binding and register its harness coordinator.

    This is setup-time harness ownership.  Installed hooks subsequently restore
    the persisted record themselves; no manager polling or manual re-arm is
    part of the delivery path.
    """

    guard = _project_guard(project_root)
    if not check_codex_adapter(guard.project).get("current"):
        raise CodexInstallConflict("Codex binding requires a current owned installation")
    queue = _external_directory(router.root, name="manager queue root")
    root = _lexical_path(coordinator_root) if coordinator_root is not None else queue / "codex-coordinator"
    if not root.exists():
        _external_directory(root.parent, name="coordinator parent")
        try:
            ensure_directory_path(root)
        except (MutationConflict, MutationUnsupported) as exc:
            raise CodexAdapterError(str(exc)) from exc
    if _is_reparse(root) or not root.is_dir():
        raise CodexAdapterError("coordinator root is not safe")
    transport = InstalledCodexHookTransport()
    coordinator = DeliveryCoordinator(
        router=router,
        adapter=FutureHostFixture("codex-bootstrap"),
        state_root=root,
        registration_generation=router.registration_generation,
    )
    CodexAdapter(transport, coordinator)
    coordinator.restore()
    record = _binding_record(guard.project, router, root)
    prior = guard.read(CODEX_BINDING_RELATIVE)
    if prior is not None:
        try:
            prior_value = json.loads(prior.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CodexInstallConflict("Codex binding record is malformed") from exc
        if prior_value != record:
            raise CodexInstallConflict("Codex binding record is stale or cross-bound")
    else:
        guard.atomic_replace(
            CODEX_BINDING_RELATIVE,
            _json_bytes(record),
            expected=guard.snapshot(CODEX_BINDING_RELATIVE),
        )
    return {"schema": CODEX_BINDING_SCHEMA, "binding": record, "state": coordinator.load_state()}


bind_codex_project = activate_codex_binding


def _load_binding_for_hook(project: Path, guard: _ProjectMutationGuard) -> dict[str, Any]:
    data = guard.read(CODEX_BINDING_RELATIVE)
    if data is None:
        raise CodexInstallConflict("installed Codex hook has no harness binding")
    if len(data) > _MAX_BINDING_BYTES:
        raise CodexInstallConflict("Codex binding record is oversized")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict("Codex binding record is malformed") from exc
    required = {
        "schema", "adapter", "adapter_version", "package_revision", "project_root",
        "project_identity", "queue_root", "queue_identity", "coordinator_root",
        "coordinator_identity", "run_id", "queue_id", "manager_session_id",
        "manager_thread_id", "manager_invocation_id", "registration_id",
        "registration_generation", "adapter_profile",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise CodexInstallConflict("Codex binding record has an invalid closed shape")
    if value["schema"] != CODEX_BINDING_SCHEMA or value["adapter"] != "codex" or value["adapter_version"] != CODEX_ADAPTER_VERSION or value["package_revision"] != CODEX_PACKAGE_REVISION:
        raise CodexInstallConflict("Codex binding record revision is stale")
    if value["project_root"] != str(project) or value["project_identity"] != _directory_identity(project):
        raise CodexInstallConflict("Codex binding project identity is stale")
    if value["adapter_profile"] != codex_profile().profile_id:
        raise CodexInstallConflict("Codex binding adapter profile is stale")
    generation = value["registration_generation"]
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
        raise CodexInstallConflict("Codex binding registration generation is invalid")
    queue = _external_directory(value["queue_root"], name="bound manager queue root")
    if _directory_identity(queue) != value["queue_identity"]:
        raise CodexInstallConflict("bound manager queue identity changed")
    coordinator = _external_directory(value["coordinator_root"], name="bound coordinator root")
    if _directory_identity(coordinator) != value["coordinator_identity"]:
        raise CodexInstallConflict("bound coordinator identity changed")
    registration = _read_external_json(
        queue / "REGISTRATION.json", name="bound manager registration"
    )
    for key in (
        "run_id", "queue_id", "manager_session_id", "manager_thread_id",
        "manager_invocation_id", "registration_id", "registration_generation",
    ):
        if registration.get(key) != value.get(key):
            raise CodexInstallConflict(f"bound manager registration mismatch in {key}")
    return value


def run_installed_codex_hook(
    project_root: str | Path,
    *,
    boundary: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the actually installed, exact-binding safe-boundary route."""

    del payload  # Hook input is never copied into the notice or receipt.
    if boundary not in {"post_tool_use", "stop"}:
        raise CodexAdapterError("supported installed Codex hooks are post_tool_use and stop")
    guard = _project_guard(project_root)
    if not check_codex_adapter(guard.project).get("current"):
        raise CodexInstallConflict("installed Codex hook is not a current trusted byte set")
    binding = _load_binding_for_hook(guard.project, guard)
    router = ManagerEventRouter(
        Path(binding["queue_root"]),
        run_id=binding["run_id"],
        queue_id=binding["queue_id"],
        manager_session_id=binding["manager_session_id"],
        manager_thread_id=binding["manager_thread_id"],
        registration_id=binding["registration_id"],
        manager_invocation_id=binding["manager_invocation_id"],
        registration_generation=binding["registration_generation"],
    )
    router.validate_binding(router.registration)
    transport = InstalledCodexHookTransport()
    coordinator = DeliveryCoordinator(
        router=router,
        adapter=FutureHostFixture("codex-bootstrap"),
        state_root=Path(binding["coordinator_root"]),
        registration_generation=binding["registration_generation"],
    )
    adapter = CodexAdapter(transport, coordinator)
    coordinator.restore()
    notice = coordinator.notice_for_wake()
    receipt: DeliveryReceipt | None = None
    continuation_requested = False
    if notice is not None:
        if boundary == "post_tool_use":
            receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
        else:
            receipt = coordinator.deliver_at_boundary(notice, boundary="finalization")
            if receipt is not None and receipt.outcome == "DELIVERED":
                continuation_requested = transport.request_continuation()
    return {
        "schema": "orchestrator-codex-installed-hook/v1",
        "boundary": boundary,
        "project_root": str(guard.project),
        "binding": {
            "run_id": binding["run_id"],
            "queue_id": binding["queue_id"],
            "manager_session_id": binding["manager_session_id"],
            "manager_thread_id": binding["manager_thread_id"],
            "registration_id": binding["registration_id"],
            "registration_generation": binding["registration_generation"],
            "adapter_profile": binding["adapter_profile"],
        },
        "notice": notice.as_record() if notice is not None else None,
        "receipt": receipt.as_record() if receipt is not None else None,
        "continuation_requested": continuation_requested,
        "pending_count": len(router.pending_events()),
        "acknowledged_by_hook": False,
        "transport_calls": list(transport.calls),
    }


def _set_self_test_state(project: Path, outcome: str) -> None:
    guard = _project_guard(project)
    manifest_path = guard.path(INSTALL_MANIFEST_RELATIVE, require_parent=True)
    manifest = _load_install_manifest(manifest_path, guard=guard)
    if manifest is None:
        raise CodexInstallConflict("synthetic self-test requires an installed adapter")
    _validate_manifest_identity(manifest, project, guard=guard)
    current = check_codex_adapter(project)
    if not current.get("current"):
        raise CodexInstallConflict("synthetic self-test refuses a modified installation")
    trust = dict(manifest.get("trust") or {})
    trust["synthetic_self_test"] = outcome
    manifest["trust"] = trust
    manifest["manifest_content_sha256"] = _manifest_content_digest(manifest)
    guard.atomic_replace(INSTALL_MANIFEST_RELATIVE, _json_bytes(manifest))


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


def run_codex_hook(
    boundary: str,
    payload: Mapping[str, Any] | None = None,
    *,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run the installed exact-binding route; static metadata is not delivery."""
    if project_root is None:
        raise CodexAdapterError("Codex hook delivery requires its bound project root")
    if payload is not None and not isinstance(payload, Mapping):
        raise CodexAdapterError("hook payload must be an object")
    return run_installed_codex_hook(project_root, boundary=boundary, payload=payload)


__all__ = [
    "CODEX_ADAPTER_SCHEMA",
    "CODEX_ADAPTER_VERSION",
    "CODEX_BINDING_SCHEMA",
    "CODEX_BINDING_RELATIVE",
    "CODEX_INSTALL_MANIFEST_SCHEMA",
    "CODEX_PACKAGE_REVISION",
    "CodexAdapter",
    "CodexAdapterError",
    "CodexInstallConflict",
    "CodexInstallRollback",
    "CodexTransport",
    "InstalledCodexHookTransport",
    "RecordingCodexTransport",
    "SyntheticCodexTransport",
    "activate_codex_binding",
    "bind_codex_project",
    "check_codex_adapter",
    "codex_profile",
    "create_codex_adapter",
    "install_codex_adapter",
    "packaged_codex_assets",
    "packaged_codex_manifest",
    "run_codex_hook",
    "run_installed_codex_hook",
    "run_synthetic_wake_self_test",
    "select_host_adapter",
    "synthetic_wake_self_test",
    "uninstall_codex_adapter",
    "upgrade_codex_adapter",
]
