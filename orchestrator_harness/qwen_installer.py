"""Owned Qwen Code project installer and documented hook-boundary route.

Qwen Code project settings live in ``.qwen/settings.json``.  This installer
owns only that project file, the project-local hook scripts, and its manifest;
it never edits a user/global Qwen home.  The hook route is deterministic and
safe-boundary-only: an installed file is not proof of a live Qwen session.
"""

from __future__ import annotations

import hashlib
import json
from importlib import resources
from pathlib import Path
from typing import Any, Mapping

from .codex_adapter import (
    CodexInstallConflict,
    CodexInstallRollback,
    _ProjectMutationGuard,
    _hash,
    _json_bytes,
    _manifest_content_digest,
    _project_guard,
    _read_bytes,
)
from .host_adapters import DeliveryCoordinator, DeliveryReceipt, FutureHostFixture
from .models import iso_utc, utc_now
from .mutation import MutationConflict, safe_relative_path
from .notifications import ManagerEventRouter
from .qwen_adapter import QwenAdapter, SyntheticQwenTransport
from .stable_io import canonical_json


QWEN_INSTALL_MANIFEST_SCHEMA = "orchestrator-qwen-install/v1"
QWEN_ADAPTER_VERSION = "qwen-v1"
QWEN_PACKAGE_REVISION = "qwen-assets-v1"
QWEN_INSTALL_MANIFEST_RELATIVE = Path(".qwen") / "orchestrator-harness-adapter.json"
QWEN_SETTINGS_RELATIVE = Path(".qwen") / "settings.json"
QWEN_BINDING_RELATIVE = Path(".qwen") / "orchestrator-harness-binding.json"
_MANAGED_EVENT_NAMES = ("PostToolUse", "Notification", "Stop")
_MANAGED_HOOK_NAMES = (
    "orchestrator-harness-post-tool-use",
    "orchestrator-harness-notification",
    "orchestrator-harness-stop",
)
_MAX_BINDING_BYTES = 128_000


class QwenInstallConflict(CodexInstallConflict):
    """A foreign or modified project destination cannot be claimed safely."""


class QwenInstallRollback(CodexInstallRollback):
    """The bounded Qwen installation transaction was restored after failure."""


def _qwen_resource(name: str) -> bytes:
    try:
        return (
            resources.files("orchestrator_harness.assets.qwen")
            .joinpath(name)
            .read_bytes()
        )
    except (FileNotFoundError, ModuleNotFoundError, OSError) as exc:
        raise QwenInstallConflict(f"packaged Qwen asset is unavailable: {name}") from exc


def packaged_qwen_manifest() -> dict[str, Any]:
    try:
        raw = json.loads(_qwen_resource("manifest.json").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QwenInstallConflict("packaged Qwen manifest is invalid") from exc
    if not isinstance(raw, dict) or raw.get("schema") != "orchestrator-qwen-adapter/v1":
        raise QwenInstallConflict("packaged Qwen manifest has an invalid schema")
    if raw.get("package_revision") != QWEN_PACKAGE_REVISION:
        raise QwenInstallConflict("packaged Qwen manifest revision is inconsistent")
    files = raw.get("files")
    if not isinstance(files, list) or not files:
        raise QwenInstallConflict("packaged Qwen manifest has no assets")
    return raw


def packaged_qwen_assets() -> dict[Path, bytes]:
    manifest = packaged_qwen_manifest()
    assets: dict[Path, bytes] = {}
    for item in manifest["files"]:
        if not isinstance(item, Mapping):
            raise QwenInstallConflict("packaged Qwen asset entry is invalid")
        destination = item.get("destination")
        resource_name = item.get("resource")
        if not isinstance(destination, str) or not isinstance(resource_name, str):
            raise QwenInstallConflict("packaged Qwen asset path is unsafe")
        try:
            relative = safe_relative_path(destination)
            resource_relative = safe_relative_path(resource_name)
        except MutationConflict as exc:
            raise QwenInstallConflict("packaged Qwen asset path is unsafe") from exc
        if item.get("content_mode") != "utf8-lf":
            raise QwenInstallConflict(f"packaged Qwen asset mode is unsupported: {relative}")
        data = _qwen_resource(resource_relative.as_posix())
        try:
            text = data.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise QwenInstallConflict(f"packaged Qwen asset is not UTF-8: {relative}") from exc
        if "\ufeff" in text:
            raise QwenInstallConflict(f"packaged Qwen asset contains a BOM: {relative}")
        if any(
            char == "\r" and (index + 1 == len(text) or text[index + 1] != "\n")
            for index, char in enumerate(text)
        ):
            raise QwenInstallConflict(f"packaged Qwen asset contains a lone CR: {relative}")
        canonical = text.replace("\r\n", "\n").encode("utf-8")
        expected = item.get("sha256")
        if not isinstance(expected, str) or hashlib.sha256(canonical).hexdigest() != expected:
            raise QwenInstallConflict(f"packaged Qwen asset hash mismatch: {relative}")
        assets[relative] = canonical
    return assets


def _fragment_digest(fragment: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(fragment).encode("utf-8")).hexdigest()


def _managed_settings_fragment() -> dict[str, Any]:
    try:
        raw = json.loads(_qwen_resource("settings.fragment.json").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QwenInstallConflict("packaged Qwen settings fragment is invalid") from exc
    if not isinstance(raw, dict) or set(raw) != {"hooks"} or not isinstance(raw["hooks"], dict):
        raise QwenInstallConflict("packaged Qwen settings fragment is not closed")
    if set(raw["hooks"]) != set(_MANAGED_EVENT_NAMES):
        raise QwenInstallConflict("packaged Qwen settings fragment has unsupported events")
    for event_name, groups in raw["hooks"].items():
        if not isinstance(groups, list) or not groups:
            raise QwenInstallConflict(f"Qwen {event_name} hook groups are invalid")
        for group in groups:
            expected_keys = {"hooks"} if event_name == "Stop" else {"matcher", "hooks"}
            if not isinstance(group, dict) or set(group) != expected_keys:
                raise QwenInstallConflict("Qwen hook group is not closed")
            if (
                event_name != "Stop"
                and (
                    not isinstance(group["matcher"], str)
                    or not group["matcher"].strip()
                )
            ) or not isinstance(group["hooks"], list):
                raise QwenInstallConflict("Qwen hook group is invalid")
            for hook in group["hooks"]:
                if not isinstance(hook, dict) or set(hook) != {"type", "name", "command"}:
                    raise QwenInstallConflict("Qwen command hook is not closed")
                if any(not isinstance(hook[key], str) or not hook[key].strip() for key in hook):
                    raise QwenInstallConflict("Qwen command hook identity is invalid")
    return raw


def _managed_paths(packaged: Mapping[Path, bytes]) -> list[str]:
    return sorted(
        {path.as_posix() for path in packaged}
        | {QWEN_SETTINGS_RELATIVE.as_posix(), QWEN_INSTALL_MANIFEST_RELATIVE.as_posix()}
    )


def _inner_hook_names(group: Mapping[str, Any]) -> set[str]:
    inner = group.get("hooks")
    if not isinstance(inner, list):
        return set()
    return {
        item["name"]
        for item in inner
        if isinstance(item, Mapping) and isinstance(item.get("name"), str)
    }


def _managed_group_matches(
    event_name: str, group: Mapping[str, Any], managed: Mapping[str, Any]
) -> bool:
    if event_name == "Stop":
        return set(group) == {"hooks"} and group.get("hooks") == managed.get("hooks")
    return group == managed


def _parse_settings(raw: bytes | None) -> dict[str, Any]:
    if raw is None:
        return {}
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QwenInstallConflict(".qwen/settings.json is not valid JSON") from exc
    if not isinstance(value, dict) or not isinstance(value.get("hooks"), dict):
        raise QwenInstallConflict(".qwen/settings.json is not hook-shaped")
    return dict(value)


def _merge_settings(raw: bytes | None) -> bytes:
    parsed = _parse_settings(raw)
    merged = dict(parsed)
    hooks = dict(merged.get("hooks") or {})
    fragment = _managed_settings_fragment()
    for event_name, managed_groups in fragment["hooks"].items():
        prior = hooks.get(event_name, [])
        if not isinstance(prior, list) or any(not isinstance(item, Mapping) for item in prior):
            raise QwenInstallConflict(f".qwen/settings.json {event_name} is invalid")
        rows = [dict(item) for item in prior]
        names = {name for group in managed_groups for name in _inner_hook_names(group)}
        for managed in managed_groups:
            ours = [row for row in rows if _inner_hook_names(row) & names]
            if ours and any(_managed_group_matches(event_name, row, managed) for row in ours):
                continue
            if ours:
                raise QwenInstallConflict(".qwen/settings.json contains an ambiguous owned hook")
            rows.append(dict(managed))
        hooks[event_name] = rows
    merged["hooks"] = hooks
    return _json_bytes(merged)


def _settings_state(raw: bytes | None) -> tuple[bool, list[str]]:
    if raw is None:
        return False, []
    parsed = _parse_settings(raw)
    hooks = parsed["hooks"]
    fragment = _managed_settings_fragment()
    present: set[str] = set()
    conflicts: set[str] = set()
    for event_name, managed_groups in fragment["hooks"].items():
        rows = hooks.get(event_name)
        if rows is None:
            continue
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise QwenInstallConflict(f".qwen/settings.json {event_name} is invalid")
        names = {name for group in managed_groups for name in _inner_hook_names(group)}
        for row in rows:
            row_names = _inner_hook_names(row)
            if not row_names & names:
                continue
            if any(_managed_group_matches(event_name, row, group) for group in managed_groups):
                present.update(row_names)
            else:
                conflicts.update(row_names)
    return all(name in present and name not in conflicts for name in _MANAGED_HOOK_NAMES), sorted(conflicts)


def _subtract_settings(raw: bytes | None) -> tuple[bytes | None, list[str], list[str]]:
    if raw is None:
        return None, [], []
    parsed = _parse_settings(raw)
    hooks = dict(parsed["hooks"])
    fragment = _managed_settings_fragment()
    removed: set[str] = set()
    conflicts: set[str] = set()
    for event_name, managed_groups in fragment["hooks"].items():
        rows = hooks.get(event_name)
        if rows is None:
            continue
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise QwenInstallConflict(f".qwen/settings.json {event_name} is invalid")
        names = {name for group in managed_groups for name in _inner_hook_names(group)}
        kept: list[dict[str, Any]] = []
        for row in rows:
            row_names = _inner_hook_names(row)
            if not row_names & names:
                kept.append(dict(row))
            elif any(_managed_group_matches(event_name, row, group) for group in managed_groups):
                removed.update(row_names)
            else:
                conflicts.update(row_names)
                kept.append(dict(row))
        if kept:
            hooks[event_name] = kept
        else:
            hooks.pop(event_name, None)
    if not removed:
        return raw, [], sorted(conflicts)
    remaining = dict(hooks)
    if remaining or any(key != "hooks" for key in parsed):
        result = dict(parsed)
        result["hooks"] = remaining
        return _json_bytes(result), sorted(removed), sorted(conflicts)
    return None, sorted(removed), sorted(conflicts)


def _load_install_manifest(path: Path, *, guard: _ProjectMutationGuard) -> dict[str, Any] | None:
    data = _read_bytes(path, guard)
    if data is None:
        return None
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QwenInstallConflict("Qwen installation manifest is malformed") from exc
    if not isinstance(value, dict) or value.get("schema") != QWEN_INSTALL_MANIFEST_SCHEMA:
        raise QwenInstallConflict("installation manifest is not owned by Qwen")
    return value


def _manifest_is_unchanged(path: Path, manifest: Mapping[str, Any], guard: _ProjectMutationGuard) -> bool:
    data = _read_bytes(path, guard)
    return (
        data is not None
        and data == _json_bytes(manifest)
        and _manifest_content_digest(manifest) == manifest.get("manifest_content_sha256")
    )


def _validate_manifest_identity(
    manifest: Mapping[str, Any], project: Path, guard: _ProjectMutationGuard
) -> None:
    packaged = packaged_qwen_assets()
    if (
        manifest.get("schema") != QWEN_INSTALL_MANIFEST_SCHEMA
        or manifest.get("adapter") != "qwen"
        or manifest.get("adapter_version") != QWEN_ADAPTER_VERSION
        or manifest.get("package_revision") != QWEN_PACKAGE_REVISION
        or manifest.get("project_root") != str(project)
        or manifest.get("project_identity") != f"{guard._project_identity[0]}:{guard._project_identity[1]}"
    ):
        raise QwenInstallConflict("Qwen installation manifest identity is invalid")
    managed_assets = {path.as_posix(): _hash(data) for path, data in packaged.items()}
    fragment = _managed_settings_fragment()
    if (
        manifest.get("managed_paths") != _managed_paths(packaged)
        or manifest.get("managed_assets") != managed_assets
        or manifest.get("managed_settings_fragment") != fragment
        or manifest.get("managed_settings_fragment_sha256") != _fragment_digest(fragment)
        or manifest.get("settings_path") != QWEN_SETTINGS_RELATIVE.as_posix()
        or manifest.get("manifest_path") != QWEN_INSTALL_MANIFEST_RELATIVE.as_posix()
        or not isinstance(manifest.get("settings_preexisting"), bool)
    ):
        raise QwenInstallConflict("Qwen installation manifest content is invalid")
    installed = manifest.get("installed_content_sha256")
    if not isinstance(installed, Mapping) or set(installed) != set(managed_assets) | {QWEN_SETTINGS_RELATIVE.as_posix()}:
        raise QwenInstallConflict("Qwen installed content set is invalid")
    trust = manifest.get("trust")
    if not isinstance(trust, Mapping) or set(trust) != {"project_layer", "synthetic_self_test", "hook_review"}:
        raise QwenInstallConflict("Qwen trust state is invalid")
    expected_fields = {
        "schema", "adapter", "adapter_version", "package_revision", "project_root",
        "project_identity", "managed_paths", "managed_assets", "managed_settings_fragment",
        "managed_settings_fragment_sha256", "settings_path", "manifest_path",
        "settings_preexisting", "installed_content_sha256", "trust", "installed_utc",
        "manifest_content_sha256",
    }
    if set(manifest) != expected_fields or _manifest_content_digest(manifest) != manifest.get("manifest_content_sha256"):
        raise QwenInstallConflict("Qwen installation manifest has unsupported content")


def _manifest_result(project: Path) -> dict[str, Any]:
    return {
        "schema": QWEN_INSTALL_MANIFEST_SCHEMA,
        "adapter": "qwen",
        "adapter_version": QWEN_ADAPTER_VERSION,
        "package_revision": QWEN_PACKAGE_REVISION,
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


def check_qwen_adapter(project_root: str | Path) -> dict[str, Any]:
    guard = _project_guard(project_root)
    project = guard.project
    result = _manifest_result(project)
    manifest_path = guard.path(QWEN_INSTALL_MANIFEST_RELATIVE)
    try:
        manifest = _load_install_manifest(manifest_path, guard=guard)
        if manifest is None:
            return result
        _validate_manifest_identity(manifest, project, guard)
    except (CodexInstallConflict, QwenInstallConflict) as exc:
        result.update({"installed": True, "ownership": "foreign_or_ambiguous", "reason": str(exc)})
        return result
    manifest_current = _manifest_is_unchanged(manifest_path, manifest, guard)
    packaged = packaged_qwen_assets()
    expected_assets = {path.as_posix(): _hash(data) for path, data in packaged.items()}
    rows: list[dict[str, Any]] = []
    current = manifest_current
    installed_hashes = manifest["installed_content_sha256"]
    for relative in _managed_paths(packaged):
        data = guard.read(relative)
        if relative == QWEN_INSTALL_MANIFEST_RELATIVE.as_posix():
            actual = _manifest_content_digest(manifest) if manifest_current else None
            expected = manifest.get("manifest_content_sha256")
        else:
            actual = _hash(data)
            expected = expected_assets.get(relative, installed_hashes.get(relative))
        same = actual == expected
        if relative in expected_assets and not same:
            current = False
        rows.append({"path": relative, "installed_sha256": expected, "current_sha256": actual, "unchanged": same, "editable": False})
    try:
        settings_present, settings_conflicts = _settings_state(guard.read(QWEN_SETTINGS_RELATIVE))
    except QwenInstallConflict:
        settings_present, settings_conflicts = False, ["invalid"]
    current = current and settings_present and not settings_conflicts
    result.update({
        "installed": True,
        "ownership": "owned",
        "managed_paths": rows,
        "owned_paths": rows,
        "current": current,
        "manifest_current": manifest_current,
        "settings_path": QWEN_SETTINGS_RELATIVE.as_posix(),
        "managed_settings_present": settings_present and not settings_conflicts,
        "manifest_revision": QWEN_PACKAGE_REVISION,
        "synthetic_self_test": manifest["trust"].get("synthetic_self_test", "not_run"),
        "hook_review": manifest["trust"].get("hook_review", "not-reviewed"),
    })
    return result


def install_qwen_adapter(project_root: str | Path, *, upgrade: bool = False) -> dict[str, Any]:
    guard = _project_guard(project_root)
    guard.ensure_directory(Path(".qwen"))
    guard.ensure_directory(Path(".qwen") / "hooks")
    project = guard.project
    packaged = packaged_qwen_assets()
    manifest_path = guard.path(QWEN_INSTALL_MANIFEST_RELATIVE, require_parent=True)
    existing = _load_install_manifest(manifest_path, guard=guard)
    if existing is not None:
        _validate_manifest_identity(existing, project, guard)
        if not _manifest_is_unchanged(manifest_path, existing, guard):
            raise QwenInstallConflict("owned Qwen installation manifest was modified")
        if check_qwen_adapter(project).get("current"):
            result = check_qwen_adapter(project)
            result.update({"operation": "upgrade" if upgrade else "install", "changed": [], "idempotent": True})
            return result
        if not upgrade:
            raise QwenInstallConflict("current Qwen installation is incomplete; explicit upgrade required")
        for relative, data in packaged.items():
            if _hash(guard.read(relative)) != _hash(data):
                raise QwenInstallConflict(f"owned Qwen file was modified: {relative}")
    else:
        for relative in packaged:
            if guard.read(relative) is not None:
                raise QwenInstallConflict(f"unmanaged Qwen hook destination exists: {relative}")

    target_relatives = [*packaged, QWEN_SETTINGS_RELATIVE, QWEN_INSTALL_MANIFEST_RELATIVE]
    original_states = {relative: guard.snapshot(relative) for relative in target_relatives}
    originals = {relative: state.content for relative, state in original_states.items()}
    changed: list[str] = []
    try:
        for relative, data in packaged.items():
            if originals[relative] != data:
                guard.atomic_replace(relative, data, expected=original_states[relative])
                changed.append(relative.as_posix())
        settings_data = _merge_settings(originals[QWEN_SETTINGS_RELATIVE])
        if originals[QWEN_SETTINGS_RELATIVE] != settings_data:
            guard.atomic_replace(QWEN_SETTINGS_RELATIVE, settings_data, expected=original_states[QWEN_SETTINGS_RELATIVE])
            changed.append(QWEN_SETTINGS_RELATIVE.as_posix())
        fragment = _managed_settings_fragment()
        managed_assets = {path.as_posix(): _hash(data) for path, data in packaged.items()}
        installed_hashes: dict[str, str | None] = dict(managed_assets)
        installed_hashes[QWEN_SETTINGS_RELATIVE.as_posix()] = _hash(settings_data)
        manifest: dict[str, Any] = {
            "schema": QWEN_INSTALL_MANIFEST_SCHEMA,
            "adapter": "qwen",
            "adapter_version": QWEN_ADAPTER_VERSION,
            "package_revision": QWEN_PACKAGE_REVISION,
            "project_root": str(project),
            "project_identity": f"{guard._project_identity[0]}:{guard._project_identity[1]}",
            "managed_paths": _managed_paths(packaged),
            "managed_assets": managed_assets,
            "managed_settings_fragment": fragment,
            "managed_settings_fragment_sha256": _fragment_digest(fragment),
            "settings_path": QWEN_SETTINGS_RELATIVE.as_posix(),
            "manifest_path": QWEN_INSTALL_MANIFEST_RELATIVE.as_posix(),
            "settings_preexisting": originals[QWEN_SETTINGS_RELATIVE] is not None,
            "installed_content_sha256": installed_hashes,
            "trust": {"project_layer": "unverified", "synthetic_self_test": "not_run", "hook_review": "not-reviewed"},
            "installed_utc": iso_utc(utc_now()),
        }
        manifest["manifest_content_sha256"] = _manifest_content_digest(manifest)
        guard.atomic_replace(QWEN_INSTALL_MANIFEST_RELATIVE, _json_bytes(manifest), expected=original_states[QWEN_INSTALL_MANIFEST_RELATIVE])
        changed.append(QWEN_INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for relative, data in originals.items():
            try:
                guard.restore(relative, data)
            except Exception as rollback_exc:  # pragma: no cover
                rollback_errors.append(f"{relative}: {type(rollback_exc).__name__}")
        detail = f"Qwen install rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise QwenInstallRollback(detail) from exc
    result = check_qwen_adapter(project)
    result.update({"operation": "upgrade" if upgrade else "install", "changed": changed, "idempotent": not bool(changed)})
    return result


def upgrade_qwen_adapter(project_root: str | Path) -> dict[str, Any]:
    return install_qwen_adapter(project_root, upgrade=True)


def uninstall_qwen_adapter(project_root: str | Path) -> dict[str, Any]:
    guard = _project_guard(project_root)
    project = guard.project
    manifest = _load_install_manifest(guard.path(QWEN_INSTALL_MANIFEST_RELATIVE), guard=guard)
    if manifest is None:
        return {"schema": QWEN_INSTALL_MANIFEST_SCHEMA, "adapter": "qwen", "project_root": str(project), "operation": "uninstall", "installed": False, "removed": [], "preserved_modified": [], "project_trust": "unverified"}
    _validate_manifest_identity(manifest, project, guard)
    manifest_path = guard.path(QWEN_INSTALL_MANIFEST_RELATIVE, require_parent=True)
    if not _manifest_is_unchanged(manifest_path, manifest, guard):
        raise QwenInstallConflict("Qwen installation manifest was modified")
    packaged = packaged_qwen_assets()
    expected_assets = {path.as_posix(): _hash(data) for path, data in packaged.items()}
    target_relatives = [*packaged, QWEN_SETTINGS_RELATIVE, QWEN_INSTALL_MANIFEST_RELATIVE, QWEN_BINDING_RELATIVE]
    original_states = {relative: guard.snapshot(relative) for relative in target_relatives}
    originals = {relative: state.content for relative, state in original_states.items()}
    removed: list[str] = []
    preserved: list[str] = []
    try:
        for relative, _data in packaged.items():
            if _hash(originals[relative]) == expected_assets[relative.as_posix()]:
                guard.delete(relative, expected=original_states[relative])
                removed.append(relative.as_posix())
            elif originals[relative] is not None:
                preserved.append(relative.as_posix())
        settings_data, settings_removed, settings_conflicts = _subtract_settings(originals[QWEN_SETTINGS_RELATIVE])
        if settings_removed:
            if settings_data is None:
                guard.delete(QWEN_SETTINGS_RELATIVE, expected=original_states[QWEN_SETTINGS_RELATIVE])
            else:
                guard.atomic_replace(QWEN_SETTINGS_RELATIVE, settings_data, expected=original_states[QWEN_SETTINGS_RELATIVE])
            removed.append(QWEN_SETTINGS_RELATIVE.as_posix())
        if settings_conflicts:
            preserved.append(QWEN_SETTINGS_RELATIVE.as_posix())
        binding_data = originals[QWEN_BINDING_RELATIVE]
        if binding_data is not None:
            try:
                binding = json.loads(binding_data.decode("utf-8"))
                if isinstance(binding, dict) and binding.get("project_root") == str(project):
                    guard.delete(QWEN_BINDING_RELATIVE, expected=original_states[QWEN_BINDING_RELATIVE])
                    removed.append(QWEN_BINDING_RELATIVE.as_posix())
                else:
                    preserved.append(QWEN_BINDING_RELATIVE.as_posix())
            except (UnicodeDecodeError, json.JSONDecodeError):
                preserved.append(QWEN_BINDING_RELATIVE.as_posix())
        guard.delete(QWEN_INSTALL_MANIFEST_RELATIVE, expected=original_states[QWEN_INSTALL_MANIFEST_RELATIVE])
        removed.append(QWEN_INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for relative, data in originals.items():
            try:
                guard.restore(relative, data)
            except Exception as rollback_exc:  # pragma: no cover
                rollback_errors.append(f"{relative}: {type(rollback_exc).__name__}")
        detail = f"Qwen uninstall rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise QwenInstallRollback(detail) from exc
    return {"schema": QWEN_INSTALL_MANIFEST_SCHEMA, "adapter": "qwen", "project_root": str(project), "operation": "uninstall", "installed": True, "removed": sorted(removed), "preserved_modified": sorted(preserved), "project_trust": "unverified", "ownership_safe": True}


def _load_binding_for_hook(project: Path, guard: _ProjectMutationGuard) -> dict[str, Any]:
    data = guard.read(QWEN_BINDING_RELATIVE)
    if data is None or len(data) > _MAX_BINDING_BYTES:
        raise QwenInstallConflict("installed Qwen hook has no valid harness binding")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QwenInstallConflict("Qwen binding record is malformed") from exc
    required = {"queue_root", "coordinator_root", "run_id", "queue_id", "manager_session_id", "manager_thread_id", "registration_id", "registration_generation"}
    if not isinstance(value, dict) or not required.issubset(value) or value.get("project_root") != str(project):
        raise QwenInstallConflict("Qwen binding record is incomplete or stale")
    return value


def run_installed_qwen_hook(
    project_root: str | Path,
    *,
    boundary: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the installed deterministic Qwen hook route without queue ack."""

    if boundary not in {"post_tool_use", "notification", "stop"}:
        raise QwenInstallConflict("supported Qwen hooks are post_tool_use, notification, and stop")
    del payload
    guard = _project_guard(project_root)
    project = guard.project
    if not check_qwen_adapter(project).get("current"):
        raise QwenInstallConflict("installed Qwen hook is not a current trusted byte set")
    if guard.read(QWEN_BINDING_RELATIVE) is None:
        return {"schema": "orchestrator-qwen-installed-hook/v1", "boundary": boundary, "project_root": str(project), "notice": None, "receipt": None, "stop_decision": {"permitted": True}, "acknowledged_by_hook": False, "transport_calls": []}
    binding = _load_binding_for_hook(project, guard)
    transport = SyntheticQwenTransport()
    router = ManagerEventRouter(
        Path(binding["queue_root"]), run_id=binding["run_id"], queue_id=binding["queue_id"],
        manager_session_id=binding["manager_session_id"], manager_thread_id=binding["manager_thread_id"],
        registration_id=binding["registration_id"], registration_generation=binding["registration_generation"],
    )
    router.validate_binding(router.registration)
    coordinator = DeliveryCoordinator(
        router=router, adapter=FutureHostFixture("qwen-bootstrap"),
        state_root=Path(binding["coordinator_root"]), registration_generation=binding["registration_generation"],
    )
    QwenAdapter(transport, coordinator)
    coordinator.restore()
    notice = coordinator.notice_for_wake()
    receipt: DeliveryReceipt | None = None
    stop_decision: dict[str, Any] | None = None
    if boundary == "post_tool_use":
        if notice is not None:
            receipt = coordinator.deliver_at_boundary(
                notice, boundary="post_tool_use"
            )
    elif boundary == "notification":
        if notice is not None:
            receipt = coordinator.deliver_at_boundary(notice, boundary="idle")
    else:
        stop_decision = coordinator.notification_stop_request().as_record()
    return {"schema": "orchestrator-qwen-installed-hook/v1", "boundary": boundary, "project_root": str(project), "notice": notice.as_record() if notice is not None else None, "receipt": receipt.as_record() if receipt is not None else None, "stop_decision": stop_decision, "acknowledged_by_hook": False, "transport_calls": list(transport.calls)}


__all__ = [
    "QWEN_ADAPTER_VERSION", "QWEN_BINDING_RELATIVE", "QWEN_INSTALL_MANIFEST_RELATIVE",
    "QWEN_INSTALL_MANIFEST_SCHEMA", "QWEN_PACKAGE_REVISION", "QWEN_SETTINGS_RELATIVE",
    "QwenInstallConflict", "QwenInstallRollback", "check_qwen_adapter", "install_qwen_adapter",
    "packaged_qwen_assets", "packaged_qwen_manifest", "run_installed_qwen_hook",
    "uninstall_qwen_adapter", "upgrade_qwen_adapter",
]
