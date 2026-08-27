"""Owned Claude Code installer: packaged assets + settings.json hooks merge.

Claude Code hook configuration lives in the project's ``.claude/settings.json``
under a nested matcher schema (each event maps to a list of ``{matcher,
hooks:[...]}`` groups), unlike Codex's flat ``.codex/hooks.json`` lists.  This
module mirrors the Codex installer's ownership/rollback transaction pattern and
adds the nested-group merge/subtract that Claude Code actually consumes.  The
project-local hooks only inspect an already-bound coordinator at
PostToolUse/Stop boundaries; nothing here claims arbitrary file-change
subscription or implicit project trust.
"""

from __future__ import annotations

import hashlib
import json
from importlib import resources
from pathlib import Path
from typing import Any, Mapping

from .codex_adapter import (
    CodexAdapterError,
    CodexInstallConflict,
    CodexInstallRollback,
    _ProjectMutationGuard,
    _hash,
    _json_bytes,
    _manifest_content_digest,
    _project_guard,
    _read_bytes,
)
from .claude_adapter import ClaudeAdapter, SyntheticClaudeTransport
from .host_adapters import (
    DeliveryCoordinator,
    DeliveryReceipt,
    FutureHostFixture,
)
from .models import iso_utc, utc_now
from .mutation import MutationConflict, safe_relative_path
from .notifications import ManagerEventRouter
from .stable_io import canonical_json


CLAUDE_INSTALL_MANIFEST_SCHEMA = "orchestrator-claude-install/v1"
CLAUDE_ADAPTER_VERSION = "claude-v1"
CLAUDE_PACKAGE_REVISION = "claude-assets-v2"
CLAUDE_INSTALL_MANIFEST_RELATIVE = Path(".claude") / "orchestrator-harness-adapter.json"
CLAUDE_SETTINGS_RELATIVE = Path(".claude") / "settings.json"
CLAUDE_BINDING_RELATIVE = Path(".claude") / "orchestrator-harness-binding.json"
_MANAGED_EVENT_NAMES = ("PostToolUse", "Stop")
_MANAGED_HOOK_IDS = ("orchestrator-harness-post-tool-use", "orchestrator-harness-stop")
_MAX_MANIFEST_BYTES = 512_000
_MAX_BINDING_BYTES = 128_000


def _claude_resource(name: str) -> bytes:
    try:
        return (
            resources.files("orchestrator_harness.assets.claude")
            .joinpath(name)
            .read_bytes()
        )
    except (FileNotFoundError, ModuleNotFoundError, OSError) as exc:
        raise CodexAdapterError(f"packaged Claude asset is unavailable: {name}") from exc


def packaged_claude_manifest() -> dict[str, Any]:
    try:
        raw = json.loads(_claude_resource("manifest.json").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexAdapterError("packaged Claude manifest is invalid") from exc
    if not isinstance(raw, dict) or raw.get("schema") != "orchestrator-claude-adapter/v1":
        raise CodexAdapterError("packaged Claude manifest has an invalid schema")
    if raw.get("package_revision") != CLAUDE_PACKAGE_REVISION:
        raise CodexAdapterError("packaged Claude manifest revision is inconsistent")
    files = raw.get("files")
    if not isinstance(files, list) or not files:
        raise CodexAdapterError("packaged Claude manifest has no assets")
    return raw


def packaged_claude_assets() -> dict[Path, bytes]:
    manifest = packaged_claude_manifest()
    assets: dict[Path, bytes] = {}
    for item in manifest["files"]:
        if not isinstance(item, Mapping):
            raise CodexAdapterError("packaged Claude asset entry is invalid")
        destination = item.get("destination")
        resource_name = item.get("resource")
        content_mode = item.get("content_mode")
        if not isinstance(destination, str) or not isinstance(resource_name, str):
            raise CodexAdapterError(
                "packaged Claude asset destination or resource is unsafe"
            )
        try:
            relative = safe_relative_path(destination)
            resource_relative = safe_relative_path(resource_name)
        except MutationConflict as exc:
            raise CodexAdapterError(
                "packaged Claude asset destination or resource is unsafe"
            ) from exc
        if not isinstance(content_mode, str) or content_mode != "utf8-lf":
            raise CodexAdapterError(
                f"packaged Claude asset content mode is unsupported: {relative}"
            )
        data = _claude_resource(resource_relative.as_posix())
        try:
            text = data.decode("utf-8", errors="strict")
        except (UnicodeDecodeError, AttributeError) as exc:
            raise CodexAdapterError(
                f"packaged Claude asset is not valid UTF-8: {relative}"
            ) from exc
        if "\ufeff" in text:
            raise CodexAdapterError(
                f"packaged Claude asset contains an unsupported UTF-8 BOM: {relative}"
            )
        if any(
            char == "\r" and (index + 1 == len(text) or text[index + 1] != "\n")
            for index, char in enumerate(text)
        ):
            raise CodexAdapterError(
                f"packaged Claude asset contains a lone carriage return: {relative}"
            )
        canonical = text.replace("\r\n", "\n").encode("utf-8")
        expected = item.get("sha256")
        if not isinstance(expected, str):
            raise CodexAdapterError(
                f"packaged Claude asset hash is missing or invalid: {relative}"
            )
        actual = hashlib.sha256(canonical).hexdigest()
        if expected != actual:
            raise CodexAdapterError(f"packaged Claude asset hash mismatch: {relative}")
        assets[relative] = canonical
    return assets


def _fragment_digest(fragment: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(fragment).encode("utf-8")).hexdigest()


def _managed_settings_fragment() -> dict[str, Any]:
    raw = json.loads(_claude_resource("settings.fragment.json").decode("utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("hooks"), dict):
        raise CodexAdapterError("packaged Claude settings fragment is invalid")
    for event_name, groups in raw["hooks"].items():
        if event_name not in _MANAGED_EVENT_NAMES:
            raise CodexAdapterError(
                "packaged Claude settings fragment has an unsupported event"
            )
        if not isinstance(groups, list) or not groups or any(
            not isinstance(group, dict) for group in groups
        ):
            raise CodexAdapterError("packaged Claude settings fragment entries are invalid")
        for group in groups:
            if set(group) != {"matcher", "hooks"} or not isinstance(
                group.get("matcher"), str
            ):
                raise CodexAdapterError(
                    "packaged Claude settings fragment entries are not closed"
                )
            inner = group.get("hooks")
            if not isinstance(inner, list) or not inner or any(
                not isinstance(item, dict) for item in inner
            ):
                raise CodexAdapterError(
                    "packaged Claude settings fragment hooks are invalid"
                )
            for item in inner:
                if set(item) != {"id", "type", "command"}:
                    raise CodexAdapterError(
                        "packaged Claude settings fragment hook entries are not closed"
                    )
                if not all(
                    isinstance(item.get(key), str) and item[key].strip()
                    for key in ("id", "type", "command")
                ):
                    raise CodexAdapterError(
                        "packaged Claude settings fragment hook identity is invalid"
                    )
    return raw


def _managed_paths(packaged: Mapping[Path, bytes]) -> list[str]:
    return sorted(
        {path.as_posix() for path in packaged}
        | {
            CLAUDE_SETTINGS_RELATIVE.as_posix(),
            CLAUDE_INSTALL_MANIFEST_RELATIVE.as_posix(),
        }
    )


def _inner_hook_ids(group: Mapping[str, Any]) -> set[str]:
    inner = group.get("hooks")
    if not isinstance(inner, list):
        return set()
    result: set[str] = set()
    for item in inner:
        if isinstance(item, Mapping) and isinstance(item.get("id"), str):
            result.add(item["id"])
    return result


def _managed_group_ids(groups: list[dict[str, Any]]) -> set[str]:
    return {inner["id"] for group in groups for inner in group["hooks"]}


def _parse_settings(raw: bytes | None) -> dict[str, Any]:
    if raw is None:
        return {}
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict(".claude/settings.json is not valid JSON") from exc
    if not isinstance(value, dict) or not isinstance(value.get("hooks"), dict):
        raise CodexInstallConflict("settings.json is not owned-shaped")
    return dict(value)


def _merge_settings(raw: bytes | None) -> bytes:
    """Deep-merge the managed PostToolUse/Stop matcher groups idempotently.

    A matcher group is "ours" iff any inner hook carries a managed id.  All
    unrelated events and unrelated matcher groups are preserved; an owned id
    pointing at a different command is an ambiguity and fails closed.
    """
    parsed = _parse_settings(raw)
    merged = dict(parsed)
    hooks = dict(merged.get("hooks") or {})
    fragment = _managed_settings_fragment()
    for event_name, managed_groups in fragment["hooks"].items():
        prior = hooks.get(event_name)
        if prior is None:
            prior = []
        if not isinstance(prior, list) or any(
            not isinstance(item, Mapping) for item in prior
        ):
            raise CodexInstallConflict(
                f".claude/settings.json {event_name} is not a list of objects"
            )
        rows: list[dict[str, Any]] = [dict(item) for item in prior]
        managed_ids = _managed_group_ids(managed_groups)
        for managed in managed_groups:
            ours = [row for row in rows if _inner_hook_ids(row) & managed_ids]
            if ours and any(row == managed for row in ours):
                continue
            if ours:
                raise CodexInstallConflict(
                    ".claude/settings.json contains an ambiguous owned hook group"
                )
            rows.append(dict(managed))
        hooks[event_name] = rows
    merged["hooks"] = hooks
    return _json_bytes(merged)


def _settings_state(raw: bytes | None) -> tuple[dict[str, Any], bool, list[str]]:
    parsed = _parse_settings(raw)
    if raw is None:
        return parsed, False, []
    hooks = parsed["hooks"]
    fragment = _managed_settings_fragment()
    present: set[str] = set()
    conflicts: set[str] = set()
    for event_name, managed_groups in fragment["hooks"].items():
        rows = hooks.get(event_name)
        if rows is None:
            continue
        if not isinstance(rows, list) or any(
            not isinstance(row, Mapping) for row in rows
        ):
            raise CodexInstallConflict(
                f".claude/settings.json {event_name} is not a list of objects"
            )
        managed_ids = _managed_group_ids(managed_groups)
        for row in rows:
            row_ids = _inner_hook_ids(row)
            if not (row_ids & managed_ids):
                continue
            if any(row == group for group in managed_groups):
                present.update(row_ids)
            else:
                conflicts.update(row_ids)
    managed_present = all(
        marker in present and marker not in conflicts for marker in _MANAGED_HOOK_IDS
    )
    return parsed, managed_present, sorted(conflicts)


def _subtract_settings(
    raw: bytes | None,
) -> tuple[bytes | None, list[str], list[str]]:
    """Remove only our matcher groups; preserve unrelated and conflicted groups.

    A now-empty event list is dropped.  If the hooks dict becomes empty and the
    file had no other top-level keys, ``(None, ...)`` is returned so the caller
    deletes the settings file.  A foreign command under one of our ids is a
    conflict and is never removed.
    """
    parsed = _parse_settings(raw)
    if raw is None:
        return None, [], []
    hooks = dict(parsed["hooks"])
    fragment = _managed_settings_fragment()
    removed: set[str] = set()
    conflicts: set[str] = set()
    for event_name, managed_groups in fragment["hooks"].items():
        rows = hooks.get(event_name)
        if not isinstance(rows, list):
            if rows is not None:
                raise CodexInstallConflict(
                    f".claude/settings.json {event_name} is not a list of objects"
                )
            continue
        kept: list[dict[str, Any]] = []
        managed_ids = _managed_group_ids(managed_groups)
        for row in rows:
            if not isinstance(row, Mapping):
                raise CodexInstallConflict(
                    f".claude/settings.json {event_name} contains invalid entries"
                )
            row_ids = _inner_hook_ids(row)
            if row_ids & managed_ids:
                if any(row == group for group in managed_groups):
                    removed.update(row_ids)
                    continue
                conflicts.update(row_ids)
                kept.append(dict(row))
            else:
                kept.append(dict(row))
        if kept:
            hooks[event_name] = kept
        else:
            hooks.pop(event_name, None)
    if not removed:
        return raw, [], sorted(conflicts)
    remaining = dict(hooks)
    other_keys = [key for key in parsed if key != "hooks"]
    if remaining or other_keys:
        merged = dict(parsed)
        merged["hooks"] = remaining
        return _json_bytes(merged), sorted(removed), sorted(conflicts)
    return None, sorted(removed), sorted(conflicts)


def _load_install_manifest(
    path: Path, *, guard: _ProjectMutationGuard | None = None
) -> dict[str, Any] | None:
    data = _read_bytes(path, guard)
    if data is None:
        return None
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict("installation manifest is malformed") from exc
    if (
        not isinstance(value, dict)
        or value.get("schema") != CLAUDE_INSTALL_MANIFEST_SCHEMA
    ):
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
            and _manifest_content_digest(manifest)
            == manifest.get("manifest_content_sha256")
        )
    except (CodexAdapterError, TypeError, ValueError):
        return False


def _validate_manifest_identity(
    manifest: Mapping[str, Any],
    project: Path,
    *,
    guard: _ProjectMutationGuard | None = None,
) -> str:
    """Validate a closed supported manifest and return its managed revision."""
    if (
        not isinstance(manifest, Mapping)
        or manifest.get("schema") != CLAUDE_INSTALL_MANIFEST_SCHEMA
    ):
        raise CodexInstallConflict("installation manifest is foreign or malformed")
    if (
        manifest.get("adapter") != "claude"
        or manifest.get("adapter_version") != CLAUDE_ADAPTER_VERSION
    ):
        raise CodexInstallConflict(
            "installation manifest belongs to another adapter revision"
        )
    if manifest.get("project_root") != str(project):
        raise CodexInstallConflict("installation manifest project identity differs")
    if (
        guard is not None
        and manifest.get("project_identity")
        != f"{guard._project_identity[0]}:{guard._project_identity[1]}"
    ):
        raise CodexInstallConflict("installation manifest directory identity differs")

    revision = manifest.get("package_revision")
    if revision != CLAUDE_PACKAGE_REVISION:
        raise CodexInstallConflict("installation manifest revision is unsupported")
    packaged = packaged_claude_assets()
    expected_paths = _managed_paths(packaged)
    if manifest.get("managed_paths") != expected_paths:
        raise CodexInstallConflict(
            "installation manifest managed path set is not supported"
        )
    expected_assets = {
        path.as_posix(): _hash(data) for path, data in packaged.items()
    }
    if manifest.get("managed_assets") != expected_assets:
        raise CodexInstallConflict(
            "installation manifest managed asset identities are not supported"
        )
    expected_fragment = _managed_settings_fragment()
    if manifest.get("managed_settings_fragment") != expected_fragment:
        raise CodexInstallConflict(
            "installation manifest settings fragment is not supported"
        )
    if manifest.get("managed_settings_fragment_sha256") != _fragment_digest(
        expected_fragment
    ):
        raise CodexInstallConflict(
            "installation manifest settings fragment identity is invalid"
        )
    if (
        manifest.get("settings_path") != CLAUDE_SETTINGS_RELATIVE.as_posix()
        or manifest.get("manifest_path")
        != CLAUDE_INSTALL_MANIFEST_RELATIVE.as_posix()
    ):
        raise CodexInstallConflict(
            "installation manifest destination contract is invalid"
        )
    if not isinstance(manifest.get("settings_preexisting"), bool):
        raise CodexInstallConflict(
            "installation manifest pre-existing settings fact is invalid"
        )
    installed = manifest.get("installed_content_sha256")
    if not isinstance(installed, Mapping) or set(installed) != set(
        expected_assets
    ) | {CLAUDE_SETTINGS_RELATIVE.as_posix()}:
        raise CodexInstallConflict(
            "installation manifest installed content set is invalid"
        )
    trust = manifest.get("trust")
    if not isinstance(trust, Mapping) or set(trust) != {
        "project_layer",
        "synthetic_self_test",
        "hook_review",
    }:
        raise CodexInstallConflict("installation manifest trust state is invalid")
    if set(manifest) != {
        "schema",
        "adapter",
        "adapter_version",
        "package_revision",
        "project_root",
        "project_identity",
        "managed_paths",
        "managed_assets",
        "managed_settings_fragment",
        "managed_settings_fragment_sha256",
        "settings_path",
        "manifest_path",
        "settings_preexisting",
        "installed_content_sha256",
        "trust",
        "installed_utc",
        "manifest_content_sha256",
    }:
        raise CodexInstallConflict("installation manifest has an unsupported field")
    if _manifest_content_digest(manifest) != manifest.get("manifest_content_sha256"):
        raise CodexInstallConflict(
            "installation manifest content identity is invalid"
        )
    return revision


def _manifest_result(project: Path) -> dict[str, Any]:
    return {
        "schema": CLAUDE_INSTALL_MANIFEST_SCHEMA,
        "adapter": "claude",
        "adapter_version": CLAUDE_ADAPTER_VERSION,
        "package_revision": CLAUDE_PACKAGE_REVISION,
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


def check_claude_adapter(project_root: str | Path) -> dict[str, Any]:
    guard = _project_guard(project_root)
    project = guard.project
    result = _manifest_result(project)
    manifest_path = guard.path(CLAUDE_INSTALL_MANIFEST_RELATIVE)
    try:
        manifest = _load_install_manifest(manifest_path, guard=guard)
        if manifest is None:
            return result
        revision = _validate_manifest_identity(manifest, project, guard=guard)
    except CodexInstallConflict as exc:
        result.update(
            {"installed": True, "ownership": "foreign_or_ambiguous", "reason": str(exc)}
        )
        return result
    result.update(
        {
            "installed": True,
            "ownership": "owned",
            "manifest_revision": revision,
            "synthetic_self_test": (manifest.get("trust") or {}).get(
                "synthetic_self_test", "not_run"
            ),
            "hook_review": (manifest.get("trust") or {}).get(
                "hook_review", "not-reviewed"
            ),
        }
    )
    manifest_current = _manifest_is_unchanged(manifest_path, manifest, guard=guard)
    packaged = packaged_claude_assets()
    expected_assets = {
        path.as_posix(): _hash(data) for path, data in packaged.items()
    }
    installed_hashes = manifest.get("installed_content_sha256", {})
    rows: list[dict[str, Any]] = []
    current = manifest_current and revision == CLAUDE_PACKAGE_REVISION
    for relative in _managed_paths(packaged):
        data = guard.read(relative)
        if relative == CLAUDE_INSTALL_MANIFEST_RELATIVE.as_posix():
            actual = _manifest_content_digest(manifest) if manifest_current else None
            expected = manifest.get("manifest_content_sha256")
        else:
            actual = _hash(data)
            expected = (
                expected_assets.get(relative)
                if relative in expected_assets
                else installed_hashes.get(relative)
            )
        same = actual == expected
        if relative in expected_assets and not same:
            current = False
        rows.append(
            {
                "path": relative,
                "installed_sha256": expected,
                "current_sha256": actual,
                "unchanged": same,
                "editable": False,
            }
        )
    try:
        _, settings_present, settings_conflicts = _settings_state(
            guard.read(CLAUDE_SETTINGS_RELATIVE)
        )
        managed_settings_present = settings_present and not settings_conflicts
    except CodexInstallConflict:
        managed_settings_present = False
    current = current and managed_settings_present
    result.update(
        {
            "managed_paths": rows,
            "owned_paths": rows,
            "current": current,
            "manifest_current": manifest_current,
            "settings_path": CLAUDE_SETTINGS_RELATIVE.as_posix(),
            "managed_settings_present": managed_settings_present,
        }
    )
    return result


def install_claude_adapter(
    project_root: str | Path, *, upgrade: bool = False
) -> dict[str, Any]:
    guard = _project_guard(project_root)
    guard.ensure_directory(Path(".claude"))
    guard.ensure_directory(Path(".claude") / "hooks")
    project = guard.project
    packaged = packaged_claude_assets()
    manifest_path = guard.path(CLAUDE_INSTALL_MANIFEST_RELATIVE, require_parent=True)
    existing_manifest = _load_install_manifest(manifest_path, guard=guard)
    existing_revision: str | None = None
    if existing_manifest is not None:
        existing_revision = _validate_manifest_identity(
            existing_manifest, project, guard=guard
        )
        if not _manifest_is_unchanged(manifest_path, existing_manifest, guard=guard):
            raise CodexInstallConflict(
                "owned installation manifest was modified; refusing overwrite"
            )
        if (
            existing_revision == CLAUDE_PACKAGE_REVISION
            and check_claude_adapter(project)["current"]
        ):
            current = check_claude_adapter(project)
            current["operation"] = "upgrade" if upgrade else "install"
            current["changed"] = []
            current["idempotent"] = True
            return current
        if existing_revision != CLAUDE_PACKAGE_REVISION and not upgrade:
            raise CodexInstallConflict(
                "an older owned Claude adapter requires explicit upgrade"
            )
        old_assets = {
            path.as_posix(): _hash(data) for path, data in packaged.items()
        }
        for relative, expected in old_assets.items():
            if _hash(guard.read(relative)) != expected:
                raise CodexInstallConflict(
                    f"owned file was modified; refusing overwrite: {relative}"
                )
    else:
        # _load_install_manifest rejects any regular foreign manifest before
        # this branch.  Packaged hook destinations may not already be claimed.
        for relative in packaged:
            if guard.read(relative) is not None:
                raise CodexInstallConflict(
                    f"unmanaged hook destination already exists: {relative}"
                )

    target_relatives = [
        *packaged,
        CLAUDE_SETTINGS_RELATIVE,
        CLAUDE_INSTALL_MANIFEST_RELATIVE,
    ]
    original_states = {
        relative: guard.snapshot(relative) for relative in target_relatives
    }
    originals = {relative: state.content for relative, state in original_states.items()}
    changed: list[str] = []
    try:
        for relative, data in packaged.items():
            if originals[relative] != data:
                guard.atomic_replace(relative, data, expected=original_states[relative])
                changed.append(str(relative))
        settings_data = _merge_settings(originals[CLAUDE_SETTINGS_RELATIVE])
        if originals[CLAUDE_SETTINGS_RELATIVE] != settings_data:
            guard.atomic_replace(
                CLAUDE_SETTINGS_RELATIVE,
                settings_data,
                expected=original_states[CLAUDE_SETTINGS_RELATIVE],
            )
            changed.append(CLAUDE_SETTINGS_RELATIVE.as_posix())
        expected_fragment = _managed_settings_fragment()
        managed_assets = {
            relative.as_posix(): _hash(value) for relative, value in packaged.items()
        }
        installed_hashes: dict[str, str | None] = dict(managed_assets)
        installed_hashes[CLAUDE_SETTINGS_RELATIVE.as_posix()] = _hash(settings_data)
        manifest = {
            "schema": CLAUDE_INSTALL_MANIFEST_SCHEMA,
            "adapter": "claude",
            "adapter_version": CLAUDE_ADAPTER_VERSION,
            "package_revision": CLAUDE_PACKAGE_REVISION,
            "project_root": str(project),
            "project_identity": f"{guard._project_identity[0]}:{guard._project_identity[1]}",
            "managed_paths": _managed_paths(packaged),
            "managed_assets": managed_assets,
            "managed_settings_fragment": expected_fragment,
            "managed_settings_fragment_sha256": _fragment_digest(expected_fragment),
            "settings_path": CLAUDE_SETTINGS_RELATIVE.as_posix(),
            "manifest_path": CLAUDE_INSTALL_MANIFEST_RELATIVE.as_posix(),
            "settings_preexisting": originals[CLAUDE_SETTINGS_RELATIVE] is not None,
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
            CLAUDE_INSTALL_MANIFEST_RELATIVE,
            _json_bytes(manifest),
            expected=original_states[CLAUDE_INSTALL_MANIFEST_RELATIVE],
        )
        changed.append(CLAUDE_INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for relative, data in originals.items():
            try:
                guard.restore(relative, data)
            except Exception as rollback_exc:  # pragma: no cover - defensive evidence
                rollback_errors.append(f"{relative}: {type(rollback_exc).__name__}")
        detail = f"Claude install rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise CodexInstallRollback(detail) from exc
    result = check_claude_adapter(project)
    result.update(
        {
            "operation": "upgrade" if upgrade else "install",
            "changed": changed,
            "idempotent": not bool(changed),
        }
    )
    return result


def upgrade_claude_adapter(project_root: str | Path) -> dict[str, Any]:
    return install_claude_adapter(project_root, upgrade=True)


def uninstall_claude_adapter(project_root: str | Path) -> dict[str, Any]:
    guard = _project_guard(project_root)
    project = guard.project
    manifest = _load_install_manifest(
        guard.path(CLAUDE_INSTALL_MANIFEST_RELATIVE), guard=guard
    )
    if manifest is None:
        return {
            "schema": CLAUDE_INSTALL_MANIFEST_SCHEMA,
            "adapter": "claude",
            "project_root": str(project),
            "operation": "uninstall",
            "installed": False,
            "removed": [],
            "preserved_modified": [],
            "project_trust": "unverified",
        }
    _validate_manifest_identity(manifest, project, guard=guard)
    manifest_path = guard.path(CLAUDE_INSTALL_MANIFEST_RELATIVE, require_parent=True)
    if not _manifest_is_unchanged(manifest_path, manifest, guard=guard):
        raise CodexInstallConflict(
            "installation manifest was modified; refusing uninstall"
        )
    packaged = packaged_claude_assets()
    expected_assets = {
        path.as_posix(): _hash(data) for path, data in packaged.items()
    }
    removed: list[str] = []
    preserved: list[str] = []
    original_states = {
        relative: guard.snapshot(relative)
        for relative in [
            *packaged,
            CLAUDE_SETTINGS_RELATIVE,
            CLAUDE_INSTALL_MANIFEST_RELATIVE,
            CLAUDE_BINDING_RELATIVE,
        ]
    }
    originals = {relative: state.content for relative, state in original_states.items()}
    try:
        for relative, _ in packaged.items():
            expected = expected_assets.get(relative.as_posix())
            if _hash(originals[relative]) == expected:
                guard.delete(relative, expected=original_states[relative])
                removed.append(relative.as_posix())
            elif originals[relative] is not None:
                preserved.append(relative.as_posix())
        settings_data, settings_removed, settings_conflicts = _subtract_settings(
            originals[CLAUDE_SETTINGS_RELATIVE]
        )
        if settings_removed:
            if settings_data is not None:
                parsed, _, _ = _settings_state(settings_data)
                no_unrelated = set(parsed) == {"hooks"} and all(
                    not parsed["hooks"].get(name, []) for name in parsed["hooks"]
                )
                if no_unrelated and manifest.get("settings_preexisting") is False:
                    guard.delete(
                        CLAUDE_SETTINGS_RELATIVE,
                        expected=original_states[CLAUDE_SETTINGS_RELATIVE],
                    )
                else:
                    guard.atomic_replace(
                        CLAUDE_SETTINGS_RELATIVE,
                        settings_data,
                        expected=original_states[CLAUDE_SETTINGS_RELATIVE],
                    )
            else:
                guard.delete(
                    CLAUDE_SETTINGS_RELATIVE,
                    expected=original_states[CLAUDE_SETTINGS_RELATIVE],
                )
            removed.append(CLAUDE_SETTINGS_RELATIVE.as_posix())
        if settings_conflicts:
            preserved.append(CLAUDE_SETTINGS_RELATIVE.as_posix())
        binding_data = originals[CLAUDE_BINDING_RELATIVE]
        if binding_data is not None:
            try:
                binding = json.loads(binding_data.decode("utf-8"))
                if (
                    isinstance(binding, dict)
                    and binding.get("project_root") == str(project)
                ):
                    guard.delete(
                        CLAUDE_BINDING_RELATIVE,
                        expected=original_states[CLAUDE_BINDING_RELATIVE],
                    )
                    removed.append(CLAUDE_BINDING_RELATIVE.as_posix())
                else:
                    preserved.append(CLAUDE_BINDING_RELATIVE.as_posix())
            except (CodexAdapterError, OSError, UnicodeDecodeError, json.JSONDecodeError):
                preserved.append(CLAUDE_BINDING_RELATIVE.as_posix())
        guard.delete(
            CLAUDE_INSTALL_MANIFEST_RELATIVE,
            expected=original_states[CLAUDE_INSTALL_MANIFEST_RELATIVE],
        )
        removed.append(CLAUDE_INSTALL_MANIFEST_RELATIVE.as_posix())
    except Exception as exc:
        rollback_errors: list[str] = []
        for relative, data in originals.items():
            try:
                guard.restore(relative, data)
            except Exception as rollback_exc:  # pragma: no cover - defensive evidence
                rollback_errors.append(f"{relative}: {type(rollback_exc).__name__}")
        detail = f"Claude uninstall rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise CodexInstallRollback(detail) from exc
    return {
        "schema": CLAUDE_INSTALL_MANIFEST_SCHEMA,
        "adapter": "claude",
        "project_root": str(project),
        "operation": "uninstall",
        "installed": True,
        "removed": sorted(removed),
        "preserved_modified": sorted(preserved),
        "project_trust": "unverified",
        "ownership_safe": True,
    }


def _load_binding_for_hook(
    project: Path, guard: _ProjectMutationGuard
) -> dict[str, Any]:
    data = guard.read(CLAUDE_BINDING_RELATIVE)
    if data is None:
        raise CodexInstallConflict("installed Claude hook has no harness binding")
    if len(data) > _MAX_BINDING_BYTES:
        raise CodexInstallConflict("Claude binding record is oversized")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexInstallConflict("Claude binding record is malformed") from exc
    if not isinstance(value, dict):
        raise CodexInstallConflict("Claude binding record is malformed")
    required = {
        "queue_root",
        "coordinator_root",
        "run_id",
        "queue_id",
        "manager_session_id",
        "manager_thread_id",
        "registration_id",
        "registration_generation",
    }
    if not required.issubset(value):
        raise CodexInstallConflict("Claude binding record is incomplete")
    if value.get("project_root") != str(project):
        raise CodexInstallConflict("Claude binding project identity is stale")
    return value


def run_installed_claude_hook(
    project_root: str | Path,
    *,
    boundary: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the actually installed, bound Claude safe-boundary route.

    ``post_tool_use`` delivers the sparse notice through the adapter's own
    transport seam; ``stop`` enforces the notification stop matrix.  With no
    persisted binding the route is honest ``SAFE_BOUNDARY_ONLY`` (nothing was
    delivered).  Transport delivery never acknowledges queue work.
    """
    if boundary not in {"post_tool_use", "stop"}:
        raise CodexAdapterError(
            "supported installed Claude hooks are post_tool_use and stop"
        )
    del payload
    guard = _project_guard(project_root)
    project = guard.project
    if not check_claude_adapter(project).get("current"):
        raise CodexInstallConflict(
            "installed Claude hook is not a current trusted byte set"
        )
    if guard.read(CLAUDE_BINDING_RELATIVE) is None:
        # No persisted runtime binding: the safe-boundary-only record is the
        # honest answer (nothing was delivered, stop is permitted).
        return {
            "schema": "orchestrator-claude-installed-hook/v1",
            "boundary": boundary,
            "project_root": str(project),
            "notice": None,
            "receipt": None,
            "stop_decision": {"permitted": True},
            "acknowledged_by_hook": False,
            "transport_calls": [],
        }
    binding = _load_binding_for_hook(project, guard)
    transport = SyntheticClaudeTransport()
    router = ManagerEventRouter(
        Path(binding["queue_root"]),
        run_id=binding["run_id"],
        queue_id=binding["queue_id"],
        manager_session_id=binding["manager_session_id"],
        manager_thread_id=binding["manager_thread_id"],
        registration_id=binding["registration_id"],
        registration_generation=binding["registration_generation"],
    )
    router.validate_binding(router.registration)
    coordinator = DeliveryCoordinator(
        router=router,
        adapter=FutureHostFixture("claude-bootstrap"),
        state_root=Path(binding["coordinator_root"]),
        registration_generation=binding["registration_generation"],
    )
    ClaudeAdapter(transport, coordinator)
    coordinator.restore()
    notice = coordinator.notice_for_wake()
    receipt: DeliveryReceipt | None = None
    stop_decision: dict[str, Any] | None = None
    if boundary == "post_tool_use":
        if notice is not None:
            receipt = coordinator.deliver_at_boundary(
                notice, boundary="post_tool_use"
            )
    else:
        if hasattr(coordinator, "notification_stop_request"):
            stop_decision = coordinator.notification_stop_request().as_record()
        else:  # pragma: no cover - defensive fallback for a fixture coordinator
            stop_decision = {"permitted": True}
    return {
        "schema": "orchestrator-claude-installed-hook/v1",
        "boundary": boundary,
        "project_root": str(project),
        "notice": notice.as_record() if notice is not None else None,
        "receipt": receipt.as_record() if receipt is not None else None,
        "stop_decision": stop_decision,
        "acknowledged_by_hook": False,
        "transport_calls": list(transport.calls),
    }


__all__ = [
    "CLAUDE_ADAPTER_VERSION",
    "CLAUDE_INSTALL_MANIFEST_SCHEMA",
    "CLAUDE_INSTALL_MANIFEST_RELATIVE",
    "CLAUDE_PACKAGE_REVISION",
    "CLAUDE_SETTINGS_RELATIVE",
    "check_claude_adapter",
    "install_claude_adapter",
    "packaged_claude_assets",
    "packaged_claude_manifest",
    "run_installed_claude_hook",
    "uninstall_claude_adapter",
    "upgrade_claude_adapter",
]
