"""Immutable, byte-exact prompt bundle primitives.

The controller only sends bytes that have been verified against this bundle.  A
bundle is deliberately a small value object instead of a prompt templating
system: callers provide the ordered components and the resulting bytes are
bound by both their individual hashes and a manifest hash.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .stable_io import canonical_json


PROMPT_BUNDLE_SCHEMA = "orchestrator-prompt-bundle/v1"
_SHA256_LENGTH = 64


class PromptBundleError(ValueError):
    """Raised when a prompt bundle is malformed, stale, or tampered with."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _digest(value: Any) -> str:
    return _sha256(canonical_json(value).encode("utf-8"))


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PromptBundleError(f"{name} must be a non-empty string")
    return value.strip()


def _digest_text(value: object, name: str) -> str:
    result = _text(value, name).lower()
    if len(result) != _SHA256_LENGTH or any(c not in "0123456789abcdef" for c in result):
        raise PromptBundleError(f"{name} must be a SHA-256 hex digest")
    return result


def _safe_component_path(path: Path, root: Path) -> Path:
    if not path.is_absolute():
        path = root / path
    if path.is_symlink():
        raise PromptBundleError("prompt component path cannot be a symlink")
    candidate = path.expanduser().resolve(strict=False)
    root_resolved = root.expanduser().resolve(strict=False)
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise PromptBundleError("prompt component path escapes run root") from exc
    if not candidate.is_file():
        raise PromptBundleError("prompt component path must be an existing regular file")
    return candidate


@dataclass(frozen=True)
class PromptComponent:
    """One ordered prompt byte source."""

    component_id: str
    content: bytes
    path: Path | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.component_id, str) or not self.component_id.strip():
            raise PromptBundleError("prompt component ID must be non-empty")
        if not isinstance(self.content, bytes):
            raise PromptBundleError("prompt component content must be bytes")

    @property
    def sha256(self) -> str:
        return _sha256(self.content)

    @property
    def size(self) -> int:
        return len(self.content)

    def record(self, *, ordinal: int) -> dict[str, Any]:
        return {
            "id": self.component_id,
            "ordinal": ordinal,
            "path": str(self.path) if self.path is not None else None,
            "sha256": self.sha256,
            "size": self.size,
        }


@dataclass(frozen=True)
class PromptBundle:
    """A frozen ordered prompt and the identities that bind it."""

    workflow_id: str
    task_card_id: str
    profile_id: str
    components: tuple[PromptComponent, ...]
    final_bytes: bytes
    final_sha256: str
    bundle_sha256: str
    schema: str = PROMPT_BUNDLE_SCHEMA
    version: int = 1

    def __post_init__(self) -> None:
        if self.schema != PROMPT_BUNDLE_SCHEMA or self.version != 1:
            raise PromptBundleError("unsupported prompt bundle schema")
        _text(self.workflow_id, "workflow_id")
        _text(self.task_card_id, "task_card_id")
        _text(self.profile_id, "profile_id")
        if not self.components:
            raise PromptBundleError("prompt bundle must contain at least one component")
        ids = [component.component_id for component in self.components]
        if len(ids) != len(set(ids)):
            raise PromptBundleError("prompt component IDs must be unique")
        composed = b"".join(component.content for component in self.components)
        if composed != self.final_bytes:
            raise PromptBundleError("prompt final bytes do not match ordered components")
        if _sha256(self.final_bytes) != self.final_sha256:
            raise PromptBundleError("prompt final SHA-256 does not match bytes")
        if self.bundle_sha256 != self.manifest_sha256():
            raise PromptBundleError("prompt bundle SHA-256 does not match manifest")

    @property
    def content_sha256(self) -> str:
        """Alias used by status/result records for the exact sent buffer."""

        return self.final_sha256

    def manifest(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "version": self.version,
            "workflow_id": self.workflow_id,
            "task_card_id": self.task_card_id,
            "profile_id": self.profile_id,
            "components": [
                component.record(ordinal=index)
                for index, component in enumerate(self.components)
            ],
            "final_sha256": self.final_sha256,
            "final_size": len(self.final_bytes),
        }

    def manifest_sha256(self) -> str:
        return _digest(self.manifest())

    def to_record(self) -> dict[str, Any]:
        record = dict(self.manifest())
        record["bundle_sha256"] = self.bundle_sha256
        return record

    def verify(self, components: Sequence[PromptComponent]) -> None:
        candidate = compose_prompt_bundle(
            workflow_id=self.workflow_id,
            task_card_id=self.task_card_id,
            profile_id=self.profile_id,
            components=components,
        )
        if candidate.to_record() != self.to_record():
            raise PromptBundleError("prompt bundle identity changed")


def compose_prompt_bundle(
    *,
    workflow_id: str,
    task_card_id: str,
    profile_id: str,
    components: Iterable[PromptComponent | tuple[str, bytes]],
) -> PromptBundle:
    normalized: list[PromptComponent] = []
    for item in components:
        if isinstance(item, PromptComponent):
            normalized.append(item)
        elif isinstance(item, tuple) and len(item) == 2:
            normalized.append(PromptComponent(item[0], item[1]))
        else:
            raise PromptBundleError("components must be PromptComponent values or (id, bytes) pairs")
    final_bytes = b"".join(component.content for component in normalized)
    final_sha256 = _sha256(final_bytes)
    provisional = PromptBundle.__new__(PromptBundle)
    object.__setattr__(provisional, "workflow_id", _text(workflow_id, "workflow_id"))
    object.__setattr__(provisional, "task_card_id", _text(task_card_id, "task_card_id"))
    object.__setattr__(provisional, "profile_id", _text(profile_id, "profile_id"))
    object.__setattr__(provisional, "components", tuple(normalized))
    object.__setattr__(provisional, "final_bytes", final_bytes)
    object.__setattr__(provisional, "final_sha256", final_sha256)
    object.__setattr__(provisional, "schema", PROMPT_BUNDLE_SCHEMA)
    object.__setattr__(provisional, "version", 1)
    object.__setattr__(provisional, "bundle_sha256", "")
    bundle_sha256 = provisional.manifest_sha256()
    object.__setattr__(provisional, "bundle_sha256", bundle_sha256)
    PromptBundle.__post_init__(provisional)
    return provisional


def bundle_from_record(record: Mapping[str, Any], *, run_root: Path) -> PromptBundle:
    """Read and verify a path-bound bundle record under ``run_root``."""

    required = {
        "schema",
        "version",
        "workflow_id",
        "task_card_id",
        "profile_id",
        "components",
        "final_sha256",
        "final_size",
        "bundle_sha256",
    }
    if set(record) != required:
        raise PromptBundleError("prompt bundle has an invalid closed shape")
    if record.get("schema") != PROMPT_BUNDLE_SCHEMA or record.get("version") != 1:
        raise PromptBundleError("prompt bundle schema/version is invalid")
    components_value = record.get("components")
    if not isinstance(components_value, list) or not components_value:
        raise PromptBundleError("prompt bundle components must be a non-empty list")
    components: list[PromptComponent] = []
    for ordinal, raw in enumerate(components_value):
        if not isinstance(raw, Mapping) or set(raw) != {"id", "ordinal", "path", "sha256", "size"}:
            raise PromptBundleError("prompt bundle component has an invalid closed shape")
        if raw.get("ordinal") != ordinal:
            raise PromptBundleError("prompt bundle component order is not contiguous")
        path_value = raw.get("path")
        if not isinstance(path_value, str) or not path_value:
            raise PromptBundleError("prompt bundle components require path-bound sources")
        path = _safe_component_path(Path(path_value), run_root)
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise PromptBundleError(f"cannot read prompt component: {exc}") from exc
        expected_size = raw.get("size")
        if not isinstance(expected_size, int) or isinstance(expected_size, bool) or expected_size != len(content):
            raise PromptBundleError("prompt component size does not match bytes")
        if _digest_text(raw.get("sha256"), "prompt component sha256") != _sha256(content):
            raise PromptBundleError("prompt component bytes do not match sha256")
        components.append(PromptComponent(_text(raw.get("id"), "prompt component id"), content, path))
    bundle = compose_prompt_bundle(
        workflow_id=_text(record.get("workflow_id"), "workflow_id"),
        task_card_id=_text(record.get("task_card_id"), "task_card_id"),
        profile_id=_text(record.get("profile_id"), "profile_id"),
        components=components,
    )
    if not isinstance(record.get("final_size"), int) or record["final_size"] != len(bundle.final_bytes):
        raise PromptBundleError("prompt final size does not match bytes")
    if bundle.to_record() != dict(record):
        raise PromptBundleError("prompt bundle manifest is stale or tampered")
    return bundle


def prompt_bundle_record_from_paths(
    *,
    workflow_id: str,
    task_card_id: str,
    profile_id: str,
    paths: Sequence[tuple[str, Path]],
    run_root: Path,
) -> dict[str, Any]:
    components = []
    for component_id, path in paths:
        safe = _safe_component_path(path, run_root)
        components.append(PromptComponent(component_id, safe.read_bytes(), safe))
    return compose_prompt_bundle(
        workflow_id=workflow_id,
        task_card_id=task_card_id,
        profile_id=profile_id,
        components=components,
    ).to_record()


__all__ = [
    "PROMPT_BUNDLE_SCHEMA",
    "PromptBundle",
    "PromptBundleError",
    "PromptComponent",
    "bundle_from_record",
    "compose_prompt_bundle",
    "prompt_bundle_record_from_paths",
]
