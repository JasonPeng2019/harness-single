"""Authorized, isolated SQLite snapshot and restore for one MemoryStore.

Public API: ``SnapshotService(authorizer, dependency_verifier=None)`` exposes
``export(source, artifact, *, scope, credential, dependencies=())`` and
``restore(artifact, target, *, scope, credential, required_capabilities=("local",))``.
``source`` is an initialized MemoryStore; ``target`` is a closed MemoryStore
whose path does not exist. The constructor-bound authorizer receives
``(action, exact_scope, store_path, credential)`` and must return literal True
for the entire exact MemoryStore file at that path and four-field scope;
no subset or multi-tenant filtering is performed.
The optional bound dependency verifier receives ``(capability, reference,
exact_scope)`` and must return literal True. Credentials and dependency refs
are per-call data, never authority. Neither operation replays effects.

An artifact directory contains ``state.sqlite3`` and ``manifest.json``. The
manifest's ``content_hash`` binds its other canonical JSON fields and
``database_sha256`` binds exact backup bytes. ``store_schema_digest`` is the
current MemoryStore SQLite schema identity. ``completeness`` is incomplete
when a protected or remote dependency lacks verified readiness. External
services are not captured simultaneously with SQLite; callers must arrange
their own source-specific readiness proof before claiming those capabilities.
Restore returns ``{"capture": manifest, "restored": {"completeness": ...,
"capabilities": {"local": bool, "experience": bool, "atlas": bool},
"dependencies": [...]}}``. Capture facts remain unchanged; restored readiness
requires both capture-time and current verifier proof for each reference.
Remote capabilities without specific references remain unavailable.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from . import contracts
from .store import MemoryStore


class SnapshotError(RuntimeError):
    """Capture or restore preflight failed; no target was installed."""


class SnapshotAuthorizationError(SnapshotError):
    """The fixed operator authority did not permit this operation."""


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _schema_digest(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE sql IS NOT NULL ORDER BY type, name"
    ).fetchall()
    return _digest([tuple(row) for row in rows])


def _expected_schema_digest() -> str:
    expected = MemoryStore(":memory:")
    expected.initialize()
    try:
        assert expected.connection is not None
        return _schema_digest(expected.connection)
    finally:
        expected.close()


def _inspect_database(path: Path) -> tuple[str, sqlite3.Connection]:
    try:
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise SnapshotError("snapshot database integrity_check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise SnapshotError("snapshot database foreign keys are inconsistent")
        schema = _schema_digest(connection)
        if schema != _expected_schema_digest():
            raise SnapshotError("snapshot database schema is incompatible")
        return schema, connection
    except (sqlite3.Error, OSError) as exc:
        if "connection" in locals():
            connection.close()
        raise SnapshotError(f"snapshot database cannot be verified: {exc}") from exc
    except BaseException:
        if "connection" in locals():
            connection.close()
        raise


def _facts(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, dict[str, str]]]:
    """Discover unresolved references and exact recovery statuses from backup bytes."""
    refs: set[tuple[str, str]] = set()
    for table in ("review_receipts", "reviewed_trajectories"):
        for (encoded,) in connection.execute(f"SELECT protected_source_refs FROM {table}"):
            for ref in json.loads(encoded):
                refs.add(("experience", str(ref)))
    for (identity,) in connection.execute("SELECT ingestion_id FROM experience_ingestions"):
        refs.add(("experience", f"everos:{identity}"))
    for (identity,) in connection.execute("SELECT publication_id FROM procedure_publications"):
        refs.add(("atlas", f"atlas-publication:{identity}"))
    for (identity,) in connection.execute("SELECT operation_id FROM procedure_remote_operations"):
        refs.add(("atlas", f"atlas-operation:{identity}"))
    for identity, kind in connection.execute(
        "SELECT operation_id, kind FROM effect_operations"
    ):
        if kind == "procedure_publication":
            refs.add(("atlas", f"atlas-effect:{identity}"))
        elif kind in ("experience_ingestion", "generated_skill_creation"):
            refs.add(("experience", f"everos-effect:{identity}"))
    pending = {
        "effect_operations": dict(connection.execute(
            "SELECT operation_id, status FROM effect_operations "
            "WHERE status IN ('pending', 'uncertain', 'in_flight')"
        ).fetchall()),
        "dispatch_operations": dict(connection.execute(
            "SELECT operation_id, status FROM operations WHERE status IN ('pending', 'ambiguous')"
        ).fetchall()),
        "procedure_remote_operations": dict(connection.execute(
            "SELECT operation_id, status FROM procedure_remote_operations "
            "WHERE status IN ('intent', 'ambiguous', 'revocation_pending')"
        ).fetchall()),
    }
    return ([{"capability": cap, "reference": ref} for cap, ref in sorted(refs)], pending)


class SnapshotService:
    """One fixed operator boundary for complete SQLite capture and fresh restore."""

    def __init__(
        self,
        authorizer: Callable[[str, dict[str, str], Path, object], bool] | None = None,
        dependency_verifier: Callable[[str, str, dict[str, str]], bool] | None = None,
    ) -> None:
        if not callable(authorizer):
            raise SnapshotAuthorizationError("operator authorizer must be configured")
        if dependency_verifier is not None and not callable(dependency_verifier):
            raise TypeError("dependency verifier must be callable")
        self._authorizer = authorizer
        self._dependency_verifier = dependency_verifier

    def _authorize(self, action: str, scope: Mapping[str, str], path: Path, credential: object) -> dict[str, str]:
        exact = contracts.normalize_experience_scope(scope)
        if set(scope) != set(exact):
            raise SnapshotAuthorizationError("scope must have exactly four identity fields")
        try:
            permitted = self._authorizer(action, exact, path, credential)
        except Exception as exc:
            raise SnapshotAuthorizationError("operator authorization failed") from exc
        if permitted is not True:
            raise SnapshotAuthorizationError("operator authorization denied")
        return exact

    def _ready(self, capability: str, reference: str, scope: dict[str, str]) -> bool:
        if self._dependency_verifier is None:
            return False
        try:
            return self._dependency_verifier(capability, reference, scope) is True
        except Exception:
            return False

    def _after_backup(self) -> None:
        """Internal fault seam after consistent backup, before publication."""

    def export(
        self, source: MemoryStore, artifact: str | Path, *, scope: Mapping[str, str],
        credential: object, dependencies: Iterable[Mapping[str, str]] = (),
    ) -> dict[str, Any]:
        # MemoryStore retains the actual opened identity for relative paths
        # when the process later changes cwd.
        opened_path = getattr(source, "_opened_path", None)
        # Preserve the exact opened identity for a relative store after cwd
        # changes. For an absolute caller path, normalize filesystem aliases
        # (notably macOS /var -> /private/var) before recording the manifest.
        source_path = (
            source.path.resolve()
            if source.path.is_absolute()
            else Path(opened_path)
            if opened_path is not None
            else source.path.resolve()
        )
        exact = self._authorize("export", scope, source_path, credential)
        if source.connection is None or not source_path.is_file():
            raise SnapshotError("source MemoryStore must be initialized")
        destination = Path(artifact).resolve()
        if destination.exists() or destination.parent == source_path.parent or not destination.parent.is_dir():
            raise SnapshotError("artifact must be a fresh directory outside the source root")
        stage = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination.parent))
        try:
            database = stage / "state.sqlite3"
            output = sqlite3.connect(database)
            try:
                source.connection.backup(output)
            finally:
                output.close()
            self._after_backup()
            schema, connection = _inspect_database(database)
            try:
                discovered, pending = _facts(connection)
            finally:
                connection.close()
            refs = {(item["capability"], item["reference"]) for item in discovered}
            for item in dependencies:
                capability, reference = item["capability"], item["reference"]
                if capability not in ("experience", "atlas") or not isinstance(reference, str) or not reference:
                    raise SnapshotError("invalid dependency reference")
                refs.add((capability, reference))
            readiness = [
                {"capability": cap, "reference": ref, "ready": self._ready(cap, ref, exact)}
                for cap, ref in sorted(refs)
            ]
            manifest: dict[str, Any] = {
                "schema": "snapshot/v1", "store_format": "memory_harness_sqlite/v1",
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "source": {"root": str(source_path.parent), "path": str(source_path),
                           "scope": exact, "scope_boundary": "whole_memory_store_file"},
                "database_file": "state.sqlite3", "database_sha256": _file_digest(database),
                "store_schema_digest": schema,
                "completeness": "complete" if all(item["ready"] for item in readiness) else "incomplete",
                "dependencies": readiness,
                "pending_effects": pending["effect_operations"],
                "pending_dispatches": pending["dispatch_operations"],
                "pending_procedure_remote": pending["procedure_remote_operations"],
                "effect_recovery": "preserved_exact_reconciliation_required_no_automatic_replay",
                "remote_simultaneity": False,
            }
            manifest["content_hash"] = _digest(manifest)
            (stage / "manifest.json").write_bytes(_json_bytes(manifest))
            with database.open("r+b") as handle:
                os.fsync(handle.fileno())
            with (stage / "manifest.json").open("r+b") as handle:
                os.fsync(handle.fileno())
            if destination.exists():
                raise SnapshotError("artifact collision")
            stage.rename(destination)
            return manifest
        finally:
            if stage.exists():
                shutil.rmtree(stage)

    def restore(
        self, artifact: str | Path, target: MemoryStore, *, scope: Mapping[str, str],
        credential: object, required_capabilities: Iterable[str] = ("local",),
    ) -> dict[str, Any]:
        target_path = target.path.resolve()
        exact = self._authorize("restore", scope, target_path, credential)
        if target.connection is not None or target_path.exists():
            raise SnapshotError("target MemoryStore must be closed and nonexistent")
        artifact_path = Path(artifact).resolve()
        try:
            manifest = json.loads((artifact_path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise SnapshotError("snapshot manifest is missing or invalid") from exc
        if not isinstance(manifest, dict) or manifest.get("schema") != "snapshot/v1":
            raise SnapshotError("snapshot manifest schema is incompatible")
        if manifest.get("store_format") != "memory_harness_sqlite/v1":
            raise SnapshotError("snapshot store format is incompatible")
        content_hash = manifest.get("content_hash")
        if not isinstance(content_hash, str) or content_hash != _digest({k: v for k, v in manifest.items() if k != "content_hash"}):
            raise SnapshotError("snapshot manifest integrity mismatch")
        source = manifest.get("source")
        if (not isinstance(source, dict) or source.get("scope") != exact or
                source.get("scope_boundary") != "whole_memory_store_file"):
            raise SnapshotError("snapshot identity scope mismatch")
        raw_source_path = source.get("path")
        if not isinstance(raw_source_path, str) or not Path(raw_source_path).is_absolute():
            raise SnapshotError("snapshot source path is not absolute")
        source_path = Path(raw_source_path)
        if str(source_path.absolute()) != raw_source_path:
            raise SnapshotError("snapshot source path identity is not canonical")
        physical_source = source_path.resolve()
        physical_target = target_path.resolve()
        if (source.get("root") != str(source_path.parent) or
                physical_source.parent in physical_target.parents or
                physical_target == physical_source):
            raise SnapshotError("snapshot source or target root collision")
        if artifact_path == target_path or artifact_path in target_path.parents or target_path in artifact_path.parents:
            raise SnapshotError("snapshot artifact and target collide")
        if manifest.get("database_file") != "state.sqlite3":
            raise SnapshotError("unsupported snapshot database file")
        database = artifact_path / "state.sqlite3"
        if not database.is_file() or _file_digest(database) != manifest.get("database_sha256"):
            raise SnapshotError("snapshot database digest mismatch")
        schema, connection = _inspect_database(database)
        try:
            discovered, pending = _facts(connection)
        finally:
            connection.close()
        if schema != manifest.get("store_schema_digest"):
            raise SnapshotError("snapshot schema identity mismatch")
        if (pending["effect_operations"] != manifest.get("pending_effects") or
                pending["dispatch_operations"] != manifest.get("pending_dispatches") or
                pending["procedure_remote_operations"] != manifest.get("pending_procedure_remote")):
            raise SnapshotError("snapshot recovery facts mismatch")
        listed = manifest.get("dependencies")
        if not isinstance(listed, list) or not all(isinstance(item, dict) for item in listed):
            raise SnapshotError("snapshot dependencies are invalid")
        pairs = {(item.get("capability"), item.get("reference")) for item in listed}
        if not {(item["capability"], item["reference"]) for item in discovered} <= pairs:
            raise SnapshotError("snapshot required dependency references are missing")
        if manifest.get("completeness") != ("complete" if all(item.get("ready") is True for item in listed) else "incomplete"):
            raise SnapshotError("snapshot completeness is inconsistent")
        requested = set(required_capabilities)
        if not requested <= {"local", "experience", "atlas"}:
            raise SnapshotError("unknown requested capability")
        current = [
            {"capability": item["capability"], "reference": item["reference"],
             "ready": item.get("ready") is True and
             self._ready(item["capability"], item["reference"], exact)}
            for item in listed
        ]
        capabilities = {"local": True}
        for capability in ("experience", "atlas"):
            covered = [item for item in current if item["capability"] == capability]
            capabilities[capability] = bool(covered) and all(item["ready"] for item in covered)
        if any(not capabilities[capability] for capability in requested):
            raise SnapshotError("required snapshot dependency is unavailable")
        if target_path.exists():
            raise SnapshotError("target collision")
        # Copy in the destination filesystem, then publish one nonexistent file
        # by atomic hard link. link() fails if another writer wins the path.
        target_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{target_path.name}-", dir=target_path.parent)
        staged = Path(temporary)
        try:
            with os.fdopen(fd, "wb") as output, database.open("rb") as input_file:
                shutil.copyfileobj(input_file, output)
                output.flush()
                os.fsync(output.fileno())
            if _file_digest(staged) != manifest["database_sha256"]:
                raise SnapshotError("staged target digest mismatch")
            os.link(staged, target_path)
        except FileExistsError as exc:
            raise SnapshotError("target collision") from exc
        finally:
            staged.unlink(missing_ok=True)
        return {
            "capture": manifest,
            "restored": {
                "completeness": "complete" if all(item["ready"] for item in current) else "incomplete",
                "capabilities": capabilities,
                "dependencies": current,
            },
        }
