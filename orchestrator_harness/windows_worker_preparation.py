"""Explicit Windows Codex capability preparation for exact worker profiles."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import processes
from .provider_adapters.codex.launcher_binding import (
    _direct_codex_executable,
    _isolation_overrides,
)
from .records import atomic_write_json
from .windows_sandbox_preflight import (
    digest,
    profile_contract_digest,
    validate_worker_profile,
)

PREPARATION_SUFFIX = ".preparation.json"
PREPARATION_SCHEMA = "windows-worker-preparation/v1"
CAP_SID_NAME = "cap_sid"
_POWERSHELL_ACL = r"""$path=$env:HARNESS_ACL_TARGET; $acl=Get-Acl -LiteralPath $path; $rows=@(foreach($entry in $acl.Access){$sid=$entry.IdentityReference.Value; try{$sid=$entry.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value}catch{}; [pscustomobject]@{sid=$sid;type=[string]$entry.AccessControlType;rights=[string]$entry.FileSystemRights;inherited=[bool]$entry.IsInherited}}); [pscustomobject]@{path=(Get-Item -LiteralPath $path).FullName;owner=$acl.Owner;aces=$rows} | ConvertTo-Json -Depth 8 -Compress"""


class WorkerPreparationError(ValueError):
    """The requested worker profiles cannot be prepared safely."""


@dataclass(frozen=True)
class WorkerProfileTarget:
    """One exact profile plus the authoritative worktree and invocation TEMP."""

    config_path: Path
    worktree: Path
    temporary: Path
    lane_id: str | None = None
    run_id: str | None = None
    invocation_hash: str | None = None
    source_profile_path: Path | None = None
    worktree_identity: tuple[int, int] | None = None
    permission_mode: str | None = None


def _active_lane_target(
    harness: Path,
    lane_id: str,
    *,
    config_path: Path | None = None,
) -> WorkerProfileTarget:
    from .config import load_config
    from .lane_authority import read_invocation
    from .lanes import find_active_lane

    config = load_config(harness)
    runtime = config.runtime_root
    _, lane = find_active_lane(runtime, lane_id)
    invocation = read_invocation(lane)
    if lane.get("provider", {}).get("id") != "codex":
        raise WorkerPreparationError(f"lane {lane_id} is not a Codex worker")
    protected = Path(lane["isolation_config_path"]).resolve(strict=True)
    if config_path is not None and protected != config_path.resolve(strict=True):
        raise WorkerPreparationError(
            "protected profile path is not the active lane's authoritative Codex config"
        )
    worktree = Path(lane["worktree_path"]).resolve(strict=True)
    identity = tuple(lane["worktree_identity"])
    metadata = worktree.lstat()
    if identity != (metadata.st_dev, metadata.st_ino):
        raise WorkerPreparationError("assigned worker worktree identity changed")
    source_profile = worktree / ".codex" / "config.toml"
    source_hash = hashlib.sha256(source_profile.read_bytes()).hexdigest()
    protected_hash = hashlib.sha256(protected.read_bytes()).hexdigest()
    if source_hash != lane.get("isolation_config_sha256") or protected_hash != lane.get(
        "isolation_config_sha256"
    ):
        raise WorkerPreparationError(
            "protected and generated worker profiles do not match lane authority"
        )
    provider = invocation.get("provider")
    launch_config = (
        provider.get("launch_config") if isinstance(provider, dict) else None
    )
    actual_mode = (
        launch_config.get("permission_mode", "worker-isolated")
        if isinstance(launch_config, dict)
        else "worker-isolated"
    )
    temporary = worktree / ".agent-workspace" / "tmp" / str(lane["run_id"])
    return WorkerProfileTarget(
        config_path=protected,
        worktree=worktree,
        temporary=temporary,
        lane_id=lane_id,
        run_id=str(lane["run_id"]),
        invocation_hash=str(lane["invocation_hash"]),
        source_profile_path=source_profile,
        worktree_identity=identity,
        permission_mode=actual_mode,
    )


def resolve_active_profile_targets(
    harness: Path, lane_ids: list[str]
) -> list[WorkerProfileTarget]:
    """Resolve lane IDs through active operator authority and protected invocations."""
    if not lane_ids or len(set(lane_ids)) != len(lane_ids):
        raise WorkerPreparationError("provide one or more unique active lane IDs")
    return [
        _active_lane_target(harness.resolve(strict=True), lane_id)
        for lane_id in lane_ids
    ]


def resolve_active_profile_for_gate(
    harness: Path, workspace: Path, config_path: Path
) -> WorkerProfileTarget:
    """Resolve the exact protected profile supplied to launch through its active lane."""
    from .config import load_config

    config = load_config(harness)
    if (
        config.root_workspace is None
        or config.root_workspace.resolve() != workspace.resolve()
    ):
        raise WorkerPreparationError(
            "launch workspace differs from configured root workspace"
        )
    lane_id = config_path.resolve(strict=True).parent.parent.parent.name
    return _active_lane_target(harness, lane_id, config_path=config_path)


def _under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def preparation_path(config_path: Path) -> Path:
    """Return the explicit sidecar path for one generated Codex profile."""
    return config_path.with_name(config_path.name + PREPARATION_SUFFIX)


def codex_home_mutex_name(codex_home: Path) -> str:
    """Return the stable per-CODEX_HOME named mutex shared by harness processes."""
    normalized = os.path.normcase(str(codex_home.resolve())).casefold()
    token = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"Local\\CodexSandboxPrepare-{token}"


def _read_cap_sids(codex_home: Path) -> tuple[dict[str, Any], str, Path]:
    path = codex_home / CAP_SID_NAME
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise WorkerPreparationError(
            f"cannot read native capability SID map {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise WorkerPreparationError(
            f"native capability SID map is not an object: {path}"
        )
    return value, hashlib.sha256(raw).hexdigest(), path


def _canonical_path(path: Path) -> str:
    return os.path.normcase(str(path.resolve())).replace("\\", "/")


def _acl_snapshot(path: Path) -> dict[str, Any]:
    if os.name != "nt":
        raise WorkerPreparationError(
            "Windows ACL observations are available only on Windows"
        )
    environment = os.environ.copy()
    environment["HARNESS_ACL_TARGET"] = str(path.resolve())
    try:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                _POWERSHELL_ACL,
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise WorkerPreparationError(
            f"read-only ACL query failed for {path}: {exc}"
        ) from exc
    if completed.returncode != 0:
        raise WorkerPreparationError(
            f"read-only ACL query failed for {path}: {completed.stderr.strip()}"
        )
    try:
        value = json.loads(completed.stdout)
    except ValueError as exc:
        raise WorkerPreparationError(
            f"read-only ACL query returned invalid JSON for {path}"
        ) from exc
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("path"), str)
        or value["path"].casefold() != str(path.resolve()).casefold()
        or not isinstance(value.get("aces"), list)
    ):
        raise WorkerPreparationError(
            f"read-only ACL query returned an incomplete record for {path}"
        )
    return value


def declared_write_roots(
    config_path: Path, permission_mode: str, *, worktree: Path
) -> tuple[Path, list[Path], set[str]]:
    """Read exact workspace/write roots and deny paths from a generated profile."""
    try:
        config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise WorkerPreparationError(
            f"worker profile is unreadable: {config_path}: {exc}"
        ) from exc
    permissions = config.get("permissions")
    profile = (
        permissions.get(permission_mode) if isinstance(permissions, dict) else None
    )
    filesystem = profile.get("filesystem") if isinstance(profile, dict) else None
    if not isinstance(filesystem, dict):
        raise WorkerPreparationError("worker profile lacks a filesystem policy")
    roots = filesystem.get(":workspace_roots")
    if not isinstance(roots, dict) or roots != {".": "write"}:
        raise WorkerPreparationError(
            "worker profile lacks the exact workspace write boundary"
        )
    worktree = worktree.resolve(strict=True)
    writes: list[Path] = []
    denies: set[str] = set()
    for raw_path, value in filesystem.items():
        if raw_path == ":workspace_roots":
            continue
        if value == "deny":
            denies.add(str(Path(raw_path).resolve()).casefold())
        elif value == "write":
            path = Path(raw_path)
            if not path.is_absolute():
                raise WorkerPreparationError(
                    f"profile write root must be absolute: {raw_path}"
                )
            writes.append(path.resolve())
        else:
            raise WorkerPreparationError(
                f"profile has an unsupported filesystem permission: {raw_path}"
            )
    return worktree, sorted(set(writes)), denies


def validate_batch_profiles(
    targets: list[WorkerProfileTarget], permission_mode: str
) -> list[dict[str, Any]]:
    """Validate the whole batch before native setup and reject incompatible deny sets."""
    if not targets:
        raise WorkerPreparationError(
            "at least one exact bootstrap-generated profile is required"
        )
    records: list[dict[str, Any]] = []
    identities: set[str] = set()
    deny_sets: list[set[str]] = []
    for target in targets:
        if target.config_path.is_symlink() or target.worktree.is_symlink():
            raise WorkerPreparationError(
                "profile and assigned worktree must not be symbolic links"
            )
        path = target.config_path.resolve(strict=True)
        identity = os.path.normcase(str(path))
        if identity in identities:
            raise WorkerPreparationError(f"profile appears more than once: {path}")
        identities.add(identity)
        if path.name.lower() not in {"config.toml", "codex-config.toml"}:
            raise WorkerPreparationError(
                f"profile is not a generated Codex worker config: {path}"
            )
        if (
            target.permission_mode is not None
            and target.permission_mode != permission_mode
        ):
            raise WorkerPreparationError(
                f"lane {target.lane_id} permission mode differs from requested preparation mode"
            )
        profile_hash = validate_worker_profile(path, permission_mode)
        worktree = target.worktree.resolve(strict=True)
        if not target.temporary.is_absolute():
            raise WorkerPreparationError(
                f"profile TEMP must be absolute: {target.temporary}"
            )
        expected_temp_parent = worktree / ".agent-workspace" / "tmp"
        if any(
            path.is_symlink()
            for path in (
                worktree / ".agent-workspace",
                expected_temp_parent,
                target.temporary,
            )
        ):
            raise WorkerPreparationError("private TEMP path contains a symbolic link")
        if not _under(target.temporary, expected_temp_parent):
            raise WorkerPreparationError(
                f"profile TEMP must be inside its private controller boundary: {target.temporary}"
            )
        if target.run_id is not None:
            expected_temp = expected_temp_parent / target.run_id
            if target.temporary.resolve() != expected_temp.resolve():
                raise WorkerPreparationError(
                    "lane TEMP does not match its protected run ID"
                )
        if target.source_profile_path is not None:
            source = target.source_profile_path.resolve(strict=True)
            if source != worktree / ".codex" / "config.toml":
                raise WorkerPreparationError(
                    "source profile is not the assigned worktree Codex config"
                )
            if digest(source) != profile_hash:
                raise WorkerPreparationError(
                    "protected and generated worker profiles differ"
                )
        if target.worktree_identity is not None:
            metadata = worktree.lstat()
            if target.worktree_identity != (metadata.st_dev, metadata.st_ino):
                raise WorkerPreparationError(
                    "assigned worktree identity changed before preparation"
                )
        if (
            path.parent.name.lower() == ".codex"
            and path.parent.parent.resolve() != worktree
        ):
            raise WorkerPreparationError(
                "raw profile path does not belong to its explicit worktree"
            )
        _, write_roots, deny_paths = declared_write_roots(
            path, permission_mode, worktree=worktree
        )
        if not worktree.is_dir() or worktree.is_symlink():
            raise WorkerPreparationError(
                f"profile worktree is not a plain directory: {worktree}"
            )
        if not write_roots:
            raise WorkerPreparationError(
                f"profile has no explicit narrow Git write roots: {path}"
            )
        for write_root in write_roots:
            if not write_root.is_dir() or write_root.is_symlink():
                raise WorkerPreparationError(
                    f"declared write root is not a plain directory: {write_root}"
                )
        deny_sets.append(deny_paths)
        records.append(
            {
                "config_path": path,
                "profile_sha256": profile_hash,
                "worktree": worktree,
                "temporary": target.temporary,
                "lane_id": target.lane_id,
                "run_id": target.run_id,
                "invocation_hash": target.invocation_hash,
                "source_profile_path": str(target.source_profile_path.resolve())
                if target.source_profile_path
                else None,
                "worktree_identity": list(target.worktree_identity)
                if target.worktree_identity
                else None,
                "write_roots": write_roots,
                "deny_paths": deny_paths,
            }
        )
    if permission_mode == "worker-isolated" and any(
        denies != deny_sets[0] for denies in deny_sets[1:]
    ):
        raise WorkerPreparationError(
            "strict profiles have incompatible global read-denial sets; refusing the batch before native setup"
        )
    return records


def _strict_deny_set(config_path: Path, worktree: Path) -> set[str]:
    return declared_write_roots(config_path, "worker-isolated", worktree=worktree)[2]


def validate_known_live_lane_compatibility(
    harness: Path,
    requested: list[dict[str, Any]],
    permission_mode: str,
) -> None:
    """Refuse changing global strict policy beneath another owned active lane."""
    if permission_mode != "worker-isolated" or not any(
        record["lane_id"] is not None for record in requested
    ):
        return
    from .config import load_config
    from .epochs import read_current_epoch
    from .lane_authority import read_invocation
    from .lanes import retained_lanes

    runtime = load_config(harness).runtime_root
    marker = read_current_epoch(runtime)
    if marker is None:
        return
    active = retained_lanes(runtime, str(marker["epoch_id"]))
    requested_by_lane = {
        record["lane_id"]: _strict_deny_set(record["config_path"], record["worktree"])
        for record in requested
        if record["lane_id"] is not None
    }
    requested_sets = [
        _strict_deny_set(record["config_path"], record["worktree"])
        for record in requested
    ]
    expected = requested_sets[0]
    live_lifecycles = {
        "prepared",
        "resuming",
        "running",
        "review_pending",
        "result_invalid",
    }
    for lane_id, lane in active.items():
        if lane.get("lifecycle") not in live_lifecycles:
            continue
        provider = lane.get("provider")
        if not isinstance(provider, dict) or provider.get("id") != "codex":
            continue
        invocation = read_invocation(lane)
        launch = invocation.get("provider", {}).get("launch_config", {})
        mode = launch.get("permission_mode", "worker-isolated")
        if mode != "worker-isolated":
            continue
        path = Path(lane["isolation_config_path"]).resolve(strict=True)
        worktree = Path(lane["worktree_path"]).resolve(strict=True)
        lane_denies = _strict_deny_set(path, worktree)
        if lane_id in requested_by_lane:
            lane_denies = requested_by_lane[lane_id]
        if lane_denies != expected:
            raise WorkerPreparationError(
                "strict preparation would change the global read-denial set beneath "
                f"owned active Codex lane {lane_id}; resolve the active-lane conflict first"
            )


def _write_ace_matches(acl: dict[str, Any], sid: object) -> bool:
    if not isinstance(sid, str) or not re.fullmatch(r"S-1-[0-9]+(?:-[0-9]+)+", sid):
        return False
    for ace in acl["aces"]:
        if (
            not isinstance(ace, dict)
            or ace.get("sid") != sid
            or ace.get("type") != "Allow"
        ):
            continue
        rights = ace.get("rights")
        if isinstance(rights, str) and any(
            right in rights for right in ("Write", "Modify", "FullControl")
        ):
            return True
    return False


def inspect_profile_capabilities(
    record: dict[str, Any], codex_home: Path
) -> dict[str, Any]:
    """Read cap_sid and ACLs and require the current SID to have each write ACE."""
    cap, cap_hash, cap_path = _read_cap_sids(codex_home)
    workspace_map = cap.get("workspace_by_cwd")
    write_map = cap.get("writable_root_by_path")
    if not isinstance(workspace_map, dict) or not isinstance(write_map, dict):
        raise WorkerPreparationError(
            "native cap_sid map lacks workspace/write-root mappings"
        )
    observations: list[dict[str, Any]] = []
    root: Path = record["worktree"]
    entries = [("workspace_by_cwd", root, workspace_map.get(_canonical_path(root)))]
    entries.extend(
        ("writable_root_by_path", path, write_map.get(_canonical_path(path)))
        for path in record["write_roots"]
    )
    for source, path, sid in entries:
        acl = _acl_snapshot(path)
        matches = _write_ace_matches(acl, sid)
        observations.append(
            {
                "mapping": source,
                "path": str(path),
                "cap_sid": sid,
                "matching_write_ace": matches,
                "acl": acl,
            }
        )
        if not matches:
            raise WorkerPreparationError(
                f"native cap_sid has no matching write ACE for {path}; preserve the observation and diagnose before another launch"
            )
    return {
        "cap_sid_path": str(cap_path),
        "cap_sid_sha256": cap_hash,
        "observations": observations,
    }


def _capability_bindings(value: dict[str, Any]) -> list[tuple[str, str, str]]:
    observations = value.get("observations")
    if not isinstance(observations, list) or not observations:
        raise WorkerPreparationError(
            "capability proof has no exact profile SID bindings"
        )
    bindings: list[tuple[str, str, str]] = []
    for item in observations:
        if not isinstance(item, dict):
            raise WorkerPreparationError(
                "capability proof contains an invalid SID binding"
            )
        mapping, path, sid = item.get("mapping"), item.get("path"), item.get("cap_sid")
        if (
            mapping not in {"workspace_by_cwd", "writable_root_by_path"}
            or not isinstance(path, str)
            or not isinstance(sid, str)
            or item.get("matching_write_ace") is not True
        ):
            raise WorkerPreparationError(
                "capability proof contains an incomplete SID binding"
            )
        bindings.append((mapping, _canonical_path(Path(path)), sid))
    return sorted(bindings)


def _ensure_owned_directory(path: Path, description: str) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise WorkerPreparationError(
            f"{description} must be a plain absolute directory: {path}"
        )
    if not path.exists():
        path.mkdir(parents=True, exist_ok=False)
    resolved = path.resolve()
    if path.is_symlink() or not resolved.is_dir():
        raise WorkerPreparationError(f"{description} must be a plain directory: {path}")
    return resolved


@contextmanager
def _mutex(codex_home: Path) -> Iterator[None]:
    """Hold a named Windows mutex for all native setup tied to this Codex home."""
    if os.name != "nt":
        raise WorkerPreparationError(
            "native Windows worker preparation is available only on Windows"
        )
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = (ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p)
    kernel32.CreateMutexW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    kernel32.ReleaseMutex.argtypes = (ctypes.c_void_p,)
    kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
    handle = kernel32.CreateMutexW(None, False, codex_home_mutex_name(codex_home))
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        result = kernel32.WaitForSingleObject(handle, 180_000)
        if result not in (0, 0x80):
            raise WorkerPreparationError(
                f"CODEX_HOME setup mutex wait failed: {result}"
            )
        try:
            yield
        finally:
            if not kernel32.ReleaseMutex(handle):
                raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel32.CloseHandle(handle)


def _run_noop(
    record: dict[str, Any],
    *,
    executable: Path,
    permission_mode: str,
    local_appdata: Path,
    temporary: Path,
    output: Path,
    index: int,
    timeout_seconds: int,
    invocation: dict[str, Any],
    persist: Callable[[], None],
) -> None:
    config_path: Path = record["config_path"]
    worktree: Path = record["worktree"]
    argv = [str(executable), "-c", 'windows.sandbox="elevated"']
    for override in _isolation_overrides(
        str(worktree), permission_mode, str(config_path)
    ):
        argv.extend(("-c", override))
    argv.extend(
        (
            "sandbox",
            "--include-managed-config",
            "--permission-profile",
            permission_mode,
            "--cd",
            str(worktree),
            "--",
            os.fspath(Path(sys.executable).resolve()),
            "-c",
            "pass",
        )
    )
    if argv[argv.index("--") + 1 :] != [
        os.fspath(Path(sys.executable).resolve()),
        "-c",
        "pass",
    ]:
        raise WorkerPreparationError(
            "refusing native start: payload is not exactly Python -c pass"
        )
    stdout_path, stderr_path = (
        output / f"profile-{index}-stdout.log",
        output / f"profile-{index}-stderr.log",
    )
    environment = os.environ.copy()
    environment.update(
        CODEX_HOME=str(record["codex_home"]),
        LOCALAPPDATA=str(local_appdata),
        TEMP=str(temporary),
        TMP=str(temporary),
    )
    invocation.update(
        {
            "argv": argv,
            "worktree": str(worktree),
            "local_appdata": str(local_appdata),
            "temp": str(temporary),
            "started_at": datetime.now(UTC).isoformat(),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }
    )
    persist()
    child = boundary = None
    try:
        with (
            stdout_path.open("wb") as stdout,
            stderr_path.open("wb") as stderr,
            open(os.devnull, "rb") as stdin,
        ):
            child = processes.spawn_provider(
                argv,
                cwd=worktree,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                env=environment,
            )
            take = getattr(child, "take_job_handle", None)
            boundary = processes.ProcessBoundary.for_process(
                child.pid, windows_job_handle=take() if callable(take) else None
            )
            invocation["pid"] = child.pid
            invocation["process_identity"] = processes.process_identity(child.pid)
            if not boundary.observe():
                raise WorkerPreparationError(
                    "native Codex process identity and Job membership are unproven"
                )
            invocation["boundary"] = boundary.record()
            persist()
            resume = getattr(child, "resume", None)
            if callable(resume):
                resume()
            invocation["exit_code"] = child.wait(timeout=timeout_seconds)
            invocation["cleanup_proven"] = boundary.cleanup(
                force=True, timeout_seconds=15.0
            )
            invocation["cleanup_proven"] = (
                processes.reap_created_process(child, timeout_seconds=15.0)
                and invocation["cleanup_proven"]
            )
            invocation["boundary"] = boundary.record()
            persist()
    except BaseException as exc:
        invocation["error"] = f"{type(exc).__name__}: {exc}"
        if boundary is not None:
            invocation["cleanup_proven"] = boundary.cleanup(
                force=True, timeout_seconds=15.0
            )
        if child is not None:
            invocation["cleanup_proven"] = processes.reap_created_process(
                child, timeout_seconds=15.0
            ) and invocation.get("cleanup_proven", False)
        if boundary is not None:
            invocation["boundary"] = boundary.record()
        persist()
        raise
    finally:
        if boundary is not None:
            invocation["boundary"] = boundary.record()
        invocation["ended_at"] = datetime.now(UTC).isoformat()
        for path, key in ((stdout_path, "stdout"), (stderr_path, "stderr")):
            if path.exists():
                invocation[f"{key}_sha256"] = digest(path)
        persist()
    if invocation.get("exit_code") != 0 or invocation.get("cleanup_proven") is not True:
        raise WorkerPreparationError(
            f"native no-op preparation failed for {worktree}; preserve logs and diagnose before another launch"
        )


def prepare_profiles(
    harness: Path,
    targets: list[WorkerProfileTarget],
    *,
    permission_mode: str,
    local_appdata: Path,
    evidence_dir: Path,
    executable: Path | None = None,
    timeout_seconds: int = 180,
) -> list[dict[str, Any]]:
    """Prepare exact profiles once each, sequentially, and publish proofs only after success."""
    if permission_mode not in {"worker-isolated", "worker-workspace"}:
        raise WorkerPreparationError(
            f"unsupported Windows worker mode: {permission_mode}"
        )
    if not 10 <= timeout_seconds <= 600:
        raise WorkerPreparationError(
            "native preparation timeout must be 10..600 seconds"
        )
    harness = harness.resolve(strict=True)
    profile_records = validate_batch_profiles(targets, permission_mode)
    validate_known_live_lane_compatibility(harness, profile_records, permission_mode)
    cache = _ensure_owned_directory(local_appdata, "LOCALAPPDATA")
    if not _under(cache, harness / "runtime"):
        raise WorkerPreparationError(
            "LOCALAPPDATA must be an explicit process-local directory below harness runtime/"
        )
    temps = [
        _ensure_owned_directory(record["temporary"], "TEMP")
        for record in profile_records
    ]
    if len(set(temps)) != len(temps):
        raise WorkerPreparationError(
            "each prepared profile requires a distinct private TEMP directory"
        )
    for record, temp in zip(profile_records, temps, strict=True):
        expected_parent = record["worktree"] / ".agent-workspace" / "tmp"
        if not _under(temp, expected_parent):
            raise WorkerPreparationError(
                f"profile TEMP must match its private controller TEMP boundary under {expected_parent}: {temp}"
            )
        record["codex_home"] = Path(
            os.environ.get("CODEX_HOME") or Path.home() / ".codex"
        ).resolve()
        record["temp"] = temp
    codex_home = profile_records[0]["codex_home"]
    if any(record["codex_home"] != codex_home for record in profile_records):
        raise WorkerPreparationError(
            "all profiles in one preparation batch must use the same CODEX_HOME"
        )
    marker = codex_home / ".sandbox" / "setup_marker.json"
    if not marker.is_file() or marker.is_symlink():
        raise WorkerPreparationError(
            f"existing native Codex setup marker is required: {marker}"
        )
    json.loads(marker.read_text(encoding="utf-8-sig"))
    native = Path(executable or _direct_codex_executable()).resolve(strict=True)
    if native.suffix.lower() != ".exe":
        raise WorkerPreparationError(
            f"native Windows Codex executable must be an .exe: {native}"
        )
    output = evidence_dir if evidence_dir.is_absolute() else harness / evidence_dir
    output = output.resolve()
    runtime = (harness / "runtime").resolve()
    if not _under(output, runtime) or output.exists() or output.is_symlink():
        raise WorkerPreparationError(
            f"evidence path must be a fresh plain directory below {runtime}: {output}"
        )
    sidecars = [preparation_path(record["config_path"]) for record in profile_records]
    if any(path.exists() or path.is_symlink() for path in sidecars):
        raise WorkerPreparationError(
            "a preparation proof already exists for one requested profile; preserving existing proofs"
        )
    contract = profile_contract_digest(harness, permission_mode)
    cli_hash = digest(native)
    marker_hash = digest(marker)
    output.mkdir(parents=True, exist_ok=False)
    invocations: list[dict[str, Any]] = []
    attempt: dict[str, Any] = {}
    try:
        # Writes the attempt ledger before any native process can be created.
        attempt = {
            "schema": "windows-worker-preparation-attempt/v1",
            "status": "IN_PROGRESS",
            "permission_mode": permission_mode,
            "codex_home": str(codex_home),
            "native_executable": str(native),
            "native_sha256": cli_hash,
            "profile_contract_sha256": contract,
            "profiles": [
                {
                    "config_path": str(record["config_path"]),
                    "profile_sha256": record["profile_sha256"],
                    "worktree": str(record["worktree"]),
                    "lane_id": record["lane_id"],
                    "run_id": record["run_id"],
                    "invocation_hash": record["invocation_hash"],
                    "source_profile_path": record["source_profile_path"],
                    "worktree_identity": record["worktree_identity"],
                    "write_roots": [str(path) for path in record["write_roots"]],
                    "local_appdata": str(cache),
                    "temp": str(record["temp"]),
                }
                for record in profile_records
            ],
            "invocations": invocations,
        }
        attempt_path = output / "attempt.json"
        atomic_write_json(attempt_path, attempt)

        def persist_attempt() -> None:
            attempt["invocations"] = invocations
            atomic_write_json(attempt_path, attempt)

        # Serialize the complete setup/readback/publication transaction for this
        # CODEX_HOME; separate preparations cannot interleave their final maps.
        with _mutex(codex_home):
            for index, (record, temp) in enumerate(
                zip(profile_records, temps, strict=True)
            ):
                # Validate argv payload before native execution.
                invocation: dict[str, Any] = {}
                invocations.append(invocation)
                persist_attempt()
                _run_noop(
                    record,
                    executable=native,
                    permission_mode=permission_mode,
                    local_appdata=cache,
                    temporary=temp,
                    output=output,
                    index=index,
                    timeout_seconds=timeout_seconds,
                    invocation=invocation,
                    persist=persist_attempt,
                )
                invocation["capability_acl"] = inspect_profile_capabilities(
                    record, codex_home
                )
                persist_attempt()
            final_capability = [
                inspect_profile_capabilities(record, codex_home)
                for record in profile_records
            ]
            for index, (record, temp, invocation, capability) in enumerate(
                zip(profile_records, temps, invocations, final_capability, strict=True)
            ):
                proof = {
                    "schema": PREPARATION_SCHEMA,
                    "status": "PREPARED",
                    "permission_mode": permission_mode,
                    "profile_path": str(record["config_path"]),
                    "profile_sha256": record["profile_sha256"],
                    "worktree_path": str(record["worktree"]),
                    "lane_id": record["lane_id"],
                    "run_id": record["run_id"],
                    "invocation_hash": record["invocation_hash"],
                    "source_profile_path": record["source_profile_path"],
                    "worktree_identity": record["worktree_identity"],
                    "profile_contract_sha256": contract,
                    "native_codex_executable": str(native),
                    "native_codex_sha256": cli_hash,
                    "codex_home": str(codex_home),
                    "local_appdata": str(cache),
                    "temp": str(temp),
                    "setup_marker_sha256": marker_hash,
                    "prepared_at": invocation["ended_at"],
                    "native_noop": invocation,
                    "capability_acl": capability,
                    "preparation_evidence_dir": str(output),
                }
                atomic_write_json(sidecars[index], proof)
            attempt["status"] = "PASS"
            attempt["published_proofs"] = [str(path) for path in sidecars]
            atomic_write_json(attempt_path, attempt)
        return [json.loads(path.read_text(encoding="utf-8")) for path in sidecars]
    except BaseException as exc:
        attempt["status"] = "FAILED"
        attempt["error"] = f"{type(exc).__name__}: {exc}"
        attempt["invocations"] = invocations
        atomic_write_json(output / "attempt.json", attempt)
        for sidecar in sidecars:
            if sidecar.exists():
                sidecar.unlink()
        raise


def validate_prepared_profile(
    harness: Path,
    workspace: Path,
    config_path: Path,
    permission_mode: str,
    *,
    executable: Path,
    codex_home: Path,
    local_appdata: Path,
    temporary: Path,
) -> dict[str, Any]:
    """Read-only launch gate for one exact profile's current capability bindings."""
    target = resolve_active_profile_for_gate(harness, workspace, config_path)
    profile = target.config_path.resolve(strict=True)
    proof_path = preparation_path(profile)
    proof = _object_for_preflight(proof_path)
    expected = {
        "schema": PREPARATION_SCHEMA,
        "status": "PREPARED",
        "permission_mode": permission_mode,
        "profile_path": str(profile),
        "profile_sha256": validate_worker_profile(profile, permission_mode),
        "worktree_path": str(target.worktree),
        "lane_id": target.lane_id,
        "run_id": target.run_id,
        "invocation_hash": target.invocation_hash,
        "source_profile_path": str(target.source_profile_path),
        "worktree_identity": list(target.worktree_identity or ()),
        "profile_contract_sha256": profile_contract_digest(harness, permission_mode),
        "native_codex_executable": str(executable.resolve()),
        "native_codex_sha256": digest(executable),
        "codex_home": str(codex_home.resolve()),
        "local_appdata": str(local_appdata.resolve()),
        "temp": str(temporary.resolve()),
        "setup_marker_sha256": digest(codex_home / ".sandbox" / "setup_marker.json"),
    }
    for key, value in expected.items():
        if proof.get(key) != value:
            raise WorkerPreparationError(
                f"prepared profile proof no longer matches {key}"
            )
    noop = proof.get("native_noop")
    boundary = noop.get("boundary") if isinstance(noop, dict) else None
    root = boundary.get("root") if isinstance(boundary, dict) else None
    identity = noop.get("process_identity") if isinstance(noop, dict) else None
    argv = noop.get("argv") if isinstance(noop, dict) else None
    if (
        not isinstance(noop, dict)
        or noop.get("exit_code") != 0
        or noop.get("cleanup_proven") is not True
        or not isinstance(boundary, dict)
        or boundary.get("kind") != "windows-job"
        or not isinstance(root, dict)
        or not isinstance(identity, dict)
        or root.get("pid") != identity.get("pid")
        or root.get("creation_time") != identity.get("creation_time")
        or not isinstance(argv, list)
        or not argv
        or Path(argv[0]).resolve() != executable.resolve()
        or argv[argv.index("--") + 1 :]
        != [os.fspath(Path(sys.executable).resolve()), "-c", "pass"]
    ):
        raise WorkerPreparationError(
            "prepared profile proof lacks a successful model-free native no-op and exact Job cleanup"
        )
    record = {
        "config_path": profile,
        "worktree": target.worktree,
        "write_roots": declared_write_roots(
            profile, permission_mode, worktree=target.worktree
        )[1],
    }
    current = inspect_profile_capabilities(record, codex_home)
    proof_capability = proof.get("capability_acl")
    if not isinstance(proof_capability, dict):
        raise WorkerPreparationError(
            "prepared profile proof has no capability SID bindings"
        )
    if _capability_bindings(proof_capability) != _capability_bindings(current):
        raise WorkerPreparationError(
            "native capability SID bindings changed for this profile after preparation"
        )
    return {
        "proof_path": str(proof_path),
        "proof_sha256": digest(proof_path),
        **expected,
    }


def _object_for_preflight(path: Path) -> dict[str, Any]:
    try:
        if (
            not path.is_file()
            or path.is_symlink()
            or not 0 < path.stat().st_size <= 1024 * 1024
        ):
            raise ValueError("missing, linked, empty, or oversized proof")
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise WorkerPreparationError(
            f"invalid prepared profile proof {path}: {exc}"
        ) from exc
    if not isinstance(value, dict) or not value:
        raise WorkerPreparationError(f"prepared profile proof is not an object: {path}")
    return value
