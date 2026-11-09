"""Read-only Windows sandbox launch gate. Never runs Codex or setup helpers."""

from __future__ import annotations

import hashlib
import json
import os
import tomllib
from pathlib import Path
from typing import Any

POLICY = "local-config/windows-sandbox-policy.json"
HEALTH = "local-config/windows-sandbox-health.json"
BLOCK = ".harness-runtime/operator-only/SANDBOX_LAUNCH_BLOCKED.json"
DEFAULT_POLICY = {
    "schema": "windows-sandbox-policy/v1",
    "max_launcher_attempts": 1,
    "max_setup_requests_per_run": 1,
    "automatic_setup": False,
    "automatic_fallback": False,
    "require_health_receipt": True,
}


class SandboxPreflightError(ValueError):
    pass


def health_path(harness: Path, permission_mode: str | None) -> Path:
    """Keep ROOT's historical receipt separate from each worker profile proof."""
    if permission_mode is None:
        return harness / HEALTH
    if permission_mode not in {"worker-isolated", "worker-workspace"}:
        raise ValueError(
            f"unsupported isolated worker permission mode: {permission_mode}"
        )
    return harness / "local-config" / f"windows-sandbox-health-{permission_mode}.json"


def _blocked(reason: str) -> SandboxPreflightError:
    return SandboxPreflightError(
        f"WINDOWS_SANDBOX_NOT_READY: {reason}. No Codex/setup process started; "
        "no automatic retry or fallback. Diagnose and repair before another launch."
    )


def _object(path: Path) -> dict[str, Any]:
    try:
        if not 0 < path.stat().st_size <= 1024 * 1024:
            raise ValueError("empty or oversized file")
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(value, dict) or not value:
            raise ValueError("expected a nonempty JSON object")
        return value
    except (OSError, ValueError) as exc:
        raise _blocked(f"invalid {path}: {exc}") from exc


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def profile_contract_digest(harness: Path, permission_mode: str) -> str:
    """Fingerprint the exact installed profile generator and launcher policy."""
    files = (
        "orchestrator_harness/bootstrap.py",
        "orchestrator_harness/isolation_policy.py",
        "orchestrator_harness/provider_adapters/codex/launcher_binding.py",
        "adapters/codex/harness/launcher_binding.py",
        "orchestrator_harness/windows_worker_preparation.py",
        "orchestrator_harness/windows_sandbox_preflight.py",
        "scripts/prepare_windows_workers.py",
        "scripts/probe_windows_worker_isolation.py",
    )
    material = {"mode": permission_mode, "sources": {}}
    for relative in files:
        path = harness / relative
        material["sources"][relative] = digest(path)
    payload = json.dumps(material, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def validate_worker_profile(config_path: Path, permission_mode: str) -> str:
    """Validate the lane's actual generated profile before a native launch."""
    try:
        raw = config_path.read_bytes()
        config = tomllib.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError(
            f"worker Codex profile is unreadable: {config_path}: {exc}"
        ) from exc
    if config.get("default_permissions") != permission_mode:
        raise ValueError(
            "worker Codex profile mode differs from the recorded invocation"
        )
    permissions = config.get("permissions")
    profile = (
        permissions.get(permission_mode) if isinstance(permissions, dict) else None
    )
    filesystem = profile.get("filesystem") if isinstance(profile, dict) else None
    if (
        not isinstance(profile, dict)
        or profile.get("extends") != ":workspace"
        or not isinstance(filesystem, dict)
        or filesystem.get(":workspace_roots") != {".": "write"}
    ):
        raise ValueError(
            "worker Codex filesystem profile is missing its workspace write boundary"
        )
    if permission_mode == "worker-workspace":
        if any(value == "deny" for value in filesystem.values()):
            raise ValueError(
                "worker-workspace profile must not depend on read-denial entries"
            )
    else:
        codex_home = Path(
            os.environ.get("CODEX_HOME") or Path.home() / ".codex"
        ).resolve()
        if filesystem.get(str(codex_home)) != "deny":
            raise ValueError(
                "worker-isolated profile does not deny the active Codex home"
            )
    if any(
        not isinstance(value, str) or value not in {"deny", "write"}
        for key, value in filesystem.items()
        if key != ":workspace_roots"
    ):
        raise ValueError(
            "worker Codex filesystem profile contains an unsupported permission"
        )
    return hashlib.sha256(raw).hexdigest()


def _native_executable(executable: str | None) -> Path:
    if (
        executable
        and Path(executable).is_absolute()
        and Path(executable).suffix.lower() == ".exe"
    ):
        return Path(executable).resolve()
    # Resolve the packaged binary without invoking even `codex --help`.
    from .provider_adapters.codex.launcher_binding import _direct_codex_executable

    return Path(_direct_codex_executable()).resolve()


def require_not_blocked(workspace: Path) -> None:
    """A run failure cannot be bypassed with another provider or permission mode."""
    path = workspace / BLOCK
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise _blocked(
            f"run-wide sandbox failure evidence cannot be read: {exc}"
        ) from exc
    raise _blocked(f"this run already recorded a sandbox startup failure: {path}")


def require_ready(
    harness: Path,
    *,
    workspace: Path | None = None,
    executable: str | None = None,
    permission_mode: str | None = None,
    isolation_config_path: Path | None = None,
) -> dict[str, Any] | None:
    policy_path = harness / POLICY
    if workspace is not None:
        require_not_blocked(workspace)
    # Windows Codex is always gated, including newly copied harnesses that
    # have no local policy yet. A local file may restate, never weaken, this
    # policy. No machine-specific PASS receipt is shipped with the product.
    policy = _object(policy_path) if policy_path.exists() else DEFAULT_POLICY
    if policy != DEFAULT_POLICY:
        raise _blocked(
            "launch policy must require one attempt, no setup/fallback, and an operator health receipt"
        )
    if os.name != "nt":
        return None
    if permission_mode is not None and permission_mode not in {
        "worker-isolated",
        "worker-workspace",
    }:
        raise _blocked(
            f"unsupported isolated worker permission mode: {permission_mode}"
        )
    if isolation_config_path is not None and permission_mode is None:
        raise _blocked(
            "a worker profile path requires an explicit worker permission mode"
        )
    profile_hash = None
    preparation: dict[str, Any] | None = None
    if isolation_config_path is not None:
        profile_mode = permission_mode
        if profile_mode is None:
            raise _blocked(
                "a worker profile path requires an explicit worker permission mode"
            )
        try:
            profile_hash = validate_worker_profile(
                Path(isolation_config_path), profile_mode
            )
        except (OSError, ValueError) as exc:
            raise _blocked(str(exc)) from exc
    expected_contract = (
        profile_contract_digest(harness, permission_mode) if permission_mode else None
    )
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").resolve()
    marker = codex_home / ".sandbox/setup_marker.json"
    _object(marker)
    # A parseable marker alone does not establish version/config compatibility.
    # Only the operator may supply this after one successful isolated probe.
    selected_health_path = health_path(harness, permission_mode)
    health = _object(selected_health_path)
    try:
        native = _native_executable(executable)
        local_appdata = Path(os.environ["LOCALAPPDATA"]).resolve()
        temporary: Path | None = None
        if isolation_config_path is not None and permission_mode is not None:
            temporary = Path(os.environ.get("TEMP", "")).resolve()
            temporary_copy = Path(os.environ.get("TMP", "")).resolve()
            if not os.environ.get("TEMP") or temporary != temporary_copy:
                raise ValueError(
                    "worker TEMP and TMP must name the same prepared private directory"
                )
        evidence = Path(health["probe_evidence_path"]).resolve()
        if not evidence.is_relative_to(harness.resolve().parent):
            raise ValueError("probe evidence must belong to this run's repository area")
        if permission_mode is None:
            if (
                health.get("schema") != "windows-sandbox-health/v1"
                or health.get("status") != "PASS"
            ):
                raise ValueError("operator health receipt is not PASS")
        else:
            if (
                health.get("schema") != "windows-sandbox-health/v2"
                or health.get("status") != "PASS"
            ):
                raise ValueError("operator health receipt is not PASS")
            if health.get("permission_mode") != permission_mode:
                raise ValueError(
                    "operator health receipt qualifies a different worker permission mode"
                )
            if health.get("profile_contract_sha256") != expected_contract:
                raise ValueError(
                    "operator health receipt qualifies a different profile generator or launcher policy"
                )
            if health.get("backend_id") != "codex-windows-elevated":
                raise ValueError(
                    "operator health receipt names an unsupported Windows Codex sandbox backend"
                )
            if isolation_config_path is not None:
                if temporary is None:
                    raise ValueError("worker private TEMP is unavailable")
                from .windows_worker_preparation import validate_prepared_profile

                preparation = validate_prepared_profile(
                    harness,
                    workspace or harness,
                    Path(isolation_config_path),
                    permission_mode,
                    executable=native,
                    codex_home=codex_home,
                    local_appdata=local_appdata,
                    temporary=temporary,
                )
            probe_profile_hash = health.get("probe_profile_sha256")
            if (
                not isinstance(probe_profile_hash, str)
                or len(probe_profile_hash) != 64
                or any(char not in "0123456789abcdef" for char in probe_profile_hash)
            ):
                raise ValueError(
                    "operator health receipt does not contain linked-profile probe evidence"
                )
            probe = _object(evidence)
            probe_profile = Path(probe.get("profile_path", "")).resolve()
            protected_before = probe.get("protected_before")
            protected_after = probe.get("protected_after")
            denied_paths = probe.get("protected_write_denials")
            if (
                probe.get("permission_mode") != permission_mode
                or probe.get("exit_code") != 0
                or probe.get("cleanup_proven") is not True
                or probe.get("protected_unchanged") is not True
                or not isinstance(protected_before, dict)
                or protected_before != protected_after
                or not isinstance(denied_paths, list)
                or not isinstance(
                    probe.get("protected_write_targets", protected_before), list
                )
                or set(denied_paths)
                != set(probe.get("protected_write_targets", protected_before))
                or not set(protected_before).issubset(set(denied_paths))
                or probe.get("unexpectedly_writable") != []
                or probe.get("probe_errors") != []
                or not isinstance(probe.get("worker_base_tip"), str)
                or not isinstance(probe.get("worker_tip"), str)
                or probe.get("worker_tip") == probe.get("worker_base_tip")
                or probe.get("profile_contract_sha256_after") != expected_contract
                or Path(probe.get("local_appdata", "")).resolve() != local_appdata
                or probe.get("profile_contract_sha256") != expected_contract
                or probe.get("profile_sha256") != probe_profile_hash
                or probe.get("codex_home") != str(codex_home)
                or (
                    permission_mode == "worker-isolated"
                    and (
                        probe.get("codex_home_read_denied") is not True
                        or not isinstance(probe.get("concurrent_workers"), list)
                        or len(probe["concurrent_workers"]) != 1
                        or probe.get("concurrent_overlap_proven") is not True
                    )
                )
            ):
                raise ValueError(
                    "retained native probe does not prove this profile, cache, read boundary, concurrent operation, and exact cleanup"
                )
            if permission_mode == "worker-isolated":
                concurrent_paths: set[str] = set()
                helper_started = probe.get("worker_helper_started")
                for worker_proof in probe["concurrent_workers"]:
                    if not isinstance(worker_proof, dict):
                        raise TypeError(
                            "strict probe has malformed concurrent worker proof"
                        )
                    worker_profile = Path(
                        worker_proof.get("profile_path", "")
                    ).resolve()
                    worker_hash = worker_proof.get("profile_sha256")
                    peer_cache_path = Path(
                        worker_proof.get("local_appdata", "")
                    ).resolve()
                    peer_started = worker_proof.get("helper_started")
                    if (
                        worker_profile == probe_profile
                        or str(worker_profile) in concurrent_paths
                        or not worker_profile.is_file()
                        or not isinstance(worker_hash, str)
                        or digest(worker_profile) != worker_hash
                        or worker_proof.get("exit_code") != 0
                        or worker_proof.get("cleanup_proven") is not True
                        or worker_proof.get("codex_home_read_denied") is not True
                        or worker_proof.get("sandbox_error") is not None
                        or peer_cache_path != local_appdata
                        or not peer_cache_path.is_dir()
                        or peer_cache_path.is_symlink()
                        or not peer_cache_path.is_relative_to(
                            (harness / "runtime").resolve()
                        )
                        or not isinstance(helper_started, (int, float))
                        or isinstance(helper_started, bool)
                        or not isinstance(peer_started, (int, float))
                        or isinstance(peer_started, bool)
                        or abs(helper_started - peer_started) >= 1.0
                        or worker_proof.get("worker_tip")
                        == worker_proof.get("worker_base_tip")
                        or not isinstance(worker_proof.get("argv"), list)
                        or not worker_proof.get("argv")
                        or Path(worker_proof["argv"][0]).resolve() != native
                        or "--permission-profile" not in worker_proof["argv"]
                        or worker_proof["argv"][
                            worker_proof["argv"].index("--permission-profile") + 1
                        ]
                        != permission_mode
                    ):
                        raise ValueError(
                            "strict probe concurrent worker evidence is incomplete or changed"
                        )
                    validate_worker_profile(worker_profile, permission_mode)
                    concurrent_paths.add(str(worker_profile))
                native_logs = probe.get("native_logs")
                if not isinstance(native_logs, dict) or set(native_logs) != {
                    "stdout.log",
                    "stderr.log",
                    "peer-stdout.log",
                    "peer-stderr.log",
                }:
                    raise ValueError(
                        "strict probe does not bind both native process logs"
                    )
                for log_record in native_logs.values():
                    if not isinstance(log_record, dict):
                        raise TypeError(
                            "strict probe has malformed native log evidence"
                        )
                    log_path = Path(log_record.get("path", "")).resolve()
                    log_hash = log_record.get("sha256")
                    if (
                        not log_path.is_relative_to(evidence.parent.resolve())
                        or not log_path.is_file()
                        or not isinstance(log_hash, str)
                        or digest(log_path) != log_hash
                    ):
                        raise ValueError(
                            "strict probe native logs are missing or changed"
                        )
            if (
                not probe_profile.is_file()
                or digest(probe_profile) != probe_profile_hash
            ):
                raise ValueError(
                    "retained linked-worker profile is missing or changed after its probe"
                )
            validate_worker_profile(probe_profile, permission_mode)
            argv = probe.get("argv")
            if (
                not isinstance(argv, list)
                or not argv
                or Path(argv[0]).resolve() != native
                or "-c" not in argv
                or 'windows.sandbox="elevated"' not in argv
                or "--include-managed-config" not in argv
                or "--permission-profile" not in argv
                or argv[argv.index("--permission-profile") + 1] != permission_mode
            ):
                raise ValueError(
                    "retained probe did not launch the verified CLI with the selected profile"
                )
        if (
            type(health.get("probe_exit_code")) is not int
            or health["probe_exit_code"] != 0
        ):
            raise ValueError("operator probe did not succeed")
        if (
            Path(health["codex_home"]).resolve() != codex_home
            or Path(health["native_codex_executable"]).resolve() != native
            or Path(health["local_appdata"]).resolve() != local_appdata
        ):
            raise ValueError("operator health receipt names a different CLI/home/cache")
        if (
            health.get("setup_marker_sha256") != digest(marker)
            or health.get("native_codex_sha256") != digest(native)
            or health.get("probe_evidence_sha256") != digest(evidence)
        ):
            raise ValueError(
                "sandbox marker, CLI binary, or probe evidence changed after verification"
            )
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise _blocked(str(exc)) from exc
    return {
        "harness_dir": str(harness.resolve()),
        "health_receipt_path": str(selected_health_path),
        "health_receipt_sha256": digest(selected_health_path),
        "permission_mode": permission_mode,
        "profile_contract_sha256": expected_contract,
        "worker_profile_sha256": profile_hash,
        "worker_preparation": preparation,
        "backend_id": health.get("backend_id"),
        "isolation_config_path": str(isolation_config_path.resolve())
        if isolation_config_path
        else None,
        "marker_path": str(marker),
        "marker_sha256": health["setup_marker_sha256"],
    }


def startup_failure(
    health: dict[str, Any], stderr_path: Path, offset: int
) -> str | None:
    """Notice invalidation or setup errors from this provider's own stderr."""
    try:
        marker = Path(health["marker_path"])
        if digest(marker) != health["marker_sha256"]:
            return "sandbox setup marker changed during this provider invocation"
        signals = (
            "sandbox setup required",
            "sandbox setup failed",
            "setup refresh had errors",
            "helper_sandbox_lock_failed",
            "codex-windows-sandbox-setup",
            "sandbox setup marker missing",
        )
        key = (str(stderr_path.resolve()), offset)
        scan = health.get("_stderr_scan")
        if not isinstance(scan, dict) or scan.get("key") != key:
            scan = {"key": key, "offset": offset, "tail": b""}
            health["_stderr_scan"] = scan
        # Snapshot the available end so a noisy live provider cannot turn this
        # check into an endless read. Keep overlap for markers split across polls.
        end = stderr_path.stat().st_size
        if end < scan["offset"]:
            return "this provider's sandbox diagnostics were truncated during the invocation"
        with stderr_path.open("rb") as stream:
            stream.seek(scan["offset"])
            while stream.tell() < end:
                chunk = stream.read(min(65536, end - stream.tell()))
                if not chunk:
                    break
                tail = scan["tail"] + chunk
                if any(signal.encode() in tail.lower() for signal in signals):
                    return "this provider reported sandbox setup/refresh instead of using the verified sandbox"
                scan["tail"] = tail[-max(map(len, signals)) :]
                scan["offset"] = stream.tell()
    except (OSError, KeyError, TypeError, ValueError) as exc:
        return f"sandbox health could not be verified during this invocation: {exc}"
    return None
