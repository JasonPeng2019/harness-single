"""Read-only Windows sandbox launch gate. Never runs Codex or setup helpers."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

POLICY = "local-config/windows-sandbox-policy.json"
HEALTH = "local-config/windows-sandbox-health.json"
BLOCK = ".harness-runtime/operator-only/SANDBOX_LAUNCH_BLOCKED.json"
DEFAULT_POLICY = {
    "schema": "windows-sandbox-policy/v1", "max_launcher_attempts": 1,
    "max_setup_requests_per_run": 1, "automatic_setup": False,
    "automatic_fallback": False, "require_health_receipt": True,
}


class SandboxPreflightError(ValueError):
    pass


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


def _native_executable(executable: str | None) -> Path:
    if executable and Path(executable).is_absolute() and Path(executable).suffix.lower() == ".exe":
        return Path(executable).resolve()
    # Resolve the packaged binary without invoking even `codex --help`.
    from .provider_adapters.codex.launcher_binding import _direct_codex_executable
    return Path(_direct_codex_executable()).resolve()


def require_ready(
    harness: Path, *, workspace: Path | None = None, executable: str | None = None,
) -> dict[str, Any] | None:
    policy_path = harness / POLICY
    # Windows Codex is always gated, including newly copied harnesses that
    # have no local policy yet. A local file may restate, never weaken, this
    # policy. No machine-specific PASS receipt is shipped with the product.
    policy = _object(policy_path) if policy_path.exists() else DEFAULT_POLICY
    if policy != DEFAULT_POLICY:
        raise _blocked("launch policy must require one attempt, no setup/fallback, and an operator health receipt")
    if os.name != "nt":
        return None
    if workspace is not None and (workspace / BLOCK).exists():
        raise _blocked(f"this run already recorded a sandbox startup failure: {workspace / BLOCK}")
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").resolve()
    marker = codex_home / ".sandbox/setup_marker.json"
    _object(marker)
    # A parseable marker alone does not establish version/config compatibility.
    # Only the operator may supply this after one successful isolated probe.
    health_path = harness / HEALTH
    health = _object(health_path)
    try:
        native = _native_executable(executable)
        local_appdata = Path(os.environ["LOCALAPPDATA"]).resolve()
        evidence = Path(health["probe_evidence_path"]).resolve()
        if not evidence.is_relative_to(harness.resolve().parent):
            raise ValueError("probe evidence must belong to this run's repository area")
        if health.get("schema") != "windows-sandbox-health/v1" or health.get("status") != "PASS":
            raise ValueError("operator health receipt is not PASS")
        if type(health.get("probe_exit_code")) is not int or health["probe_exit_code"] != 0:
            raise ValueError("operator probe did not succeed")
        if (Path(health["codex_home"]).resolve() != codex_home
                or Path(health["native_codex_executable"]).resolve() != native
                or Path(health["local_appdata"]).resolve() != local_appdata):
            raise ValueError("operator health receipt names a different CLI/home/cache")
        if (health.get("setup_marker_sha256") != digest(marker)
                or health.get("native_codex_sha256") != digest(native)
                or health.get("probe_evidence_sha256") != digest(evidence)):
            raise ValueError("sandbox marker, CLI binary, or probe evidence changed after verification")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise _blocked(str(exc)) from exc
    return {"harness_dir": str(harness.resolve()), "health_receipt_path": str(health_path), "health_receipt_sha256": digest(health_path),
            "marker_path": str(marker), "marker_sha256": health["setup_marker_sha256"]}


def startup_failure(health: dict[str, Any], stderr_path: Path, offset: int) -> str | None:
    """Notice invalidation or setup errors from this provider's own stderr."""
    try:
        marker = Path(health["marker_path"])
        if digest(marker) != health["marker_sha256"]:
            return "sandbox setup marker changed during this provider invocation"
        with stderr_path.open("rb") as stream:
            stream.seek(offset)
            tail = stream.read(65536).decode("utf-8", errors="replace").lower()
        if any(signal in tail for signal in (
            "sandbox setup required", "sandbox setup failed", "setup refresh had errors",
            "helper_sandbox_lock_failed", "codex-windows-sandbox-setup", "sandbox setup marker missing",
        )):
            return "this provider reported sandbox setup/refresh instead of using the verified sandbox"
    except (OSError, KeyError, TypeError, ValueError) as exc:
        return f"sandbox health could not be verified during this invocation: {exc}"
    return None
