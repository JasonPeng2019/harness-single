from __future__ import annotations

from pathlib import Path
from typing import Any


def _positive_pid(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RuntimeError(f"{name} must be a positive PID")
    return value


def _creation(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} must be an exact creation identity")
    return value


def validate_cross_os_identity_relation(
    host_identity: dict[str, Any],
    linux_identity: dict[str, Any],
    *,
    nonce: str,
    invocation_id: str,
) -> None:
    """Validate two identities joined by protocol identity, never by PID equality.

    Windows process IDs and Linux namespace process IDs are separate identity
    domains.  The one-use nonce and invocation ID are the only relation across
    that boundary; each side must independently carry its PID and creation
    identity.
    """

    if not isinstance(nonce, str) or not nonce or not isinstance(invocation_id, str) or not invocation_id:
        raise RuntimeError("cross-OS relation requires a nonce and invocation ID")
    if host_identity.get("platform") != "windows" or linux_identity.get("platform") != "linux":
        raise RuntimeError("cross-OS relation has invalid platform labels")
    if host_identity.get("nonce") != nonce or linux_identity.get("nonce") != nonce:
        raise RuntimeError("cross-OS relation nonce mismatch")
    if host_identity.get("invocation_id") != invocation_id or linux_identity.get("invocation_id") != invocation_id:
        raise RuntimeError("cross-OS relation invocation mismatch")
    _positive_pid(host_identity.get("pid"), "host PID")
    _creation(host_identity.get("created_utc"), "host creation identity")
    _positive_pid(linux_identity.get("pid"), "Linux bridge PID")
    _creation(linux_identity.get("created_utc"), "Linux bridge creation identity")
    # PID equality across the two domains is never a relation and is never
    # asserted: the nonce and invocation ID above are the only binding, and
    # each side carries its own independent PID/creation identity.


def provider_identity_matches(
    status_pid: object,
    status_created_utc: object,
    observed_pid: object,
    observed_created_utc: object,
) -> bool:
    """Require exact PID and creation identity equality within one OS."""

    return (
        isinstance(status_pid, int)
        and not isinstance(status_pid, bool)
        and isinstance(status_created_utc, str)
        and isinstance(observed_pid, int)
        and not isinstance(observed_pid, bool)
        and isinstance(observed_created_utc, str)
        and status_pid == observed_pid
        and status_created_utc == observed_created_utc
    )


def validate_codex_identity(
    codex_pid: int,
    pinned_codex: Path,
    ancestry_chain: list[dict[str, Any]],
    *,
    provider_pid: int,
    observed_executable: Path | None,
) -> None:
    """Validate the actual pinned executable and its exact provider ancestry."""

    if not isinstance(codex_pid, int) or codex_pid <= 0:
        raise RuntimeError("observed Codex PID is invalid")
    if not isinstance(provider_pid, int) or provider_pid <= 0:
        raise RuntimeError("provider root PID is invalid")
    if not ancestry_chain or ancestry_chain[0].get("pid") != codex_pid:
        raise RuntimeError("Codex ancestry does not start at the observed Codex PID")
    if ancestry_chain[-1].get("pid") != provider_pid:
        raise RuntimeError("Codex ancestry does not terminate at the controller-owned provider")
    pids = [item.get("pid") for item in ancestry_chain]
    if any(not isinstance(pid, int) for pid in pids) or len(set(pids)) != len(pids):
        raise RuntimeError("Codex ancestry contains invalid or repeated identities")
    if any(
        item.get("ppid") != ancestry_chain[index + 1].get("pid")
        for index, item in enumerate(ancestry_chain[:-1])
    ):
        raise RuntimeError("Codex ancestry has a broken parent chain")
    if observed_executable is None:
        raise RuntimeError("observed Codex executable is unavailable")
    try:
        observed_stat = observed_executable.stat()
        pinned_stat = pinned_codex.resolve(strict=True).stat()
    except OSError as exc:
        raise RuntimeError(f"cannot inspect pinned Codex identity: {exc}") from exc
    if (
        observed_stat.st_dev != pinned_stat.st_dev
        or observed_stat.st_ino != pinned_stat.st_ino
    ):
        raise RuntimeError("observed Codex executable is not the pinned installed Codex")


__all__ = [
    "provider_identity_matches",
    "validate_codex_identity",
    "validate_cross_os_identity_relation",
]
