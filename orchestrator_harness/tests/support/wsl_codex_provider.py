"""Provider-only Linux bridge for the Windows public controller route.

The Windows lane controller launches this file through ``wsl.exe`` as its
provider child.  This bridge claims a prepared Linux isolation state once,
executes only the pinned Codex command inside the prepared cgroup/netns and
bubblewrap boundary, forwards the provider protocol, and records redacted
identity/cleanup facts.  It never launches or imports the public controller.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    from .wsl_real_agent_driver import bwrap_base
except ImportError:  # Direct support-script execution.
    from wsl_real_agent_driver import bwrap_base  # type: ignore[no-redef]


BRIDGE_SCHEMA = "orchestrator-wsl-provider-bridge/v1"


def _sandbox_arguments(arguments: list[str]) -> list[str]:
    rewritten: list[str] = []
    index = 0
    while index < len(arguments):
        value = arguments[index]
        if value == "--cd" and index + 1 < len(arguments):
            rewritten.extend((value, "/workspace"))
            index += 2
            continue
        if value == "--output-last-message" and index + 1 < len(arguments):
            rewritten.extend((value, "/workspace/.agent-workspace/real_agent_last_message.txt"))
            index += 2
            continue
        rewritten.append(value)
        index += 1
    return rewritten


def _provider_argv(command: list[str], base: list[str]) -> list[str]:
    """Build the exact pinned Codex argv, retaining the adapter's ``exec``."""

    if not command or command[0] != "exec":
        raise RuntimeError("native Codex provider adapter must supply an exec action")
    return [*base, "/opt/codex/bin/codex", *_sandbox_arguments(command)]


def _linux_creation_identity(pid: int) -> str:
    """Return a boot-scoped Linux process-start identity, not a bare PID."""

    stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    close = stat.rfind(")")
    fields = stat[close + 2 :].split()
    if len(fields) <= 19:
        raise RuntimeError("Linux process stat lacks a start identity")
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    return f"{boot_id}:{fields[19]}"


def _fork_provider(command: list[str]) -> int:
    """Run the containment launcher without an unowned Python Popen handle."""

    pid = os.fork()
    if pid == 0:
        try:
            os.execv(command[0], command)
        except BaseException:
            os._exit(127)
    return pid


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one prepared Codex provider bridge")
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--invocation-id", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--claim", required=True, type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    args.command = list(args.command)
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    return args


def _support_helpers() -> tuple[Any, ...]:
    try:
        from . import wsl_real_agent_driver as support
    except ImportError:
        import wsl_real_agent_driver as support  # type: ignore[no-redef]
    return (
        support.atomic_json,
        support.claim_prepared_state,
        support.cgroup_processes,
        support.discover_codex,
        support.kill_cgroup,
        support.proc_exe,
        support.validate_prepared_state,
    )


def main() -> int:
    args = _parse()
    (
        atomic_json,
        claim_prepared_state,
        cgroup_processes,
        discover_codex,
        kill_cgroup,
        proc_exe,
        validate_prepared_state,
    ) = _support_helpers()
    state_value = json.loads(args.state.read_text(encoding="utf-8"))
    state = validate_prepared_state(
        state_value, nonce=args.nonce, invocation_id=args.invocation_id
    )
    claim = claim_prepared_state(
        args.claim, nonce=args.nonce, invocation_id=args.invocation_id
    )
    pinned = Path(state["pinned_codex"]).resolve(strict=True)
    pinned_stat = pinned.stat()
    if pinned_stat.st_dev != state.get("pinned_codex_device") or pinned_stat.st_ino != state.get("pinned_codex_inode"):
        raise RuntimeError("prepared pinned Codex device/inode changed")
    base = bwrap_base(
        Path(state["bwrap"]), Path(state["release"]), Path(state["workspace"]),
        Path(state["codex_home"]), str(state["proxy_url"]),
    )
    provider_argv = _provider_argv(args.command, base)
    cgroup_launcher = Path(state["cgroup_launcher"])
    launch_command = [
        "/usr/bin/python3", str(cgroup_launcher), "--cgroup", str(state["cgroup"]),
        "--network-namespace", str(state["network_namespace"]), "--", *provider_argv,
    ]
    linux_bridge_pid = _fork_provider(launch_command)
    linux_bridge_created = _linux_creation_identity(linux_bridge_pid)
    codex_pid: int | None = None
    codex_created: str | None = None
    codex_chain: list[dict[str, Any]] = []
    exit_code: int | None = None
    failure: BaseException | None = None
    try:
        codex_pid, codex_chain = discover_codex(
            Path(state["cgroup"]), pinned, linux_bridge_pid, timeout=30.0
        )
        codex_created = _linux_creation_identity(codex_pid)
        _ = proc_exe(codex_pid)
        _, wait_status = os.waitpid(linux_bridge_pid, 0)
        exit_code = os.waitstatus_to_exitcode(wait_status)
        if exit_code != 0:
            raise RuntimeError(f"prepared Codex provider exited with {exit_code}")
    except BaseException as exc:
        failure = exc
        try:
            kill_cgroup(Path(state["cgroup"]))
        except BaseException as cleanup_error:
            failure = RuntimeError(f"{exc}; cleanup failed: {cleanup_error}")
        try:
            _, wait_status = os.waitpid(linux_bridge_pid, 0)
            exit_code = os.waitstatus_to_exitcode(wait_status)
        except ChildProcessError:
            pass
    finally:
        remaining = sorted(cgroup_processes(Path(state["cgroup"])))
        evidence = {
            "schema": BRIDGE_SCHEMA,
            "status": "PASS" if failure is None and not remaining else "FAIL",
            "nonce": args.nonce,
            "invocation_id": args.invocation_id,
            "prepared_claim": claim,
            "linux_bridge": {
                "platform": "linux", "pid": linux_bridge_pid,
                "created_utc": linux_bridge_created,
                "nonce": args.nonce, "invocation_id": args.invocation_id,
            },
            "codex": {
                "pid": codex_pid,
                "created_utc": codex_created,
                "pinned_device": pinned_stat.st_dev,
                "pinned_inode": pinned_stat.st_ino,
                "pinned_sha256": hashlib.sha256(pinned.read_bytes()).hexdigest(),
                "ancestry": codex_chain,
            },
            "sandbox": {
                "workspace_target": "/workspace", "codex_target": "/opt/codex",
                "host_auth_target": "/home/agent/.codex", "mnt_c_exposed": False,
                "usb_exposed": False, "capabilities_dropped": True,
                "network_namespace": state["network_namespace"],
            },
            "exit_code": exit_code,
            "cgroup_remaining": remaining,
            "cleanup_complete": not remaining,
            "credentials_in_evidence": False,
        }
        if failure is not None:
            evidence["error_type"] = type(failure).__name__
        atomic_json(args.evidence, evidence)
        # The preparation waiter owns the namespace/cgroup cleanup.  This
        # marker is intentionally only a control signal, never a transcript.
        release_signal = Path(str(state["release_signal"]))
        atomic_json(release_signal, {"nonce": args.nonce, "invocation_id": args.invocation_id})
    if failure is not None:
        raise failure
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
