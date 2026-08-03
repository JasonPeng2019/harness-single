from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .allowlist_connect_proxy import AllowlistProxy
except ImportError:  # Direct support-script execution.
    from allowlist_connect_proxy import (  # pyright: ignore[reportImplicitRelativeImport]
        AllowlistProxy,
    )


NOBODY = 65534
ALLOWED_OPENAI_ENDPOINTS = {
    ("chatgpt.com", 443),
}
FORBIDDEN_COMMAND_MARKERS = (
    "byo-firmware-mcp",
    "pyocd",
    "jlink",
    "openocd",
    "st-util",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(
    argv: list[str],
    *,
    check: bool = True,
    input_text: str | None = None,
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=check,
        input=input_text,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def copy_harness_source(source_root: Path, destination: Path) -> Path:
    source = source_root / "orchestrator_harness"

    def ignored(_directory: str, names: list[str]) -> set[str]:
        return {
            name
            for name in names
            if name in {".real-agent", "test-evidence", "__pycache__", ".pytest_cache"}
            or name.endswith(".pyc")
        }

    shutil.copytree(source, destination / "orchestrator_harness", ignore=ignored)
    return destination


def token_expiry(token: str) -> str:
    parts = token.split(".")
    if len(parts) != 3:
        raise RuntimeError("access token is not a JWT")
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    value = json.loads(base64.urlsafe_b64decode(payload))
    return (
        datetime.fromtimestamp(int(value["exp"]), timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def minimal_auth(source: Path, destination: Path) -> tuple[list[str], str, str]:
    value = json.loads(source.read_text(encoding="utf-8"))
    tokens = value.get("tokens")
    if not isinstance(tokens, dict):
        raise RuntimeError("source Codex auth has no token object")
    access = tokens.get("access_token")
    account = tokens.get("account_id")
    id_token = tokens.get("id_token")
    if (
        not isinstance(access, str)
        or not isinstance(account, str)
        or not isinstance(id_token, str)
    ):
        raise RuntimeError(
            "source Codex auth lacks access token/account/ID format fields"
        )
    expiry = token_expiry(access)
    id_expiry = token_expiry(id_token)
    if datetime.fromisoformat(expiry.replace("Z", "+00:00")) <= datetime.now(
        timezone.utc
    ):
        raise RuntimeError("source Codex access token is expired")
    if datetime.fromisoformat(id_expiry.replace("Z", "+00:00")) > datetime.now(
        timezone.utc
    ):
        raise RuntimeError(
            "Codex requires an ID-format field; refusing to expose one until it is expired"
        )
    minimized = {
        "auth_mode": value.get("auth_mode", "chatgpt"),
        "OPENAI_API_KEY": None,
        "tokens": {
            # Codex 0.146.0 requires a syntactically valid ID-token field at load time.
            # This copy is already expired and is not a usable bearer credential.
            "id_token": id_token,
            "access_token": access,
            "refresh_token": "",
            "account_id": account,
        },
        "last_refresh": value.get("last_refresh"),
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(minimized, indent=2) + "\n", encoding="utf-8")
    os.chmod(destination, 0o600)
    secrets: list[str] = []
    for key in ("access_token", "refresh_token", "id_token"):
        item = tokens.get(key)
        if isinstance(item, str) and item:
            secrets.append(item)
    api_key = value.get("OPENAI_API_KEY")
    if isinstance(api_key, str) and api_key:
        secrets.append(api_key)
    return secrets, expiry, id_expiry


def cgroup_processes(cgroup: Path) -> set[int]:
    result: set[int] = set()
    for file in cgroup.rglob("cgroup.procs"):
        try:
            result.update(int(line) for line in file.read_text().splitlines() if line)
        except (OSError, ValueError):
            continue
    return result


def proc_exe(pid: int) -> Path | None:
    try:
        return Path(os.readlink(f"/proc/{pid}/exe"))
    except OSError:
        return None


def proc_ppid(pid: int) -> int:
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    close = stat.rfind(")")
    return int(stat[close + 2 :].split()[1])


def proc_command(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return ""
    return raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()


def proc_nspid(pid: int) -> list[int]:
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
    except OSError:
        return []
    for line in lines:
        if line.startswith("NSpid:"):
            return [int(item) for item in line.split()[1:]]
    return []


def ancestry(pid: int, stop_pid: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    current = pid
    while current > 0 and current not in seen:
        seen.add(current)
        result.append(
            {
                "pid": current,
                "ppid": proc_ppid(current),
                "exe": str(proc_exe(current) or ""),
                "command": proc_command(current),
                "nspid": proc_nspid(current),
            }
        )
        if current == stop_pid:
            return result
        current = int(result[-1]["ppid"])
    raise RuntimeError(f"Codex PID {pid} is not descended from sandbox root {stop_pid}")


def discover_codex(
    cgroup: Path, pinned_codex: Path, root_pid: int, timeout: float
) -> tuple[int, list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout
    pinned = pinned_codex.resolve()
    observed: dict[int, dict[str, str]] = {}
    while time.monotonic() < deadline:
        if not Path(f"/proc/{root_pid}").exists():
            raise RuntimeError(
                "sandbox root exited before actual Codex identity was captured"
            )
        for pid in sorted(cgroup_processes(cgroup)):
            executable = proc_exe(pid)
            observed[pid] = {
                "exe": str(executable or ""),
                "command": proc_command(pid),
            }
            if executable is None:
                continue
            try:
                candidate_stat = os.stat(f"/proc/{pid}/exe")
                pinned_stat = pinned.stat()
                matches = (
                    candidate_stat.st_dev == pinned_stat.st_dev
                    and candidate_stat.st_ino == pinned_stat.st_ino
                )
            except OSError:
                matches = False
            if matches:
                chain = ancestry(pid, root_pid)
                if len(chain) < 2:
                    raise RuntimeError("Codex ancestry has no containment supervisor")
                return pid, chain
        time.sleep(0.05)
    raise TimeoutError(
        f"actual Codex process did not appear in the dedicated cgroup; "
        f"pinned={pinned}; observed={observed}"
    )


def make_network_namespace(identifier: str, proxy_audit: Path) -> dict[str, Any]:
    suffix = (int(identifier[:4], 16) % 200) + 20
    namespace = f"oh-{identifier[:8]}"
    host_if = f"ohh{identifier[:8]}"[:15]
    inner_if = f"ohi{identifier[:8]}"[:15]
    host_ip = f"10.253.{suffix}.1"
    inner_ip = f"10.253.{suffix}.2"
    run(["ip", "netns", "add", namespace])
    try:
        run(["ip", "link", "add", host_if, "type", "veth", "peer", "name", inner_if])
        run(["ip", "link", "set", inner_if, "netns", namespace])
        run(["ip", "addr", "add", f"{host_ip}/30", "dev", host_if])
        run(["ip", "link", "set", host_if, "up"])
        run(["ip", "netns", "exec", namespace, "ip", "link", "set", "lo", "up"])
        run(
            [
                "ip",
                "netns",
                "exec",
                namespace,
                "ip",
                "addr",
                "add",
                f"{inner_ip}/30",
                "dev",
                inner_if,
            ]
        )
        run(["ip", "netns", "exec", namespace, "ip", "link", "set", inner_if, "up"])
    except BaseException:
        run(["ip", "netns", "del", namespace], check=False)
        raise
    proxy = AllowlistProxy(host_ip, ALLOWED_OPENAI_ENDPOINTS, proxy_audit)
    proxy.start()
    rules = f"""
table inet harness {{
  chain input {{
    type filter hook input priority 0; policy drop;
    iifname "lo" accept
    ct state established,related accept
  }}
  chain output {{
    type filter hook output priority 0; policy drop;
    oifname "lo" accept
    ip daddr {host_ip} tcp dport {proxy.port} accept
  }}
}}
"""
    try:
        run(
            ["ip", "netns", "exec", namespace, "nft", "-f", "-"],
            input_text=rules,
        )
    except BaseException:
        proxy.close()
        run(["ip", "netns", "del", namespace], check=False)
        raise
    return {
        "name": namespace,
        "host_if": host_if,
        "inner_if": inner_if,
        "host_ip": host_ip,
        "inner_ip": inner_ip,
        "proxy_port": proxy.port,
        "proxy": proxy,
    }


def bwrap_base(
    bwrap: Path,
    release: Path,
    workspace: Path,
    codex_home: Path,
    proxy_url: str,
) -> list[str]:
    return [
        "setpriv",
        f"--reuid={NOBODY}",
        f"--regid={NOBODY}",
        "--clear-groups",
        str(bwrap),
        "--unshare-user",
        "--unshare-ipc",
        "--unshare-pid",
        "--unshare-uts",
        "--unshare-cgroup",
        "--die-with-parent",
        "--new-session",
        "--tmpfs",
        "/",
        "--ro-bind",
        "/usr",
        "/usr",
        "--symlink",
        "usr/bin",
        "/bin",
        "--symlink",
        "usr/lib",
        "/lib",
        "--symlink",
        "usr/lib64",
        "/lib64",
        "--dir",
        "/etc",
        "--ro-bind",
        "/etc/ssl",
        "/etc/ssl",
        "--ro-bind",
        "/etc/passwd",
        "/etc/passwd",
        "--ro-bind",
        "/etc/group",
        "/etc/group",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--dir",
        "/opt",
        "--ro-bind",
        str(release),
        "/opt/codex",
        "--dir",
        "/home",
        "--dir",
        "/home/agent",
        "--bind",
        str(codex_home),
        "/home/agent/.codex",
        "--bind",
        str(workspace),
        "/workspace",
        "--chdir",
        "/workspace",
        "--clearenv",
        "--setenv",
        "HOME",
        "/home/agent",
        "--setenv",
        "CODEX_HOME",
        "/home/agent/.codex",
        "--setenv",
        "PATH",
        "/opt/codex/bin:/usr/bin:/bin",
        "--setenv",
        "SSL_CERT_DIR",
        "/etc/ssl/certs",
        "--setenv",
        "PYTHONNOUSERSITE",
        "1",
        "--setenv",
        "HTTP_PROXY",
        proxy_url,
        "--setenv",
        "HTTPS_PROXY",
        proxy_url,
        "--setenv",
        "http_proxy",
        proxy_url,
        "--setenv",
        "https_proxy",
        proxy_url,
        "--setenv",
        "NO_PROXY",
        "",
        "--setenv",
        "no_proxy",
        "",
        "--cap-drop",
        "ALL",
    ]


def preflight(
    namespace: str,
    base: list[str],
    proxy_host: str,
    proxy_port: int,
) -> dict[str, Any]:
    script = r"""
import json, os, pathlib, socket

def connect(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.4):
            return True
    except OSError:
        return False

def proxy_status(target):
    with socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=2) as sock:
        sock.sendall(("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n").encode())
        return sock.recv(128).split(b"\r\n", 1)[0].decode("ascii", "replace")

caps = ""
for line in pathlib.Path("/proc/self/status").read_text().splitlines():
    if line.startswith("CapEff:"):
        caps = line.split()[1]
route = pathlib.Path("/proc/net/route").read_text()
value = {
    "uid": os.getuid(),
    "gid": os.getgid(),
    "cap_eff": caps,
    "mnt_c_present": pathlib.Path("/mnt/c").exists(),
    "usb_present": pathlib.Path("/dev/bus/usb").exists(),
    "repo_marker_present": pathlib.Path("/workspace/BYO-Firmware-MCP").exists(),
    "workspace_writable": os.access("/workspace", os.W_OK),
    "usr_writable": os.access("/usr", os.W_OK),
    "default_route_present": any(line.split()[1] == "00000000" for line in route.splitlines()[1:] if len(line.split()) > 1),
    "metadata_direct": connect("169.254.169.254", 80),
    "internet_direct": connect("1.1.1.1", 443),
    "host_other_port": connect(PROXY_HOST, 1),
    "denied_proxy_status": proxy_status("example.com:443"),
    "allowed_proxy_status": proxy_status("chatgpt.com:443"),
}
print(json.dumps(value, sort_keys=True))
""".replace("PROXY_HOST", repr(proxy_host)).replace("PROXY_PORT", str(proxy_port))
    completed = run(
        ["ip", "netns", "exec", namespace, *base, "/usr/bin/python3", "-c", script],
        timeout=30,
    )
    result = json.loads(completed.stdout.splitlines()[-1])
    expected = {
        "uid": NOBODY,
        "gid": NOBODY,
        "cap_eff": "0000000000000000",
        "mnt_c_present": False,
        "usb_present": False,
        "repo_marker_present": False,
        "workspace_writable": True,
        "usr_writable": False,
        "default_route_present": False,
        "metadata_direct": False,
        "internet_direct": False,
        "host_other_port": False,
        "denied_proxy_status": "HTTP/1.1 403 Forbidden",
        "allowed_proxy_status": "HTTP/1.1 200 Connection Established",
    }
    if result != expected:
        raise RuntimeError(f"isolation preflight mismatch: {result}")
    return result


def wait_for(path: Path, process: subprocess.Popen[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            raise RuntimeError(f"sandbox exited {process.returncode} before {path}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {path}")


def redact_tree(root: Path, secrets: list[str]) -> None:
    encoded = [(secret, secret.encode()) for secret in secrets if secret]
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        raw = path.read_bytes()
        changed = raw
        for _text, secret in encoded:
            changed = changed.replace(secret, b"<REDACTED>")
        if changed != raw:
            path.write_bytes(changed)
        for _text, secret in encoded:
            if secret in path.read_bytes():
                raise RuntimeError(f"credential remains in evidence file {path}")


def copy_evidence(temp_root: Path, evidence: Path, secrets: list[str]) -> None:
    evidence.mkdir(parents=True, exist_ok=True)
    for name in ("synthetic-suite", "watcher-state"):
        source = temp_root / name
        if source.exists():
            target = evidence / name
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(source, target)
    for name in (
        "isolation-preflight.json",
        "proxy-audit.jsonl",
        "process-ancestry.json",
        "cgroup-evidence.json",
        "driver-failure.txt",
    ):
        source = temp_root / name
        if source.exists():
            shutil.copy2(source, evidence / name)
    redact_tree(evidence, secrets)


def kill_cgroup(cgroup: Path) -> None:
    if not cgroup.exists():
        return
    if cgroup_processes(cgroup):
        (cgroup / "cgroup.kill").write_text("1\n", encoding="ascii")
    deadline = time.monotonic() + 10
    while cgroup_processes(cgroup) and time.monotonic() < deadline:
        time.sleep(0.05)
    if cgroup_processes(cgroup):
        raise RuntimeError("dedicated real-agent cgroup did not become empty")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--auth-json", required=True)
    parser.add_argument("--codex-root", required=True)
    parser.add_argument("--model", default="gpt-5.6-terra")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError("WSL real-agent driver requires root for netns/cgroup setup")

    source_root = Path(args.source_root).resolve()
    evidence = Path(args.evidence_dir).resolve()
    auth_source = Path(args.auth_json).resolve()
    codex_root = Path(args.codex_root).resolve()
    codex_link = codex_root / "bin" / "codex"
    if not codex_link.exists():
        raise RuntimeError(
            f"Linux Codex is missing at {codex_link}; run install_wsl_codex.ps1"
        )
    pinned_codex = codex_link.resolve()
    release = pinned_codex.parent.parent
    bwrap = release / "codex-resources" / "bwrap"
    if not bwrap.is_file():
        raise RuntimeError(f"pinned bubblewrap is unavailable: {bwrap}")

    identifier = args.run_id
    if len(identifier) != 32 or any(
        character not in "0123456789abcdef" for character in identifier
    ):
        raise RuntimeError("run-id must be exactly 32 lowercase hexadecimal characters")
    temp_root = Path(f"/tmp/orchestrator-harness-real-agent-{identifier}")
    copied_root = copy_harness_source(source_root, temp_root / "runner-source")
    sys.path.insert(0, str(copied_root))
    from orchestrator_harness.cli import watch_once
    from orchestrator_harness.config import load_config
    from orchestrator_harness.processes import process_snapshot

    synthetic_suite = temp_root / "synthetic-suite"
    synthetic_run = synthetic_suite / "runs" / "HARNESS_REAL_AGENT"
    agent_workspace = synthetic_run / ".agent-workspace"
    codex_home = temp_root / "codex-home"
    watcher_state = copied_root / "orchestrator_harness" / ".real-agent-state"
    for directory in (agent_workspace, codex_home):
        directory.mkdir(parents=True, exist_ok=True)
    helper = synthetic_run / "synthetic_agent_helper.py"
    shutil.copy2(
        copied_root
        / "orchestrator_harness"
        / "tests"
        / "support"
        / "synthetic_agent_helper.py",
        helper,
    )
    secrets, access_expiry, expired_id_expiry = minimal_auth(
        auth_source, codex_home / "auth.json"
    )
    os.chown(codex_home, NOBODY, NOBODY)
    os.chown(codex_home / "auth.json", NOBODY, NOBODY)
    for root, directories, files in os.walk(synthetic_run):
        os.chown(root, NOBODY, NOBODY)
        for name in directories:
            os.chown(Path(root) / name, NOBODY, NOBODY)
        for name in files:
            os.chown(Path(root) / name, NOBODY, NOBODY)

    config_path = temp_root / "config.json"
    atomic_json(
        config_path,
        {
            "suite_root": str(synthetic_suite),
            "run_globs": ["runs/*"],
            "workspace_relpath": ".agent-workspace",
            "output_dir": str(watcher_state),
            "poll_interval_seconds": 0.1,
            "watch_timeout_seconds": 30,
            "request_warning_seconds": 30,
            "request_critical_seconds": 10,
            "process_start_tolerance_seconds": 2,
            "stable_read_delay_seconds": 0.02,
        },
    )
    config = load_config(config_path)

    cgroup = Path("/sys/fs/cgroup") / f"orchestrator-harness-{identifier}"
    cgroup.mkdir()
    (cgroup / "pids.max").write_text("64\n")
    (cgroup / "memory.max").write_text(f"{1024 * 1024 * 1024}\n")
    (cgroup / "memory.swap.max").write_text("0\n")
    (cgroup / "cpu.max").write_text("200000 100000\n")
    proxy_audit = temp_root / "proxy-audit.jsonl"
    network: dict[str, Any] | None = None
    process: subprocess.Popen[str] | None = None
    stdout_stream = None
    stderr_stream = None
    event_types: list[str] = []
    request_hash = ""
    failure: BaseException | None = None
    result: dict[str, Any] | None = None
    try:
        network = make_network_namespace(identifier, proxy_audit)
        proxy_url = f"http://{network['host_ip']}:{network['proxy_port']}"
        base = bwrap_base(bwrap, release, synthetic_run, codex_home, proxy_url)
        isolation = preflight(
            network["name"], base, network["host_ip"], network["proxy_port"]
        )
        isolation.update(
            {
                "network_namespace": network["name"],
                "allowed_endpoints": sorted(
                    f"{host}:{port}" for host, port in ALLOWED_OPENAI_ENDPOINTS
                ),
                "access_token_expiry_utc": access_expiry,
                "refresh_token_present": False,
                "id_token_present": "expired-format-only",
                "expired_id_token_expiry_utc": expired_id_expiry,
                "api_key_present": False,
                "captured_utc": utc_now(),
            }
        )
        atomic_json(temp_root / "isolation-preflight.json", isolation)

        last_message = agent_workspace / "real_agent_last_message.txt"
        jsonl = agent_workspace / "real_agent_codex.jsonl"
        stderr_path = agent_workspace / "real_agent_codex.stderr.log"
        stdout_stream = jsonl.open("w", encoding="utf-8")
        stderr_stream = stderr_path.open("w", encoding="utf-8")
        argv = [
            "ip",
            "netns",
            "exec",
            network["name"],
            *base,
            "/opt/codex/bin/codex",
            "-C",
            "/workspace",
            "-m",
            args.model,
            "-c",
            'model_reasoning_effort="high"',
            "-c",
            'service_tier="priority"',
            "-c",
            "mcp_servers={}",
            "-c",
            'approval_policy="never"',
            "-c",
            'approvals_reviewer="user"',
            "-s",
            "danger-full-access",
            "-a",
            "never",
            "exec",
            "--skip-git-repo-check",
            "--ignore-user-config",
            "--json",
            "--output-last-message",
            "/workspace/.agent-workspace/real_agent_last_message.txt",
            "-",
        ]

        cgroup_launcher = (
            copied_root
            / "orchestrator_harness"
            / "tests"
            / "support"
            / "cgroup_exec.py"
        )
        process = subprocess.Popen(
            [
                "/usr/bin/python3",
                str(cgroup_launcher),
                "--cgroup",
                str(cgroup),
                "--",
                *argv,
            ],
            cwd=synthetic_run,
            stdin=subprocess.PIPE,
            stdout=stdout_stream,
            stderr=stderr_stream,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        codex_pid, chain = discover_codex(cgroup, pinned_codex, process.pid, 20)
        cgroup_pids = cgroup_processes(cgroup)
        if any(int(item["pid"]) not in cgroup_pids for item in chain[:-1]):
            raise RuntimeError(
                "Codex containment ancestry escaped the dedicated cgroup"
            )
        immediate_parent = int(chain[0]["ppid"])
        snapshot = process_snapshot()
        codex_observation = snapshot.by_pid.get(codex_pid)
        parent_observation = snapshot.by_pid.get(immediate_parent)
        if (
            not snapshot.complete
            or codex_observation is None
            or parent_observation is None
            or codex_observation.created_utc is None
            or parent_observation.created_utc is None
        ):
            raise RuntimeError("cannot prove actual Codex outer process identity")
        process_evidence = {
            "sandbox_root_pid": process.pid,
            "codex_pid": codex_pid,
            "codex_created_utc": codex_observation.created_utc.isoformat().replace(
                "+00:00", "Z"
            ),
            "controller_created_utc": parent_observation.created_utc.isoformat().replace(
                "+00:00", "Z"
            ),
            "immediate_parent_pid": immediate_parent,
            "ancestry": chain,
            "cgroup": str(cgroup),
            "cgroup_pids_at_capture": sorted(cgroup_pids),
            "pinned_codex_sha256": hashlib.sha256(
                pinned_codex.read_bytes()
            ).hexdigest(),
        }
        atomic_json(temp_root / "process-ancestry.json", process_evidence)
        atomic_json(
            agent_workspace / "outer_identity.json",
            {
                "pid": codex_pid,
                "started_utc": process_evidence["codex_created_utc"],
                "executable_sha256": process_evidence["pinned_codex_sha256"],
            },
        )
        started = process_evidence["codex_created_utc"]
        status_path = agent_workspace / "real_agent_controller.status.json"
        atomic_json(
            status_path,
            {
                "state": "running",
                "controller_pid": immediate_parent,
                "codex_pid": codex_pid,
                "outer_supervisor_pid": process.pid,
                "doer": "RealAgent",
                "task": "HARNESS-REAL-AGENT",
                "phase": "synthetic same-turn relay wait",
                "thread_id": "real-agent-synthetic-session",
                "started_utc": started,
                "controller_started_utc": process_evidence["controller_created_utc"],
                "codex_started_utc": process_evidence["codex_created_utc"],
                "board_tokens": [],
                "mcp_servers": [],
                "sandbox": "danger-full-access-inside-bubblewrap",
                "approval_policy": "never",
                "model": args.model,
                "service_tier": "priority",
            },
        )
        prompt = (
            "Authorized local host-only orchestration-harness validation. "
            "Work only in this synthetic directory. No firmware server, MCP server, board, "
            "probe, serial endpoint, hardware utility, network fetch, or parent-directory "
            "inspection is in scope. Run exactly this one command and wait for it to finish:\n\n"
            "python3 ./synthetic_agent_helper.py ./.agent-workspace "
            "--identity-file ./.agent-workspace/outer_identity.json --wait-seconds 90\n\n"
            "Do not run any other command. When it exits zero, reply SYNTHETIC_RELAY_COMPLETE."
        )
        assert process.stdin is not None
        process.stdin.write(prompt)
        process.stdin.close()

        _, initial_events = watch_once(config, no_write=False)
        event_types.extend(event["type"] for event in initial_events)
        request_path = (
            agent_workspace
            / "permission-requests"
            / "real-agent-synthetic-request.json"
        )
        wait_for(request_path, process, 120)
        helper_pid = None
        for pid in sorted(cgroup_processes(cgroup)):
            if "synthetic_agent_helper.py" in proc_command(pid):
                helper_pid = pid
                break
        if helper_pid is None:
            raise RuntimeError("synthetic helper process was not visible in the cgroup")
        helper_snapshot = process_snapshot()
        helper_process = helper_snapshot.by_pid.get(helper_pid)
        if (
            not helper_snapshot.complete
            or helper_process is None
            or helper_process.created_utc is None
        ):
            raise RuntimeError("cannot prove synthetic helper process identity")
        helper_record = agent_workspace / "helper_process.json"
        atomic_json(
            helper_record,
            {
                "role": "synthetic-real-agent-helper",
                "pid": helper_pid,
                "started_utc": helper_process.created_utc.isoformat().replace(
                    "+00:00", "Z"
                ),
            },
        )
        os.chown(helper_record, NOBODY, NOBODY)
        _, request_events = watch_once(config, no_write=False)
        event_types.extend(event["type"] for event in request_events)
        if "RELAY_READY" not in event_types:
            raise AssertionError(f"watcher did not emit RELAY_READY: {event_types}")
        for pid in cgroup_processes(cgroup):
            command = proc_command(pid).lower()
            scrubbed = command.replace("mcp_servers={}", "")
            if any(marker in scrubbed for marker in FORBIDDEN_COMMAND_MARKERS):
                raise AssertionError(
                    f"forbidden provider process in cgroup: {pid} {command}"
                )

        request_hash = hashlib.sha256(request_path.read_bytes()).hexdigest()
        relay_path = (
            agent_workspace
            / "permission-requests"
            / "real-agent-synthetic-request.relay.json"
        )
        atomic_json(
            relay_path,
            {
                "decision": "approved",
                "request_sha256": request_hash,
                "run_id": "real-agent-synthetic-run",
                "session_id": "real-agent-synthetic-session",
                "scope": "synthetic host-only no-hardware relay",
            },
        )
        os.chown(relay_path, NOBODY, NOBODY)
        _, relayed_events = watch_once(config, no_write=False)
        event_types.extend(event["type"] for event in relayed_events)
        code = process.wait(timeout=150)
        stdout_stream.close()
        stdout_stream = None
        stderr_stream.close()
        stderr_stream = None
        status_value = json.loads(status_path.read_text(encoding="utf-8"))
        status_value.update(
            {
                "state": "exited",
                "exit_code": code,
                "ended_utc": utc_now(),
            }
        )
        atomic_json(status_path, status_value)
        if code != 0:
            raise RuntimeError(
                f"real Codex exited {code}: "
                + stderr_path.read_text(encoding="utf-8", errors="replace")[-4000:]
            )
        _, final_events = watch_once(config, no_write=False)
        event_types.extend(event["type"] for event in final_events)
        required = {
            "CONTROLLER_ACTIVE",
            "CONTROLLER_EXITED",
            "HELPER_ACTIVE",
            "HELPER_EXITED",
            "RELAY_READY",
            "RELAYED",
            "CHECKPOINT_UPDATED",
            "RESOURCE_RELEASE_POSSIBLE",
        }
        missing = sorted(required - set(event_types))
        if missing:
            raise AssertionError(
                f"missing real-agent watcher events {missing}: {event_types}"
            )
        forbidden_mcp_events = {
            "MCP_ACTIVE",
            "MCP_EXITED",
            "MCP_STATE_UNKNOWN",
        } & set(event_types)
        if forbidden_mcp_events:
            raise AssertionError(
                "real-agent fixture declares no MCP/provider lifetime but emitted "
                f"{sorted(forbidden_mcp_events)}: {event_types}"
            )
        helper_result = json.loads(
            (agent_workspace / "synthetic-helper-result.json").read_text(
                encoding="utf-8"
            )
        )
        if helper_result.get("status") != "PASS":
            raise AssertionError(f"synthetic helper did not pass: {helper_result}")
        if "SYNTHETIC_RELAY_COMPLETE" not in last_message.read_text(
            encoding="utf-8", errors="replace"
        ):
            raise AssertionError("real agent did not report synthetic relay completion")
        result = {
            "status": "PASS",
            "completed_utc": utc_now(),
            "model": args.model,
            "reasoning_effort": "high",
            "service_tier": "priority",
            "sandbox": "danger-full-access inside OS-enforced WSL2/bubblewrap",
            "mcp_servers": [],
            "event_types": event_types,
            "request_sha256": request_hash,
            "access_token_expiry_utc": access_expiry,
            "network_allowlist": sorted(
                f"{host}:{port}" for host, port in ALLOWED_OPENAI_ENDPOINTS
            ),
            "cgroup_limits": {
                "pids.max": 64,
                "memory.max": 1024 * 1024 * 1024,
                "memory.swap.max": 0,
                "cpu.max": "200000 100000",
            },
        }
    except BaseException as exc:
        failure = exc
        (temp_root / "driver-failure.txt").write_text(
            f"{type(exc).__name__}: {exc}\n", encoding="utf-8"
        )
    finally:
        try:
            kill_cgroup(cgroup)
            if process is not None and process.poll() is None:
                process.wait(timeout=10)
        except BaseException as cleanup_error:
            if failure is None:
                failure = cleanup_error
        if stdout_stream is not None:
            stdout_stream.close()
        if stderr_stream is not None:
            stderr_stream.close()
        cgroup_evidence = {
            "limits": {
                name: (cgroup / name).read_text().strip()
                for name in ("pids.max", "memory.max", "memory.swap.max", "cpu.max")
                if (cgroup / name).exists()
            },
            "events": (cgroup / "memory.events").read_text().splitlines()
            if (cgroup / "memory.events").exists()
            else [],
            "remaining_pids": sorted(cgroup_processes(cgroup)),
            "captured_utc": utc_now(),
        }
        atomic_json(temp_root / "cgroup-evidence.json", cgroup_evidence)
        if cgroup_evidence["remaining_pids"]:
            failure = failure or RuntimeError("real-agent cgroup is not empty")
        if network is not None:
            network["proxy"].close()
            run(["ip", "netns", "del", network["name"]], check=False)
        if result is not None and failure is None:
            atomic_json(temp_root / "REAL_AGENT_TEST_RESULT.json", result)
        if watcher_state.exists():
            preserved_watcher = temp_root / "watcher-state"
            if preserved_watcher.exists():
                shutil.rmtree(preserved_watcher)
            shutil.copytree(watcher_state, preserved_watcher)
        copy_evidence(temp_root, evidence, secrets)
        if result is not None and failure is None:
            atomic_json(evidence / "REAL_AGENT_TEST_RESULT.json", result)
        else:
            atomic_json(
                evidence / "REAL_AGENT_TEST_RESULT.json",
                {
                    "status": "FAIL",
                    "completed_utc": utc_now(),
                    "failure": f"{type(failure).__name__}: {failure}",
                },
            )
        redact_tree(evidence, secrets)
        shutil.rmtree(temp_root, ignore_errors=True)
        try:
            cgroup.rmdir()
        except OSError:
            pass
    if failure is not None:
        raise failure
    assert result is not None
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
