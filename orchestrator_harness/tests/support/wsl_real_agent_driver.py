"""Prepared Linux isolation for the Windows-owned public real-agent route.

This module never launches the public controller.  It prepares one disposable
Linux namespace/cgroup and waits for the Windows controller to launch the
provider bridge.  The bridge claims the prepared state with a one-use nonce;
the controller, Git repository, receipts, lifecycle, and all mutations remain
on Windows.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .allowlist_connect_proxy import AllowlistProxy
except ImportError:  # Direct support-script execution.
    from allowlist_connect_proxy import AllowlistProxy  # type: ignore[no-redef]


NOBODY = 65534
ALLOWED_OPENAI_ENDPOINTS = {("chatgpt.com", 443)}
PREPARED_STATE_SCHEMA = "orchestrator-wsl-prepared-state/v1"
PREPARED_CLAIM_SCHEMA = "orchestrator-wsl-prepared-claim/v1"
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
    completed = subprocess.run(
        argv,
        check=False,
        input=input_text,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(
            f"Linux command failed ({completed.returncode}): "
            f"{' '.join(argv)[-800:]}\n"
            f"stdout={completed.stdout[-1200:]!r}\n"
            f"stderr={completed.stderr[-1200:]!r}"
        )
    return completed


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def chown_tree(root: Path, uid: int = NOBODY, gid: int = NOBODY) -> None:
    if not root.is_dir():
        raise RuntimeError(f"workspace ownership root is unavailable: {root}")
    os.chown(root, uid, gid)
    for directory, directories, files in os.walk(root):
        for name in directories:
            os.chown(Path(directory) / name, uid, gid)
        for name in files:
            os.chown(Path(directory) / name, uid, gid)


def copy_harness_source(source_root: Path, destination: Path) -> Path:
    """Retained deterministic helper for local isolation tests."""

    source = source_root / "orchestrator_harness"

    def ignored(_directory: str, names: list[str]) -> set[str]:
        return {
            name
            for name in names
            if name in {".real-agent", "test-evidence", "__pycache__", ".pytest_cache"}
            or name.endswith(".pyc")
        }

    shutil.copytree(source, destination / "orchestrator_harness", ignore=ignored)
    common = source_root / "harness_common"
    if not common.is_dir():
        raise RuntimeError(f"required harness_common source is unavailable: {common}")
    shutil.copytree(common, destination / "harness_common", ignore=ignored)
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
    """Copy only the provider's required auth shape into disposable storage."""

    value = json.loads(source.read_text(encoding="utf-8"))
    tokens = value.get("tokens")
    if not isinstance(tokens, dict):
        raise RuntimeError("source Codex auth has no token object")
    access = tokens.get("access_token")
    account = tokens.get("account_id")
    id_token = tokens.get("id_token")
    refresh = tokens.get("refresh_token")
    if not all(isinstance(item, str) and item for item in (access, account, id_token)):
        raise RuntimeError("source Codex auth lacks token/account/ID fields")
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
            "Codex ID-format field is not expired; refusing to expose it"
        )
    minimized = {
        "auth_mode": value.get("auth_mode", "chatgpt"),
        "OPENAI_API_KEY": None,
        "tokens": {
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
    secrets = [
        item for item in (access, id_token, refresh) if isinstance(item, str) and item
    ]
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
        return (
            Path(f"/proc/{pid}/cmdline")
            .read_bytes()
            .replace(b"\0", b" ")
            .decode("utf-8", "replace")
            .strip()
        )
    except OSError:
        return ""


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
    raise RuntimeError(f"process {pid} is not descended from supervisor {stop_pid}")


def discover_codex(
    cgroup: Path,
    pinned_codex: Path,
    root_pid: int,
    timeout: float,
) -> tuple[int, list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout
    pinned_stat = pinned_codex.resolve(strict=True).stat()
    while time.monotonic() < deadline:
        for pid in sorted(cgroup_processes(cgroup)):
            executable = proc_exe(pid)
            if executable is None:
                continue
            try:
                candidate = os.stat(f"/proc/{pid}/exe")
            except OSError:
                continue
            if (
                candidate.st_dev == pinned_stat.st_dev
                and candidate.st_ino == pinned_stat.st_ino
            ):
                chain = ancestry(pid, root_pid)
                if len(chain) < 2:
                    raise RuntimeError("pinned Codex has no containment supervisor")
                return pid, chain
        time.sleep(0.05)
    raise TimeoutError("pinned Codex did not appear in the prepared cgroup")


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
  chain input {{ type filter hook input priority 0; policy drop;
    iifname \"lo\" accept
    ct state established,related accept
  }}
  chain output {{ type filter hook output priority 0; policy drop;
    oifname \"lo\" accept
    ip daddr {host_ip} tcp dport {proxy.port} accept
  }}
}}
"""
    try:
        run(["ip", "netns", "exec", namespace, "nft", "-f", "-"], input_text=rules)
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
    """Return the complete audited sandbox mount/environment manifest."""

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
    namespace: str, base: list[str], proxy_host: str, proxy_port: int
) -> dict[str, Any]:
    script = r"""
import json, os, pathlib, socket, subprocess
def connect(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.4): return True
    except OSError: return False
def proxy_status(target):
    with socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=2) as sock:
        sock.sendall(("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n").encode())
        return sock.recv(128).split(b"\r\n", 1)[0].decode("ascii", "replace")
caps = ""
for line in pathlib.Path("/proc/self/status").read_text().splitlines():
    if line.startswith("CapEff:"): caps = line.split()[1]
route = pathlib.Path("/proc/net/route").read_text()
def git(*args):
    completed = subprocess.run(
        ["git", "-C", "/workspace", *args], capture_output=True, text=True,
        timeout=30,
    )
    return completed
git_base = git("rev-parse", "HEAD")
marker = pathlib.Path("/workspace/.preflight-git-proof")
marker.write_text("preflight\n")
added = git("add", ".preflight-git-proof")
committed = git("commit", "-m", "preflight git proof")
if git_base.returncode == 0:
    resetted = git("reset", "--hard", git_base.stdout.strip())
else:
    resetted = git("reset", "--hard")
clean = git("status", "--porcelain")
head = git("rev-parse", "HEAD")
codex_probe = subprocess.run(
    [str(pathlib.Path("/opt/codex/bin") / "codex"), "--version"],
    capture_output=True, text=True, timeout=30,
)
value = {
 "uid": os.getuid(), "gid": os.getgid(), "cap_eff": caps,
 "mnt_c_present": pathlib.Path("/mnt/c").exists(), "usb_present": pathlib.Path("/dev/bus/usb").exists(),
 "repo_marker_present": pathlib.Path("/workspace/BYO-Firmware-MCP").exists(), "workspace_writable": os.access("/workspace", os.W_OK),
 "usr_writable": os.access("/usr", os.W_OK),
 "default_route_present": any(line.split()[1] == "00000000" for line in route.splitlines()[1:] if len(line.split()) > 1),
 "metadata_direct": connect("169.254.169.254", 80), "internet_direct": connect("1.1.1.1", 443),
 "host_other_port": connect(PROXY_HOST, 1), "denied_proxy_status": proxy_status("example.com:443"),
 "allowed_proxy_status": proxy_status("chatgpt.com:443"),
 "git_proof": {
     "base_present": git_base.returncode == 0,
     "committed": committed.returncode == 0,
     "reset_clean": resetted.returncode == 0 and clean.stdout.strip() == "",
     "head_restored": git_base.returncode == 0 and head.stdout.strip() == git_base.stdout.strip(),
 },
 "codex_probe": {
     "started": codex_probe.returncode == 0,
     "version": codex_probe.stdout.strip()[:80],
     "stderr_tail": codex_probe.stderr.strip()[-120:],
 },
}
print(json.dumps(value, sort_keys=True))
""".replace("PROXY_HOST", repr(proxy_host)).replace("PROXY_PORT", str(proxy_port))
    completed = run(
        ["ip", "netns", "exec", namespace, *base, "/usr/bin/python3", "-c", script],
        timeout=30,
    )
    result = json.loads(completed.stdout.splitlines()[-1])
    codex_probe = result.pop("codex_probe")
    if codex_probe.get("started") is not True or not codex_probe.get("version"):
        raise RuntimeError(
            f"pinned Codex did not start in the prepared sandbox: {codex_probe}"
        )
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
        "git_proof": {
            "base_present": True,
            "committed": True,
            "reset_clean": True,
            "head_restored": True,
        },
    }
    if result != expected:
        raise RuntimeError(f"isolation preflight mismatch: {result}")
    result["codex_probe"] = codex_probe
    return result


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
        if any(secret in path.read_bytes() for _text, secret in encoded):
            raise RuntimeError(f"credential remains in evidence file {path}")


def copy_evidence(
    temp_root: Path,
    evidence: Path,
    secrets: list[str],
    state_path: Path | None = None,
) -> None:
    """Copy only the explicitly redacted isolation summaries."""

    evidence.mkdir(parents=True, exist_ok=True)
    for name in (
        "isolation-preflight.json",
        "proxy-audit.jsonl",
        "cgroup-evidence.json",
        "prepared-state.json",
        "driver-failure.txt",
    ):
        source = temp_root / name
        if source.exists():
            shutil.copy2(source, evidence / name)
    # The prepared-state object is written to the Windows-visible state path
    # (READY, then CLEANED); retain that exact final object as evidence too.
    if state_path is not None and state_path.exists():
        shutil.copy2(state_path, evidence / "prepared-state.json")
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
        raise RuntimeError("prepared real-agent cgroup did not become empty")


def _valid_hex(value: str, length: int, label: str) -> None:
    if len(value) != length or any(ch not in "0123456789abcdef" for ch in value):
        raise RuntimeError(f"{label} must be lowercase hexadecimal of length {length}")


def validate_prepared_state(
    value: object, *, nonce: str, invocation_id: str
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError("prepared state is not an object")
    if value.get("schema") != PREPARED_STATE_SCHEMA or value.get("status") != "READY":
        raise RuntimeError("prepared state is not READY")
    if value.get("nonce") != nonce:
        raise RuntimeError("prepared state nonce mismatch")
    if value.get("invocation_id") != invocation_id:
        raise RuntimeError("prepared state invocation mismatch")
    if value.get("consumed") is not False:
        raise RuntimeError("prepared state was already consumed")
    for key in (
        "cgroup",
        "network_namespace",
        "proxy_url",
        "workspace",
        "codex_home",
        "release",
        "pinned_codex",
        "bwrap",
        "cgroup_launcher",
        "release_signal",
    ):
        if not isinstance(value.get(key), str) or not value[key]:
            raise RuntimeError(f"prepared state lacks {key}")
    if value["network_namespace"] != f"oh-{str(value['run_id'])[:8]}":
        raise RuntimeError("prepared state namespace does not bind to run identity")
    return value


def claim_prepared_state(
    claim_path: Path, *, nonce: str, invocation_id: str
) -> dict[str, Any]:
    """Claim the preparation exactly once with O_EXCL."""

    claim = {
        "schema": PREPARED_CLAIM_SCHEMA,
        "nonce": nonce,
        "invocation_id": invocation_id,
        "linux_bridge_pid": os.getpid(),
        "claimed_utc": utc_now(),
    }
    claim_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(claim_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise RuntimeError("prepared state was claimed more than once") from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(claim, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    return claim


def _prepare(args: argparse.Namespace) -> int:
    if os.geteuid() != 0:
        raise RuntimeError("Linux preparation requires root for cgroup/netns setup")
    _valid_hex(args.run_id, 32, "run-id")
    _valid_hex(args.nonce, 64, "nonce")
    if not args.invocation_id or any(ch.isspace() for ch in args.invocation_id):
        raise RuntimeError("invocation ID is invalid")
    codex_root = Path(args.codex_root).resolve(strict=True)
    if args.codex_root != "/opt/orchestrator-harness-codex":
        raise RuntimeError("Codex root is not the authorized isolated installation")
    codex_link = codex_root / "bin" / "codex"
    pinned_codex = codex_link.resolve(strict=True)
    release = pinned_codex.parent.parent
    # The pinned Codex release may ship its own bubblewrap resource; the
    # isolated Ubuntu image also provides a system bubblewrap.  Either is
    # admitted only as an exact regular file, and the chosen binary is
    # recorded in the prepared state so the provider bridge cannot substitute
    # a different sandbox tool.
    bwrap = release / "codex-resources" / "bwrap"
    bwrap_source = "pinned-release"
    if not bwrap.is_file():
        bwrap = Path("/usr/bin/bwrap")
        bwrap_source = "system-ubuntu"
    if not bwrap.is_file():
        raise RuntimeError(f"bubblewrap is unavailable: {bwrap}")
    workspace = Path(args.workspace).resolve(strict=True)
    if not workspace.is_dir() or not (workspace / ".git").is_dir():
        raise RuntimeError("prepared workspace must contain its own .git directory")
    state_path = Path(args.state).resolve(strict=False)
    release_signal = Path(args.release_signal).resolve(strict=False)
    evidence = Path(args.evidence).resolve(strict=False)
    temp_root = Path(f"/tmp/orchestrator-harness-real-agent-{args.run_id}")
    temp_root.mkdir(mode=0o700)
    # The sandbox uid must be able to traverse the whole disposable root to
    # reach the bound auth/codex-home; giving the root itself to the sandbox
    # owner keeps every later bind inside a nobody-owned subtree.
    os.chown(temp_root, NOBODY, NOBODY)
    cgroup = Path("/sys/fs/cgroup") / f"orchestrator-harness-{args.run_id}"
    network: dict[str, Any] | None = None
    secrets: list[str] = []
    failure: BaseException | None = None
    ready_state: dict[str, Any] | None = None
    try:
        codex_home = temp_root / "codex-home"
        secrets, access_expiry, id_expiry = minimal_auth(
            Path(args.auth_json).resolve(strict=True), codex_home / "auth.json"
        )
        chown_tree(workspace)
        os.chown(codex_home, NOBODY, NOBODY)
        os.chown(codex_home / "auth.json", NOBODY, NOBODY)
        cgroup.mkdir()
        (cgroup / "pids.max").write_text("64\n", encoding="ascii")
        (cgroup / "memory.max").write_text(
            str(1024 * 1024 * 1024) + "\n", encoding="ascii"
        )
        if (cgroup / "memory.swap.max").exists():
            (cgroup / "memory.swap.max").write_text("0\n", encoding="ascii")
        (cgroup / "cpu.max").write_text("200000 100000\n", encoding="ascii")
        network = make_network_namespace(args.run_id, temp_root / "proxy-audit.jsonl")
        proxy_url = f"http://{network['host_ip']}:{network['proxy_port']}"
        isolation = preflight(
            network["name"],
            bwrap_base(bwrap, release, workspace, codex_home, proxy_url),
            network["host_ip"],
            network["proxy_port"],
        )
        atomic_json(
            temp_root / "isolation-preflight.json",
            {
                **isolation,
                "network_namespace": network["name"],
                "allowed_endpoints": sorted(
                    f"{host}:{port}" for host, port in ALLOWED_OPENAI_ENDPOINTS
                ),
                "access_token_expiry_utc": access_expiry,
                "id_token_format": "expired-format-only",
                "captured_utc": utc_now(),
            },
        )
        pinned_stat = pinned_codex.stat()
        ready_state = {
            "schema": PREPARED_STATE_SCHEMA,
            "status": "READY",
            "consumed": False,
            "run_id": args.run_id,
            "nonce": args.nonce,
            "invocation_id": args.invocation_id,
            "workspace": str(workspace),
            "cgroup": str(cgroup),
            "network_namespace": network["name"],
            "proxy_host": network["host_ip"],
            "proxy_port": network["proxy_port"],
            "proxy_url": proxy_url,
            "codex_home": str(codex_home),
            "release": str(release),
            "pinned_codex": str(pinned_codex),
            "pinned_codex_device": pinned_stat.st_dev,
            "pinned_codex_inode": pinned_stat.st_ino,
            "bwrap": str(bwrap),
            "cgroup_launcher": str(Path(args.cgroup_launcher).resolve(strict=True)),
            "provider_entry": str(Path(args.provider_entry).resolve(strict=True)),
            "release_signal": str(release_signal),
            "bwrap_source": bwrap_source,
            "prepared_linux_pid": os.getpid(),
            "prepared_utc": utc_now(),
            "credentials_in_state": False,
            "preflight": isolation,
        }
        atomic_json(state_path, ready_state)
        # The Windows host decides when the provider attempt is over.  This
        # waiter owns only Linux preparation resources and never launches a
        # controller or provider.
        deadline = time.monotonic() + args.wait_seconds
        while not release_signal.exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        if not release_signal.exists():
            raise TimeoutError("Windows route did not release prepared Linux resources")
    except BaseException as exc:
        failure = exc
        atomic_json(
            state_path,
            {
                "schema": PREPARED_STATE_SCHEMA,
                "status": "FAILED",
                "run_id": args.run_id,
                "nonce": args.nonce,
                "invocation_id": args.invocation_id,
                "credentials_in_state": False,
                "error_type": type(exc).__name__,
            },
        )
        (temp_root / "driver-failure.txt").write_text(
            type(exc).__name__ + "\n", encoding="utf-8"
        )
    finally:
        try:
            kill_cgroup(cgroup)
        except BaseException as exc:
            if failure is None:
                failure = exc
        cgroup_evidence = {
            "limits": {
                name: (cgroup / name).read_text().strip()
                for name in ("pids.max", "memory.max", "memory.swap.max", "cpu.max")
                if (cgroup / name).exists()
            },
            "remaining_pids": sorted(cgroup_processes(cgroup)),
            "captured_utc": utc_now(),
        }
        atomic_json(temp_root / "cgroup-evidence.json", cgroup_evidence)
        if network is not None:
            network["proxy"].close()
            run(["ip", "netns", "del", network["name"]], check=False)
        atomic_json(
            state_path,
            {
                **(ready_state or {}),
                "status": "CLEANED",
                "consumed": True,
                "cleanup_complete": not cgroup_evidence["remaining_pids"],
                "cleaned_utc": utc_now(),
                "credentials_in_state": False,
                "preparation_error_type": type(failure).__name__
                if failure is not None
                else None,
            },
        )
        copy_evidence(temp_root, evidence, secrets, state_path=state_path)
        shutil.rmtree(temp_root, ignore_errors=True)
        try:
            cgroup.rmdir()
        except OSError:
            pass
    if failure is not None:
        raise failure
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one Linux provider-isolation side"
    )
    parser.add_argument("--mode", choices=("prepare",), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--invocation-id", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--release-signal", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--auth-json", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--codex-root", required=True)
    parser.add_argument("--cgroup-launcher", required=True)
    parser.add_argument("--provider-entry", required=True)
    parser.add_argument("--wait-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    return _prepare(args)


if __name__ == "__main__":
    raise SystemExit(main())
