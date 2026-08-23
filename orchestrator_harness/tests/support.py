from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from orchestrator_harness.config import HarnessConfig, load_config
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot, iso_utc
from orchestrator_harness.workspace_overlay import ingest_super_cache, prepare_worktree

NOW = datetime(2026, 7, 30, 12, 0, 0, tzinfo=timezone.utc)


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare_fixture_overlay_receipt(
    root: Path,
    target_worktree: Path,
    *,
    role: str = "subagent",
    receipt_path: Path | None = None,
) -> Path:
    """Prepare a real, nonempty super-cache overlay for a provider fixture.

    Fixture callers opt in at the provider-launch boundary; parser-only and
    explicit no-cache tests remain receipt-free by construction.
    """

    token = hashlib.sha256(str(target_worktree.resolve()).encode()).hexdigest()[:12]
    source = root / f".fixture-overlay-source-{token}"
    harness = root / f".fixture-overlay-harness-{token}"
    source.mkdir(parents=True, exist_ok=True)
    harness.mkdir(parents=True, exist_ok=True)
    (source / "fixture-overlay" / "receipt-proof.txt").parent.mkdir(
        parents=True, exist_ok=True
    )
    (source / "fixture-overlay" / "receipt-proof.txt").write_text(
        "nonempty fixture overlay\n", encoding="utf-8"
    )
    ingest_super_cache(source_folder=source, harness_worktree=harness)
    receipt = receipt_path or (
        target_worktree / ".agent-workspace" / "fixture-overlay.receipt.json"
    )
    if receipt.is_file():
        try:
            existing = json.loads(receipt.read_text(encoding="utf-8"))
            created = existing.get("created_paths", [])
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return receipt
        if isinstance(created, list) and all(
            isinstance(relative, str) and (target_worktree / relative).exists()
            for relative in created
        ):
            return receipt
    prepare_worktree(
        super_cache=harness / "super-cache",
        target_worktree=target_worktree,
        role=role,
        receipt_path=receipt,
    )
    return receipt


@dataclass
class SuiteFixture:
    temporary: TemporaryDirectory[str]
    root: Path
    harness_root: Path
    suite_root: Path
    config_path: Path
    config: HarnessConfig

    @classmethod
    def create(cls) -> "SuiteFixture":
        temporary = TemporaryDirectory()
        root = Path(temporary.name)
        harness = root / "harness"
        suite = root / "suite"
        harness.mkdir()
        suite.mkdir()
        config_path = harness / "config.json"
        write_json(
            config_path,
            {
                "suite_root": str(suite),
                "run_globs": ["runs/*"],
                "workspace_relpath": ".agent-workspace",
                "output_dir": ".state",
                "poll_interval_seconds": 0.05,
                "watch_timeout_seconds": 0.1,
                "request_warning_seconds": 120,
                "request_critical_seconds": 30,
                "process_start_tolerance_seconds": 2,
                "stable_read_delay_seconds": 0,
            },
        )
        config = load_config(config_path, harness_root=harness)
        return cls(temporary, root, harness, suite, config_path, config)

    def close(self) -> None:
        self.temporary.cleanup()

    def workspace(self, run: str = "A00_test") -> Path:
        workspace = self.suite_root / "runs" / run / ".agent-workspace"
        workspace.mkdir(parents=True, exist_ok=True)
        return workspace

    def status(
        self,
        *,
        run: str = "A00_test",
        label: str = "atlas_boundary_001",
        doer: str = "Atlas",
        task: str = "A00",
        state: str = "running",
        controller_pid: int = 101,
        codex_pid: int = 102,
        board_tokens: list[str] | None = None,
        mcp_servers: list[str] | None = None,
        declared_lane_id: str | None = None,
        started: datetime = NOW,
    ) -> Path:
        path = self.workspace(run) / f"{label}_controller.status.json"
        value = {
            "state": state,
            "controller_pid": controller_pid,
            "codex_pid": codex_pid,
            "doer": doer,
            "task": task,
            "phase": "synthetic",
            "thread_id": f"thread-{doer.lower()}",
            "started_utc": iso_utc(started),
            "controller_started_utc": iso_utc(started),
            "codex_started_utc": iso_utc(started),
            "board_tokens": board_tokens or [],
            "mcp_servers": mcp_servers or [],
        }
        if declared_lane_id is not None:
            value["declared_lane_id"] = declared_lane_id
        write_json(
            path,
            value,
        )
        return path

    def jsonl(
        self,
        *,
        run: str = "A00_test",
        label: str = "atlas_boundary_001",
        terminal: str | None = None,
    ) -> Path:
        path = self.workspace(run) / f"{label}_codex.jsonl"
        lines = [{"type": "thread.started"}, {"type": "turn.started"}]
        if terminal:
            lines.append({"type": terminal})
        path.write_text(
            "".join(json.dumps(item) + "\n" for item in lines), encoding="utf-8"
        )
        return path

    def process_snapshot(
        self,
        *,
        controller_pid: int = 101,
        codex_pid: int = 102,
        complete: bool = True,
        created: datetime = NOW,
        codex_parent: int | None = None,
        missing_codex: bool = False,
        missing_created: bool = False,
    ) -> ProcessSnapshot:
        timestamp = None if missing_created else created
        processes = [
            ProcessInfo(controller_pid, 1, "python.exe", "controller", timestamp)
        ]
        if not missing_codex:
            processes.append(
                ProcessInfo(
                    codex_pid,
                    controller_pid if codex_parent is None else codex_parent,
                    "codex.exe",
                    "codex exec",
                    timestamp,
                )
            )
        return ProcessSnapshot(
            complete,
            tuple(processes),
            () if complete else ("synthetic incomplete inventory",),
            "fake",
        )


@dataclass
class TemporaryGitRepository:
    """Small argv-only Git fixture for coding-lane tests."""

    root: Path
    branch: str

    @classmethod
    def create(cls, root: Path, *, branch: str = "coding") -> "TemporaryGitRepository":
        root.mkdir(parents=True, exist_ok=True)
        fixture = cls(root, branch)
        fixture.git("init", "--initial-branch", branch)
        fixture.git("config", "user.email", "tests@example.invalid")
        fixture.git("config", "user.name", "Harness Tests")
        (root / "tracked.txt").write_text("initial\n", encoding="utf-8")
        fixture.git("add", "tracked.txt")
        fixture.git("commit", "-m", "initial")
        return fixture

    def git(self, *args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(self.root), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            shell=False,
            text=True,
        )
        return completed.stdout.strip()

    @property
    def common_dir(self) -> Path:
        value = self.git("rev-parse", "--git-common-dir")
        path = Path(value)
        return (
            (self.root / path).resolve() if not path.is_absolute() else path.resolve()
        )

    @property
    def head(self) -> str:
        return self.git("rev-parse", "HEAD")

    def declaration(self) -> dict[str, str]:
        return {
            "common_dir": str(self.common_dir),
            "worktree_root": str(self.root.resolve()),
            "branch": self.branch,
            "base_commit": self.head,
        }

    def linked_worktree(self, path: Path, branch: str) -> "TemporaryGitRepository":
        self.git("worktree", "add", "-b", branch, str(path), self.head)
        return TemporaryGitRepository(path, branch)


SYNTHETIC_CONTROLLER = r"""
from __future__ import annotations
import json, os, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

workspace=Path(sys.argv[1])
workspace.mkdir(parents=True,exist_ok=True)
requests=workspace/"permission-requests"
requests.mkdir(exist_ok=True)
child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(120)"])
started=datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
status=workspace/"synthetic_controller.status.json"
status.write_text(json.dumps({
  "state":"running","controller_pid":os.getpid(),"codex_pid":child.pid,
  "doer":"Synthetic","task":"T00","phase":"relay wait","thread_id":"synthetic-session",
  "started_utc":started,"controller_started_utc":started,"codex_started_utc":started,
  "board_tokens":[],"mcp_servers":[]
},indent=2)+"\n",encoding="utf-8")
(workspace/"synthetic_codex.jsonl").write_text(
  json.dumps({"type":"thread.started","thread_id":"synthetic-session"})+"\n"+
  json.dumps({"type":"turn.started"})+"\n",encoding="utf-8")
request=requests/"synthetic-request.json"
request.write_text(json.dumps({
  "schema":"synthetic-request/v1","request_id":"synthetic-request",
  "created_utc":started,"run":{"session_id":"synthetic-session"},
  "live_lifetime":{"run_id":"synthetic-run","process":{"pid":os.getpid(),"started_utc":started}},
  "relay_path":".agent-workspace/permission-requests/synthetic-request.relay.json",
  "zero_action_before_relay":True
},indent=2)+"\n",encoding="utf-8")
relay=requests/"synthetic-request.relay.json"
deadline=time.time()+60
while time.time()<deadline and not relay.exists():
  time.sleep(.05)
if not relay.exists():
  child.terminate(); child.wait(timeout=5); raise SystemExit(4)
(workspace/"PARALLEL_CHECKPOINT.md").write_text("# Synthetic checkpoint\n\nRelay observed.\n",encoding="utf-8")
with (workspace/"synthetic_codex.jsonl").open("a",encoding="utf-8") as f:
  f.write(json.dumps({"type":"turn.completed","usage":{}})+"\n")
child.terminate()
child.wait(timeout=5)
status.write_text(json.dumps({
  "state":"exited","controller_pid":os.getpid(),"codex_pid":child.pid,
  "doer":"Synthetic","task":"T00","phase":"complete","thread_id":"synthetic-session",
  "started_utc":started,"controller_started_utc":started,"codex_started_utc":started,
  "ended_utc":datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
  "exit_code":0,"board_tokens":[],"mcp_servers":[]
},indent=2)+"\n",encoding="utf-8")
"""


def launch_synthetic_controller(workspace: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", SYNTHETIC_CONTROLLER, str(workspace)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
