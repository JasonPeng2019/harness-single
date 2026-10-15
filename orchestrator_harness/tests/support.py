from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Write one deterministic fixture record for product-owned tests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class PreparedRootFixture:
    """A disposable split ROOT with an actual initialized, owned monitor.

    Logical/native-session strings in unit tests exercise record contracts;
    they are not evidence of a native model invocation or sandbox enforcement.
    Every monitor is a real child, retained and reaped through its original
    handle before the disposable directory is released.
    """

    def __init__(self, case, *, profile: str = "managed", provider_id: str = "codex",
                 same_workspace: bool = False) -> None:
        from unittest.mock import patch
        from orchestrator_harness import processes, setup
        from orchestrator_harness.config import compute_config_identity, load_config, load_resource_manifest
        from orchestrator_harness.epochs import open_epoch
        from orchestrator_harness.root_session import publish_root_session

        self.case = case
        self.temporary = tempfile.TemporaryDirectory(prefix="prepared-root-")
        case.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name)
        self.harness = self.home / "harness"
        self.product = self.home / "product"
        self.manager = self.product if same_workspace else self.home / "manager"
        for path in {self.harness, self.product, self.manager}:
            path.mkdir()
        write_json(self.harness / "harness-config.json", {
            "root_workspace": str(self.product), "manager_workspace": str(self.manager),
            "managed_coordination": "enabled" if profile == "managed" else "disabled",
        })
        write_json(self.harness / "resource-manifest.json", {
            "schema": "resource-manifest/v1", "resources": [],
        })
        self.config = load_config(self.harness)
        self.rt = self.config.runtime_root
        self.identity = compute_config_identity(self.config, load_resource_manifest(self.harness))
        self.children = []
        case.addCleanup(self.close)
        setup.set_runtime_state(self.rt, "OPEN")
        real_spawn = processes.spawn_detached

        def spawn(*args, **kwargs):
            child = real_spawn(*args, **kwargs)
            self.children.append(child)
            return child

        self.environment = dict(os.environ)
        self.environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
        for key in ("HARNESS_ROOT_SESSION_ID", "HARNESS_SESSION_ID", "HARNESS_BINDING_ID",
                    "CODEX_THREAD_ID", "CLAUDE_SESSION_ID", "QWEN_SESSION_ID"):
            self.environment.pop(key, None)
        with patch.dict(os.environ, self.environment, clear=True), patch.object(processes, "spawn_detached", side_effect=spawn):
            startup = setup._start_monitor(self.harness, self.rt, self.config, self.identity)
        self.monitor = setup._wait_monitor_ready(self.rt, self.identity, startup)
        self.epoch = open_epoch(self.rt, self.config, load_resource_manifest(self.harness))
        self.session = publish_root_session(self.config, run_id="unit-prepared-run", provider_id=provider_id)

    def close(self) -> None:
        from orchestrator_harness import processes, setup

        setup.set_runtime_state(self.rt, "SHUTTING_DOWN")
        for child in self.children:
            if child.poll() is None:
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.case.assertTrue(processes.reap_created_process(child))
            self.case.assertIsNotNone(child.poll(), "owned monitor must be reaped before fixture removal")
        setup.set_runtime_state(self.rt, "CLOSED")

    def command(self, *argv: str, environment: dict[str, str] | None = None):
        return subprocess.run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch",
                               "--json", *argv], cwd=self.harness,
                              env={**self.environment, **(environment or {})},
                              capture_output=True, text=True, encoding="utf-8", timeout=20)


FIXTURE_TASK_CARD = {
    "schema": "project-task-card/v1", "task": "Exercise the native lane contract",
    "acceptance_criteria": ["Preserve exact assignment and cleanup authority"],
    "deliverables": ["Deterministic fixture evidence"],
    "reason_for_acceptance_and_deliverables": "Test lifecycle behavior with genuine protected inputs",
}


def prepare_lane_fixture(rt: Path, epoch_id: str, lane: dict[str, Any], *,
                         task_card: dict[str, Any] | None = None) -> dict[str, Any]:
    """Stage a real linked Git assignment before a test injects its failure.

    This is explicit fixture setup, never an automatic fallback after corrupting
    authority. It does not synthesize sandbox enforcement or provider cleanup.
    The caller owns the disposable runtime and its parent ROOT repository.
    """
    from orchestrator_harness import bootstrap, git_source, lane_authority
    from orchestrator_harness.lanes import write_lane

    root, worktree = rt.absolute().parent, Path(lane["worktree_path"]).absolute()
    if root == worktree or not worktree.is_relative_to(root):
        raise ValueError("fixture needs a separate owned ROOT and linked worktree")
    root.mkdir(parents=True, exist_ok=True)

    def git(*argv: str) -> None:
        result = subprocess.run(["git", "-C", str(root), "-c", "core.hooksPath=nul", *argv],
                                capture_output=True, env=git_source.environment(), timeout=20)
        if result.returncode:
            raise AssertionError(result.stderr.decode("utf-8", "replace"))

    if not (root / ".git").exists():
        git("init", "-q")
        (root / ".gitignore").write_text(
            ".agent-workspace/\n.codex/\nRESULT.json\n" + rt.name + "/\n"
            + worktree.relative_to(root).as_posix() + "/\n", encoding="utf-8")
        (root / ".fixture-source").write_text("native fixture source\n", encoding="utf-8")
        git("add", ".gitignore", ".fixture-source")
        git("-c", "user.name=Fixture", "-c", "user.email=fixture@local", "commit", "-qm", "fixture base")
    branch = lane.get("branch") or f"lane/{epoch_id}/{lane['lane_id']}/work"
    worktree.mkdir(parents=True, exist_ok=True)
    if not (worktree / ".git").exists():
        # Keep pre-existing transcript/prompt inputs while making the directory
        # an actual linked worktree. Both sides of every move are fixture-owned.
        with tempfile.TemporaryDirectory(prefix="lane-inputs-", dir=root) as raw:
            staging = Path(raw).absolute()
            assert staging.is_relative_to(root)
            for entry in worktree.iterdir():
                destination = staging / entry.name
                assert entry.absolute().is_relative_to(root) and destination.is_relative_to(root)
                entry.rename(destination)
            try:
                git("worktree", "add", "-qb", branch, str(worktree))
            finally:
                for entry in staging.iterdir():
                    destination = worktree / entry.name
                    assert destination.is_relative_to(root)
                    entry.rename(destination)

    record = dict(lane)
    record.setdefault("lifecycle", "running")
    record.setdefault("process", {})
    record.setdefault("exclusive_resources", [])
    record.setdefault("result_path", str(worktree / "RESULT.json"))
    record["branch"] = branch
    provider = dict(record["provider"])
    provider.setdefault("model", "fixture")
    provider.setdefault("launch_config", {})
    record["provider"] = provider
    if provider["id"] == "codex" and not (worktree / ".codex/config.toml").exists():
        (worktree / ".codex").mkdir(exist_ok=True)
        (worktree / ".codex/config.toml").write_text('sandbox_mode = "workspace-write"\n', encoding="utf-8")
    card = dict(task_card or FIXTURE_TASK_CARD)
    bootstrap._write_worker_prompt(worktree, card, managed=False)
    bootstrap._write_invocation(worktree, lane_id=record["lane_id"], run_id=record["run_id"],
                                provider_id=provider["id"], model=provider["model"],
                                launch_config=provider["launch_config"],
                                exclusive_resources=record["exclusive_resources"])
    bootstrap._write_worker_binding(worktree, rt, record["lane_id"], record["run_id"])
    prepared = lane_authority.stage_authority(rt, epoch_id, record, card)
    for field in lane_authority.CONTROL_FILES:
        if field in ("prompt_path", "task_card_path", "invocation_path"):
            continue
        previous = lane.get(field)
        if previous and Path(previous).is_relative_to(worktree) and Path(previous).is_file():
            Path(prepared[field]).write_bytes(Path(previous).read_bytes())
    marker_path = rt / "CURRENT_EPOCH.json"
    marker = json.loads(marker_path.read_text(encoding="utf-8")) if marker_path.exists() else {}
    write_json(marker_path, {**marker, "schema": "current-epoch/v1", "epoch_id": epoch_id})
    write_json(rt / "RUNTIME_STATE.json", {"schema": "runtime-state/v1", "state": "OPEN"})
    return write_lane(rt, epoch_id, record["lane_id"], prepared)


def exited_lease_holder_fixture(rt: Path, resources: list[str], *, lane_id: str, run_id: str) -> None:
    """Acquire with a genuine contained holder, then prove that exact holder gone."""
    from orchestrator_harness import processes
    from orchestrator_harness.leases import acquire_leases

    rt.mkdir(parents=True, exist_ok=True)
    with open(os.devnull, "rb") as stdin, open(os.devnull, "wb") as stdout, open(os.devnull, "wb") as stderr:
        child = processes.spawn_provider([sys.executable, "-B", "-c", "pass"], cwd=rt,
                                         stdin=stdin, stdout=stdout, stderr=stderr)
        boundary = None
        try:
            job = child.take_job_handle() if os.name == "nt" else None
            boundary = processes.ProcessBoundary.for_process(child.pid, windows_job_handle=job)
            if boundary.root_creation_time is None or not boundary.observe():
                raise AssertionError("cannot establish owned lease fixture boundary")
            acquire_leases(rt, resources, lane_id=lane_id, run_id=run_id, pid=child.pid,
                           creation_time=boundary.root_creation_time)
            if os.name == "nt":
                child.resume()
            if child.wait(timeout=10) != 0:
                raise AssertionError("lease fixture child failed")
        finally:
            try:
                if boundary is not None and not boundary.cleanup(force=True, timeout_seconds=10):
                    raise AssertionError("lease fixture exact boundary cleanup is unproven")
            finally:
                child.wait(timeout=10)
                if os.name == "nt":
                    child.close()


def seed_provider_history(rt: Path, lane: dict[str, Any], *, completed: int = 0,
                          epoch_id: str | None = None) -> None:
    """Trusted native launch fixture, distinct from the worker's usage mirror."""
    from orchestrator_harness import retry_policy
    session = lane.get("session") or {}
    lane["session"] = {}
    retry_policy.initialize_history(rt, lane)
    run = lane["run_id"]
    for index in range(completed):
        lane["run_id"] = f"fixture-prior-{index}"
        token = retry_policy.reserve_invocation(rt, lane)
        native = session.get("session_id") or "fixture-native"
        retry_policy.record_started(rt, lane, token)
        retry_policy.record_outcome(rt, lane, token, provider_started=True,
                                   cleanup_proven=True, session_id=native)
        lane["session"] = {"session_id": native}
    lane["run_id"] = run
    lane["session"] = session or lane["session"]
    if epoch_id is not None:
        lane["lifecycle"] = "running"
        lane.setdefault("process", {})
        write_json(rt / "epochs" / epoch_id / "lanes" / lane["lane_id"] / "lane.json",
                   {"schema": "lane/v1", **lane})
        marker_path = rt / "CURRENT_EPOCH.json"
        marker = json.loads(marker_path.read_text(encoding="utf-8")) if marker_path.exists() else {}
        write_json(marker_path, {**marker, "schema": "current-epoch/v1", "epoch_id": epoch_id})
        write_json(rt / "RUNTIME_STATE.json", {"schema": "runtime-state/v1", "state": "OPEN"})


def seed_acceptance_chain(rt: Path, epoch_id: str, lane: dict[str, Any]) -> dict[str, Any]:
    """A real authoritative pair for tests of accepted retirement."""
    from orchestrator_harness.core import content_hash
    from orchestrator_harness.epochs import lane_record_dir
    from orchestrator_harness.lanes import write_lane
    write_lane(rt, epoch_id, lane["lane_id"], lane)
    linked = {"lane_id": lane["lane_id"], "run_id": lane["run_id"], "task_card_id": "fixture-card",
              "task_card_hash": "fixture-card-hash", "result_id": "fixture-result",
              "result_hash": "fixture-result-hash", "commit": "fixture-commit"}
    review = {"schema": "completion-review/v1", **linked, "review_outcome": "PASS"}
    review["content_hash"] = content_hash(review)
    acceptance = {"schema": "orchestrator-acceptance/v1", **linked, "approval": "ACCEPTED",
                  "accepted_by": "ROOT", "review_ref": review["content_hash"]}
    acceptance["content_hash"] = content_hash(acceptance)
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    write_json(folder / "COMPLETION_REVIEW.json", review)
    write_json(folder / "ORCHESTRATOR_ACCEPTANCE.json", acceptance)
    return acceptance
