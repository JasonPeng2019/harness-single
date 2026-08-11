from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness.codex_adapter import (
    CodexAdapter,
    SyntheticCodexTransport,
    activate_codex_binding,
    check_codex_adapter,
    create_codex_adapter,
    install_codex_adapter,
    select_host_adapter,
    synthetic_wake_self_test,
    uninstall_codex_adapter,
)
from orchestrator_harness.config import ConfigError, load_config
from orchestrator_harness.host_adapters import (
    DELIVERY_NOTICE_SCHEMA,
    DeliveryBindingError,
    DeliveryCoordinator,
    FutureHostFixture,
    UnsupportedHostAdapterError,
)
from orchestrator_harness.lane_lifecycle import (
    ImmutableViewError,
    allocate_immutable_source_view,
    retire_terminal_lane,
    validate_lane_archive,
    write_lifecycle_registry_record,
)
from orchestrator_harness.models import ProcessSnapshot
from orchestrator_harness.notifications import ManagerEventRouter


class S4ContractTests(unittest.TestCase):
    def _router(self, root: Path, *, session: str = "session-s4") -> ManagerEventRouter:
        return ManagerEventRouter(
            root,
            run_id="run-s4",
            queue_id="queue-s4",
            manager_session_id=session,
            manager_thread_id="thread-s4",
            registration_id="registration-s4",
        )

    def _adapter(self, root: Path, *, session: str = "session-s4") -> tuple[CodexAdapter, DeliveryCoordinator, SyntheticCodexTransport]:
        router = self._router(root, session=session)
        transport = SyntheticCodexTransport()
        adapter = create_codex_adapter(router, transport=transport)
        return adapter, adapter.coordinator, transport

    @staticmethod
    def _event(event_id: str, *, priority: int = 2, severity: str = "warning") -> dict[str, object]:
        return {
            "event_id": event_id,
            "type": "MANAGER_SIGNAL",
            "identity": f"synthetic:{event_id}",
            "data": {
                "signal_id": event_id,
                "lane_id": "synthetic:s4",
                "manager_actionable": True,
                "severity": severity,
            },
            "priority": priority,
        }

    @staticmethod
    def _publish_lifecycle_record(
        root: Path,
        lane: Path,
        *,
        lane_id: str,
        revision: str,
        retained_ref: str = "refs/heads/main",
        target_revision: str | None = None,
    ) -> Path:
        runtime = root / "runtime"
        runtime.mkdir(exist_ok=True)
        workspace = lane / ".agent-workspace"
        workspace.mkdir(exist_ok=True)
        invocation = root / f"{lane_id}-invocation.json"
        invocation.write_text(json.dumps({"schema": "orchestrator-coding-invocation/v1", "lane_id": lane_id}) + "\n", encoding="utf-8")
        common = Path(subprocess.run(["git", "-C", str(lane), "rev-parse", "--git-common-dir"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip())
        if not common.is_absolute():
            common = (lane / common).resolve()
        branch = subprocess.run(["git", "-C", str(lane), "symbolic-ref", "--short", "HEAD"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip()
        generation = f"generation-{lane_id}"
        status = workspace / "status.json"
        status.write_text(json.dumps({
            "schema": "orchestrator-lane-controller/v1", "state": "PROVIDER_EXITED",
            "lane_id": lane_id, "worker_invocation_id": f"worker-{lane_id}",
            "invocation_schema": "orchestrator-coding-invocation/v1",
            "lifecycle_registry_generation": generation,
            "worktree_root": str(lane.resolve()), "repository_common_dir": str(common.resolve()),
            "branch": branch,
        }) + "\n", encoding="utf-8")
        write_lifecycle_registry_record(
            runtime,
            lane_id=lane_id,
            run_root=lane,
            invocation_path=invocation,
            status_path=status,
            invocation_schema="orchestrator-coding-invocation/v1",
            worker_invocation_id=f"worker-{lane_id}",
            generation=generation,
            state="PROVIDER_EXITED",
            repository={
                "worktree_root": str(lane.resolve()), "common_dir": str(common.resolve()),
                "branch": branch, "expected_head": revision,
                "retained_ref": retained_ref, "target_revision": target_revision or revision,
            },
            controller={"pid": 9201, "created_utc": "2000-01-01T00:00:00Z"},
            worker={"pid": 9202, "created_utc": "2000-01-01T00:00:00Z"},
            helpers=[], retained_ref=retained_ref, target_revision=target_revision or revision,
        )
        return runtime

    def test_S4_CODEX_INSTALL_WAKE_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            (project / ".codex").mkdir()
            unrelated = project / ".codex" / "settings.json"
            unrelated.write_bytes(b'{"unrelated":true}\n')
            install = install_codex_adapter(project)
            self.assertTrue(install["current"])
            self.assertEqual("unverified", install["project_trust"])
            router = self._router(root / "manager", session="installed-session")
            activate_codex_binding(project, router)
            router.admit(self._event("installed-hook"))
            hook = project / ".codex" / "hooks" / "orchestrator_harness_post_tool_use.py"
            env = os.environ.copy()
            repo_root = str(Path(__file__).resolve().parents[2])
            env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
            completed = subprocess.run(
                [sys.executable, str(hook)],
                cwd=project,
                input='{"provider_payload":"ignored"}\n',
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            evidence = json.loads(completed.stdout)
            self.assertEqual("orchestrator-codex-installed-hook/v1", evidence["schema"])
            self.assertEqual(DELIVERY_NOTICE_SCHEMA, evidence["notice"]["schema"])
            self.assertNotIn("event_id", evidence["notice"])
            self.assertEqual("DELIVERED", evidence["receipt"]["outcome"])
            self.assertFalse(evidence["acknowledged_by_hook"])
            self.assertEqual(1, evidence["pending_count"])
            self.assertEqual("PostToolUse", evidence["transport_calls"][0]["method"])
            self.assertEqual(b'{"unrelated":true}\n', unrelated.read_bytes())
            self.assertEqual("owned", check_codex_adapter(project)["ownership"])

    def test_S4_CODEX_INSTALL_LIFECYCLE_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            project = Path(raw) / "project"
            project.mkdir()
            prior_hooks = project / ".codex"
            prior_hooks.mkdir()
            hooks = prior_hooks / "hooks.json"
            original = b'{"hooks":{"UserHook":[{"id":"user"}]},"other":7}\n'
            hooks.write_bytes(original)
            first = install_codex_adapter(project)
            self.assertTrue(first["current"])
            self.assertTrue(install_codex_adapter(project)["idempotent"])
            hooks.write_bytes(hooks.read_bytes() + b"\n")
            upgraded = install_codex_adapter(project, upgrade=True)
            self.assertTrue(upgraded["current"])
            result = uninstall_codex_adapter(project)
            self.assertIn(b"UserHook", hooks.read_bytes())
            self.assertNotIn(".codex/hooks.json", result["preserved_modified"])

    def test_S4_NONPREEMPTIVE_DELIVERY_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter, coordinator, transport = self._adapter(Path(raw) / "manager")
            coordinator.register()
            coordinator.begin_bounded_task("long-tool")
            coordinator.router.admit(self._event("during-tool"))
            notice = coordinator.notice_for_wake()
            deferred = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
            self.assertIsNotNone(deferred)
            self.assertEqual("DELIVERY_DEFERRED", deferred.outcome)
            self.assertEqual([], transport.calls)
            delivered = adapter.post_tool_use(task_label="long-tool")
            self.assertIsNotNone(delivered)
            self.assertEqual("DELIVERED", delivered.outcome)
            self.assertEqual("task-completed:long-tool", coordinator.markers[-1])
            self.assertEqual("PostToolUse", transport.calls[0]["method"])

    def test_S4_CODEX_IDLE_FINALIZE_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manager = Path(raw) / "manager"
            adapter, coordinator, transport = self._adapter(manager)
            coordinator.register()
            coordinator.router.admit(self._event("idle"))
            receipt = adapter.turn_completed()
            self.assertEqual("DELIVERED", receipt.outcome)
            self.assertEqual(
                ["turn/completed", "thread/inject_items", "turn/start"],
                [call["method"] for call in transport.calls],
            )
            restored_adapter, restored, _ = self._adapter(manager)
            restored.register()
            self.assertEqual("run-s4", restored.binding_identity["run_id"])
            restored.acknowledge_event("idle")
            self.assertTrue(restored.close_binding())
            self.assertEqual("RELEASED", restored.load_state()["status"])
            del restored_adapter

    def test_S4_BINDING_REPLAY_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manager = Path(raw) / "manager"
            adapter, coordinator, _ = self._adapter(manager)
            coordinator.register()
            coordinator.router.admit(self._event("replay"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            wrong = dict(notice.as_record())
            wrong["manager_thread_id"] = "other-thread"
            with self.assertRaises(DeliveryBindingError):
                coordinator.notice_for_wake(wrong)
            receipt = coordinator.deliver_at_boundary(notice)
            self.assertEqual("DELIVERED", receipt.outcome)
            self.assertEqual(["replay"], [item["event_id"] for item in coordinator.pending_events()])
            restored = create_codex_adapter(self._router(manager), transport=SyntheticCodexTransport()).coordinator
            restored.register()
            replayed = restored.notice_for_wake()
            self.assertEqual(notice.notice_id, replayed.notice_id)
            with self.assertRaises(ValueError):
                restored.acknowledge_event(receipt)  # type: ignore[arg-type]
            del adapter

    def test_S4_SPARSE_NOTICE_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            _, coordinator, _ = self._adapter(Path(raw) / "manager")
            coordinator.register()
            self.assertIsNone(coordinator.router.admit({"event_id": "observed", "type": "RAW_OUTPUT", "identity": "raw", "data": {}}))
            coordinator.router.admit(self._event("routine", priority=5, severity="info"))
            notice = coordinator.notice_for_wake()
            coordinator.router.admit(self._event("urgent", priority=0, severity="critical"))
            coalesced = coordinator.notice_for_wake()
            self.assertEqual(notice.notice_id, coalesced.notice_id)
            self.assertEqual(2, coalesced.pending_count)
            self.assertEqual("critical", coalesced.highest_severity)
            self.assertNotIn("event_id", coalesced.as_record())

    def test_S4_HOST_EXTENSION_CONTRACT_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            router = self._router(Path(raw) / "manager")
            future = select_host_adapter("future-host", router)
            self.assertIsInstance(future, FutureHostFixture)
            self.assertFalse(future.profile.implemented)
            self.assertFalse(any(future.profile.capabilities.as_record().values()))
            with self.assertRaises(UnsupportedHostAdapterError):
                future.deliver_notice(None, boundary="idle")  # type: ignore[arg-type]
            current = select_host_adapter("codex", router)
            self.assertTrue(current.profile.implemented)
            self.assertEqual("codex", current.profile.kind)

    def test_S4_WATCHER_SIMPLIFICATION_001(self) -> None:
        from orchestrator_harness.cli import build_parser
        from orchestrator_harness.watcher_integration import watcher_recovery_projection

        with self.assertRaises(SystemExit):
            build_parser().parse_args(["watch", "--managed"])
        recovery = watcher_recovery_projection([
            {"alert_id": "a", "state": "open"},
            {"alert_id": "a", "state": "acknowledged"},
            {"alert_id": "b", "state": "resolved"},
        ])
        self.assertEqual(["a"], recovery["actionable"])

    def test_S4_CONFIG_MIGRATION_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "config.json"
            path.write_text(json.dumps({"request_warning_seconds": 1.5}), encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_config(path, harness_root=Path(raw))
            path.write_text(json.dumps({"manager_heartbeat_timeout_seconds": 420}), encoding="utf-8")
            config = load_config(path, harness_root=Path(raw))
            self.assertTrue(config.migration_diagnostics)

    def test_S4_IMMUTABLE_VIEW_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.mkdir()
            def git(*args: str) -> str:
                return subprocess.run(["git", "-C", str(source), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip()
            git("init", "--initial-branch", "main")
            git("config", "user.email", "s4@example.invalid")
            git("config", "user.name", "S4")
            (source / "source.txt").write_text("frozen\n", encoding="utf-8")
            git("add", ".")
            git("commit", "-m", "source")
            revision = git("rev-parse", "HEAD")
            view = allocate_immutable_source_view(
                source,
                revision=revision,
                retained_ref="HEAD",
                view_root=root / "view",
                result_root=root / "result",
                cache_root=root / "cache",
                view_id="s4-view",
            )
            self.assertEqual(revision, view.retained_commit)
            self.assertTrue(view.assert_read_only())
            with self.assertRaises(ImmutableViewError):
                view.write_source("source.txt", b"escape")
            self.assertTrue((view.result_root / "VIEW_READY.json").is_file())

    def test_S4_LANE_RETIREMENT_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main = root / "main"
            main.mkdir()
            def git(cwd: Path, *args: str) -> str:
                return subprocess.run(["git", "-C", str(cwd), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip()
            git(main, "init", "--initial-branch", "main")
            git(main, "config", "user.email", "s4@example.invalid")
            git(main, "config", "user.name", "S4")
            (main / "tracked.txt").write_text("tracked\n", encoding="utf-8")
            git(main, "add", ".")
            git(main, "commit", "-m", "initial")
            lane = root / "lane"
            git(main, "worktree", "add", "-b", "s4-lane", str(lane), "HEAD")
            revision = git(lane, "rev-parse", "HEAD")
            refs = []
            evidence = root / "evidence"
            evidence.mkdir()
            for name in ("task", "result", "findings", "acceptance", "transcript", "dependency"):
                path = evidence / f"{name}.json"
                path.write_text("{}\n", encoding="utf-8")
                refs.append(path)
            lane_workspace = lane / ".agent-workspace"
            lane_workspace.mkdir()
            exclude = Path(git(lane, "rev-parse", "--git-path", "info/exclude"))
            if not exclude.is_absolute():
                exclude = lane / exclude
            exclude.write_text(".agent-workspace/\n", encoding="utf-8")
            runtime = self._publish_lifecycle_record(
                root, lane, lane_id="S4.P", revision=revision,
            )
            with patch(
                "orchestrator_harness.lane_lifecycle.process_snapshot",
                return_value=ProcessSnapshot(True, (), (), "synthetic-test"),
            ):
                result = retire_terminal_lane(
                    lane,
                    root / "archive",
                    lane_id="S4.P",
                    run_coordinate=runtime,
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("CLOSED", result.outcome)
            self.assertFalse(lane.exists())
            self.assertEqual("orchestrator-lane-archive/v1", validate_lane_archive(result.archive_path)["schema"])

            dirty = root / "dirty"
            git(main, "worktree", "add", "-b", "s4-dirty", str(dirty), "HEAD")
            (dirty / "tracked.txt").write_text("dirty\n", encoding="utf-8")
            blocked = retire_terminal_lane(
                dirty, root / "archive-dirty", lane_id="dirty", run_coordinate=runtime,
            )
            self.assertEqual("VISIBLE", blocked.outcome)
            self.assertTrue(dirty.exists())
            (dirty / "tracked.txt").write_text("tracked\n", encoding="utf-8")
            git(main, "worktree", "remove", str(dirty))

    def test_S4_ADAPTER_HELPER_LIFECYCLE_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manager = Path(raw) / "manager"
            _, coordinator, _ = self._adapter(manager)
            coordinator.register()
            coordinator.router.admit(self._event("helper"))
            restarted = create_codex_adapter(self._router(manager), transport=SyntheticCodexTransport()).coordinator
            restarted.register()
            self.assertEqual(1, len(restarted.pending_events()))
            self.assertFalse(restarted.close_binding())
            restarted.acknowledge_event("helper")
            self.assertTrue(restarted.close_binding())

    def test_S4_FORBIDDEN_CALLS_001(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter, coordinator, transport = self._adapter(Path(raw) / "manager")
            coordinator.register()
            coordinator.router.admit(self._event("safe"))
            adapter.turn_completed()
            methods = [call["method"] for call in transport.calls]
            forbidden = {
                "/".join(("turn", "steer")),
                "/".join(("turn", "interrupt")),
                "/".join(("process", "terminate")),
            }
            self.assertTrue(forbidden.isdisjoint(methods))
            self.assertTrue(any(method == "turn/start" for method in methods))


if __name__ == "__main__":
    unittest.main()
