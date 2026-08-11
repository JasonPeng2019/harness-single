from __future__ import annotations

import json
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import orchestrator_harness.lane_controller as lane_controller
from orchestrator_harness import codex_adapter

from orchestrator_harness.codex_adapter import (
    CodexAdapter,
    CodexAdapterError,
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
    lifecycle_registry_path,
    retire_terminal_lane,
    validate_lane_archive,
)
from orchestrator_harness.models import ProcessSnapshot
from orchestrator_harness.notifications import ManagerEventRouter


class S4ContractTests(unittest.TestCase):
    def test_S4_CODEX_PACKAGED_ASSET_CANONICALIZATION_001(self) -> None:
        original_resource = codex_adapter._package_resource

        def crlf_resource(name: str) -> bytes:
            data = original_resource(name)
            if name == "orchestrator_harness_post_tool_use.py":
                return data.replace(b"\n", b"\r\n")
            if name == "orchestrator_harness_stop.py":
                lines = data.splitlines(keepends=True)
                return b"".join(
                    line.replace(b"\n", b"\r\n") if index % 2 else line
                    for index, line in enumerate(lines)
                )
            return data

        with patch.object(codex_adapter, "_package_resource", side_effect=crlf_resource):
            assets = codex_adapter.packaged_codex_assets()
        for relative, resource in (
            (Path(".codex/hooks/orchestrator_harness_post_tool_use.py"), "orchestrator_harness_post_tool_use.py"),
            (Path(".codex/hooks/orchestrator_harness_stop.py"), "orchestrator_harness_stop.py"),
        ):
            expected = original_resource(resource)
            self.assertEqual(expected, assets[relative])
            self.assertNotIn(b"\r\n", assets[relative])
            self.assertNotIn(b"\r", assets[relative])

    def test_S4_CODEX_PACKAGED_ASSET_INTEGRITY_REJECTS_NONCANONICAL_INPUT_001(self) -> None:
        original_resource = codex_adapter._package_resource
        resource_name = "orchestrator_harness_post_tool_use.py"
        original = original_resource(resource_name)

        def expect_rejected(transform) -> None:
            def substitute(name: str) -> bytes:
                data = original_resource(name)
                return transform(data) if name == resource_name else data

            with patch.object(codex_adapter, "_package_resource", side_effect=substitute):
                with self.assertRaises(CodexAdapterError):
                    codex_adapter.packaged_codex_assets()

        with self.subTest(case="invalid-utf8"):
            expect_rejected(lambda data: data + b"\xff")
        with self.subTest(case="bom"):
            expect_rejected(lambda data: b"\xef\xbb\xbf" + data)
        with self.subTest(case="lone-carriage-return"):
            expect_rejected(lambda data: data + b"\r")
        with self.subTest(case="non-newline-mutation"):
            self.assertIn(b"import", original)
            expect_rejected(lambda data: data.replace(b"import", b"IMPORT", 1))

    def test_S4_CODEX_PACKAGED_ASSET_MODE_is_closed_001(self) -> None:
        original_resource = codex_adapter._package_resource

        def expect_rejected(mode_marker: object) -> None:
            def substitute(name: str) -> bytes:
                data = original_resource(name)
                if name != "manifest.json":
                    return data
                manifest = json.loads(data.decode("utf-8"))
                entry = manifest["files"][0]
                if mode_marker is None:
                    entry.pop("content_mode", None)
                else:
                    entry["content_mode"] = mode_marker
                return json.dumps(manifest).encode("utf-8")

            with patch.object(codex_adapter, "_package_resource", side_effect=substitute):
                with self.assertRaises(CodexAdapterError):
                    codex_adapter.packaged_codex_assets()

        for label, marker in (
            ("missing", None),
            ("non-string", 1),
            ("unknown", "binary"),
        ):
            with self.subTest(case=label):
                expect_rejected(marker)

    def test_S4_CODEX_PACKAGED_ASSET_PATHS_REJECT_UNSAFE_001(self) -> None:
        original_resource = codex_adapter._package_resource

        def expect_rejected(field: str, value: str) -> None:
            def substitute(name: str) -> bytes:
                data = original_resource(name)
                if name != "manifest.json":
                    return data
                manifest = json.loads(data.decode("utf-8"))
                manifest["files"][0][field] = value
                return json.dumps(manifest).encode("utf-8")

            with patch.object(codex_adapter, "_package_resource", side_effect=substitute):
                with self.assertRaises(CodexAdapterError):
                    codex_adapter.packaged_codex_assets()

        for field, value in (
            ("destination", ""),
            ("destination", "../escape.py"),
            ("resource", "../escape.py"),
        ):
            with self.subTest(field=field, value=value):
                expect_rejected(field, value)

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
        """Use the real coding controller admission/publication path."""

        workspace = lane / ".agent-workspace"
        workspace.mkdir(exist_ok=True)
        common = Path(subprocess.run(["git", "-C", str(lane), "rev-parse", "--git-common-dir"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip())
        if not common.is_absolute():
            common = (lane / common).resolve()
        branch = subprocess.run(["git", "-C", str(lane), "symbolic-ref", "--short", "HEAD"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip()
        exclude = Path(subprocess.run(["git", "-C", str(lane), "rev-parse", "--git-path", "info/exclude"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip())
        if not exclude.is_absolute():
            exclude = lane / exclude
        existing_exclude = exclude.read_text(encoding="utf-8") if exclude.exists() else ""
        if ".agent-workspace/" not in existing_exclude:
            exclude.write_text(existing_exclude + ".agent-workspace/\n", encoding="utf-8")
        prompt = workspace / "repair-004-prompt.md"
        prompt.write_text("synthetic production lifecycle prompt\n", encoding="utf-8")
        fake = root / "fake-provider.py"
        fake.write_text(
            "import json,sys\n"
            "sys.stdin.read()\n"
            "print(json.dumps({'type':'thread.started','thread_id':'synthetic-lifecycle-thread'}), flush=True)\n"
            "print(json.dumps({'type':'turn.completed'}), flush=True)\n",
            encoding="utf-8",
        )
        runtime = root / "runtime"
        runtime.mkdir(exist_ok=True)
        invocation = root / f"{lane_id.replace(':', '-')}-invocation.json"
        status = workspace / "controller.status.json"
        value = {
            "schema": lane_controller.CODING_INVOCATION_SCHEMA,
            "action": "start",
            "run_root": str(lane),
            "runtime_root": str(runtime),
            "event_log_path": str(runtime / "events" / "controller.jsonl"),
            "worker_invocation_id": f"worker-{lane_id}",
            "lane_id": lane_id,
            "task": "synthetic lifecycle",
            "phase": "repair",
            "prompt_path": str(prompt),
            "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
            "output_paths": {
                "status": str(status),
                "jsonl": str(workspace / "controller.jsonl"),
                "stderr": str(workspace / "controller.stderr.log"),
                "last_message": str(workspace / "last-message.txt"),
            },
            "exclusive_resources": [],
            "repository": {
                "common_dir": str(common.resolve()), "worktree_root": str(lane.resolve()),
                "branch": branch, "base_commit": revision,
            },
            "codex": {
                "model": "synthetic", "reasoning_effort": "medium", "service_tier": "priority",
                "command": [sys.executable, str(fake)], "config_overrides": [],
                "sandbox": "workspace-write", "approval_policy": "never",
            },
        }
        invocation.write_text(json.dumps(value), encoding="utf-8")
        if lane_controller.main([str(invocation)]) != 0:
            raise AssertionError("synthetic production controller did not complete")
        return lifecycle_registry_path(lane, lane_id, f"worker-{lane_id}")

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
            self._publish_lifecycle_record(
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
                dirty, root / "archive-dirty", lane_id="dirty",
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
