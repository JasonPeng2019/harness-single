from __future__ import annotations

import json
import importlib
import os
import pkgutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from orchestrator_harness.codex_adapter import (
    CODEX_ADAPTER_VERSION,
    CODEX_INSTALL_MANIFEST_SCHEMA,
    CODEX_PACKAGE_REVISION,
    CodexAdapterError,
    CodexInstallConflict,
    install_codex_adapter,
    check_codex_adapter,
    uninstall_codex_adapter,
)
from orchestrator_harness.host_adapters import (
    AdapterCapabilities,
    DeliveryCoordinator,
    DeliveryReceipt,
    HostAdapter,
    HostProfile,
)
from orchestrator_harness.lane_lifecycle import (
    ImmutableViewError,
    allocate_immutable_source_view,
    retire_terminal_lane,
    validate_lane_archive,
)
from orchestrator_harness.notifications import ManagerEventRouter
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot
from orchestrator_harness.stable_io import SafeOutput
from orchestrator_harness import codex_adapter, lane_lifecycle


class _WrongReceiptAdapter(HostAdapter):
    @property
    def profile(self) -> HostProfile:
        return HostProfile("codex", "codex-v1", AdapterCapabilities.codex(), True)

    def deliver_notice(self, notice, *, boundary):
        return DeliveryReceipt(
            receipt_id="wrong-receipt",
            notice_id=notice.notice_id,
            run_id=notice.run_id,
            queue_id=notice.queue_id,
            manager_session_id=notice.manager_session_id,
            manager_thread_id="cross-bound-thread",
            registration_id=notice.registration_id,
            registration_generation=notice.registration_generation,
            observed_queue_revision=notice.observed_queue_revision,
            boundary=boundary,
            outcome="DELIVERED",
            delivered_utc=notice.observed_utc,
            adapter_profile=notice.adapter_profile,
        )


class S4RepairRegressionTests(unittest.TestCase):
    @staticmethod
    def _git(cwd: Path, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.stdout.strip()

    @staticmethod
    def _event(event_id: str) -> dict[str, object]:
        return {
            "event_id": event_id,
            "type": "MANAGER_SIGNAL",
            "identity": f"repair:{event_id}",
            "data": {
                "signal_id": event_id,
                "lane_id": "repair:s4",
                "manager_actionable": True,
                "severity": "warning",
            },
        }

    @staticmethod
    def _process_evidence(
        path: Path,
        *,
        processes: list[dict[str, object]] | None = None,
        identities: dict[str, dict[str, object]] | None = None,
        binding: dict[str, object] | None = None,
        provider: str = "synthetic",
    ) -> None:
        persisted = identities or {
            "controller": {"pid": 9001, "created_utc": "2000-01-01T00:00:00Z"},
            "worker": {"pid": 9002, "created_utc": "2000-01-01T00:00:00Z"},
            "helper": {"pid": 9003, "created_utc": "2000-01-01T00:00:00Z"},
        }
        evidence: dict[str, object] = {
            "schema": "orchestrator-process-evidence/v1",
            "complete": True,
            "provider": provider,
            "identities": persisted,
            "processes": processes or [],
        }
        if binding is not None:
            evidence["lane_binding"] = binding
        path.write_text(json.dumps(evidence) + "\n", encoding="utf-8")

    @classmethod
    def _lane_binding(
        cls,
        lane: Path,
        *,
        lane_id: str,
        expected_head: str,
        retained_ref: str,
        target_revision: str,
    ) -> dict[str, object]:
        common = Path(cls._git(lane, "rev-parse", "--git-common-dir"))
        if not common.is_absolute():
            common = (lane / common).resolve()
        return {
            "lane_id": lane_id,
            "worktree": str(lane.resolve()),
            "git_common_dir": str(common.resolve()),
            "branch": cls._git(lane, "symbolic-ref", "--short", "HEAD"),
            "expected_head": expected_head,
            "retained_ref": retained_ref,
            "target_revision": target_revision,
        }

    @classmethod
    def _git_fixture(cls, root: Path) -> tuple[Path, Path, str]:
        main = root / "main"
        main.mkdir()
        cls._git(main, "init", "--initial-branch", "main")
        cls._git(main, "config", "user.email", "s4-repair@example.invalid")
        cls._git(main, "config", "user.name", "S4 repair")
        (main / "tracked.txt").write_text("tracked\n", encoding="utf-8")
        cls._git(main, "add", ".")
        cls._git(main, "commit", "-m", "initial")
        lane = root / "lane"
        cls._git(main, "worktree", "add", "-b", "repair-lane", str(lane), "HEAD")
        return main, lane, cls._git(lane, "rev-parse", "HEAD")

    @staticmethod
    def _archive_refs(root: Path) -> list[Path]:
        evidence = root / "evidence"
        evidence.mkdir(exist_ok=True)
        refs: list[Path] = []
        for name in ("task", "result", "findings", "acceptance", "transcript", "dependency"):
            path = evidence / f"{name}.json"
            path.write_text(json.dumps({"name": name}) + "\n", encoding="utf-8")
            refs.append(path)
        return refs

    @classmethod
    def _lane_process_path(cls, lane: Path) -> Path:
        workspace = lane / ".agent-workspace"
        workspace.mkdir(exist_ok=True)
        exclude = Path(cls._git(lane, "rev-parse", "--git-path", "info/exclude"))
        if not exclude.is_absolute():
            exclude = lane / exclude
        existing = exclude.read_text(encoding="utf-8") if exclude.exists() else ""
        if ".agent-workspace/" not in existing:
            exclude.write_text(existing + ".agent-workspace/\n", encoding="utf-8")
        return workspace / "process.json"

    def test_FC1_closed_manifest_rejects_foreign_prior_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            (project / ".codex").mkdir()
            outside = root / "outside.txt"
            outside.write_bytes(b"user bytes\n")
            identity = project.stat()
            forged = {
                "schema": CODEX_INSTALL_MANIFEST_SCHEMA,
                "adapter": "codex",
                "adapter_version": CODEX_ADAPTER_VERSION,
                "package_revision": CODEX_PACKAGE_REVISION,
                "project_root": str(project),
                "project_identity": f"{identity.st_dev}:{identity.st_ino}",
                "managed_paths": ["outside.txt"],
                "prior_content": {"outside.txt": {"present": True, "bytes_b64": "dHJhaXQ="}},
            }
            (project / ".codex" / "orchestrator-harness-adapter.json").write_text(
                json.dumps(forged), encoding="utf-8"
            )
            checked = check_codex_adapter(project)
            self.assertEqual("foreign_or_ambiguous", checked["ownership"])
            with self.assertRaises(CodexInstallConflict):
                install_codex_adapter(project)
            with self.assertRaises(CodexInstallConflict):
                uninstall_codex_adapter(project)
            self.assertEqual(b"user bytes\n", outside.read_bytes())

    def test_FC2_reparse_parent_is_rejected_before_project_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            hooks = project / ".codex" / "hooks"
            hooks.mkdir(parents=True)
            outside = root / "outside"
            outside.mkdir()
            guard = codex_adapter._ProjectMutationGuard(project, prepare_codex=True)
            moved = outside / "original-hooks"
            swapped = False

            def swap_parent(*args, **kwargs):
                nonlocal swapped
                if not swapped:
                    hooks.rename(moved)
                    hooks.mkdir()
                    swapped = True

            try:
                with patch("orchestrator_harness.mutation._before_commit", side_effect=swap_parent):
                    with self.assertRaises(CodexInstallConflict):
                        guard.atomic_replace(Path(".codex/hooks/repair.txt"), b"bounded")
                self.assertFalse((outside / "repair.txt").exists())
            finally:
                for child in hooks.glob("*"):
                    child.unlink(missing_ok=True)
                hooks.rmdir()
                moved.rename(hooks)

    def test_FC10_changed_target_after_authorization_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            hooks = project / ".codex" / "hooks"
            hooks.mkdir(parents=True)
            target = hooks / "repair.txt"
            target.write_bytes(b"authorized-before\n")
            guard = codex_adapter._ProjectMutationGuard(project, prepare_codex=True)
            changed = False

            def change_target():
                nonlocal changed
                target.write_bytes(b"user-after-authorization\n")
                changed = True

            with patch("orchestrator_harness.mutation._before_commit", side_effect=change_target):
                with self.assertRaises(CodexInstallConflict):
                    guard.atomic_replace(
                        Path(".codex/hooks/repair.txt"),
                        b"managed-after-authorization\n",
                    )
            self.assertTrue(changed)
            self.assertEqual(b"user-after-authorization\n", target.read_bytes())

    def test_FC11_parent_reparse_swap_has_no_outside_write(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            hooks = project / ".codex" / "hooks"
            hooks.mkdir(parents=True)
            outside = root / "outside"
            outside.mkdir()
            sentinel = outside / "sentinel.txt"
            sentinel.write_bytes(b"outside-user-bytes\n")
            moved = outside / "original-hooks"
            guard = codex_adapter._ProjectMutationGuard(project, prepare_codex=True)
            swapped = False

            def swap_parent() -> None:
                nonlocal swapped
                hooks.rename(moved)
                result = subprocess.run(
                    ["cmd.exe", "/c", "mklink", "/J", str(hooks), str(outside)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if result.returncode != 0:
                    raise AssertionError(f"junction fixture failed: {result.stderr}")
                swapped = True

            try:
                with patch("orchestrator_harness.mutation._before_commit", side_effect=swap_parent, create=True):
                    with self.assertRaises(CodexInstallConflict):
                        guard.atomic_replace(Path(".codex/hooks/reparse.txt"), b"must stay inside\n")
                self.assertTrue(swapped)
                self.assertEqual(b"outside-user-bytes\n", sentinel.read_bytes())
                self.assertEqual({"original-hooks", "sentinel.txt"}, {path.name for path in outside.glob("*")})
                self.assertFalse((outside / "reparse.txt").exists())
            finally:
                if hooks.exists() or os.path.lexists(hooks):
                    hooks.rmdir()
                if moved.exists():
                    moved.rename(hooks)

    def test_FC12_changed_target_head_after_authorization_stays_visible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, authorized = self._git_fixture(root)
            (lane / "changed-after-auth.txt").write_text("changed\n", encoding="utf-8")
            self._git(lane, "add", ".")
            self._git(lane, "commit", "-m", "change after authorization")
            actual_head = self._git(lane, "rev-parse", "HEAD")
            self.assertNotEqual(authorized, actual_head)
            refs = self._archive_refs(root)
            process = self._lane_process_path(lane)
            self._process_evidence(
                process,
                binding=self._lane_binding(
                    lane,
                    lane_id="repair",
                    expected_head=authorized,
                    retained_ref="refs/heads/main",
                    target_revision=authorized,
                ),
            )
            result = retire_terminal_lane(
                lane,
                root / "archive",
                lane_id="repair",
                retained_revision=authorized,
                retained_ref="refs/heads/main",
                target_revision=authorized,
                process_evidence=process,
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", result.outcome)
            self.assertTrue(lane.exists())
            self.assertEqual(actual_head, self._git(lane, "rev-parse", "HEAD"))
            del main

    def test_FC13_foreign_worktree_binding_stays_visible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            foreign = root / "foreign"
            self._git(main, "worktree", "add", "-b", "foreign-lane", str(foreign), "HEAD")
            refs = self._archive_refs(root)
            process = self._lane_process_path(lane)
            self._process_evidence(
                process,
                binding=self._lane_binding(
                    foreign,
                    lane_id="foreign",
                    expected_head=revision,
                    retained_ref="refs/heads/main",
                    target_revision=revision,
                ),
            )
            result = retire_terminal_lane(
                lane,
                root / "archive",
                lane_id="repair",
                retained_revision=revision,
                retained_ref="refs/heads/main",
                target_revision=revision,
                process_evidence=process,
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", result.outcome)
            self.assertTrue(lane.exists())
            self._git(main, "worktree", "remove", "--force", str(foreign))
            del lane

    def test_FC14_retirement_uses_fresh_complete_process_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            process = self._lane_process_path(lane)
            created = datetime(2000, 1, 1, tzinfo=timezone.utc)
            identities = {
                role: {"pid": 9000 + index, "created_utc": "2000-01-01T00:00:00Z"}
                for index, role in enumerate(("controller", "worker", "helper"))
            }
            self._process_evidence(
                process,
                identities=identities,
                binding=self._lane_binding(
                    lane,
                    lane_id="live",
                    expected_head=revision,
                    retained_ref="refs/heads/main",
                    target_revision=revision,
                ),
            )
            provider = MagicMock(return_value=ProcessSnapshot(
                complete=True,
                processes=tuple(
                    ProcessInfo(
                        pid=9000 + index,
                        ppid=1,
                        name="python",
                        command_line="synthetic live process",
                        created_utc=created,
                    )
                    for index in range(3)
                ),
                provider="synthetic-test",
            ))
            with patch.object(lane_lifecycle, "process_snapshot", provider, create=True):
                result = retire_terminal_lane(
                    lane,
                    root / "archive",
                    lane_id="live",
                    retained_revision=revision,
                    retained_ref="refs/heads/main",
                    target_revision=revision,
                    process_evidence=process,
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertTrue(provider.called)
            self.assertEqual("VISIBLE", result.outcome)
            self.assertEqual("LIVE_USE_PROVEN", result.reason)
            self.assertTrue(lane.exists())
            del main

    def test_FC15_retained_tests_import_without_removed_authority(self) -> None:
        package = importlib.import_module("orchestrator_harness.tests")
        failures: list[str] = []
        for module in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
            if not module.name.rsplit(".", 1)[-1].startswith("test_"):
                continue
            try:
                importlib.import_module(module.name)
            except Exception as exc:  # pragma: no cover - failure detail is asserted below
                failures.append(f"{module.name}: {type(exc).__name__}: {exc}")
        self.assertEqual([], failures)

    def test_FC4_cross_bound_receipt_is_not_successfully_journaled(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "manager"
            router = ManagerEventRouter(
                root,
                run_id="run-repair",
                queue_id="queue-repair",
                manager_session_id="session-repair",
                manager_thread_id="thread-repair",
                registration_id="registration-repair",
            )
            coordinator = DeliveryCoordinator(router, _WrongReceiptAdapter())
            coordinator.register()
            router.admit(self._event("wrong-receipt"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            receipt = coordinator.deliver_at_boundary(notice)
            self.assertEqual("DELIVERY_REJECTED", receipt.outcome)
            self.assertEqual(1, len(router.pending_events()))
            self.assertEqual("RECEIPT_BINDING_MISMATCH", receipt.error_class)

    def test_FC5_removed_policy_has_no_live_output_surface(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            store = SafeOutput(harness_root=root, output_root=root / "output", forbidden_roots=())
            store.prepare()
            store.commit(snapshot={"observed_utc": "2026-01-01T00:00:00Z"}, events=[], conditions={})
            self.assertFalse((root / "output" / "pending-notification.json").exists())
            self.assertFalse(hasattr(store, "load_notification_state"))
            self.assertFalse(hasattr(store, "claim_managed_watcher"))
            import orchestrator_harness.attention_sprint as sprint
            self.assertFalse(hasattr(sprint, "validate_sprint_boundary"))

    def test_FC6_immutable_allocation_rechecks_retained_ref_and_cleans_failure(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.mkdir()
            self._git(source, "init", "--initial-branch", "main")
            self._git(source, "config", "user.email", "s4-repair@example.invalid")
            self._git(source, "config", "user.name", "S4 repair")
            (source / "source.txt").write_text("source\n", encoding="utf-8")
            self._git(source, "add", ".")
            self._git(source, "commit", "-m", "source")
            revision = self._git(source, "rev-parse", "HEAD")
            tree = self._git(source, "rev-parse", "HEAD^{tree}")
            unreachable = self._git(source, "commit-tree", tree, "-m", "unreachable")
            with self.assertRaises(ImmutableViewError):
                allocate_immutable_source_view(
                    source,
                    revision=unreachable,
                    retained_ref="HEAD",
                    view_root=root / "failed-view",
                    result_root=root / "failed-result",
                    cache_root=root / "failed-cache",
                )
            self.assertFalse((root / "failed-view").exists())
            self.assertFalse((root / "failed-result" / "VIEW_READY.json").exists())
            original_atomic_json = lane_lifecycle._atomic_json
            def fail_ready(path: Path, value: dict[str, object]) -> None:
                if path.name == "VIEW_READY.json":
                    raise RuntimeError("synthetic READY publication failure")
                original_atomic_json(path, value)
            with patch("orchestrator_harness.lane_lifecycle._atomic_json", side_effect=fail_ready):
                with self.assertRaises(RuntimeError):
                    allocate_immutable_source_view(
                        source,
                        revision=revision,
                        retained_ref="HEAD",
                        view_root=root / "publication-failed-view",
                        result_root=root / "publication-failed-result",
                        cache_root=root / "publication-failed-cache",
                    )
            self.assertFalse((root / "publication-failed-view").exists())
            self.assertFalse((root / "publication-failed-result" / "VIEW_READY.json").exists())
            view = allocate_immutable_source_view(
                source,
                revision=revision,
                retained_ref="HEAD",
                view_root=root / "view",
                result_root=root / "result",
                cache_root=root / "cache",
            )
            with self.assertRaises(ImmutableViewError):
                view.write_source("source.txt", b"direct write")
            self.assertTrue(view.assert_read_only())

    def test_FC7_archive_remains_self_contained_after_source_evidence_disappears(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            lane_evidence_root = lane / ".agent-workspace"
            lane_evidence_root.mkdir()
            exclude = Path(self._git(lane, "rev-parse", "--git-path", "info/exclude"))
            if not exclude.is_absolute():
                exclude = lane / exclude
            exclude.write_text(".agent-workspace/\n", encoding="utf-8")
            refs = self._archive_refs(lane_evidence_root)
            process = lane_evidence_root / "evidence" / "process.json"
            self._process_evidence(
                process,
                binding=self._lane_binding(
                    lane,
                    lane_id="repair",
                    expected_head=revision,
                    retained_ref="refs/heads/main",
                    target_revision=revision,
                ),
            )
            result = retire_terminal_lane(
                lane,
                root / "archive",
                lane_id="repair",
                retained_revision=revision,
                retained_ref="refs/heads/main",
                target_revision=revision,
                process_evidence=process,
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("CLOSED", result.outcome)
            for path in [*refs, process]:
                path.unlink(missing_ok=True)
            self.assertEqual("repair", validate_lane_archive(result.archive_path)["lane_id"])
            del main

    def test_FC8_unretained_and_archive_incomplete_lanes_stay_visible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            process = self._lane_process_path(lane)
            self._process_evidence(
                process,
                binding=self._lane_binding(
                    lane,
                    lane_id="unretained",
                    expected_head=revision,
                    retained_ref="refs/heads/does-not-exist",
                    target_revision=revision,
                ),
            )
            blocked = retire_terminal_lane(
                lane,
                root / "archive-unretained",
                lane_id="unretained",
                retained_revision=revision,
                retained_ref="refs/heads/does-not-exist",
                target_revision=revision,
                process_evidence=process,
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", blocked.outcome)
            self.assertTrue(lane.exists())
            incomplete = retire_terminal_lane(
                lane,
                root / "archive-incomplete",
                lane_id="incomplete",
                retained_revision=revision,
                retained_ref="refs/heads/main",
                target_revision=revision,
                process_evidence=process,
                task_ref=None, result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", incomplete.outcome)
            self.assertTrue(lane.exists())
            current_created = "2026-01-01T00:00:00Z"
            live_pids = (os.getpid(), os.getpid() + 1, os.getpid() + 2)
            live_evidence = self._lane_process_path(lane).with_name("live-process.json")
            self._process_evidence(
                live_evidence,
                identities={
                    role: {"pid": pid, "created_utc": current_created}
                    for role, pid in zip(("controller", "worker", "helper"), live_pids)
                },
                processes=[
                    {"pid": pid, "ppid": 1, "name": "python", "command_line": "synthetic repair test", "created_utc": current_created}
                    for pid in live_pids
                ],
                binding=self._lane_binding(
                    lane,
                    lane_id="live",
                    expected_head=revision,
                    retained_ref="refs/heads/main",
                    target_revision=revision,
                ),
            )
            with patch.object(
                lane_lifecycle,
                "process_snapshot",
                return_value=ProcessSnapshot(
                    True,
                    tuple(
                        ProcessInfo(pid, 1, "python", "synthetic repair test", datetime(2026, 1, 1, tzinfo=timezone.utc))
                        for pid in live_pids
                    ),
                    (),
                    "synthetic-test",
                ),
            ):
                live = retire_terminal_lane(
                    lane,
                    root / "archive-live",
                    lane_id="live",
                    retained_revision=revision,
                    retained_ref="refs/heads/main",
                    target_revision=revision,
                    process_evidence=live_evidence,
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("VISIBLE", live.outcome)
            self.assertEqual("LIVE_USE_PROVEN", live.reason)
            (lane / "unmerged.txt").write_text("unmerged\n", encoding="utf-8")
            self._git(lane, "add", ".")
            self._git(lane, "commit", "-m", "unmerged")
            lane_revision = self._git(lane, "rev-parse", "HEAD")
            self._process_evidence(
                process,
                binding=self._lane_binding(
                    lane,
                    lane_id="unmerged",
                    expected_head=lane_revision,
                    retained_ref="HEAD",
                    target_revision=revision,
                ),
            )
            unmerged = retire_terminal_lane(
                lane,
                root / "archive-unmerged",
                lane_id="unmerged",
                retained_revision=lane_revision,
                retained_ref="HEAD",
                target_revision=revision,
                process_evidence=process,
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", unmerged.outcome)
            self.assertEqual("UNMERGED_WORK_PRESENT", unmerged.reason)
            self._git(main, "worktree", "remove", str(lane))

    def test_FC9_receipt_shape_and_delivery_path_cannot_be_ack(self) -> None:
        with self.assertRaises(ValueError):
            DeliveryReceipt.from_record({
                "schema": "orchestrator-delivery-receipt/v1",
                "receipt_id": "r", "notice_id": "n", "run_id": "run", "queue_id": "queue",
                "manager_session_id": "session", "registration_id": "registration",
                "registration_generation": 1, "observed_queue_revision": 1,
                "boundary": "post_tool_use", "outcome": "DELIVERED",
                "delivered_utc": "2026-01-01T00:00:00Z", "adapter_profile": "codex/codex-v1",
                "attempt": 1,
            })


if __name__ == "__main__":
    unittest.main()
