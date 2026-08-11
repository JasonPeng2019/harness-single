from __future__ import annotations

import json
import hashlib
import ctypes
import io
import importlib
import os
import pkgutil
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from unittest.mock import MagicMock, patch

import orchestrator_harness.lane_controller as lane_controller

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
    lifecycle_registry_path,
    retire_terminal_lane,
    validate_lane_archive,
)
from orchestrator_harness.notifications import ManagerEventRouter
from orchestrator_harness.models import ProcessBoundaryInventory, ProcessInfo, ProcessSnapshot, iso_utc
from orchestrator_harness.process_supervisor import CleanupResult, ProcessBoundary, ProcessSupervisor
from orchestrator_harness.processes import process_group_inventory, targeted_process_query
from orchestrator_harness.resource_locks import ResourceClaims, ResourceLockError, _owner_state
from orchestrator_harness.stable_io import AppendLockError, PathKeyedAppendLock, SafeOutput
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
    def _lifecycle_record(
        cls,
        root: Path,
        lane: Path,
        *,
        lane_id: str,
        expected_head: str,
        retained_ref: str,
        target_revision: str,
        identities: dict[str, dict[str, object]] | None = None,
        helpers: list[dict[str, object]] | None = None,
        state: str = "PROVIDER_EXITED",
        worktree: Path | None = None,
        expected_code: int = 0,
    ) -> Path:
        """Create lifecycle authority through the real controller path."""

        workspace = lane / ".agent-workspace"
        workspace.mkdir(exist_ok=True)
        bound_worktree = lane
        common = Path(cls._git(lane, "rev-parse", "--git-common-dir"))
        if not common.is_absolute():
            common = (lane / common).resolve()
        branch = cls._git(lane, "symbolic-ref", "--short", "HEAD")
        exclude = Path(cls._git(lane, "rev-parse", "--git-path", "info/exclude"))
        if not exclude.is_absolute():
            exclude = lane / exclude
        existing_exclude = exclude.read_text(encoding="utf-8") if exclude.exists() else ""
        if ".agent-workspace/" not in existing_exclude:
            exclude.write_text(existing_exclude + ".agent-workspace/\n", encoding="utf-8")
        prompt = workspace / "repair-004-prompt.md"
        prompt.write_text("synthetic production lifecycle prompt\n", encoding="utf-8")
        fake = root / f"fake-provider-{lane_id.replace(':', '-')}.py"
        helper_count = len(helpers or [])
        fake.write_text(
            "import json,sys,subprocess,time\n"
            "sys.stdin.read()\n"
            f"helpers=[subprocess.Popen([sys.executable,'-c','import time; time.sleep(2.0)']) for _ in range({helper_count})]\n"
            "print(json.dumps({'type':'thread.started','thread_id':'synthetic-lifecycle-thread'}), flush=True)\n"
            "time.sleep(2.5)\n"
            "[helper.wait() for helper in helpers]\n"
            "print(json.dumps({'type':'turn.completed'}), flush=True)\n",
            encoding="utf-8",
        )
        runtime = root / "runtime"
        runtime.mkdir(exist_ok=True)
        invocation = root / f"{lane_id.replace(':', '-')}-invocation.json"
        status = workspace / "controller.status.json"
        value = {
            "schema": lane_controller.CODING_INVOCATION_SCHEMA,
            "action": "start", "run_root": str(lane), "runtime_root": str(runtime),
            "event_log_path": str(runtime / "events" / f"{lane_id.replace(':', '-')}.jsonl"),
            "worker_invocation_id": f"worker-{lane_id}", "lane_id": lane_id,
            "task": "synthetic lifecycle", "phase": "repair",
            "prompt_path": str(prompt), "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
            "output_paths": {
                "status": str(status), "jsonl": str(workspace / "controller.jsonl"),
                "stderr": str(workspace / "controller.stderr.log"), "last_message": str(workspace / "last-message.txt"),
            },
            "exclusive_resources": [],
            "repository": {"common_dir": str(common.resolve()), "worktree_root": str(lane.resolve()), "branch": branch, "base_commit": expected_head},
            "codex": {"model": "synthetic", "reasoning_effort": "medium", "service_tier": "priority", "command": [sys.executable, str(fake)], "config_overrides": [], "sandbox": "workspace-write", "approval_policy": "never"},
        }
        invocation.write_text(json.dumps(value), encoding="utf-8")
        if lane_controller.main([str(invocation)]) != expected_code:
            raise AssertionError(f"synthetic production controller returned an unexpected code (wanted {expected_code})")
        return lifecycle_registry_path(lane, lane_id, f"worker-{lane_id}")

    def _run_synthetic_controller_boundary(
        self,
        final_inventory: ProcessBoundaryInventory | None,
        *,
        final_inventory_error: BaseException | None = None,
        attach_error: BaseException | None = None,
        arm_failures: list[str] | None = None,
        post_popen_error: BaseException | None = None,
        captured_exception: dict[str, BaseException] | None = None,
        real_supervisor: bool = False,
        poll_error: BaseException | None = None,
        terminate_error: BaseException | None = None,
        wait_plan: list[object] | None = None,
        kill_error: BaseException | None = None,
    ) -> tuple[int, ResourceClaims, list[None], list[None], MagicMock]:
        """Drive lane_controller.run with a disposable production-shaped boundary."""

        from orchestrator_harness.tests.test_controller_lock_cleanup import ControllerLockCleanupTests

        case = ControllerLockCleanupTests("test_unproven_child_shutdown_retains_owned_claim")
        case.setUp()
        try:
            invocation = case.invocation()
            owner = ProcessInfo(101, 1, "controller", "controller", datetime(2026, 8, 11, tzinfo=timezone.utc))
            child_identity = ProcessInfo(202, 101, "codex", "codex", datetime(2026, 8, 11, 0, 0, 1, tzinfo=timezone.utc))

            class SyntheticProcess:
                pid = child_identity.pid

                def __init__(self) -> None:
                    self.stdin = io.BytesIO()
                    self.stdout = io.BytesIO()
                    self.stderr = io.BytesIO()
                    self.calls: list[str] = []
                    self.poll_error = poll_error
                    self.terminate_error = terminate_error
                    self.wait_plan = list(wait_plan or [])
                    self.kill_error = kill_error

                def poll(self) -> int | None:
                    self.calls.append("poll")
                    if self.poll_error is not None:
                        raise self.poll_error
                    return None

                def terminate(self) -> None:
                    self.calls.append("terminate")
                    if self.terminate_error is not None:
                        raise self.terminate_error

                def wait(self, *, timeout: float | None = None) -> int:
                    del timeout
                    self.calls.append("wait")
                    value = self.wait_plan.pop(0) if self.wait_plan else 0
                    if isinstance(value, BaseException):
                        raise value
                    return int(value)

                def kill(self) -> None:
                    self.calls.append("kill")
                    if self.kill_error is not None:
                        raise self.kill_error

            process = SyntheticProcess()

            class SyntheticBoundary:
                kind = "synthetic-boundary"
                identity = "synthetic-boundary:1"

                def __init__(self) -> None:
                    self.popen_kwargs: dict[str, object] = {}
                    self.use_final_inventory = real_supervisor

                def attach(self, _process: object, _identity: ProcessInfo) -> None:
                    if attach_error is not None:
                        raise attach_error

                def inventory(self) -> ProcessBoundaryInventory:
                    if self.use_final_inventory:
                        if final_inventory_error is not None:
                            raise final_inventory_error
                        if final_inventory is None:
                            return ProcessBoundaryInventory(
                                False,
                                self.kind,
                                self.identity,
                                errors=("synthetic final inventory unavailable",),
                                source="synthetic-final",
                            )
                        return final_inventory
                    return ProcessBoundaryInventory(True, self.kind, self.identity, source="synthetic")

                def cleanup_owned(self, **_kwargs: object) -> ProcessBoundaryInventory:
                    return self.inventory()

                def to_record(self, inventory: ProcessBoundaryInventory) -> dict[str, object]:
                    def item_record(item: ProcessInfo) -> dict[str, object]:
                        return {"pid": item.pid, "created_utc": iso_utc(item.created_utc), "name": item.name}

                    return {
                        "schema": "orchestrator-process-boundary/v1",
                        "kind": self.kind,
                        "identity": self.identity,
                        "complete": inventory.complete,
                        "inventory_source": inventory.source,
                        "errors": list(inventory.errors),
                        "members": [item_record(item) for item in inventory.observed_processes],
                        "live_members": [item_record(item) for item in inventory.processes],
                    }

                def close(self) -> None:
                    return None

            boundary = SyntheticBoundary()
            cleanup = CleanupResult(
                pid=child_identity.pid,
                expected_created_utc=iso_utc(child_identity.created_utc),
                status="REAPED",
                stages=("FINAL_REAP", "BOUNDARY_EMPTY"),
                final_reap=True,
                cleanup_confirmed=True,
                identity_verified=True,
                exit_code=0,
                reaped_after="synthetic",
                owned_boundary_empty=True,
                boundary_complete=True,
                boundary_cleanup="boundary-empty",
            )

            class SyntheticSupervisor:
                def wait_for_exit(self) -> int:
                    return 0

                def cleanup(self) -> CleanupResult:
                    return cleanup

                def boundary_inventory(self) -> ProcessBoundaryInventory:
                    if final_inventory_error is not None:
                        raise final_inventory_error
                    assert final_inventory is not None
                    return final_inventory

            supervisor = SyntheticSupervisor()
            captured: list[ResourceClaims] = []
            release_calls: list[None] = []
            retain_calls: list[None] = []

            def claims_factory(
                root: Path, lane_id: str, worker_id: str, process_info: ProcessInfo,
            ) -> ResourceClaims:
                claims = ResourceClaims(
                    root,
                    lane_id,
                    worker_id,
                    process_info,
                    identity_provider=lambda pid: {"pid": pid, "created_utc": f"identity:{pid}"},
                )
                original_release = claims.release_all
                original_retain = claims.retain_boundary

                def release() -> list[str]:
                    release_calls.append(None)
                    return original_release()

                def retain(*, boundary: Mapping[str, object] | None, identities: list[Mapping[str, object]]) -> list[str]:
                    retain_calls.append(None)
                    return original_retain(boundary=boundary, identities=identities)

                claims.release_all = release
                claims.retain_boundary = retain  # type: ignore[method-assign]
                if arm_failures is not None:
                    claims.arm_boundary = lambda: list(arm_failures)  # type: ignore[method-assign]
                captured.append(claims)
                return claims

            popen = MagicMock(return_value=process)
            identity_results: list[object] = [
                owner,
                post_popen_error if post_popen_error is not None else child_identity,
            ]
            with (
                patch.object(lane_controller, "_identity", side_effect=identity_results),
                patch.object(lane_controller, "ResourceClaims", side_effect=claims_factory),
                patch.object(lane_controller.subprocess, "Popen", popen),
                patch.object(lane_controller.ProcessBoundary, "prepare", return_value=boundary),
                (
                    nullcontext()
                    if real_supervisor
                    else patch.object(lane_controller, "ProcessSupervisor", return_value=supervisor)
                ),
            ):
                try:
                    result = lane_controller.run(invocation)
                except BaseException as exc:
                    if captured_exception is None:
                        raise
                    captured_exception["exception"] = exc
                    result = 1
            self.assertEqual(1, len(captured))
            captured[0]._test_status = (  # type: ignore[attr-defined]
                json.loads(invocation.status_path.read_text(encoding="utf-8"))
                if invocation.status_path.exists()
                else {}
            )
            return result, captured[0], release_calls, retain_calls, popen
        finally:
            case.tearDown()

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

    def test_FC16_caller_selected_lane_record_cannot_authorize_retirement(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            foreign_runtime = root / "foreign-runtime"
            foreign_runtime.mkdir()
            (foreign_runtime / "LIFECYCLE.json").write_text(
                json.dumps({"record_sha256": "recomputed-foreign-record", "lane_id": "repair"}) + "\n",
                encoding="utf-8",
            )
            with patch.object(
                lane_lifecycle,
                "process_snapshot",
                return_value=ProcessSnapshot(True, (), (), "synthetic-test"),
            ):
                result = retire_terminal_lane(
                    lane,
                    root / "archive",
                    lane_id="repair",
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("VISIBLE", result.outcome)
            self.assertTrue(lane.exists())
            del main

    def test_FC17_missing_foreign_or_modified_fixed_record_stays_visible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            runtime = root / "runtime"
            runtime.mkdir()
            missing = retire_terminal_lane(
                lane, root / "archive-missing", lane_id="missing",
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", missing.outcome)
            self._lifecycle_record(
                root, lane, lane_id="modified", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
            )
            registry = lifecycle_registry_path(lane, "modified", "worker-modified")
            value = json.loads(registry.read_text(encoding="utf-8"))
            value["repository"]["branch"] = "forged-branch"
            value["record_sha256"] = lane_lifecycle._registry_digest(value)
            registry.write_text(json.dumps(value) + "\n", encoding="utf-8")
            modified = retire_terminal_lane(
                lane, root / "archive-modified", lane_id="modified",
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", modified.outcome)
            self.assertTrue(lane.exists())
            del main

    def test_FC18_registry_change_after_initial_proof_keeps_lane_and_archive_visible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            runtime = self._lifecycle_record(
                root, lane, lane_id="changed", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
            )
            registry = lifecycle_registry_path(lane, "changed", "worker-changed")
            changed = False

            def mutate_after_initial_proof() -> None:
                nonlocal changed
                value = json.loads(registry.read_text(encoding="utf-8"))
                value["lifecycle"]["state"] = "PROVIDER_EXITED"
                value["record_sha256"] = "forged-after-proof"
                registry.write_text(json.dumps(value) + "\n", encoding="utf-8")
                changed = True

            with patch.object(lane_lifecycle, "_after_initial_retirement_proof", side_effect=mutate_after_initial_proof):
                with patch.object(
                    lane_lifecycle,
                    "process_snapshot",
                    return_value=ProcessSnapshot(True, (), (), "synthetic-test"),
                ):
                    result = retire_terminal_lane(
                        lane, root / "archive", lane_id="changed",
                        task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                        acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                    )
            self.assertTrue(changed)
            self.assertEqual("VISIBLE", result.outcome)
            self.assertTrue(lane.exists())
            self.assertIsNotNone(result.archive_path)
            self.assertEqual("PENDING", validate_lane_archive(result.archive_path)["close_result"])
            del main

    def test_FC23_foreign_runtime_forgery_is_ignored_by_canonical_retirement(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            foreign = root / "foreign-runtime" / "anything"
            foreign.mkdir(parents=True)
            (foreign / "LIFECYCLE.json").write_text(json.dumps({
                "schema": "orchestrator-lifecycle-registry/v1",
                "authority": "controller-admitted-canonical-coordinate",
                "record_sha256": "recomputed-but-not-authority",
                "repository": {"worktree_root": str(lane), "expected_head": revision},
                "identities": {"controller": {"pid": 999991, "created_utc": "2000-01-01T00:00:00Z"}, "worker": {"pid": 999992, "created_utc": "2000-01-01T00:00:00Z"}, "helpers": []},
                "lifecycle": {"state": "PROVIDER_EXITED", "complete": True, "helpers_complete": True},
            }) + "\n", encoding="utf-8")
            result = retire_terminal_lane(
                lane, root / "archive", lane_id="repair",
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", result.outcome)
            self.assertTrue(lane.exists())
            del main

    def test_FC24_retirement_has_no_redirect_parameter_and_controller_rejects_reused_admission(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            import inspect
            self.assertNotIn("run_coordinate", inspect.signature(retire_terminal_lane).parameters)
            self._lifecycle_record(root, lane, lane_id="reuse", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision)
            second = self._lifecycle_record(root, lane, lane_id="reuse", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision, expected_code=2)
            self.assertTrue(second.is_file())
            del main

    def test_FC25_controller_publishes_real_one_and_multiple_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            one = root / "one"
            many = root / "many"
            self._git(main, "worktree", "add", "-b", "one-lane", str(one), "HEAD")
            self._git(main, "worktree", "add", "-b", "many-lane", str(many), "HEAD")
            one_path = self._lifecycle_record(root, one, lane_id="one", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision, helpers=[{"name": "synthetic-one"}])
            many_path = self._lifecycle_record(root, many, lane_id="many", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision, helpers=[{"name": "synthetic-one"}, {"name": "synthetic-two"}])
            one_record = json.loads(one_path.read_text(encoding="utf-8"))
            many_record = json.loads(many_path.read_text(encoding="utf-8"))
            self.assertEqual(1, len(one_record["identities"]["helpers"]))
            self.assertEqual(2, len(many_record["identities"]["helpers"]))
            self.assertTrue(one_record["boundary"]["complete"] and many_record["boundary"]["complete"])
            self._git(main, "worktree", "remove", str(one))
            self._git(main, "worktree", "remove", str(many))
            del lane

    def test_FC26_new_boundary_helper_after_first_proof_keeps_archive_pending(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            registry_path = self._lifecycle_record(root, lane, lane_id="late-helper", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision)
            record = json.loads(registry_path.read_text(encoding="utf-8"))
            empty = ProcessSnapshot(True, (), (), "synthetic-test")
            new_helper = ProcessInfo(999993, int(record["identities"]["worker"]["pid"]), "helper", "synthetic late helper", datetime(2026, 1, 1, tzinfo=timezone.utc), boundary_id=record["boundary"]["identity"])
            with patch.object(lane_lifecycle, "process_snapshot", side_effect=[empty, ProcessSnapshot(True, (new_helper,), (), "synthetic-test")]):
                result = retire_terminal_lane(
                    lane, root / "archive", lane_id="late-helper",
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("VISIBLE", result.outcome)
            self.assertIsNotNone(result.archive_path)
            self.assertEqual("PENDING", validate_lane_archive(result.archive_path)["close_result"])
            self.assertTrue(lane.exists())
            del main

    def test_FC27_final_publication_and_boundary_unsupported_are_controller_failures(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            with patch.object(lane_controller, "_update_lifecycle_registry", side_effect=lane_controller.LaneLifecycleError("synthetic final publication failure")):
                path = self._lifecycle_record(root, lane, lane_id="publication-failure", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision, expected_code=2)
            failed = json.loads(path.read_text(encoding="utf-8"))
            self.assertFalse(failed["lifecycle"]["complete"])
            self.assertFalse(failed["lifecycle"]["helpers_complete"])
            with patch.object(lane_controller.ProcessBoundary, "prepare", side_effect=lane_controller.ProcessBoundaryUnsupported("synthetic unsupported boundary")):
                unsupported = self._lifecycle_record(root, lane, lane_id="unsupported-boundary", expected_head=revision, retained_ref="refs/heads/main", target_revision=revision, expected_code=1)
            unsupported_record = json.loads(unsupported.read_text(encoding="utf-8"))
            self.assertFalse(unsupported_record["lifecycle"]["complete"])
            self.assertTrue(lane.exists())
            del main

    def test_FC28_distinct_workers_contend_on_one_fixed_production_owner(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            workspace = lane / ".agent-workspace"
            workspace.mkdir(exist_ok=True)
            common = Path(self._git(lane, "rev-parse", "--git-common-dir"))
            if not common.is_absolute():
                common = (lane / common).resolve()
            branch = self._git(lane, "symbolic-ref", "--short", "HEAD")
            repository = {
                "worktree_root": str(lane.resolve()),
                "common_dir": str(common.resolve()),
                "branch": branch,
                "expected_head": revision,
                "starting_head": revision,
                "retained_ref": "refs/heads/main",
                "target_revision": revision,
            }
            from orchestrator_harness.lane_lifecycle import _admit_lifecycle_registry

            original = _admit_lifecycle_registry
            barrier = threading.Barrier(2)
            results: list[object] = []

            def admitted(worker: str) -> None:
                invocation = root / f"{worker}.invocation.json"
                invocation.write_bytes(b"{}\n")
                try:
                    results.append(lane_lifecycle._admit_lifecycle_registry(
                        lane,
                        lane_id="fixed-owner",
                        run_root=lane,
                        invocation_path=invocation,
                        status_path=workspace / f"{worker}.status.json",
                        invocation_schema="orchestrator-coding-invocation/v1",
                        worker_invocation_id=worker,
                        generation=worker,
                        state="RUNNING_CODEX",
                        repository=repository,
                        controller=ProcessInfo(1001 if worker == "worker-a" else 1002, 1, "controller", "controller", datetime(2026, 1, 1, tzinfo=timezone.utc)),
                    ))
                except Exception as exc:
                    results.append(exc)

            with patch.object(lane_lifecycle, "_admit_lifecycle_registry", side_effect=lambda *args, **kwargs: (barrier.wait(timeout=10), original(*args, **kwargs))[1]):
                first = threading.Thread(target=admitted, args=("worker-a",))
                second = threading.Thread(target=admitted, args=("worker-b",))
                first.start(); second.start(); first.join(15); second.join(15)
            self.assertEqual(2, len(results))
            self.assertEqual(1, sum(isinstance(item, object) and not isinstance(item, Exception) for item in results))
            self.assertEqual(1, sum(isinstance(item, Exception) for item in results))
            owner = lifecycle_registry_path(lane, "fixed-owner", "worker-a")
            self.assertEqual(owner, lifecycle_registry_path(lane, "fixed-owner", "worker-b"))
            self.assertTrue(owner.is_file())
            self.assertFalse((owner.parent / "worker-a").exists())
            self.assertFalse((owner.parent / "worker-b").exists())
            del main

    def test_FC29_production_start_then_resume_reuses_one_admitted_generation(self) -> None:
        from orchestrator_harness.tests.test_coding_lane_controller import CodingLaneControllerTests

        case = CodingLaneControllerTests("test_start_records_identity_events_and_configured_codex_argv")
        case.setUp()
        try:
            start, _ = case.invocation(action="start", worker_id="worker-1")
            self.assertEqual(0, lane_controller.main([str(start)]))
            start_status = json.loads((case.workspace / "controller.status.json").read_text(encoding="utf-8"))
            generation = start_status["lifecycle_registry_generation"]
            resume, _ = case.invocation(action="resume", worker_id="worker-1")
            self.assertEqual(0, lane_controller.main([str(resume)]))
            resumed_status = json.loads((case.workspace / "controller.status.json").read_text(encoding="utf-8"))
            record = json.loads(lifecycle_registry_path(case.run_root, "coding:worker-1", "worker-1").read_text(encoding="utf-8"))
            self.assertEqual(generation, resumed_status["lifecycle_registry_generation"])
            self.assertEqual(generation, record["run"]["generation"])
            self.assertTrue(record["lifecycle"]["complete"])
            self.assertTrue(record["lifecycle"]["helpers_complete"])
            lane_lifecycle._load_canonical_lifecycle_registry(case.run_root, lane_id="coding:worker-1")
        finally:
            case.tearDown()

    def test_FC30_linux_escape_and_adoption_inventory_is_not_group_only(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        root = ProcessInfo(10, 1, "provider", "provider", now, 10, 10)
        child = ProcessInfo(11, 10, "helper", "helper", now, 10, 10)
        adopted = ProcessInfo(11, 1, "helper", "helper", now, 99, 99)
        first = process_group_inventory(
            10,
            session_id=10,
            root_pid=10,
            root_identity=root,
            controller_pid=1,
            owned_history=(root,),
            snapshot_provider=lambda: ProcessSnapshot(True, (root, child), (), "synthetic-linux"),
        )
        second = process_group_inventory(
            10,
            session_id=10,
            root_pid=10,
            root_identity=root,
            controller_pid=1,
            owned_history=first.observed_processes,
            snapshot_provider=lambda: ProcessSnapshot(True, (adopted,), (), "synthetic-linux"),
        )
        unobserved = process_group_inventory(
            10,
            session_id=10,
            root_pid=10,
            root_identity=root,
            controller_pid=1,
            owned_history=(root,),
            snapshot_provider=lambda: ProcessSnapshot(True, (adopted,), (), "synthetic-linux"),
        )
        group_only = process_group_inventory(
            10,
            snapshot_provider=lambda: ProcessSnapshot(True, (), (), "synthetic-linux"),
        )
        self.assertTrue(first.complete and second.complete)
        self.assertEqual([11], [item.pid for item in second.processes])
        self.assertFalse(unobserved.complete)
        self.assertFalse(group_only.complete)

    @unittest.skipUnless(
        os.name == "posix" and Path("/proc").is_dir(),
        "real POSIX process-boundary oracle is not supported on this host",
    )
    def test_FC30_real_posix_subprocess_escape_is_owned_and_reaped(self) -> None:
        boundary = ProcessBoundary.prepare()
        helper_code = "import os,time; os.setsid(); time.sleep(30)"
        provider_code = (
            "import subprocess,sys,time\n"
            f"subprocess.Popen([sys.executable, '-c', {helper_code!r}])\n"
            "time.sleep(0.5)\n"
        )
        provider = subprocess.Popen(
            [sys.executable, "-c", provider_code],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **boundary.popen_kwargs,
        )
        supervisor: ProcessSupervisor | None = None
        cleanup_result = None
        try:
            query = targeted_process_query(provider.pid)
            self.assertTrue(query.complete and query.process is not None)
            assert query.process is not None
            boundary.attach(provider, query.process)
            supervisor = ProcessSupervisor(
                provider,
                query.process,
                graceful_timeout_seconds=0.5,
                force_timeout_seconds=0.5,
                boundary=boundary,
            )
            self.assertEqual(0, supervisor.wait_for_exit())
            observed = boundary.inventory()
            self.assertTrue(observed.complete)
            self.assertTrue(any(item.pid != provider.pid for item in observed.processes))
            cleanup_result = supervisor.cleanup()
            self.assertTrue(cleanup_result.proved_reap)
            final = boundary.inventory()
            self.assertTrue(final.complete and not final.processes)
        finally:
            if supervisor is not None and (cleanup_result is None or not cleanup_result.proved_reap):
                try:
                    supervisor.cleanup()
                except Exception:
                    boundary.terminate_owned()
            if provider.poll() is None:
                try:
                    provider.kill()
                    provider.wait(timeout=1)
                except (OSError, subprocess.TimeoutExpired):
                    pass
            boundary.close()

    def test_FC31_retained_live_or_uncertain_helper_blocks_release_and_reclaim(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        helper_created = iso_utc(now)
        owner = ProcessInfo(101, 1, "controller", "controller", now)
        active: dict[int, dict[str, object]] = {
            101: {"pid": 101, "created_utc": "controller-exact"},
            202: {"pid": 202, "created_utc": helper_created},
        }
        with tempfile.TemporaryDirectory() as raw:
            claims = ResourceClaims(
                Path(raw), "lane", "worker", owner,
                process_provider=lambda: ProcessSnapshot(True, (), (), "synthetic"),
                identity_provider=lambda pid: active.get(pid),
            )
            claims.acquire_all(["resource"], on_wait=lambda wait: self.fail(str(wait)))
            self.assertEqual([], claims.retain_boundary(
                boundary={"complete": True, "identity": "boundary"},
                identities=[{"pid": 202, "created_utc": helper_created, "creation_identity": helper_created}],
            ))
            active.pop(101)
            live = ProcessSnapshot(True, (ProcessInfo(202, 1, "helper", "helper", now),), (), "synthetic")
            incomplete = ProcessSnapshot(False, (), ("query incomplete",), "synthetic")
            self.assertEqual("RETAINED_PROCESS_LIVE", _owner_state(claims.held[0], live, lambda pid: active.get(pid))[0])
            self.assertEqual("INVENTORY_UNKNOWN", _owner_state(claims.held[0], incomplete, lambda pid: active.get(pid))[0])

    def test_FC32_exact_absence_reclaims_retained_claim_once(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        helper_created = iso_utc(now)
        owner = ProcessInfo(101, 1, "controller", "controller", now)
        active: dict[int, dict[str, object]] = {
            101: {"pid": 101, "created_utc": "controller-exact"},
            202: {"pid": 202, "created_utc": helper_created},
        }
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            claims = ResourceClaims(root, "lane", "worker", owner, process_provider=lambda: ProcessSnapshot(True, (), (), "synthetic"), identity_provider=lambda pid: active.get(pid))
            claims.acquire_all(["resource"], on_wait=lambda wait: self.fail(str(wait)))
            claims.retain_boundary(boundary={"complete": True, "identity": "boundary"}, identities=[{"pid": 202, "created_utc": helper_created, "creation_identity": helper_created}])
            active.pop(101); active.pop(202)
            contender = ResourceClaims(
                root, "lane", "contender", ProcessInfo(303, 1, "controller", "controller", now),
                process_provider=lambda: ProcessSnapshot(True, (), (), "synthetic"),
                identity_provider=lambda pid: active.get(pid) or ({"pid": 303, "created_utc": "contender-exact"} if pid == 303 else None),
            )
            waits: list[dict[str, object]] = []
            contender.acquire_all(["resource"], on_wait=waits.append)
            self.assertEqual(["resource"], [item["resource"] for item in contender.held])
            self.assertTrue(any(item.get("state") == "PROVEN_STALE" for item in contender.findings))
            self.assertEqual([], contender.release_all())

    def test_FC33_final_live_reinventory_revokes_release_and_retains_claim(self) -> None:
        helper = ProcessInfo(
            303, 202, "helper", "helper", datetime(2026, 8, 11, 0, 0, 2, tzinfo=timezone.utc)
        )
        final = ProcessBoundaryInventory(
            True,
            "synthetic-boundary",
            "synthetic-boundary:1",
            (helper,),
            (helper,),
            source="synthetic-final",
        )
        result, claims, releases, retains, _ = self._run_synthetic_controller_boundary(final)
        self.assertEqual(1, result)
        self.assertEqual([], releases)
        self.assertTrue(retains)
        self.assertTrue(claims.held)
        self.assertEqual([303], [item["pid"] for item in claims.held[0]["retained_processes"]])

    def test_FC34_final_incomplete_reinventory_revokes_release_and_retains_claim(self) -> None:
        final = ProcessBoundaryInventory(
            False,
            "synthetic-boundary",
            "synthetic-boundary:1",
            errors=("synthetic final snapshot incomplete",),
            source="synthetic-final",
        )
        result, claims, releases, retains, _ = self._run_synthetic_controller_boundary(final)
        self.assertEqual(1, result)
        self.assertEqual([], releases)
        self.assertTrue(retains)
        self.assertFalse(claims.held[0]["retained_boundary"]["complete"])
        thrown_result, thrown_claims, thrown_releases, thrown_retains, _ = self._run_synthetic_controller_boundary(
            None,
            final_inventory_error=RuntimeError("synthetic final inventory error"),
        )
        self.assertEqual(1, thrown_result)
        self.assertEqual([], thrown_releases)
        self.assertTrue(thrown_retains)
        self.assertFalse(thrown_claims.held[0]["retained_boundary"]["complete"])

    def test_FC35_failed_retention_preserves_armed_claim_against_contender(self) -> None:
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        active: dict[int, dict[str, object]] = {
            101: {"pid": 101, "created_utc": "identity:101"},
            303: {"pid": 303, "created_utc": "identity:303"},
        }
        owner = ProcessInfo(101, 1, "controller", "controller", now)
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            claims = ResourceClaims(
                root,
                "lane",
                "worker",
                owner,
                process_provider=lambda: ProcessSnapshot(True, (), (), "synthetic"),
                identity_provider=lambda pid: active.get(pid),
            )
            claims.acquire_all(["resource"], on_wait=lambda wait: self.fail(str(wait)))
            self.assertEqual([], claims.arm_boundary())
            with patch("orchestrator_harness.resource_locks.mutation_replace", side_effect=OSError("synthetic retention write failure")):
                self.assertEqual(
                    ["resource"],
                    claims.retain_boundary(
                        boundary={"complete": False, "errors": ["uncertain"]},
                        identities=[],
                    ),
                )
            active.pop(101)
            contender = ResourceClaims(
                root,
                "lane",
                "contender",
                ProcessInfo(303, 1, "controller", "controller", now),
                process_provider=lambda: ProcessSnapshot(True, (), (), "synthetic"),
                identity_provider=lambda pid: active.get(pid),
            )
            waits: list[dict[str, object]] = []

            class WaitAbort(Exception):
                pass

            with patch("orchestrator_harness.resource_locks.time.sleep", side_effect=WaitAbort()):
                with self.assertRaises(WaitAbort):
                    contender.acquire_all(["resource"], on_wait=waits.append)
            self.assertEqual("INVENTORY_UNKNOWN", waits[0]["state"])
            self.assertTrue(claims.held[0]["boundary_may_exist"])

    def test_FC36_prelaunch_arming_failure_forbids_provider_popen(self) -> None:
        empty = ProcessBoundaryInventory(True, "synthetic-boundary", "synthetic-boundary:1", source="synthetic")
        result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
            empty,
            arm_failures=["resource"],
        )
        self.assertEqual(1, result)
        self.assertEqual(0, popen.call_count)
        self.assertEqual([None], releases)
        self.assertEqual([], retains)
        self.assertEqual([], claims.held)

    def test_FC37_windows_job_assigned_and_list_mismatch_is_incomplete(self) -> None:
        from ctypes import wintypes

        for assigned, listed in ((1, 0), (4, 2)):
            with self.subTest(assigned=assigned, listed=listed):
                calls = 0

                def query(
                    _handle: object,
                    _info_class: int,
                    buffer: object,
                    _size: int,
                    returned: object,
                    *,
                    assigned_value: int = assigned,
                    listed_value: int = listed,
                ) -> bool:
                    nonlocal calls
                    calls += 1
                    payload = assigned_value.to_bytes(4, "little") + listed_value.to_bytes(4, "little")
                    ctypes.memmove(buffer, payload, len(payload))
                    returned._obj.value = len(payload)  # type: ignore[attr-defined]
                    return True

                boundary = ProcessBoundary(kind="windows-job", identity="job:repair")
                boundary._job_handle = object()
                with patch(
                    "orchestrator_harness.process_supervisor._windows_job_api",
                    return_value=(None, None, None, query, None, None, None, wintypes),
                ):
                    observed = boundary.inventory()
                self.assertFalse(observed.complete)
                self.assertTrue(any("assigned=" in error and "listed=" in error for error in observed.errors))
                self.assertGreaterEqual(calls, 1)

    def test_FC38_no_repository_attach_failure_never_uses_direct_reap_for_release(self) -> None:
        incomplete = ProcessBoundaryInventory(
            False,
            "synthetic-boundary",
            "synthetic-boundary:1",
            errors=("attach left boundary partial",),
            source="synthetic-final",
        )
        result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
            incomplete,
            attach_error=lane_controller.ProcessBoundaryUnsupported("synthetic attach failure"),
        )
        self.assertEqual(1, result)
        self.assertEqual(1, popen.call_count)
        self.assertEqual([], releases)
        self.assertTrue(retains)
        self.assertTrue(claims.held)

    def test_FC39_launcher_filter_is_presentation_only(self) -> None:
        root = ProcessInfo(10, 1, "provider", "python provider.py", datetime(2026, 8, 11, tzinfo=timezone.utc))
        same_tail = ProcessInfo(11, 10, "launcher", "python provider.py", datetime(2026, 8, 11, 0, 0, 1, tzinfo=timezone.utc))
        empty_command = ProcessInfo(12, 10, "launcher", "", datetime(2026, 8, 11, 0, 0, 2, tzinfo=timezone.utc))
        real_helper = ProcessInfo(13, 11, "helper", "python helper.py", datetime(2026, 8, 11, 0, 0, 3, tzinfo=timezone.utc))
        self.assertTrue(lane_controller._is_launcher_descendant(same_tail, root, provider_root_pid=root.pid))
        self.assertTrue(lane_controller._is_launcher_descendant(empty_command, root, provider_root_pid=root.pid))
        boundary = ProcessBoundary(kind="synthetic-boundary", identity="synthetic-boundary:1")
        record = boundary.to_record(
            ProcessBoundaryInventory(
                True,
                boundary.kind,
                boundary.identity,
                (same_tail, empty_command, real_helper),
                (same_tail, empty_command, real_helper),
                source="synthetic-authoritative",
            )
        )
        self.assertEqual({11, 12, 13}, {item["pid"] for item in record["members"]})
        self.assertEqual({11, 12, 13}, {item["pid"] for item in record["live_members"]})

    def test_FC40_post_popen_keyboard_interrupt_is_postlaunch_and_fail_closed(self) -> None:
        final = ProcessBoundaryInventory(
            False,
            "synthetic-boundary",
            "synthetic-boundary:1",
            errors=("post-Popen synthetic final inventory incomplete",),
            source="synthetic-final",
        )
        result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
            final,
            post_popen_error=KeyboardInterrupt(),
            real_supervisor=True,
        )
        self.assertEqual(130, result)
        self.assertEqual(1, popen.call_count)
        self.assertEqual([], releases)
        self.assertTrue(retains)
        self.assertTrue(claims.held)

    def test_FC41_post_popen_exception_routes_and_uncaught_baseexception_retain(self) -> None:
        final = ProcessBoundaryInventory(
            False,
            "synthetic-boundary",
            "synthetic-boundary:1",
            errors=("post-Popen synthetic final inventory incomplete",),
            source="synthetic-final",
        )
        for error in (RuntimeError("ordinary post-Popen failure"), ResourceLockError("post-Popen lock failure")):
            with self.subTest(error=type(error).__name__):
                result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
                    final,
                    post_popen_error=error,
                    real_supervisor=True,
                )
                self.assertEqual(1, result)
                self.assertEqual(1, popen.call_count)
                self.assertEqual([], releases)
                self.assertTrue(retains)
                self.assertTrue(claims.held)
        captured: dict[str, BaseException] = {}
        result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
            final,
            post_popen_error=BaseException("uncaught post-Popen failure"),
            captured_exception=captured,
            real_supervisor=True,
        )
        self.assertEqual(1, result)
        self.assertIsInstance(captured.get("exception"), BaseException)
        self.assertEqual(1, popen.call_count)
        self.assertEqual([], releases)
        self.assertTrue(retains)
        self.assertTrue(claims.held)

    def test_FC42_windows_job_equal_counts_require_returned_pid_bytes(self) -> None:
        from ctypes import wintypes

        for returned_mode in ("header-only", "beyond-allocation"):
            with self.subTest(returned_mode=returned_mode):
                calls = 0

                def query(
                    _handle: object,
                    _info_class: int,
                    buffer: object,
                    size: int,
                    returned: object,
                ) -> bool:
                    nonlocal calls
                    calls += 1
                    payload = (1).to_bytes(4, "little") + (1).to_bytes(4, "little")
                    ctypes.memmove(buffer, payload, len(payload))
                    returned._obj.value = 8 if returned_mode == "header-only" else size + 1  # type: ignore[attr-defined]
                    return True

                boundary = ProcessBoundary(kind="windows-job", identity="job:repair")
                boundary._job_handle = object()
                with patch(
                    "orchestrator_harness.process_supervisor._windows_job_api",
                    return_value=(None, None, None, query, None, None, None, wintypes),
                ):
                    observed = boundary.inventory()
                self.assertFalse(observed.complete)
                self.assertTrue(any("returned" in error or "truncated" in error for error in observed.errors))
                self.assertEqual(1, calls)

    def test_FC43_windows_job_retry_policy_is_bounded_for_oversized_and_changing_counts(self) -> None:
        from ctypes import wintypes

        calls = 0

        def oversized_query(
            _handle: object,
            _info_class: int,
            buffer: object,
            _size: int,
            returned: object,
        ) -> bool:
            nonlocal calls
            calls += 1
            payload = (65537).to_bytes(4, "little") + (65537).to_bytes(4, "little")
            ctypes.memmove(buffer, payload, len(payload))
            returned._obj.value = len(payload)  # type: ignore[attr-defined]
            return True

        boundary = ProcessBoundary(kind="windows-job", identity="job:repair")
        boundary._job_handle = object()
        with patch(
            "orchestrator_harness.process_supervisor._windows_job_api",
            return_value=(None, None, None, oversized_query, None, None, None, wintypes),
        ):
            observed = boundary.inventory()
        self.assertFalse(observed.complete)
        self.assertLessEqual(calls, 8)
        self.assertTrue(any("supported" in error or "capacity" in error for error in observed.errors))

        changing_calls = 0
        changing = iter(((1, 0), (4, 2), (8, 4), (16, 8), (32, 16), (64, 32), (128, 64), (256, 128)))

        def changing_query(
            _handle: object,
            _info_class: int,
            buffer: object,
            _size: int,
            returned: object,
        ) -> bool:
            nonlocal changing_calls
            changing_calls += 1
            assigned, listed = next(changing, (512, 256))
            payload = assigned.to_bytes(4, "little") + listed.to_bytes(4, "little")
            ctypes.memmove(buffer, payload, len(payload))
            returned._obj.value = 8 + ctypes.sizeof(ctypes.c_void_p) * listed  # type: ignore[attr-defined]
            return True

        boundary = ProcessBoundary(kind="windows-job", identity="job:repair-changing")
        boundary._job_handle = object()
        with patch(
            "orchestrator_harness.process_supervisor._windows_job_api",
            return_value=(None, None, None, changing_query, None, None, None, wintypes),
        ):
            observed = boundary.inventory()
        self.assertFalse(observed.complete)
        self.assertLessEqual(changing_calls, 8)
        self.assertTrue(any("inconsistent" in error or "retry" in error for error in observed.errors))

    def test_FC44_identity_uncertain_supervisor_still_runs_direct_handle_cleanup(self) -> None:
        final = ProcessBoundaryInventory(
            False,
            "synthetic-boundary",
            "synthetic-boundary:1",
            errors=("identity-seam final inventory is incomplete",),
            source="synthetic-final",
        )
        failures: tuple[BaseException, ...] = (
            KeyboardInterrupt(),
            RuntimeError("ordinary identity failure"),
            ResourceLockError("resource identity failure"),
            BaseException("uncaught identity failure"),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                captured: dict[str, BaseException] = {}
                result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
                    final,
                    post_popen_error=failure,
                    captured_exception=captured if type(failure) is BaseException else None,
                    real_supervisor=True,
                )
                self.assertEqual(130 if isinstance(failure, KeyboardInterrupt) else 1, result)
                if type(failure) is BaseException:
                    self.assertIsInstance(captured.get("exception"), BaseException)
                process = popen.return_value
                self.assertEqual(["poll", "terminate", "wait"], process.calls)
                self.assertEqual([], releases)
                self.assertTrue(retains)
                self.assertTrue(claims.held)
                status = claims._test_status  # type: ignore[attr-defined]
                direct = status["direct_handle_cleanup"]
                self.assertTrue(direct["final_reap"])
                self.assertTrue(direct["identity_uncertain"])
                self.assertFalse(status["resource_claim_release_safe"])

    def test_FC45_identity_uncertain_handle_cleanup_is_bounded_and_truthful(self) -> None:
        final = ProcessBoundaryInventory(
            False,
            "synthetic-boundary",
            "synthetic-boundary:1",
            errors=("identity-seam final inventory is incomplete",),
            source="synthetic-final",
        )
        cases: tuple[tuple[str, dict[str, object], set[str]], ...] = (
            (
                "terminate-wait-timeout",
                {"wait_plan": [subprocess.TimeoutExpired("codex", 5.0), 137]},
                {"kill"},
            ),
            ("poll-failure", {"poll_error": RuntimeError("poll failed")}, set()),
            ("terminate-failure", {"terminate_error": RuntimeError("terminate failed")}, set()),
            (
                "wait-failure",
                {"wait_plan": [RuntimeError("wait failed"), 137]},
                {"kill"},
            ),
            (
                "kill-failure",
                {
                    "wait_plan": [subprocess.TimeoutExpired("codex", 5.0), RuntimeError("final wait failed")],
                    "kill_error": RuntimeError("kill failed"),
                },
                {"kill"},
            ),
        )
        for name, plan, required in cases:
            with self.subTest(case=name):
                result, claims, releases, retains, popen = self._run_synthetic_controller_boundary(
                    final,
                    post_popen_error=RuntimeError("identity lookup failed"),
                    real_supervisor=True,
                    **plan,
                )
                self.assertEqual(1, result)
                process = popen.return_value
                self.assertEqual(1, process.calls.count("poll"))
                self.assertEqual(1, process.calls.count("terminate"))
                self.assertLessEqual(process.calls.count("wait"), 2)
                self.assertLessEqual(process.calls.count("kill"), 1)
                self.assertTrue(required.issubset(process.calls))
                self.assertEqual([], releases)
                self.assertTrue(retains)
                self.assertTrue(claims.held)
                status = claims._test_status  # type: ignore[attr-defined]
                direct = status["direct_handle_cleanup"]
                self.assertTrue(direct["identity_uncertain"])
                self.assertTrue(direct["errors"])
                self.assertFalse(status["resource_claim_release_safe"])

    def test_FC22_controller_registry_binds_zero_one_and_multiple_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            runtime = self._lifecycle_record(
                root, lane, lane_id="helpers", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
                helpers=[
                    {"name": "helper-a", "identity": {"pid": 9301, "created_utc": "2000-01-01T00:00:00Z"}},
                    {"name": "helper-b", "identity": {"pid": 9302, "created_utc": "2000-01-01T00:00:00Z"}},
                ],
            )
            value = json.loads(lifecycle_registry_path(lane, "helpers", "worker-helpers").read_text(encoding="utf-8"))
            self.assertEqual(2, len(value["identities"]["helpers"]))
            self.assertTrue(all(item["pid"] > 0 and item["created_utc"] for item in value["identities"]["helpers"]))
            self.assertTrue(value["boundary"]["complete"])
            self.assertEqual("job-object+CIM" if os.name == "nt" else "/proc", value["boundary"]["inventory_source"])
            with patch.object(
                lane_lifecycle,
                "process_snapshot",
                return_value=ProcessSnapshot(True, (), (), "synthetic-test"),
            ):
                result = retire_terminal_lane(
                    lane, root / "archive", lane_id="helpers",
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("CLOSED", result.outcome)
            del main

    def test_FC19_post_final_target_proof_never_overwrites_new_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            hooks = project / ".codex" / "hooks"
            hooks.mkdir(parents=True)
            target = hooks / "late-target.txt"
            target.write_bytes(b"authorized-before\n")
            guard = codex_adapter._ProjectMutationGuard(project, prepare_codex=True)

            def change_after_final_proof() -> None:
                target.write_bytes(b"user-after-final-proof\n")

            with patch(
                "orchestrator_harness.mutation._after_target_proof",
                side_effect=change_after_final_proof,
                create=True,
            ):
                with self.assertRaises(CodexInstallConflict):
                    guard.atomic_replace(target.relative_to(project), b"must-not-win\n")
            self.assertEqual(b"user-after-final-proof\n", target.read_bytes())

    def test_FC19_post_final_target_proof_never_deletes_new_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            hooks = project / ".codex" / "hooks"
            hooks.mkdir(parents=True)
            target = hooks / "late-delete.txt"
            target.write_bytes(b"authorized-before\n")
            guard = codex_adapter._ProjectMutationGuard(project, prepare_codex=True)

            def change_after_final_proof() -> None:
                target.write_bytes(b"user-after-final-proof\n")

            with patch(
                "orchestrator_harness.mutation._after_target_proof",
                side_effect=change_after_final_proof,
                create=True,
            ):
                with self.assertRaises(CodexInstallConflict):
                    guard.delete(Path(".codex/hooks/late-delete.txt"))
            self.assertEqual(b"user-after-final-proof\n", target.read_bytes())

    def test_FC20_post_anchor_parent_relocation_never_writes_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            hooks = project / ".codex" / "hooks"
            hooks.mkdir(parents=True)
            outside = root / "outside"
            outside.mkdir()
            moved = outside / "original-hooks"
            guard = codex_adapter._ProjectMutationGuard(project, prepare_codex=True)

            def relocate_after_anchor() -> None:
                hooks.rename(moved)
                result = subprocess.run(
                    ["cmd.exe", "/c", "mklink", "/J", str(hooks), str(outside)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if result.returncode != 0:
                    raise AssertionError(f"junction fixture failed: {result.stderr}")

            try:
                with patch(
                    "orchestrator_harness.mutation._after_anchor",
                    side_effect=relocate_after_anchor,
                    create=True,
                ):
                    with self.assertRaises(CodexInstallConflict):
                        guard.atomic_replace(Path(".codex/hooks/outside.txt"), b"must-stay-inside\n")
                self.assertFalse((outside / "outside.txt").exists())
                self.assertTrue(project.exists())
            finally:
                if hooks.exists() or os.path.lexists(hooks):
                    hooks.rmdir()
                if moved.exists():
                    moved.rename(hooks)

    def test_FC21_append_lock_substitution_never_creates_lock_or_payload_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            outside = root / "outside"
            outside.mkdir()
            target = project / "events.jsonl"
            lock_root = project / "locks"
            lock_root.mkdir()
            moved = outside / "original-locks"

            def substitute_lock_root() -> None:
                lock_root.rename(moved)
                result = subprocess.run(
                    ["cmd.exe", "/c", "mklink", "/J", str(lock_root), str(outside)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if result.returncode != 0:
                    raise AssertionError(f"junction fixture failed: {result.stderr}")

            try:
                lock = PathKeyedAppendLock(target, lock_root=lock_root)
                with patch(
                    "orchestrator_harness.mutation._after_anchor",
                    side_effect=substitute_lock_root,
                    create=True,
                ):
                    with self.assertRaises(AppendLockError):
                        with lock:
                            target.write_bytes(b"must-not-bypass\n")
                self.assertFalse(any(outside.glob("*.lock")))
                self.assertFalse((outside / "events.jsonl").exists())
            finally:
                if lock_root.exists() or os.path.lexists(lock_root):
                    lock_root.rmdir()
                if moved.exists():
                    moved.rename(lock_root)

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
            refs = self._archive_refs(root)
            self._lifecycle_record(
                root, lane, lane_id="repair", expected_head=authorized,
                retained_ref="refs/heads/main", target_revision=authorized,
            )
            (lane / "changed-after-auth.txt").write_text("changed\n", encoding="utf-8")
            self._git(lane, "add", ".")
            self._git(lane, "commit", "-m", "change after authorization")
            actual_head = self._git(lane, "rev-parse", "HEAD")
            self.assertNotEqual(authorized, actual_head)
            result = retire_terminal_lane(
                lane,
                root / "archive",
                lane_id="repair",
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
            registry = self._lifecycle_record(
                root, lane, lane_id="repair", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
                worktree=foreign,
            )
            value = json.loads(registry.read_text(encoding="utf-8"))
            value["repository"]["worktree_root"] = str(foreign.resolve())
            value["record_sha256"] = lane_lifecycle._registry_digest(value)
            registry.write_text(json.dumps(value) + "\n", encoding="utf-8")
            result = retire_terminal_lane(
                lane,
                root / "archive",
                lane_id="repair",
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
            runtime = self._lifecycle_record(
                root, lane, lane_id="live", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
            )
            live_record = json.loads(runtime.read_text(encoding="utf-8"))
            live_created = datetime.fromisoformat(live_record["identities"]["controller"]["created_utc"].replace("Z", "+00:00"))
            provider = MagicMock(return_value=ProcessSnapshot(
                complete=True,
                processes=(ProcessInfo(
                    pid=live_record["identities"]["controller"]["pid"],
                    ppid=1,
                    name="python",
                    command_line="synthetic live controller",
                    created_utc=live_created,
                ),),
                provider="synthetic-test",
            ))
            with patch.object(lane_lifecycle, "process_snapshot", provider, create=True):
                result = retire_terminal_lane(
                    lane,
                    root / "archive",
                    lane_id="live",
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
            self._lifecycle_record(
                root, lane, lane_id="repair", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
            )
            with patch.object(
                lane_lifecycle,
                "process_snapshot",
                return_value=ProcessSnapshot(True, (), (), "synthetic-test"),
            ):
                result = retire_terminal_lane(
                    lane,
                    root / "archive",
                    lane_id="repair",
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("CLOSED", result.outcome)
            for path in refs:
                path.unlink(missing_ok=True)
            self.assertEqual("repair", validate_lane_archive(result.archive_path)["lane_id"])
            del main

    def test_FC8_unretained_and_archive_incomplete_lanes_stay_visible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            main, lane, revision = self._git_fixture(root)
            refs = self._archive_refs(root)
            unretained_registry = self._lifecycle_record(
                root, lane, lane_id="unretained", expected_head=revision,
                retained_ref="refs/heads/does-not-exist", target_revision=revision,
            )
            unretained_record = json.loads(unretained_registry.read_text(encoding="utf-8"))
            unretained_record["repository"]["retained_ref"] = "refs/heads/does-not-exist"
            unretained_record["record_sha256"] = lane_lifecycle._registry_digest(unretained_record)
            unretained_registry.write_text(json.dumps(unretained_record) + "\n", encoding="utf-8")
            blocked = retire_terminal_lane(
                lane,
                root / "archive-unretained",
                lane_id="unretained",
                task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", blocked.outcome)
            self.assertTrue(lane.exists())
            incomplete = retire_terminal_lane(
                lane,
                root / "archive-incomplete",
                lane_id="incomplete",
                task_ref=None, result_ref=refs[1], findings_ref=refs[2],
                acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
            )
            self.assertEqual("VISIBLE", incomplete.outcome)
            self.assertTrue(lane.exists())
            live_runtime = self._lifecycle_record(
                root, lane, lane_id="live", expected_head=revision,
                retained_ref="refs/heads/main", target_revision=revision,
            )
            live_record = json.loads(live_runtime.read_text(encoding="utf-8"))
            live_controller = live_record["identities"]["controller"]
            live_created = datetime.fromisoformat(live_controller["created_utc"].replace("Z", "+00:00"))
            with patch.object(
                lane_lifecycle,
                "process_snapshot",
                return_value=ProcessSnapshot(
                    True,
                    (ProcessInfo(live_controller["pid"], 1, "python", "synthetic live controller", live_created),),
                    (),
                    "synthetic-test",
                ),
            ):
                live = retire_terminal_lane(
                    lane,
                    root / "archive-live",
                    lane_id="live",
                    task_ref=refs[0], result_ref=refs[1], findings_ref=refs[2],
                    acceptance_ref=refs[3], transcript_ref=refs[4], dependency_ref=refs[5],
                )
            self.assertEqual("VISIBLE", live.outcome)
            self.assertEqual("LIVE_USE_PROVEN", live.reason)
            (lane / "unmerged.txt").write_text("unmerged\n", encoding="utf-8")
            self._git(lane, "add", ".")
            self._git(lane, "commit", "-m", "unmerged")
            lane_revision = self._git(lane, "rev-parse", "HEAD")
            unmerged_registry = self._lifecycle_record(
                root, lane, lane_id="unmerged", expected_head=lane_revision,
                retained_ref="HEAD", target_revision=revision,
            )
            unmerged_record = json.loads(unmerged_registry.read_text(encoding="utf-8"))
            unmerged_record["repository"]["target_revision"] = revision
            unmerged_record["record_sha256"] = lane_lifecycle._registry_digest(unmerged_record)
            unmerged_registry.write_text(json.dumps(unmerged_record) + "\n", encoding="utf-8")
            unmerged = retire_terminal_lane(
                lane,
                root / "archive-unmerged",
                lane_id="unmerged",
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
