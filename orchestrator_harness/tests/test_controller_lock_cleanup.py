from __future__ import annotations

import io
import json
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.models import ProcessInfo
from orchestrator_harness.resource_locks import ResourceClaims, claim_filename
from orchestrator_harness.workspace_overlay import (
    SUPER_CACHE_NAME,
    ingest_super_cache,
    prepare_worktree,
)


NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)


class UnreapableChild:
    pid = 202

    def __init__(self) -> None:
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO()
        self.stderr = io.BytesIO()
        self.terminated = False
        self.killed = False

    def poll(self) -> None:
        return None

    def wait(self, timeout: float | None = None) -> int:
        if timeout is None:
            raise RuntimeError("child wait interrupted")
        raise subprocess.TimeoutExpired("codex", timeout)

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


class ReapedAfterKillChild(UnreapableChild):
    def wait(self, timeout: float | None = None) -> int:
        if timeout is None:
            raise RuntimeError("child wait interrupted")
        if self.killed:
            return 137
        raise subprocess.TimeoutExpired("codex", timeout)


class ControllerLockCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.overlay_temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        overlay_root = Path(self.overlay_temporary.name)
        overlay_source = overlay_root / "source"
        overlay_source.mkdir()
        overlay_harness = overlay_root / "harness"
        overlay_harness.mkdir()
        ingest_super_cache(
            source_folder=overlay_source, harness_worktree=overlay_harness
        )
        self.overlay_receipt = self.workspace / "overlay-receipt.json"
        prepare_worktree(
            super_cache=overlay_harness / SUPER_CACHE_NAME,
            target_worktree=self.root,
            role="subagent",
            receipt_path=self.overlay_receipt,
        )

    def tearDown(self) -> None:
        self.overlay_temporary.cleanup()
        self.temporary.cleanup()

    def invocation(self) -> controller.Invocation:
        prompt = b"focused test\n"
        return controller.Invocation(
            controller.CODING_INVOCATION_SCHEMA,
            "worker-1",
            "start",
            self.root,
            self.workspace,
            self.root / "prompt.md",
            "0" * 64,
            prompt,
            None,
            None,
            "worker-1",
            "coding:worker-1",
            "test",
            "implementation",
            "coding:worker-1",
            [],
            [],
            [],
            {},
            ["named-resource"],
            self.runtime / "claims",
            "test-model",
            "low",
            "priority",
            ["codex"],
            [],
            "workspace-write",
            "never",
            None,
            None,
            self.workspace / "status.json",
            self.workspace / "output.jsonl",
            self.workspace / "stderr.log",
            self.workspace / "last-message.txt",
            self.runtime / "events.jsonl",
            overlay_receipt=self.overlay_receipt,
        )

    def test_unproven_child_shutdown_retains_owned_claim(self) -> None:
        invocation = self.invocation()
        owner = ProcessInfo(101, 1, "controller", "controller", NOW)
        child_identity = ProcessInfo(202, 101, "codex", "codex", NOW)
        child = UnreapableChild()
        captured: list[ResourceClaims] = []
        release_calls: list[None] = []

        def claims_factory(
            root: Path,
            lane_id: str,
            worker_id: str,
            process: ProcessInfo,
        ) -> ResourceClaims:
            claims = ResourceClaims(
                root,
                lane_id,
                worker_id,
                process,
                identity_provider=lambda pid: {
                    "pid": pid,
                    "created_utc": f"identity:{pid}",
                },
            )
            original_release_all = claims.release_all

            def tracked_release_all() -> list[str]:
                release_calls.append(None)
                return original_release_all()

            claims.release_all = tracked_release_all
            captured.append(claims)
            return claims

        with (
            mock.patch.object(
                controller, "_identity", side_effect=[owner, child_identity]
            ),
            mock.patch.object(controller, "ResourceClaims", side_effect=claims_factory),
            mock.patch.object(controller.subprocess, "Popen", return_value=child),
        ):
            self.assertEqual(1, controller.run(invocation))

        claims = captured[0]
        lock_root = invocation.resource_lock_root
        assert lock_root is not None
        claim_path = lock_root / claim_filename("named-resource")
        self.assertTrue(claim_path.is_file())
        self.assertEqual(
            ["named-resource"], [claim["resource"] for claim in claims.held]
        )
        self.assertEqual([], release_calls)
        self.assertTrue(child.terminated)
        self.assertTrue(child.killed)

        status = json.loads(invocation.status_path.read_text(encoding="utf-8"))
        self.assertEqual("COORDINATION_FAILED", status["state"])
        self.assertEqual(claims.held, status["held_resource_claims"])
        self.assertTrue(
            status["coordination_failure"]["child_shutdown"]["terminate_wait_timed_out"]
        )
        self.assertTrue(
            status["coordination_failure"]["child_shutdown"]["kill_wait_timed_out"]
        )
        events = [
            json.loads(line)
            for line in invocation.event_log.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual("COORDINATION_FAILED", events[-1]["event"])
        self.assertEqual(claims.held, events[-1]["retained_claims"])

    def test_proven_kill_and_reap_releases_owned_claim(self) -> None:
        invocation = self.invocation()
        owner = ProcessInfo(101, 1, "controller", "controller", NOW)
        child_identity = ProcessInfo(202, 101, "codex", "codex", NOW)
        child = ReapedAfterKillChild()
        captured: list[ResourceClaims] = []
        release_calls: list[None] = []

        def claims_factory(
            root: Path,
            lane_id: str,
            worker_id: str,
            process: ProcessInfo,
        ) -> ResourceClaims:
            claims = ResourceClaims(
                root,
                lane_id,
                worker_id,
                process,
                identity_provider=lambda pid: {
                    "pid": pid,
                    "created_utc": f"identity:{pid}",
                },
            )
            original_release_all = claims.release_all
            claims.release_all = lambda: (
                release_calls.append(None) or original_release_all()
            )
            captured.append(claims)
            return claims

        with (
            mock.patch.object(
                controller, "_identity", side_effect=[owner, child_identity]
            ),
            mock.patch.object(controller, "ResourceClaims", side_effect=claims_factory),
            mock.patch.object(controller.subprocess, "Popen", return_value=child),
        ):
            self.assertEqual(1, controller.run(invocation))

        lock_root = invocation.resource_lock_root
        assert lock_root is not None
        claim_path = lock_root / claim_filename("named-resource")
        self.assertTrue(child.killed)
        self.assertEqual([None], release_calls)
        self.assertFalse(claim_path.exists())
        self.assertEqual([], captured[0].held)


if __name__ == "__main__":
    unittest.main()
