from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import orchestrator_harness.lane_controller as controller
from orchestrator_harness import cli
from orchestrator_harness.lane_lifecycle import lifecycle_registry_path
from orchestrator_harness.tests.support import (
    TemporaryGitRepository,
    prepare_fixture_overlay_receipt,
)

FAKE_CODEX = r"""
import json, os, sys
argv = sys.argv[1:]
capture = os.environ.get("CODING_CONTROLLER_CAPTURE")
if capture:
    open(capture, "w", encoding="utf-8").write(json.dumps(argv))
secret_capture = os.environ.get("CODING_CONTROLLER_SECRET_CAPTURE")
if secret_capture:
    open(secret_capture, "w", encoding="utf-8").write(
        os.environ.get("ORCHESTRATOR_ROOT_ADJUDICATION_SECRET", "<missing>")
    )
sys.stdin.read()
if os.environ.get("CODING_CONTROLLER_NO_THREAD") != "1":
    print(json.dumps({"type": "thread.started", "thread_id": os.environ.get("CODING_CONTROLLER_THREAD", "coding-thread")}), flush=True)
print(json.dumps({"type": "turn.completed"}), flush=True)
raise SystemExit(int(os.environ.get("CODING_CONTROLLER_EXIT", "0")))
"""


class CodingLaneControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run_root = self.root / "run"
        self.repository = TemporaryGitRepository.create(self.run_root)
        self.workspace = self.run_root / ".agent-workspace"
        self.workspace.mkdir(parents=True)
        self.runtime_root = self.root / "runtime"
        self.runtime_root.mkdir()
        self.prompt = self.run_root / "prompt.md"
        self.prompt.write_text("Implement the focused change.\n", encoding="utf-8")
        self.fake = self.root / "fake_codex.py"
        self.fake.write_text(FAKE_CODEX, encoding="utf-8")
        self.capture = self.root / "argv.json"

    def tearDown(self) -> None:
        for key in (
            "CODING_CONTROLLER_CAPTURE",
            "CODING_CONTROLLER_NO_THREAD",
            "CODING_CONTROLLER_THREAD",
            "CODING_CONTROLLER_EXIT",
            "CODING_CONTROLLER_SECRET_CAPTURE",
            controller.ROOT_ADJUDICATION_SECRET_ENV,
        ):
            os.environ.pop(key, None)
        self.temporary.cleanup()

    def invocation(
        self,
        *,
        action: str = "start",
        worker_id: str = "worker-1",
        with_overlay: bool = False,
    ) -> tuple[Path, dict[str, object]]:
        outputs = {
            "status": str(self.workspace / "controller.status.json"),
            "jsonl": str(self.workspace / "codex.jsonl"),
            "stderr": str(self.workspace / "codex.stderr.log"),
            "last_message": str(self.workspace / "last-message.txt"),
        }
        value: dict[str, object] = {
            "schema": controller.CODING_INVOCATION_SCHEMA,
            "action": action,
            "run_root": str(self.run_root),
            "runtime_root": str(self.runtime_root),
            "event_log_path": str(self.runtime_root / "events" / "controller.jsonl"),
            "worker_invocation_id": worker_id,
            "lane_id": "coding:worker-1",
            "task": "focused test",
            "phase": "implementation",
            "prompt_path": str(self.prompt),
            "prompt_sha256": hashlib.sha256(self.prompt.read_bytes()).hexdigest(),
            "output_paths": outputs,
            "resources": ["workspace"],
            "repository": self.repository.declaration(),
            "codex": {
                "model": "gpt-5.6-codex",
                "reasoning_effort": "high",
                "service_tier": "priority",
                "command": [sys.executable, str(self.fake)],
                "config_overrides": ["feature_flag=true"],
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        if with_overlay:
            value["overlay_receipt"] = str(
                prepare_fixture_overlay_receipt(self.root, self.run_root)
            )
        path = self.workspace / f"{action}.invocation.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path, value

    def _write(self, path: Path, value: dict[str, object]) -> None:
        path.write_text(json.dumps(value), encoding="utf-8")

    def test_minimal_coding_invocation_has_only_provider_neutral_contract(self) -> None:
        path, _ = self.invocation()
        parsed = controller.load_invocation(path)
        self.assertEqual(controller.CODING_INVOCATION_SCHEMA, parsed.invocation_schema)
        self.assertEqual("worker-1", parsed.worker_invocation_id)

    def test_coding_settings_alias_remains_an_accepted_route_field(self) -> None:
        path, raw = self.invocation()
        raw["codex_settings"] = raw.pop("codex")
        self._write(path, raw)
        self.assertEqual(
            controller.CODING_INVOCATION_SCHEMA,
            controller.load_invocation(path).invocation_schema,
        )

    def test_unknown_schema_and_prompt_integrity_or_confinement_are_rejected(
        self,
    ) -> None:
        path, raw = self.invocation()
        raw["schema"] = "unknown/v1"
        self._write(path, raw)
        with self.assertRaisesRegex(
            controller.InvocationError, "unsupported invocation schema"
        ):
            controller.load_invocation(path)
        path, raw = self.invocation()
        raw["output_paths"] = {
            **cast(dict[str, Any], raw["output_paths"]),
            "jsonl": str(self.root / "escape.jsonl"),
        }  # type: ignore[arg-type]
        self._write(path, raw)
        with self.assertRaisesRegex(
            controller.InvocationError, "escapes its allowed root"
        ):
            controller.load_invocation(path)
        path, raw = self.invocation()
        raw["event_log_path"] = str(self.root / "outside-events.jsonl")
        self._write(path, raw)
        with self.assertRaisesRegex(
            controller.InvocationError, "escapes its allowed root"
        ):
            controller.load_invocation(path)
        path, _ = self.invocation()
        self.prompt.write_text("mutated after invocation", encoding="utf-8")
        with self.assertRaisesRegex(
            controller.InvocationError, "prompt bytes do not match"
        ):
            controller.load_invocation(path)

    def test_coding_rejects_every_firmware_only_discriminator(self) -> None:
        firmware_only = {
            "policy_sha256": "0" * 64,
            "leases": ["board:firmware"],
            "board_tokens": ["board:firmware"],
            "mcp_servers": ["firmware-mcp"],
            "server_snapshot": {"head": "firmware"},
        }
        for field, value in firmware_only.items():
            with self.subTest(field=field):
                path, raw = self.invocation()
                raw[field] = value
                self._write(path, raw)
                with self.assertRaisesRegex(
                    controller.InvocationError, "unknown top-level fields"
                ):
                    controller.load_invocation(path)

    def test_start_records_identity_events_and_configured_codex_argv(self) -> None:
        path, _ = self.invocation(with_overlay=True)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        status = json.loads(
            (self.workspace / "controller.status.json").read_text(encoding="utf-8")
        )
        self.assertEqual("orchestrator-lane-controller/v1", status["schema"])
        self.assertEqual("CODEX_EXITED", status["state"])
        self.assertEqual("worker-1", status["worker_invocation_id"])
        self.assertEqual(
            controller.CODING_INVOCATION_SCHEMA, status["invocation_schema"]
        )
        self.assertEqual("coding-thread", status["thread_id"])
        argv = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertIn("--dangerously-bypass-approvals-and-sandbox", argv)
        self.assertNotIn("--sandbox", argv)
        self.assertIn('approval_policy="never"', argv)
        self.assertIn("gpt-5.6-codex", argv)
        self.assertIn('model_reasoning_effort="high"', argv)
        self.assertIn('service_tier="priority"', argv)
        self.assertIn("feature_flag=true", argv)
        events = [
            json.loads(line)
            for line in (self.runtime_root / "events" / "controller.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertEqual(
            ["CODEX_STARTED", "CODEX_EXITED"], [event["event"] for event in events]
        )
        self.assertTrue(
            all(event["worker_invocation_id"] == "worker-1" for event in events)
        )

    def test_start_publishes_fixed_controller_lifecycle_registry(self) -> None:
        path, _ = self.invocation(with_overlay=True)
        self.assertEqual(0, controller.main([str(path)]))
        registry_path = lifecycle_registry_path(
            self.run_root, "coding:worker-1", "worker-1"
        )
        record = json.loads(registry_path.read_text(encoding="utf-8"))
        self.assertEqual("orchestrator-lifecycle-registry/v1", record["schema"])
        self.assertEqual(
            "controller-admitted-canonical-coordinate", record["authority"]
        )
        self.assertTrue(record["lifecycle"]["complete"])
        self.assertTrue(record["lifecycle"]["helpers_complete"])
        helpers = record["identities"]["helpers"]
        self.assertEqual(len({item["pid"] for item in helpers}), len(helpers))
        self.assertTrue(
            all(
                item["pid"] > 0 and item["created_utc"] and item["name"]
                for item in helpers
            )
        )
        self.assertTrue(record["boundary"]["complete"])
        self.assertEqual([], record["boundary"]["live_members"])
        self.assertEqual("worker-1", record["run"]["worker_invocation_id"])
        self.assertEqual(
            str(self.run_root.resolve()), record["repository"]["worktree_root"]
        )
        self.assertEqual(
            record["coordinate"]["common_dir"], record["repository"]["common_dir"]
        )

    def test_start_without_thread_is_failure_and_resume_identity_mismatches_are_rejected(
        self,
    ) -> None:
        path, raw = self.invocation(with_overlay=True)
        os.environ["CODING_CONTROLLER_NO_THREAD"] = "1"
        self.assertEqual(1, controller.main([str(path)]))
        status = json.loads(
            (self.workspace / "controller.status.json").read_text(encoding="utf-8")
        )
        self.assertEqual("LAUNCH_FAILED", status["state"])
        os.environ.pop("CODING_CONTROLLER_NO_THREAD")
        path, raw = self.invocation(action="resume")
        raw["resume_identity"] = {
            "worker_invocation_id": "another-worker",
            "thread_id": "coding-thread",
        }
        self._write(path, raw)
        with self.assertRaisesRegex(
            controller.InvocationError, "resume identity worker_invocation_id mismatch"
        ):
            controller.load_invocation(path)
        path, _ = self.invocation(
            action="resume", worker_id="worker-2", with_overlay=True
        )
        # REQ-O35: identity-mismatched resume emits a declared same-role
        # structured handoff with fabricated_continuity=false instead of
        # silently rejecting and losing the logical task.
        self.assertEqual(1, controller.main([str(path)]))
        status = json.loads(
            (self.workspace / "controller.status.json").read_text(encoding="utf-8")
        )
        self.assertEqual("PROVIDER_HANDOFF", status["state"])
        handoff = status["provider_handoff"]
        self.assertIsNotNone(handoff)
        self.assertFalse(handoff["fabricated_continuity"])
        self.assertIn("worker_invocation_id", handoff["reason"])

    def test_resource_acquisition_publishes_running_only_after_child_launch_and_popen_failure_releases_claim(
        self,
    ) -> None:
        path, _ = self.invocation(with_overlay=True)
        status_path = self.workspace / "controller.status.json"
        lock_root = self.runtime_root / "coding-resource-locks"
        published: list[dict[str, object]] = []
        original_atomic_json = controller._atomic_json
        original_popen: Any = controller.subprocess.Popen

        def record(path: Path, value: dict[str, object]) -> None:
            published.append(dict(value))
            original_atomic_json(path, value)

        def launch(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
            command = args[0] if args else None
            if isinstance(command, list) and command[:2] == [
                sys.executable,
                str(self.fake),
            ]:
                self.assertTrue(status_path.exists())
                self.assertTrue(any(lock_root.iterdir()))
            return original_popen(*args, **kwargs)

        with (
            patch.object(controller, "_atomic_json", side_effect=record),
            patch.object(controller.subprocess, "Popen", side_effect=launch),
        ):
            self.assertEqual(0, controller.main([str(path)]))
        self.assertTrue(any(item["state"] == "RUNNING_CODEX" for item in published))
        first_registry = lifecycle_registry_path(
            self.run_root, "coding:worker-1", "worker-1"
        )
        first_registry.unlink()
        first_registry.parent.rmdir()

        path, _ = self.invocation(
            worker_id="worker-popen-failure", with_overlay=True
        )

        def fail_codex_launch(
            *args: object, **kwargs: object
        ) -> subprocess.Popen[bytes]:
            command = args[0] if args else None
            if isinstance(command, list) and command[:2] == [
                sys.executable,
                str(self.fake),
            ]:
                raise OSError("synthetic Popen failure")
            return original_popen(*args, **kwargs)

        with patch.object(
            controller.subprocess, "Popen", side_effect=fail_codex_launch
        ):
            self.assertEqual(1, controller.main([str(path)]))
        status = json.loads(status_path.read_text(encoding="utf-8"))
        self.assertEqual("LAUNCH_FAILED", status["state"])
        self.assertEqual([], status["held_resource_claims"])
        self.assertFalse(any(lock_root.iterdir()))

    def test_resume_rejects_child_thread_mismatch(self) -> None:
        start, _ = self.invocation(with_overlay=True)
        self.assertEqual(0, controller.main([str(start)]))
        resume, raw = self.invocation(action="resume", with_overlay=True)
        raw["resume_thread_id"] = "coding-thread"
        self._write(resume, raw)
        os.environ["CODING_CONTROLLER_THREAD"] = "wrong-thread"
        self.assertEqual(1, controller.main([str(resume)]))
        status = json.loads(
            (self.workspace / "controller.status.json").read_text(encoding="utf-8")
        )
        self.assertEqual("CONTROLLER_FAILED", status["state"])
        self.assertIn("does not match", status["error"])

    def test_resume_rejects_a_branch_switch_after_start(self) -> None:
        start, _ = self.invocation(with_overlay=True)
        self.assertEqual(0, controller.main([str(start)]))
        self.repository.git("checkout", "-b", "switched")
        resume, _ = self.invocation(action="resume", with_overlay=True)
        self.assertEqual(2, controller.main([str(resume)]))

    def test_root_adjudication_preserves_failure_and_records_pass(self) -> None:
        secret = controller.generate_root_adjudication_secret()
        status_path = self.workspace / "controller.status.json"
        original = {
            "state": "CONTROLLER_FAILED",
            "error": "synthetic controller failure",
            "controller_pid": 123,
            "root_adjudication_commitment": controller.root_adjudication_commitment(
                secret
            ),
        }
        status_path.write_text(json.dumps(original), encoding="utf-8")
        updated = controller.adjudicate_controller_status(
            status_path,
            root_secret=secret,
            root_identity="root-session-1",
            rationale=(
                "independent evidence review accepted the controller failure "
                "disposition"
            ),
            decided_utc="2026-08-22T00:00:00Z",
        )
        self.assertEqual("PASS", updated["state"])
        self.assertEqual(original, updated["original_controller_status"])
        self.assertEqual("CONTROLLER_FAILED", updated["original_state"])
        self.assertEqual("PASS", updated["effective_state"])
        self.assertEqual("ROOT", updated["root_adjudication"]["authority"])
        with self.assertRaises(controller.InvocationError):
            controller.adjudicate_controller_status(
                status_path,
                root_secret=controller.generate_root_adjudication_secret(),
                root_identity="root-session-2",
                rationale="duplicate decision must be rejected",
            )
        with self.assertRaises(controller.InvocationError):
            controller.adjudicate_controller_status(
                status_path,
                root_secret=controller.generate_root_adjudication_secret(),
                root_identity="worker-session",
                rationale="worker cannot adjudicate",
            )

    def test_root_adjudication_has_no_worker_or_public_cli_forge_path(self) -> None:
        secret = controller.generate_root_adjudication_secret()
        status_path = self.workspace / "controller.status.json"
        original = {
            "state": "CONTROLLER_FAILED",
            "error": "synthetic failure",
            "root_adjudication_commitment": controller.root_adjudication_commitment(
                secret
            ),
        }
        status_path.write_text(json.dumps(original), encoding="utf-8")
        with self.assertRaises(TypeError):
            controller.adjudicate_controller_status(  # type: ignore[call-arg]
                status_path,
                root_identity="worker-session",
                rationale="missing authority must fail closed",
            )
        worker_code = (
            "import sys\n"
            "import orchestrator_harness.lane_controller as c\n"
            "try:\n"
            "    c.adjudicate_controller_status(\n"
            "        sys.argv[1], root_secret=c.generate_root_adjudication_secret(),\n"
            "        root_identity='worker', rationale='forge'\n"
            "    )\n"
            "except c.InvocationError:\n"
            "    raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        )
        worker = subprocess.run(
            [
                sys.executable,
                "-c",
                worker_code,
                str(status_path),
            ],
            cwd=str(Path(__file__).resolve().parents[2]),
            env={
                key: value
                for key, value in os.environ.items()
                if key != controller.ROOT_ADJUDICATION_SECRET_ENV
            },
            check=False,
        )
        self.assertEqual(0, worker.returncode)
        self.assertEqual(
            1,
            cli.main(
                [
                    "adjudicate",
                    "--status",
                    str(status_path),
                    "--root-identity",
                    "ROOT-forged-by-worker",
                    "--rationale",
                    "public flags cannot create authority",
                ]
            ),
        )
        self.assertEqual(original, json.loads(status_path.read_text(encoding="utf-8")))

    def test_root_secret_is_bound_as_commitment_and_stripped_from_provider(self) -> None:
        secret = controller.generate_root_adjudication_secret()
        capture = self.root / "secret.txt"
        os.environ["CODING_CONTROLLER_SECRET_CAPTURE"] = str(capture)
        os.environ[controller.ROOT_ADJUDICATION_SECRET_ENV] = secret
        path, _ = self.invocation()
        self.assertEqual(0, controller.main([str(path)]))
        status = json.loads(
            (self.workspace / "controller.status.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            controller.root_adjudication_commitment(secret),
            status["root_adjudication_commitment"],
        )
        self.assertNotIn(secret, json.dumps(status))
        self.assertEqual("<missing>", capture.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
