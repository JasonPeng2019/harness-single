from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from orchestrator_harness import processes
from orchestrator_harness.tests.support import write_json


SOURCE_PRODUCT_ROOT = Path(__file__).resolve().parents[2]
FAKE_CODEX = (
    Path(__file__).resolve().parent
    / "support"
    / "fake_codex_macos_product_smoke.py"
)


@unittest.skipUnless(
    sys.platform == "darwin", "macOS product demonstration requires Darwin"
)
class HermeticMacOSProductSmokeTests(unittest.TestCase):
    """Exercise the macOS public product lifecycle without a provider account."""

    maxDiff = None

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="harness-product-smoke-")
        self.addCleanup(temporary.cleanup)
        self.sandbox = Path(temporary.name)
        self.product_root = self.sandbox / "harness-single"
        self.target = self.sandbox / "target-repository"
        self.fake_bin = self.sandbox / "fake-bin"
        self.empty_git_template = self.sandbox / "empty-git-template"
        self.empty_git_hooks = self.sandbox / "empty-git-hooks"
        self.runtime = self.target / ".harness-runtime"
        self.empty_git_template.mkdir()
        self.empty_git_hooks.mkdir()
        self.adversarial_ambient_git = (
            self._testMethodName
            == "test_macos_product_demonstration_ignores_ambient_git_injection"
        )
        ambient = os.environ.copy()
        if self.adversarial_ambient_git:
            ambient.update(self._adversarial_git_environment())
        self.ambient_environment = ambient
        self.environment = self._sanitized_environment(ambient)
        self.environment["PATH"] = os.pathsep.join(
            [str(self.fake_bin), self.environment.get("PATH", "")]
        )
        self.environment["PYTHONPATH"] = os.pathsep.join(
            [str(self.product_root), self.environment.get("PYTHONPATH", "")]
        ).rstrip(os.pathsep)
        self._copy_product()
        self._install_fake_codex()
        self._create_target_repository()
        self._write_local_config()
        self.addCleanup(self._cleanup_harness_processes)

    def _adversarial_git_environment(self) -> dict[str, str]:
        """Ambient Git state that would break or redirect an unsanitized demo."""

        poisoned_hooks = self.sandbox / "poisoned-hooks"
        poisoned_hooks.mkdir()
        hook = poisoned_hooks / "pre-commit"
        hook.write_text("#!/bin/sh\nexit 97\n", encoding="utf-8")
        hook.chmod(hook.stat().st_mode | stat.S_IXUSR)
        poisoned_global = self.sandbox / "poisoned-global.gitconfig"
        poisoned_global.write_text(
            "[commit]\n\tgpgSign = true\n[core]\n\thooksPath = "
            f"{poisoned_hooks}\n",
            encoding="utf-8",
        )
        return {
            "GIT_CONFIG_COUNT": "2",
            "GIT_CONFIG_KEY_0": "commit.gpgSign",
            "GIT_CONFIG_VALUE_0": "true",
            "GIT_CONFIG_KEY_1": "core.hooksPath",
            "GIT_CONFIG_VALUE_1": str(poisoned_hooks),
            "GIT_CONFIG_GLOBAL": str(poisoned_global),
            "GIT_DIR": str(self.sandbox / "redirected-git-dir"),
            "GIT_WORK_TREE": str(self.sandbox / "redirected-worktree"),
            "GIT_INDEX_FILE": str(self.sandbox / "redirected-index"),
            "GIT_OBJECT_DIRECTORY": str(self.sandbox / "redirected-objects"),
            "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(
                self.sandbox / "redirected-alternates"
            ),
        }

    def _sanitized_environment(self, source: dict[str, str]) -> dict[str, str]:
        """Remove ambient Git routing/config state and install controlled defaults."""

        environment = {
            key: value
            for key, value in source.items()
            if not key.upper().startswith("GIT_")
        }
        environment.update(
            {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_SYSTEM": os.devnull,
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_TEMPLATE_DIR": str(self.empty_git_template),
            }
        )
        return environment

    def _copy_product(self) -> None:
        self.product_root.mkdir(parents=True)
        for name in ("orchestrator_harness", "harness_common", "adapters", "super-cache"):
            shutil.copytree(
                SOURCE_PRODUCT_ROOT / name,
                self.product_root / name,
                ignore=shutil.ignore_patterns(
                    "__pycache__", "*.pyc", "*.pyo", ".pytest_cache"
                ),
            )

    def _git(self, *args: str, cwd: Path | None = None) -> str:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd or self.target,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=self.environment,
        )
        return completed.stdout.strip()

    def _create_target_repository(self) -> None:
        self.target.mkdir()
        self._git(
            "init",
            "--initial-branch=main",
            f"--template={self.empty_git_template}",
        )
        self._git("config", "--local", "user.name", "Synthetic Harness Smoke")
        self._git(
            "config",
            "--local",
            "user.email",
            "synthetic-harness-smoke@example.invalid",
        )
        self._git("config", "--local", "commit.gpgSign", "false")
        self._git("config", "--local", "tag.gpgSign", "false")
        self._git("config", "--local", "core.hooksPath", str(self.empty_git_hooks))
        (self.target / "README.md").write_text(
            "# Disposable synthetic harness target\n", encoding="utf-8"
        )
        self._git("add", "README.md")
        self._git(
            "commit",
            "--no-verify",
            "--no-gpg-sign",
            "-m",
            "synthetic smoke base",
        )

    def _install_fake_codex(self) -> None:
        self.fake_bin.mkdir()
        target = self.fake_bin / "codex"
        shutil.copy2(FAKE_CODEX, target)
        target.chmod(target.stat().st_mode | stat.S_IXUSR)

    def _write_local_config(self) -> None:
        local = self.product_root / "local-config"
        write_json(
            local / "harness-config.json",
            {
                "root_workspace": str(self.target),
                "managed_coordination": "disabled",
            },
        )
        write_json(
            local / "resource-manifest.json",
            {"schema": "resource-manifest/v1", "resources": []},
        )

    def _operator(self, *args: str, timeout: float = 45.0) -> dict[str, Any]:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "orchestrator_harness.operator_launch",
                "--json",
                *args,
            ],
            cwd=self.product_root,
            env=self.environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )
        try:
            result = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            self.fail(
                f"operator output was not JSON for {args}: returncode={completed.returncode}; "
                f"stdout={completed.stdout!r}; stderr={completed.stderr!r}; error={exc}"
            )
        self.assertEqual(
            0,
            completed.returncode,
            f"operator failed for {args}: {json.dumps(result, indent=2)}; "
            f"stderr={completed.stderr!r}",
        )
        self.assertTrue(result["ok"], result)
        return result

    def _cleanup_harness_processes(self) -> None:
        """Best-effort exact-identity cleanup when an assertion stops the smoke."""

        state_path = self.runtime / "RUNTIME_STATE.json"
        if not state_path.is_file():
            return
        if self._read_json(state_path).get("state") != "CLOSED":
            try:
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "orchestrator_harness.operator_launch",
                        "--json",
                        "harness",
                        "shutdown",
                    ],
                    cwd=self.product_root,
                    env=self.environment,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=15.0,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                pass
        identity_records: list[dict[str, Any]] = []
        monitor_path = self.runtime / "monitor" / "MONITOR.json"
        if monitor_path.is_file():
            identity_records.append(self._read_json(monitor_path))
        for lane_path in self.runtime.glob("epochs/*/lanes/*/lane.json"):
            lane = self._read_json(lane_path)
            identity_records.append(dict(lane.get("process") or {}))
            status_path = Path(str(lane.get("controller_status_path") or ""))
            if status_path.is_file():
                status = self._read_json(status_path)
                identity_records.append(dict(status.get("provider_state") or {}))
                boundary = status.get("process_boundary") or {}
                identity_records.extend(
                    dict(item) for item in boundary.get("processes", [])
                )
        for identity in identity_records:
            pid = identity.get("pid")
            creation_time = identity.get("creation_time")
            if processes.identity_matches(pid, creation_time):
                processes.terminate_process(pid, creation_time, force=True)

    def _read_json(self, path: Path) -> dict[str, Any]:
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertIsInstance(value, dict)
        return value

    def _wait_identity_gone(
        self, identity: dict[str, Any], timeout: float = 15.0
    ) -> None:
        pid = identity.get("pid")
        creation_time = identity.get("creation_time")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not processes.identity_matches(pid, creation_time):
                return
            time.sleep(0.1)
        self.assertFalse(
            processes.identity_matches(pid, creation_time),
            f"exact process identity still live: {identity}",
        )

    def _wait_lane_lifecycle(
        self, lane_path: Path, expected: str, timeout: float = 15.0
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        lane: dict[str, Any] = {}
        while time.monotonic() < deadline:
            lane = self._read_json(lane_path)
            if lane.get("lifecycle") == expected:
                return lane
            time.sleep(0.1)
        self.fail(
            f"lane did not reach {expected!r}: lifecycle={lane.get('lifecycle')!r}"
        )

    def _assert_macos_product_demonstration(self) -> None:
        forbidden_git_environment = {
            "GIT_CONFIG_COUNT",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        }
        self.assertTrue(forbidden_git_environment.isdisjoint(self.environment))
        self.assertFalse(
            any(key.startswith("GIT_CONFIG_KEY_") for key in self.environment)
        )
        self.assertFalse(
            any(key.startswith("GIT_CONFIG_VALUE_") for key in self.environment)
        )
        self.assertEqual(os.devnull, self.environment["GIT_CONFIG_GLOBAL"])
        self.assertEqual(os.devnull, self.environment["GIT_CONFIG_SYSTEM"])
        self.assertEqual("1", self.environment["GIT_CONFIG_NOSYSTEM"])
        self.assertEqual(
            str(self.empty_git_template), self.environment["GIT_TEMPLATE_DIR"]
        )
        self.assertEqual("false", self._git("config", "--bool", "commit.gpgSign"))
        self.assertEqual("false", self._git("config", "--bool", "tag.gpgSign"))
        self.assertEqual(
            str(self.empty_git_hooks), self._git("config", "core.hooksPath")
        )
        self.assertEqual(
            self.target.resolve(),
            Path(self._git("rev-parse", "--show-toplevel")).resolve(),
        )

        source_guard_paths = tuple(
            SOURCE_PRODUCT_ROOT / name
            for name in ("local-config", ".harness-runtime", "runtime")
        )
        source_guard_before = {path: path.exists() for path in source_guard_paths}

        setup = self._operator("harness", "setup")
        self.assertEqual("SETUP_OK", setup["code"])
        self.assertEqual(
            "OPEN", self._read_json(self.runtime / "RUNTIME_STATE.json")["state"]
        )

        base_commit = self._git("rev-parse", "HEAD")
        task_card = self.sandbox / "task-card.json"
        write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "card_id": "synthetic-product-smoke",
                "task": (
                    "Produce a synthetic RESULT.json only. This is a hermetic fake-provider "
                    "macOS product demonstration and never live-provider proof."
                ),
                "branch": "lane/synthetic-product-smoke",
                "base_commit": base_commit,
            },
        )
        bootstrap = self._operator(
            "lane",
            "bootstrap",
            "--lane-id",
            "synthetic-smoke",
            "--provider",
            "codex",
            "--model",
            "synthetic-model",
            "--task-card",
            str(task_card),
        )
        self.assertEqual("BOOTSTRAP_OK", bootstrap["code"])
        epoch = self._read_json(self.runtime / "CURRENT_EPOCH.json")
        lane_dir = (
            self.runtime
            / "epochs"
            / epoch["epoch_id"]
            / "lanes"
            / "synthetic-smoke"
        )
        lane_path = lane_dir / "lane.json"
        lane = self._read_json(lane_path)
        worktree = Path(lane["worktree_path"])
        self.assertTrue(worktree.is_dir())
        self.assertTrue(str(worktree).startswith(str(self.sandbox)))

        launch = self._operator(
            "lane", "launch", "--lane-id", "synthetic-smoke", timeout=60.0
        )
        self.assertEqual("LAUNCH_OK", launch["code"])
        watch = self._operator(
            "watch", "--until-actionable", "--timeout", "30s", timeout=40.0
        )
        self.assertEqual("WATCH_ACTIONABLE", watch["code"])
        if "review_pending" not in watch["summary"]:
            diagnostic_lane = self._read_json(lane_path)
            diagnostic_status = self._read_json(
                Path(diagnostic_lane["controller_status_path"])
            )
            self.fail(
                f"watch did not reach review_pending: watch={watch}; lane={diagnostic_lane}; "
                f"status={diagnostic_status}"
            )

        lane = self._read_json(lane_path)
        self.assertEqual("review_pending", lane["lifecycle"])
        self.assertTrue(lane["result_validation"]["clean"])
        self.assertEqual(base_commit, lane["result_validation"]["commit"])
        controller_identity = dict(lane["process"])
        status = self._read_json(Path(lane["controller_status_path"]))
        provider_identity = {
            "pid": status["provider_state"]["pid"],
            "creation_time": status["provider_state"]["creation_time"],
        }
        result = self._read_json(worktree / "RESULT.json")
        self.assertIn("Synthetic fake-provider", result["summary"])
        self.assertTrue(result["evidence"][0]["synthetic"])
        self.assertFalse(result["evidence"][0]["live_provider_proof"])
        synthetic_evidence_path = Path(result["evidence"][0]["path"])
        synthetic_evidence_bytes = synthetic_evidence_path.read_bytes()
        synthetic_evidence_hash = hashlib.sha256(
            synthetic_evidence_bytes
        ).hexdigest()
        synthetic_evidence = self._read_json(synthetic_evidence_path)
        self.assertTrue(synthetic_evidence["synthetic"])
        self.assertFalse(synthetic_evidence["live_provider_proof"])
        self.assertTrue(synthetic_evidence["prompt_received"])
        self.assertEqual("exec", synthetic_evidence["argv"][0])
        self.assertIn("--json", synthetic_evidence["argv"])
        self.assertIn("synthetic-model", synthetic_evidence["argv"])
        transcript = Path(lane["transcript_path"]).read_text(encoding="utf-8")
        self.assertIn("synthetic-smoke-session", transcript)

        review = self._operator(
            "lane",
            "completion-review",
            "--lane-id",
            "synthetic-smoke",
            "--review-outcome",
            "PASS",
            "--approval",
            "ACCEPTED",
            "--review-summary",
            "Accepted synthetic hermetic macOS product-demonstration evidence.",
        )
        self.assertEqual("COMPLETION_REVIEW_OK", review["code"])
        acceptance_path = lane_dir / "ORCHESTRATOR_ACCEPTANCE.json"
        acceptance = self._read_json(acceptance_path)
        self.assertEqual("ACCEPTED", acceptance["approval"])
        accepted_lane = self._wait_lane_lifecycle(lane_path, "accepted")
        self.assertEqual(
            acceptance["content_hash"],
            accepted_lane["acceptance_advancement"]["content_hash"],
        )

        retire = self._operator(
            "lane", "retire", "--acceptance-ref", str(acceptance_path), timeout=60.0
        )
        self.assertEqual("RETIRE_OK", retire["code"])
        self.assertFalse(worktree.exists(), "retirement must remove the Git worktree")
        self._wait_identity_gone(controller_identity)
        self._wait_identity_gone(provider_identity)
        retained_lane = self._read_json(lane_path)
        self.assertEqual("retired", retained_lane["lifecycle"])
        self.assertEqual(2, len(retire["evidence_paths"]))
        for evidence_path in retire["evidence_paths"]:
            self.assertTrue(Path(evidence_path).exists(), evidence_path)
        archive = Path(retire["evidence_paths"][1])
        archive_manifest = self._read_json(archive / "RETIREMENT_ARCHIVE.json")
        self.assertEqual("retirement-archive/v1", archive_manifest["schema"])
        self.assertTrue(archive_manifest["git_proof"]["clean"])
        self.assertTrue(archive_manifest["process_proof"]["controller_gone"])
        self.assertTrue(archive_manifest["process_proof"]["provider_gone"])
        self.assertTrue(
            {
                ".agent-workspace/task-card.json",
                "RESULT.json",
                "COMPLETION_REVIEW.json",
                "ORCHESTRATOR_ACCEPTANCE.json",
                ".agent-workspace/invocation.json",
                ".agent-workspace/controller.status.json",
                ".agent-workspace/controller.events.jsonl",
                ".agent-workspace/provider-transcript.jsonl",
                ".agent-workspace/provider-stderr.txt",
                ".agent-workspace/controller.attempts.jsonl",
                ".agent-workspace/synthetic-provider-evidence.json",
                "lane.json",
            }.issubset(archive_manifest["files"])
        )
        archived_evidence = (
            archive / ".agent-workspace" / "synthetic-provider-evidence.json"
        )
        self.assertEqual(synthetic_evidence_bytes, archived_evidence.read_bytes())
        evidence_metadata = archive_manifest["files"][
            ".agent-workspace/synthetic-provider-evidence.json"
        ]
        self.assertEqual(synthetic_evidence_hash, evidence_metadata["sha256"])
        self.assertEqual(len(synthetic_evidence_bytes), evidence_metadata["size"])

        shutdown = self._operator("harness", "shutdown", timeout=60.0)
        self.assertEqual("SHUTDOWN_OK", shutdown["code"])
        self.assertEqual(
            "CLOSED", self._read_json(self.runtime / "RUNTIME_STATE.json")["state"]
        )
        monitor = self._read_json(self.runtime / "monitor" / "MONITOR.json")
        self._wait_identity_gone(monitor)
        self.assertEqual(
            source_guard_before,
            {path: path.exists() for path in source_guard_paths},
            "the product smoke must not create config or runtime state in source",
        )

    def test_macos_product_demonstration_completes_public_cli_lifecycle(
        self,
    ) -> None:
        self.assertFalse(self.adversarial_ambient_git)
        self._assert_macos_product_demonstration()

    def test_macos_product_demonstration_ignores_ambient_git_injection(
        self,
    ) -> None:
        self.assertTrue(self.adversarial_ambient_git)
        self.assertEqual("2", self.ambient_environment["GIT_CONFIG_COUNT"])
        self.assertEqual(
            "commit.gpgSign", self.ambient_environment["GIT_CONFIG_KEY_0"]
        )
        self.assertEqual("true", self.ambient_environment["GIT_CONFIG_VALUE_0"])
        self.assertNotEqual(
            self.ambient_environment["GIT_CONFIG_GLOBAL"],
            self.environment["GIT_CONFIG_GLOBAL"],
        )
        self._assert_macos_product_demonstration()


if __name__ == "__main__":
    unittest.main()
