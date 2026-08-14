"""Focused provider-owned bounded-policy tests for REQ-O43.

Covers editable launcher JSON mutation, Git-ignore file/directory/wildcard/
negation exclusions with policy-source provenance, host-shell command-chain
segmentation, fail-closed configuration/Git handling, stateless concurrent
evaluation, fake lane-managed session separation, and installed-hook nested
command enforcement.  Only disposable local fake repositories and processes
are used; no real provider, network, hardware, MCP, USB, or display checkout
is touched.
"""
from __future__ import annotations

import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from orchestrator_harness.codex_adapter import install_codex_adapter
from orchestrator_harness.codex_bounded_policy import (
    BoundedPolicyError,
    EXCLUSIONS_RELATIVE,
    LAUNCHERS_RELATIVE,
    bounded_policy_status,
    command_segments,
    guard_pre_tool_use,
    is_excluded,
    load_launchers,
    validate_exclusions,
)
from orchestrator_harness.codex_adapter import packaged_codex_assets


POLICY_JSON = {
    "schema": "bounded-launchers/v1",
    "launcher_categories": {
        "python_script": ["python", "python3", "py"],
        "powershell_file": ["powershell", "pwsh"],
        "posix_shell": ["bash", "sh", "zsh"],
    },
}


def _write_policy(root: Path, *, launchers: object = POLICY_JSON, exclusions: str = "") -> None:
    if not (root / ".git").exists():
        subprocess.run(["git", "init", "--quiet", str(root)], check=True, capture_output=True, text=True)
    policies = root / ".codex" / "policies"
    policies.mkdir(parents=True, exist_ok=True)
    (policies / "bounded-launchers.json").write_text(
        json.dumps(launchers), encoding="utf-8",
    )
    (policies / "bounded-exclusions.gitignore").write_text(exclusions, encoding="utf-8")


def _deny(result: object) -> bool:
    return (
        isinstance(result, dict)
        and result.get("hookSpecificOutput", {}).get("permissionDecision") == "deny"
    )


def _script(root: Path, relative: str) -> Path:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# fixture\n", encoding="utf-8")
    return target


class BoundedPolicyModuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        _write_policy(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _guard(self, command: str, *, cwd: Path | None = None, workdir: str | None = None) -> dict[str, object]:
        tool_input: dict[str, object] = {"command": command}
        if workdir is not None:
            tool_input["workdir"] = workdir
        payload: dict[str, object] = {"tool_input": tool_input}
        if cwd is not None:
            payload["cwd"] = str(cwd)
        return guard_pre_tool_use(payload, policy_root=self.root)

    # ---- editable launcher JSON ----

    def test_launcher_mutation_changes_coverage(self) -> None:
        # Default launchers cover python; a covered script that is not
        # excluded is denied, while an unknown launcher stays outside.
        self.assertTrue(_deny(self._guard("python task.py")))
        self.assertTrue(_deny(self._guard("python3 -I task.py")))
        self.assertTrue(_deny(self._guard("py -3.12 task.py")))
        self.assertEqual({}, self._guard("custom-python task.py"))

        _write_policy(self.root, launchers={
            "schema": "bounded-launchers/v1",
            "launcher_categories": {
                "python_script": ["custom-python"],
                "powershell_file": ["powershell", "pwsh"],
                "posix_shell": ["bash", "sh", "zsh"],
            },
        })
        # The renamed launcher is now covered and the previous one is not.
        self.assertEqual({}, self._guard("python task.py"))
        self.assertTrue(_deny(self._guard("custom-python task.py")))

    def test_launcher_load_rejects_incomplete_or_wrong_shapes(self) -> None:
        invalid = (
            {"schema": "bounded-launchers/v2", "launcher_categories": POLICY_JSON["launcher_categories"]},
            {"schema": "bounded-launchers/v1", "launcher_categories": {}},
            {
                "schema": "bounded-launchers/v1",
                "launcher_categories": {
                    "python_script": "python",
                    "powershell_file": ["powershell"],
                    "posix_shell": ["bash"],
                },
            },
            {
                "schema": "bounded-launchers/v1",
                "launcher_categories": {
                    "python_script": ["python", 3],
                    "powershell_file": ["powershell"],
                    "posix_shell": ["bash"],
                },
            },
            {"extra": True},
        )
        for value in invalid:
            with self.subTest(value=value):
                _write_policy(self.root, launchers=value)
                with self.assertRaises(BoundedPolicyError):
                    load_launchers(self.root)
                self.assertTrue(_deny(self._guard("python task.py")))

    # ---- Git-ignore exclusions with provenance ----

    def test_gitignore_file_directory_wildcard_negation_and_sibling_prefix(self) -> None:
        _write_policy(
            self.root,
            exclusions="**/lane-launchers/*\n!**/lane-launchers/bounded.py\ntools/lane launchers/\n",
        )
        excluded = _script(self.root, "nested/lane-launchers/start.py")
        reincluded = _script(self.root, "nested/lane-launchers/bounded.py")
        spaced = _script(self.root, "tools/lane launchers/start.py")
        sibling = _script(self.root, "tools/lane-launchers-backup/start.py")
        _script(self.root, "tools/lane launchers/start.ps1")
        self.assertEqual({}, self._guard(f"python -I {excluded}", cwd=self.root))
        self.assertEqual({}, self._guard(f'python "{spaced}"', cwd=self.root))
        self.assertEqual({}, self._guard('powershell -File "tools/lane launchers/start.ps1"', cwd=self.root))
        self.assertTrue(_deny(self._guard("python nested/lane-launchers/bounded.py", cwd=self.root)))
        self.assertTrue(_deny(self._guard("python tools/lane-launchers-backup/start.py", cwd=self.root)))
        self.assertFalse(is_excluded(str(reincluded), cwd=self.root, policy_root=self.root, exclusions=validate_exclusions(self.root)))

    def test_ordinary_repository_ignore_is_not_policy_authorization(self) -> None:
        script = _script(self.root, "ignored-launchers/start.py")
        (self.root / ".gitignore").write_text("ignored-launchers/\n", encoding="utf-8")
        result = self._guard(f"python {script}", cwd=self.root)
        self.assertTrue(_deny(result))
        self.assertIn("bounded-execution policy", result["hookSpecificOutput"]["permissionDecisionReason"])

    def test_quoted_paths_workdir_precedence_and_outside_root_paths(self) -> None:
        _write_policy(self.root, exclusions="launchers/\n")
        inside = _script(self.root, "launchers/start.py")
        self.assertEqual({}, self._guard(f'python "{inside}"', cwd=self.root))
        wrong_cwd = self.root / "wrong-cwd"
        wrong_cwd.mkdir()
        self.assertEqual(
            {},
            self._guard(
                "python launchers/start.py",
                cwd=wrong_cwd,
                workdir=str(self.root),
            ),
        )
        outside = self.root.parent / f"{self.root.name}-outside" / "launchers" / "start.py"
        outside.parent.mkdir(parents=True)
        outside.write_text("# fixture\n", encoding="utf-8")
        self.assertTrue(_deny(self._guard(f'python "{outside}"', cwd=self.root)))

    def test_python_inline_module_and_shell_command_text_remain_bounded(self) -> None:
        _write_policy(self.root, exclusions="/**\n")
        for command in (
            'python -c "print(1)"',
            "python -m pytest -q",
            "bash -lc 'echo hi'",
            "sh -c 'echo hi'",
        ):
            result = self._guard(command, cwd=self.root)
            self.assertTrue(_deny(result), command)

    def test_powershell_file_and_direct_ps1_are_covered(self) -> None:
        _write_policy(self.root, exclusions="tools/run/\n")
        _script(self.root, "tools/run/start.ps1")
        _script(self.root, "tools/start.ps1")
        for allowed in (
            'powershell -File "tools/run/start.ps1"',
            'pwsh -NoProfile -File tools/run/start.ps1',
            r'.\tools\run\start.ps1',
        ):
            self.assertEqual({}, self._guard(allowed, cwd=self.root), allowed)
        for denied in (
            'powershell -File "tools/start.ps1"',
            r'.\tools\start.ps1',
        ):
            self.assertTrue(_deny(self._guard(denied, cwd=self.root)), denied)

    def test_uv_wsl_and_call_operator_launcher_variants(self) -> None:
        _write_policy(self.root, exclusions="tools/lane-launchers/\n")
        for relative in ("tools/lane-launchers/start.py", "tools/lane-launchers/start.sh", "tools/lane-launchers/start.ps1"):
            _script(self.root, relative)
        commands = (
            "uv run --project .codex/dev --locked python tools/lane-launchers/start.py",
            'uv run --project .codex/dev --locked -- python -X utf8 "tools/lane-launchers/start.py"',
            r"& 'C:\Python\python.exe' tools/lane-launchers/start.py",
            "wsl bash tools/lane-launchers/start.sh",
            "wsl.exe bash tools/lane-launchers/start.sh",
            ".\\tools\\lane-launchers\\start.ps1",
        )
        for command in commands:
            self.assertEqual({}, self._guard(command, cwd=self.root), command)

    # ---- host-shell chain segmentation ----

    def test_command_segments_split_only_outside_quotes(self) -> None:
        command = (
            "python a.py; python b.py | python c.py || python d.py && python e.py"
            "\r\npython f.py"
        )
        segments = command_segments(command)
        self.assertEqual(
            ["python a.py", "python b.py", "python c.py", "python d.py", "python e.py", "python f.py"],
            segments,
        )

    def test_command_segments_honor_powershell_quote_and_backtick_rules(self) -> None:
        # Semicolons and pipes inside quotes never split.
        quoted = "python -c 'print(1); print(2)' ; python task.py"
        self.assertEqual(["python -c 'print(1); print(2)'", "python task.py"], command_segments(quoted))
        double_quoted = 'python -c "print(1) | print(2)" | python task.py'
        self.assertEqual(['python -c "print(1) | print(2)"', "python task.py"], command_segments(double_quoted))
        # Backtick escapes the next character (the escaped token stays in the
        # one un-split segment); backslash never escapes inside single-quoted
        # text, so the semicolon there is still inside quotes.
        backtick = "python task.py `; python other.py"
        self.assertEqual(["python task.py `; python other.py"], command_segments(backtick))
        single_quoted_backslash = r"python 'a\;b'; python task.py"
        self.assertEqual([r"python 'a\;b'", "python task.py"], command_segments(single_quoted_backslash))
        double_quote_backtick_escape = 'python "a`"b"; python task.py'
        self.assertEqual(['python "a`"b"', "python task.py"], command_segments(double_quote_backtick_escape))

    def test_supervisor_exclusion_does_not_exempt_appended_covered_segment(self) -> None:
        _write_policy(self.root, exclusions=".codex/scripts/Invoke-BoundedTest.ps1\n")
        supervisor = (
            "powershell -File .codex/scripts/Invoke-BoundedTest.ps1 "
            "-Command 'python -m pytest -q' -WorkingDirectory . "
            "-ExpectedUpperBoundSeconds 30 -CleanupAllowanceSeconds 7 "
            "-MaximumLifetimeSeconds 37 -HeartbeatIntervalSeconds 10 "
            "-TimeoutBasis measured -ResultPath runtime/test.json"
        )
        self.assertEqual({}, self._guard(supervisor, cwd=self.root))
        appended = supervisor + "; python .codex/scripts/verify.py"
        result = self._guard(appended, cwd=self.root)
        self.assertTrue(_deny(result))

        multiline = supervisor + "\npython .codex/scripts/verify.py"
        self.assertTrue(_deny(self._guard(multiline, cwd=self.root)))

    def test_current_packaged_policy_excludes_only_approved_orchestration_paths(self) -> None:
        packaged = {path.as_posix(): data for path, data in packaged_codex_assets().items()}
        launchers = json.loads(packaged[".codex/policies/bounded-launchers.json"])
        self.assertEqual("bounded-launchers/v1", launchers["schema"])
        self.assertEqual(
            {"python_script", "powershell_file", "posix_shell"},
            set(launchers["launcher_categories"]),
        )
        exclusions = packaged[".codex/policies/bounded-exclusions.gitignore"].decode("utf-8")
        lines = [line.strip() for line in exclusions.splitlines() if line.strip() and not line.lstrip().startswith("#")]
        self.assertEqual(
            {".codex/scripts/Invoke-BoundedTest.ps1", ".codex/scripts/stable_runner.py"},
            set(lines),
        )
        self.assertNotIn("maintenance", exclusions)
        self.assertNotIn("tests/", exclusions)

    # ---- fail closed ----

    def test_fail_closed_missing_malformed_non_utf8_and_incomplete_policy(self) -> None:
        _write_policy(self.root, exclusions="task.py\n")
        _script(self.root, "task.py")
        (self.root / ".codex" / "policies" / "bounded-launchers.json").write_text("not json", encoding="utf-8")
        malformed = self._guard("python task.py", cwd=self.root)
        self.assertTrue(_deny(malformed))
        self.assertIn("configuration error", malformed["hookSpecificOutput"]["permissionDecisionReason"])

        _write_policy(self.root, exclusions="task.py\n")
        (self.root / ".codex" / "policies" / "bounded-exclusions.gitignore").write_bytes(b"\xff\xfe")
        non_utf8 = self._guard("python task.py", cwd=self.root)
        self.assertTrue(_deny(non_utf8))

        _write_policy(self.root, launchers={
            "schema": "bounded-launchers/v1",
            "launcher_categories": {
                "python_script": ["python"],
                "powershell_file": ["powershell"],
            },
        }, exclusions="task.py\n")
        incomplete = self._guard("python task.py", cwd=self.root)
        self.assertTrue(_deny(incomplete))
        self.assertIn("configuration error", incomplete["hookSpecificOutput"]["permissionDecisionReason"])

    def test_fail_closed_when_git_cannot_start_or_evaluate(self) -> None:
        _write_policy(self.root, exclusions="task.py\n")
        _script(self.root, "task.py")

        def missing_git(*_args: object, **_kwargs: object) -> object:
            raise FileNotFoundError("git is unavailable")

        with mock.patch("orchestrator_harness.codex_bounded_policy.subprocess.run", side_effect=missing_git):
            result = self._guard("python task.py", cwd=self.root)
        self.assertTrue(_deny(result))
        self.assertIn("configuration error", result["hookSpecificOutput"]["permissionDecisionReason"])

        def failing_git(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess([], 128, "", "fatal: not a git repository")

        with mock.patch("orchestrator_harness.codex_bounded_policy.subprocess.run", side_effect=failing_git):
            result = self._guard("python task.py", cwd=self.root)
        self.assertTrue(_deny(result))
        self.assertIn("git could not evaluate", result["hookSpecificOutput"]["permissionDecisionReason"])

    def test_git_evaluation_requires_a_repository(self) -> None:
        plain = self.root.parent / f"{self.root.name}-plain"
        policies = plain / ".codex" / "policies"
        policies.mkdir(parents=True)
        shutil.copy2(self.root / ".codex" / "policies" / "bounded-launchers.json", policies)
        (policies / "bounded-exclusions.gitignore").write_text("task.py\n", encoding="utf-8")
        (plain / "task.py").write_text("# fixture\n", encoding="utf-8")
        result = guard_pre_tool_use(
            {"cwd": str(plain), "tool_input": {"command": "python task.py"}},
            policy_root=plain,
        )
        self.assertTrue(_deny(result))
        self.assertIn("git could not evaluate", result["hookSpecificOutput"]["permissionDecisionReason"])

    # ---- stateless concurrency / boundary ----

    def test_stateless_concurrent_evaluation(self) -> None:
        _write_policy(self.root, exclusions="launchers/\n")
        _script(self.root, "launchers/start.py")
        payload = {
            "cwd": str(self.root),
            "tool_input": {"command": "python launchers/start.py"},
        }
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: guard_pre_tool_use(payload, policy_root=self.root), range(40)))
        self.assertEqual([{}] * 40, results)

    def test_direct_executables_and_read_only_mentions_stay_outside(self) -> None:
        for command in (
            "pytest -q",
            "ruff check .",
            "basedpyright --project pyrightconfig.json",
            "npm test",
            "Get-Content tools/check.ps1",
            "Get-Content C:\\Python\\python.exe",
            "rg -n 'python -m pytest|basedpyright' AGENTS.md .codex",
        ):
            self.assertEqual({}, self._guard(command), command)

    def test_missing_command_field_is_a_noop(self) -> None:
        self.assertEqual({}, guard_pre_tool_use({"tool_input": {}}, policy_root=self.root))
        self.assertEqual({}, guard_pre_tool_use({}, policy_root=self.root))

    def test_bounded_policy_status_readback(self) -> None:
        _write_policy(self.root, exclusions=".codex/scripts/stable_runner.py\n")
        status = bounded_policy_status(self.root)
        self.assertEqual("orchestrator-codex-bounded-policy/v1", status["schema"])
        self.assertTrue(status["launchers_valid"])
        self.assertTrue(status["exclusions_valid"])
        self.assertIn("python", status["launchers"]["python_script"])
        self.assertIn(".codex/scripts/stable_runner.py", status["exclusion_lines"])
        self.assertIsNone(status["error"])
        (self.root / ".codex" / "policies" / "bounded-launchers.json").write_text("broken", encoding="utf-8")
        broken = bounded_policy_status(self.root)
        self.assertFalse(broken["launchers_valid"])
        self.assertIn("error", broken)
        self.assertIsNotNone(broken["error"])

    def test_lane_managed_stable_runner_exclusion_has_no_session_deadline(self) -> None:
        # The fake orchestration launcher is the only approved Python-script
        # exclusion; the policy readback must show exactly that plus the
        # supervisor entrypoint, and a session command through it is allowed.
        _write_policy(self.root, exclusions=".codex/scripts/stable_runner.py\n.codex/scripts/Invoke-BoundedTest.ps1\n")
        _script(self.root, ".codex/scripts/stable_runner.py")
        session_launch = (
            "python -I .codex/scripts/stable_runner.py "
            "--module orchestrator_harness.lane_controller invocation.json"
        )
        self.assertEqual({}, self._guard(session_launch, cwd=self.root))
        self.assertTrue(_deny(self._guard("python .codex/scripts/verify.py", cwd=self.root)))


class InstalledHookTests(unittest.TestCase):
    """The packaged worktree hook enforces the same policy when installed."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.project = self.root / "project"
        self.project.mkdir()
        subprocess.run(["git", "init", "--quiet", str(self.project)], check=True, capture_output=True, text=True)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _hook_result(self, payload: dict[str, object]) -> dict[str, object]:
        hook = self.project / ".codex" / "hooks" / "orchestrator_harness_bounded_policy.py"
        repo_root = str(Path(__file__).resolve().parents[2])
        env = os.environ.copy()
        env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [sys.executable, str(hook)],
            cwd=self.project,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        return json.loads(completed.stdout)

    def test_installed_hook_denies_nested_covered_commands_and_allows_exclusions(self) -> None:
        install = install_codex_adapter(self.project)
        self.assertTrue(install["current"])
        self.assertTrue((self.project / ".codex" / "policies" / "bounded-launchers.json").is_file())
        _script(self.project, ".codex/scripts/stable_runner.py")
        denied = self._hook_result({
            "cwd": str(self.project),
            "tool_input": {"command": 'python -c "print(1)"'},
        })
        self.assertEqual("deny", denied["hookSpecificOutput"]["permissionDecision"])
        allowed = self._hook_result({
            "cwd": str(self.project),
            "tool_input": {"command": "python -I .codex/scripts/stable_runner.py invocation.json"},
        })
        self.assertEqual({}, allowed)
        denied_appended = self._hook_result({
            "cwd": str(self.project),
            "tool_input": {"command": "python -I .codex/scripts/stable_runner.py invocation.json; python .codex/scripts/verify.py"},
        })
        self.assertEqual("deny", denied_appended["hookSpecificOutput"]["permissionDecision"])


if __name__ == "__main__":
    unittest.main()
