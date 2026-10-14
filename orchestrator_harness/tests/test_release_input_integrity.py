"""Release credit must cover actual source and executable target arguments."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from orchestrator_harness import release_checks as checks


class ReleaseInputIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "-q")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                 "commit", "--allow-empty", "-qm", "base")
        (self.root / "source").mkdir()
        (self.root / "source/checked.py").write_text("answer = 1\n", encoding="utf-8")
        self.git("add", "source/checked.py")
        self.spec = checks.CheckSpec("fixture-source", "source discovery", "fast",
                                     ("python", "-m", "unittest"), (), (),
                                     input_scopes=(checks.InputScope("tree", "source"),))

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.root), *arguments], text=True).strip()

    def fingerprint(self):
        return checks.dependency_fingerprint(self.spec, self.root)

    def test_added_changed_and_removed_untracked_source_invalidates_credit(self):
        original = self.fingerprint()
        added = self.root / "source/test_added.py"
        added.write_text("answer = 2\n", encoding="utf-8")
        first = self.fingerprint()
        self.assertNotEqual(original, first)
        self.assertIn("source/test_added.py", checks.resolve_input_scope(self.spec, self.root))
        added.write_text("answer = 3\n", encoding="utf-8")
        self.assertNotEqual(first, self.fingerprint())
        added.unlink()
        self.assertEqual(original, self.fingerprint())

    def test_git_ignored_python_is_still_a_discoverable_source_input(self):
        (self.root / ".gitignore").write_text("source/test_ignored.py\n", encoding="utf-8")
        original = self.fingerprint()
        (self.root / "source/test_ignored.py").write_text("assert True\n", encoding="utf-8")
        self.assertIn("source/test_ignored.py", checks.resolve_input_scope(self.spec, self.root))
        self.assertNotEqual(original, self.fingerprint())

    def test_hardlinked_source_cannot_supply_ambiguous_credit_bytes(self):
        os.link(self.root / "source/checked.py", self.root / "source/alias.py")
        with self.assertRaises(checks.SelectionError):
            self.fingerprint()

    def test_owned_runtime_and_bytecode_are_not_source_inputs(self):
        self.spec = checks.CheckSpec("fixture-all", "Python source", "fast",
                                     ("python", "-m", "unittest"), (), (),
                                     input_scopes=(checks.InputScope("glob", "**/*.py"),))
        original = self.fingerprint()
        for relative in ("runtime/worker.py", "local-config/provider.py", "source/__pycache__/cache.py"):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("owned output\n", encoding="utf-8")
        self.assertEqual(original, self.fingerprint())

    def test_shipped_hidden_payload_is_source_not_worker_runtime(self):
        payload = self.root / "super-cache/workspace/.agent-workspace/hook.py"
        payload.parent.mkdir(parents=True)
        payload.write_text("answer = 1\n", encoding="utf-8")
        spec = checks.CheckSpec("fixture-payload", "installed hooks", "fast",
                                ("python", "-m", "unittest"), (), (),
                                input_scopes=(checks.InputScope("tree", "super-cache"),))
        self.assertIn(payload.relative_to(self.root).as_posix(), checks.resolve_input_scope(spec, self.root))
        original = checks.dependency_fingerprint(spec, self.root)
        payload.write_text("answer = 2\n", encoding="utf-8")
        self.assertNotEqual(original, checks.dependency_fingerprint(spec, self.root))

    def test_unit_and_aggregate_scopes_cover_consumed_provider_and_cache_trees(self):
        for scopes in (checks._ORCHESTRATOR_UNIT_INPUT_SCOPES, checks._RELEASE_AGGREGATE_INPUT_SCOPES):
            for relative in ("adapters/codex/root/AGENTS.md", "super-cache/workspace/.agent-workspace/hook.py"):
                self.assertTrue(any(checks._scope_matches(scope, relative) for scope in scopes), relative)
        self.assertTrue(any(checks._scope_matches(scope, "orchestrator_harness/processes.py")
                            for scope in checks._WATCHER_UNIT_INPUT_SCOPES))

    def test_bound_aggregate_has_exact_root_branch_and_tip_as_literal_arguments(self):
        source = checks.read_source_identity(self.root)
        spec = checks.get_check(checks.RELEASE_AGGREGATE_ID)
        command = checks.command_for_source(spec, source)
        self.assertEqual(source.source_root, command[command.index("-RepositoryRoot") + 1])
        self.assertEqual(source.branch, command[command.index("-ExpectedBranch") + 1])
        self.assertEqual(source.tip, command[command.index("-ExpectedTip") + 1])
        self.assertFalse(any(part in checks._COMMAND_SOURCE_FIELDS for part in command))
        self.assertFalse(spec.to_record(source)["command_requires_source"])

    def test_source_dependent_registry_preserves_definition_fingerprint(self):
        definition = checks.registry_record()
        bound = checks.registry_record(self.root)
        self.assertEqual(definition["registry_fingerprint"], bound["registry_fingerprint"])
        plain_aggregate = next(x for x in definition["checks"] if x["stable_id"] == checks.RELEASE_AGGREGATE_ID)
        aggregate = next(x for x in bound["checks"] if x["stable_id"] == checks.RELEASE_AGGREGATE_ID)
        self.assertTrue(plain_aggregate["command_requires_source"])
        self.assertFalse(aggregate["command_requires_source"])
        self.assertEqual(plain_aggregate["command"], aggregate["command_template"])

    def test_selection_and_credit_report_the_executable_bound_command(self):
        source = checks.read_source_identity(self.root)
        spec = checks.CheckSpec("fixture-command", "source-bound command", "release",
                                ("powershell", "-ExpectedBranch", "{branch}", "-ExpectedTip", "{tip}"), (), ())
        selected = checks.SelectedCheck(spec, "a" * 64, "missing-credit")
        decision = checks.SelectionDecision("release", source, (selected,), (), (), (), (),
                                             "b" * 64, (), ())
        expected = list(checks.command_for_source(spec, source))
        self.assertEqual(expected, decision.to_record()["selected"][0]["command"])
        credit = checks.credit_record(spec, self.root)
        disposition = checks.disposition_record(spec, self.root, status="SKIP", reason="fixture only")
        self.assertEqual(expected, credit["command"])
        self.assertEqual(expected, disposition["command"])
        json.dumps(decision.to_record())

    def test_argument_binding_does_not_interpret_branch_shell_syntax(self):
        source = checks.SourceIdentity(str(self.root), str(self.root / ".git"),
                                       "feature/$(whoami)", "a" * 40)
        command = checks.command_for_source(checks.get_check(checks.RELEASE_AGGREGATE_ID), source)
        self.assertEqual("feature/$(whoami)", command[command.index("-ExpectedBranch") + 1])

    def test_native_registry_cli_returns_bound_aggregate_arguments(self):
        import sys
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
        output = subprocess.check_output(
            [sys.executable, "-B", "-m", "orchestrator_harness.release_checks", "registry", "--root", str(self.root)],
            cwd=self.root, env=environment, text=True)
        registry = json.loads(output)
        aggregate = next(x for x in registry["checks"] if x["stable_id"] == checks.RELEASE_AGGREGATE_ID)
        self.assertFalse(aggregate["command_requires_source"])
        command = aggregate["command"]
        self.assertEqual(self.git("branch", "--show-current"), command[command.index("-ExpectedBranch") + 1])
        self.assertEqual(self.git("rev-parse", "HEAD"), command[command.index("-ExpectedTip") + 1])


if __name__ == "__main__":
    unittest.main()
