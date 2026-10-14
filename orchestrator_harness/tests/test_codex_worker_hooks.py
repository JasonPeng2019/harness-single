"""RED specification for the Codex worker hook wrappers.

The super-cache worker payload declares two Codex hooks (PostToolUse and Stop)
that must be implemented by thin wrapper scripts under
adapters/codex/super-cache/.codex/hooks/.  The payload is copied unchanged
into an arbitrary worker worktree, so each wrapper resolves its agent
workspace relative to its installed location, asks the shared
hook-dispatch.py for a decision, and translates that decision into the Codex
hook output contract.  This module is the test-first RED slice: the unittest
run must fail until the wrapper files exist.
"""

import importlib.util
import json
import unittest
from pathlib import Path
from unittest import mock

WORKTREE_ROOT = Path(__file__).resolve().parents[2]
SUPER_CACHE_DIR = WORKTREE_ROOT / "adapters" / "codex" / "super-cache"
HOOKS_DIR = SUPER_CACHE_DIR / ".codex" / "hooks"
HOOKS_JSON_PATH = SUPER_CACHE_DIR / ".codex" / "hooks.json"
POST_TOOL_USE_WRAPPER_PATH = HOOKS_DIR / "orchestrator_harness_post_tool_use.py"
STOP_WRAPPER_PATH = HOOKS_DIR / "orchestrator_harness_stop.py"
AGENT_WORKSPACE = HOOKS_DIR.parents[1] / ".agent-workspace"
HOOK_DISPATCH_PATH = AGENT_WORKSPACE / "hook-dispatch.py"

POST_TOOL_USE_BOUNDARY = "post-tool-use"
STOP_BOUNDARY = "stop"
OLD_HOOK_SCRIPT = "post-tool-use.py"


def _load_wrapper(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise AssertionError("RED: wrapper not found at %s" % path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_post_tool_use_wrapper():
    return _load_wrapper(POST_TOOL_USE_WRAPPER_PATH, "codex_worker_post_tool_use")


def _load_stop_wrapper():
    return _load_wrapper(STOP_WRAPPER_PATH, "codex_worker_stop")


def _declared_commands():
    declaration = json.loads(HOOKS_JSON_PATH.read_text(encoding="utf-8"))
    return [
        hook["command"]
        for event_hooks in declaration["hooks"].values()
        for matcher in event_hooks
        for hook in matcher["hooks"]
    ]


def _invoked_command(run):
    if run.call_args.args:
        return [str(part) for part in run.call_args.args[0]]
    return [str(part) for part in run.call_args.kwargs["args"]]


class CodexWorkerHooksTestCase(unittest.TestCase):
    def test_wrapper_files_exist(self):
        self.assertTrue(
            POST_TOOL_USE_WRAPPER_PATH.is_file(),
            "RED: missing %s" % POST_TOOL_USE_WRAPPER_PATH,
        )
        self.assertTrue(
            STOP_WRAPPER_PATH.is_file(),
            "RED: missing %s" % STOP_WRAPPER_PATH,
        )

    def test_declaration_wires_exact_wrapper_scripts(self):
        commands = _declared_commands()
        self.assertIn(
            "python .codex/hooks/orchestrator_harness_post_tool_use.py", commands
        )
        self.assertIn("python .codex/hooks/orchestrator_harness_stop.py", commands)

    def test_old_hook_script_is_absent_from_declaration(self):
        for command in _declared_commands():
            self.assertNotIn(OLD_HOOK_SCRIPT, command)

    def test_old_hook_script_is_absent_from_hooks_dir(self):
        self.assertFalse(
            (HOOKS_DIR / OLD_HOOK_SCRIPT).is_file(),
            "GREEN must delete the superseded %s" % (HOOKS_DIR / OLD_HOOK_SCRIPT),
        )

    def test_wrappers_are_importable(self):
        _load_post_tool_use_wrapper()
        _load_stop_wrapper()

    def test_post_tool_use_allow_translates_to_empty_output(self):
        wrapper = _load_post_tool_use_wrapper()
        self.assertEqual(wrapper.translate({"decision": "ALLOW"}), {})

    def test_post_tool_use_notice_translates_to_hook_specific_output(self):
        wrapper = _load_post_tool_use_wrapper()
        output = wrapper.translate(
            {"decision": "NOTICE", "notice": ["evt-pending-1", "evt-pending-2"]}
        )
        specific = output["hookSpecificOutput"]
        self.assertEqual(specific["hookEventName"], "PostToolUse")
        self.assertTrue(specific["additionalContext"])
        self.assertIn("evt-pending-1", specific["additionalContext"])
        self.assertIn("evt-pending-2", specific["additionalContext"])

    def test_post_tool_use_reject_translates_to_stop(self):
        wrapper = _load_post_tool_use_wrapper()
        output = wrapper.translate({"decision": "REJECT", "reason": "binding missing"})
        self.assertIs(output["continue"], False)
        self.assertTrue(output["stopReason"])

    def test_stop_allow_translates_to_empty_output(self):
        wrapper = _load_stop_wrapper()
        self.assertEqual(wrapper.translate({"decision": "ALLOW"}), {})

    def test_stop_reject_translates_to_block(self):
        wrapper = _load_stop_wrapper()
        output = wrapper.translate(
            {"decision": "REJECT", "reason": "unresolved assignments"}
        )
        self.assertEqual(output["decision"], "block")
        self.assertTrue(output["reason"])

    def test_post_tool_use_agent_workspace(self):
        wrapper = _load_post_tool_use_wrapper()
        self.assertEqual(
            Path(wrapper.agent_workspace()).resolve(), AGENT_WORKSPACE.resolve()
        )

    def test_stop_agent_workspace(self):
        wrapper = _load_stop_wrapper()
        self.assertEqual(
            Path(wrapper.agent_workspace()).resolve(), AGENT_WORKSPACE.resolve()
        )

    def test_post_tool_use_dispatch_calls_shared_dispatcher_with_own_boundary(self):
        wrapper = _load_post_tool_use_wrapper()
        with mock.patch("subprocess.run") as run:
            run.return_value = mock.Mock(
                stdout=json.dumps({"decision": "ALLOW"}), returncode=0
            )
            decision = wrapper.dispatch_decision()
        self.assertEqual(decision, {"decision": "ALLOW"})
        run.assert_called_once()
        command = _invoked_command(run)
        self.assertIn(str(HOOK_DISPATCH_PATH), command)
        self.assertIn("--boundary", command)
        self.assertEqual(
            command[command.index("--boundary") + 1], POST_TOOL_USE_BOUNDARY
        )
        self.assertNotIn(STOP_BOUNDARY, command)

    def test_stop_dispatch_calls_shared_dispatcher_with_own_boundary(self):
        wrapper = _load_stop_wrapper()
        with mock.patch("subprocess.run") as run:
            run.return_value = mock.Mock(
                stdout=json.dumps({"decision": "ALLOW"}), returncode=0
            )
            decision = wrapper.dispatch_decision()
        self.assertEqual(decision, {"decision": "ALLOW"})
        run.assert_called_once()
        command = _invoked_command(run)
        self.assertIn(str(HOOK_DISPATCH_PATH), command)
        self.assertIn("--boundary", command)
        self.assertEqual(
            command[command.index("--boundary") + 1], STOP_BOUNDARY
        )
        self.assertNotIn(POST_TOOL_USE_BOUNDARY, command)


if __name__ == "__main__":
    unittest.main()
