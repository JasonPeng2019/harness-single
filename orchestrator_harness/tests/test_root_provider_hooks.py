from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import root_hook_wrapper as wrapper

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROVIDERS = {
    "codex": ".codex",
    "claude-code": ".claude",
    "qwen-code": ".qwen",
}


class RootHookWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.workspace = Path(self.temporary.name)
        self.dotdir = self.workspace / ".codex"
        self.dotdir.mkdir()
        self.binding = self.dotdir / "orchestrator-harness-binding.json"
        self.binding.write_text(
            json.dumps(
                {
                    "schema": "harness-hook-binding/v1",
                    "role": "root",
                    "provider_id": "codex",
                    "harness_root": str(REPOSITORY_ROOT.resolve()),
                    "runtime_root": str((self.workspace / "runtime").resolve()),
                    "liveness_path": ".agent-workspace/root-hook-liveness.jsonl",
                    "delivery_receipt_path": ".agent-workspace/root-hook-delivery.jsonl",
                }
            ),
            encoding="utf-8",
        )

    def test_notice_is_translated_and_liveness_is_recorded(self) -> None:
        notice = {
            "provider_id": "codex",
            "unresolved_count": 2,
            "event_classes": ["LANE_STATUS_CHANGED"],
            "at": "now",
        }
        with patch.object(
            wrapper, "dispatch", return_value={"decision": "NOTICE", "notice": notice}
        ) as dispatch:
            output = wrapper.run("codex", "post-tool-use", self.binding)
        dispatch.assert_called_once_with(REPOSITORY_ROOT.resolve(), "post-tool-use", "codex")
        self.assertEqual(
            output["hookSpecificOutput"]["hookEventName"], "PostToolUse"
        )
        self.assertIn('"unresolved_count":2', output["hookSpecificOutput"]["additionalContext"])
        liveness = self.workspace / ".agent-workspace" / "root-hook-liveness.jsonl"
        record = json.loads(liveness.read_text(encoding="utf-8").strip())
        self.assertEqual("root", record["role"])
        self.assertEqual("codex", record["provider_id"])

    def test_stop_rejects_and_missing_binding_fails_closed(self) -> None:
        with patch.object(
            wrapper,
            "dispatch",
            return_value={"decision": "REJECT", "reason": "unresolved"},
        ):
            self.assertEqual(
                {"decision": "block", "reason": "unresolved"},
                wrapper.run("codex", "stop", self.binding),
            )
        self.binding.unlink()
        result = wrapper.run("codex", "stop", self.binding)
        self.assertEqual("block", result["decision"])
        self.assertIn("installed codex hook has no harness binding", result["reason"])

    def test_delivery_receipt_uses_bound_runtime(self) -> None:
        runtime = (self.workspace / "runtime").resolve()
        with (
            patch.dict(os.environ, {"HARNESS_EVENT_ID": "event-1"}),
            patch.object(wrapper, "dispatch", return_value={"decision": "ALLOW"}),
            patch.object(wrapper, "append_delivery_history") as delivery,
        ):
            self.assertEqual({}, wrapper.run("codex", "post-tool-use", self.binding))
        delivery.assert_called_once_with(runtime, "event-1")
        receipt = json.loads(
            (self.workspace / ".agent-workspace" / "root-hook-delivery.jsonl")
            .read_text(encoding="utf-8")
            .strip()
        )
        self.assertEqual("DELIVERED", receipt["outcome"])
        self.assertEqual("event-1", receipt["event_id"])


class RootProviderPayloadTests(unittest.TestCase):
    def test_restored_codex_root_hooks_delegate_to_worker_binding(self) -> None:
        """A Git-restored tracked hook must remain usable inside a worker lane."""
        source_hooks = (
            REPOSITORY_ROOT / "adapters" / "codex" / "root" / ".codex" / "hooks"
        )
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            installed_hooks = workspace / ".codex" / "hooks"
            installed_hooks.mkdir(parents=True)
            agent_workspace = workspace / ".agent-workspace"
            agent_workspace.mkdir()
            (agent_workspace / "harness-hook-binding.json").write_text(
                json.dumps(
                    {
                        "schema": "harness-hook-binding/v1",
                        "role": "worker",
                        "lane_id": "lane-1",
                        "run_id": "run-1",
                    }
                ),
                encoding="utf-8",
            )
            (agent_workspace / "hook-dispatch.py").write_text(
                "import json\nprint(json.dumps({'decision': 'ALLOW'}))\n",
                encoding="utf-8",
            )

            for filename in (
                "orchestrator_harness_post_tool_use.py",
                "orchestrator_harness_stop.py",
            ):
                with self.subTest(filename=filename):
                    installed = installed_hooks / filename
                    shutil.copy2(source_hooks / filename, installed)
                    completed = subprocess.run(
                        [sys.executable, str(installed)],
                        cwd=workspace,
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    self.assertEqual({}, json.loads(completed.stdout))

    def test_native_root_wrappers_and_declarations(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            with self.subTest(provider=provider_id):
                root = REPOSITORY_ROOT / "adapters" / provider_id / "root" / dotdir
                hooks = root / "hooks"
                self.assertTrue((hooks / "orchestrator_harness_post_tool_use.py").is_file())
                self.assertTrue((hooks / "orchestrator_harness_stop.py").is_file())
                self.assertFalse((hooks / "post-tool-use.py").exists())
                settings_path = root / ("hooks.json" if provider_id == "codex" else "settings.json")
                settings = json.loads(settings_path.read_text(encoding="utf-8"))
                declarations = settings["hooks"]
                for event, filename in (
                    ("PostToolUse", "orchestrator_harness_post_tool_use.py"),
                    ("Stop", "orchestrator_harness_stop.py"),
                ):
                    commands = [item["command"] for group in declarations[event] for item in group["hooks"]]
                    self.assertTrue(any(filename in command for command in commands))
                if provider_id == "qwen-code":
                    self.assertTrue((hooks / "orchestrator_harness_notification.py").is_file())
                    commands = [item["command"] for group in declarations["Notification"] for item in group["hooks"]]
                    self.assertTrue(any("orchestrator_harness_notification.py" in command for command in commands))

    def test_every_wrapper_imports_with_unbound_template(self) -> None:
        filenames = (
            "orchestrator_harness_post_tool_use.py",
            "orchestrator_harness_stop.py",
        )
        for provider_id, dotdir in PROVIDERS.items():
            for filename in filenames:
                path = REPOSITORY_ROOT / "adapters" / provider_id / "root" / dotdir / "hooks" / filename
                spec = importlib.util.spec_from_file_location(
                    f"_root_{provider_id}_{filename}", path
                )
                self.assertIsNotNone(spec)
                self.assertIsNotNone(spec.loader)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.assertTrue(callable(module.main))


if __name__ == "__main__":
    unittest.main()
