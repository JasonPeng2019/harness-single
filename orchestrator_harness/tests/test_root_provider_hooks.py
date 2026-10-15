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
from types import SimpleNamespace

from orchestrator_harness import root_hook_wrapper as wrapper
from orchestrator_harness.setup import _materialized_binding
from orchestrator_harness.tests.support import PreparedRootFixture

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROVIDERS = {
    "codex": ".codex",
    "claude-code": ".claude",
    "qwen-code": ".qwen",
}


class RootHookWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = PreparedRootFixture(self)
        self.workspace = self.fixture.manager
        self.harness = self.fixture.harness
        self.product = self.fixture.product
        environment = patch.dict(os.environ, self.fixture.environment, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        self.dotdir = self.workspace / ".codex"
        self.dotdir.mkdir()
        self.binding = self.dotdir / "orchestrator-harness-binding.json"
        self.binding.write_text(
            json.dumps(
                _materialized_binding({
                    "schema": "harness-hook-binding/v1",
                    "role": "root",
                    "provider_id": "codex",
                    "harness_root": None,
                    "runtime_root": None,
                    "liveness_path": ".agent-workspace/root-hook-liveness.jsonl",
                    "delivery_receipt_path": ".agent-workspace/root-hook-delivery.jsonl",
                }, self.harness, self.product / ".harness-runtime")
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
        dispatch.assert_called_once_with(self.harness.resolve(), "post-tool-use", "codex")
        self.assertEqual(
            output["hookSpecificOutput"]["hookEventName"], "PostToolUse"
        )
        self.assertIn('"unresolved_count":2', output["hookSpecificOutput"]["additionalContext"])
        liveness = self.workspace / ".agent-workspace" / "root-hook-liveness.jsonl"
        record = json.loads(liveness.read_text(encoding="utf-8").strip())
        self.assertEqual("root", record["role"])
        self.assertEqual("codex", record["provider_id"])
        self.assertEqual(self.fixture.session["session_id"], record["session_id"])
        self.assertEqual("codex", record["binding_id"])

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
        runtime = (self.product / ".harness-runtime").resolve()
        with (
            patch.dict(os.environ, {"HARNESS_EVENT_ID": "event-1"}),
            patch.object(wrapper, "dispatch", return_value={"decision": "ALLOW"}),
            patch.object(wrapper, "append_delivery_history") as delivery,
        ):
            self.assertEqual({}, wrapper.run("codex", "post-tool-use", self.binding))
        delivery.assert_called_once_with(runtime, "event-1", session_id=self.fixture.session["session_id"], binding_id="codex")
        receipt = json.loads(
            (self.workspace / ".agent-workspace" / "root-hook-delivery.jsonl")
            .read_text(encoding="utf-8")
            .strip()
        )
        self.assertEqual("DELIVERED", receipt["outcome"])
        self.assertEqual("event-1", receipt["event_id"])
        self.assertEqual(self.fixture.session["session_id"], receipt["session_id"])

    def test_session_mismatch_fails_before_dispatch_and_liveness(self) -> None:
        with patch.dict(os.environ, {"HARNESS_ROOT_SESSION_ID": "another-root"}), patch.object(wrapper, "dispatch") as dispatch:
            result = wrapper.run("codex", "post-tool-use", self.binding)
        self.assertIs(result["continue"], False)
        self.assertIn("ROOT_SESSION_MISMATCH", result["stopReason"])
        dispatch.assert_not_called()
        self.assertFalse((self.workspace / ".agent-workspace/root-hook-liveness.jsonl").exists())

    def test_actual_hook_delivery_and_public_ack_share_the_prepared_scope(self) -> None:
        from orchestrator_harness.manager_queue import promote_event, read_manager_queue
        event = promote_event(self.fixture.rt, event_type="COMPLETION_REVIEW_REQUIRED",
                    event_class="COMPLETION_REVIEW_REQUIRED", lane_id="unit-lane", run_id="unit-run",
                    summary="unit unresolved review")
        output = wrapper.run("codex", "post-tool-use", self.binding)
        self.assertIn("hookSpecificOutput", output)
        current = read_manager_queue(self.fixture.rt)["events"][0]
        self.assertEqual("PENDING", current["state"])
        receipt = current["delivery_history"][0]
        self.assertEqual(event["event_id"], receipt["event_id"])
        self.assertEqual(self.fixture.session["session_id"], receipt["session_id"])
        self.assertEqual("codex", receipt["binding_id"])
        self.assertEqual("hook", receipt["source"])
        ack = self.fixture.command("manager", "acknowledge", "--event-id", event["event_id"])
        self.assertEqual(0, ack.returncode, ack.stdout + ack.stderr)
        watched = self.fixture.command("watch", "--until-actionable", "--timeout", "0s")
        self.assertEqual("WATCH_TIMEOUT", json.loads(watched.stdout)["code"])
        self.assertEqual("ACKNOWLEDGED", read_manager_queue(self.fixture.rt)["events"][0]["state"])

    def test_plain_hook_is_queue_free_without_prepared_session(self) -> None:
        from orchestrator_harness.tests.support import write_json
        from orchestrator_harness.root_session import root_session_path
        root_session_path(self.fixture.config).unlink()
        config_path = self.harness / "harness-config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        write_json(config_path, {**config, "managed_coordination": "disabled"})
        binding = json.loads(self.binding.read_text(encoding="utf-8"))
        write_json(self.binding, _materialized_binding(binding, self.harness, self.fixture.rt))
        with patch.dict(os.environ, {"HARNESS_EVENT_ID": "stale-event"}), patch.object(wrapper, "append_delivery_history") as delivery:
            self.assertEqual({}, wrapper.run("codex", "post-tool-use", self.binding))
        delivery.assert_not_called()


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
