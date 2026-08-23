"""Focused regression tests for provider optional-field handling.

``invocation._provider`` applies the provider-specific optional-field
allow-list to every provider.id, so ``config_overrides`` / ``service_tier`` /
``approval_policy`` were accepted for ``claude-code`` but
``ClaudeCodeProviderAdapter.build_argv`` never read any of the three -- accepted
then silently dropped.

Contract under test (honor + reject, no silent drop):
1. ``config_overrides`` for claude-code is HONORED via the child-process env
   channel: ``_provider_launch_spec`` translates it into
   ``ProviderLaunchSpec.env_overrides`` (the ``model_provider="ollama"`` alias
   expands to the three Ollama redirect variables; explicit ``ANTHROPIC_*``
   entries pass verbatim), and the controller merges those entries into the
   child env AFTER isolation/allow-list filtering.
2. ``service_tier`` / ``approval_policy`` on claude-code raise
   ``InvocationValidationError`` naming the offending field(s): Claude Code has
   no service-tier or approval-policy flags.
3. The Codex adapter argv is unchanged (still carries all three via ``-c``).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.invocation import (
    CANONICAL_INVOCATION_SCHEMA,
    InvocationValidationError,
    parse_canonical_invocation,
)
from orchestrator_harness.profile import RuntimeProfile
from orchestrator_harness.provider import (
    ClaudeCodeProviderAdapter,
    CodexProviderAdapter,
    ProviderAdapterError,
    ProviderLaunchSpec,
    claude_config_override_env,
)
from orchestrator_harness.prompt_bundle import prompt_bundle_record_from_paths
from orchestrator_harness.task import TASK_CARD_SCHEMA, record_sha256
from orchestrator_harness.tests.support import prepare_fixture_overlay_receipt

FAKE_CLAUDE = r"""
import json, os, sys
envdump = sys.argv[1]
with open(envdump, 'w', encoding='utf-8') as f:
    json.dump(
        {k: os.environ.get(k) for k in (
            'ANTHROPIC_BASE_URL', 'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_API_KEY')},
        f, sort_keys=True, indent=2,
    )
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'session-env'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': 'session-env'}), flush=True)
"""


def _canonical(
    root: Path, provider_id: str, *, with_overlay: bool = False
) -> dict[str, object]:
    run = root / "run"
    workspace = run / ".agent-workspace"
    workspace.mkdir(parents=True)
    runtime = root / "runtime"
    runtime.mkdir()
    prompt_a = run / "prompt-a.md"
    prompt_b = run / "prompt-b.md"
    prompt_a.write_bytes(b"workflow\n")
    prompt_b.write_bytes(b"task\n")
    card = {
        "schema": TASK_CARD_SCHEMA,
        "card_id": "card-1",
        "lane_id": "lane-1",
        "stage_cohort_id": "cohort-1",
        "worker_invocation_id": "worker-1",
        "objective": "Do the bounded task",
        "revision": "r1",
    }
    profile = RuntimeProfile(
        "profile-1",
        "implementer",
        provider_id,
        "deepseek-v4-flash:0731-cloud",
        ("Read",),
        ("repo",),
        ("resource-1",),
        (),
        (),
    )
    bundle = prompt_bundle_record_from_paths(
        workflow_id="workflow-1",
        task_card_id="card-1",
        profile_id="profile-1",
        paths=(("instructions", prompt_a), ("task", prompt_b)),
        run_root=run,
    )
    value: dict[str, object] = {
        "schema": CANONICAL_INVOCATION_SCHEMA,
        "action": "start",
        "run_root": str(run),
        "runtime_root": str(runtime),
        "lane_id": "lane-1",
        "worker_invocation_id": "worker-1",
        "cohort_id": "cohort-1",
        "workflow": {"id": "workflow-1", "version": "1"},
        "task_card": {"id": "card-1", "revision": "r1", "sha256": record_sha256(card)},
        "role": "implementer",
        "provider": {
            "id": provider_id,
            "model": "deepseek-v4-flash:0731-cloud",
            "command": [provider_id],
            "config_overrides": ['model_provider="ollama"'],
            "service_tier": "standard",
            "approval_policy": "never",
        },
        "profile": profile.to_record(),
        "prompt_bundle": bundle,
        "output_paths": {
            "status": str(workspace / "worker_controller.status.json"),
            "jsonl": str(workspace / "worker_provider.jsonl"),
            "stderr": str(workspace / "worker.stderr.log"),
            "last_message": str(workspace / "worker.last-message"),
        },
        "event_log_path": str(runtime / "events.jsonl"),
        "resources": ["resource-1"],
    }
    if with_overlay:
        value["overlay_receipt"] = str(
            prepare_fixture_overlay_receipt(root, run)
        )
    return value


def _provider(raw: dict[str, object]) -> dict[str, object]:
    return cast(dict[str, object], raw["provider"])


class ClaudeCodeOptionalFieldAllowlistTests(unittest.TestCase):
    def test_claude_code_service_tier_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw = _canonical(Path(temporary) / "root", "claude-code")
            del _provider(raw)["config_overrides"]
            with self.assertRaisesRegex(
                InvocationValidationError, "service_tier"
            ):
                parse_canonical_invocation(raw)

    def test_claude_code_approval_policy_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw = _canonical(Path(temporary) / "root", "claude-code")
            provider = _provider(raw)
            del provider["config_overrides"]
            del provider["service_tier"]
            with self.assertRaisesRegex(
                InvocationValidationError, "approval_policy"
            ):
                parse_canonical_invocation(raw)

    def test_claude_code_both_fields_named_together(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw = _canonical(Path(temporary) / "root", "claude-code")
            del _provider(raw)["config_overrides"]
            with self.assertRaisesRegex(
                InvocationValidationError,
                "provider.claude-code has no equivalent for field.*"
                "provider.approval_policy, provider.service_tier",
            ):
                parse_canonical_invocation(raw)

    def test_codex_same_fields_stay_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw = _canonical(Path(temporary) / "root", "codex")
            canonical = parse_canonical_invocation(raw)
            self.assertEqual("codex", canonical.provider_id)

    def test_claude_code_config_overrides_populate_env_channel(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = _canonical(root / "root", "claude-code")
            provider = _provider(raw)
            del provider["service_tier"]
            del provider["approval_policy"]
            path = root / "claude.invocation.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            invocation = controller.load_invocation(path)
            spec = controller._provider_launch_spec(invocation, None)
            self.assertEqual(
                {
                    "ANTHROPIC_BASE_URL": "http://localhost:11434",
                    "ANTHROPIC_AUTH_TOKEN": "ollama",
                    "ANTHROPIC_API_KEY": "",
                },
                dict(spec.env_overrides),
            )
            # build_argv still validates the translation (fail loud), and the
            # argv itself stays claude-shaped (no -c entries).
            argv = ClaudeCodeProviderAdapter().build_argv(spec)
            self.assertNotIn('-c', argv)
            self.assertNotIn('model_provider="ollama"', " ".join(argv))

    def test_claude_code_explicit_anthropic_override_passes_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = _canonical(root / "root", "claude-code")
            provider = _provider(raw)
            del provider["service_tier"]
            del provider["approval_policy"]
            provider["config_overrides"] = [
                "ANTHROPIC_BASE_URL=http://localhost:9999",
                'ANTHROPIC_API_KEY=""',
            ]
            path = root / "claude-explicit.invocation.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            invocation = controller.load_invocation(path)
            spec = controller._provider_launch_spec(invocation, None)
            self.assertEqual(
                {
                    "ANTHROPIC_BASE_URL": "http://localhost:9999",
                    "ANTHROPIC_API_KEY": "",
                },
                dict(spec.env_overrides),
            )

    def test_claude_code_unknown_override_is_rejected_not_dropped(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = _canonical(root / "root", "claude-code")
            provider = _provider(raw)
            del provider["service_tier"]
            del provider["approval_policy"]
            provider["config_overrides"] = ["feature_flag=true"]
            path = root / "claude-bad.invocation.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            invocation = controller.load_invocation(path)
            # The controller path fails loud before any adapter work.
            with self.assertRaisesRegex(controller.InvocationError, "ANTHROPIC"):
                controller._provider_launch_spec(invocation, None)
            # The standalone adapter contract path is fail-loud as well.
            spec = ProviderLaunchSpec(
                action="start",
                command=("claude",),
                model="m",
                reasoning_effort="medium",
                service_tier="standard",
                session_id=None,
                run_root=Path("C:/run"),
                last_message_path=Path("C:/run/.agent-workspace/last"),
                config_overrides=("feature_flag=true",),
            )
            with self.assertRaisesRegex(ProviderAdapterError, "ANTHROPIC"):
                ClaudeCodeProviderAdapter().build_argv(spec)
            with self.assertRaises(ProviderAdapterError):
                claude_config_override_env("model_provider=other-backend")

    def test_env_overrides_merge_only_declared_keys_after_isolation(self) -> None:
        inherited = {
            "PATH": "C:/bin",
            "SYSTEMROOT": "C:/Windows",
            "ANTHROPIC_AUTH_TOKEN": "inherited-secret",
            "GITHUB_TOKEN": "ambient-secret",
        }
        isolated, cleared = controller.isolated_coding_child_environment(inherited)
        # The allow-list drops every non-listed variable (including inherited
        # ANTHROPIC_* and credential-ish names); ``cleared`` only reports the
        # physical-lane prefixes.  The isolation guarantee is that neither the
        # inherited token nor the ambient token is present in the child env.
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", isolated)
        self.assertNotIn("GITHUB_TOKEN", isolated)
        self.assertEqual(["PATH", "SYSTEMROOT"], sorted(isolated))
        self.assertEqual([], cleared)
        merged = controller.apply_provider_env_overrides(
            isolated,
            {
                "ANTHROPIC_BASE_URL": "http://localhost:11434",
                "ANTHROPIC_AUTH_TOKEN": "ollama",
                "ANTHROPIC_API_KEY": "",
            },
        )
        self.assertEqual(
            {
                "PATH": "C:/bin",
                "SYSTEMROOT": "C:/Windows",
                "ANTHROPIC_BASE_URL": "http://localhost:11434",
                "ANTHROPIC_AUTH_TOKEN": "ollama",
                "ANTHROPIC_API_KEY": "",
            },
            merged,
        )
        # No overrides -> the isolated env is returned untouched.
        self.assertEqual(
            isolated,
            controller.apply_provider_env_overrides(isolated, {}),
        )

    def test_env_overrides_merge_keeps_parent_env_when_unisolated(self) -> None:
        with patch.dict(os.environ, {"PATH": "C:/bin"}, clear=False):
            merged = controller.apply_provider_env_overrides(
                None, {"ANTHROPIC_BASE_URL": "http://localhost:11434"}
            )
        assert merged is not None
        self.assertEqual("http://localhost:11434", merged["ANTHROPIC_BASE_URL"])
        self.assertIn("PATH", merged)

    def test_codex_argv_unchanged_still_carries_all_three(self) -> None:
        spec = ProviderLaunchSpec(
            action="start",
            command=("codex",),
            model="deepseek-v4-flash:0731-cloud",
            reasoning_effort="medium",
            service_tier="standard",
            session_id=None,
            run_root=Path("C:/run"),
            last_message_path=Path("C:/run/.agent-workspace/last"),
            config_overrides=('model_provider="ollama"',),
            approval_policy="never",
        )
        argv = " ".join(CodexProviderAdapter().build_argv(spec))
        self.assertIn('-c model_provider="ollama"', argv)
        self.assertIn('-c service_tier="standard"', argv)
        self.assertIn('-c approval_policy="never"', argv)

    def test_claude_lane_env_redirect_through_controller(self) -> None:
        # Full spawn-path proof: a claude-code lane with the ollama alias
        # reaches the child as the three ANTHROPIC_* env entries, the inherited
        # ANTHROPIC_AUTH_TOKEN is replaced (never passed through), and the lane
        # completes with system/init + result/success.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root / "fake_claude_env.py"
            fake.write_text(FAKE_CLAUDE, encoding="utf-8")
            envdump = root / "child-env.json"
            raw = _canonical(root / "root", "claude-code", with_overlay=True)
            provider = _provider(raw)
            del provider["service_tier"]
            del provider["approval_policy"]
            provider["command"] = [sys.executable, str(fake), str(envdump)]
            os.environ["ANTHROPIC_AUTH_TOKEN"] = "inherited-secret-must-not-leak"
            invocation_path = root / "lane.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            try:
                exit_code = controller.main([str(invocation_path)])
            finally:
                os.environ.pop("ANTHROPIC_AUTH_TOKEN", None)
            workspace = root / "root" / "run" / ".agent-workspace"
            status = json.loads(
                (workspace / "worker_controller.status.json").read_text(
                    encoding="utf-8"
                )
            )
            child_env = json.loads(envdump.read_text(encoding="utf-8"))
            self.assertEqual(0, exit_code)
            self.assertEqual("PROVIDER_EXITED", status["state"])
            self.assertEqual("COMPLETED", status["provider_terminal_outcome"])
            self.assertEqual(
                "http://localhost:11434", child_env["ANTHROPIC_BASE_URL"]
            )
            self.assertEqual("ollama", child_env["ANTHROPIC_AUTH_TOKEN"])
            self.assertEqual("", child_env["ANTHROPIC_API_KEY"])
            self.assertNotEqual(
                "inherited-secret-must-not-leak", child_env["ANTHROPIC_AUTH_TOKEN"]
            )


if __name__ == "__main__":
    unittest.main()
