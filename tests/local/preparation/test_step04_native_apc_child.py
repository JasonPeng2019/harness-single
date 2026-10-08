"""The near-match APC child runs through one real candidate-harness lane.

BEHAVIOR-02 requires the product harness, not a callback-only fake, to own the
one bounded drafting child.  This file drives the explicit near-match
``PreparationService`` path with the native launcher over the real
``run_bootstrap``/``run_launch``/``run_force_stop`` consumers, a deterministic
provider behavior that materializes the child's bounded artifact, and proves
that only the validated proposal is returned while ROOT acceptance stays
separate.
"""

from __future__ import annotations

import contextlib
import inspect
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "harness"))

from memory_harness import (
    apc,
    config,
    contracts,
    experience,
    harness_bridge,
    harness_child,
    local_adapters,
    preparation,
    privacy,
    store,
    templates,
)
from orchestrator_harness import bootstrap, core as harness_core, lanes, launch, processes, setup


BINDING = {
    "provider": "codex",
    "model": "test-model",
    "cli": "codex",
    "effort": "low",
    "source": "explicit",
}

ROOT_REPLAN = {
    "requested_by": "ROOT",
    "reason": "the tests exercise the explicit ROOT replan path",
}

BINDING_SOURCE = (
    "PROVIDER_ID = 'codex'\n"
    "ADAPTER_VERSION = 'test-v1'\n"
    "def validate_launch_config(*, model, launch_config): return dict(launch_config)\n"
    "def build_argv(*, model, launch_config, **kwargs):\n"
    "    configured = validate_launch_config(model=model, launch_config=launch_config)\n"
    "    if configured.get('launcher') == 'ollama':\n"
    "        return ['ollama', 'launch', 'codex', '--model', model, '--yes', '--', 'codex', 'exec']\n"
    "    return ['codex', 'exec']\n"
    "def parse_line(line): return None\n"
)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class QuietEvidenceService:
    """An explicit local evidence seam with no reviewed trajectories yet."""

    def search_recent_evidence(self, scope, query):
        return []


class NativeChildHarness:
    """A minimal but real harness root that bootstrap/launch accept."""

    EPOCH = "epoch-1"

    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.root_workspace = self.root / "root-workspace"
        self.harness.mkdir()
        self.root_workspace.mkdir()
        self.write_json(
            self.harness / "harness-config.json",
            {
                "root_workspace": str(self.root_workspace),
                "managed_coordination": "enabled",
            },
        )
        self.write_json(
            self.harness / "resource-manifest.json",
            {"schema": "resource-manifest/v1", "resources": []},
        )
        workspace = self.harness / "super-cache" / "workspace" / ".agent-workspace"
        for name, contents in {
            "README.md": "base workspace\n",
            "lane-queue.py": "# lane queue\n",
            "manager-notify.py": "# manager notify\n",
            "result-stop-check.py": "# result stop check\n",
        }.items():
            self.write_text(workspace / name, contents)
        self.write_text(
            self.harness / "adapters" / "codex" / "super-cache" / ".codex" / "worker.txt",
            "codex worker payload\n",
        )
        self.write_text(
            self.harness
            / "orchestrator_harness"
            / "provider_adapters"
            / "codex"
            / "launcher_binding.py",
            BINDING_SOURCE,
        )
        self.runtime = self.root_workspace / ".harness-runtime"
        for source, relative in setup._plan_active_cache(self.harness):
            target = self.runtime / "super-cache" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        self.launch_options: dict[str, str] = {}
        self.bootstrap_calls: list[str] = []
        self.provider_spawns: list[str] = []
        self.spawn_calls: list[dict] = []
        self.stop_calls: list[str] = []

    def close(self) -> None:
        self.temporary.cleanup()

    def write_text(self, path: Path, contents: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    def write_json(self, path: Path, value: object) -> None:
        self.write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")

    # -- real harness entry points (only OS-level seams are patched) --------

    def bootstrap_patches(self):
        def fake_git_add(root, branch, target, base_commit) -> None:
            target.mkdir(parents=True, exist_ok=True)

        def record_bootstrap(**kwargs):
            self.bootstrap_calls.append(kwargs["lane_id"])
            return real_bootstrap(**kwargs)

        real_bootstrap = bootstrap.run_bootstrap
        return [
            patch("orchestrator_harness.config.find_harness_root", return_value=self.harness),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": self.EPOCH}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(bootstrap, "_git_worktree_add", side_effect=fake_git_add),
            patch.object(bootstrap.subprocess, "run"),
            patch.object(bootstrap, "run_bootstrap", side_effect=record_bootstrap),
        ]

    def lane_lookup(self, lane_id: str) -> dict:
        return lanes.read_lane(self.runtime, self.EPOCH, lane_id)

    def launch_patches(self, *, provider_behavior):
        def spawn_side_effect(argv, **kwargs):
            lane_id = argv[-1]
            self.spawn_calls.append(
                {
                    "argv": list(argv),
                    "lane_id": lane_id,
                    "env_present": "env" in kwargs,
                    "env": kwargs.get("env"),
                }
            )
            provider_behavior(lane_id)
            return MagicMock(pid=41)

        def provider_spawn(*args, **kwargs):
            self.provider_spawns.append("direct")
            raise AssertionError("no direct provider process may be started")

        return [
            patch.object(launch, "find_harness_root", return_value=self.harness),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", side_effect=lambda rt, lane_id: (self.EPOCH, self.lane_lookup(lane_id))),
            patch.object(launch.processes, "spawn_detached", side_effect=spawn_side_effect),
            patch.object(
                launch.processes,
                "process_identity",
                return_value={"pid": 41, "creation_time": "ct-1"},
            ),
            patch.object(launch.processes, "terminate_process", return_value=True),
            patch.object(launch.processes, "cleanup_recorded_process_boundary", return_value=True),
            patch.object(launch.subprocess, "run"),
            patch.object(processes, "spawn_provider", side_effect=provider_spawn),
        ]

    def idle_child(self):
        """A deterministic lane body that only proves terminal cleanup."""

        def behavior(lane_id: str) -> None:
            lane = self.lane_lookup(lane_id)
            self.write_json(
                Path(lane["controller_status_path"]),
                {
                    "schema": "controller-status/v1",
                    "lane_id": lane_id,
                    "run_id": lane["run_id"],
                    "controller_state": "exited",
                    "provider_state": {"state": "exited", "exit_code": 0, "pid": 41},
                    "cleanup_proven": True,
                    "recorded_status": "review_pending",
                },
            )

        return behavior

    def deterministic_child(self, *, artifact_name: str = "apc-result.json"):
        """The deterministic native child for this fixture.

        It reads the restricted drafting card the adapter queued, drafts only
        the permitted surface, and writes the bounded artifact plus the real
        result/v1 record and the controller's terminal status.
        """

        def behavior(lane_id: str) -> None:
            lane = self.lane_lookup(lane_id)
            worktree = Path(lane["worktree_path"])
            card = json.loads(
                (worktree / ".agent-workspace" / "task-card.json").read_text(encoding="utf-8")
            )
            match = re.search(r"```json\n(.*?)\n```", card["task"], re.DOTALL)
            assert match is not None, "the child card must carry the exact bounded request"
            request = json.loads(match.group(1))
            content = {
                "fixed_steps": list(request["template"]["fixed_steps"]),
                "verification_intent": request["template"]["verification_intent"],
            }
            if "bindings" in request["permitted_edits"]:
                content["bindings"] = {"failure": "parser test fails", "component": "parser"}
            proposal = contracts.make_plan(
                plan_id="native-apc-proposal-1",
                objective_id=request["parent_objective_id"],
                route="ordinary",
                state="proposed",
                content=content,
                source={
                    "template_id": request["template_id"],
                    "template_version": request["template_version"],
                    "branch": "apc_proposal",
                    "apc_request": request["content_hash"],
                },
            )
            artifact = apc.make_apc_result(request, proposal)
            self.write_json(worktree / ".agent-workspace" / artifact_name, artifact)
            result = {
                "schema": "result/v1",
                "lane_id": lane_id,
                "run_id": lane["run_id"],
                "outcome": "PASS",
                "summary": "bounded drafting artifact written for ROOT review",
                "evidence": [str(worktree / ".agent-workspace" / artifact_name)],
                "completed_at": "2026-09-23T12:00:00Z",
            }
            result["content_hash"] = harness_core.content_hash(result)
            self.write_json(worktree / "RESULT.json", result)
            self.write_json(
                Path(lane["controller_status_path"]),
                {
                    "schema": "controller-status/v1",
                    "lane_id": lane_id,
                    "run_id": lane["run_id"],
                    "controller_state": "exited",
                    "provider_state": {"state": "exited", "exit_code": 0, "pid": 41},
                    "cleanup_proven": True,
                    "recorded_status": "review_pending",
                },
            )

        return behavior


class NativeApcChildTests(unittest.TestCase):
    """One near match reaches one real candidate-harness child; ROOT acceptance stays separate."""

    def setUp(self) -> None:
        self.fixture = NativeChildHarness()
        self.addCleanup(self.fixture.close)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "direct_fill_threshold": 0.5,
                "near_match_threshold": 0.2,
            }
        )
        self.memory_store = store.MemoryStore(self.fixture.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.addCleanup(self.memory_store.close)
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        self.stores = local_adapters.make_local_search_stores(
            experience_service=QuietEvidenceService(),
            scope=experience.ExperienceScope(
                application="harness",
                project="product",
                namespace="native-apc-child",
                owner="root-agent",
            ),
            registry=templates.load_default_templates(),
            limits=self.limits,
        )
        self.plan = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    # -- helpers -----------------------------------------------------------

    def _launcher(self, *, launch_options=None):
        return harness_child.make_native_apc_launcher(
            session=harness_child.DraftingChildSession(
                runtime_root=self.fixture.runtime,
                task_card_dir=self.fixture.root / "apc-cards",
                base_commit="test-base",
                provider=BINDING["provider"],
                model=BINDING["model"],
                launch_options=dict(
                    self.fixture.launch_options
                    if launch_options is None
                    else launch_options
                ),
            ),
            deadline=self.clock() + 240.0,
            clock=self.clock,
            lane_lookup=self.fixture.lane_lookup,
            sleep=lambda seconds: self.clock.advance(1.0),
        )

    def _prepare(self, launcher, binding=BINDING):
        return self.service.prepare(
            task_card=contracts.make_task_card(
                task="Fix the regression failure in the parser test", base_commit="base-1"
            ),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            stores=list(self.stores),
            root_replan=ROOT_REPLAN,
            apc_binding=binding,
            apc_launcher=launcher,
        )

    def _native_stack(self, *, provider_behavior, force_stop=None):
        """Enter the real bootstrap/launch consumers, with OS-level seams patched."""

        stack = contextlib.ExitStack()
        for item in self.fixture.bootstrap_patches():
            stack.enter_context(item)
        for item in self.fixture.launch_patches(provider_behavior=provider_behavior):
            stack.enter_context(item)
        real_force_stop = launch.run_force_stop

        def stop_side_effect(lane_id, **kwargs):
            self.fixture.stop_calls.append(lane_id)
            if force_stop is not None:
                return force_stop
            return real_force_stop(lane_id, **kwargs)

        stack.enter_context(patch.object(launch, "run_force_stop", side_effect=stop_side_effect))
        return stack

    def _operations(self, outcome):
        return self.memory_store.list_apc_child_operations(outcome.decision["decision_id"])

    # -- the native boundary ------------------------------------------------

    def test_near_match_runs_one_native_child_and_returns_only_a_proposal(self) -> None:
        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            outcome = self._prepare(self._launcher())

        self.assertEqual("apc_proposal", outcome.disposition["branch"])
        self.assertEqual("proposed", outcome.proposal["state"])
        self.assertEqual("proposed", outcome.plan["state"])
        self.assertIsNone(outcome.disposition.get("root_acceptance"))

        # Exactly one child lane was queued and launched through the real
        # candidate-harness entry points, and no direct provider process ran.
        self.assertEqual(1, len(self.fixture.bootstrap_calls))
        lane_id = self.fixture.bootstrap_calls[0]
        self.assertTrue(lane_id.startswith("apc-child-"), lane_id)
        self.assertEqual([], self.fixture.provider_spawns)
        lane = self.fixture.lane_lookup(lane_id)

        # The queued lane carries the exact explicit binding effort, never a
        # silently inherited or session-substituted launch option.
        self.assertEqual(
            {"reasoning_effort": BINDING["effort"]},
            dict(lane["provider"]["launch_config"]),
        )

        operations = self._operations(outcome)
        self.assertEqual(1, len(operations))
        child = operations[0]
        self.assertEqual("reconciled", child["status"])
        observed = child["observed_invocation"]
        self.assertEqual("controller:41:ct-1", observed["invocation_id"])
        self.assertEqual(lane_id, observed["lane_id"])
        self.assertEqual(lane["run_id"], observed["run_id"])
        self.assertEqual(
            {"reasoning_effort": BINDING["effort"]}, dict(observed["launch_config"])
        )
        self.assertEqual(BINDING, child["binding"])
        self.assertTrue(child["cleanup"]["cleanup_proven"])
        self.assertIn("retired", child["cleanup"]["state"])

        # The child card carried only restricted drafting material: no memory
        # control state, no credentials, and no parent authority.
        workspace = Path(lane["worktree_path"]) / ".agent-workspace"
        card = json.loads((workspace / "task-card.json").read_text(encoding="utf-8"))
        self.assertNotIn("memory_handoff", card)
        self.assertFalse((workspace / "memory-state.sqlite3").exists())
        self.assertFalse((workspace / "memory-dispatch.json").exists())
        card_text = card["task"]
        self.assertIn("Drafting-only", card_text)
        self.assertIn("may_approve", card_text)
        self.assertIn(BINDING["provider"], card_text)
        self.assertIn(f"- effort: {BINDING['effort']}", card_text)

        # The harness itself retired the exact child lane.
        self.assertEqual("retired", self.fixture.lane_lookup(lane_id)["lifecycle"])

    def test_session_launch_options_cannot_override_the_explicit_binding(self) -> None:
        launcher = self._launcher(launch_options={"reasoning_effort": "high"})

        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            outcome = self._prepare(launcher)

        # The conflicting session value is refused before anything is queued,
        # so the explicit binding effort is never silently overridden.
        self.assertEqual([], self.fixture.bootstrap_calls)
        self.assertEqual("fresh", outcome.disposition["branch"])
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("ApcChildUnavailableError", reason)
        self.assertIn("binding", reason)
        self.assertIsNone(outcome.proposal)
        self.assertEqual([], self.fixture.provider_spawns)
        operations = self._operations(outcome)
        # The refusal is terminal: no child existed, so a later attempt is not
        # blocked by a live owner.
        self.assertEqual(["refused"], [operation["status"] for operation in operations])
        self.assertIn("refused", contracts.APC_CHILD_TERMINAL_STATUSES)

    def test_unproven_cleanup_cannot_reconcile_a_proposal(self) -> None:
        launcher = self._launcher()
        unproven = {
            "ok": False,
            "code": "FORCE_STOP_PROCESS_SURVIVED",
            "summary": "a lane process could not be terminated even forcibly",
            "evidence_paths": [],
            "next_action": "escalate to the operator/host",
        }

        with self._native_stack(
            provider_behavior=self.fixture.deterministic_child(), force_stop=unproven
        ):
            outcome = self._prepare(launcher)

        # The child produced a bounded artifact, but its exact retirement was
        # not proven, so nothing is shown to ROOT as a proposal.
        self.assertIsNone(outcome.proposal)
        self.assertEqual("fresh", outcome.disposition["branch"])
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("ApcChildRejectedError", reason)
        self.assertIn("cleanup is not proven", reason)

        lane_id = self.fixture.bootstrap_calls[0]
        operations = self._operations(outcome)
        # One durable record per attempt: the latest exact status persists.
        self.assertEqual(["cleanup_pending"], [operation["status"] for operation in operations])
        pending = operations[-1]
        self.assertFalse(pending["cleanup"]["cleanup_proven"])
        self.assertIn("cleanup_pending", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertEqual(lane_id, pending["observed_invocation"]["lane_id"])
        # The unproven child keeps its exact identity, and its lane is still
        # live: the unresolved native ownership stays visible, never hidden.
        self.assertEqual("controller:41:ct-1", pending["observed_invocation"]["invocation_id"])
        self.assertEqual("running", self.fixture.lane_lookup(lane_id)["lifecycle"])

    def test_unavailable_native_child_falls_back_without_an_unresolved_claim(self) -> None:
        launcher = self._launcher()

        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            with patch.object(
                bootstrap,
                "run_bootstrap",
                return_value={
                    "ok": False,
                    "code": "BOOTSTRAP_ADAPTER_MISSING",
                    "summary": "launcher binding missing for provider codex",
                    "evidence_paths": [],
                    "next_action": "configure the provider adapter",
                },
            ):
                outcome = self._prepare(launcher)

        self.assertEqual("fresh", outcome.disposition["branch"])
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("ApcChildUnavailableError", reason)
        self.assertEqual([], self.fixture.provider_spawns)
        operations = self._operations(outcome)
        # A proven-not-launched failure is terminal, so it never blocks a
        # later attempt the way an unresolved live child would.
        self.assertEqual(["refused"], [operation["status"] for operation in operations])
        self.assertIn("refused", contracts.APC_CHILD_TERMINAL_STATUSES)

    # -- the repaired native boundary defects -------------------------------

    def test_apc_child_controller_environment_is_scrubbed_without_touching_legacy(self) -> None:
        control = {
            "MEMORY_HARNESS_CONTROL_TOKEN": "control-token",
            "MEMORY_HARNESS_POLICY_TOKEN": "policy-token",
            "APC_CHILD_INHERITED_SENTINEL": "kept",
        }
        with patch.dict(os.environ, control, clear=False):
            with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
                outcome = self._prepare(self._launcher())
            inherited_minus_control = privacy.worker_environment(os.environ)
            self.assertIn("MEMORY_HARNESS_CONTROL_TOKEN", os.environ)

        self.assertEqual("apc_proposal", outcome.disposition["branch"])
        lane_id = self.fixture.bootstrap_calls[0]
        spawns = [call for call in self.fixture.spawn_calls if call["lane_id"] == lane_id]
        self.assertEqual(1, len(spawns))
        self.assertTrue(
            spawns[0]["env_present"],
            "the APC child controller must not inherit the parent environment",
        )
        env = spawns[0]["env"]
        self.assertIsInstance(env, dict)
        for key in ("MEMORY_HARNESS_CONTROL_TOKEN", "MEMORY_HARNESS_POLICY_TOKEN"):
            self.assertNotIn(key, env)
        # The scrub keeps everything else, so this is a scrubbed inheritance
        # rather than an unrelated empty environment.
        self.assertEqual("kept", env["APC_CHILD_INHERITED_SENTINEL"])
        self.assertEqual(inherited_minus_control, env)

        # The provider only ever inherits the controller environment: the
        # provider spawn has no environment parameter at all.
        self.assertEqual(
            {"argv", "cwd", "stdin", "stdout", "stderr"},
            set(inspect.signature(processes.spawn_provider).parameters),
        )

        # The child card declares the scrubbed boundary explicitly and still
        # carries no enhanced memory handoff.
        lane = self.fixture.lane_lookup(lane_id)
        card = json.loads(
            (
                Path(lane["worktree_path"]) / ".agent-workspace" / "task-card.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual("scrubbed", card["worker_environment"])
        self.assertNotIn("memory_handoff", card)

    def test_legacy_lane_keeps_the_inherited_controller_environment(self) -> None:
        legacy_card = contracts.make_task_card(
            task="legacy lane without optional memory", base_commit="base-1"
        )
        card_path = self.fixture.root / "legacy-cards" / "legacy-lane.task-card.json"
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(legacy_card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            queued = bootstrap.run_bootstrap(
                lane_id="legacy-lane",
                provider=BINDING["provider"],
                model=BINDING["model"],
                launch_config={"reasoning_effort": BINDING["effort"]},
                exclusive_resources=[],
                task_card_path=str(card_path),
            )
            self.assertTrue(queued.get("ok"), queued)
            launched = launch.run_launch("legacy-lane")
            self.assertTrue(launched.get("ok"), launched)

        spawns = [call for call in self.fixture.spawn_calls if call["lane_id"] == "legacy-lane"]
        self.assertEqual(1, len(spawns))
        self.assertFalse(
            spawns[0]["env_present"],
            "a legacy or all-off lane keeps the inherited controller environment",
        )
        self.assertIsNone(spawns[0]["env"])

    def test_binding_cli_must_match_the_selected_provider_adapter(self) -> None:
        mismatched = {**BINDING, "cli": "test-cli"}

        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            outcome = self._prepare(self._launcher(), binding=mismatched)

        # Nothing was queued or launched: the cli the explicit binding names is
        # not the cli the selected adapter actually starts.
        self.assertEqual([], self.fixture.bootstrap_calls)
        self.assertEqual([], self.fixture.spawn_calls)
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertIsNone(outcome.proposal)
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("ApcChildUnavailableError", reason)
        self.assertIn("cli", reason)
        operations = self._operations(outcome)
        self.assertEqual(["refused"], [operation["status"] for operation in operations])

    def test_two_explicit_binding_configurations_select_without_source_changes(self) -> None:
        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            codex_outcome = self._prepare(self._launcher())
            ollama_outcome = self._prepare(
                self._launcher(launch_options={"launcher": "ollama"}),
                binding={**BINDING, "cli": "ollama"},
            )

        self.assertEqual("apc_proposal", codex_outcome.disposition["branch"])
        self.assertEqual("apc_proposal", ollama_outcome.disposition["branch"])
        lane_ids = self.fixture.bootstrap_calls
        self.assertEqual(2, len(lane_ids))
        self.assertEqual(
            {"reasoning_effort": BINDING["effort"]},
            dict(self.fixture.lane_lookup(lane_ids[0])["provider"]["launch_config"]),
        )
        self.assertEqual(
            {"launcher": "ollama", "reasoning_effort": BINDING["effort"]},
            dict(self.fixture.lane_lookup(lane_ids[1])["provider"]["launch_config"]),
        )

    def test_launch_that_consumes_the_allowance_retains_exact_ownership(self) -> None:
        def slow_child(lane_id: str) -> None:
            # The launch handshake alone consumed the whole child allowance.
            self.clock.advance(600.0)
            self.fixture.deterministic_child()(lane_id)

        with self._native_stack(provider_behavior=slow_child):
            outcome = self._prepare(self._launcher())

        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertIsNone(outcome.proposal)
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("ApcChildTimeoutError", reason)

        lane_id = self.fixture.bootstrap_calls[0]
        # Exactly one child was queued and launched, and nothing native was
        # started after the deadline, not even cancellation.
        self.assertEqual(1, len(self.fixture.bootstrap_calls))
        self.assertEqual([], self.fixture.stop_calls)

        operations = self._operations(outcome)
        self.assertEqual(["cleanup_pending"], [operation["status"] for operation in operations])
        pending = operations[-1]
        self.assertIn("cleanup_pending", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertEqual("controller:41:ct-1", pending["observed_invocation"]["invocation_id"])
        self.assertEqual(lane_id, pending["observed_invocation"]["lane_id"])
        self.assertFalse(pending["cleanup"]["cleanup_proven"])
        # The exact owned child stays visible instead of being relaunched.
        self.assertEqual("running", self.fixture.lane_lookup(lane_id)["lifecycle"])

    def test_blocking_native_launch_is_bounded_within_the_allowance(self) -> None:
        """A launch that blocks past the allowance never outlives it.

        The prepared lane really exists (the fixture's patched bootstrap
        consumers queue it), but its launch acknowledgement only lands long
        after the one shared deadline.  The adapter must answer inside the
        allowance with the exact unresolved lane identity and must not start
        result collection or cancellation for it.
        """

        gate = threading.Event()
        late_effects_done = threading.Event()
        holder: list[threading.Thread] = []
        stop_probe = MagicMock(
            return_value={
                "ok": True,
                "code": "FORCE_STOP_OK",
                "summary": "lane force-stopped and retired",
                "evidence_paths": [],
                "next_action": "none",
            }
        )

        def blocking_launch(lane_id: str, **kwargs):
            holder.append(threading.current_thread())
            gate.wait(timeout=10.0)
            # The acknowledgement lands late, after the allowance expired; a
            # real launch would have recorded the controller identity and its
            # terminal cleanup by then.
            lanes.update_lane(
                self.fixture.runtime,
                self.fixture.EPOCH,
                lane_id,
                lambda current: {
                    **current,
                    "process": {"pid": 41, "creation_time": "ct-1"},
                },
            )
            self.fixture.deterministic_child()(lane_id)
            late_effects_done.set()
            return {
                "ok": True,
                "code": "LAUNCH_OK",
                "summary": "late acknowledgement",
                "evidence_paths": [],
                "next_action": "none",
            }

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            launcher = harness_child.make_native_apc_launcher(
                session=harness_child.DraftingChildSession(
                    runtime_root=self.fixture.runtime,
                    task_card_dir=self.fixture.root / "apc-cards",
                    base_commit="test-base",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                ),
                deadline=self.clock() + 1.5,
                clock=self.clock,
                launch_fn=blocking_launch,
                stop_fn=stop_probe,
                sleep=lambda seconds: None,
            )
            request = apc.make_apc_request(
                template={"template_id": "template-1", "version": 1, "allowed_edits": ["bindings"]},
                parent_decision_id="decision-1",
                parent_objective_id="objective-1",
                permitted_edits=["bindings"],
                binding=BINDING,
            )

            with patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ):
                started = time.monotonic()
                try:
                    observed = launcher(request)
                finally:
                    elapsed = time.monotonic() - started
                    released = gate.is_set()
                    gate.set()
                    for thread in holder:
                        if thread is not threading.current_thread():
                            thread.join(timeout=5.0)
                    late_effects_done.wait(timeout=5.0)

        # The adapter answered inside its own allowance instead of blocking on
        # the native call, and it started no later effectful phase.
        self.assertFalse(released)
        self.assertLess(elapsed, 2.0)
        self.assertGreater(elapsed, 0.2)
        self.assertEqual("launch", observed["phase"])
        self.assertEqual(self.fixture.bootstrap_calls, [observed["lane_id"]])
        self.assertNotIn("invocation_id", observed)
        # No cancellation started for the lane the launch never acknowledged.
        stop_probe.assert_not_called()

    def test_launch_failure_retires_the_prepared_lane_before_refusal(self) -> None:
        failed_launch = {
            "ok": False,
            "code": "LAUNCH_PROVIDER_START_FAILED",
            "summary": "the provider process could not be started",
            "evidence_paths": [],
            "next_action": "resolve the error and re-launch",
        }

        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            with patch.object(launch, "run_launch", return_value=failed_launch):
                outcome = self._prepare(self._launcher())

        lane_id = self.fixture.bootstrap_calls[0]
        # The prepared lane was retired before the refusal was recorded, so a
        # later attempt cannot inherit a half-prepared lane.
        self.assertEqual([lane_id], self.fixture.stop_calls)
        self.assertEqual("retired", self.fixture.lane_lookup(lane_id)["lifecycle"])
        self.assertEqual("fresh", outcome.disposition["branch"])
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("ApcChildUnavailableError", reason)
        operations = self._operations(outcome)
        self.assertEqual(["refused"], [operation["status"] for operation in operations])
        self.assertIn("refused", contracts.APC_CHILD_TERMINAL_STATUSES)

    def test_unproven_retirement_after_launch_failure_stays_unresolved(self) -> None:
        failed_launch = {
            "ok": False,
            "code": "LAUNCH_PROVIDER_START_FAILED",
            "summary": "the provider process could not be started",
            "evidence_paths": [],
            "next_action": "resolve the error and re-launch",
        }
        survived = {
            "ok": False,
            "code": "FORCE_STOP_PROCESS_SURVIVED",
            "summary": "a lane process could not be terminated even forcibly",
            "evidence_paths": [],
            "next_action": "escalate to the operator/host",
        }

        with self._native_stack(
            provider_behavior=self.fixture.deterministic_child(), force_stop=survived
        ):
            with patch.object(launch, "run_launch", return_value=failed_launch):
                outcome = self._prepare(self._launcher())
            retry = self._prepare(self._launcher())

        lane_id = self.fixture.bootstrap_calls[0]
        # Exactly one queue and one bounded retirement attempt happened, and no
        # retry relaunched anything while the exact child stays unresolved.
        self.assertEqual([lane_id], self.fixture.bootstrap_calls)
        self.assertEqual([lane_id], self.fixture.stop_calls)
        self.assertEqual("fresh", retry.disposition["branch"])
        retry_reason = retry.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("reconcile", retry_reason)

        operations = self._operations(outcome)
        self.assertEqual(["ambiguous"], [operation["status"] for operation in operations])
        ambiguous = operations[-1]
        self.assertIn("ambiguous", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        # The exact lane ownership stays visible instead of being hidden.
        self.assertEqual(lane_id, ambiguous["launch_intent"]["lane_id"])
        self.assertEqual("prepared", self.fixture.lane_lookup(lane_id)["lifecycle"])


    def test_unreadable_native_acknowledgements_keep_the_exact_lane_ownership(self) -> None:
        """ROOT's ownership check: an unreadable acknowledgement is unresolved.

        The product harness really queued this exact deterministic lane, but its
        queue or launch acknowledgement answered with something unreadable.  The
        adapter and the accepted reconciliation path must keep that exact lane
        identity visible as an unresolved child, and a later attempt must
        reconcile it instead of blindly relaunching it.
        """

        request = apc.make_apc_request(
            template={
                "template_id": "template-1",
                "version": 1,
                "allowed_edits": ["bindings"],
            },
            parent_decision_id="decision-1",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=BINDING,
        )

        # (a) The queue acknowledgement itself is unreadable.
        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            launcher = harness_child.make_native_apc_launcher(
                session=harness_child.DraftingChildSession(
                    runtime_root=self.fixture.runtime,
                    task_card_dir=self.fixture.root / "apc-cards",
                    base_commit="test-base",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                ),
                deadline=self.clock() + 240.0,
                clock=self.clock,
                lane_lookup=self.fixture.lane_lookup,
                bootstrap_fn=lambda **kwargs: ["not-a-mapping-queue-ack"],
                sleep=lambda seconds: None,
            )
            with patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ):
                observed = launcher(request)

        lane_id = observed["lane_id"]
        self.assertTrue(lane_id.startswith("apc-child-"), lane_id)
        self.assertEqual("bootstrap", observed["phase"])
        self.assertNotIn("invocation_id", observed)

        # The durable record keeps that exact lane identity instead of a bare
        # refusal, and a retry must reconcile it rather than relaunch.
        with self.assertRaises(harness_bridge.ApcChildAmbiguityError) as caught:
            harness_bridge.run_apc_child(
                request=request,
                template_record={
                    "template_id": "template-1",
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                launcher=lambda _request: observed,
                store=self.memory_store,
                limits=self.limits,
                clock=self.clock,
                deadline=self.clock() + 240.0,
            )
        self.assertIn("reconcile the exact child", str(caught.exception))

        operations = self.memory_store.list_apc_child_operations("decision-1")
        self.assertEqual(["ambiguous"], [operation["status"] for operation in operations])
        ambiguous = operations[-1]
        self.assertIn("ambiguous", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertEqual(lane_id, ambiguous["launch_intent"]["lane_id"])
        self.assertIsNone(ambiguous.get("observed_invocation"))

        with self.assertRaises(harness_bridge.ApcChildAmbiguityError):
            harness_bridge.run_apc_child(
                request=request,
                template_record={
                    "template_id": "template-1",
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                launcher=lambda _request: observed,
                store=self.memory_store,
                limits=self.limits,
                clock=self.clock,
                deadline=self.clock() + 240.0,
            )

        # (b) The launch acknowledgement is unreadable for an already-queued lane.
        launch_request = apc.make_apc_request(
            template={
                "template_id": "template-2",
                "version": 1,
                "allowed_edits": ["bindings"],
            },
            parent_decision_id="decision-2",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=BINDING,
        )

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            launcher = harness_child.make_native_apc_launcher(
                session=harness_child.DraftingChildSession(
                    runtime_root=self.fixture.runtime,
                    task_card_dir=self.fixture.root / "apc-cards",
                    base_commit="test-base",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                ),
                deadline=self.clock() + 240.0,
                clock=self.clock,
                lane_lookup=self.fixture.lane_lookup,
                launch_fn=lambda lane_id, **kwargs: "not-a-mapping-launch-ack",
                sleep=lambda seconds: None,
            )
            with patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ):
                launched_observed = launcher(launch_request)

        launched_lane_id = launched_observed["lane_id"]
        self.assertEqual("launch", launched_observed["phase"])
        self.assertNotIn("invocation_id", launched_observed)
        self.assertEqual(
            [launched_lane_id],
            [lane for lane in self.fixture.bootstrap_calls if lane == launched_lane_id],
        )

        with self.assertRaises(harness_bridge.ApcChildAmbiguityError) as caught:
            harness_bridge.run_apc_child(
                request=launch_request,
                template_record={
                    "template_id": "template-2",
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                launcher=lambda _request: launched_observed,
                store=self.memory_store,
                limits=self.limits,
                clock=self.clock,
                deadline=self.clock() + 240.0,
            )
        self.assertIn("reconcile the exact child", str(caught.exception))

        launch_operations = self.memory_store.list_apc_child_operations("decision-2")
        self.assertEqual(["ambiguous"], [operation["status"] for operation in launch_operations])
        launched_ambiguous = launch_operations[-1]
        self.assertEqual(launched_lane_id, launched_ambiguous["launch_intent"]["lane_id"])
        self.assertIsNone(launched_ambiguous.get("observed_invocation"))

    def test_unreadable_lane_record_after_launch_keeps_the_exact_lane_ownership(self) -> None:
        """A launched lane whose record cannot be read back stays unresolved.

        The launch really happened, so the exact lane identity must remain
        visible on the durable operation even though its recorded lane identity
        could not be read back; nothing may be relaunched blindly and no result
        may be claimed for it.
        """

        request = apc.make_apc_request(
            template={
                "template_id": "template-3",
                "version": 1,
                "allowed_edits": ["bindings"],
            },
            parent_decision_id="decision-3",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=BINDING,
        )

        def unreadable_lane(lane_id: str):
            raise lanes.LaneError("LANE_RECORD_INVALID", "lane record unreadable")

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            launcher = harness_child.make_native_apc_launcher(
                session=harness_child.DraftingChildSession(
                    runtime_root=self.fixture.runtime,
                    task_card_dir=self.fixture.root / "apc-cards",
                    base_commit="test-base",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                ),
                deadline=self.clock() + 240.0,
                clock=self.clock,
                lane_lookup=unreadable_lane,
                sleep=lambda seconds: None,
            )
            with patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ):
                observed = launcher(request)

        lane_id = observed["lane_id"]
        self.assertEqual("launch", observed["phase"])
        self.assertNotIn("invocation_id", observed)
        self.assertIn(lane_id, self.fixture.bootstrap_calls)

        with self.assertRaises(harness_bridge.ApcChildAmbiguityError) as caught:
            harness_bridge.run_apc_child(
                request=request,
                template_record={
                    "template_id": "template-3",
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                launcher=lambda _request: observed,
                store=self.memory_store,
                limits=self.limits,
                clock=self.clock,
                deadline=self.clock() + 240.0,
            )
        self.assertIn("reconcile the exact child", str(caught.exception))

        operations = self.memory_store.list_apc_child_operations("decision-3")
        self.assertEqual(["ambiguous"], [operation["status"] for operation in operations])
        ambiguous = operations[-1]
        self.assertIn("ambiguous", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertEqual(lane_id, ambiguous["launch_intent"]["lane_id"])
        self.assertIsNone(ambiguous.get("observed_invocation"))

    def test_launch_failure_without_controller_identity_stays_unresolved(self) -> None:
        """A lane acknowledged without a recorded controller stays unresolved.

        The harness recorded no pid or creation time for the exact lane, so its
        identity must remain visible as unresolved instead of a bare error that
        would hide the live ownership from reconciliation.
        """

        request = apc.make_apc_request(
            template={
                "template_id": "template-4",
                "version": 1,
                "allowed_edits": ["bindings"],
            },
            parent_decision_id="decision-4",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=BINDING,
        )

        identity_less = {
            "ok": True,
            "code": "LAUNCH_OK",
            "summary": "launch acknowledged without a controller identity",
            "evidence_paths": [],
            "next_action": "none",
        }

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            launcher = harness_child.make_native_apc_launcher(
                session=harness_child.DraftingChildSession(
                    runtime_root=self.fixture.runtime,
                    task_card_dir=self.fixture.root / "apc-cards",
                    base_commit="test-base",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                ),
                deadline=self.clock() + 240.0,
                clock=self.clock,
                lane_lookup=self.fixture.lane_lookup,
                launch_fn=lambda lane_id, **kwargs: identity_less,
                sleep=lambda seconds: None,
            )
            with patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ):
                observed = launcher(request)

        lane_id = observed["lane_id"]
        self.assertEqual("launch", observed["phase"])
        self.assertNotIn("invocation_id", observed)

        with self.assertRaises(harness_bridge.ApcChildAmbiguityError) as caught:
            harness_bridge.run_apc_child(
                request=request,
                template_record={
                    "template_id": "template-4",
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                launcher=lambda _request: observed,
                store=self.memory_store,
                limits=self.limits,
                clock=self.clock,
                deadline=self.clock() + 240.0,
            )
        self.assertIn("reconcile the exact child", str(caught.exception))

        operations = self.memory_store.list_apc_child_operations("decision-4")
        self.assertEqual(["ambiguous"], [operation["status"] for operation in operations])
        ambiguous = operations[-1]
        self.assertIn("ambiguous", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertEqual(lane_id, ambiguous["launch_intent"]["lane_id"])
        self.assertIsNone(ambiguous.get("observed_invocation"))


    # -- the remaining reviewed defects -------------------------------------

    def test_expired_native_queue_refuses_without_creating_a_lane(self) -> None:
        """An expired queue allowance cannot start a late lane creation."""

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = self.fixture.root / "expired-cards" / "expired-lane.task-card.json"
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            queued = bootstrap.run_bootstrap(
                lane_id="expired-lane",
                provider=BINDING["provider"],
                model=BINDING["model"],
                launch_config={"reasoning_effort": BINDING["effort"]},
                exclusive_resources=[],
                task_card_path=str(card_path),
                allowance_seconds=0.0,
            )

        self.assertFalse(queued.get("ok"), queued)
        self.assertEqual("BOOTSTRAP_ALLOWANCE_EXPIRED", queued.get("code"))
        worktree = self.fixture.runtime / "worktrees" / self.fixture.EPOCH / "expired-lane"
        self.assertFalse(worktree.exists())

    def test_expired_native_launch_and_cleanup_refuse_without_effects(self) -> None:
        """Launch and cancellation both refuse a spent allowance.

        The prepared lane exists, so the refusal must leave its exact
        ownership visible (never a fabricated launch) and no controller or
        termination effect may start after the allowance.
        """

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = (
            self.fixture.root / "expired-cards" / "expired-launch-lane.task-card.json"
        )
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            queued = bootstrap.run_bootstrap(
                lane_id="expired-launch-lane",
                provider=BINDING["provider"],
                model=BINDING["model"],
                launch_config={"reasoning_effort": BINDING["effort"]},
                exclusive_resources=[],
                task_card_path=str(card_path),
            )
            self.assertTrue(queued.get("ok"), queued)

            launched = launch.run_launch(
                "expired-launch-lane", allowance_seconds=0.0
            )
            stopped = launch.run_force_stop(
                "expired-launch-lane", allowance_seconds=0.0
            )
            termination_calls = launch.processes.terminate_process.call_count

        self.assertFalse(launched.get("ok"), launched)
        self.assertEqual("LAUNCH_ALLOWANCE_EXPIRED", launched.get("code"))
        self.assertEqual(
            "prepared", self.fixture.lane_lookup("expired-launch-lane")["lifecycle"]
        )
        self.assertFalse(stopped.get("ok"), stopped)
        self.assertEqual("FORCE_STOP_ALLOWANCE_EXPIRED", stopped.get("code"))
        self.assertEqual(0, termination_calls)

    def test_native_phases_receive_the_effective_cutoff_not_the_default(self) -> None:
        """The bridge's effective cutoff governs every native phase.

        The launcher is deliberately built with a longer default; the exact
        deadline handed to ``run_apc_child`` must be the one the native
        queue, launch, and cancellation consumers receive, so queue, launch,
        collection, validation, and cleanup all answer to one absolute bound.
        """

        seen: dict = {}
        real_bootstrap = bootstrap.run_bootstrap
        real_launch = launch.run_launch
        real_force_stop = launch.run_force_stop

        def recording_bootstrap(**kwargs):
            seen["queue"] = dict(kwargs)
            return real_bootstrap(**kwargs)

        def recording_launch(lane_id, **kwargs):
            seen["launch"] = dict(kwargs)
            return real_launch(lane_id, **kwargs)

        def recording_stop(lane_id, **kwargs):
            seen["cleanup"] = dict(kwargs)
            return real_force_stop(lane_id, **kwargs)

        effective = self.clock() + 120.0
        with self._native_stack(provider_behavior=self.fixture.deterministic_child()):
            launcher = harness_child.make_native_apc_launcher(
                session=harness_child.DraftingChildSession(
                    runtime_root=self.fixture.runtime,
                    task_card_dir=self.fixture.root / "apc-cards",
                    base_commit="test-base",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                ),
                deadline=self.clock() + 400.0,
                clock=self.clock,
                lane_lookup=self.fixture.lane_lookup,
                bootstrap_fn=recording_bootstrap,
                launch_fn=recording_launch,
                stop_fn=recording_stop,
                sleep=lambda seconds: None,
            )
            request = apc.make_apc_request(
                template={
                    "template_id": "template-1",
                    "version": 1,
                    "allowed_edits": ["bindings"],
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                parent_decision_id="decision-1",
                parent_objective_id="objective-1",
                permitted_edits=["bindings"],
                binding=BINDING,
            )
            attempt = harness_bridge.run_apc_child(
                request=request,
                template_record={
                    "template_id": "template-1",
                    "fixed_steps": [],
                    "verification_intent": "verify",
                },
                launcher=launcher,
                store=self.memory_store,
                limits=self.limits,
                clock=self.clock,
                deadline=effective,
            )

        self.assertEqual("reconciled", attempt.child_operation["status"])
        remaining = effective - self.clock()
        self.assertEqual(remaining, seen["queue"]["allowance_seconds"])
        self.assertEqual(remaining, seen["launch"]["allowance_seconds"])
        self.assertEqual(remaining, seen["cleanup"]["allowance_seconds"])

    # -- a late effect may not outlive the one enclosing deadline ----------

    def test_queue_that_runs_long_cannot_create_a_late_lane(self) -> None:
        """A queue call that outruns the allowance leaves no late lane.

        The call enters inside the allowance and then runs past that one
        absolute instant; the first effectful phase (the lane worktree) must
        refuse instead of materializing a lane the enclosing attempt already
        stopped waiting for.
        """

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = self.fixture.root / "late-cards" / "late-lane.task-card.json"
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        worktree_add = MagicMock()

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            fixture_open_epoch = bootstrap.open_epoch

            def slow_open_epoch(*args, **kwargs):
                time.sleep(0.2)
                return fixture_open_epoch(*args, **kwargs)

            with patch.object(bootstrap, "open_epoch", side_effect=slow_open_epoch):
                with patch.object(bootstrap, "_git_worktree_add", worktree_add):
                    queued = bootstrap.run_bootstrap(
                        lane_id="late-lane",
                        provider=BINDING["provider"],
                        model=BINDING["model"],
                        launch_config={"reasoning_effort": BINDING["effort"]},
                        exclusive_resources=[],
                        task_card_path=str(card_path),
                        allowance_seconds=0.05,
                    )

        self.assertFalse(queued.get("ok"), queued)
        self.assertEqual("BOOTSTRAP_ALLOWANCE_EXPIRED", queued.get("code"))
        worktree_add.assert_not_called()
        worktree = (
            self.fixture.runtime / "worktrees" / self.fixture.EPOCH / "late-lane"
        )
        self.assertFalse(worktree.exists())

    def test_queue_that_runs_long_cannot_publish_a_lane_record(self) -> None:
        """A queue call that outruns the allowance publishes no lane record.

        The worktree may already exist when the allowance expires mid-call,
        but the durable lane record is the ownership claim and may not appear
        after the caller stopped waiting; the attempt refuses without
        publishing instead of leaving a late lane behind.
        """

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = (
            self.fixture.root / "late-cards" / "late-publish-lane.task-card.json"
        )
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        prepare_lane_memory = bootstrap.memory_handoff.prepare_lane_memory

        def slow_prepare_lane_memory(*args, **kwargs):
            time.sleep(0.2)
            return prepare_lane_memory(*args, **kwargs)

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            with patch.object(
                bootstrap.memory_handoff,
                "prepare_lane_memory",
                side_effect=slow_prepare_lane_memory,
            ):
                queued = bootstrap.run_bootstrap(
                    lane_id="late-publish-lane",
                    provider=BINDING["provider"],
                    model=BINDING["model"],
                    launch_config={"reasoning_effort": BINDING["effort"]},
                    exclusive_resources=[],
                    task_card_path=str(card_path),
                    allowance_seconds=0.05,
                )

        self.assertFalse(queued.get("ok"), queued)
        self.assertEqual("BOOTSTRAP_ALLOWANCE_EXPIRED", queued.get("code"))
        record_path = lanes.lane_record_path(
            self.fixture.runtime, self.fixture.EPOCH, "late-publish-lane"
        )
        self.assertFalse(record_path.is_file())

    def test_launch_that_runs_long_cannot_start_a_late_controller(self) -> None:
        """A launch call that outruns the allowance starts no controller.

        The prepared lane exists; the launch's first blocking native phase
        consumes the allowance, so the controller spawn must refuse and the
        prepared lane keeps its exact ownership instead of a late launch.
        """

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = (
            self.fixture.root / "late-cards" / "late-launch-lane.task-card.json"
        )
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            queued = bootstrap.run_bootstrap(
                lane_id="late-launch-lane",
                provider=BINDING["provider"],
                model=BINDING["model"],
                launch_config={"reasoning_effort": BINDING["effort"]},
                exclusive_resources=[],
                task_card_path=str(card_path),
            )
            self.assertTrue(queued.get("ok"), queued)
            fixture_read_state = launch.read_runtime_state

            def slow_read_runtime_state(*args, **kwargs):
                time.sleep(0.2)
                return fixture_read_state(*args, **kwargs)

            with patch.object(
                launch, "read_runtime_state", side_effect=slow_read_runtime_state
            ):
                launched = launch.run_launch(
                    "late-launch-lane", allowance_seconds=0.05
                )

        self.assertFalse(launched.get("ok"), launched)
        self.assertEqual("LAUNCH_ALLOWANCE_EXPIRED", launched.get("code"))
        self.assertEqual([], self.fixture.spawn_calls)
        self.assertEqual(
            "prepared", self.fixture.lane_lookup("late-launch-lane")["lifecycle"]
        )

    def test_cleanup_that_runs_long_cannot_start_a_late_termination(self) -> None:
        """A cancellation call that outruns the allowance starts no stop.

        The lane lookup is the first blocking native phase of force-stop;
        when it consumes the allowance, the termination phase must refuse
        instead of retiring the lane after the caller stopped waiting, so the
        exact prepared ownership stays visible and no stop effect starts.
        """

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = (
            self.fixture.root / "late-cards" / "late-stop-lane.task-card.json"
        )
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            queued = bootstrap.run_bootstrap(
                lane_id="late-stop-lane",
                provider=BINDING["provider"],
                model=BINDING["model"],
                launch_config={"reasoning_effort": BINDING["effort"]},
                exclusive_resources=[],
                task_card_path=str(card_path),
            )
            self.assertTrue(queued.get("ok"), queued)
            fixture_find_active_lane = launch.find_active_lane

            def slow_find_active_lane(*args, **kwargs):
                time.sleep(0.2)
                return fixture_find_active_lane(*args, **kwargs)

            with patch.object(
                launch, "find_active_lane", side_effect=slow_find_active_lane
            ):
                stopped = launch.run_force_stop(
                    "late-stop-lane", allowance_seconds=0.05
                )
            termination_calls = launch.processes.terminate_process.call_count

        self.assertFalse(stopped.get("ok"), stopped)
        self.assertEqual("FORCE_STOP_ALLOWANCE_EXPIRED", stopped.get("code"))
        self.assertEqual(0, termination_calls)
        self.assertEqual(
            "prepared", self.fixture.lane_lookup("late-stop-lane")["lifecycle"]
        )

    # -- the remaining reviewed bootstrap defects ---------------------------

    def test_slow_bootstrap_preflight_cannot_renew_the_caller_cutoff(self) -> None:
        """A slow provider validation cannot renew the caller's one cutoff.

        The allowance is the enclosing attempt's remaining time, so it is
        captured when the call enters.  Validating the provider launch config
        afterwards may itself run long; that must leave the call expired
        instead of minting a fresh allowance that starts a late worktree.
        """

        card = contracts.make_task_card(
            task="restricted drafting child", base_commit="test-base"
        )
        card["worker_environment"] = "scrubbed"
        card_path = (
            self.fixture.root / "late-cards" / "slow-preflight-lane.task-card.json"
        )
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        worktree_add = MagicMock()
        real_validate = bootstrap._validate_provider_launch_config

        def slow_validate(*args, **kwargs):
            time.sleep(0.2)
            return real_validate(*args, **kwargs)

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            with patch.object(
                bootstrap,
                "_validate_provider_launch_config",
                side_effect=slow_validate,
            ):
                with patch.object(bootstrap, "_git_worktree_add", worktree_add):
                    queued = bootstrap.run_bootstrap(
                        lane_id="slow-preflight-lane",
                        provider=BINDING["provider"],
                        model=BINDING["model"],
                        launch_config={"reasoning_effort": BINDING["effort"]},
                        exclusive_resources=[],
                        task_card_path=str(card_path),
                        allowance_seconds=0.05,
                    )

        self.assertFalse(queued.get("ok"), queued)
        self.assertEqual("BOOTSTRAP_ALLOWANCE_EXPIRED", queued.get("code"))
        self.assertIn("expired before the lane worktree", queued.get("summary", ""))
        worktree_add.assert_not_called()
        worktree = (
            self.fixture.runtime
            / "worktrees"
            / self.fixture.EPOCH
            / "slow-preflight-lane"
        )
        self.assertFalse(worktree.exists())
        effects = queued.get("attempt_effects") or {}
        self.assertIs(False, effects.get("worktree_created"))
        self.assertIs(False, effects.get("lane_record_written"))
        self.assertIs(True, effects.get("rollback_proven"))

    def test_unproven_bootstrap_rollback_stays_unresolved_not_terminal(self) -> None:
        """An unproven bootstrap rollback keeps exact ownership unresolved.

        The queue really created the child worktree before its preparation
        failed, and the attempt's rollback could not be proven; the exact lane
        and worktree identity must stay visible on the durable operation as
        unresolved, and a later attempt must reconcile it instead of a
        terminal refusal or a blind relaunch.
        """

        request = apc.make_apc_request(
            template={
                "template_id": "template-6",
                "version": 1,
                "allowed_edits": ["bindings"],
            },
            parent_decision_id="decision-6",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=BINDING,
        )
        lane_id = "apc-child-" + str(request["content_hash"])[:12]
        worktree = (
            self.fixture.runtime / "worktrees" / self.fixture.EPOCH / lane_id
        )

        def failed_memory_prepare(*args, **kwargs):
            raise RuntimeError(
                "memory preparation failed after the worktree existed"
            )

        template_record = {
            "template_id": "template-6",
            "fixed_steps": [],
            "verification_intent": "verify",
        }

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            with patch.object(
                bootstrap.memory_handoff,
                "prepare_lane_memory",
                side_effect=failed_memory_prepare,
            ):
                with self.assertRaises(
                    harness_bridge.ApcChildAmbiguityError
                ) as caught:
                    harness_bridge.run_apc_child(
                        request=request,
                        template_record=template_record,
                        launcher=self._launcher(),
                        store=self.memory_store,
                        limits=self.limits,
                        clock=self.clock,
                        deadline=self.clock() + 240.0,
                    )
                self.assertIn("reconcile the exact child", str(caught.exception))
                self.assertEqual([lane_id], self.fixture.bootstrap_calls)

                # The unproven child stays exact and unresolved: a later
                # attempt must reconcile it, never relaunch it blindly.
                with self.assertRaises(
                    harness_bridge.ApcChildAmbiguityError
                ) as retry_caught:
                    harness_bridge.run_apc_child(
                        request=request,
                        template_record=template_record,
                        launcher=self._launcher(),
                        store=self.memory_store,
                        limits=self.limits,
                        clock=self.clock,
                        deadline=self.clock() + 240.0,
                    )
                self.assertIn("already exists", str(retry_caught.exception))
                self.assertEqual([lane_id], self.fixture.bootstrap_calls)

        operations = self.memory_store.list_apc_child_operations("decision-6")
        self.assertEqual(
            ["ambiguous"], [operation["status"] for operation in operations]
        )
        ambiguous = operations[-1]
        self.assertIn("ambiguous", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertIsNone(ambiguous.get("observed_invocation"))
        # The exact unresolved ownership: the phase, lane, and worktree the
        # product harness may still own stay visible for reconciliation.
        self.assertEqual("bootstrap", ambiguous["launch_intent"]["phase"])
        self.assertEqual(lane_id, ambiguous["launch_intent"]["lane_id"])
        self.assertEqual(str(worktree), ambiguous["launch_intent"]["worktree_path"])

    # -- the failed git-add ownership boundary ------------------------------

    def test_failed_git_add_boundary_keeps_exact_residual_ownership(self) -> None:
        """A failed git worktree add never hides a surviving attempt path.

        The real add boundary runs here with only the OS-level git calls
        faked: ``git worktree add`` fails after leaving the attempt worktree
        path behind and its internal cleanup cannot remove it.  The exact lane
        and path must stay visible as an unresolved operation and a retry must
        reconcile it instead of relaunching; the same boundary with nothing
        left behind stays a proven-no-child refusal.
        """

        real_add = bootstrap._git_worktree_add

        def fake_git(partial_path):
            def run(argv, **kwargs):
                command = list(argv)[3:]
                if command[:2] == ["worktree", "add"]:
                    if partial_path is not None:
                        partial_path.mkdir(parents=True, exist_ok=True)
                    return bootstrap.subprocess.CompletedProcess(
                        argv, 128, "", "fatal: could not create worktree dir"
                    )
                if command[:2] == ["worktree", "remove"]:
                    return bootstrap.subprocess.CompletedProcess(
                        argv, 1, "", "fatal: not a working tree"
                    )
                if command[:1] == ["show-ref"]:
                    return bootstrap.subprocess.CompletedProcess(argv, 1, "", "")
                return bootstrap.subprocess.CompletedProcess(argv, 0, "", "")

            return run

        request = apc.make_apc_request(
            template={
                "template_id": "template-7",
                "version": 1,
                "allowed_edits": ["bindings"],
            },
            parent_decision_id="decision-7",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=BINDING,
        )
        lane_id = "apc-child-" + str(request["content_hash"])[:12]
        worktree = (
            self.fixture.runtime / "worktrees" / self.fixture.EPOCH / lane_id
        )
        template_record = {
            "template_id": "template-7",
            "fixed_steps": [],
            "verification_intent": "verify",
        }

        with self._native_stack(provider_behavior=self.fixture.idle_child()):
            with patch.object(bootstrap, "_git_worktree_add", real_add):
                with patch.object(
                    bootstrap.subprocess, "run", side_effect=fake_git(worktree)
                ):
                    with self.assertRaises(
                        harness_bridge.ApcChildAmbiguityError
                    ) as caught:
                        harness_bridge.run_apc_child(
                            request=request,
                            template_record=template_record,
                            launcher=self._launcher(),
                            store=self.memory_store,
                            limits=self.limits,
                            clock=self.clock,
                            deadline=self.clock() + 240.0,
                        )
                    self.assertIn("reconcile the exact child", str(caught.exception))
                    self.assertEqual([lane_id], self.fixture.bootstrap_calls)
                    # The partial path this attempt created is preserved, not
                    # silently deleted, while its ownership stays visible.
                    self.assertTrue(worktree.is_dir())
                    with self.assertRaises(
                        harness_bridge.ApcChildAmbiguityError
                    ) as retry:
                        harness_bridge.run_apc_child(
                            request=request,
                            template_record=template_record,
                            launcher=self._launcher(),
                            store=self.memory_store,
                            limits=self.limits,
                            clock=self.clock,
                            deadline=self.clock() + 240.0,
                        )
                    self.assertIn("already exists", str(retry.exception))
                    self.assertEqual([lane_id], self.fixture.bootstrap_calls)

            # A failed add can also leave only the attempt-created branch
            # behind, with no path: that branch is still this attempt's exact
            # identity, so the operation stays unresolved rather than becoming
            # a terminal no-child refusal.
            branch_request = apc.make_apc_request(
                template={
                    "template_id": "template-7",
                    "version": 1,
                    "allowed_edits": ["bindings"],
                },
                parent_decision_id="decision-700",
                parent_objective_id="objective-1",
                permitted_edits=["bindings"],
                binding=BINDING,
            )
            branch_lane = "apc-child-" + str(branch_request["content_hash"])[:12]
            branch_ref = f"refs/heads/lane/{branch_lane}"
            show_ref_calls = {"count": 0}

            def fake_git_branch_only(argv, **kwargs):
                command = list(argv)[3:]
                if command[:2] == ["worktree", "add"]:
                    return bootstrap.subprocess.CompletedProcess(
                        argv, 128, "", "fatal: could not create worktree dir"
                    )
                if command[:2] == ["worktree", "remove"]:
                    return bootstrap.subprocess.CompletedProcess(
                        argv, 1, "", "fatal: not a working tree"
                    )
                if command[:1] == ["show-ref"]:
                    show_ref_calls["count"] += 1
                    if show_ref_calls["count"] == 1:
                        # The branch did not exist before this attempt.
                        return bootstrap.subprocess.CompletedProcess(argv, 1, "", "")
                    # The attempt-created branch survives its cleanup.
                    return bootstrap.subprocess.CompletedProcess(argv, 0, "", "")
                return bootstrap.subprocess.CompletedProcess(argv, 0, "", "")

            with patch.object(bootstrap, "_git_worktree_add", real_add):
                with patch.object(
                    bootstrap.subprocess, "run", side_effect=fake_git_branch_only
                ):
                    with self.assertRaises(harness_bridge.ApcChildAmbiguityError):
                        harness_bridge.run_apc_child(
                            request=branch_request,
                            template_record=template_record,
                            launcher=self._launcher(),
                            store=self.memory_store,
                            limits=self.limits,
                            clock=self.clock,
                            deadline=self.clock() + 240.0,
                        )

            # The same failed boundary with nothing left behind stays a proven
            # no-child refusal with no identity claimed for the later attempt.
            absent_request = apc.make_apc_request(
                template={
                    "template_id": "template-7",
                    "version": 1,
                    "allowed_edits": ["bindings"],
                },
                parent_decision_id="decision-70",
                parent_objective_id="objective-1",
                permitted_edits=["bindings"],
                binding=BINDING,
            )
            absent_lane = "apc-child-" + str(absent_request["content_hash"])[:12]
            absent_worktree = (
                self.fixture.runtime
                / "worktrees"
                / self.fixture.EPOCH
                / absent_lane
            )
            with patch.object(bootstrap, "_git_worktree_add", real_add):
                with patch.object(
                    bootstrap.subprocess, "run", side_effect=fake_git(None)
                ):
                    with self.assertRaises(
                        harness_bridge.ApcChildUnavailableError
                    ):
                        harness_bridge.run_apc_child(
                            request=absent_request,
                            template_record=template_record,
                            launcher=self._launcher(),
                            store=self.memory_store,
                            limits=self.limits,
                            clock=self.clock,
                            deadline=self.clock() + 240.0,
                        )
            self.assertFalse(absent_worktree.exists())

        operations = self.memory_store.list_apc_child_operations("decision-7")
        self.assertEqual(
            ["ambiguous"], [operation["status"] for operation in operations]
        )
        ambiguous = operations[-1]
        self.assertIn("ambiguous", contracts.APC_CHILD_UNRESOLVED_STATUSES)
        self.assertIsNone(ambiguous.get("observed_invocation"))
        self.assertEqual("bootstrap", ambiguous["launch_intent"]["phase"])
        self.assertEqual(lane_id, ambiguous["launch_intent"]["lane_id"])
        self.assertEqual(str(worktree), ambiguous["launch_intent"]["worktree_path"])

        # The branch-only survivor is unresolved with its exact lane and the
        # attempt-created branch ref kept visible for reconciliation.
        branch_operations = self.memory_store.list_apc_child_operations("decision-700")
        self.assertEqual(
            ["ambiguous"], [operation["status"] for operation in branch_operations]
        )
        branch_ambiguous = branch_operations[-1]
        self.assertEqual(
            branch_lane, branch_ambiguous["launch_intent"]["lane_id"]
        )
        self.assertIn(
            branch_ref, branch_ambiguous["launch_intent"]["acknowledgement"]
        )

        absent_operations = self.memory_store.list_apc_child_operations("decision-70")
        self.assertEqual(
            ["refused"], [operation["status"] for operation in absent_operations]
        )
        self.assertIn("refused", contracts.APC_CHILD_TERMINAL_STATUSES)

if __name__ == "__main__":
    unittest.main()
