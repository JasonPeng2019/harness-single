from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import apc, atlas, config, contracts, experience, privacy, runtime, store, templates


class StageAIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store_path = self.root / "memory-state.sqlite3"
        self.policy = privacy.PrivacyPolicy(
            known_secrets=("synthetic-secret-alpha-1234567890",),
            forbidden_environment_keys=("MEMORY_HARNESS_CONTROL_TOKEN",),
        )
        self.store = store.MemoryStore(self.store_path)
        self.store.initialize()
        self.memory_runtime = runtime.MemoryRuntime(self.store, privacy_policy=self.policy)
        self.plan = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "implement", "verify"]},
            accepted_by="ROOT",
        )
        self.card = contracts.make_task_card(
            task="Repair the regression and verify it",
            base_commit="base-1",
            branch="lane/integration",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1",
                route="ordinary",
                plan=self.plan,
                configuration={"strategy": "standard"},
            ),
        )

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def test_durable_reopen_linked_outcome_replay_and_conflict(self) -> None:
        prepared = self.memory_runtime.prepare(
            plan=self.plan,
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.root / "worktree",
            base_commit="base-1",
        )
        envelope = prepared.envelope
        self.memory_runtime.dispatch(envelope, lambda record: {"invocation_id": "ctrl-1"})
        outcome = self.memory_runtime.record_outcome(
            decision_id=envelope["decision_id"],
            plan_id=self.plan["plan_id"],
            plan_digest=self.plan["content_hash"],
            status="PASS",
            evidence_digest="evidence-digest",
            linked_run_id="run-1",
        )
        replay = self.memory_runtime.record_outcome(
            decision_id=envelope["decision_id"],
            plan_id=self.plan["plan_id"],
            plan_digest=self.plan["content_hash"],
            status="PASS",
            evidence_digest="evidence-digest",
            linked_run_id="run-1",
        )
        self.assertEqual(outcome["outcome_id"], replay["outcome_id"])

        self.store.close()
        reopened = store.MemoryStore(self.store_path)
        reopened.initialize()
        reopened_runtime = runtime.MemoryRuntime(reopened)
        try:
            persisted = reopened.get_outcome(outcome["decision_id"])
            self.assertEqual(outcome["outcome_id"], persisted["outcome_id"])
            with self.assertRaisesRegex(store.OutcomeConflictError, "conflict"):
                reopened_runtime.record_outcome(
                    decision_id=envelope["decision_id"],
                    plan_id=self.plan["plan_id"],
                    plan_digest=self.plan["content_hash"],
                    status="FAIL",
                    evidence_digest="different-evidence",
                    linked_run_id="run-1",
                )
        finally:
            reopened.close()

    def test_deterministic_atlas_fixture_and_sanitized_representation(self) -> None:
        examples = experience.load_experience_examples(self.policy)
        documents = [atlas.build_representation(example, self.policy) for example in examples]
        fixture = atlas.LocalAtlasFixture(documents)
        query = atlas.build_atlas_query(
            "regression repair synthetic-secret-alpha-1234567890",
            objective_id="objective-regression",
            route="ordinary",
            privacy_policy=self.policy,
        )
        results = fixture.search(query)
        self.assertTrue(results)
        self.assertEqual(results, fixture.search(query))
        self.assertNotIn("synthetic-secret-alpha-1234567890", json.dumps(query.to_record()))

    def test_direct_fill_and_fresh_fallback_are_deterministic(self) -> None:
        registry = templates.load_default_templates()
        template = templates.select_template("Fix the regression failure", registry)
        self.assertIsNotNone(template)
        direct = templates.direct_fill(
            template,
            {"failure": "test_regression fails", "component": "parser"},
            objective_id="objective-1",
            route="ordinary",
        )
        self.assertEqual("proposed", direct["state"])
        self.assertEqual(template.template_id, direct["source"]["template_id"])
        self.assertEqual(template.fixed_steps, tuple(direct["content"]["fixed_steps"]))

        with self.assertRaisesRegex(templates.MissingBindingsError, "failure"):
            templates.direct_fill(template, {}, objective_id="objective-1", route="ordinary")

        fresh = templates.fresh_plan(objective_id="objective-1", route="ordinary")
        self.assertEqual("fresh", fresh["state"])
        self.assertNotIn("source", fresh)

    def test_apc_request_and_result_are_bound_to_parent_template_and_permitted_edits(self) -> None:
        registry = templates.load_default_templates()
        template = templates.select_template("Fix the regression failure", registry)
        binding = {
            "provider": "synthetic-provider",
            "model": "synthetic-model",
            "cli": "synthetic-cli",
            "effort": "medium",
            "source": "explicit",
        }
        request = apc.make_apc_request(
            template=template,
            parent_decision_id="decision-1",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding=binding,
        )
        apc.validate_apc_request(request)
        draft = contracts.make_plan(
            plan_id="apc-draft-1",
            objective_id="objective-1",
            route="ordinary",
            state="proposed",
            content={"fixed_steps": list(template.fixed_steps), "bindings": {"failure": "test"}},
            source={"template_id": template.template_id, "apc_request": request["content_hash"]},
        )
        result = apc.make_apc_result(request, draft)
        apc.validate_apc_result(result, request)
        self.assertEqual("decision-1", result["parent_decision_id"])
        self.assertEqual("objective-1", result["parent_objective_id"])
        self.assertEqual(template.template_id, result["template_id"])
        self.assertEqual(template.version, result["template_version"])
        self.assertEqual(request["template_digest"], result["template_digest"])
        self.assertEqual(("bindings",), tuple(result["permitted_edits"]))
        self.assertEqual("proposed", result["proposed_plan"]["state"])

        with self.assertRaisesRegex(apc.APCError, "permitted"):
            apc.make_apc_request(
                template=template,
                parent_decision_id="decision-1",
                parent_objective_id="objective-1",
                permitted_edits=["fixed_steps"],
                binding=binding,
            )

    def test_all_off_and_deferred_strategy_do_not_create_learner_state(self) -> None:
        resolved = config.resolve_config({"all_features": False})
        self.assertTrue(resolved.all_off)
        with self.assertRaisesRegex(config.DeferredCapabilityError, "deferred/not implemented"):
            config.resolve_config({"strategy": "learned"})


if __name__ == "__main__":
    unittest.main()
