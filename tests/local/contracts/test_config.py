from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import config


class FixedConfigurationTests(unittest.TestCase):
    def test_standard_problem_focused_and_deeper_resolve_deterministically(self) -> None:
        standard = config.resolve_config({"strategy": "standard"})
        problem = config.resolve_config({"strategy": "problem_focused"})
        deeper = config.resolve_config({"strategy": "deeper"})
        self.assertEqual("standard", standard.strategy)
        self.assertEqual("problem_focused", problem.strategy)
        self.assertEqual("deeper", deeper.strategy)
        self.assertEqual(standard, config.resolve_config({"strategy": "standard"}))

    def test_unknown_strategy_uses_recorded_standard_fallback(self) -> None:
        resolved = config.resolve_config({"strategy": "not-a-strategy"})
        self.assertEqual("standard", resolved.strategy)
        self.assertIn("fallback", resolved.reason.lower())

    def test_all_off_is_available_and_contains_no_memory_pipeline(self) -> None:
        resolved = config.resolve_config({"all_features": False})
        self.assertTrue(resolved.all_off)
        self.assertFalse(resolved.experience_read)
        self.assertFalse(resolved.experience_write)
        self.assertFalse(resolved.template_memory)
        self.assertFalse(resolved.apc)
        self.assertFalse(resolved.light_adaptation)

    def test_learned_mode_is_rejected_before_preparation(self) -> None:
        requests = [
            {"strategy": "learned"},
            {"learned_mode": True},
            {"learned_selection": True},
            {"policy_load": True},
            {"training": True},
            {"policy_update": True},
        ]
        for request in requests:
            with self.assertRaisesRegex(config.DeferredCapabilityError, "deferred/not implemented"):
                config.resolve_config(request)

    def test_feature_prerequisites_resolve_to_effective_off(self) -> None:
        resolved = config.resolve_config(
            {
                "experience_write": False,
                "generated_skill_creation": True,
                "template_memory": False,
                "apc": True,
                "light_adaptation": True,
            }
        )
        self.assertFalse(resolved.generated_skill_creation)
        self.assertFalse(resolved.apc)
        self.assertFalse(resolved.light_adaptation)

        deeper_disabled = config.resolve_config(
            {"strategy": "deeper", "deeper": False}
        )
        self.assertEqual("standard", deeper_disabled.strategy)
        self.assertIn("fallback", deeper_disabled.reason)

    def test_atlas_requires_both_launched_payload_and_independent_block_proof(self) -> None:
        evidence = {
            "launched_payload": {"verified": True, "evidence_id": "payload-7"},
            "unrelated_destination_block": {
                "blocked": True, "independent": True,
                "source_kind": "independent_network_boundary", "evidence_id": "egress-4",
            },
        }
        resolved = config.resolve_network_mode("atlas_memory_only", evidence=evidence)
        self.assertEqual("soft_guardrail_network", resolved.effective_mode)
        self.assertEqual(evidence, resolved.input_evidence)
        self.assertNotIn("verified_launched_payload:payload-7", resolved.enforcement_sources)
        self.assertIn("unverified", " ".join(resolved.disclosed_limits))

        context = {
            "objective_id": "objective-1", "task_card_digest": "card-1",
            "plan_id": "plan-1", "plan_digest": "plan-hash-1",
            "decision_id": "decision-1", "route": "ordinary", "plan_state": "candidate",
        }
        for verifier in (None, lambda evidence, context: False,
                         lambda evidence, context: (_ for _ in ()).throw(RuntimeError("unavailable")),
                         lambda evidence, context: {"context": {**context, "decision_id": "other"},
                                                     "launched_payload": evidence["launched_payload"],
                                                     "unrelated_destination_block": evidence["unrelated_destination_block"]}):
            with self.subTest(verifier=verifier):
                weaker = config.NetworkResolver(verifier=verifier).resolve(
                    "atlas_memory_only", evidence=evidence, context=context,
                )
                self.assertEqual("soft_guardrail_network", weaker.effective_mode)
                self.assertIn("shell egress", " ".join(weaker.disclosed_limits))
        trusted = config.NetworkResolver(verifier=lambda evidence, received: received == context)
        verified = trusted.resolve("atlas_memory_only", evidence=evidence, context=context)
        self.assertEqual("atlas_memory_only", verified.effective_mode)
        self.assertEqual(context, verified.context)
        self.assertIs(verified.verification_result, True)
        self.assertIn("verified_launched_payload:payload-7", verified.enforcement_sources)
        self.assertIn("independent_egress_block:egress-4", verified.enforcement_sources)
        self.assertEqual("soft_guardrail_network", trusted.resolve(
            "atlas_memory_only", evidence=evidence,
        ).effective_mode)
        self.assertEqual("soft_guardrail_network", trusted.resolve(
            "atlas_memory_only", evidence=evidence, context={**context, "decision_id": "other"},
        ).effective_mode)
        verified_record = config.NetworkResolver(verifier=lambda facts, received: {
            "context": received, "launched_payload": facts["launched_payload"],
            "unrelated_destination_block": facts["unrelated_destination_block"],
        }).resolve("atlas_memory_only", evidence=evidence, context=context)
        self.assertEqual("atlas_memory_only", verified_record.effective_mode)
        self.assertEqual(context, verified_record.verification_result["context"])
        with self.assertRaises(TypeError):
            config.resolve_network_mode("atlas_memory_only", evidence=evidence, verifier=lambda *_: True)
        with self.assertRaises(TypeError):
            trusted.resolve("atlas_memory_only", evidence=evidence, context=context,
                            verifier=lambda *_: True)

        for absent in ("launched_payload", "unrelated_destination_block"):
            with self.subTest(absent=absent):
                weaker = config.resolve_network_mode(
                    "atlas_memory_only", evidence={k: v for k, v in evidence.items() if k != absent}
                )
                self.assertEqual("soft_guardrail_network", weaker.effective_mode)
                self.assertIn(absent, " ".join(weaker.disclosed_limits))
                self.assertIn("shell egress", " ".join(weaker.disclosed_limits))

        same_user = config.resolve_network_mode("atlas_memory_only", evidence={
            **evidence, "unrelated_destination_block": {
                "blocked": True, "independent": True,
                "source_kind": "same_user_process", "evidence_id": "process-flags",
            },
        })
        self.assertEqual("soft_guardrail_network", same_user.effective_mode)
        self.assertIn("independent", " ".join(same_user.disclosed_limits))
        self.assertEqual("soft_guardrail_network", config.NetworkResolver(
            verifier=lambda evidence, context: True,
        ).resolve("atlas_memory_only", evidence=same_user.input_evidence,
                  context=context).effective_mode)

    def test_soft_restricted_and_invalid_network_requests(self) -> None:
        soft = config.resolve_network_mode("soft_guardrail_network")
        self.assertEqual("soft_guardrail_network", soft.effective_mode)
        self.assertIn("shell egress", " ".join(soft.disclosed_limits))
        self.assertNotIn("hardened", " ".join(soft.enforcement_sources))
        restricted = config.resolve_network_mode("restricted_local")
        self.assertEqual("restricted_local", restricted.effective_mode)
        self.assertIn("service_entry_policy_required", restricted.enforcement_sources)
        self.assertIn("optional Atlas task-path", " ".join(restricted.disclosed_limits))
        for mode in ("", "unknown", "Atlas_Memory_Only", None):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                config.resolve_network_mode(mode)
        with self.assertRaises(ValueError):
            config.resolve_network_mode("atlas_memory_only", evidence={
                "launched_payload": {"verified": True},
            })


if __name__ == "__main__":
    unittest.main()
