from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import atlas, apc, contracts, experience, privacy


class PrivacyBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = privacy.PrivacyPolicy(
            known_secrets=("synthetic-secret-alpha-1234567890",),
            forbidden_environment_keys=("MEMORY_HARNESS_CONTROL_TOKEN",),
        )

    def test_mandatory_secret_blocks_without_rewriting_the_raw_source(self) -> None:
        raw_task = "Use synthetic-secret-alpha-1234567890 in the repository."
        with self.assertRaisesRegex(privacy.MandatorySecretError, "configured secret"):
            privacy.guard_mandatory(raw_task, self.policy)
        self.assertIn("synthetic-secret-alpha-1234567890", raw_task)

    def test_optional_content_is_omitted_and_raw_evidence_is_unchanged(self) -> None:
        raw = "Historical note mentions synthetic-secret-alpha-1234567890."
        sanitized = privacy.sanitize_optional(raw, self.policy)
        self.assertIsNone(sanitized)
        self.assertIn("synthetic-secret-alpha-1234567890", raw)

    def test_worker_environment_has_no_control_credentials(self) -> None:
        environment = {
            "PATH": "/usr/bin",
            "MEMORY_HARNESS_CONTROL_TOKEN": "synthetic-secret-alpha-1234567890",
            "OPENAI_API_KEY": "provider-transport-secret",
        }
        safe = privacy.worker_environment(environment, self.policy)
        self.assertEqual(
            {"PATH": "/usr/bin", "OPENAI_API_KEY": "provider-transport-secret"},
            safe,
        )
        self.assertNotIn("synthetic-secret-alpha-1234567890", json.dumps(safe))

    def test_worker_prompt_has_no_control_credentials_or_forbidden_authority(self) -> None:
        prompt = privacy.worker_prompt(
            "Inspect the failing test.",
            {"steps": ["read", "fix"]},
            optional_content="Prior failure mentions synthetic-secret-alpha-1234567890.",
            privacy_policy=self.policy,
        )
        self.assertNotIn("synthetic-secret-alpha-1234567890", prompt)
        self.assertNotIn("MEMORY_HARNESS_CONTROL_TOKEN", prompt)
        self.assertNotIn("approve/publish/revoke", prompt)

    def test_mandatory_plan_secret_blocks_instead_of_being_rewritten(self) -> None:
        plan = {"steps": ["Use synthetic-secret-alpha-1234567890"]}
        with self.assertRaisesRegex(privacy.MandatorySecretError, "configured secret"):
            privacy.worker_prompt(
                "Inspect the failing test.",
                plan,
                privacy_policy=self.policy,
            )
        self.assertIn("synthetic-secret-alpha-1234567890", plan["steps"][0])

    def test_experience_fixture_sanitizes_but_preserves_raw_authoritative_record(self) -> None:
        examples = experience.load_experience_examples(self.policy)
        self.assertTrue(examples)
        raw = examples[0].raw_content
        derived = experience.derived_optional_content(examples[0], self.policy)
        self.assertNotIn("synthetic-secret-alpha-1234567890", json.dumps(derived))
        self.assertIn("synthetic-secret-alpha-1234567890", raw)

    def test_atlas_query_never_contains_provider_authentication(self) -> None:
        query = atlas.build_atlas_query(
            "Find the prior regression repair",
            objective_id="objective-1",
            route="ordinary",
            privacy_policy=self.policy,
            credentials={"api_key": "synthetic-secret-alpha-1234567890"},
        )
        serialized = json.dumps(query.to_record())
        self.assertNotIn("synthetic-secret-alpha-1234567890", serialized)
        self.assertNotIn("api_key", serialized)

    def test_apc_payload_never_contains_provider_authentication(self) -> None:
        template = {
            "template_id": "template-1",
            "version": 1,
            "required_fields": ["failure"],
            "allowed_edits": ["bindings"],
        }
        request = apc.make_apc_request(
            template=template,
            parent_decision_id="decision-1",
            parent_objective_id="objective-1",
            permitted_edits=["bindings"],
            binding={
                "provider": "codex",
                "model": "model-a",
                "cli": "codex",
                "effort": "high",
                "source": "explicit",
            },
            credentials={"api_key": "synthetic-secret-alpha-1234567890"},
        )
        serialized = json.dumps(request)
        self.assertNotIn("synthetic-secret-alpha-1234567890", serialized)
        self.assertNotIn("api_key", serialized)

    def test_worker_bound_meaning_matrix(self) -> None:
        unsafe = (
            {"APIKEY": "synthetic-credential-value"},
            "API_KEY=synthetic-credential-value",
            '{"nested":{"api_key":"synthetic-credential-value"}}',
            '{"nested":{"api_\\u006bey":"synthetic-credential-value"}}',
            {"nested": [{"publish_authority": "enabled"}]},
            {"authority": True},
            "{\"nested\":{\"publish_\\u0061uthority\":true}}",
            'credential=synthetic-secret-alpha-1234567890',
            'credential=synthetic-secret-alpha-1234567890'.replace('secret', 'secr\\u0065t'),
        )
        for value in unsafe:
            with self.subTest(value=value):
                with self.assertRaises(privacy.MandatorySecretError) as error:
                    privacy.guard_worker_bound(value, self.policy)
                self.assertNotIn("synthetic-credential-value", str(error.exception))
                self.assertNotIn("synthetic-secret-alpha-1234567890", str(error.exception))

    def test_worker_bound_preserves_safe_meaning(self) -> None:
        safe = (
            {"role_label": "publisher"},
            {"shared_publication": "historical example"},
            {"publish_authority": False},
            "publish_authority=false",
            "The 2024 report discussed how approval worked.",
            {"schema": "trusted-procedure-approval/v1", "authority_evidence": {"review": "ROOT approved"}},
            {"schema": "trusted-procedure-revision/v1", "body": "Read the log"},
            {"task_credential_channel": {"scope": "task_only", "validated": True, "channel_id": "task-secret"}},
        )
        for value in safe:
            with self.subTest(value=value):
                privacy.guard_worker_bound(value, self.policy)

    def test_optional_and_remote_worker_bound_policy(self) -> None:
        unsafe = {"content": {"nested": {"publish_authority": True}}}
        self.assertIsNone(privacy.sanitize_optional(unsafe, self.policy))
        with self.assertRaises(privacy.RemotePayloadPrivacyError):
            privacy.guard_worker_bound_remote(unsafe, self.policy)
        # Publication records retain their approved authority meaning; their
        # credential values still fail the existing whole-publication scan.
        privacy.guard_remote_payload({"authority": "ROOT approval"}, self.policy)
        with self.assertRaises(privacy.RemotePayloadPrivacyError):
            privacy.guard_remote_payload({"api_key": "synthetic-credential-value"}, self.policy)

    def test_worker_prompt_omits_only_unsafe_optional_item(self) -> None:
        prompt = privacy.worker_prompt("Inspect", {"steps": ["read"]}, optional_content=[
            {"id": "safe", "content": "prior regression"},
            {"id": "unsafe", "content": "APIKEY=credential-value"},
        ], privacy_policy=self.policy)
        self.assertIn("prior regression", prompt)
        self.assertNotIn("credential-value", prompt)

    def test_validated_procedure_approval_preserves_authority_evidence(self) -> None:
        scope = {"application": "app", "namespace": "ns", "project": "p", "owner": "ROOT"}
        procedure = contracts.make_procedure_revision(
            logical_name="safe approval", origin="curated", origin_scope=scope,
            body="Inspect the result", references=[],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
            source={"kind": "curated_authoring"},
        )
        approval = contracts.make_procedure_approval(
            approval_id="approval-1", procedure=procedure, issuer="ROOT",
            recipients=[scope], authority_evidence={"publish_authority": True},
        )
        trusted = privacy.PrivacyPolicy(
            known_secrets=self.policy.known_secrets,
            trusted_approval_hashes=(approval["content_hash"],),
        )
        privacy.guard_worker_bound({"procedure": procedure, "approval": approval}, trusted)
        compact = contracts.make_procedure_compact_representation(
            procedure=procedure, content={"steps": ["Inspect"]},
        )
        compact_approval = contracts.make_procedure_compact_approval(
            procedure=procedure, full_approval=approval, representation=compact,
            approval_id="compact-approval-1", issuer="ROOT",
            authority_evidence={"publish_authority": True},
        )
        trusted = privacy.PrivacyPolicy(
            known_secrets=self.policy.known_secrets,
            trusted_approval_hashes=(approval["content_hash"], compact_approval["content_hash"]),
        )
        privacy.guard_worker_bound({
            "content": {"procedure": procedure, "approval": approval},
            "compact_representation": compact, "compact_approval": compact_approval,
        }, trusted)
        with self.assertRaises(privacy.MandatorySecretError):
            privacy.guard_worker_bound({"procedure": procedure, "approval": {
                **approval, "authority_evidence": {"api_key": "credential-value"},
            }}, self.policy)
        with self.assertRaises(privacy.MandatorySecretError):
            privacy.guard_worker_bound({"authority_evidence": {"publish_authority": True}}, self.policy)

    def test_worker_environment_filters_equivalent_control_keys(self) -> None:
        environment = {
            "PATH": "/usr/bin", "OPENAI_API_KEY": "provider-transport-secret",
            "MEMORY_HARNESS_APIKEY": "control", "OTHER_PUBLISH_AUTHORITY": "yes",
            "MEMORY_HARNESS_CONTROL_ENDPOINT": "local-control-socket",
        }
        self.assertEqual(
            {"PATH": "/usr/bin", "OPENAI_API_KEY": "provider-transport-secret"},
            privacy.worker_environment(environment, self.policy),
        )

    def test_rejected_review_role_action_and_embedded_json_matrix(self) -> None:
        unsafe = (
            {"ROOT_ROLE_ENABLED": True},
            {"allow_policy_write": True},
            {"tools": {"publish_enabled": True}},
            {"ROOT_APPROVAL_TOKEN": "synthetic-control-value"},
            'prior note: {"publish_authority":true}',
            'prior note: {"publish_\\u0061uthority":true}',
            'prior note: "{\\"publish_\\u0061uthority\\":true}"',
        )
        for value in unsafe:
            with self.subTest(value=value):
                with self.assertRaises(privacy.MandatorySecretError) as error:
                    privacy.guard_worker_bound(value, self.policy)
                self.assertNotIn("synthetic-control-value", str(error.exception))
                self.assertIsNone(privacy.sanitize_optional(value, self.policy))
                with self.assertRaises(privacy.RemotePayloadPrivacyError):
                    privacy.safe_query_payload(value, self.policy)
                with self.assertRaises(privacy.RemotePayloadPrivacyError):
                    privacy.guard_worker_bound_remote(value, self.policy)
        self.assertEqual(
            {"OPENAI_API_KEY": "provider-transport-secret"},
            privacy.worker_environment({
                "ROOT_APPROVAL_TOKEN": "synthetic-control-value",
                "OPENAI_API_KEY": "provider-transport-secret",
            }, self.policy),
        )

    def test_structural_approval_cannot_self_assert_worker_authority(self) -> None:
        scope = {"application": "app", "namespace": "ns", "project": "p", "owner": "untrusted"}
        procedure = contracts.make_procedure_revision(
            logical_name="self claim", origin="curated", origin_scope=scope,
            body="Inspect", references=[],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
            source={"kind": "curated_authoring"},
        )
        approval = contracts.make_procedure_approval(
            approval_id="self", procedure=procedure, issuer="untrusted",
            recipients=[scope], authority_evidence={"publish_authority": True},
        )
        value = {"procedure": procedure, "approval": approval}
        with self.assertRaises(privacy.MandatorySecretError):
            privacy.guard_worker_bound(value, self.policy)
        with self.assertRaises(privacy.RemotePayloadPrivacyError):
            privacy.safe_query_payload(value, self.policy)
        with self.assertRaises(privacy.RemotePayloadPrivacyError):
            privacy.guard_worker_bound_remote(value, self.policy)
        prompt = privacy.worker_prompt("Inspect", {"steps": ["read"]},
                                       optional_content=[value], privacy_policy=self.policy)
        self.assertNotIn("publish_authority", prompt)

    def test_canonical_key_spelling_matrix_across_worker_sinks(self) -> None:
        cases = (
            ("ROOTApprovalToken", "credential"),
            ("ROOT_APPROVALTOKEN", "credential"),
            ("MEMORY_HARNESS_ROOT_APPROVALTOKEN", "credential"),
            ("ROOTRoleEnabled", "authority"),
            ("allowPOLICYWrite", "authority"),
        )
        for key, kind in cases:
            for value in ({key: True}, f"{key}=true", f"note: {json.dumps({key: True})}"):
                with self.subTest(key=key, value=value):
                    with self.assertRaises(privacy.MandatorySecretError) as error:
                        privacy.guard_mandatory(value, self.policy)
                    self.assertNotIn(key, str(error.exception))
                    self.assertIsNone(privacy.sanitize_optional(value, self.policy))
                    with self.assertRaises(privacy.RemotePayloadPrivacyError):
                        privacy.safe_query_payload(value, self.policy)
                    with self.assertRaises(privacy.RemotePayloadPrivacyError):
                        privacy.guard_worker_bound_remote(value, self.policy)
                    if kind == "credential":
                        with self.assertRaises(privacy.RemotePayloadPrivacyError):
                            privacy.guard_remote_payload(value, self.policy)
                    else:
                        privacy.guard_remote_payload(value, self.policy)
            self.assertNotIn(key, privacy.worker_environment({key: "true"}, self.policy))

    def test_parsed_json_spans_preserve_safe_values(self) -> None:
        safe = (
            'note: {"publish_authority":false}',
            'note: {"ROOT_ROLE_ENABLED":false}',
            'note: {"authority":"historical_evidence_only"}',
            'note: "{\\"publish_authority\\":false}"',
            'note: {"ROOT_\\u0052OLE_ENABLED":false}',
        )
        for value in safe:
            with self.subTest(value=value):
                privacy.guard_mandatory(value, self.policy)
                self.assertEqual(value, privacy.sanitize_optional(value, self.policy))
                privacy.safe_query_payload(value, self.policy)
                privacy.guard_worker_bound_remote(value, self.policy)
        for value in ('note: {"ROOTRoleEnabled":true}',
                      'note: "{\\"ROOTApprovalToken\\":true}"',
                      'note: {"ROOT_\\u0041PPROVALTOKEN":true}'):
            with self.subTest(value=value):
                with self.assertRaises(privacy.MandatorySecretError):
                    privacy.guard_mandatory(value, self.policy)

    def test_task_channel_variant_requires_valid_shape(self) -> None:
        valid = {"TaskCredentialChannel": {"scope": "task_only", "validated": True,
                                            "channel_id": "task-channel"}}
        privacy.guard_mandatory(valid, self.policy)
        invalid = {"TaskCredentialChannel": {**valid["TaskCredentialChannel"], "validated": False}}
        with self.assertRaises(privacy.MandatorySecretError):
            privacy.guard_mandatory(invalid, self.policy)
        self.assertIsNone(privacy.sanitize_optional(invalid, self.policy))
        with self.assertRaises(privacy.RemotePayloadPrivacyError):
            privacy.safe_query_payload(invalid, self.policy)

    def test_residual_assignment_lexer_blocks_spaced_governed_keys(self) -> None:
        cases = (
            ("ROOT ROLE ENABLED", "authority"),
            ("allow POLICY write", "authority"),
            ("ROOT APPROVAL TOKEN", "credential"),
            ("root-APPROVAL token", "credential"),
            ("memory_harness ROOT approval.token", "credential"),
        )
        for key, kind in cases:
            assigned = f"note: {key}=synthetic-control-value"
            for value in ({key: True}, assigned,
                          f"note: {json.dumps({key: True})}",
                          f'note: "{json.dumps({key: True}).replace(chr(34), chr(92) + chr(34))}"'):
                with self.subTest(key=key, value=value):
                    with self.assertRaises(privacy.MandatorySecretError) as error:
                        privacy.guard_mandatory(value, self.policy)
                    self.assertNotIn("synthetic-control-value", str(error.exception))
                    self.assertIsNone(privacy.sanitize_optional(value, self.policy))
                    with self.assertRaises(privacy.RemotePayloadPrivacyError):
                        privacy.safe_query_payload(value, self.policy)
                    with self.assertRaises(privacy.RemotePayloadPrivacyError):
                        privacy.guard_worker_bound_remote(value, self.policy)
                    if kind == "credential":
                        with self.assertRaises(privacy.RemotePayloadPrivacyError):
                            privacy.guard_remote_payload(value, self.policy)
                    else:
                        privacy.guard_remote_payload(value, self.policy)
            self.assertNotIn("NOTE", privacy.worker_environment({"NOTE": assigned}, self.policy))
        self.assertEqual(
            {"OPENAI_API_KEY": "provider-transport-secret", "HISTORY": "prior approval discussion"},
            privacy.worker_environment({
                "OPENAI_API_KEY": "provider-transport-secret", "HISTORY": "prior approval discussion",
                "CONTROL_NOTE": "ROOT APPROVAL TOKEN=synthetic-control-value",
            }, self.policy),
        )

    def test_residual_assignment_scalar_boundaries(self) -> None:
        safe = (
            "note: publish_authority=false.",
            "note: authority=historical_evidence_only.",
            'note: {"publish_authority":false} and authority=excluded.',
            "note: ROOT ROLE ENABLED=false.",
            "note: authority='evidence_only'.",
            "note: publish_authority=null)",
            "The historical report discussed approval and publication authority.",
            {"role_label": "publisher", "shared_publication": "historical example"},
        ) + tuple(f"note: authority={token}." for token in
                  ("none", "false", "0", "null", "excluded",
                   "historical_evidence_only", "evidence_only"))
        for value in safe:
            with self.subTest(value=value):
                privacy.guard_mandatory(value, self.policy)
                self.assertEqual(value, privacy.sanitize_optional(value, self.policy))
                privacy.safe_query_payload(value, self.policy)
                privacy.guard_worker_bound_remote(value, self.policy)
        unsafe = (
            "note: ROOT ROLE ENABLED=true.",
            "note: authority=false.evil",
            "note: authority=false0",
            "note: authority=historical_evidence_only_extra",
            'note: authority="false.evil"',
            "note: authority=excluded. and ROOT APPROVAL TOKEN=x",
            'note: {"publish_authority":false} and allow POLICY write=true',
        )
        for value in unsafe:
            with self.subTest(value=value):
                with self.assertRaises(privacy.MandatorySecretError):
                    privacy.guard_mandatory(value, self.policy)
                self.assertIsNone(privacy.sanitize_optional(value, self.policy))
                self.assertEqual(privacy.REDACTION_MARKER, privacy.sanitize_text(value, self.policy))

    def test_safe_assignment_trailer_orders_preserve_exact_values(self) -> None:
        tokens = ("none", "false", "0", "null", "excluded",
                  "historical_evidence_only", "evidence_only")
        for token in tokens:
            safe = (
                f"note (authority={token}.)",
                f"note (authority={token}).",
                f'note (authority="{token}".)',
                f'note (authority="{token}").',
            )
            for value in safe:
                with self.subTest(value=value):
                    privacy.guard_mandatory(value, self.policy)
                    self.assertEqual(value, privacy.sanitize_optional(value, self.policy))
                    privacy.safe_query_payload(value, self.policy)
                    privacy.guard_worker_bound_remote(value, self.policy)
        for value in ("publish_authority=false.)", "authority=none.)))))))",
                      "authority=none,role_label=historical",
                      "The historical report discussed how approval worked."):
            with self.subTest(value=value):
                privacy.guard_mandatory(value, self.policy)
        for value in ("authority=false.evil", "authority=false0",
                      "authority=historical_evidence_only_extra",
                      "authority=false.)evil", "authority=none.))))))))",
                      "authority=none,ROOT APPROVAL TOKEN=x",
                      "authority=none.) and ROOT APPROVAL TOKEN=x",
                      "publish_authority=true.)", "ROOT APPROVAL TOKEN=x"):
            with self.subTest(value=value):
                with self.assertRaises(privacy.MandatorySecretError) as error:
                    privacy.guard_mandatory(value, self.policy)
                self.assertNotIn("ROOT APPROVAL TOKEN=x", str(error.exception))


if __name__ == "__main__":
    unittest.main()
