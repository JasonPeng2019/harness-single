from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.git_safety import GitSafetyError, validate_finding_triage, validate_findings
from firmware_acceptance import finding_gate_fragment


class FindingGateTests(unittest.TestCase):
    def _value(self, findings: list[object]) -> dict[str, object]:
        return {"schema": "orchestrator-review-findings/v1", "lane_id": "L", "worker_invocation_id": "W", "role": "reviewer", "commit": "a" * 40, "findings": findings}

    def _finding(self) -> dict[str, object]:
        return {"id": "F1", "category": "FUNCTIONALITY_BREAKING", "affected_ids": ["C39"], "evidence": ["test://f1"], "observed": "bad", "expected": "good", "reproduction": "run fixture", "impact": "unsafe", "no_fix_consequence": "remains unsafe", "smallest_fix": "one check", "complexity": "small", "regression_risk": "bounded", "verification_cost": "one test", "alternatives": "none lower risk", "cost_benefit": "impact exceeds risk", "problem_outweighs_fix_risk": True}

    def test_empty_pass_and_admissible_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "FINDINGS.json"
            path.write_text(json.dumps(self._value([])), encoding="utf-8")
            self.assertEqual(0, validate_findings(path, lane_id="L", worker_invocation_id="W", role="reviewer", commit="a" * 40, outcome="PASS")["count"])
            path.write_text(json.dumps(self._value([self._finding()])), encoding="utf-8")
            self.assertEqual(1, validate_findings(path, lane_id="L", worker_invocation_id="W", role="reviewer", commit="a" * 40, outcome="FAIL")["count"])

    def test_rejects_pass_gap_and_missing_reproduction(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "FINDINGS.json"
            path.write_text(json.dumps(self._value([self._finding()])), encoding="utf-8")
            with self.assertRaises(GitSafetyError):
                validate_findings(path, lane_id="L", worker_invocation_id="W", role="reviewer", commit="a" * 40, outcome="PASS")
            broken = self._finding(); del broken["reproduction"]
            path.write_text(json.dumps(self._value([broken])), encoding="utf-8")
            with self.assertRaises(GitSafetyError):
                validate_findings(path, lane_id="L", worker_invocation_id="W", role="reviewer", commit="a" * 40, outcome="FAIL")

    def test_triage_and_role_fragment_are_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary) / ".agent-workspace"; workspace.mkdir()
            self.assertEqual("reviewer", finding_gate_fragment("reviewer", workspace)["finding_gate"]["role"])
            findings = workspace / "FINDINGS.json"; findings.write_text("{}", encoding="utf-8")
            digest = __import__("hashlib").sha256(findings.read_bytes()).hexdigest()
            value = {"schema":"orchestrator-review-triage/v1","owner":"ROOT-IM","findings_path":str(findings),"findings_sha256":digest,"decisions":[{"id":"F1","decision":"ACCEPT","rationale":"evidence","conclusion":"PROBLEM_OUTWEIGHS_FIX_RISK","smallest_fix":"one check"}]}
            validate_finding_triage(value, findings_path=findings, findings_sha256=digest, finding_ids={"F1"})
