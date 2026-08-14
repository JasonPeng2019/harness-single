from __future__ import annotations
import json, subprocess
from dataclasses import dataclass
from typing import Any, Protocol

ALLOWED = {"loop", "starvation", "regression", "manager_failure", "inefficiency"}
OWNERSHIP_PHASE_RULES = (
    {
        "case": "ordinary_setup_or_preparation_missing_request_identity",
        "disposition": "not_alertable",
        "rule": "Absence of request identity during ordinary setup or preparation is not a defect.",
    },
    {
        "case": "controller_pre_start_missing_declared_mcp_lifetime",
        "disposition": "not_alertable",
        "rule": "A declared MCP lifetime may be absent while the controller is preparing to start it.",
    },
    {
        "case": "ownership_failure",
        "disposition": "alertable_only_with_explicit_evidence",
        "rule": "An ownership failure requires explicit evidence that the lane is awaiting a permission relay plus missing or inconsistent lane-correlated active lifetime or request evidence.",
    },
    {
        "case": "specific_ownership_defects",
        "disposition": "alertable",
        "rule": "Real duplicate ownership, resource conflict, stale or mismatched relay, and request-awaiting identity loss remain alertable.",
    },
)
STALE_STATUS_QUARANTINE_RULE = {
    "case": "stale_status_exact_identity_quarantine",
    "disposition": "not_alertable_until_one_full_watcher_poll_interval_without_exact_recovery",
    "rule": "A single quarantined STALE_STATUS transition is not alertable. Only the same exact identity still stale after one full watcher poll interval is eligible for manager_failure classification; recovered, unrelated, and other sustained defects retain their ordinary evidence rules.",
}


def evaluator_instructions() -> str:
    return (
        "Return only one strict verdict JSON. Report only cited orchestration defects; exclude ordinary long work, hardware/provider waits, single retries, and firmware/server/test failures.\nPhase/ownership rules:\n"
        + "\n".join(
            f"- [{rule['disposition']}] {rule['rule']}"
            for rule in OWNERSHIP_PHASE_RULES
        )
        + "\nStale-status rule:\n- ["
        + STALE_STATUS_QUARANTINE_RULE["disposition"]
        + "] "
        + STALE_STATUS_QUARANTINE_RULE["rule"]
    )


@dataclass(frozen=True)
class Verdict:
    defect: bool
    kind: str | None
    severity: str
    summary: str
    implicated: tuple[str, ...]
    evidence: tuple[dict[str, Any], ...]


def validate_verdict(raw: Any, *, packet: dict[str, Any] | None = None) -> Verdict:
    if not isinstance(raw, dict) or set(raw) - {
        "defect",
        "kind",
        "severity",
        "summary",
        "implicated",
        "evidence",
    }:
        raise ValueError("unsupported verdict fields")
    defect = raw.get("defect")
    kind = raw.get("kind")
    severity = raw.get("severity")
    summary = raw.get("summary")
    implicated = raw.get("implicated", [])
    evidence = raw.get("evidence", [])
    if (
        not isinstance(defect, bool)
        or severity not in {"warning", "error", "critical"}
        or not isinstance(summary, str)
        or len(summary) > 500
    ):
        raise ValueError("invalid verdict")
    if defect and (kind not in ALLOWED or not evidence):
        raise ValueError("defect requires supported kind and evidence")
    if not defect and kind is not None:
        raise ValueError("healthy verdict cannot have kind")
    if not isinstance(implicated, list) or not all(
        isinstance(x, str) and x for x in implicated
    ):
        raise ValueError("invalid implicated")
    if not isinstance(evidence, list) or not all(
        isinstance(x, dict)
        and isinstance(x.get("path"), str)
        and isinstance(x.get("sha256"), str)
        and isinstance(x.get("offset"), int)
        and len(x["sha256"]) == 64
        and all(ch in "0123456789abcdef" for ch in x["sha256"].lower())
        for x in evidence
    ):
        raise ValueError("invalid evidence")
    if packet is not None:
        observed = {
            (item.get("path"), item.get("sha256"), item.get("offset"))
            for item in packet.get("observations", [])
            if isinstance(item, dict)
        }
        if any(
            (item["path"], item["sha256"], item["offset"]) not in observed
            for item in evidence
        ):
            raise ValueError("evidence is not present in review packet")
    return Verdict(defect, kind, severity, summary, tuple(implicated), tuple(evidence))


class Evaluator(Protocol):
    def evaluate(self, packet: dict[str, Any]) -> Verdict: ...


class FakeEvaluator:
    def __init__(self, verdict: dict[str, Any] | None = None):
        self.verdict = verdict or {
            "defect": False,
            "kind": None,
            "severity": "warning",
            "summary": "healthy",
            "implicated": [],
            "evidence": [],
        }

    def evaluate(self, packet: dict[str, Any]) -> Verdict:
        return validate_verdict(self.verdict, packet=packet)


class TerraHighEvaluator:
    """Launcher contract: configured command receives packet JSON on stdin and emits one verdict JSON."""

    model = "gpt-5.6-terra"
    reasoning = "high"

    def __init__(self, command: tuple[str, ...]):
        self.command = command

    def evaluate(self, packet: dict[str, Any]) -> Verdict:
        if not self.command:
            raise RuntimeError("Terra-high evaluator command is required")
        prompt = {
            "role": "GPT-5.6-terra-high harness watcher",
            "instructions": evaluator_instructions(),
            "packet": packet,
        }
        result = subprocess.run(
            self.command,
            input=json.dumps(prompt),
            text=True,
            capture_output=True,
            timeout=120,
            check=False,
        )
        if result.returncode:
            raise RuntimeError("Terra-high evaluator failed")
        return validate_verdict(json.loads(result.stdout), packet=packet)
