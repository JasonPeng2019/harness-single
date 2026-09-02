"""Claim inventory and small, implementation-independent record oracles."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class Claim:
    name: str
    requirements: tuple[str, ...]
    scenario: str
    trigger: str
    expected: str
    oracle: str
    cleanup: str
    evidence_class: str


CLAIMS: tuple[Claim, ...] = (
    Claim("CHECK-U1", ("REQ-001", "REQ-004", "REQ-005", "REQ-006"), "Layout, configuration, idempotent setup, and epoch transition.", "Create a fresh disposable workspace, then setup and open an epoch.", "Only the derived runtime is mutable; setup is idempotent and immutable configuration requires a new epoch.", "Inspect exact runtime records and source-tree digest before/after; reject an epoch/config mismatch.", "Remove the disposable workspace after shutdown.", "synthetic/static"),
    Claim("CHECK-U2", ("REQ-002", "REQ-003", "REQ-008", "REQ-009"), "Managed/plain profiles, cache/adapters, monitor, and queue roles.", "Bootstrap both profiles and promote a terminal managed status twice.", "Managed receives one queue event and coordination material; plain has neither; the monitor deduplicates without role cross-talk.", "Assert queue header, event state/history, profile-specific paths, and exactly one promotion for an unchanged status.", "Dispose queues and temporary worktrees; no live hook claim is made.", "synthetic/static"),
    Claim("CHECK-U3", ("REQ-007", "REQ-010", "REQ-011", "REQ-012"), "Lane lifecycle, leases, review, resume, force-stop, retirement, and shutdown.", "Launch contending lanes, complete one result, review it, then resume/retire paths.", "Acquisition is all-or-nothing and fail-fast; acceptance is a linked pair; leases leave only after cleanup proof or explicit force stop.", "Inspect lane/run identities, pair hashes, lease ownership, and cleanup proof; reject stale results and accepted resume.", "Release only exact fixture identities and delete the disposable repository.", "synthetic"),
    Claim("CHECK-U4", ("REQ-013", "REQ-014"), "All record schemas, atomic replacement, links, remediation, and public CLI vocabulary.", "Write valid records then introduce malformed, stale, one-sided, and wrong-hash variants.", "Bad records are surfaced rather than repaired into success; only the documented state transitions and CLI outcome vocabularies are valid.", "Canonical SHA-256, strict schema/identity validation, pair-integrity checks, and prior-file preservation after a rejected replacement.", "Remove all temporary sibling files and fixture records.", "synthetic/static"),
    Claim("CHECK-U5", ("REQ-015", "REQ-016"), "Portable paths/process identities, generic adapter surface, packaging, and disposable flow.", "Use a temporary root with spaces and a fake provider/process identity.", "Paths are joined rather than platform-shaped, process identity includes creation time, and no fake result is labeled live proof.", "Validate portable Path records, PID-plus-creation comparison, package import, and fixture evidence classification.", "TemporaryDirectory cleanup and exact-identity verification.", "synthetic/static"),
    Claim("CHECK-LIVE-1", ("REQ-017",), "Native Stop-hook acceptance/rejection for ROOT and worker.", "M09 invokes each authorized shipped CLI in a disposable repository.", "Unresolved work and missing/invalid results reject; valid PASS/FAIL/BLOCKED terminal results pass.", "Native CLI exit/hook evidence, not a fake hook or transcript inference.", "Target-specific cooperative shutdown and exact process-identity confirmation.", "live-only"),
    Claim("CHECK-LIVE-2", ("REQ-017",), "Native role queue isolation.", "M09 leaves one role's queue pending while exercising the other role.", "One role's queue cannot block or advance the other role's work.", "Read both durable queue records plus native hook outcome and run IDs.", "Acknowledge/close only the created events, then remove the disposable runtime.", "live-only"),
    Claim("CHECK-LIVE-3", ("REQ-017",), "Native monitor liveness recovery.", "M09 makes the recorded monitor dead, hung, and deliberately stopped in separate attempts.", "Dead/hung is restarted under the monitor lock; deliberately stopped/dead is not resurrected.", "Compare PID plus creation time and heartbeat/stop state before and after a real ROOT tool boundary.", "Stop only the exact recorded monitor and prove it absent before disposal.", "live-only"),
    Claim("CHECK-LIVE-4", ("REQ-017",), "Native serial exclusive-lease reuse.", "M09 runs two authorized lanes requesting the same resource in sequence.", "The second launch is fail-fast while held, then succeeds only after exact cleanup proof releases the first lease.", "Inspect both lease holder identities, launch result, cleanup proof, and final empty lease directory.", "Gracefully retire each lane or force-stop only the exact recorded identity.", "live-only"),
)


def claim(name: str) -> Claim:
    return next(item for item in CLAIMS if item.name == name)


def canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(value: Mapping[str, Any]) -> str:
    clean = dict(value)
    clean.pop("content_hash", None)
    return hashlib.sha256(canonical_json(clean).encode("utf-8")).hexdigest()


def assert_valid_result(value: Mapping[str, Any], lane_id: str, run_id: str) -> None:
    required = {"schema", "lane_id", "run_id", "outcome", "summary", "evidence", "content_hash", "completed_at"}
    missing = required - set(value)
    if missing or value.get("schema") != "result/v1":
        raise AssertionError(f"invalid result schema/fields: missing={sorted(missing)}")
    if value.get("lane_id") != lane_id or value.get("run_id") != run_id:
        raise AssertionError("result identity is stale or belongs to another lane")
    if value.get("outcome") not in {"PASS", "FAIL", "BLOCKED"}:
        raise AssertionError("result outcome is outside the normative enum")
    if not isinstance(value.get("evidence"), list) or value.get("content_hash") != content_hash(value):
        raise AssertionError("result evidence or canonical content hash is invalid")


def assert_review_pair(review: Mapping[str, Any], acceptance: Mapping[str, Any]) -> None:
    if review.get("schema") != "completion-review/v1" or acceptance.get("schema") != "orchestrator-acceptance/v1":
        raise AssertionError("review/acceptance schemas are invalid")
    for field in ("lane_id", "run_id", "task_card_hash", "result_hash", "commit"):
        if review.get(field) != acceptance.get(field):
            raise AssertionError(f"review pair link differs at {field}")
    if acceptance.get("review_ref") != content_hash(review):
        raise AssertionError("acceptance does not link the factual review hash")
    forced = acceptance.get("force_accept_reason")
    if acceptance.get("approval") == "ACCEPTED" and review.get("review_outcome") != "PASS" and not forced:
        raise AssertionError("non-PASS acceptance lacks explicit force reason")


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    """A fixture oracle's same-directory replacement primitive, never product code."""
    temporary = path.with_name(f".{path.name}.acceptance-tmp")
    try:
        temporary.write_text(canonical_json(value), encoding="utf-8")
        json.loads(temporary.read_text(encoding="utf-8"))
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def covered_requirements(items: Iterable[Claim] = CLAIMS) -> set[str]:
    return {requirement for item in items for requirement in item.requirements}
