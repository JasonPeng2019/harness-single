"""Claim-to-test map for DEL-002 feature scenarios, independent of product code."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScenarioFamily:
    name: str
    requirements: tuple[str, ...]
    tests: tuple[str, ...]
    evidence: str


SCENARIO_FAMILIES: tuple[ScenarioFamily, ...] = (
    ScenarioFamily("outbox-and-queue", ("REQ-001", "REQ-002"), ("test_addendum_observation_oracles.AddendumObservationOracleTests.test_outbox_retry_rejects_archive_before_admission", "test_addendum_observation_oracles.AddendumObservationOracleTests.test_watch_and_review_reject_state_mutation_or_inferred_acceptance"), "outbox/queue snapshots and top-level event history"),
    ScenarioFamily("review-and-hooks", ("REQ-003", "REQ-004", "REQ-005"), ("test_addendum_observation_oracles.AddendumObservationOracleTests.test_watch_and_review_reject_state_mutation_or_inferred_acceptance", "test_addendum_observation_oracles.AddendumObservationOracleTests.test_hook_heartbeat_and_recovery_authority_reject_mutants"), "review pair, durable receipt and content-free hook notice"),
    ScenarioFamily("monitor", ("REQ-006", "REQ-007"), ("test_addendum_observation_oracles.AddendumObservationOracleTests.test_hook_heartbeat_and_recovery_authority_reject_mutants",), "atomic heartbeat and ROOT-directed recovery notice"),
    ScenarioFamily("result-correction", ("REQ-008", "REQ-009"), ("test_addendum_observation_oracles.AddendumObservationOracleTests.test_correction_and_lease_audit_reject_limit_and_live_holder_mutants",), "six-attempt ledger, native session argv and cleanup identity"),
    ScenarioFamily("orphan-and-audit", ("REQ-010", "REQ-011"), ("test_addendum_observation_oracles.AddendumObservationOracleTests.test_correction_and_lease_audit_reject_limit_and_live_holder_mutants",), "global lease scan and locked STARTED/terminal audit"),
    ScenarioFamily("portable-lifecycle", ("REQ-012", "REQ-013", "REQ-014", "REQ-015"), ("test_check_u1.CheckU1Tests", "test_check_u3.CheckU3Tests", "test_check_u4.CheckU4Tests", "test_check_u5.CheckU5Tests"), "portable records, paths, process identity and CLI contract"),
    ScenarioFamily("asset-and-live-controls", ("REQ-016", "REQ-017"), ("test_claim_map.ClaimMapTests", "test_live_matrix_contract.LiveMatrixContractTests"), "claim map, exhaustive matrix, native gaps and fake-only boundary"),
)


def requirement_to_tests() -> dict[str, tuple[str, ...]]:
    mapping: dict[str, list[str]] = {}
    for family in SCENARIO_FAMILIES:
        for requirement in family.requirements:
            mapping.setdefault(requirement, []).extend(family.tests)
    return {requirement: tuple(tests) for requirement, tests in mapping.items()}
