"""DEL-002's machine-readable 82-gap-to-oracle map.

These are *acceptance* contracts, not a mirror of candidate implementation.  A
row names the observable scenario and the test which must be run against a
candidate.  The source-only map lets review catch a missing audited gap before
a product run is mistaken for coverage.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GapOracle:
    gap_id: str
    check: str
    test: str
    scenario: str
    trigger: str
    expected: str
    cleanup: str
    invariant: str


_GROUPS = (
    ("CHECK-U1", "test_check_u1.CheckU1Tests", "Fresh runtime setup, epoch admission and Git isolation", "Create two configured lanes and attempt an epoch-breaking change", "one derived ignored runtime, one active epoch, unique lane IDs, and no source-tree mutation", "shutdown the disposable repository and verify its exact worktree cleanup", "provider-agnostic ownership and immutable epoch identity", "A1 A2 A3 A9 A10 C3 C5 C6 D2 D4 D5 D8 D10 D17 G21"),
    ("CHECK-U2", "test_check_u2.CheckU2Tests", "Managed/plain materialization, monitor promotion and worker ownership", "Bootstrap both profiles, promote twice, and consume an escalation", "only managed receives its own queue/payload; a monitor transaction yields one event and one outbox consumption", "remove queues, copied payloads and disposable worktrees", "controller/worker/monitor writer isolation", "A6 A8 B7 B8 B11 B14 C16 C17 E5 E7 E8 E12 F2 F3 F6 F12 F14a F14b H8 H9 H24 H25"),
    ("CHECK-U3", "test_check_u3.CheckU3Tests", "Concurrent lifecycle, lease, review, resume and retirement transitions", "Race epoch/lease contenders; reject, recover and resume a reviewed lane", "all-or-none fail-fast leases; valid review pair; BOUND-009/010 lifecycle; cleanup before release", "release only exact holder identities and delete the disposable repository", "run identity, pair integrity, and cleanup-proof-first", "A11 D13 D18 G4a G10 G11 G12 G12a G14a G14b G16 G22 H4 H5 H7 H14 H15 H18 H20 H21 H23 H30"),
    ("CHECK-U4", "test_check_u4.CheckU4Tests", "Crash-safe records and exact public command failures", "Inject a crash before replacement and submit malformed/stale records and CLI requests", "old-or-new complete record only; strict schemas and stable code/stream/action contract", "remove unique temporary siblings and fixture records", "atomic durable identity and no inferred repair", "A14 A14a A14b A15 A16 H1 H13 H19 H29 H32 I2a I3 I5 I6 I13 I14"),
    ("CHECK-U5", "test_check_u5.CheckU5Tests", "Portable fake-only rehearsal and honest evidence boundary", "Run a local fake process in a disposable root with spaces", "PID plus creation identity is required and fake evidence is never live proof", "prove exact child closure and remove all local fake state", "portable primitives and truthful claim scope", "I8a I12"),
    ("CHECK-LIVE-1", "test_live_matrix_contract.LiveMatrixContractTests.test_live_checks_remain_reserved_without_m09_authorization", "Native ROOT/worker Stop behavior for each shipped provider", "M09 authorized target attempts unresolved and valid terminal cases", "native evidence proves role-specific rejection and PASS/FAIL/BLOCKED acceptance", "target-specific cooperative shutdown by exact identity", "no fake hook is live evidence", "I9a I9b"),
    ("CHECK-LIVE-2", "test_live_matrix_contract.LiveMatrixContractTests.test_live_checks_remain_reserved_without_m09_authorization", "Native provider binding, discovery and queue isolation on new and resumed lanes", "M09 exercises Codex, Claude Code and Qwen Code with one opposite-role queue pending", "each native binding and durable receipt is run-scoped; the other queue cannot block it", "acknowledge/close created events and remove runtime", "provider role isolation", "I9"),
    ("CHECK-LIVE-3", "test_live_matrix_contract.LiveMatrixContractTests.test_live_checks_remain_reserved_without_m09_authorization", "Native monitor recovery", "M09 makes the recorded monitor dead, hung and intentionally stopped", "dead/hung restart; deliberate stop never restarts", "stop exact monitor identities and remove runtime", "PID-plus-creation liveness", "I10"),
    ("CHECK-LIVE-4", "test_live_matrix_contract.LiveMatrixContractTests.test_live_checks_remain_reserved_without_m09_authorization", "Native serial lease reuse", "M09 requests the same resource from two lanes", "second launch fails while held and succeeds only after first cleanup proof", "retire/stop exact lanes and prove empty lease directory", "cleanup proof precedes resource reuse", "I11"),
)


GAP_ORACLES: tuple[GapOracle, ...] = tuple(
    GapOracle(
        gap_id=gap,
        check=check,
        test=test,
        scenario=scenario,
        trigger=trigger,
        expected=expected,
        cleanup=cleanup,
        invariant=invariant,
    )
    for check, test, scenario, trigger, expected, cleanup, invariant, ids in _GROUPS
    for gap in ids.split()
)
