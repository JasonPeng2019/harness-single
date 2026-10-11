# Resolved: cancellation of resumed, never-launched invocations

Status: **Resolved in version-b.** Implementation commit:
[`b095b40`](https://github.com/JasonPeng2019/harness-single/commit/b095b4012c8a6a3f6535bd109f68e519a04ee346)
(`fix: cancel resumed lanes before launch`).
Reported and validated locally: 2026-10-10.

This document records the implemented behavior, changed files and validation of
that commit in the portable harness. It is a resolution record for this issue.

## Current behavior

`resume-lane` prepares a fresh invocation in the existing worktree and saved
provider session. It starts no controller or provider. The prepared resume has
`lifecycle="running"`, the literal `launch_pending=true`, `process={}`, and no
current controller-status file. The lifecycle label describes bookkeeping;
process evidence establishes whether an invocation started.

Both `harness shutdown` and `lane force-stop` recognize this never-launched
invocation and retire it cleanly. They also recognize initial `prepared` lanes
with exactly empty process records and genuinely absent controller status.
A missing status file alone does not establish cleanup of a started invocation.

The original inconsistency was in force-stop: it accepted missing status only
for `prepared` lanes and reported `FORCE_STOP_PROCESS_SURVIVED` for a resumed
pending lane without observing a surviving process. Commit `b095b40` resolves
that inconsistency and retains the earlier shutdown recognition of pending
resumes from `1bcc08e`.

| Invocation evidence | Implemented handling |
| --- | --- |
| Initial `prepared` lane, `process={}`, status genuinely absent | Shutdown and force-stop accept never-launched cleanup. |
| Resumed `running` lane, `launch_pending is True`, `process={}`, status genuinely absent | Shutdown and force-stop accept never-launched cleanup. |
| Failed spawn with a matching `launch_failure.run_id` and `controller_not_created is True`, `process={}`, status absent | Separate durable proof records that no controller was created; cleanup does not depend on restoring the pending flag. |
| Started or unattested running invocation with missing status; unknown process metadata; invalid or unreadable status | Cleanup remains unproven. |
| Changed lane/run or epoch at cancellation's effect boundary | Cancellation refuses stale ownership before releasing leases or retiring a newer invocation. |

An existing malformed record, mismatched lane/run, directory or non-regular
status path is not absence. Exact controller identities in status remain
relevant even when the lane's own process record is empty.

## Implemented changes

| File | Change |
| --- | --- |
| [lane_cleanup.py](orchestrator_harness/lane_cleanup.py) | Adds shared `observe_controller_status`, `never_launched`, `controller_not_created` and `cleanup_is_proven` helpers. Status observations distinguish valid, absent, invalid and unreadable evidence. |
| [launch.py](orchestrator_harness/launch.py) | Aligns force-stop with the shared never-launched proof, preserves failed-start evidence, validates ownership before effects, and distinguishes uncertain cleanup from an observed surviving identity. Launch checks prior cleanup and consumes pending state before process creation. |
| [shutdown.py](orchestrator_harness/shutdown.py) | Uses shared cleanup proof, refuses unreadable lane records, and revalidates invocation ownership and cleanup before lease release and retirement. Shutdown requests strict lease deletion error reporting. |
| [resume.py](orchestrator_harness/resume.py) | Serializes preparation against launch/cancellation, checks runtime and epoch before ownership changes, clears prior failed-start bookkeeping for a fresh run, and refuses to discard an unknown started invocation. |
| [lanes.py](orchestrator_harness/lanes.py) | Adds the native lane-operation lock and `require_same_run`, which checks lane ID, run ID, worktree path and provider identity. |
| [leases.py](orchestrator_harness/leases.py) | Makes force-stop lease release match both lane ID and run ID. Adds strict deletion error reporting for shutdown; deletion failure prevents retirement. |
| [release_checks.py](orchestrator_harness/release_checks.py) | Includes `lane_cleanup.py` in the public-route dependency fingerprint. |
| [Command reference](orchestrator_harness/README.md) | Documents pending-invocation cancellation, exact-owner lease release and cleanup diagnostics. |

Launch persists `launch_pending=False` before spawning a controller. A native
spawn failure records run-bound proof that no controller was created. When
identity registration fails after creation, recovery records the captured exact
controller identity and whether its handle was reaped. Reaping a controller
alone does not establish provider/helper cleanup. A child without a provable
creation identity is not targeted by numeric PID.

Native lane-operation locks serialize launch, resume and cancellation effects
without blocking the controller's ordinary lane-record writes. Shutdown and
force-stop revalidate ownership under the lane-record lock before releasing the
captured run's leases and recording retirement. Stale cancellation preserves a
newer run's state and leases.

Force-stop now reports the reason supported by its evidence:

- `FORCE_STOP_CLEANUP_UNPROVEN`: controller or provider cleanup lacks proof.
- `FORCE_STOP_PROCESS_SURVIVED`: an exact recorded identity remains live after
  the cleanup attempt.
- `FORCE_STOP_OWNERSHIP_CHANGED`: lane/run or epoch ownership changed before
  cancellation effects.
- `FORCE_STOP_LEASE_RELEASE_FAILED`: exact-owner lease deletion failed.

The native ROOT Stop policy still rejects `SHUTTING_DOWN` and permits `CLOSED`.
Successful shutdown completes cleanup, closes the epoch/runtime and stops the
monitor; the resolution does not depend on weakening the Stop policy.

## Recorded validation

The new [pending cancellation tests](orchestrator_harness/tests/test_pending_cancellation.py)
cover public-command closure, pending cancellation, invalid/unknown evidence,
exact-owner lease release, stale ownership, failed starts and duplicate-launch
refusal. Existing [launch lifecycle](orchestrator_harness/tests/test_launch_lifecycle.py),
[shutdown pending-launch](orchestrator_harness/tests/test_shutdown_pending_launch.py),
[resume ownership](orchestrator_harness/tests/test_resume_identity_guard.py) and
[exact controller-exit](orchestrator_harness/tests/test_cleanup_exit_races.py)
regressions accompany those changes. Existing launch fixtures now preserve
successive lane mutations and account for the pre-spawn pending-state write.

| Validation | Recorded result |
| --- | --- |
| Top-level portable harness test modules | 379 tests run; 375 passed and four environment-gated skips. Includes the public-command fixture; excludes the separate `v2_acceptance` collection. |
| Focused lifecycle, cancellation, failed-start and ownership checks | 72 tests passed. |
| Strengthened registration/cleanup-failure regression | Passed for both initial and resumed lanes, with termination-failure and reap-failure subcases. |
| Public launcher and monitor fixture | All three sequences below passed. |
| Syntax parsing, Git whitespace checks and release selector loading | Passed. |

The public fixture exercised these native command sequences in fresh disposable
managed runtimes:

1. `resume-lane`, then `harness shutdown`, with `lane launch` omitted.
2. `resume-lane`, then `lane force-stop`, then `harness shutdown`, with launch omitted.
3. Initial `lane bootstrap`, then `harness shutdown`, with launch omitted.

Each sequence verified successful command exits, lane retirement, exact-owner
lease release, epoch closure, runtime `CLOSED`, monitor `STOPPED`, absence of the
exact monitor PID-plus-creation identity, and ROOT Stop allowance.

These tests used real public-launcher and monitor processes, disposable Git
worktrees and fixture session history. They launched no real provider and do not
constitute real-agent or hardware validation. Runtime output stayed under the
version-b `runtime/` tree.

The full recursive discovery run also exercised acceptance tests whose
project-specific matrix drivers are absent under `.agent-workspace/` in this
portable checkout. Eight of those tests failed to execute successfully (seven
failures and one error). That recorded limitation is separate from this resolved
lifecycle issue; the full recursive suite is not reported as passing.
