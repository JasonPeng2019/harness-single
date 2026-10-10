# Harness issue: resumed attempt never launched blocks shutdown and Stop hook

Reported: 2026-10-10. Source assessment refreshed: 2026-10-10.
Status: **OPEN — partly addressed in `harness/harness-single`; force-stop remains
inconsistent, the campaign copy is unchanged, and full native closure is unverified.**

## Summary

`resume-lane` prepares a fresh invocation without starting its controller or
provider. It records `lifecycle="running"`, `launch_pending=true`, and `process={}`,
and removes the previous invocation's current controller status. In the campaign
copy, shutdown requires that missing status, while force-stop recognizes an
unstarted invocation only when its lifecycle is `prepared`.

Consequently, pausing between resume and launch leaves the runtime
`SHUTTING_DOWN`. The native ROOT Stop hook rejects that state and repeatedly
prevents the manager's response from ending, even with no unresolved queue events
and the campaign goal explicitly paused.

The concrete defect is inconsistent handling of a never-launched invocation.
The `running` label can be intentional bookkeeping if every consumer respects
`launch_pending`; the label alone is not proof that a worker process started.

## Observed reproduction and evidence

Observed in `test/harness`, on Windows, using the public native launcher:

1. Resume a stopped, unaccepted lane with a valid amendment task card.
2. Pause before issuing `lane launch` for its fresh invocation.
3. Run `harness shutdown`.
4. Follow its recommendation to run `lane force-stop` for the named lane.
5. Try to end the ROOT response.

The exact affected identity was:

- Epoch: `dfd73f42772f4c0d838489f1e4a7f9f0`.
- Lane: `hil-execution-11-t01-discovery`.
- Fresh invocation: `2dfeb7a0ef5d4591a42e114134724d3e`.
- State: `lifecycle="running"`, `launch_pending=true`, `process={}`.
- Current controller status: absent; fresh invocation never launched.
- Manager queue: zero unresolved obligations before shutdown.

Commands were run from `test/harness` with the campaign's recorded operator
environment. Shutdown exited 1:

```text
SHUTDOWN_LANE_CLEANUP_UNPROVEN: lane hil-execution-11-t01-discovery processes could not be proven gone; force-stop the lane
```

The prescribed exact force-stop also exited 1:

```text
FORCE_STOP_PROCESS_SURVIVED: a lane process could not be terminated even forcibly
```

That second message results from a failed state predicate in this case; it does
not establish an observed surviving process. The runtime stayed `SHUTTING_DOWN`,
the epoch remained active, and the monitor had not stopped. The four latest
DeepSeek provider invocations had already exited at the Ollama quota limit; quota
exhaustion is separate from this lifecycle defect.

The repeated hook message was:

```text
ROOT hook cannot prove that the runtime is OPEN
```

Primary receipts in the parent workspace, outside this harness repository:

- [Command exit codes and exact messages](../../test/evidence/pause-20261010/PAUSE-COMMANDS.json).
- [Runtime, monitor and lane snapshot](../../test/evidence/pause-20261010/RUNTIME-SNAPSHOT.json).
- [Pause hook analysis](../../test/evidence/pause-20261010/STOP-HOOK-BLOCKER.md).
- [Provider exits and queue disposition](../../test/evidence/resume-20261010/OLLAMA-QUOTA-BLOCKER-1843.json).
- [Paused campaign handoff](../../test/HANDOFF.md).

## Relevant code and confirmed cause

Line numbers describe the files inspected on 2026-10-10.

1. **Preparation:** `resume.py`, `_clear_prior_run` around line 132, removes
   `controller.status.json`. `run_resume` / `replace_run` around lines 331–347
   writes the fresh invocation as running, pending launch, with no process.
   Its success message explicitly says to launch it to start the provider.
2. **Campaign shutdown:** `test/harness/orchestrator_harness/shutdown.py`,
   `_lane_clean` around lines 119–130, returns false when controller status is
   absent. It does not recognize never-launched attempts.
3. **Force-stop:** `_terminate_lane_processes` returns
   `lane.get("lifecycle") == "prepared"` when both controller PID and status are
   absent. This is at campaign `launch.py:377–378` and top-level
   `harness-single/orchestrator_harness/launch.py:389–390`. A resumed pending
   invocation therefore fails this check.
4. **Repeated response blocking:** `root_hook_dispatch.py:184–191` permits
   `CLOSED`, but rejects `SHUTTING_DOWN` at the Stop boundary. It never reaches
   the zero-unresolved-obligations check in that state.

## Current implementation status

The top-level checkout is at `1bcc08e1d14b43a1d3b87df4a7b3b3af1102736e`
(`fix: distinguish pending launches during harness shutdown`). Its relevant
launch, shutdown and regression-test files match that commit. The findings below
describe implemented source behavior; they are not a passing native retest.

| Part of the issue | Current top-level status | Source evidence |
| --- | --- | --- |
| Shutdown recognizes a resumed, never-launched invocation | Implemented in `1bcc08e` | [shutdown.py](orchestrator_harness/shutdown.py), `_lane_clean`, lines 119–138. Accepts `prepared`, or `running` with `launch_pending is True`, only with `process == {}` and an absent status path. |
| A launch attempt cannot retain the never-launched flag after process creation | Implemented in `1bcc08e` | [launch.py](orchestrator_harness/launch.py), `run_launch`, lines 254–269. Persists `running` / `launch_pending=False` before spawning the controller. |
| Force-stop uses the same never-launched proof | Not addressed | `launch.py`, `_terminate_lane_processes`, lines 383–390, still accepts only lifecycle `prepared` when controller PID/status are absent. A resumed `running` / pending invocation is rejected. |
| Cleanup errors distinguish uncertain state from an observed surviving process | Not addressed | `launch.py`, `run_force_stop`, lines 484–491, maps a false cleanup result to `FORCE_STOP_PROCESS_SURVIVED` and an unkillable-process message, including the state mismatch above. |
| Regression coverage for the implemented changes | Test code present; not executed in this assessment | [test_shutdown_pending_launch.py](orchestrator_harness/tests/test_shutdown_pending_launch.py) covers the shutdown predicate and negative cases; [test_launch_lifecycle.py](orchestrator_harness/tests/test_launch_lifecycle.py) covers the before-spawn transition and failed registration/cleanup. These do not establish full public-command closure. |
| The hardware campaign receives the fix | Not applied | `test/harness/orchestrator_harness/shutdown.py:127–128` still returns false for absent status. |
| Shutdown retires the invocation, releases its leases, closes the epoch/runtime, stops the monitor and permits ROOT Stop | Unverified for this reproduction in the top-level copy | Requires the native retest below; source inspection and isolated predicate tests are insufficient. |

The refreshed byte comparison still finds `resume.py` and
`root_hook_dispatch.py` identical across the two copies, with `launch.py` and
`shutdown.py` different. The recorded reproduction concerns the older campaign
copy. Do not overwrite the top-level fixes with that source.

The shutdown predicate already rejects an existing malformed status file or
status directory, a nonempty/unknown process record, and a running invocation
without the pending-launch attestation. Preserve these checks when aligning
force-stop. The Stop hook already permits `CLOSED`; weakening its rejection of
`SHUTTING_DOWN` would hide incomplete cleanup rather than finish this fix.

## What could be improved in the existing fix

These are source-based recommendations, not additional reproduced native
failures. No implementation changes are made by this report.

1. **Give never-launched proof one definition.** Use the same strict check in
   shutdown and force-stop. Simply adding `running` to force-stop's allowed
   labels would accept invocations that started. Keep the explicit pending flag,
   exactly empty process record and genuinely absent status requirement. Expose
   absent, invalid and unreadable status as distinct observations instead of
   making callers interpret the same `None` result.
2. **Keep the before-spawn transition, but improve failed-start recovery.** The
   early `launch_pending=False` write prevents unsafe cleanup claims. However,
   `run_launch` can subsequently fail before recording controller identity; its
   termination/wait paths do not themselves persist a terminal cleanup outcome.
   Review recoverability when no controller was created, or when a captured
   controller was demonstrably terminated and reaped. Persist only the outcome
   actually proven for that invocation. Do not restore the pending flag or claim
   absence merely because identity registration or cleanup raised an error.
3. **Report the actual cleanup reason.** A state mismatch, unreadable record or
   missing proof should describe cleanup as unproven. Reserve surviving-process
   wording for an observed live identity that could not be cleaned up. The
   current boolean result and force-stop error collapse these different cases.
4. **Check ownership across the cancellation sequence.** Revalidate the exact
   epoch/lane/run before spawning, retiring or releasing resources when launch,
   cancellation and resume can overlap. `update_lane` locks individual writes,
   but the inspected launch/force-stop callbacks do not check the captured
   `run_id`; `force_release_leases` selects by lane ID alone. These are concrete
   review points for the race controls, not proof that a race occurred. A stale
   cancellation must not retire or release leases belonging to a newer run.
5. **Verify the complete command sequence.** Add the resumed-pending case to
   force-stop coverage and verify both public shutdown and force-stop paths in
   a disposable runtime. Assert retirement, exact-owner lease release, monitor
   exit, runtime closure and Stop-hook allowance, including the negative and
   ownership-change cases below. Run this validation on GitHub Actions only.

## Expected behavior and proposed remaining work

A verified never-launched attempt should be cancellable and retire cleanly during
shutdown. The runtime should then reach `CLOSED`, its monitor should stop, and
the native ROOT Stop hook should allow the response to end.

- Finish force-stop alignment and cleanup diagnostics while retaining the
  implemented shutdown predicate and before-spawn transition. Missing status or
  a lifecycle label alone must not count as proof of cleanup.
- Resolve and test the failed-start and exact-ownership review points above,
  preserving process identity and lease ownership during cancellation races.
- Obtain passing native results for the complete retest procedure before closing
  this issue. Existing test source and commit presence are not passing evidence.
- Once corrected and verified, explicitly update the campaign's harness copy
  under separate authorization; its paused runtime still has the old code.

Changing the label to `prepared` is an alternative design choice, but changing
only that label does not fix the campaign shutdown function, which rejects any
absent status. A consistent proof of never-launched state is required.

Temporarily disabling the manager's native Stop hook would only stop the repeated
prompts. It would not complete process/resource cleanup or fix the lifecycle issue.

## Retest procedure

Run suites, native process fixtures and this public-launcher retest on GitHub
Actions ONLY. Use a disposable managed runtime with a stopped, unaccepted lane
that has valid provider/session history. Do not retarget or restart the paused
hardware campaign. This document update does not authorize campaign deployment.

```text
python -m orchestrator_harness.operator_launch resume-lane --lane-id <lane> --resume-task-card <valid-card.json>
# Deliberately omit lane launch.
python -m orchestrator_harness.operator_launch harness shutdown
```

Verify successful exit, retired lane, exact-owner lease release, closed epoch,
runtime `CLOSED`, stopped monitor, and ROOT Stop-hook allowance. Separately verify
exact force-stop on another resumed, never-launched invocation. Cover initial
prepared lanes as well.

Negative cases must continue to refuse fabricated cleanup: launch already entered
with `launch_pending=false`; recorded/live controller identity; nonempty or unknown
process metadata; an existing malformed/unreadable status file or status directory;
and ownership changes during cancellation. Cover failed starts before controller
identity registration, with both proven cleanup and genuinely unknown outcomes.
Keep the current test that an unattested running invocation with missing status
fails closed; add the distinct, explicitly pending resumed invocation that should
cancel successfully. Verify that a newer run retains its state and leases when a
stale cancellation reaches its effect boundary.

## Work performed for this report

This refresh inspected current source, tests and Git metadata, compared the two
source copies, and edited this Markdown only. Linked historical run receipts were
not reloaded. No harness or firmware implementation changes, hook changes,
runtime-record repairs, process kills, builds, tests, commits, pushes or parent-run
operations were performed. The campaign goal remains paused; this update supplies
no new proof of its runtime cleanup. The issue remains open and partly addressed.
