# Resumed, never-launched attempt blocks shutdown and ROOT Stop

Reported: 2026-10-10.
Status: **FIXED in version-b**, with local regression and native-command fixture validation.

## Defect

`resume-lane` prepares a fresh invocation without starting its controller or
provider. It records `lifecycle="running"`, `launch_pending=true`, and
`process={}`, and removes the previous invocation's current controller status.
The `running` label is bookkeeping, not proof that a process started.

Shutdown already recognized this explicitly pending invocation. Force-stop
still treated missing controller status as clean only for `prepared` lanes.
Consequently, force-stop rejected a resumed, never-launched attempt and reported
`FORCE_STOP_PROCESS_SURVIVED` without evidence of a surviving process.
An implementation that also rejects this state during shutdown leaves the
runtime `SHUTTING_DOWN`, which the native ROOT Stop hook correctly rejects.

This issue concerns the portable version-b harness. Project-specific campaign
paths, process identities, external receipts, deployment instructions and the
GitHub Actions-only restriction from the original report have been removed.
They do not define this repository's implementation or validation requirements.

## Correction

- `orchestrator_harness/lane_cleanup.py` provides one never-launched proof for
  shutdown and force-stop: an initial `prepared` lane, or `running` with the
  literal `launch_pending=true`, an exactly empty process record, and a genuinely
  absent controller-status path. Status observations distinguish absent, invalid
  and unreadable records. A directory, malformed record or mismatched invocation
  never counts as absence.
- Launch consumes the pending state before creating any process. Native spawn
  failure records separate, run-bound proof that no controller was created.
  Failed identity registration preserves exact controller identity and whether
  its handle was reaped. Resume refuses to discard an unknown failed start.
  Reaping alone does not prove provider/helper cleanup;
  missing or uncertain evidence remains unproven. The pending flag is never
  restored to disguise a failed start. A prepared label also cannot authorize
  launch while controller cleanup remains unproven.
- Force-stop reports `FORCE_STOP_CLEANUP_UNPROVEN` for missing or uncertain proof.
  `FORCE_STOP_PROCESS_SURVIVED` requires an exact recorded identity still observed
  live after the cleanup attempt.
- Native lane-operation locks serialize launch, resume and cancellation effects.
  Lane-record mutations revalidate the captured invocation; cancellation checks
  its epoch and releases leases only for the exact lane/run owner. A stale
  cancellation cannot retire a newer run or release its leases.
- The ROOT Stop hook continues to reject `SHUTTING_DOWN` and allow `CLOSED`.
  Cleanup completes through the native harness; hook policy is unchanged.

## Reproduction and expected result

Use a disposable managed runtime and a stopped, unaccepted lane with saved
provider/session history and a valid amendment task card:

```text
python -m orchestrator_harness.operator_launch resume-lane --lane-id <lane> --resume-task-card <card.json>
# Deliberately omit lane launch.
python -m orchestrator_harness.operator_launch harness shutdown
```

Expect exit 0, retired lane, exact-owner lease release, closed epoch, runtime
`CLOSED`, exact monitor-process exit and ROOT Stop allowance. Separately verify
`lane force-stop --lane-id <lane>` for another resumed, never-launched attempt,
then shut down that runtime. Initial prepared lanes must also retire cleanly.

Negative cases must refuse fabricated cleanup: pending false or unattested;
recorded/live or unknown controller identity; nonempty process metadata;
malformed, unreadable, mismatched or non-file status; ownership changes; and
failed starts whose controller/provider cleanup remains unknown.

## Validation

Regression tests are in `test_shutdown_pending_launch.py`,
`test_launch_lifecycle.py`, `test_pending_cancellation.py` and the existing exact
process-exit and resume-ownership tests under `orchestrator_harness/tests/`.
The public-command test uses real launcher and monitor processes with disposable
Git worktrees and fixture session history. It launches no real provider and
claims no real-agent or hardware validation.

Run locally from version-b with temporary files beneath `runtime/`:

```text
python -B -m unittest orchestrator_harness.tests.test_pending_cancellation
python -B -m unittest discover -s orchestrator_harness/tests -t .
```

Local results (2026-10-10):

- Focused lifecycle, shutdown, force-stop, failed-start and ownership checks:
  **72 tests passed**.
- Public launcher/monitor fixture: **all three sequences passed** (resumed
  pending shutdown, resumed pending force-stop followed by shutdown, initial
  prepared shutdown). Each verified lane retirement, exact-owner lease release,
  epoch closure, runtime `CLOSED`, exact monitor exit and ROOT Stop allowance.
- Top-level portable harness suite: **379 tests ran successfully, with four
  environment-gated skips**. This includes the public-command fixture and
  excludes the separate `v2_acceptance` fixture collection.
- Syntax parsing and `git diff --check` passed. The stable release selector
  loaded successfully and includes the shared cleanup module in public-route
  fingerprints.

Full recursive discovery also includes acceptance tests requiring absent
project-specific matrix drivers under `.agent-workspace/`. Eight of those tests
cannot pass in this portable snapshot (seven failures and one error). Those
missing fixtures are separate from this lifecycle fix; they were not recreated
or used as evidence of cleanup.
