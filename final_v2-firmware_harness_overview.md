# Final v2 Firmware Harness Overview

## Delivery status

The firmware-enabled v2 harness is closed as a working WIP delivery at user direction on
2026-08-21. Its code baseline is commit
`5ab4b1f2f9170c3e57c35883bcb1ad22a2d04815` (tree
`17e67a7affd2d16e1b1104e1df3dec22bfa198eb`); this branch adds only this closure documentation.

“Closed” means the harness source is being published for use and no Firmware sprint remains
authoritative. It does **not** mean every historical hardware test passed, every external server
is defect-free, or that the abandoned three-clean-sprint campaign was completed. Suite 14 was
stopped because the project was closed; it is not acceptance evidence.

## What the harness does

The harness is a small execution boundary for a manager that already knows what work to do.
For ordinary coding lanes it provides:

- A versioned invocation/result contract, a lane controller that starts one provider worker, and
  durable status, event, checkpoint, and result records.
- Exact PID-plus-creation-time process ownership, bounded shutdown/reap evidence, and cleanup that
  releases named claims only after the owned process boundary is gone.
- Opaque exclusive named locks for real non-Git conflicts, waiting/recovery records, and safe
  resume of a lane with a replacement provider session.
- Git worktree/branch/base identity validation, current-tip result validation, and a thin public
  launcher for a manager that wants a detached controller.
- A process-local provider-adapter contract: a registered adapter owns its command construction,
  transcript decoding, terminal outcome, redaction, and any safe-boundary notification delivery.
- A diagnostic observer and optional watcher. They reconcile durable state and surface actionable
  facts; they do not execute a worker action or replace the manager's acknowledgement queue.
- A release-check registry that selects fast or affected checks from declared dependencies and
  reuses prior credit only when the command, inputs, source identity, and ancestor relationship
  still match.

## Firmware compatibility surface

Firmware support is deliberately an optional, policy-bound compatibility route rather than a
second scheduler.

- Existing schema-less Firmware invocations remain supported alongside ordinary coding
  invocations. They keep their own policy, authorization, lease, relay, and physical-cleanup
  rules.
- A caller can declare a `FirmwareAction`, a capability name, one canonical physical resource, an
  MCP tool/method contract, and a public policy binding. The harness validates that declaration
  and passes the declared arguments unchanged.
- `FirmwareHardwareAdapter` can use the caller-provided snapshot, launch, process-identity, and
  transport seams to perform the declared MCP initialization and tool call. Permit-expiry checks
  occur before launch, enqueue, and dispatch; cleanup retains the claim until both the owned
  process boundary and transport are closed.
- The firmware path shares process-boundary and cleanup machinery with coding lanes, while keeping
  board identity, firmware server behavior, and hardware authorization outside the generic core.

## What it does not do

The harness intentionally does not:

- Plan work, choose a test suite, schedule a dependency graph, merge code, decide acceptance, or
  promote a branch. Those remain manager and project decisions.
- Act as a task database, source-control system, file-ownership service, or generic hardware
  scheduler.
- Provide a firmware server, board profile, probe, fixture, firmware image, credentials, MCP
  implementation, or permission to touch physical hardware.
- Turn a missing/corrupt external result, unavailable dependency, host permission denial, or
  server defect into a successful result. It records and contains those facts; the responsible
  project decides how to recover.
- Guarantee that a manager follows the intended sprint-recovery policy. Historical suites exposed
  manager configuration/evidence mistakes even when the target harness itself had no new defect.

## Known evidence boundary

Suite 8 found two target-harness defects: Windows detached launch fallback and Windows Unicode
console event delivery. Both were repaired, reviewed, and integrated into the code baseline above.
Suites 9 through 13 found no further confirmed target-harness defect. Their findings concerned
manager/evidence handling, a Windows host permission restriction, the separate BYO Firmware MCP
server, or the tested hardware paths.

That is strong practical evidence that the core harness is stable. It is not a claim of exhaustive
hardware validation, and it does not convert the historical `0/3` formal clean-sprint tally into
three accepted clean sprints.

## Use it now

Start with `QUICK_RULES.md` and `QUICK_START.md`. Create isolated Git worktrees for concurrent
coding lanes, run a read-only discovery scan, then have your manager launch one controller per
lane. For Firmware work, supply your own policy-bound invocation and use only the resources and
MCP operations that the caller has explicitly authorized.

Historical Firmware campaign artifacts are retained outside this portable source tree as records;
they are not required to run the harness.
