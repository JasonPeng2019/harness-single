# Final v2 Harness Overview

## Delivery status

The v2 runner publishes one lane-management/controller path. It accepts the provider-neutral
`orchestrator-worker-invocation/v1` schema and the retained Codex
`orchestrator-coding-invocation/v1` adapter shape. Controller, provider adapters, resource claims,
event routing, resume handling, and cleanup proofs are shared by Codex, Claude Code, and Qwen Code
lanes.

## What the harness does

The harness is a small execution boundary for a manager that already knows what work to do. It
provides:

- A versioned invocation/result contract, a lane controller that starts one provider worker, and
  durable status, event, checkpoint, and result records.
- Exact PID-plus-creation-time process ownership, bounded shutdown/reap evidence, and cleanup that
  releases named claims only after the owned process boundary is gone.
- Opaque exclusive named locks for real non-Git conflicts, waiting/recovery records, and safe
  resume of a lane with a replacement provider session.
- Git worktree/branch/base identity validation, current-tip result validation, and a thin public
  launcher for a manager that wants a detached controller.
- A process-local provider-adapter contract: a registered adapter owns its command construction,
  transcript decoding, terminal outcome, redaction, and safe-boundary notification delivery.
- A diagnostic observer and optional watcher. They reconcile durable state and surface actionable
  facts; they do not execute a worker action or replace the manager's acknowledgement queue.
- A release-check registry that selects fast or affected checks from declared dependencies and
  reuses prior credit only when the command, inputs, source identity, and ancestor relationship
  still match.

## Canonical lane contract

Both supported input shapes feed the same controller lifecycle. The removed schema-less firmware
shape is unsupported and fails ordinary invocation validation before workspace, event, or provider
work starts; there is no compatibility rejection branch.

## What it does not do

The harness intentionally does not:

- Plan work, choose a test suite, schedule a dependency graph, merge code, decide acceptance, or
  promote a branch. Those remain manager and project decisions.
- Act as a task database, source-control system, file-ownership service, or generic scheduler.
- Turn a missing/corrupt external result, unavailable dependency, or host permission denial into a
  successful result. It records and contains those facts; the responsible project decides how to
  recover.
- Guarantee that a manager follows the intended recovery policy. The manager remains responsible
  for acceptance and evidence decisions.

## Use it now

Start with `QUICK_RULES.md` and `QUICK_START.md`. Create isolated Git worktrees for concurrent
coding lanes, run a read-only discovery scan, then have the manager launch one canonical
controller per lane. The optional watcher may provide deterministic diagnostics around the same
event and status records.
