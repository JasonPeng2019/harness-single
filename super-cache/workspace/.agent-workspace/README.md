# Super-cache workspace base

This directory is the provider-neutral workspace base staged into every
managed worktree by `lane bootstrap`.  It carries the shared
`.agent-workspace/` skeleton and the three provider-neutral helpers that the
worker skills call.

## Contents

- `result-stop-check.py` — validates the worktree `RESULT.json` against
  `result/v1` (schema, lane/run identity, outcome, summary, evidence, and
  content hash) and exits 0/1.
- `lane-queue.py` — advances one ROOT assignment in the worker inbox
  (`lane-inbox/v1`) through PENDING -> ACKNOWLEDGED -> COMPLETE | BLOCKED.
- `manager-notify.py` — writes one escalation notice into
  `manager-notifications/` for the monitor to promote into the manager queue.

## Rules

- Helpers are provider-neutral and portable on Windows and Linux.
- Helpers prefer the current runtime record primitives
  (`orchestrator_harness.records`, `orchestrator_harness.core`) when the
  package is importable and fall back to portable stdlib equivalents.
- Helpers never write the manager queue; the worker only touches its own
  inbox and outbox.
- Bootstrap writes the authoritative worker binding with runtime paths into
  `.agent-workspace/harness-hook-binding.json`; the payload binding is the
  static template the helpers read.
