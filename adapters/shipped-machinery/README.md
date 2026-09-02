# Shipped machinery (adapter authors)

The shipped machinery is the product-owned material every adapter payload
relies on: the provider-neutral helpers in
`super-cache/workspace/.agent-workspace/`, the managed hook/binding records,
and the registered launcher-binding contract.

## Provider-neutral helpers

- `result-stop-check.py` — validates the worktree `RESULT.json` against
  `result/v1` (schema, lane/run identity, outcome, summary, evidence,
  content hash) and exits 0/1.
- `lane-queue.py` — advances one ROOT assignment in the worker inbox
  (`lane-inbox/v1`) through PENDING -> ACKNOWLEDGED -> COMPLETE | BLOCKED.
- `manager-notify.py` — writes one escalation notice into
  `manager-notifications/` for the monitor to promote into the manager queue.

Helpers prefer the current runtime record primitives
(`orchestrator_harness.records`, `orchestrator_harness.core`) when the
package is importable and fall back to portable stdlib equivalents.  They
never write the manager queue.

## Hook and binding records

- `orchestrator-harness-binding.json` — the payload binding
  (`harness-hook-binding/v1`) with `role` (`root` or `worker`) and
  `provider_id`; the worker payload also declares `inbox_path`, `outbox_dir`,
  `result_path`, and `result_stop_check`.
- `hooks/` — portable PostToolUse hook scripts that append liveness receipts
  and, for ROOT with `HARNESS_EVENT_ID`, managed delivery receipts.
- Bootstrap writes the authoritative worker binding with runtime paths into
  `.agent-workspace/harness-hook-binding.json`; payload bindings are the
  static templates the hooks read.

## Portability

All shipped scripts are stdlib-first and run on Windows and Linux.  Adapter
authors must keep hooks and helpers free of provider-specific imports and
must not add a second public CLI.
