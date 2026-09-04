# Orchestrator Harness v2: Command and Contract Reference

The package exposes one public launcher:

```powershell
python -m orchestrator_harness.operator_launch [--json] {harness,lane,resume-lane,manager,lease,send-lane-notification,scan,watch,health}
```

Every public command returns the structured result `{ ok, code, summary, evidence_paths,
next_action }`; `--json` emits the machine-readable result object. Success prints a short status
line and exits 0; failure prints one stable failure code and a message on stderr and exits
non-zero. The harness never schedules work, chooses providers, decides outcomes, accepts
results, operates hardware, or replaces the root orchestrator.

## Command reference

### `harness` — runtime lifecycle

- `harness setup [--overwrite]` — one-time idempotent integration. Validates
  `harness-config.json` and `resource-manifest.json`, preflights the entire catalog before
  writing anything, stages and byte-verifies the active super-cache, installs the ROOT payloads,
  writes the active resource manifest and lease directory, and starts the persistent monitor.
  It starts no lane or provider and never creates an epoch or worktree. `--overwrite` re-integrates.
- `harness shutdown` — end the whole runtime: OPEN -> SHUTTING_DOWN, each lane's controller cleans
  its own processes, the monitor stops, the active-epoch marker clears, and the runtime closes.
  It never kills by broad process name.

### `lane` — lane lifecycle

- `lane bootstrap --lane-id <id> --provider <id> --model <model> [--exclusive-resource <id>]
  --task-card <path>` — prepare one lane: create the worktree, stage the super-cache base and the
  selected provider payload, write the worker binding and inbox/outbox, and open the epoch on
  first use. `--exclusive-resource` may repeat; every name must be declared in
  `resource-manifest.json`.
- `lane launch --lane-id <id>` — start one prepared lane's controller and provider.
- `lane completion-review (--event-id <id> | --lane-id <id>) --review-outcome {PASS,FAIL,BLOCKED}
  --approval {ACCEPTED,REJECTED} --review-summary <text> [--evidence <path>] [--force-accept]
  [--force-reason <text>]` — record ROOT's factual finding and the separate accept/reject
  decision as the linked `COMPLETION_REVIEW.json` / `ORCHESTRATOR_ACCEPTANCE.json` pair outside
  the worktree, and close the managed review event.
- `lane force-stop --lane-id <id>` — hard-stop one stuck lane by exact recorded identity and
  retire it.
- `lane retire --acceptance-ref <path>` — gracefully retire one accepted lane (the reference is
  the `orchestrator-acceptance/v1` record with `approval: ACCEPTED`), cleanup-proof-first and
  archive-first.

### `resume-lane`

- `resume-lane --lane-id <id> --resume-task-card <path> [--rationale <text>]` — re-run a
  stopped, unaccepted lane in its same worktree and provider session with a fresh `run_id`. It
  never reconstructs a session, PID, or worktree.

### `manager` — manager-queue commands (managed only)

- `manager acknowledge --event-id <id>` — move one event PENDING -> ACKNOWLEDGED. Acknowledge
  only the envelope's top-level `event_id` after handling the event; `data.signal_id` is not an
  acknowledgement ID.
- `manager close --event-id <id> --outcome {COMPLETE,BLOCKED} --summary <text>` — close an
  acknowledged event.

### `lease` — orphaned lease recovery

- `lease force-release --resource-id <id>` — release one declared resource only after the exact
  holder is no longer live and current lane/run evidence proves release is safe. The command fails
  closed with a stable `code` and actionable `next_action`; never hand-edit lease records.

### `send-lane-notification`

- `send-lane-notification --lane-id <id> --prompt <text>` — append one assignment to a running
  managed lane's inbox.

### `scan`, `watch`, `health`

- `scan [--no-write]` — read-only lane-status snapshot.
- `watch [--until-actionable] [--timeout <duration>] [--until-event <id>]` — block until an
  actionable condition exists (or the named manager event appears). Durations accept `30s`, `5m`,
  `1h`.
- `health reconcile` — rebuild active-lanes and re-derive status (manual entry point to the
  monitor's automatic reconciliation).
- `health monitor-recover` — recover the persistent monitor. Managed coordination only and the
  runtime must be OPEN: starts a missing monitor, restarts a dead one, and force-stops a hung one
  before replacement. A deliberately stopped monitor (stop mark) is never restarted.

## Configuration and one-time integration

`harness-config.json` at the harness root is the closed two-key `harness-config/v1` shape: a
required absolute `root_workspace` and optional `managed_coordination` (`enabled` or `disabled`;
default `enabled`). No other keys are legal. `resource-manifest.json` is the closed
`resource-manifest/v1` shape: a literal `schema` field plus a `resources` list; every nonempty
entry declares a nonempty `id` and `exclusive: true`, and duplicate IDs or unknown fields are
invalid. ROOT never passes paths, feature flags, or a profile again on any later command; the
stored config selects everything.

`managed_coordination: enabled` (managed) stages a manager queue for the epoch: ROOT uses
`manager acknowledge`/`manager close` and `send-lane-notification`, and the monitor promotes
worker notices into the queue. `disabled` (plain) omits the manager queue.

## Providers and the adapter catalog

Three provider IDs ship: `codex`, `claude-code`, and `qwen-code`. Each has one tree under
`adapters/<provider-id>/`:

- `root/` — the provider's ROOT payload (config, hooks, binding, eight ROOT skills).
- `super-cache/` — the managed worker payload (worker hooks, config, binding, two worker skills).
- `harness/launcher_binding.py` — the launcher binding, registered under
  `orchestrator_harness/provider_adapters/<provider-id>/` by setup.
- `shipped-machinery/` — adapter-author notes.

`super-cache/custom/` is the custom file-only adapter tree: reserved for local, non-shipped
payload files. The shipped tree keeps only a marker README, and runtime staging never reads it
unless an operator explicitly places payloads there.

## Super-cache and bootstrap

`super-cache/workspace/` is the provider-neutral base staged into every managed worktree by
`lane bootstrap`: the shared `.agent-workspace/` skeleton plus three provider-neutral helpers:

- `result-stop-check.py` — validates the worktree `RESULT.json` against `result/v1` (schema,
  lane/run identity, outcome, summary, evidence, content hash) and exits 0/1.
- `lane-queue.py` — advances one ROOT assignment in the worker inbox (`lane-inbox/v1`) through
  PENDING -> ACKNOWLEDGED -> COMPLETE | BLOCKED.
- `manager-notify.py` — writes one escalation notice into `manager-notifications/` for the
  monitor to promote into the manager queue.

Bootstrap writes the authoritative worker binding with runtime paths into
`.agent-workspace/harness-hook-binding.json`; payload bindings are the static templates the
helpers read. Helpers never write the manager queue; the worker only touches its own inbox and
outbox.

## Lane lifecycle and evidence

Lifecycle: task card -> `.agent-workspace/RESULT.json` -> `COMPLETION_REVIEW.json` ->
`ORCHESTRATOR_ACCEPTANCE.json`. The worker writes the result; ROOT records the factual finding
and the separate accept/reject decision with `lane completion-review`. A result is merge-ready
only after schema, lane/run identity, outcome, summary, evidence, and content-hash validation
succeed against the current branch tip in a clean worktree. Reported checks are evidence; the
harness does not execute them.

Stop and cleanup use exact recorded identity (PID plus creation time) only; unknown identity is
never safe absence, and no process is ever killed by broad name or command matching. Static
examples and fixtures are never live provider proof: only a real recorded live run with exact
identity evidence (provider, session, command provenance, delivery receipts) can claim provider
proof.

## Read-only shipped source

The shipped source (`adapters/`, `super-cache/`, `orchestrator_harness/`) is read-only product
material. Launcher bindings are shipped/read-only: setup validates catalog binding bytes against
the registered binding and never copies a binding into product source. Local configuration
belongs under `local-config/`; runtime state lives under the configured root workspace's
`.harness-runtime` with a fresh epoch per live run. Keep source directories free of runtime
output.

## Release checks

`orchestrator_harness.release_checks` is the one stable-ID registry/selector owning fast,
affected, full, and release checks. Credit requires declared-input fingerprints plus exact source
root, Git common directory, and branch identity, with the recorded origin tip still an ancestor
of the current tip; unknown, divergent, mixed, or stale credit is not green evidence.
