# Portable Coding Orchestrator Harness (v2)

The Harness v2 coordinates ordinary software work across Git branches and worktrees. One public
launcher exposes the entire operator surface:

```powershell
python -m orchestrator_harness.operator_launch [--json] {harness,lane,resume-lane,manager,lease,send-lane-notification,scan,watch,health}
```

Every public command returns the small structured result `{ ok, code, summary, evidence_paths,
next_action }`; `--json` emits it as the machine-readable result object. Success prints a short
status line and exits 0; failure prints one stable failure code on stderr and exits non-zero.

## What the harness does and does not do

- Does: prepare and launch one coding worker per lane, publish durable lane/event records,
  serialize declared exclusive resources, deliver manager events, and record ROOT's review and
  acceptance.
- Does not: schedule work, choose providers, decide outcomes, accept results, operate hardware,
  or replace the root orchestrator. The persistent manager owns planning, launches, decisions,
  integration, acceptance, promotion, and final cleanup. The optional watcher is diagnostic-only
  with `evaluator_enabled: false`.

## Public CLI surface

The sole public CLI is `operator_launch`. Its groups and subcommands:

- `harness setup [--overwrite]` — one-time idempotent integration; `harness shutdown` — end the
  whole runtime.
- `lane bootstrap --lane-id --provider --model [--provider-option NAME=VALUE]
  [--exclusive-resource] --task-card` — prepare one
  lane; `lane launch --lane-id` — start one prepared lane.
- `lane completion-review (--event-id|--lane-id) --review-outcome {PASS,FAIL,BLOCKED} --approval
  {ACCEPTED,REJECTED} --review-summary [--evidence] [--force-accept] [--force-reason]` — record
  ROOT's review and acceptance.
- `lane force-stop --lane-id` — hard-stop one stuck lane; `lane retire --acceptance-ref` —
  gracefully retire one accepted lane.
- `resume-lane --lane-id --resume-task-card [--rationale]` — re-run a stopped, unaccepted lane.
- `manager acknowledge --event-id <top-level-event-id>` — move one event PENDING -> ACKNOWLEDGED; `manager close
  --event-id --outcome {COMPLETE,BLOCKED} --summary <text>` — close an acknowledged event (managed only).
- `lease force-release --resource-id <id>` — recover one declared orphaned resource lease. The
  command refuses an exact live holder and fails closed unless current process and lane/run evidence
  proves release is safe; follow its returned `next_action` instead of hand-editing lease records.
- `send-lane-notification --lane-id --prompt` — append one assignment to a running managed lane.
- `scan --no-write` — read-only lane-status snapshot; `watch --until-actionable [--timeout]
  [--until-event]` — block until an actionable condition exists.
- `health reconcile` — rebuild active-lanes and re-derive status.

## One-time ROOT sequence

1. Write `harness-config.json` at the harness root: the closed two-key `harness-config/v1` shape
   with a required absolute `root_workspace` and optional `managed_coordination` (`enabled` or
   `disabled`; default `enabled`). No other keys are legal.
2. Write `resource-manifest.json`: the closed `resource-manifest/v1` shape with a literal `schema`
   field and a `resources` list. Every nonempty entry declares a nonempty `id` and
   `exclusive: true`; duplicate IDs and unknown fields are invalid.
3. Run `harness setup` (repeat with `--overwrite` to re-integrate). Setup validates the config and
   manifest, preflights the entire catalog before writing anything, stages and byte-verifies the
   active super-cache, installs the ROOT payloads, writes the active resource manifest and lease
   directory, and starts the persistent monitor. It starts no lane or provider and never creates
   an epoch or worktree.

Setup preserves existing `.codex/` and `.claude/` directories. It adds the harness-owned skills,
hook scripts, and binding files, while structurally merging harness hook registrations into an
existing `.codex/hooks.json` or `.claude/settings.json`. Existing fields, permissions, and hook
groups remain in place. An existing `.codex/config.toml` is preserved byte-for-byte and must already
set `[features] hooks = true`. `--overwrite` replaces only harness-owned payload files; it does not
replace these shared provider configuration files.

### Managed vs plain

`managed_coordination: "enabled"` (managed) stages a manager queue for the epoch: ROOT uses
`manager acknowledge`/`manager close` and `send-lane-notification`, and the monitor promotes
worker notices into the queue. Managed watch wakes from an unresolved queue event or an
actionable lane/resource state and records a durable per-session/queue delivery receipt for
queue events. `disabled` (plain) omits the manager
queue; lanes run without ROOT-to-worker assignments and plain scan/watch never reads it.

## Providers and the adapter catalog

Three provider IDs ship: `codex`, `claude-code`, and `qwen-code`. Each has one tree under
`adapters/<provider-id>/` with `root/` (ROOT payload), `super-cache/` (worker payload), and
`harness/launcher_binding.py` (registered under
`orchestrator_harness/provider_adapters/<provider-id>/` by setup). `super-cache/custom/` is the
custom file-only adapter tree: reserved for local, non-shipped payload files. The shipped tree
keeps only a marker README, and runtime staging never reads it unless an operator explicitly
places payloads there.

Every lane model and model preference is explicit bootstrap configuration and is preserved for
native resume. Codex requires `reasoning_effort` and `service_tier` provider options; Claude Code
requires `effort`; Qwen Code currently has no provider option beyond its required model. Missing,
unknown, or duplicate options fail before an epoch, worktree, controller, or provider is created.
Launcher bindings never select a model, effort, service tier, or fallback.

## Super-cache and bootstrap

`super-cache/workspace/` is the provider-neutral base staged into every managed worktree by
`lane bootstrap`: the shared `.agent-workspace/` skeleton plus the three provider-neutral helpers
(`result-stop-check.py`, `lane-queue.py`, `manager-notify.py`). Bootstrap also materializes the
selected provider's worker payload, writes the authoritative worker binding with runtime paths,
and creates the lane's inbox/outbox. Bootstrap opens the epoch on first use and reuses it while
the immutable configuration is unchanged.

## Worker RESULT vs ROOT review/acceptance

The worker writes `RESULT.json` at the worktree root (`result/v1`). A result is merge-ready only
after schema, lane/run identity, outcome, summary, evidence, and content-hash validation succeed
against the current branch tip in a clean worktree. ROOT then records the factual finding and the
separate accept/reject decision with `lane completion-review`, which writes the linked
`COMPLETION_REVIEW.json` and `ORCHESTRATOR_ACCEPTANCE.json` pair outside the worktree and closes
its own managed review event. Lifecycle: task card -> RESULT.json -> COMPLETION_REVIEW.json ->
ORCHESTRATOR_ACCEPTANCE.json.

An invalid result may receive at most five corrective prompts in the same native provider
session, with per-attempt transcript, stderr, cleanup, and validation evidence. A sixth invalid
attempt, a missing native resume session, or a provider startup/auth/process failure ends as
`provider_exited_no_result`; it never starts a fresh context.

## Resume, force-stop, retire, shutdown

- `resume-lane` re-runs a stopped, unaccepted lane in its same worktree and provider session with
  a fresh `run_id`; it never reconstructs a session, PID, or worktree.
- `lane force-stop` hard-stops one stuck lane by exact recorded identity.
- `lane retire` gracefully retires one accepted lane (by acceptance reference) with
  cleanup-proof-first and archive-first semantics.
- `harness shutdown` ends the whole runtime: OPEN -> SHUTTING_DOWN, each lane's controller cleans
  its own processes, the monitor stops, the active-epoch marker clears, and the runtime closes.
  It never kills by broad process name.

## Read-only shipped source

The shipped source (`adapters/`, `super-cache/`, `orchestrator_harness/`) is read-only product
material. Launcher bindings are shipped/read-only: setup validates catalog binding bytes against
the registered binding and never copies a binding into product source. Local configuration
belongs under `local-config/`; runtime state lives under the configured root workspace's
`.harness-runtime` with a fresh epoch per live run. Keep source directories free of runtime
output.

## Evidence and provider proof

Reported checks are evidence; the harness does not execute them. Static examples and fixtures are
never live provider proof: only a real recorded live run with exact identity evidence (provider,
session, command provenance, delivery receipts) can claim provider proof.
`orchestrator_harness.release_checks` is the one stable-ID registry/selector owning fast,
affected, full, and release checks; credit requires declared-input fingerprints plus exact source
root, Git common directory, and branch identity, with the recorded origin tip still an ancestor
of the current tip.

## Documentation

- `QUICK_START.md` — shortest live run.
- `QUICK_RULES.md` — authority and safety rules.
- `orchestrator_harness/README.md` — command and contract reference.
- `orchestrator_harness/SPEC.md` — harness contracts.
- `docs/HARNESS_WATCHER_GUIDE.md` — optional diagnostic watcher.
- `adapters/README.md` — provider adapter catalog and binding contract.
