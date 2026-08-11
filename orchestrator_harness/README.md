# Coding Orchestrator Harness

The package reconciles coding-lane facts and delivers sparse, durable manager
events. It does not schedule work, choose providers, accept results, operate
hardware, or replace the root orchestrator. S3 `ManagerEventRouter` remains the
single queue, wake, delivery-evidence, and acknowledgement authority.

## Start here

Use a fresh ignored configuration and runtime root for each epoch:

```powershell
python -m orchestrator_harness --config local-config/harness.json scan --no-write
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60
```

`scan`, `watch --once`, `watch --until-event`, and `watch --until-actionable`
are diagnostic/public waits. They do not start a foreground managed watcher,
renew a heartbeat, or create a second notification queue. The native manager
discovers work through its blocking wait and acknowledges the envelope's
top-level `event_id` through the S3 manager API; a worker `data.signal_id` is
not an acknowledgement ID.

## Codex adapter

The current implemented profile is selected through one versioned capability
interface. Future-host profiles are contract fixtures only and make no install
or implementation claim.

```powershell
python -m orchestrator_harness adapter install --host codex --project-root <disposable-project>
python -m orchestrator_harness adapter check --host codex --project-root <disposable-project>
python -m orchestrator_harness adapter upgrade --host codex --project-root <disposable-project>
python -m orchestrator_harness adapter uninstall --host codex --project-root <disposable-project>
```

The installer owns only the packaged project-local hook files, the managed hook
entries, and its installation manifest. It validates the `.codex` shape before
writing, uses same-directory atomic replacements, records only closed managed
content identities, refuses ambiguous ownership, and restores bounded changes
on failure. Uninstall subtracts exact managed fragments/assets and preserves
unrelated or modified user content.
The manifest separately reports installed bytes, synthetic self-test status,
project-layer trust, and hook review state; installation never silently grants
project trust.

Codex command hooks are synchronous lifecycle hooks. PostToolUse is a safe
tool-result boundary and Stop is a finalization backstop. The persistent
harness coordinator owns the binding-specific wake subscription and replay; a
hook does not directly watch arbitrary file changes. App Server fixtures use
`thread/inject_items`, `turn/completed`, and `turn/start` for idle continuation.
Delivery notices contain binding identity, queue revision, pending count,
highest class/severity, timestamp, and adapter profile only. Delivery receipts
are transport evidence and never acknowledge pending events.

## Immutable views and terminal lanes

Static work can receive an exact-commit read-only source view with separate
writable result and cache roots:

```powershell
python -m orchestrator_harness view allocate --source-root <repo> --revision <full-commit> `
  --retained-ref <ref> --view-root <view> --result-root <results> --cache-root <cache>
```

The allocation publishes a ready record only after the exact revision and
read-only boundary are established. A partial allocation has no ready record
and never writes results beneath the source view.

Terminal retirement is archive-first:

```powershell
python -m orchestrator_harness lane retire --lane-root <linked-worktree> `
  --archive-root <archive> --lane-id <lane> --run-coordinate <run-root> `
  --task-ref <task.json> --result-ref <result.json> --findings-ref <findings.json> `
  --acceptance-ref <acceptance.json> --transcript-ref <transcript.json> `
  --dependency-ref <dependency.json>
```

The controller owns one fixed lifecycle record derived from the run coordinate
and lane ID outside the retiring worktree. Retirement validates that record,
freshly observes every persisted process identity twice, then copies and
validates task/result/findings/acceptance/transcript/dependency/process evidence
and content hashes before normal `git worktree remove`. Dirty, live, ambiguous,
unretained, unmerged, or archive-failed lanes remain visible.

## Configuration migration

Retained configuration covers discovery, bounded reads, and diagnostic waits.
Integer counts and limits reject fractional values; durations must be finite.
Removed watcher, heartbeat, attention, review/no-progress, and obsolete
tolerance keys produce explicit migration diagnostics for compatibility readers
and have no S4 runtime effect. They are not present in `config.example.json`.

## Diagnostic watcher

The optional watcher remains diagnostic-only with `evaluator_enabled: false`.
Its recovery projection is limited to `open`, `acknowledged`, and `resolved`.
Watcher diagnostics do not become manager wake events and do not perform
repair, scheduling, or acknowledgement.

## Exit codes

- `0`: diagnostic operation or lifecycle operation completed
- `1`: configuration, safety, ownership, or lifecycle proof failed
- `3`: bounded diagnostic wait timed out

## Legacy firmware suite use

The schema-less legacy firmware compatibility path remains separate from the
coding contract. It may retain firmware-specific evidence readers, but it is
not a host-adapter implementation and is not part of the ordinary coding
workflow. A schema-less legacy firmware result is never accepted as a coding
lane result.
