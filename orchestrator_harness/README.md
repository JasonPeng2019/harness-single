# MCP-Trial-3 Orchestrator Harness

This package is a read-only watcher for the parallel firmware-test lanes. It does not schedule
agents, operate hardware, write permission relays, change leases, edit evidence, or repair the
server. The active main orchestrator consumes its events and remains the only authority.

## Commands

From the portable repository root:

```powershell
python -m orchestrator_harness --config orchestrator_harness/config.example.json scan --no-write
python -m orchestrator_harness --config orchestrator_harness/config.example.json watch --once
python -m orchestrator_harness --config orchestrator_harness/config.example.json watch --until-event --timeout 60
python -m orchestrator_harness --config orchestrator_harness/config.example.json watch --until-actionable --timeout 60
# With attention logging enabled, supply the root identity:
python -m orchestrator_harness --config <config-under-multi-agent-logs> watch --until-actionable --timeout 60 --manager-session-id <session> --manager-invocation-id <invocation>
python -m orchestrator_harness --config orchestrator_harness/config.example.json watch --managed
python -m orchestrator_harness --config orchestrator_harness/config.example.json ack --event-id <event-id>
python -m orchestrator_harness --config orchestrator_harness/config.example.json heartbeat
python -m orchestrator_harness --config orchestrator_harness/config.example.json watch stop
```

`scan` never writes. Every watcher write is confined to the configured harness output directory;
it never writes a run, experiment, server, relay, or evidence path. The output directory may contain:

- `snapshot.json` and `events.jsonl`
- `pending-notification.json`
- managed-watch runtime and active-management history records when `watch --managed` is used.

Events use stable IDs and at-least-once delivery. Consumers must deduplicate by `event_id`.

`watch --until-actionable` is the root manager's bounded wait: it records normal observation
changes silently and returns one durable notification only when manager review is needed. The
same notification is returned until its exact `ack` command succeeds, except when a newly
actionable event has strictly higher admitted priority. In that case the pending event is moved to
durable deferred state, the urgent event is delivered, and the displaced event is restored after
the urgent event is acknowledged. Equal- or lower-priority events never preempt. The
`pending-notification.json` state lives only in the configured harness output directory and stores
the priority admitted when the event became pending.

With attention logging enabled, the returned wake fields are evidence, not a manager action. The root's **first logged action** after return must be `MANAGER_WAKE_RECEIVED`, followed by the matching wake-bearing `MANAGER_WAIT_FINISHED` (same wake ID, transport, session, invocation, and wait activity ID), and only then a scan, claim, decision, or response. A quiet timeout has no wake fields or wake records.

### Managed watcher workflow

Use `watch --managed` once for a foreground, self-arming manager connection:

```text
start managed watch -> receive one durable JSONL notification -> manager reviews/acts
-> acknowledge its exact event ID -> the same watcher reconciles and re-arms
```

An unacknowledged pending notification is retained and is re-delivered by a later managed-watch
process, unless it is durably displaced by a strictly higher-priority actionable event as described
above. While a notification is pending, the watcher continues reconciling but does not print it
again in the same process. Acknowledging a `MANAGER_REVIEW_DUE` reminder starts the next review
interval; acknowledging any other event does not substitute for that whole-suite review.

While lanes are active, managed mode can report `MANAGER_REVIEW_DUE`, `LANE_NO_PROGRESS`, and
`LANE_STAGE_REPEAT` in addition to the existing actionable events. They are review aids: the root
manager must inspect the relevant checkpoint, request, or doer output before deciding what to do.
Existing live request and process-safety conditions take priority, followed by stage repetition,
no-progress, review reminders, and checkpoint/result notifications.

The manager may use `heartbeat` (or `watch heartbeat`) during a long review. `watch stop` writes
only a cooperative stop request; it never signals or kills a process. Managed mode is not a
detached service: it exits cleanly when stopped, when its manager heartbeat expires, or when the
exact launching-owner process identity disappears. Pending state remains durable across those
exits.

External doers may publish immutable JSON files in
`<run>/.agent-workspace/manager-signals/*.json`. A valid `manager-signal/v1` object has required
`signal_id`, `kind` (`HELP`, `FEEDBACK`, `INSTRUCTION`, `PASS`, or `CHECKPOINT`), `created_utc`,
`lane_id`, and concise `summary` fields. Optional `task`, `phase`, and `evidence_paths` are
reported; `deadline_utc` is the ordinary response deadline and ordering deadline when no delivery
deadline is declared. Optional delivery metadata is `delivery_deadline_utc` (a UTC timestamp not before
`created_utc`), `agent_blocked` (a boolean), and `attention_epoch_id` (a non-empty string).
Response and delivery deadlines remain separate: only a blocked `HELP` signal
with a valid delivery deadline receives urgent delivery ordering ahead of routine stale status.
Discovery accepts only final ordinary JSON names; hidden JSON and `*.tmp.json` staging files are
ignored. For a counted attention chain, the worker prepares the staged payload, durably records
`AGENT_SIGNAL_CREATED`, captures exact UTC immediately before atomically renaming the payload to
its final ordinary JSON path, then records `AGENT_SIGNAL_PUBLISHED` with that captured time and
records `AGENT_WAIT_STARTED`. Every recorder call must return a real `record_id`. Publication
logging is passive evidence and does not participate in discovery or wake delivery.
The watcher never opens evidence paths, changes signals, or performs a response action.

When a retained `MANAGER_SIGNAL` is observed but native liveness rules intentionally make it
non-actionable, the harness appends passive `HARNESS_EVENT_INELIGIBLE` attention evidence. Its
bounded reason is `ALREADY_ANSWERED`, `INVALID_LANE_ID`, or `LANE_NOT_LIVE`. This record is written
after observation and never changes admission, pending state, selection, scheduling, or wake
delivery; it exists only so post-run analysis does not falsely blame the harness for correctly
rejecting historical or malformed authority.

For M5, the 90-second delivery value is a diagnostic target, not an automatic sprint verdict. The
post-sprint analyzer may accept a longer interval only when gap-free canonical records prove the
persistent manager was doing genuine work or handling an earlier genuine request. This does not
relax the harness contract: late harness observation and time spent waiting for the native harness
remain harness-delay evidence.

## Exit codes

- `0`: scan/event completed
- `1`: configuration, safety, or observation failure
- `3`: bounded watch timeout

## Required suite use

For an overlapping or HIL/server-consuming suite epoch, the active main orchestrator **must**
create a fresh config from `config.example.json` whose `run_globs` cover every run the epoch may
activate. Never reuse a prior epoch's config, output state, process records, or notification
records as a new live runtime. The watcher is not a coordinator and does not replace the manager
or its durable ledgers. Each manager epoch is:

```text
reconcile -> start/recover one managed watcher -> launch every eligible lane
-> consume one durable actionable notification -> manager reviews/acts
-> acknowledge the exact event ID -> rescan every lane and lease
-> fill newly eligible work -> heartbeat/checkpoint -> repeat
```

Doers remain in their existing turn while a bounded helper gate is active; they return only at a
genuine checkpoint, provider/lease wait, or completed slice. The manager alone reviews and writes
relays, assigns/releases leases, classifies failures, and serializes server repairs.

During active HIL the manager inspects every live doer normally every two to three minutes and
renews the heartbeat before expiry during long reviews. On completion it uses `watch stop` and
confirms exact watcher/owner identity cleanup, or durably hands off heartbeat ownership. If the
manager disappears, heartbeat expiry is the expected fail-closed shutdown.

## Operational interpretation

- Controller records use `RUNNING_CODEX`, `CODEX_EXITED`, `CONTROLLER_INTERRUPTED`,
  `LAUNCH_FAILED`, or `CONTROLLER_FAILED`; `CODEX_EXITED` covers both zero and nonzero Codex
  exits and records `exit_code`. They atomically record separate controller/Codex creation times
  and parent identities.
- `RUNNING_CODEX` means separate controller and Codex PID-plus-provider-creation-time identities
  and parentage match within the capped two-second precision tolerance.
- `PROCESS_STATE_UNKNOWN` means the process provider could not prove identity. It never implies a
  safe lease release or relaunch.
- `RELAY_READY` means every declared PID in an unrelayed request's producer lifetime is provably
  live. Partial evidence is ambiguous.
- A path-only relay candidate never counts. `RELAYED` requires the watcher-computed request hash
  and live producer identity to match relay contents. A request becomes lane-owned only with an
  exact session ID or declared lane ID; unscoped historical requests remain standalone observations.
- Controller/helper/MCP exit, checkpoint/result, provider wait, request state, and
  `RESOURCE_RELEASE_POSSIBLE` are independent events; a checkpoint never hides a process exit.
  “Possible” is informational only: the harness never releases a lease.
- The watcher never changes the suite ledger. Its reconciled snapshot is the operational view.
  Each HIL boundary still requires a unique MCP/state/artifact/log root, recorded PID ancestry,
  first-public-artifact proof, and only exact descendant cleanup—never broad command matching.

The watcher cannot wake an inactive or closed conversation. It cannot make autonomous decisions,
approve or publish relays, operate hardware, control processes, or replace the root manager.
Managed mode remains foreground-only and therefore exits after its bounded heartbeat grace when
the manager no longer renews it. `watch --until-actionable` remains a compatibility mode for a
single bounded wait; suite execution uses `watch --managed` so one active manager connection
self-arms after acknowledgements.

## Tests

```powershell
python -m compileall -q orchestrator_harness
python -m unittest discover -s orchestrator_harness/tests -t . -v
python orchestrator_harness/tests/wsl_cleanup_guard_test.py
python orchestrator_harness/tests/real_agent_test.py
```

The ordinary watcher host suite does not require WSL. The optional real-agent acceptance test
requires an explicitly selected disposable WSL2 distribution, then adds bubblewrap mount/user/PID
isolation, a default-deny network namespace, an exact-destination CONNECT proxy, and a bounded
cgroup. MCP is empty and no firmware server, Windows drive, USB device, or board is visible.

Install the pinned Linux Codex test tool into that disposable distribution:

```powershell
powershell -ExecutionPolicy Bypass -File orchestrator_harness/install_wsl_codex.ps1 -Distro <disposable-distro>
```

Remove the entire added tool footprint afterward:

```powershell
wsl.exe -d <disposable-distro> -u root -- rm -rf /opt/orchestrator-harness-codex
```

Every real-agent run removes its WSL-local temporary workspace and credential. Its optional output
directory is disposable and must not be carried into a new suite epoch.

### Manager-side detached launch

The read-only watcher never launches work. The active manager may explicitly use the separate
operator primitive for a long-lived manager owner, optional watcher, or lane controller:

```powershell
python -m orchestrator_harness.operator_launch --receipt <absolute-receipt.json> --label <label> --role <owner|watcher|controller> --cwd <existing-dir> -- <program> <args...>
```

It launches without a shell, writes an atomic receipt with PID plus provider creation identity,
and fails closed if that identity cannot be proved. It is not imported or invoked by watcher paths
and has no scheduling, acknowledgement, lease, hardware, or process-kill authority.

### General coding lane invocation

`python -m orchestrator_harness.lane_controller <invocation.json>` accepts the explicit
`orchestrator-coding-invocation/v1` schema for one coding worker turn. The schema requires a
`runtime_root`, confines the event log beneath that root, confines the prompt to `run_root`, and
confines all output paths to `run_root/.agent-workspace`. Prompt bytes must match
`prompt_sha256`. It also requires a `repository` object declaring the actual Git `common_dir`,
`worktree_root` (which must equal `run_root`), attached short `branch`, and full `base_commit`.
The coding controller status path must be a direct, non-hidden lowercase `*.json` file under
`.agent-workspace` and cannot be the reserved `RESULT.json`. This lets duplicate detection inspect
every permitted status path while ignoring JSON files without the coding controller schemas.
The controller obtains all Git facts with bounded `subprocess` argv calls and never invokes a
shell. Git inspection uses a minimal execution environment and does not inherit repository,
worktree, object, index, or Git configuration redirection variables. See
`examples/coding.invocation.example.json` for the complete start shape.

Coding invocations carry generic `resources` and Codex launch settings; they do not require the
firmware policy, server snapshot, board token, MCP server, lease, relay, or hardware fields. The
schema-less legacy firmware shape remains policy-bound. Any other explicit schema is rejected.

Before a coding launch, the controller proves the declared common directory and worktree root,
requires the exact attached branch, resolves the base commit, and records the canonical common
directory, worktree, branch, base commit, and starting commit in controller status. It rejects a
duplicate worktree or branch claimed by another coding status only when that status says
`RUNNING_CODEX` and both recorded PID/creation identities and parentage are currently live. Git
worktree paths use Windows case-normalized comparisons. Historical exited or stale status does not
reserve a worktree or branch.

For `resume`, reuse the same output paths and `worker_invocation_id`, and provide the prior thread as
either `resume_thread_id` or `resume_identity.thread_id`. If `resume_identity` also includes
`worker_invocation_id`, it must match the top-level value. The controller rejects requested worker
or thread identities that differ from persisted status. Status and lane events record both
`invocation_schema` and `worker_invocation_id`. Resume also requires the persisted repository,
worktree, branch, base, and original starting-commit identity; the real worktree and branch are
revalidated immediately before launch.

A coding lane completes only with `.agent-workspace/RESULT.json` using
`orchestrator-lane-result/v1` and these fields:

```json
{
  "schema": "orchestrator-lane-result/v1",
  "lane_id": "S1.P",
  "worker_invocation_id": "s1-product-001",
  "branch": "work/s1-product",
  "commit": "0123456789abcdef0123456789abcdef01234567",
  "outcome": "PASS",
  "summary": "Implemented and verified the focused change.",
  "checks": [
    {"name": "focused tests", "command": "python -m unittest ...", "outcome": "PASS"}
  ]
}
```

`outcome` is `PASS`, `FAIL`, or `BLOCKED`; checks are bounded and use `PASS`, `FAIL`, `SKIP`, or
`NOT_RUN`. Reported command strings are evidence only and are never executed. The result lane,
worker, and branch must match current controller status, its commit must equal the real current
branch tip, and the project worktree must be clean. Ordinary ignored runtime state is excluded by
Git. Invalid coding result evidence is bounded in controller status and the reconciled snapshot;
it disappears from the current snapshot when a corrected valid result replaces it. A firmware
shaped result is never accepted for a coding controller. Schema-less firmware lanes retain their
legacy result route. Discovery binds a coding result to the exact controller status matching both
its lane and worker invocation, so controller recency in another lane cannot reroute the result.
A schema-less legacy result binds only when one firmware controller owns that run workspace.
