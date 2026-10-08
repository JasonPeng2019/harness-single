# Quick Start: Harness v2 Operator Run

The shortest live run: configure once, setup, bootstrap, launch, watch, review, retire. All
commands use the one public launcher:

```powershell
python -m orchestrator_harness.operator_launch <group> <command> [options]
```

## 1. Configure the harness root

Write `harness-config.json` at the harness root (closed two-key `harness-config/v1` shape):

```json
{
  "root_workspace": "<absolute-path-to-root-workspace>",
  "managed_coordination": "enabled"
}
```

Write `resource-manifest.json` (closed `resource-manifest/v1` shape):

```json
{
  "schema": "resource-manifest/v1",
  "resources": []
}
```

Declare each exclusive resource as `{"id": "<name>", "exclusive": true}` in `resources`.
`managed_coordination: "disabled"` runs plain lanes without the manager queue.

## 2. One-time setup

```powershell
python -m orchestrator_harness.operator_launch harness setup
```

Setup is idempotent; re-run with `--overwrite` to re-integrate harness-owned files. Existing Codex
and Claude configuration is preserved, with harness hooks merged into the provider hook files. An
existing `.codex/config.toml` must already set `[features] hooks = true`. Setup starts no lane or
provider.

## 3. Bootstrap one lane

```powershell
python -m orchestrator_harness.operator_launch lane bootstrap `
  --lane-id lane-01 --provider codex --model <model> `
  --provider-option reasoning_effort=<effort> `
  --provider-option service_tier=<tier> `
  --task-card <path-to-task-card.json>
```

`--task-card` names a `project-task-card/v1` file (task text, branch, base commit). Add
`--exclusive-resource <id>` for each resource declared in the manifest. Bootstrap creates the
worktree, stages the super-cache base and the selected provider payload, and writes the worker
binding. Shipped provider IDs: `codex`, `claude-code`, `qwen-code`.

Launch preferences have no harness defaults. Codex requires `reasoning_effort` and `service_tier`;
Claude Code requires `effort`; Qwen Code currently requires only `--model`. Each option uses a
repeated `--provider-option NAME=VALUE`. Bootstrap rejects missing, duplicate, or unsupported
preferences before creating the lane, and resume reuses the recorded values unchanged.

## 4. Launch

```powershell
python -m orchestrator_harness.operator_launch lane launch --lane-id lane-01
```

## 5. Wait and acknowledge (managed)

```powershell
python -m orchestrator_harness.operator_launch watch --until-actionable --timeout 5m
python -m orchestrator_harness.operator_launch manager acknowledge --event-id <top-level-event-id>
```

Acknowledge only the envelope's top-level `event_id` after handling the event; `data.signal_id`
is not an acknowledgement ID. Close handled events with:

```powershell
python -m orchestrator_harness.operator_launch manager close --event-id <id> --outcome COMPLETE --summary <text>
```

## 6. Review and accept

The worker writes `RESULT.json` at the worktree root (`result/v1`). ROOT records the factual finding
and the separate accept/reject decision:

```powershell
python -m orchestrator_harness.operator_launch lane completion-review `
  --lane-id lane-01 --review-outcome PASS --approval ACCEPTED --review-summary <summary>
```

## 7. Retire

```powershell
python -m orchestrator_harness.operator_launch lane retire --acceptance-ref <acceptance-ref>
```

## Resume, force-stop, shutdown

- Resume a stopped, unaccepted lane: `resume-lane --lane-id lane-01 --resume-task-card <card>`,
  then `lane launch --lane-id lane-01`.
- Hard-stop a stuck lane: `lane force-stop --lane-id lane-01`.
- End the whole runtime: `harness shutdown`.

## Notes

Follow the important events (memory recall and its results, worker prompts and
messages, lane outcomes, and failures) in real time with:

```powershell
.\tools\Watch-MonitorLog.ps1
```

The append-only `<runtime>\monitor\MONITOR_IMPORTANT.log` holds the selected
events. Routine monitor passes, heartbeats, scans, raw provider events, and
full launch payloads stay in the separate `MONITOR_DETAIL.log` trace. Operator
memory registration and storage events go to
`MONITOR_OPERATOR_MEMORY.log`. For older
runs without the important-events file, the watcher filters that detailed trace
at read time. It accepts `-RuntimeRoot`, `-Tail`, and `-NoWait`.

For runs using `MEMORY_HARNESS_WORKER_RECALL=1`, each worker bootstrap queries
the shared experience stores using ROOT's exact task assignment, then gives
the selected context to that worker. Recall records include the query, lane,
results, and final worker delivery. Optional memory configuration for the native
workflow is in [`docs/WORKER_MEMORY.md`](docs/WORKER_MEMORY.md).

For the full execution trace—including queue snapshots on admission, complete
ROOT-to-worker prompts, controller/provider launches, every native worker JSONL
event, and expanded memory queries/results—follow the separate detailed log:

```powershell
.\tools\Watch-DetailedMonitorLog.ps1 -RuntimeRoot <runtime> -Tail 100 -All
```

Credential-shaped fields and obvious bearer/database/API secrets are redacted;
task, prompt, and recalled memory text remain visible. Pass `-Raw` with `-All`
to print the exact JSONL records. `-NoWait` prints the tail once and exits.

- Run `scan --no-write` before launch for a read-only lane-status snapshot.
- `send-lane-notification --lane-id <id> --prompt <assignment>` appends one assignment to a
  running managed lane.
- `orchestrator_harness.release_checks` is the one stable-ID registry/selector for fast,
  affected, full, and release checks; credit requires exact source root, Git common directory,
  and branch identity.
- Static examples and fixtures are never live provider proof; only a real recorded live run with
  exact identity evidence can claim provider proof.
- Stop and cleanup use exact recorded PID-plus-creation identity only; never kill by broad process
  name or command matching.
- Use a fresh epoch and fresh runtime directories for every live run; retire lanes or shut down
  before re-running.
