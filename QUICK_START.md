# Quick Start: Harness v2 Operator Run

The shortest live run: configure once, setup, bootstrap, launch, watch, review, retire. All
lane operations use the one public launcher:

```powershell
python -m orchestrator_harness.operator_launch <group> <command> [options]
```

## 1. Configure the harness root

Write `harness-config.json` at the harness root (closed `harness-config/v1` shape):

```json
{
  "root_workspace": "<absolute-path-to-product-workspace>",
  "manager_workspace": "<absolute-path-for-ROOT-hooks-and-skills>",
  "managed_coordination": "enabled"
}
```

`root_workspace` selects the Git product workspace; `manager_workspace` is optional and selects
where ROOT hooks, skills and `.agent-workspace` are installed (defaults to `root_workspace`).

Write `resource-manifest.json` (closed `resource-manifest/v1` shape):

```json
{
  "schema": "resource-manifest/v1",
  "resources": []
}
```

Declare each exclusive resource as `{"id": "<name>", "exclusive": true}` in `resources`.
`managed_coordination: "disabled"` runs plain lanes without the manager queue.

## 2. Prepare before ROOT starts

For a fresh Codex ROOT, the operator prepares the configured run copy before opening ROOT's
Codex session. On Windows, the read-only sandbox gate requires real operator health evidence
for the exact CLI/home/process-local LOCALAPPDATA cache. Follow
[the integration guide](docs/INTEGRATED_RUN_FIXES.md); no PASS receipt ships with the harness.

```powershell
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <configured-manager-workspace> --run-id <id>
```

`--workspace` must equal the configured `manager_workspace`. This calls native setup once, verifies
hooks/monitor and records a matching receipt under that workspace's `.agent-workspace/`. The usage
collector requires it before a fresh harness ROOT launch. ROOT continues that prepared runtime.
An existing runtime without the matching receipt is refused; setup after ROOT starts cannot
install hooks into that already-running session. Optional public-check policy must be installed
before preparation so the operator-owned service is ready before any worker starts.

For other providers or explicit manual integration, the native setup command remains:

```powershell
python -m orchestrator_harness.operator_launch harness setup
```

Setup is idempotent; use `--overwrite` only for a separately diagnosed integration change.
Each invocation gets one attempt; never retry a failed setup or refresh a live session. Existing Codex
and Claude configuration is preserved, with harness hooks merged into the provider hook files. An
existing `.codex/config.toml` must already set `[features] hooks = true`. Setup starts no lane or
provider.

## 3. Bootstrap one lane

Create a task card using `examples/project-task-card.example.json` as the template. Every
`project-task-card/v1` card requires non-empty `task`, `acceptance_criteria`, `deliverables`, and
`reason_for_acceptance_and_deliverables` fields. The criteria and deliverables are non-empty arrays
of non-empty strings; the reason is a non-empty string. `branch` and `base_commit` remain optional.

```json
{
  "schema": "project-task-card/v1",
  "task": "Implement the assigned change.",
  "acceptance_criteria": ["The focused regression tests pass."],
  "deliverables": ["Implementation, tests, and documentation."],
  "reason_for_acceptance_and_deliverables": "Together these prove the requested behavior is complete.",
  "branch": "lane/example",
  "base_commit": "HEAD"
}
```

```powershell
python -m orchestrator_harness.operator_launch lane bootstrap `
  --lane-id lane-01 --provider codex --model <model> `
  --provider-option reasoning_effort=<effort> `
  --provider-option service_tier=<tier> `
  --task-card <path-to-task-card.json>
```

`--task-card` names that file. Invalid or legacy v1 cards missing any required field are rejected;
add valid values and retry. Add `--exclusive-resource <id>` for each resource declared in the
manifest. Bootstrap creates the worktree, stages the super-cache base and the selected provider
payload, and writes the worker binding. Shipped provider IDs: `codex`, `claude-code`, `qwen-code`.

Launch preferences have no harness defaults. Codex requires `reasoning_effort` and `service_tier`;
Claude Code requires `effort`; Qwen Code currently requires only `--model`. Each option uses a
repeated `--provider-option NAME=VALUE`. Bootstrap rejects missing, duplicate, or unsupported
preferences before creating the lane, and resume reuses the recorded values unchanged.

Codex also accepts `--provider-option permission_mode=yolo` for explicitly requested
approval/sandbox bypass. The adapter passes `--yolo` and omits worker-isolated and
Windows sandbox overrides. Without this option the existing worker-isolated mode
remains in use; `permission_mode=worker-isolated` selects it explicitly. Claude Code
uses `--permission-mode bypassPermissions`, equivalent to its permission-bypass CLI
flag. Permission bypass does not change the assigned repository or task ownership.

For the native Codex OSS transport, add `--provider-option launcher=ollama` and provide the
operator-owned `local-config/ollama-model-catalog.json` with exactly one entry for the exact
`--model` slug advertising the configured `reasoning_effort`. The Ollama launcher passes
`--oss --local-provider ollama` and disables the provider's built-in web search, whose 0.40.2 path
can corrupt the replayed conversation; built-in web search is unavailable in this mode. The
configured model/effort, hooks, isolation and approval policy are unchanged, and nothing switches
providers automatically.

## 4. Launch

```powershell
python -m orchestrator_harness.operator_launch lane launch --lane-id lane-01
```

Inside the lane the worker edits only its own isolated worktree, commits the configured project
inputs, requests a configured check when public-check policy is installed, and writes `RESULT.json`
for the current committed tip. A stale, non-tip or dirty result is not completion, and launches or
checks are never retried automatically.

## 5. Wait and acknowledge (managed)

```powershell
python -m orchestrator_harness.operator_launch watch --until-actionable --timeout 5m
python -m orchestrator_harness.operator_launch manager acknowledge --event-id <top-level-event-id>
```

Acknowledge only the envelope's top-level `event_id` after handling the event; `data.signal_id`
is not an acknowledgement ID. An event already delivered and acknowledged for this ROOT session is
not redelivered: after handling it, run `watch` again to block for new work. Close handled events
with:

```powershell
python -m orchestrator_harness.operator_launch manager close --event-id <id> --outcome COMPLETE --summary <text>
```

## 6. Review and accept

The worker writes `RESULT.json` for the current committed tip at the worktree root (`result/v1`).
ROOT records the factual finding and the separate accept/reject decision. Select the acknowledged
review event with `--event-id` in managed mode (plain mode uses `--lane-id`):

```powershell
python -m orchestrator_harness.operator_launch lane completion-review `
  --event-id <review-event-id> --review-outcome PASS --approval ACCEPTED --review-summary <summary>
```

`ACCEPTED` requires a `PASS` finding; `--force-accept` cannot override that. Review returns the
acceptance reference used to retire the lane.

## 7. Retire

```powershell
python -m orchestrator_harness.operator_launch lane retire --acceptance-ref <acceptance-ref>
```

## Resume, force-stop, shutdown

- Resume a stopped, unaccepted lane with a fresh complete task card and an explicit rationale:
  `resume-lane --lane-id lane-01 --resume-task-card <fresh-card.json> --rationale <reason>`,
  then `lane launch --lane-id lane-01`. It reuses the retained worktree, session and history. The
  allowance is the initial assignment plus at most one explicit manual retry per lane/session,
  across run IDs; there is no automatic retry or model switch. A retired lane in the current epoch
  refuses resume and keeps its records; follow the returned next action.
- Cancel a prepared or running stuck lane: `lane force-stop --lane-id lane-01`. It stops only the
  exact recorded PID-plus-creation identities and refuses an unknown identity.
- End the whole runtime: `harness shutdown`. Managed ROOT Stop rejects OPEN, including an empty
  queue with a live worker. CLOSED is published only for proven cleanup, not for task success.

## Follow the run

```powershell
.\tools\Watch-MonitorLog.ps1
```

The viewer follows `<runtime>\monitor\MONITOR_IMPORTANT.log`, which contains
worker prompts, useful worker messages, queue admissions, lane outcomes, and
failures. Routine scans and full provider events go to `MONITOR_DETAIL.log`.
For the full trace, run:

```powershell
.\tools\Watch-DetailedMonitorLog.ps1 -RuntimeRoot <runtime> -Tail 100 -All
```

Use `-NoWait` to print once and `-Raw` for the JSONL records. Credential-shaped
values are redacted in the views; task and prompt text remains visible.

## Notes

- Run the harness from a full installed layout (module wheel plus the sibling product trees and
  operator configuration), not from a bare wheel install; see `README.md`.
- Run `scan --no-write` before launch for a read-only lane-status snapshot.
- `send-lane-notification --lane-id <id> --prompt <assignment>` appends one assignment to a
  running managed lane.
- `orchestrator_harness.release_checks` is the one stable-ID registry/selector for fast,
  affected, full, and release checks; credit requires exact source root, Git common directory,
  and branch identity.
  Use `registry --root <repository>` for executable source-bound registry commands. The aggregate
  safeguard receives the exact root, expected branch and tip; plain `registry` displays its template.
- Static examples and fixtures are never live provider proof; only a real recorded live run with
  exact identity evidence can claim provider proof.
- Stop and cleanup use exact recorded PID-plus-creation identity only; never kill by broad process
  name or command matching.
- Use a fresh epoch and fresh runtime directories for every live run; retire lanes or shut down
  before re-running.
