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
  "managed_coordination": "enabled",
  "checkout_paths": ["src", "docs/operator guides"]
}
```

`root_workspace` selects the Git product workspace; `manager_workspace` is optional and selects
where ROOT hooks, skills and `.agent-workspace` are installed (defaults to `root_workspace`).
Optional `checkout_paths` selects literal repository-relative directories to materialize in new
worker worktrees. Omit it for a full checkout. Sparse checkout is not an access-control boundary;
bootstrap refuses if selected paths omit required tracked inputs.

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

For a fresh managed ROOT, run pre-launch preparation FIRST: it performs the one native setup
internally and records the run-bound receipt. The explicit working recipe below uses full-access Codex ROOT. This is an operating compromise:
Codex ROOT commands have the current user's access, without OS sandbox enforcement. Worker
permissions are selected independently in step 3; the normal recipe does not select yolo. Native
worktree ownership, hooks, review and cleanup still apply.

```powershell
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <configured-manager-workspace> --run-id <id> --sandbox danger-full-access
```

`--workspace` must equal the configured `manager_workspace`. Select the ROOT provider explicitly:
the default is `--provider codex`, and `--provider claude-code` or `--provider qwen-code` are
equally valid selections (omit `--sandbox` for those providers). The
usage collector and a fresh ROOT launch expect the matching receipt under that workspace's
`.agent-workspace/`. An existing runtime without it is refused, and setup after ROOT starts cannot
install hooks into that already-running session. Optional public-check policy must be installed
before preparation so the operator-owned service is ready before any worker starts.

Launch Codex ROOT with the same selected mode; for example:

```powershell
python -B scripts/collect_codex_usage.py run `
  --run-id <id> --task <task-name> --arm harness `
  --cwd <configured-manager-workspace> --prompt-file <root-prompt.txt> `
  --results-dir <owned-results-directory> --harness-dir . `
  --harness-runtime <configured-product-workspace>/.harness-runtime `
  --model <model> --reasoning-effort <effort> --service-tier <tier> `
  --sandbox danger-full-access --watchdog-hours <hours> --trust-project-hooks
```

For an isolated ROOT instead, use `--sandbox workspace-write` or `--sandbox read-only` in both
preparation and launch. Omission still defaults to `workspace-write`; the harness never falls
back to full access. On Windows these isolated modes require genuine health proof for the exact
CLI/home/process-local LOCALAPPDATA cache. Follow [the integration guide](docs/INTEGRATED_RUN_FIXES.md);
no PASS receipt ships with the harness. Preparation binds the selected mode, and launch/resume
refuses a different mode. An existing run-wide sandbox-failure block also refuses full access;
changing permissions is not recovery for an already failed run.

Manual native integration remains available:

```powershell
python -m orchestrator_harness.operator_launch harness setup
```

It is for plain/manual integration and for reopening an already prepared ROOT. Reopening an
already prepared ROOT revalidates the existing receipt instead of repeating setup. For a
prepared CLOSED runtime, run native `harness setup` first, then pre-launch preparation with the
SAME run ID and ROOT sandbox mode: preparation revalidates the prior receipt/session/config/hooks and the exact stopped
service owner, explicitly starts the enabled service with a fresh startup identity, and archives
the old owner/config/receipt. A lane resume or launch never starts the service, and nothing
retries automatically.
Use `--overwrite` only for a separately diagnosed integration change, give each invocation one
attempt, and never retry a failed setup or refresh a live session. Existing Codex and Claude
configuration is preserved, with harness hooks merged into the provider hook files. An existing
`.codex/config.toml` must already set `[features] hooks = true`. Setup starts no lane or provider,
and runtime records or receipts are never deleted or fabricated.

### Claude Code ROOT usage collection

For a fresh Claude Code ROOT, prepare with `--provider claude-code` and collect through the
provider-aware route:

```powershell
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <configured-manager-workspace> --run-id <id> --provider claude-code
python -B scripts/collect_usage.py run --provider claude-code --run-id <id> --task <task> --arm harness --cwd <configured-manager-workspace> --prompt-file <root-prompt.txt> --results-dir <owned-results-directory> --model <model> --effort <effort> --harness-runtime <configured-product-workspace>/.harness-runtime --harness-dir . --trust-project-hooks --watchdog-hours <hours>
```

Use `collect_usage.py resume --provider claude-code --task-file <root-run-dir>/task.json
--prompt-file <prompt>` for an existing saved ROOT session. `snapshot`, `gate`, `finalize`, and
`summarize` are report operations; finalization writes an owned output archive. Existing Codex
`collect_codex_usage.py` run/resume commands remain compatible; `collect_usage.py run/resume`
also accepts their legacy Codex options.

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
preferences before creating the lane. On resume, restate every required model option. An omitted
Codex `permission_mode` preserves the effective existing mode; changing it requires the explicit
option. An unchanged effective profile keeps the saved session.

For Codex workers, a fresh Windows bootstrap with no permission option records
`permission_mode=worker-workspace`, which keeps the write sandbox and narrow Git grants without
the broad sibling/operator read-denial list. This mode does not promise read confidentiality. Strict read isolation requires separate qualification.
Fresh non-Windows workers keep the existing default. Select strict Windows read isolation
explicitly with `--provider-option permission_mode=worker-isolated`; select full access explicitly
with `--provider-option permission_mode=yolo`. Yolo passes `--yolo` without OS sandbox
enforcement and is not the normal worker recipe. Historical invocations with no recorded mode
retain strict meaning. ROOT and worker modes are independent. Claude Code uses
`--permission-mode bypassPermissions`; permission bypass does not change repository/task ownership.

For the native Codex OSS transport, add `--provider-option launcher=ollama` and provide the
operator-owned `local-config/ollama-model-catalog.json` with exactly one entry for the exact
`--model` slug advertising the configured `reasoning_effort`. The Ollama launcher passes
`--oss --local-provider ollama` and disables the provider's built-in web search, whose 0.40.2 path
can corrupt the replayed conversation; built-in web search is unavailable in this mode. The
configured model/effort, hooks, isolation and approval policy are unchanged, and nothing switches
providers automatically.

### One-attempt Windows worker qualification

Before claiming Windows worker operation, run one owned native probe against the installed
harness. It creates a temporary two-worktree fixture, checks lane writes and commits, attempts
writes to protected sentinels, records profile/CLI/config hashes and exact cleanup, and publishes a
mode-specific health receipt only when the observations pass. The strict `worker-isolated` probe
also attempts a read of a non-secret sentinel inside the actual active CODEX_HOME and runs two
separately generated strict profiles concurrently, each with its own cache and TEMP directory.
Strict qualification requires both concurrent worktrees to write and commit, the home reads and
protected writes to be denied, and exact cleanup for both native CLI process boundaries. It does not
configure or repair the sandbox. Reduced-mode proof does not qualify strict mode; absent or stale
receipts do not qualify a mode. Each installation and operator must qualify the intended workflow
for that mode before relying on it.

```powershell
python -B scripts/probe_windows_worker_isolation.py --harness-dir . `
  --output-dir runtime/windows-worker-probe/<run-id> `
  --local-appdata runtime/windows-worker-probe-cache/<run-id> `
  --permission-mode worker-workspace
# For strict isolation, use a fresh evidence/cache path and an explicit strict mode:
python -B scripts/probe_windows_worker_isolation.py --harness-dir . `
  --output-dir runtime/windows-worker-probe-strict/<run-id> `
  --local-appdata runtime/windows-worker-probe-strict-cache/<run-id> `
  --permission-mode worker-isolated
python -B scripts/check_windows_sandbox.py --harness-dir . `
  --workspace <absolute-root-workspace> --permission-mode worker-workspace
```

This writes `local-config/windows-sandbox-health-<permission-mode>.json`; the legacy ROOT receipt
`local-config/windows-sandbox-health.json` is separate. A prior worker receipt is preserved; pass
`--replace-existing-health` only when intentionally replacing it after retaining the prior evidence.
A strict receipt requires actual active-home read denial and successful concurrent two-profile
native operation. The probe does not establish public-check workflow qualification; validate that
workflow for the installation and mode you intend to use.

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

- Resume a stopped, unaccepted lane with a fresh complete task card and rationale:
  `resume-lane --lane-id lane-01 --resume-task-card <fresh-card.json> --rationale <reason>`, then
  `lane launch --lane-id lane-01`. Without profile overrides this keeps the saved provider session.
  To replace its model/profile, supply `--model <model>` and the complete provider option set; this
  starts a fresh provider session in the same worktree and preserves dirty files, commits, protected
  history, and retry/resource counts. The lane still permits only its initial attempt plus one explicit
  manual retry across run IDs. A retired lane in the current epoch refuses resume and keeps its records.
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
