# Quick Start: Harness v2 Operator Run

The shortest live run: configure once, setup, bootstrap, launch, watch, review, retire. Start in
the `harness-single/` directory; it is the one Harness v2 product root and launch directory. All
commands use the one public launcher:

```powershell
python -m orchestrator_harness.operator_launch <group> <command> [options]
```

## 1. Create local configuration

Create the ignored `local-config/` directory. Copy
`examples/harness-config.example.json` to `local-config/harness-config.json`, then replace its
example path with the absolute path to the Git repository that ROOT will coordinate. The config
has the closed two-key `harness-config/v1` shape:

```json
{
  "root_workspace": "<absolute-path-to-root-workspace>",
  "managed_coordination": "enabled"
}
```

Copy `examples/resource-manifest.example.json` to
`local-config/resource-manifest.json` (closed `resource-manifest/v1` shape):

```json
{
  "schema": "resource-manifest/v1",
  "resources": []
}
```

Declare each exclusive resource as `{"id": "<name>", "exclusive": true}` in `resources`.
`managed_coordination: "disabled"` runs plain lanes without the manager queue.

For compatibility, an existing pair at the `harness-single/` root is still readable, but the
local pair takes precedence and the two directories are never mixed. The launcher stops
discovery at the product root, so a missing local config cannot select a stale parent config.

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
  --lane-id lane-01 --provider codex --model <model> --task-card <path-to-task-card.json>
```

`--task-card` names a `project-task-card/v1` file (task text, branch, base commit). Add
`--exclusive-resource <id>` for each resource declared in the manifest. Bootstrap creates the
worktree, stages the super-cache base and the selected provider payload, and writes the worker
binding. Shipped provider IDs: `codex`, `claude-code`, `qwen-code`.

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
- Run the hermetic **macOS product demonstration** without a provider account:
  `python -m unittest orchestrator_harness.tests.test_macos_product_smoke -v`. It uses only
  disposable repositories, sanitized ambient Git state, and a synthetic fake Codex executable;
  its evidence is not live-provider proof. This dedicated macOS demonstration skips on every
  non-macOS host rather than reporting a cross-platform pass.
