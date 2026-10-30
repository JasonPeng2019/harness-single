# Portable Coding Orchestrator Harness (v2)

The Harness v2 coordinates ordinary software work across Git branches and worktrees. One public
launcher exposes the entire operator surface:

```powershell
python -m orchestrator_harness.operator_launch [--json] {harness,lane,resume-lane,manager,lease,send-lane-notification,scan,watch,health}
```

Every public command returns the small structured result `{ ok, code, summary, evidence_paths,
next_action }`; `--json` emits it as the machine-readable result object. Success prints a short
status line and exits 0; failure prints one stable failure code on stderr and exits non-zero.

Configure `harness-config.json` before use. The closed `harness-config/v1` shape has a
required absolute `root_workspace` (the Git product and runtime workspace) and two optional
keys: `manager_workspace` (an absolute workspace for ROOT hooks, skills and `.agent-workspace`;
defaults to `root_workspace`) and `managed_coordination` (`enabled` or `disabled`; default
`enabled`). The shipped configuration has an empty `root_workspace`; commands refuse it until
the operator supplies an absolute workspace path. The harness never selects a developer's
workspace and does not install or query an experience memory store.

See [portable path configuration](docs/PATH_PORTABILITY.md) for tool discovery,
unconfigured examples and the explicit WSL isolation mount inputs.

See [the integrated run fixes](docs/INTEGRATED_RUN_FIXES.md) for Codex ROOT prelaunch
preparation, the mandatory Windows sandbox health gate, the optional direct worker public
build/test service, and remaining task-completion work.

## Installation layout

A full source checkout already is the full layout: configure `harness-config.json` and
`resource-manifest.json` there and run pre-launch preparation. A wheel installation is
module-only, so a `--target` install needs the sibling trees copied in from the same source:

1. Use a fixed full source checkout/copy and a Python >=3.11 installation that already provides
   pip, setuptools >= 68 and wheel.
2. Build the module wheel from that source:
   `python -B -m pip wheel --no-deps --no-build-isolation --wheel-dir <wheel-dir> <source>/orchestrator_harness`
3. Install it into a new harness root target:
   `python -B -m pip install --no-deps --no-index --no-compile --target <new-harness-root> <actual-wheel>`
4. From the same source, copy `adapters/`, `super-cache/`, `scripts/`, `tools/`,
   `harness_watcher_implementation/`, `docs/`, `examples/`, the top-level operating files and
   `.gitignore` into the target, preserving the pip-installed modules. Test material is
   validation-only and is not part of the module wheel.
5. Create explicit operator `harness-config.json` and `resource-manifest.json` in the target and
   run pre-launch preparation first.

Installing the product does not by itself prove a live provider or sandbox run.

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
  ROOT's review and acceptance; the managed `--event-id` selector requires the acknowledged
  review event and closes it. ACCEPTED requires a PASS finding.
- `lane force-stop --lane-id` — hard-stop one stuck lane (prepared or running) by exact
  recorded PID-plus-creation identity and release its leases; an unobservable or conflicting
  identity refuses instead of guessing. `lane retire --acceptance-ref` — gracefully retire one
  accepted lane.
- `resume-lane --lane-id --resume-task-card --rationale` — re-run a stopped or otherwise
  resumable unaccepted lane in its retained worktree/session with a fresh complete task card and an
  explicit rationale; a lane retired in the current epoch refuses resume while supported retained
  stopped lanes across closed epochs remain resumable, and there is no automatic retry loop.
- `manager acknowledge --event-id <top-level-event-id>` — move one event PENDING -> ACKNOWLEDGED; `manager close
  --event-id --outcome {COMPLETE,BLOCKED} --summary <text>` — close an acknowledged event (managed only).
- `lease force-release --resource-id <id>` — recover one declared orphaned resource lease. The
  command refuses an exact live holder and fails closed unless current process and lane/run evidence
  proves release is safe; follow its returned `next_action` instead of hand-editing lease records.
- `send-lane-notification --lane-id --prompt` — append one assignment to a running managed lane.
- `scan --no-write` — read-only lane-status snapshot; `watch --until-actionable [--timeout]
  [--until-event]` — block until an actionable condition exists. An event already delivered and
  acknowledged for this ROOT session is not redelivered; after handling it, watch again for new
  work.
- `health reconcile` — rebuild active-lanes and re-derive status.

## One-time ROOT sequence

1. Write `harness-config.json` at the harness root: the closed `harness-config/v1` shape with a
   required absolute `root_workspace`, optional absolute `manager_workspace` (ROOT hooks, skills
   and `.agent-workspace`; defaults to `root_workspace`), and optional `managed_coordination`
   (`enabled` or `disabled`; default `enabled`). No other keys are legal.
2. Write `resource-manifest.json`: the closed `resource-manifest/v1` shape with a literal `schema`
   field and a `resources` list. Every nonempty entry declares a nonempty `id` and
   `exclusive: true`; duplicate IDs and unknown fields are invalid.
3. For a fresh managed ROOT, run pre-launch preparation FIRST; it performs the one native
   integration internally:
   `python -B scripts/prepare_root_launch.py --harness-dir . --workspace <configured-manager-workspace> --run-id <id>`.
   Windows Codex needs the read-only sandbox health receipt before preparation; other ROOT
   providers are selected explicitly (`--provider claude-code` or `--provider qwen-code`) and the
   Codex health gate is never bypassed. Preparation validates the config and manifest, preflights
   the entire catalog before writing anything, stages and byte-verifies the active super-cache,
   installs the ROOT payloads, writes the active resource manifest and lease directory, starts the
   persistent monitor, and records the run-bound receipt. It starts no lane or provider and never
   creates an epoch or worktree.
4. Manual native integration remains available with
   `python -m orchestrator_harness.operator_launch harness setup` for plain/manual integration and
   for reopening an already prepared ROOT (`--overwrite` only for a separately diagnosed
   integration change). Reopening an already prepared ROOT revalidates the existing receipt
   instead of repeating setup. For a prepared CLOSED runtime, run native `harness setup` first,
   then pre-launch preparation with the SAME run ID: preparation revalidates the prior
   receipt/session/config/hooks and the exact stopped service owner, explicitly starts the enabled
   service with a fresh startup identity, and archives the old owner, config and receipt. A lane
   resume or launch never starts the service, and nothing retries automatically. Never delete or
   fabricate runtime records or receipts.

ROOT payloads (skills, hook scripts, binding files and the `.agent-workspace` helpers) are
installed into the configured `manager_workspace`. Setup preserves existing `.codex/` and `.claude/`
directories. It adds the harness-owned skills, hook scripts, and binding files, while structurally
merging harness hook registrations into an existing `.codex/hooks.json` or `.claude/settings.json`.
Existing fields, permissions, hook groups and unrelated files remain in place. An existing
`.codex/config.toml` is preserved byte-for-byte and must already set `[features] hooks = true`.
`--overwrite` replaces only harness-owned payload files; it does not replace these shared provider
configuration files.

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

Codex lanes can select the native OSS transport with `--provider-option launcher=ollama` (the
default is the ordinary Codex launcher). That launcher passes `--oss --local-provider ollama`,
requires the operator-owned `local-config/ollama-model-catalog.json` to contain exactly one entry
for the lane's exact model slug advertising the configured `reasoning_effort`, and passes that
catalog to the provider. Ollama 0.40.2's built-in web-search path can corrupt the replayed
conversation, so the Ollama launcher sets `web_search="disabled"`; built-in web search is
unavailable in this launcher. Hooks, worker isolation, approval policy and authentication handling
are unchanged: the launcher never bypasses the configured permissions and never falls back to
another provider. Provider staffing fallbacks are a ROOT planning decision, not harness behavior.

## Super-cache and bootstrap

`super-cache/workspace/` is the provider-neutral base staged into every managed worktree by
`lane bootstrap`: the shared `.agent-workspace/` skeleton plus the four provider-neutral helpers
(`result-stop-check.py`, `lane-queue.py`, `manager-notify.py`, `hook-dispatch.py`); plain mode
omits the manager-queue and notice helpers. Bootstrap also materializes the
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

Each launcher invokes its provider once. An invalid result or provider failure preserves
per-attempt transcript, stderr, usage and cleanup evidence as `provider_exited_no_result`;
it never automatically retries, resumes, repairs or switches providers. Diagnose the failure
before a separate launch. A lane/native session permits its initial assignment plus at most
one explicit manual retry, counted across run IDs in protected operator-only invocation history.
Worker-local attempt logs are diagnostic mirrors and cannot reset that allowance.

Managed ROOT Stop requires a CLOSED runtime even when its queue is empty or no epoch exists.
CLOSED proves cleanup; it does not prove the user's task was completed or independently validated.

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
Scoped fingerprints include tracked and untracked source, including ignored Python files that
discovery can import, while excluding owned runtime/build/cache output. They include consumed
provider and super-cache payloads. Registry version 2 uses whole-argument source placeholders for
the aggregate safeguard; `python -m orchestrator_harness.release_checks registry --root <repository>`
and selector output bind its exact repository, expected branch and tip. Plain `registry` marks
that command as requiring source binding. Pass argument arrays directly; do not evaluate the
rendered command as shell text.

## Documentation

- `QUICK_START.md` — shortest live run.
- `QUICK_RULES.md` — authority and safety rules.
- `orchestrator_harness/README.md` — command and contract reference.
- `orchestrator_harness/SPEC.md` — harness contracts.
- `docs/HARNESS_WATCHER_GUIDE.md` — optional diagnostic watcher.
- `docs/COLLECTOR_RECOVERY.md` — supported recovery for a sealed malformed or unmetered worker transcript blocking ROOT collector continuation, with safe shutdown, force-stop and retry steps.
- `adapters/README.md` — provider adapter catalog and binding contract.
