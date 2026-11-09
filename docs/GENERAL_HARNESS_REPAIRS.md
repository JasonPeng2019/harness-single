# General harness repairs and portable operation

This guide describes the R1–R7 protections and F1–F3/R8 behavior. It is an operating guide, not
evidence of a live provider run. Keep prior completion reports as historical evidence; tests and
demos qualify only the source and installation they actually exercised.

## Repair summary

- **R1/R2 — Claude hooks and upgrade migration.** ROOT and worker hook commands quote
  `${CLAUDE_PROJECT_DIR}`. Setup migrates only exact known legacy hook groups, deduplicates
  old-plus-current registrations, and preserves unrelated provider settings.
- **R3 — inherited Codex profiles.** Bootstrap regenerates recognized generated profile suffixes
  for the current worktree and Git identity, while preserving user configuration and refusing
  altered owned content.
- **R4 — private Windows worker temporary files.** Restricted Codex workers receive private
  per-invocation TEMP/TMP directories. This mitigates shared-temp traversal; it does not qualify
  every Windows sandbox profile.
- **R5/R6 — finalization and accounting integrity.** A live controller retains finalization
  authority over provisional results; usage accounting rejects duplicate same-run controller-start
  evidence. Neither behavior starts a retry or second provider.
- **R7 — ROOT mode admission.** Codex preparation, launch and receipts bind the selected ROOT mode.
  Explicit `danger-full-access` skips readiness for the unused OS sandbox but preserves existing
  run-wide failure blocks and ownership checks. Full access has no OS sandbox enforcement.
- **F1 — optional worker checkout scope.** Optional `checkout_paths` selects literal repository-
  relative directories for new worktrees. Omission keeps a full checkout. Git cone-mode sparse
  checkout preserves the assigned base commit and history, may include top-level/ancestor files,
  and is not an access boundary. Bootstrap refuses when selected paths omit required tracked inputs;
  it never silently expands the requested scope.
- **F2 — explicit resume profile changes.** Ordinary resume keeps the saved session and effective
  profile. A complete explicit changed profile validates before mutation and starts a fresh native
  session in the same provider adapter and worktree. On Codex resume, omission of `permission_mode`
  preserves the lane's effective mode, including legacy strict; a permission change must be explicit. Dirty files, commits, assignment, protected
  history, retry counts and resource accounting remain. Conversation history is not transferred;
  ROOT's model/provider is outside this feature. See [resume profiles](RESUME_PROFILES.md).
- **F3 — provider-aware usage accounting.** `scripts/collect_usage.py` provides provider-aware
  collection and reporting while preserving the existing Codex collector interface. Claude usage
  uses the top-level terminal `modelUsage` rollup; comparison total is uncached input plus output,
  cache read/create values remain separate, and missing values remain unknown. Resume deltas are
  keyed by provider and native session. Reports keep per-provider totals and identify mixed metric
  scope. Fixtures cover parsing and accounting; they do not qualify the actual Claude CLI, which
  requires a successful authenticated live run in the intended installation.
- **R8 — normal Windows worker mode.** A fresh Windows Codex bootstrap with omitted worker mode
  records `worker-workspace`, which keeps write restrictions and narrow Git grants without the
  broad sibling/operator read-denial list. Explicit `worker-isolated` retains strict denials;
  explicit `yolo` is full access. Non-Windows defaults are unchanged. Missing mode in a historical
  saved invocation retains strict meaning. ROOT mode is independent. `worker-workspace` gives no
  read-confidentiality guarantee and does not establish protection from files still readable by
  native/OS permissions.

## Portable operating outline

Use a disposable harness copy and a small project unrelated to the harness or reference material.
Configure only explicit, portable placeholders; shipped files do not select a model or developer
workspace.

1. Configure `harness-config.json` with the project's absolute `root_workspace`, optional
   `manager_workspace`, and `managed_coordination`. Optionally add
   `"checkout_paths": ["src", "docs/operator guides"]` to materialize selected worker directories;
   omit it for full checkout. Configure the resource manifest and a complete
   `project-task-card/v1` card. Read [Quick Start](../QUICK_START.md) for full setup details.
2. Select the ROOT provider and prepare before ROOT starts. The explicit full-access ROOT example
   is:

   ```powershell
   python -B scripts/prepare_root_launch.py --harness-dir . --workspace <manager-workspace> --run-id <run-id> --sandbox danger-full-access
   ```

   This is an operating compromise: ROOT commands have the current user's access, without OS
   sandbox enforcement. Launch Codex ROOT through `scripts/collect_codex_usage.py run` with the
   same `--sandbox danger-full-access`. `worker-workspace` and ROOT full access are independent
   selections. Other ROOT providers use their provider-aware collection route and do not take
   Codex `--sandbox`.
3. On Windows, qualify `worker-workspace` against the installed harness with the one-attempt
   native probe and check its mode-specific receipt before relying on that mode:

   ```powershell
   python -B scripts/probe_windows_worker_isolation.py --harness-dir . --output-dir runtime/windows-worker-probe/<run-id> --local-appdata runtime/windows-worker-probe-cache/<run-id>
   python -B scripts/check_windows_sandbox.py --harness-dir . --workspace <root-workspace> --permission-mode worker-workspace
   ```

   The probe is mode-specific and does not qualify strict read isolation. Each installation and
   operator must earn a mode-specific probe receipt and qualify the intended concurrent-project and
   public-check workflows before relying on reduced mode. These instructions are not evidence that
   a probe or workflow has passed.
4. Bootstrap a lane with an explicit provider, model, and complete required provider options.
   Fresh Windows Codex bootstrap omission selects and records `worker-workspace`. Select strict
   `worker-isolated` or full-access `yolo` only when the operator intends that mode. Resume omission
   preserves its existing effective mode; changing it requires an explicit `permission_mode` option. Non-Windows behavior is unchanged.
   For a model/profile transition, use `resume-lane` with `--model` and the complete option set;
   omitted overrides continue the saved session. Resume starts no provider until `lane launch`.
5. In managed mode, wait through native `watch`, handle events and acknowledge the top-level event
   ID only after handling. Review the current result, retire accepted lanes, then run native
   `harness shutdown` and confirm CLOSED cleanup evidence.

For provider-aware usage, use `scripts/collect_usage.py` for Claude `run`/`resume`, and its
`snapshot`, `gate`, `finalize`, or `summarize` operations as needed. The provider identity comes
from the ROOT launch and protected worker invocation records; collection reads native evidence and
does not resume or modify a session. Claude collection requires the CLI/auth context configured by
the operator. Fixtures do not qualify the actual Claude CLI; that requires a successful authenticated
live run in the intended installation.

## Evidence boundary

Native `watch --until-actionable` remains the manager work-discovery path. This extension does not
claim a repair for the historical Stop-hook/compaction timeout; that cause remains unresolved.

The probe script and usage collectors produce evidence only when actually run. Each installation
and operator must qualify the selected Windows worker mode and intended concurrent-project/public-
check workflow; strict read isolation requires separate qualification. Historical R1–R7 results
remain scoped to their recorded source revision and do not qualify newer features.
