# Quick Start: Ordinary Coding

## 1. Prepare lanes

Start from a stable committed base. Create one branch and worktree for each concurrent task:

```powershell
git worktree add -b lane/parser ..\project-worktrees\parser <base-commit>
git worktree add -b lane/api ..\project-worktrees\api <base-commit>
```

Do not switch branches in an active lane worktree. A later split starts only after the parent lane
commits; every child branches from that exact commit.

### Root-restricted campaign bootstrap

When the campaign coordinator is prohibited from entering the experiment tree, it must not create
the worktree, task card, invocation, or overlay receipt itself. Write the bootstrap manifest and its
result/launch receipts outside that tree, then use the public operator boundary to run the one-shot
preparation process:

```powershell
python -m orchestrator_harness.operator_launch `
  --receipt <runtime>/bootstrap-operator-launch.json `
  --label coding-lane-bootstrap `
  --role coding-lane-bootstrap `
  --cwd <harness-root> `
  -- python -m orchestrator_harness.lane_bootstrap <outside-fresh-manifest.json> `
     --result <runtime>/bootstrap-result.json
```

The manifest identifies the source Git root/base/branch, `experiment_root`, canonical mapping plus
workflow role, lane ID, worktree name, task card, resource claims, prompt, non-provider launch
options, and runtime root. The
bootstrap process creates only the declared linked worktree beneath
`experiment_root/worktrees/`, its completed overlay receipt, dispatch artifacts, and controller
invocation. It does not launch a provider, controller, MCP server, or hardware action. Read the
result outside the experiment tree, admit that exact invocation, then use the normal public
operator/controller launch below.

For a board-free `ROLE_CHECKER` readiness lane only, a manifest may include an explicit untracked
source candidate:

```json
"source_snapshot": {
  "source_root": "C:/source-repository/Firmware/app",
  "allowed_root": "C:/source-repository",
  "exclude_paths": ["build"]
}
```

`source_root` must be below `allowed_root` (which defaults to `source_repository_root`); each
exclusion is a relative descendant path. Use an explicit `allowed_root` when a committed lane base
and an untracked candidate belong to different repositories. Bootstrap copies that exact content
only to `.agent-workspace/source-snapshot`, writes a complete file-hash manifest beside it, and
returns its digest, file count, and byte count in the outside-tree receipt. The original checkout is
never staged, committed, or changed. This is read-only readiness evidence, not a committed candidate
and not authorization for a build, source edit, flash, MCP, or hardware action.

If bootstrap fails after creating a worktree, do not remove it directly. Use the same outside-tree
manifest through the public operator boundary with `--cleanup` and a new result path. The cleanup
mode reads that exact worktree's porcelain status and removes it only when there are no tracked or
untracked changes:

```powershell
python -m orchestrator_harness.operator_launch `
  --receipt <runtime>/cleanup-operator-launch.json `
  --label coding-lane-bootstrap-cleanup `
  --role coding-lane-bootstrap-cleanup `
  --cwd <harness-root> `
  -- python -m orchestrator_harness.lane_bootstrap <outside-fresh-manifest.json> `
     --cleanup --result <runtime>/cleanup-result.json
```

When ROOT cannot read the experiment tree, export the controller status and worker result through
the same public boundary. The exporter refuses an output beneath that tree:

```powershell
python -m orchestrator_harness.operator_launch `
  --receipt <runtime>/status-export-operator-launch.json `
  --label lane-status-export `
  --role lane-status-export `
  --cwd <harness-root> `
  -- python -m orchestrator_harness.lane_status_export <lane-invocation.json> `
     --result <runtime>/status-export.json
```

Use `LANE-EXEC-*` only for `ROLE_EXECUTOR`; a non-executor role uses its own stable lane ID and
resolves directly through the canonical mapping.

## 2. Configure discovery

```powershell
New-Item -ItemType Directory -Force local-config | Out-Null
Copy-Item examples/harness.example.json local-config/harness.json
```

Set `suite_root` to the directory containing `worktrees/`, keep `run_globs` as `worktrees/*`, and
choose a fresh ignored `runtime/orchestrator-harness/<epoch>` output. Validate before launch:

```powershell
python -m orchestrator_harness --config local-config/harness.json scan --no-write
```

## 3. Launch lane controllers

For each lane, copy the shape in `examples/coding.invocation.example.json`, replace every path and
Git identity, write the prompt beneath that lane's `.agent-workspace`, and calculate its SHA-256.
Use `exclusive_resources` only for non-Git resources that cannot be shared concurrently.

```powershell
python -m orchestrator_harness.lane_controller <lane-invocation.json>
```

The manager may use `orchestrator_harness.operator_launch` for a detached, identity-recorded
controller. The harness does not schedule controllers automatically.

For the copyable public launch shape, see `examples/public-coding-launch.example.md`. It composes
the operator launcher and native lane controller; the controller remains the owner of provider
lifecycle, claims, durable events, semantic resume, result validation, and cleanup.

## 4. Wait and acknowledge

```powershell
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60
```

Handle one returned event, verify the corresponding action or durable record, then acknowledge the
top-level `event_id` exactly:

```python
router.acknowledge("<top-level-event-id>", binding=router.registration)
```

The acknowledgement is a manager action through the bound S3 `ManagerEventRouter`; this
diagnostic CLI has no competing acknowledgement queue. `data.signal_id` is not an acknowledgement
ID.

Repeat after timeouts. Never poll worker transcripts or add a relay as an alternate discovery path.

## 5. Finish each lane

Workers may update `.agent-workspace/PARALLEL_CHECKPOINT.md` during progress. Completion requires a
clean committed branch plus `.agent-workspace/RESULT.json` matching
`examples/coding.result.example.json`. The commit must be the current branch tip.
The terminal result must be written exactly at `.agent-workspace/RESULT.json`; never write a bare
`RESULT.json` at a worktree or experiment root. Start from the workspace-local
`.agent-workspace/RESULT_TEMPLATE.json` when the runner bootstrap provides it.
Terminal outcomes are `PASS`, `FAIL`, or `BLOCKED`; individual check outcomes are `PASS`, `FAIL`,
`SKIP`, or `NOT_RUN`. Explain incomplete or indeterminate evidence in the relevant summary.

Normal named-lock contention waits automatically. Investigate only malformed, stale, unknown, or
excessive-wait evidence. Never delete another invocation's claim.

### Select release checks

The versioned registry/selector is the single owner of fast, affected, full, and release checks:

```powershell
python -m orchestrator_harness.release_checks select --intent fast --root <repository-root>
python -m orchestrator_harness.release_checks select --intent affected --root <repository-root> `
  --changed-domain public-launch
```

Credit is retained only for the same stable ID, exact declared dependency fingerprint, command,
tier, output contract, and exact source root/common-directory/branch identity; its recorded origin
tip must be an ancestor of the current tip. A dependency or contract mutation invalidates that
credit. Fast is local and does not select WSL or a real agent. Full/release may enumerate the
accumulated component checks, but the aggregate safeguard remains ROOT-owned and is not run in an ordinary coding lane.

## 6. Merge and accept

```powershell
git worktree add -b integration/candidate ..\project-worktrees\candidate <base-commit>
git -C ..\project-worktrees\candidate merge --no-edit lane/parser
git -C ..\project-worktrees\candidate merge --no-edit lane/api
python -m unittest discover -s ..\project-worktrees\candidate -v
```

Use a manager-assigned merge worker when conflict resolution or integration work is needed. Its
invocation identifies the integration branch and merge inputs. Publish and validate its result,
then treat that commit as the candidate. Run acceptance before promoting it over the frozen
known-good revision.

## 7. Clean up

Stop managed observation cooperatively if active, prove exact process identities absent, confirm
all named claims released, preserve results, then remove disposable worktrees with `git worktree
remove`. Never remove a dirty or unmerged lane without an explicit manager decision.

Run the complete local example at any time:

```powershell
python examples/disposable_coding_fixture.py
```

The legacy policy-bound firmware path remains supported but is not part of this quick start.

## Optional firmware seam and dual-path operation

The legacy policy-bound firmware path remains supported but is not part of this quick start. For an
existing policy-bound firmware lane, start from `examples/legacy-firmware.invocation.example.json`;
it intentionally has no `schema` field and no coding V1 Git/runtime/resource fields. Do not migrate
a schema-less firmware fixture just to share an observer epoch.

`examples/dual-path-manager.example.md` is the copyable native-manager sequence for one coding and
one legacy firmware lane. It uses a fresh config/runtime, `scan --no-write`, and one native
`watch --until-actionable` call. Its acknowledgement is the returned envelope's top-level
`event_id`, never `data.signal_id`.

The optional firmware seam is caller-declared and installed with the package. A caller builds a
`FirmwareCampaignPack` from a declared capability name, `FirmwareAction` declarations (MCP tool
name, positive method version, positive maximum duration, exact required argument names), canonical
resource identities, and a nonempty public policy binding; there is no default pack or built-in
fixture. The broker-compatible `FirmwareHardwareAdapter` takes that pack plus caller-supplied
snapshot, launch/configuration, child-identity, and transport seams, reuses `ProcessBoundary` and
`ProcessSupervisor`, and sends exact MCP traffic (`initialize` with the caller-declared protocol
version, `notifications/initialized`, then `tools/call`). Permit-expiry checks precede launch,
enqueue, and dispatch; public results never leak private launch/configuration data; and cleanup
retains the claim until both the owned process boundary and transport are proven closed. The
pack/adapter plus the existing `CapabilityBroker` is the complete optional firmware seam; it is not
connected to the lane controller and introduces no controller or runtime protocol.

Resume a coding controller only with its persisted identity and output paths; the controller, not a
new wrapper, verifies that resume identity:

```powershell
python -m orchestrator_harness.lane_controller C:/absolute/path/to/coding.resume.invocation.json
```

That resume invocation keeps the same `worker_invocation_id` and supplies the recorded thread as
`resume_thread_id` or `resume_identity.thread_id`. Legacy firmware resumes under its retained
policy-bound contract.

Before disposal, let each bounded diagnostic wait return and request cooperative stop from the
recorded owner. Prove absence using every recorded PID plus creation time from its status/launch
record. Do not kill by process name or command text:

```powershell
# For each recorded { pid, creation_time_utc }, prove that the exact identity is absent (or stop only
# that exact still-live identity cooperatively and re-check it).
$process = Get-CimInstance Win32_Process -Filter "ProcessId = <recorded-pid>" -ErrorAction SilentlyContinue
if ($process) { [Management.ManagementDateTimeConverter]::ToDateTime($process.CreationDate).ToUniversalTime().ToString('o') }
git -C C:/absolute/path/to/worktree status --porcelain
git worktree remove C:/absolute/path/to/worktree
```

Treat a missing process as absence only after the recorded PID-plus-creation identity was checked;
a reused PID is a different process and must never be stopped. Confirm named claims and pending
events in the epoch runtime before removing a clean, preserved worktree.

## Candidate-only final safeguard

After acceptance and pre-safeguard admission, run the launcher from the reserved candidate
worktree with the caller-supplied expected branch (there is no default branch):

```powershell
$candidateRoot = "<candidate-root>"
& (Join-Path $candidateRoot "tools/Invoke-CandidateSafeguard.ps1") -RepositoryRoot $candidateRoot -ExpectedBranch "<candidate-branch>"
& (Join-Path $candidateRoot "tools/Invoke-CandidateSafeguard.ps1") -RepositoryRoot $candidateRoot -ExpectedBranch "<candidate-branch>" -Run
```

The first command prints its bound checks. The second runs the selector-owned Ruff, formatting,
retained non-expanded BasedPyright baseline, compilation, orchestrator and watcher unit
discoveries, attention retention, and synthetic cleanup components. It refuses any non-reserved,
ambiguous, dirty, stable-runner, or wrong-branch root; it is a safeguard launcher, not a scheduler,
retry controller, or alternate harness.
