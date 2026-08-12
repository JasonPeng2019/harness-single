# Quick Start: Ordinary Coding

## 1. Prepare lanes

Start from a stable committed base. Create one branch and worktree for each concurrent task:

```powershell
git worktree add -b lane/parser ..\project-worktrees\parser <base-commit>
git worktree add -b lane/api ..\project-worktrees\api <base-commit>
```

Do not switch branches in an active lane worktree. A later split starts only after the parent lane
commits; every child branches from that exact commit.

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

## Firmware V2 acceptance and dual-path operation

The acceptance kit is static, host-only input: read `firmware_acceptance/README.md` and its
`ACCEPTANCE_MANIFEST.json` before configuring a firmware acceptance attempt. It launches neither
MCP nor hardware. Keep `examples/coding.invocation.example.json` unchanged for coding V1. For an
existing policy-bound firmware lane, start from
`examples/legacy-firmware.invocation.example.json`; it intentionally has no `schema` field and no
coding V1 Git/runtime/resource fields. Do not migrate a schema-less firmware fixture just to share
an observer epoch.

`examples/dual-path-manager.example.md` is the copyable native-manager sequence for one coding and
one legacy firmware lane. It uses a fresh config/runtime, `scan --no-write`, and one native
`watch --until-actionable` call. Its acknowledgement is the returned envelope's top-level
`event_id`, never `data.signal_id`.

For a firmware-v2 implementation epoch, create fresh ignored paths first (replace the placeholders
with a unique epoch and the actual suite root), then inspect the frozen registry rather than
rewriting it:

```powershell
$epoch = "firmware-v2-$(Get-Date -Format yyyyMMdd-HHmmss)"
New-Item -ItemType Directory -Force local-config, "runtime/orchestrator-harness/$epoch" | Out-Null
Copy-Item examples/harness.example.json "local-config/$epoch.json"
# Set suite_root, run_globs, and output_dir to this epoch's real paths.
python -m orchestrator_harness --config "local-config/$epoch.json" scan --no-write
python -m orchestrator_harness --config "local-config/$epoch.json" watch --until-actionable --timeout 60
# In the bound manager router, acknowledge the returned envelope only:
router.acknowledge("<top-level-event-id>", binding=router.registration)
Get-Content <accepted-evidence>/passed-tests.json
```

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

## Firmware V2 release roles

`ROOT-IM` is the outside implementation coordinator: it decides implementation findings, starts
the acceptance topology, and never receives the subagent model/tier assignment. `F.C3.O` is the
fresh acceptance orchestrator: it decides only final target-project work and submits assignments to
`C3-HARNESS`. `C3-HARNESS` is the candidate control plane under test, not an agent; it alone launches
target workers and owns their lifecycle. `F.C3.W` is a separately launched read-only watcher.

Every headless child role uses the exact assigned model with no substitution, explicit reasoning effort,
explicit `service_tier="priority"`, isolated `-C` root, `--dangerously-bypass-approvals-and-sandbox`,
`--dangerously-bypass-hook-trust`, `--ignore-user-config`, and `--json`. Fast means exactly
`service_tier="priority"`; default-tier roles must not set it. The required assignments are:

| Role | Model / effort | Tier |
| --- | --- | --- |
| Acceptance orchestrator (`F.C3.O`) | GPT-5.6 Sol / high | Fast (`priority`) |
| Production coder | GPT-5.6 Terra / medium | Fast (`priority`) |
| Reviewer or test writer | GPT-5.6 Terra / medium | Fast (`priority`) |
| Doer or test executor | GPT-5.6 Luna / high | Fast (`priority`) |
| Acceptance watcher (`F.C3.W`) | GPT-5.6 Terra / medium | Fast (`priority`) |

Production coding remains singleton/serial. At most three agents may exist beside `ROOT-IM`; actual
role fan-out is one through three and only for independent review, test-writing, or test-execution
slices. The deterministic watcher remains diagnostic-only with `evaluator_enabled: false`.

## Candidate-only final safeguard

After C4 and pre-safeguard admission—not now—run the launcher from the reserved candidate worktree:

```powershell
$candidateRoot = "<candidate-root>"
& (Join-Path $candidateRoot "tools/Invoke-CandidateSafeguard.ps1") -RepositoryRoot $candidateRoot
& (Join-Path $candidateRoot "tools/Invoke-CandidateSafeguard.ps1") -RepositoryRoot $candidateRoot -Run
```

The first command prints its bound checks. The second runs the selector-owned Ruff, formatting,
retained non-expanded BasedPyright baseline, compilation, orchestrator and watcher unit
discoveries, attention retention, and synthetic cleanup components. It refuses any non-reserved, ambiguous, dirty,
stable-runner, or wrong-branch root; it is a safeguard launcher, not a scheduler, retry controller,
or alternate harness.
