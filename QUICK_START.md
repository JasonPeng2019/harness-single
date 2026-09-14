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

## 4. Wait and acknowledge

```powershell
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60
```

Handle one returned event, verify the corresponding action or durable record, then acknowledge the
top-level `event_id` exactly:

```powershell
python -m orchestrator_harness --config local-config/harness.json ack --event-id <event-id>
```

Repeat after timeouts. Never poll worker transcripts or add a relay as an alternate discovery path.

## 5. Finish each lane

Workers may update `.agent-workspace/PARALLEL_CHECKPOINT.md` during progress. Completion requires a
clean committed branch plus `.agent-workspace/RESULT.json` matching
`examples/coding.result.example.json`. The commit must be the current branch tip.

Use the native result emitter to avoid hand-written identity and envelope errors. The worker writes
`.agent-workspace/result-facts.json` with exactly these three fields (actual outcomes, never defaults):

```json
{"outcome":"PASS","summary":"Focused work completed.","checks":[{"name":"unit tests","command":"python -m unittest tests.test_parser","outcome":"PASS"}]}
```

From the harness checkout, publish using the current ROOT-authored invocation as context:

```powershell
python -m orchestrator_harness.result_emit --context <absolute-current-invocation.json> --facts <absolute-result-facts.json>
```

From a worker worktree, use the absolute path to `orchestrator_harness/result_emit.py` instead of
`-m orchestrator_harness.result_emit`. ROOT supplies this exact command in each worker card.
`--check-only` runs preflight without writing. Publication derives lane/worker IDs from the context,
checks the declared Git identity and clean current tip, applies the same validator as the controller,
atomically replaces `.agent-workspace/RESULT.json`, and validates readback. It runs no test, commit,
cleanup, or acceptance operation. Exit 0 means the envelope is valid, including valid FAIL/BLOCKED
results; semantic acceptance remains the manager's decision.

Result outcomes are PASS/FAIL/BLOCKED. Check outcomes are PASS/FAIL/SKIP/NOT_RUN; BLOCKED is not a
check outcome. At most 64 check summaries fit the frozen schema; retain full observations in the
separate task handoff and summarize them truthfully rather than discarding failures. Extra fields
are rejected, never silently stripped. On error, correct only the reported fact/shape issue and
rerun the emitter; do not replay unchanged checks or alter findings to manufacture PASS. A dirty
worktree needs its authorized commit or an honest unresolved report; the emitter never commits it.
The controller returns nonzero for a missing or invalid coding result even if the provider exits 0,
and preserves its thread identity for a separately authorized same-thread correction.

For an external worker launched by `operator_launch`, ROOT supplies a result context with schema
`orchestrator-lane-result-context/v1`, `run_root`, `lane_id`, `worker_invocation_id`, and the same
`repository` declaration as a coding invocation, plus absolute `receipt_path`. The emitter checks
the receipt's launched state, label, and cwd against the context. This context records ROOT's worker
identity; the receipt itself does not establish a worker invocation ID. The worker must not edit
either identity source. Receipt validation proves correlation, not process cleanup or task success.
Workers still author the detailed handoff required by their task; this helper validates the coding
result envelope and Git state, not arbitrary domain-specific handoff schemas or the truth of findings.

Normal named-lock contention waits automatically. Investigate only malformed, stale, unknown, or
excessive-wait evidence. Never delete another invocation's claim.

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
