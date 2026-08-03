# Portable Coding Orchestrator Harness

This repository coordinates ordinary software work across Git branches and worktrees. A persistent
manager plans lanes and integration; externally launched lane controllers validate identity,
launch one coding worker, and serialize opaque named resources; the native observer reconciles
durable state and delivers events. The optional deterministic watcher records diagnostics only.

The harness is not a scheduler, task database, dependency engine, project checker, or file
ownership system. Git worktrees isolate code. The manager remains responsible for planning,
launching, decisions, merges, checks, acceptance, and promotion.

## Coding topology

```text
frozen baseline -> lane branches/worktrees -> integration branch/worktree
                       |                              |
                 coding controllers             merge worker
                       |                              |
                       +------ native observer -------+
                                      |
                              persistent manager
```

- Create one branch and worktree per concurrently active lane.
- Branch child lanes from an exact stable parent commit.
- Use a manager-assigned merge lane to combine completed lane branches.
- Keep a known-good frozen revision unchanged during execution.
- Treat the integrated commit as a candidate until acceptance checks pass.
- Promote only the accepted candidate; never promote a dirty worktree or an unvalidated result.

## Start here

1. Read `QUICK_RULES.md` and `QUICK_START.md`.
2. Copy `examples/harness.example.json` to ignored `local-config/harness.json`.
3. Set `suite_root` to the parent that contains `worktrees/` and use `worktrees/*` as `run_globs`.
4. Give each manager epoch a fresh `runtime/orchestrator-harness/<epoch>` output directory.
5. Run a read-only discovery scan before launching workers:

```powershell
python -m orchestrator_harness --config local-config/harness.json scan --no-write
```

The complete coding invocation, result, and lock-record shapes are in `examples/`. Paths and Git
IDs in those static examples are placeholders and must be replaced with facts from the active lane.

## Manager loop

The persistent manager launches controllers explicitly:

```powershell
python -m orchestrator_harness.lane_controller <absolute-invocation.json>
```

It then repeatedly uses the native blocking wait:

```powershell
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60
```

Exit `0` returns one JSON event. Exit `3` is a quiet timeout. Exit `1` is a configuration,
observation, or safety failure. Handle the event, verify the durable action, and acknowledge only
its top-level native `event_id`:

```powershell
python -m orchestrator_harness --config local-config/harness.json ack --event-id <event-id>
```

`data.signal_id` identifies a source signal and is not an acknowledgement ID. Delivery is
at-least-once, so consumers deduplicate by `event_id`. An event remains pending until exact
acknowledgement succeeds.

## Lane records

Every coding invocation uses `orchestrator-coding-invocation/v1` and records:

- lane and worker invocation IDs;
- worktree, attached branch, Git common directory, and base commit;
- prompt path and SHA-256;
- controller output paths under `.agent-workspace`;
- manager-epoch event and resource-lock roots; and
- zero or more exact opaque `exclusive_resources` names.

`PARALLEL_CHECKPOINT.md` is resumable progress, not completion. A merge-ready
`.agent-workspace/RESULT.json` uses `orchestrator-lane-result/v1` and must match the current lane,
worker invocation, branch, and real branch-tip commit. The project worktree must be clean. Reported
checks are evidence; the harness does not execute them. The merge worker runs integration checks.

Named locks use exact string matching and atomic claims. Normal contention stays in
`WAITING_RESOURCE` and does not require manager action. Malformed, unknown, stale, or excessive
wait evidence fails safe. A controller releases only claims matching its exact invocation and
PID-plus-creation identity.

## Split, merge, accept

1. Commit the stable parent before splitting.
2. Create child branches at that exact commit and add separate worktrees.
3. Write one bounded invocation per lane and launch one controller per worktree.
4. Wait for native events; review checkpoints and exact validated results.
5. Create a dedicated integration branch/worktree from the declared base.
6. Merge the completed input branches and resolve conflicts in that merge lane.
7. Run the target project's tests in the merge worktree.
8. Publish the merge lane result, select the candidate commit, and run acceptance.
9. Promote the candidate only after acceptance; retain or restore the frozen revision on failure.

## Cleanup

Stop managed observation cooperatively with `watch stop` when used. Confirm controller, worker, and
watcher PID-plus-creation identities are absent; named claims are released; worktrees are clean;
and no pending event is silently discarded. Remove disposable worktrees with Git only after their
commits and results have been preserved. Runtime data belongs under ignored `runtime/`.

## Disposable integration fixture

This local smoke creates a temporary Python repository, two contending coding lanes, a merge lane,
stale and valid results, durable event delivery and exact acknowledgement, then removes everything:

```powershell
python examples/disposable_coding_fixture.py
```

Use `--keep <new-directory>` only when inspecting failed evidence. No network, Codex service,
firmware record, or physical resource is used.

## Supported firmware compatibility path

The schema-less policy-bound firmware invocation and passive board, relay, lease, MCP, and hardware
observation remain supported. They are a separate compatibility path and are not required by
`orchestrator-coding-invocation/v1`. Firmware work must continue to follow its existing policy,
authorization, lease, relay, and physical cleanup rules.

## Documentation

- `QUICK_START.md`: shortest ordinary coding run.
- `QUICK_RULES.md`: authority and safety rules.
- `orchestrator_harness/README.md`: command and contract reference.
- `orchestrator_harness/SPEC.md`: observer requirements, including the legacy firmware path.
- `docs/HARNESS_WATCHER_GUIDE.md`: optional diagnostic watcher.
- `PORTABLE_CONTENTS.md`: packaged contents and exclusions.
