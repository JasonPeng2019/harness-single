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

## Public release surface

The supported launch journey is the operator boundary followed by the native lane-controller
CLI. `orchestrator_harness.public_launch` is only a thin composition of those two existing
interfaces; it does not own a second lifecycle or workflow engine. A controller validates one
coding or schema-less legacy invocation, launches one provider, publishes durable status/events,
and owns claims, semantic resume, result validation, and archive-first cleanup. The capability
broker and optional firmware adapter remain on the legacy route; workers never receive hardware
endpoints or credentials.

Release checks are declared once in `orchestrator_harness.release_checks`. Each stable check ID
declares its exact command, tier, dependency domains/files, platform or external requirements,
and credit contract. Select the shortest decisive invalidated checks first:

```powershell
python -m orchestrator_harness.release_checks select --intent fast --root <repository-root>
python -m orchestrator_harness.release_checks select --intent affected --root <repository-root> `
  --changed-path orchestrator_harness/lane_controller.py
```

Fast is local and never selects the WSL/real-agent journey or accumulated release assurance.
Affected selection includes only consumers of the supplied dependency paths/domains; full/release
may enumerate every check, including the ROOT-owned safeguard, but this producer does not run
that accumulated gate. Credit is reusable only with the same canonical declared-input
fingerprints and exact source root, Git common directory, branch, and full tip. Missing, stale,
malformed, unknown, or mixed-root credit is selected again.

`examples/public-coding-launch.example.md` and `examples/release-selection.example.json` are
copyable shapes. The public disposable journey is host-only and uses fake provider output in
temporary Git worktrees; it proves terminal events, result/Git identity, and exact cleanup without
hardware, MCP, WSL, a real provider, credentials, network, or installation.

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

The harness has no competing acknowledgement CLI. In the manager process, acknowledge through the
bound S3 `ManagerEventRouter` API: `router.acknowledge("<top-level-event-id>",
binding=router.registration)`. The envelope's top-level `event_id` is the only acknowledgement
ID; `data.signal_id` identifies a worker signal.

Delivery is at-least-once, so consumers deduplicate by `event_id`. An event remains pending until
exact acknowledgement succeeds.

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

Return from the bounded diagnostic wait and stop any externally managed observer through its
recorded owner. Confirm controller, worker, and watcher PID-plus-creation identities are absent;
named claims are released; worktrees are clean;
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

The firmware-v2 static acceptance material is in `firmware_acceptance/`. The retained
`examples/legacy-firmware.invocation.example.json` and
`examples/dual-path-manager.example.md` show how legacy firmware and coding V1 coexist without a
schema migration or an alternate event loop. `QUICK_START.md` contains the release-role assignments,
fresh-epoch/operator commands, exact identity cleanup procedure, and the candidate-only safeguard
entry point. Release record shells are intentionally small and live in `release_evidence_templates/`.

## Documentation

- `QUICK_START.md`: shortest ordinary coding run.
- `QUICK_RULES.md`: authority and safety rules.
- `orchestrator_harness/README.md`: command and contract reference.
- `orchestrator_harness/SPEC.md`: observer requirements, including the legacy firmware path.
- `docs/HARNESS_WATCHER_GUIDE.md`: optional diagnostic watcher; its M5 and named-experiment
  procedures are retained legacy firmware compatibility guidance.
- `PORTABLE_CONTENTS.md`: packaged contents and exclusions.
