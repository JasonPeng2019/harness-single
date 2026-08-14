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
fingerprints and exact source root, Git common directory, and branch, while its origin tip remains
an ancestor of the current tip. Missing, stale,
malformed, unknown, or mixed-root credit is selected again.

`examples/public-coding-launch.example.md` and `examples/release-selection.example.json` are
copyable shapes. The public disposable journey is host-only and uses fake provider output in
temporary Git worktrees; it proves terminal events, result/Git identity, and exact cleanup without
hardware, MCP, WSL, a real provider, credentials, network, or installation.

## Registered provider adapters

Provider launch semantics live inside one small versioned adapter contract
(`orchestrator-provider-adapter/v1`).  The generic core never hard-codes a
provider: it selects a registered adapter and consumes only the declared
lifecycle contract.  Codex and Claude Code are maintained built-ins; a
separately registered external CLI adapter becomes selectable without edits
to generic dispatch, workflow, task, event, supervisor, or cleanup code.

A minimal adapter implements the required methods:

- `build_argv(spec)` ? build the complete child command from the
  provider-neutral `ProviderLaunchSpec`.
- `encode_prompt(prompt)` ? transport the prompt bytes.
- `parse_transcript_line(line)` ? extract provider-neutral events.
- `terminal_outcome(event, exit_code)` ? classify the terminal outcome.
- `redact_argv(argv)` ? return the complete redacted command provenance.
  The selected adapter owns every provider-specific credential spelling;
  the generic core never guesses flag names.  The one returned value is used
  identically for `ProviderEvidence.command_provenance` and
  `launcher_settings.argv`, so the two durable surfaces cannot drift.
  Inheriting the `BaseProviderAdapter` generic fallback is rejected at
  registration: the adapter must implement `redact_argv` explicitly, and a
  generic-safe adapter may explicitly delegate to `redact_command`.
- `deliver_notification(coordinator, notice, *, boundary)` ? required only
  when `notification=True`: one real safe-boundary delivery binding that
  emits the exact content-free wake through the per-binding
  `DeliveryCoordinator`.  Without it, registration is rejected and the
  adapter must declare `notification=False` (`SAFE_BOUNDARY_ONLY`).

Registration is process-local: it must happen in the same interpreter that
parses and launches the invocation, before `controller.main(...)` runs.  A
fresh interpreter contains only the built-ins.  The supported operator shape
is a small wrapper that registers the adapter, then invokes the native
controller:

```python
# my_cli_bootstrap.py -- run: python my_cli_bootstrap.py start.invocation.json
import sys
from orchestrator_harness.lane_controller import main
from orchestrator_harness.provider import (
    BaseProviderAdapter, ProviderCapabilities, ProviderEvent,
    ProviderLaunchSpec, register_provider_adapter,
)

class MyCliProviderAdapter(BaseProviderAdapter):
    provider_id = "my-cli"
    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command, "--run", "--model", spec.model]
    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt
    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        return None
    def terminal_outcome(self, event, exit_code) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"
    def redact_argv(self, argv):
        # Own every credential spelling; the generic core never guesses.
        redacted, redact_next = [], False
        for token in argv:
            if redact_next:
                redacted.append("<redacted>"); redact_next = False
            elif token == "--auth":
                redacted.append("<redacted>"); redact_next = True
            else:
                redacted.append(token)
        if redact_next:
            redacted.append("<redacted>")
        return tuple(redacted)

register_provider_adapter(
    "my-cli",
    MyCliProviderAdapter(),
    version="my-cli-v1",
    capabilities=ProviderCapabilities(
        launch=True, prompt=True, event_result=True, session=True,
        resume=True, permission=True, configuration=True, notification=False,
    ),
)
sys.exit(main([sys.argv[1]]))
```

The canonical invocation selects the adapter with `provider.id = "my-cli"`
and the matching `profile.provider = "my-cli"`; invocation validation admits
only registered provider IDs.  Each adapter truthfully declares launch,
prompt, event/result, session, resume, permission, configuration, and
notification capabilities.  Unsupported operations return actionable
classified results (`classify_operation`), never opaque failures.  Command
construction, prompt transport, result/session parsing, permission mapping,
and redacted provenance live in the selected adapter.  Same-role resume is
optional: unsupported or identity-mismatched resume preserves the logical
task state and starts a declared same-role structured handoff
(`decide_resume_or_handoff`) without fabricated continuity.  Evidence binds
adapter identity/version, capabilities, the selected configuration digest,
attempt/session identity, and the adapter-owned redacted command provenance
(`build_provider_evidence`).

## Deferred notification

Notification is an adapter-owned operation.  `notification_mode(provider_id)`
returns the selected adapter's executable mode: `WAKE` with the exact
content-free, non-preemptive wake text, or an honest `SAFE_BOUNDARY_ONLY`
when immediate wake is unavailable.  A `notification=True` adapter must also
implement `deliver_notification(coordinator, notice, *, boundary)`: the one
real safe-boundary delivery binding that emits the exact wake through the
per-binding `DeliveryCoordinator`.  The installed Codex hook route consumes
that same seam; adapters without the binding stay `SAFE_BOUNDARY_ONLY` and
perform no immediate wake.  The one durable per-binding active-queue
authority is the S3 `notifications.ManagerEventRouter`; `host_adapters.DeliveryCoordinator`
admits typed notification items through that router and keeps only the
OPEN/EXTERNALLY_BLOCKED decoration (with the exact required external
actor/action) in its own per-binding state.  The wake never contains
notification payload and never stops, redirects, or preempts the active
directive.  At the safe boundary, acknowledgement atomically records success
and mechanically removes each addressed item from the router queue; transport
receipts never acknowledge queue work and `CLOSED` is never retained as an
active state.  The installed Codex hook routes (`run_installed_codex_hook`)
enforce the stop matrix: any `OPEN` item rejects stop without payload
disclosure, an empty active queue permits stop, and all-`EXTERNALLY_BLOCKED`
items permit stop only after the final response names every notification ID
and its exact required external actor/action, while those items remain active.
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

The optional firmware seam is caller-declared and installed with the package:

- `orchestrator_harness.firmware_campaign` declares a validated `FirmwareAction` (MCP tool name,
  positive method version, positive maximum duration, and the exact required argument names) and
  builds a `FirmwareCampaignPack` only from a caller-declared capability name, action
  declarations, canonical resource identities, and a nonempty public policy binding. There is no
  default pack, fixture, action, manifest, policy, path, hash, server revision, provider, or
  machine value. `resolve()` requires the requested capability and action, exactly one declared
  resource, and exactly the action's declared argument keys, and passes caller arguments through
  unchanged. A lane ID is never assumed to be a board/resource ID. The approval policy binds the
  declared policy, capability, action, MCP tool, method version, maximum duration, canonical
  resource, and resource identity; a caller may deliberately include a revision/hash in its policy
  binding, and the harness never generates one.
- `orchestrator_harness.firmware_adapter` provides the broker-compatible `FirmwareHardwareAdapter`,
  which requires the pack plus caller-supplied snapshot, launch/configuration, child-identity, and
  transport seams. It reuses `ProcessBoundary` and `ProcessSupervisor` and sends exact MCP traffic
  (`initialize` with the caller-declared protocol version, `notifications/initialized`, then
  `tools/call` with the declared tool and unchanged arguments). Permit-expiry checks precede
  launch, enqueue, and dispatch; public results never leak private launch/configuration data; and
  cleanup retains the claim until both the owned process boundary and transport are proven closed.
- The public types are exported from `orchestrator_harness` (`FirmwareAction`,
  `FirmwareCampaignPack`, `FirmwareOperation`, `FirmwareHardwareAdapter`,
  `HardwareCapabilityAdapter`). The pack/adapter plus the existing `CapabilityBroker` is the
  complete optional firmware seam; it is not connected to the lane controller and introduces no
  controller or runtime protocol.

The retained `examples/legacy-firmware.invocation.example.json` and
`examples/dual-path-manager.example.md` show how legacy firmware and coding V1 coexist without a
schema migration or an alternate event loop. `QUICK_START.md` contains the fresh-epoch/operator
commands, exact identity cleanup procedure, and the candidate-only safeguard entry point. Release
record shells are intentionally small and live in `release_evidence_templates/`; the packaged
canonical copies are listed by `orchestrator_harness.release_assets`.

## Documentation

- `QUICK_START.md`: shortest ordinary coding run.
- `QUICK_RULES.md`: authority and safety rules.
- `orchestrator_harness/README.md`: command and contract reference.
- `orchestrator_harness/SPEC.md`: observer requirements, including the legacy firmware path.
- `docs/HARNESS_WATCHER_GUIDE.md`: optional diagnostic watcher; its M5 and named-experiment
  procedures are retained legacy firmware compatibility guidance.
- `PORTABLE_CONTENTS.md`: packaged contents and exclusions.
