# Coding Orchestrator Harness

The package reconciles coding-lane facts and delivers sparse, durable manager
events. It does not schedule work, choose providers, accept results, operate
hardware, or replace the root orchestrator. S3 `ManagerEventRouter` remains the
single queue, wake, delivery-evidence, and acknowledgement authority.

## Start here

Use a fresh ignored configuration and runtime root for each epoch:

```powershell
python -m orchestrator_harness --config local-config/harness.json scan --no-write
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60
```

`scan`, `watch --once`, `watch --until-event`, and `watch --until-actionable`
are diagnostic/public waits. They do not start a foreground managed watcher,
renew a heartbeat, or create a second notification queue. The native manager
discovers work through its blocking wait and acknowledges the envelope's
top-level `event_id` through the S3 manager API; a worker `data.signal_id` is
not an acknowledgement ID.

## Codex adapter

The current implemented profile is selected through one versioned capability
interface. Future-host profiles are contract fixtures only and make no install
or implementation claim.

```powershell
python -m orchestrator_harness adapter install --host codex --project-root <disposable-project>
python -m orchestrator_harness adapter check --host codex --project-root <disposable-project>
python -m orchestrator_harness adapter upgrade --host codex --project-root <disposable-project>
python -m orchestrator_harness adapter uninstall --host codex --project-root <disposable-project>
```

The installer owns only the packaged project-local hook files, the managed hook
entries, and its installation manifest. It validates the `.codex` shape before
writing, uses same-directory atomic replacements, records only closed managed
content identities, refuses ambiguous ownership, and restores bounded changes
on failure. Uninstall subtracts exact managed fragments/assets and preserves
unrelated or modified user content.
The manifest separately reports installed bytes, synthetic self-test status,
project-layer trust, and hook review state; installation never silently grants
project trust.

Codex command hooks are synchronous lifecycle hooks. PostToolUse is a safe
tool-result boundary and Stop is a finalization backstop. The persistent
harness coordinator owns the binding-specific wake subscription and replay; a
hook does not directly watch arbitrary file changes. App Server fixtures use
`thread/inject_items`, `turn/completed`, and `turn/start` for idle continuation.

## Qwen Code adapter

The native runner provider is `qwen-code`; its host adapter is selected with
`--host qwen` (the `qwen-code` host spelling is also accepted):

```powershell
python -m orchestrator_harness adapter install --host qwen --project-root <disposable-project>
python -m orchestrator_harness adapter check --host qwen --project-root <disposable-project>
python -m orchestrator_harness adapter uninstall --host qwen --project-root <disposable-project>
```

The installer owns only `.qwen/settings.json`, `.qwen/hooks/`, and its
project-local manifest. It uses Qwen Code’s documented top-level `hooks`
settings form, command-hook `name` fields, `PostToolUse`/`Stop` groups, and a
`Notification` group matched to `idle_prompt`. Existing project settings and
unrelated hook groups are preserved; no user/global Qwen configuration is
written. Delivery receipts are transport evidence and never acknowledge
manager queue work. The deterministic fixture and tests prove installation,
preservation, and sparse delivery; they make no live Qwen hook-session claim.
Delivery notices contain binding identity, queue revision, pending count,
highest class/severity, timestamp, and adapter profile only. Delivery receipts
are transport evidence and never acknowledge pending events.

## Registered provider adapters

Provider semantics live inside one small versioned adapter contract
(`orchestrator-provider-adapter/v1`).  The generic core selects a registered
adapter and never hard-codes a provider.  Codex and Claude Code are
maintained built-ins; a separately registered external CLI adapter becomes
selectable without edits to generic dispatch, workflow, task, event,
supervisor, or cleanup code.

A minimal adapter implements the required methods: `build_argv(spec)`,
`encode_prompt(prompt)`, `parse_transcript_line(line)`,
`terminal_outcome(event, exit_code)`, and `redact_argv(argv)` ? the
adapter-owned complete redacted command provenance.  The generic core never
guesses provider credential spellings; the one adapter-produced value is used
identically for `ProviderEvidence.command_provenance` and
`launcher_settings.argv`.  Inheriting the `BaseProviderAdapter` generic
fallback is rejected at registration: the adapter must implement
`redact_argv` explicitly, and a generic-safe adapter may explicitly delegate
to `redact_command`.  A `notification=True` adapter must additionally
implement `deliver_notification(coordinator, notice, *, boundary)`: one real
safe-boundary delivery binding through the per-binding `DeliveryCoordinator`.

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
    BaseProviderAdapter,
    ProviderCapabilities,
    ProviderEvent,
    ProviderLaunchSpec,
    register_provider_adapter,
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
                redacted.append("<redacted>")
                redact_next = False
            elif token == "--auth":
                redacted.append("<redacted>")
                redact_next = True
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
        launch=True,
        prompt=True,
        event_result=True,
        session=True,
        resume=True,
        permission=True,
        configuration=True,
        notification=False,
    ),
)
sys.exit(main([sys.argv[1]]))
```

The canonical invocation selects the adapter with `provider.id = "my-cli"`
and the matching `profile.provider = "my-cli"`; invocation validation admits
only registered provider IDs.  `provider_registry()` returns the immutable
registered set; `provider_adapter` selects one adapter; `classify_operation`
returns an actionable classified result for unsupported operations.  Command
construction, prompt transport, result/session parsing, permission mapping,
and redacted provenance live in the selected adapter.  Same-role resume is
optional: unsupported or identity-mismatched resume preserves the logical
task state and starts a declared same-role structured handoff without
fabricated continuity (`decide_resume_or_handoff`).  Evidence binds adapter
identity/version, capabilities, the selected configuration digest,
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
## Public launch and check selection

`orchestrator_harness.public_launch.launch_lane_controller` is a thin public composition of
`operator_launch.launch_process` and the `lane_controller` CLI. The operator receipt binds the
exact controller PID plus creation identity; the controller owns provider lifecycle, durable
events/results, claims, semantic resume, and archive-first cleanup. The disposable public journey
uses fake provider output in temporary Git worktrees and does not require WSL, hardware, MCP,
credentials, or a real provider.

`orchestrator_harness.release_checks` is the only check registry/selector. It exposes stable IDs,
declared dependency domains/files, exact commands, tiers, platform requirements, and credit
contracts. `fast` is local, `affected` consumes changed domains/paths, and `full`/`release` can
enumerate release assurance. Credit is valid only when its canonical fingerprint and exact source
root, Git common directory, and branch match, with the recorded origin tip still an ancestor of
the current tip. The selector rejects stale, malformed,
unknown, or mixed-root records and orders shortest decisive invalidated checks first. The
WSL/real-agent check is explicitly non-fast; this producer records its selection only.

Release-tier components have stable IDs for Ruff, formatting, BasedPyright, compilation,
orchestrator/watcher discovery, attention retention, and synthetic cleanup. The aggregate
candidate safeguard is a ROOT-owned launcher and is excluded from the component selection it
executes. Package release assets use one canonical `assets/release/` tree; checkout examples and
templates are retained copies bound to those packaged resources.

The portable candidate safeguard derives or accepts its repository root, reads back the exact
Git top level/branch/tip, and gets its check commands from that selector. It retains the candidate
branch and BasedPyright-baseline safety boundary without a developer-specific checkout path.

## Immutable views and terminal lanes

Static work can receive an exact-commit read-only source view with separate
writable result and cache roots:

```powershell
python -m orchestrator_harness view allocate --source-root <repo> --revision <full-commit> `
  --retained-ref <ref> --view-root <view> --result-root <results> --cache-root <cache>
```

The allocation publishes a ready record only after the exact revision and
read-only boundary are established. A partial allocation has no ready record
and never writes results beneath the source view.

Terminal retirement is archive-first:

```powershell
python -m orchestrator_harness lane retire --lane-root <linked-worktree> `
  --archive-root <archive> --lane-id <lane> `
  --task-ref <task.json> --result-ref <result.json> --findings-ref <findings.json> `
  --acceptance-ref <acceptance.json> --transcript-ref <transcript.json> `
  --dependency-ref <dependency.json>
```

The controller first admits one generation-bound lifecycle record at a reserved
coordinate under the target repository's canonical Git common directory. The
coordinate binds the live canonical worktree, common directory, lane, branch,
and worker invocation; every worker contends on that one fixed owner file, and
retirement derives it from live Git state without a caller-selected runtime
path. Resume loads the admitted generation before publishing resumed status
and compare-and-swap updates the same owner. The controller establishes an
OS-backed provider process boundary and durably arms every resource claim before
launch. The returned Popen handle is the authoritative postlaunch fact on
every exception, interrupt, and finalization route; a process-nonnull route
must clean up and retain unless the explicit final release proof succeeds. It
records complete zero/one/many helper
evidence; Linux uses subreaper adoption plus group/session, descendant, and
exact retained-identity inventory, while unsupported or incomplete inventory
is nonterminal. Termination waits, re-inventories, and reaps the complete
boundary, then takes a separate final empty inventory before releasing claims;
unresolved exact identities remain claim blockers across controller exit. The
Windows Job inventory rejects assigned/list count mismatches before accepting
zero or member evidence. Job member slots are read only from a valid returned
byte extent, and oversized or nonconverging count responses fail incomplete
under a bounded retry policy. Retirement validates admission and freshly
observes the complete ownership boundary twice, then copies and validates
task/result/findings/acceptance/transcript/dependency/process evidence and
content hashes before normal `git worktree remove`. Dirty, live, ambiguous,
unretained, unmerged, or archive-failed lanes remain visible.

### Explicit ROOT adjudication

A manager may pre-authorize later adjudication of a known harness failure by
generating a one-launch secret with `generate_root_adjudication_secret`, retaining
that secret in ROOT, and setting `ORCHESTRATOR_ROOT_ADJUDICATION_SECRET` only in
the controller launch environment. The controller persists a commitment and an
HMAC-authenticated status, then removes the secret before starting the provider.

If the terminal controller state is `CONTROLLER_FAILED`, ROOT may call
`adjudicate_controller_status` with the retained secret, its identity, and a
rationale. A valid decision preserves the complete original failure, records the
decision, and exposes effective `PASS`. Missing, mismatched, worker-modified, or
unsigned status fails closed. Lanes launched without the secret remain valid but
cannot be adjudicated; the public CLI intentionally has no adjudication authority.

## Workspace overlay and bounded policy readback

One provider-neutral overlay owner prepares identified worktrees from an
ordinary editable `super-cache` folder before a provider launch and restores
them exactly at retirement:

```powershell
python -m orchestrator_harness workspace super-cache ingest --source <folder> --harness-worktree <harness>
python -m orchestrator_harness workspace prepare --super-cache <harness>/super-cache `
  --worktree <target> --role subagent --receipt <receipt.json>
```

Ingest refreshes `HARNESS_WORKTREE/super-cache` to exactly the supplied
folder contents (never the container folder itself) and is never reported
complete on failure. Preparation copies the cache contents current for that
call: directories merge recursively without overwriting, missing paths are
created, and an existing-file collision is rejected unless its relative path
is declared in the optional root `.super-cache.json` `append_text` list,
in which case the payload's exact bytes are appended to the existing regular
UTF-8 text file. The complete operation is preflighted before any mutation and
writes a minimal no-hash receipt (target worktree ID, role, completed state,
affected paths, operations, created paths, and exact pre/post bytes for
affected files). The cache and declaration stay mutable; a later re-ingest or
direct cache edit affects only later preparations.

The lane controller verifies the receipt immediately before launch (present
receipts must be completed and name that worktree and the `subagent` role;
absence is allowed), and subagent lane retirement calls the same restoration
function. Restoration compares each affected file's current bytes with the
receipt's exact post-prepare bytes, restores appended preimages or removes
receipt-created files, removes only empty receipt-created directories, and
preserves later edits while reporting retirement blocked. The external owner
of an orchestrator worktree calls the same function from its own retirement
path. Preparation must complete before process creation; it cannot
retroactively change an already-running session.

The Codex adapter packages an editable launcher JSON and Git-ignore exclusion
file under the project's `.codex/policies`. Git evaluates ordinary
file/directory/wildcard/negation patterns for resolved script paths; an
exclusion is accepted only when Git's winning verbose match came from the
provider's declared exclusion file and is not negated. The supervisor
entrypoint and the approved stable-runner script are the only initial
exclusions, so the lane-managed provider session receives no bounded-test
deadline while non-excluded covered nested commands launched inside agent
worktrees still require the one supervisor. Configuration and Git failures
fail closed with an actionable denial. Read the installed policy with:

```powershell
python -m orchestrator_harness adapter check --host codex --project-root <project>
```

The check result includes `bounded_policy` launcher/exclusion readback.

## Configuration migration

Retained configuration covers discovery, bounded reads, and diagnostic waits.
Integer counts and limits reject fractional values; durations must be finite.
Removed watcher, heartbeat, attention, review/no-progress, and obsolete
tolerance keys produce explicit migration diagnostics for compatibility readers
and have no S4 runtime effect. They are not present in `config.example.json`.

## Diagnostic watcher

The optional watcher remains diagnostic-only with `evaluator_enabled: false`.
Its recovery projection is limited to `open`, `acknowledged`, and `resolved`.
Watcher diagnostics do not become manager wake events and do not perform
repair, scheduling, or acknowledgement.

## Exit codes

- `0`: diagnostic operation or lifecycle operation completed
- `1`: configuration, safety, ownership, or lifecycle proof failed
- `3`: bounded diagnostic wait timed out

## Ordinary coding lane contract

The canonical coding invocation path has one lane-management/controller implementation with two
supported input shapes: the provider-neutral `orchestrator-worker-invocation/v1` schema and the
retained Codex `orchestrator-coding-invocation/v1` adapter. The removed schema-less firmware shape
is unsupported and fails ordinary invocation validation before any workspace, event, or provider
work starts.
