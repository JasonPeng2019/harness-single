# Orchestrator Harness Specification

## Purpose

Provide a durable event-driven view of parallel coding lanes. Coding lanes use separate Git
branches/worktrees, provider-neutral `orchestrator-worker-invocation/v1` and retained
`orchestrator-coding-invocation/v1` input identities, validated branch-tip results, and opaque
exclusive named resources. The persistent manager owns planning, split/merge ordering, launches, decisions,
acceptance, promotion, and cleanup.

The canonical source product root and launch directory is `harness-single/`. Local operator
configuration is the paired, ignored `local-config/harness-config.json` and
`local-config/resource-manifest.json` surface. Product-root discovery stops at source markers even
when that pair is missing; it must not capture configuration from an ancestor directory. A
same-root pair remains a compatibility fallback only, and records from the two locations are
never mixed.

Both supported input shapes feed the same lane-management/controller path. The removed schema-less
firmware shape is unsupported and fails ordinary invocation validation; no compatibility rejection
branch exists for it.

## S4 host delivery and lifecycle boundary

The S4 boundary adds one versioned capability-selected host interface. The
Codex profile is implemented; future providers are contract fixtures only.
Core delivery consumes the exact S3 manager binding (`run_id`, `queue_id`,
manager session/thread, registration ID and generation) and never branches on
provider names.

The harness-owned `DeliveryCoordinator` persists registration and replay state
beside, but does not duplicate, the S3 queue. A wake creates a bounded
`DeliveryNotice` containing identity, queue revision, pending count, highest
class/severity, timestamp, and adapter profile. It contains no event payload.
`DeliveryReceipt` is transport evidence; only a separate manager action may
create `ManagerEventAck` and call the S3 acknowledgement boundary. Wake
attempts are at-least-once, coalesced, bounded-retry, and exact-binding.

Codex command hooks are synchronous. PostToolUse and Stop are safe-boundary
integration points, while a persistent coordinator owns the subscription and
replay. App Server fixtures use `thread/inject_items`, `turn/completed`, and
`turn/start` for idle continuation. Delivery never changes an active turn.
Project trust and hook review are reported honestly rather than silently
granted by installation.

The diagnostic observer retains scan and blocking waits. Foreground managed
watcher, heartbeat/re-arm, attention-sprint/timeline/integrity, and competing
pending-notification policy are not S4 runtime behavior. Optional watcher
recovery is only `open`, `acknowledged`, or `resolved`.

Static work may allocate an exact full-commit read-only source view with
separate writable result/cache roots. Terminal lanes use archive-first
retirement: copied and hash-bound task/result/findings/acceptance/transcript/
dependency/process evidence, retained revision, clean/no-live/no-unmerged proofs,
and discarded cache inventory are validated before normal Git worktree close.
Any failed proof leaves the terminal lane visible.

The watcher is a monitor, not another manager. It must make crashes, stale state, permission
requests, helper expiry, checkpoints, provider waits, results, duplicated controllers, and resource
conflicts visible promptly. The main orchestrator remains the only authority that schedules work,
changes leases, publishes relays, recovers lanes, and classifies failures.

## Coding operating loop

```text
main orchestrator freezes a stable base and plans branch/worktree lanes
  -> launches manager-selected lanes and lets exact named locks serialize contention
  -> waits in a bounded watcher exit-on-event call
  -> reviews and handles the event
  -> acknowledges the exact native event ID and reconciles every lane
  -> repeats
  -> launches a merge lane, runs project checks, accepts and promotes the candidate
```

`PARALLEL_CHECKPOINT.md` records resumable progress. `RESULT.json` is merge-ready only when its
lane, invocation, branch, full current tip, checks shape, and clean-worktree evidence validate.
The harness does not execute reported checks or infer a dependency graph.

## Functional requirements

### R1. Suite-local discovery

1. Accept a JSON configuration naming the suite root, run globs, run workspace path, watcher output
   directory, poll interval, and warning thresholds.
2. Discover controller status files under configured run workspaces.
3. Group controller attempts by run plus canonical doer and identify the latest attempt without
   discarding older simultaneously live attempts.
4. Discover Codex JSONL, permission request, relay, helper-process, checkpoint, and result files
   associated with each run.
5. Work with the request schemas present in configured run workspaces.

### R2. Truthful process reconciliation

1. Capture one OS process snapshot per scan.
2. Record PID, parent PID, name, command line, and creation time when the platform exposes it.
3. Treat a process identity as PID plus normalized creation time. New status records must carry
   separate `controller_started_utc` and `codex_started_utc` values captured from the process
   provider; a shared `started_utc` is chronology only and cannot prove either identity.
   The configured comparison tolerance is capped at two seconds for provider precision. Reconcile
   liveness, expected parent identity, and terminal JSONL events.
4. Never treat a raw `state="running"` declaration as authoritative.
5. Derive explicit operational states:
   `RUNNING_CODEX`, `WAITING_RESOURCE`, `WAITING_RELAY`, `HELPER_RUNNING`, `STALE_STATUS`, `CHECKPOINTED`,
   `TERMINAL_RESULT`, `EXITED`, or `UNKNOWN`.
6. Detect multiple live controller attempts for the same doer/run.
7. Support dependency injection of a fake process provider for deterministic tests.
8. If the process snapshot is incomplete, CIM fails, a required creation time is unavailable, or
   a parent identity cannot be proved, derive `PROCESS_STATE_UNKNOWN`. Never infer absence,
   staleness, lease release, or safe relaunch from incomplete process evidence.
9. A declared terminal status is not proof of exit. A live recorded controller/Codex identity
   makes it `STALE_STATUS`; unknown identity evidence makes it `PROCESS_STATE_UNKNOWN`; only
   complete evidence that every recorded identity is absent or replaced permits `EXITED`.

### R3. Request and relay monitoring

1. Read every candidate request with a bounded stat/read/stat stability check. Retry finitely when
   size, modification time, or file identity changes. Hash the stable bytes actually read; an
   optional SHA-256 sidecar is only a consistency check.
2. Distinguish request files from relay files without relying on only one naming convention.
3. Extract request identity, run/session identity, board/probe identity, live-lifetime PIDs,
   explicit relay path, creation time, and expiry/deadline data when present.
4. Use an explicit relay path only to locate a candidate. A relay counts as bound only when its
   contents match the watcher-computed request SHA-256 and the available live producer identity
   such as run/session/lifetime. Missing or contradictory bindings produce
   `RELAY_UNBOUND`/`RESOURCE_AMBIGUOUS`, never a relayed state.
5. Emit `RELAY_READY` only when the request is unrelayed and its declared producing lifetime is
   live.
6. Emit `REQUEST_STALE` when an unrelayed request's producing lifetime is provably absent.
7. Report expiry warnings when a finite deadline is known.
8. Never create, modify, approve, or delete a request or relay.
9. An unstable or partially written request produces an observation error and never
   `RELAY_READY`.
10. Every declared PID in a request lifetime is required. Emit `RELAY_READY` only when all are
    live; mixed live/absent/mismatched evidence is ambiguous. A bound relay whose producer later
    exits becomes `RELAYED_INACTIVE`, not a second `RELAYED` event.

### R4. Lane and resource events

Emit concise machine-readable events on state changes, including:

- controller/Codex/helper/MCP start or exit;
- stale controller state or PID/parent/creation-time mismatch;
- terminal Codex JSONL event;
- new, changed, relayed, stale, or expiring request;
- checkpoint, provider wait, completion, or result;
- `RESOURCE_RELEASE_POSSIBLE` only after every recorded controller, Codex, helper, and MCP
  identity is absent/replaced and ownership is unambiguous;
- duplicate controller;
- conflicting or ambiguous ownership of boards, probes, serial/VCOM endpoints, radio peers or
  channels, mutable state/artifact roots, and MCP lifetimes;
- malformed or ambiguous observed data.

Events must identify the run, doer, task, phase, relevant path, exact hash, relevant PIDs, and
reason when available.

Process state, request state, helper/MCP state, checkpoint/result presence, provider wait, and
release candidacy are independent conditions. A checkpoint or result must never mask
`CONTROLLER_EXITED`; helper/MCP exit and `RESOURCE_RELEASE_POSSIBLE` must remain observable
separately. The event never changes or releases a lease.

MCP lifecycle evidence is explicit, never inferred from a generic request `run_id`. It must come
from an explicit MCP/provider process record or an explicitly MCP/provider-owned request lifetime,
and it must correlate to the exact declared server plus lane/session. Evidence from another lane,
session, or server cannot satisfy or suppress an MCP observation.

### R5. Bounded event-driven interface

Provide:

1. `scan`: perform one reconciliation and print a JSON snapshot.
2. `watch --once`: compare one scan with the persisted watcher observation and emit changed events.
3. `watch --until-event`: poll without model involvement and return immediately on the first
   material event batch.
4. A finite timeout and distinct timeout exit code.
5. JSON Lines output suitable for the main orchestrator.
6. Atomic watcher-owned `snapshot.json` and append-only `events.jsonl`.
7. An explicit `--no-write` mode.
8. Stable deterministic event IDs. On the first recorded watch, emit the current material state.
9. At-least-once crash consistency: append and flush/fsync events before advancing the persisted
   snapshot cursor. A crash may cause the same event ID to be emitted again but must not lose an
   event; consumers deduplicate by event ID.
10. Diagnostic output never creates a pending-notification selector or acknowledgement state.
    Current actionable delivery, coalescing, retry, and acknowledgement are owned exclusively by
    the accepted S3 ManagerEventRouter binding and its separate manager action boundary.

The watcher cannot revive an inactive conversation. The main orchestrator must remain in a bounded
scheduling epoch and invoke the exit-on-event interface again after handling each event.

### R6. Read-only and fail-closed behavior

1. All writes must stay beneath the configured watcher output directory.
2. Never write under `fresh-experiments`, `.agent-workspace`, an external provider runtime, or any observed
   lane directory.
3. Never launch, resume, stop, signal, or kill a lane, helper, MCP process, or agent.
4. Never invoke external provider tools or modify server code.
5. Never assign/release a lease, write a permission relay, classify a production defect, or edit
   evidence.
6. Malformed files produce observation errors, not destructive recovery.
7. Reject alternate data stream syntax and every symlink, junction, mount/reparse point, or other
   indirection in existing output-path components. Revalidate containment and components
   immediately before temporary-file creation and immediately before `os.replace`.
8. Reject output roots that overlap the suite's observed runs, `.agent-workspace`,
   `fresh-experiments`, or an external provider runtime. Detect replacement of the validated output root
   before every write.
9. The default configuration must be safe for a stopped suite.

### R7. Portability and dependencies

1. Use Python 3.11+ standard library only.
2. Support Windows process discovery through one fixed, bounded, argument-free PowerShell/CIM
   introspection subprocess. No configuration-derived text may enter its command. This is the only
   workload-external process the watcher itself may launch.
3. Support Linux process discovery through `/proc` where practical.
4. The production watcher must not require WSL, a service installation, administrator rights, or
   a database. The explicitly invoked real-agent acceptance test may require WSL2 for its outer
   OS-enforced isolation boundary.

## Nonfunctional requirements

- Small enough to audit; prefer clear modules over a single opaque script.
- Deterministic snapshots with stable ordering.
- Atomic state writes and tolerant reads of concurrently replaced files.
- Bounded file reads; tail JSONL rather than loading unbounded logs.
- Clear error messages and documented exit codes.
- No busy model polling.
- No dependency on an external provider service.

## Test requirements

### Host-only unit tests

Cover:

1. running controller with valid parentage;
2. missing controller or Codex child;
3. stale `running`;
4. terminal JSONL with delayed controller status;
5. PID reuse/creation-time mismatch;
6. missing process timestamps, parent PID reuse, and partial/CIM-failed snapshots;
7. normal controller exit;
8. stable request reads, concurrent replacement, and exact computed hash;
9. matching relay plus stale relay at a reused explicit path;
10. request bytes changing after a prior event;
11. stale request;
12. finite expiry warning;
13. malformed JSON and malformed sidecar;
14. checkpoint and result transitions;
15. duplicate controllers;
16. conflicting and ambiguous board/probe/serial/radio/root/MCP ownership;
17. stable event deduplication;
18. crash between event-log fsync and snapshot-cursor update;
19. atomic state persistence and restart behavior;
20. output containment, junction/reparse escape, output-root replacement, and ADS rejection;
21. Windows and synthetic process-provider parsing;
22. process-launch monkeypatch proving that only the fixed Windows introspection command is
    allowlisted;
23. missing separate controller/Codex creation timestamps and over-wide tolerance rejection;
24. a multi-PID request with one live and one exited producer remaining ambiguous;
25. independent controller/helper exit, checkpoint, provider-wait, and resource-release events;
26. cleanup after a root launcher exits while a cgroup descendant remains;
27. a declared terminal status with a live child or incomplete inventory;
28. explicit MCP active/exit/unknown events and a declared MCP name without PID evidence blocking
    release candidacy.

### Host-only integration tests

Launch harmless local Python controller/child/helper processes in temporary directories and prove:

1. the watcher observes real PID ancestry;
2. a synthetic request produces `RELAY_READY`;
3. an externally written synthetic relay advances the helper;
4. process exit produces a release/stale transition;
5. no observed lane file is modified.

### Real-agent dry-run test

Launch one real Linux Codex CLI agent in a temporary harness-owned synthetic run inside an
OS-enforced WSL2/bubblewrap boundary, outside every real suite run/server/state root, with an
explicit host-only prompt. The trusted outer WSL supervisor may see harness source and the host
evidence destination, but the agent mount namespace contains only a read-only Linux runtime, a
pinned read-only Codex release, a writable synthetic workspace, and a writable ephemeral
`CODEX_HOME`. It must not contain `/mnt/c`, the repository, WSL home/root data, an external
provider service, or host devices.

Run the agent as `nobody` with all capabilities dropped in fresh mount, user, PID, IPC, UTS, and
cgroup namespaces. Put the complete bubblewrap/init/Codex/helper tree in a dedicated cgroup-v2
subtree with explicit PID, memory, and CPU ceilings. A trusted supervisor outside that cgroup must
discover the actual outer Codex PID by pinned executable identity, record its complete ancestry,
and always use `cgroup.kill` before proving the cgroup empty, even if the root launcher already
exited. A fixed trusted launcher joins the cgroup and then `exec`s; never use `preexec_fn` after
starting proxy threads. An outer guard keyed by the run ID must remove the cgroup, network
namespace, WSL-local workspace, and credential even when setup or evidence preservation fails.

Give the agent no refresh token, valid ID bearer token, API key, or durable host credential.
Synthesize its ephemeral auth file from only the current non-refreshable access token, account
identity, and—only when the pinned CLI parser requires the field—an already-expired ID-token-format
value. Record expiries without recording tokens, scan/redact all preserved evidence for source
credential values, and delete the WSL-local auth copy after the run.

Use a dedicated network namespace with no default route. Its nftables default-deny policy may
reach only one host-side CONNECT proxy port. The proxy may connect only to the exact official
OpenAI HTTPS endpoints required by Codex; direct host, LAN, metadata, DNS, and arbitrary Internet
access must fail in a preflight. Use an empty effective MCP configuration and scrub every
hardware/vendor environment variable.

Within that outer containment, use `danger-full-access` with `approval_policy="never"` so the
deterministic Python helper is not silently declined by Codex's workspace command policy. This
inner unrestricted setting is not hardware authorization and cannot escape the OS boundary.
Fail closed before launch if every containment fact cannot be proved. The agent must run a
provided deterministic synthetic lane helper that:

1. writes a non-hardware synthetic request;
2. waits within the same Codex turn for a synthetic relay written by the test driver/main
   orchestrator;
3. writes a checkpoint and exits.

The watcher must independently observe controller start/exit, helper start/exit, `RELAY_READY`,
the single live `RELAYED` transition, `RELAYED_INACTIVE`, the checkpoint, and
`RESOURCE_RELEASE_POSSIBLE` (an observation, never an automatic lease release). Verify outer-PID
ancestry and cgroup membership, preserve the proxy
allow/deny audit, and verify that no MCP/provider child appears or MCP lifecycle event is emitted.
The test must not inspect or mutate an external provider service, invoke external tools, reserve a
real resource, or operate hardware.

## Acceptance criteria

1. Every host-only unit and integration test passes.
2. The real-agent dry-run passes or, if the external model service is unavailable, the exact
   provider failure is preserved and the local agent-controller integration still passes. The
   harness itself may not be called green until a real-agent run succeeds.
3. A live-tree scan of the stopped current suite reports stale declarations truthfully without
   changing any suite file.
4. Repeating an unchanged scan emits no duplicate material events.
5. `--until-event` returns promptly for a synthetic event and returns the documented timeout code
   when nothing changes.
6. Tests prove the watcher cannot write a relay, mutate a lane, or launch/kill any lane, agent,
   helper, MCP, provider, or other workload process. Only the fixed, bounded Windows
   introspection subprocess is permitted.
7. An independent adversarial review finds no blocking safety, liveness, or correctness defect.

## Explicit non-goals

- General-purpose autonomous coordinator.
- Autonomous scheduler or lease allocator.
- Automatic hardware approval or relay publication.
- Automatic process cleanup, restart, or crash recovery.
- Production-server modification.
- Provider/test execution.
- Rewriting historical controller files.
- Waking an entirely inactive conversation.

### R8. External operator launch boundary

A separate manager-invoked operator utility may detach a long-lived owner, optional watcher, or
lane controller and atomically record its exact PID-plus-creation identity. It is outside the
watcher execution path: the watcher must not import or invoke it, and the utility must not
schedule, acknowledge, grant leases, operate hardware, or kill processes.
