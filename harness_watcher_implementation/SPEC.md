# Harness Watcher specification

## Purpose

Add an optional, passive deterministic watcher service around `orchestrator_harness`. It observes
manager, harness, and lane logs every five minutes. Outside M5, an optional isolated evaluator may
ask one explicitly configured role to classify only clear orchestration defects. The watcher has
no built-in evaluator provider, model, effort, tier, or command.
The service itself is not an AI subagent. In M5 its evaluator is disabled, so it only records and
diagnoses evidence and cannot notify or wake the manager. Healthy work continues without
interruption. When separately authorized evaluation is enabled outside M5, a confirmed defect becomes retained
local diagnostic evidence; the main orchestrator alone
pauses work, repairs the harness, and resumes the suite.

## Required components

1. `settings.py` exports the global `harness_watcher_active = True`.
2. Every watcher integration checks that flag at runtime. When false:
   - no watcher service, polling, evaluation, alert, or watcher-specific logging occurs;
   - the original `orchestrator_harness` commands and behavior remain unchanged;
   - watcher commands return a truthful disabled/no-op result.
3. A background-service CLI starts, inspects, and cooperatively stops one watcher process. The
   service:
   - polls at 300-second intervals in production;
   - does not mutate experiment evidence, leases, relays, agents, controllers, hardware, or the
     production server;
   - exits if disabled or cooperatively stopped;
   - records PID plus process creation identity so stale/reused PIDs are not trusted.
4. Each poll reads only configured, bounded log tails and builds an immutable review packet.
5. The deterministic watcher service has no model role. Its optional evaluator requires an
   explicit `evaluator_command` and `evaluator_identity` before it can be enabled and is isolated
   behind an interface so host tests can use deterministic fakes. When enabled, a verdict must
   conform to a strict schema and cite observed evidence. Counted M5 sprints set
   `evaluator_enabled: false`.
6. Detection admits only meaningful harness defects:
   - repeated/looping/redundant stages;
   - progress starvation beyond the configured bound;
   - regression to a previously completed stage;
   - manager monitoring/notification/coordination/instruction failures;
   - clearly defective or inefficient harness behavior.
   Ordinary long work, a hardware/provider wait, a single retry, unchanged healthy work, and a
   firmware/server/test failure are not watcher defects.
7. Optional local alerts are durable, stable-ID and deduplicated. They never enter the managed
   queue or wake ROOT. Local acknowledgement records receipt of diagnostic evidence only.
8. Recovery is manager-driven and ordered:
   `STOP_ASSIGNING -> CHECKPOINT_REQUESTED -> PAUSED -> REPAIRED -> RESUMED -> RESOLVED`.
   The legacy local alert API only records/validates these bookkeeping transitions. It never
   performs recovery actions. Native diagnostic projection uses open/acknowledged/resolved;
   neither ledger is a managed acceptance or shutdown requirement.
9. After `RESOLVED`, the alert is no longer actionable and the watcher returns to passive
   monitoring.

## Logging contract

Runtime logs live below a fresh repository-root `runtime/harness-watcher/<epoch>/` directory:

```text
runtime/harness-watcher/<epoch>/
├── orchestrator/
├── harness/
├── watcher/
└── subagents/
    └── <safe-subagent-name-or-id>/
```

All records are append-only canonical JSONL. Each contains schema, UTC timestamp, source category,
source ID, event type, and structured data. Routing is determined by validated source metadata, not
by caller-controlled paths. The logging library must be process-safe enough for independent
appenders and must never write outside this tree.

The service automatically records:

- harness observations and actionable-event lifecycle;
- manager heartbeats, acknowledgements, and recovery transitions available in manager logs;
- watcher polls, evaluator outcomes, alerts, and service lifecycle;
- per-lane/subagent progress derived from reconciled harness records.

An explicit logger API is also available to the orchestrator and permitted external worker
controllers for events not observable from existing logs. It is not a harness wrapper or wake
path.

## M5 diagnostic-only profile

Counted M5 sprints run the deterministic watcher with `evaluator_enabled: false`. It records and
analyzes evidence but does not launch a model, notify or wake the manager, acknowledge requests,
make decisions, or repair anything.

For M5, the 90-second request window is a diagnostic target rather than an automatic failure. A
longer request is acceptable only when a gap-free canonical interval covers the actual overrun
from the delivery deadline through actionability and proves genuine manager work or handling an
earlier genuine request. Late harness observation, a native blocking wait, otherwise-idle delay,
and partial, overlapping, contradictory, or unknown activity are not valid reasons.

## Diagnostic evidence and authority

Optional alerts retain stable IDs, severity, summary, implicated roles, cited source paths,
hashes/offsets and configured evaluator identity. They remain local diagnostic evidence.
`orchestrator_harness.watcher_integration` does not create `HARNESS_WATCHER_ALERT` queue
conditions or route native acknowledgement to the watcher. The watcher cannot add a product
termination gate. The default evaluator is disabled and no evaluator topology is enabled by
an implementation review.

## Interfaces and configuration

Configuration is explicit: select observations and a fresh service epoch under repository
`runtime/`, disjoint from inputs and the configuration file. No-config calls do not guess an
external workspace or write among source trees. Start serializes admission with a kernel lock,
preserves stopping/live/unobservable owners, and refuses a used service epoch. Status keeps
live stopping processes running until exact exit is proven. Ownership tokens guard service
sleep and terminal writes; native readiness identifies the launched child.

Ordinary polling preserves partial byte records, respects the sampled interval, serializes
cursor updates and deduplicates routed raw-byte identities on crash replay. Every output family
uses shared guarded file operations. Windows native mutation is covered by current fixtures;
POSIX mutation support is an unresolved compatibility obligation, not a portable PASS.

- Python package and CLI: `python -m harness_watcher_implementation ...`
- Production poll interval defaults to exactly 300 seconds.
- Config declares observed log roots, runtime/log root, evaluator command, no-progress threshold,
  bounded tail sizes, and the matching harness config/output.
- No command commits, pushes, deploys, flashes, resets, or operates hardware.

## Validation

1. Unit tests cover routing, path safety, feature-off no-op behavior, polling, strict verdict
   validation, deduplication, local recovery bookkeeping, and resolution without native queue delivery.
2. Integration tests prove the original harness behavior is byte/exit-equivalent where applicable
   when disabled and that enabled healthy observations do not produce alerts.
3. A board-free lifecycle canary proves duplicate-start rejection, exact PID/creation identity
   reporting, cooperative stop, and owner-identity loss for the watcher subprocess.
4. Practical suite canaries were retired with the scripts that ran them; they are historical
   only and are not a current runnable asset.
5. Reviewer and independent tester results are retained under
   `runtime/merge-validation/`.

## Non-goals

- autonomous scheduling, approval, lease control, process killing, hardware action, or server edit;
- diagnosing ordinary firmware/server/test failures;
- replacing the existing harness or main orchestrator;
- speculative heuristics that interrupt plausible healthy work;
- a detached service that survives its owning manager indefinitely.
