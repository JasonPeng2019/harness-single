# Harness Watcher specification

## Purpose

Add an optional, passive deterministic watcher service around `orchestrator_harness`. It observes
manager, harness, and lane logs every five minutes. Outside M5, an optional isolated evaluator may
ask one persistent `gpt-5.6-terra` high-reasoning role to classify only clear orchestration defects.
The service itself is not an AI subagent. In M5 its evaluator is disabled, so it only records and
diagnoses evidence and cannot notify or wake the manager. Healthy work continues without
interruption. When evaluation is enabled outside M5, a confirmed defect becomes one durable alert
delivered through the existing managed-harness notification path; the main orchestrator alone
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
5. The deterministic watcher service has no model role. Its optional evaluator is configured as
   `gpt-5.6-terra`, high reasoning, and isolated behind an interface so host tests can use
   deterministic fakes. When enabled, a verdict must conform to a strict schema and cite observed
   evidence. Counted M5 sprints set `evaluator_enabled: false`.
6. Detection admits only meaningful harness defects:
   - repeated/looping/redundant stages;
   - progress starvation beyond the configured bound;
   - regression to a previously completed stage;
   - manager monitoring/notification/coordination/instruction failures;
   - clearly defective or inefficient harness behavior.
   Ordinary long work, a hardware/provider wait, a single retry, unchanged healthy work, and a
   firmware/server/test failure are not watcher defects.
7. Alerts are durable, stable-ID, deduplicated, and redelivered until acknowledged through the
   existing managed-harness notification mechanism. The harness gains only a conditional read of
   watcher alerts and never lets the watcher act as authority.
8. Recovery is manager-driven and ordered:
   `STOP_ASSIGNING -> CHECKPOINT_REQUESTED -> PAUSED -> REPAIRED -> RESUMED -> RESOLVED`.
   The watcher only records/validates transitions. It never performs them.
9. After `RESOLVED`, the alert is no longer actionable and the watcher returns to passive
   monitoring.

## Logging contract

Runtime logs live below a fresh repository-root `harness_watcher/<epoch>/` directory:

```text
harness_watcher/<epoch>/
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

## Alert delivery

An unresolved watcher alert is exposed by `orchestrator_harness` as
`HARNESS_WATCHER_ALERT` only when `harness_watcher_active` is true. It uses the original durable
pending-notification and exact-event acknowledgement path. Alert data includes:

- stable alert/event IDs;
- severity and concise summary;
- implicated roles/lanes;
- cited log paths, hashes/offsets, and signatures;
- the required safe-response sequence;
- watcher model/reasoning metadata.

Acknowledging the harness notification means “manager received it,” not “fixed.” Resolution
requires the ordered recovery ledger to reach `RESOLVED`.

## Interfaces and configuration

- Python package and CLI: `python -m harness_watcher_implementation ...`
- Production poll interval defaults to exactly 300 seconds.
- Config declares observed log roots, runtime/log root, evaluator command, no-progress threshold,
  bounded tail sizes, and the matching harness config/output.
- No command commits, pushes, deploys, flashes, resets, or operates hardware.

## Validation

1. Unit tests cover routing, path safety, feature-off no-op behavior, polling, strict verdict
   validation, deduplication, alert redelivery, recovery ordering, and resolution.
2. Integration tests prove the original harness behavior is byte/exit-equivalent where applicable
   when disabled and that enabled healthy observations do not produce alerts.
3. A board-free lifecycle canary proves duplicate-start rejection, exact PID/creation identity
   reporting, cooperative stop, and owner-identity loss for the watcher subprocess.
4. Practical suite canaries were retired with the scripts that ran them; they are historical
   only and are not a current runnable asset.
5. Reviewer and independent tester results are retained under
   `harness_watcher_implementation/test_results/`.

## Non-goals

- autonomous scheduling, approval, lease control, process killing, hardware action, or server edit;
- diagnosing ordinary firmware/server/test failures;
- replacing the existing harness or main orchestrator;
- speculative heuristics that interrupt plausible healthy work;
- a detached service that survives its owning manager indefinitely.
