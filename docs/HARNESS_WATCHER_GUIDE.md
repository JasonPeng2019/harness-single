# Harness Watcher operating guide

> The watcher is an optional diagnostic around the canonical coding workflow. It does not launch
> providers, manage lanes, or replace the harness event loop. For operation, start with
> `QUICK_START.md` and keep evaluation disabled unless a separately reviewed configuration enables it.

The Harness Watcher is an **optional deterministic, read-only diagnostic service** around the
required `orchestrator_harness`. It is not an AI subagent, suite manager, wake bridge, or replacement
for the managed harness event loop. It tails configured logs, records health and attention evidence,
and can durably publish a diagnosed harness alert when its optional evaluator is enabled.

The implementation is under `harness_watcher_implementation/`; each live epoch writes only under
`runtime/harness-watcher/<epoch>/`. When evaluation is disabled, the watcher only collects deterministic
diagnostics. A separate result reviewer may inspect retained evidence after the run has fully
stopped; it is not part of the watcher or live runtime.

## Roles and authority

| Component | Responsibility | Durable record |
|---|---|---|
| Main orchestrator | Owns scheduling, resource leases, agent instructions, exact permission relays, evidence acceptance, failure classification, repair barriers, and recovery decisions. | `runtime/orchestrator-harness/<epoch>/MANAGER_LOG.jsonl`, current suite ledgers, and the affected run's status/evidence. |
| `orchestrator_harness` | Read-only reconciliation and durable actionable-event delivery. It observes lanes and process identities but never schedules, approves, leases, kills, flashes, or edits evidence. | The fresh epoch output directory named by its config, including `snapshot.json`, `events.jsonl`, and `pending-notification.json`. |
| Harness Watcher | Deterministically tails bounded new log data and writes health/attention diagnostics. When evaluation is enabled, it validates a packet-bound verdict and may publish a durable `HARNESS_WATCHER_ALERT`; in diagnostic-only mode it never invokes a model or publishes evaluator-derived alerts. | `runtime/harness-watcher/<epoch>/watcher/`, including events, cursor/state, alerts, lifecycle, and recovery history. |
| Experiment workers | Explicitly configured provider/model/preferences. Each owns one catalog task at a time and reports progress, checkpoint, help, and completion signals; they never manage another lane or edit the production server. | Their experiment root plus `runtime/harness-watcher/<epoch>/subagents/<safe-id>/`. |
| Experiment reviewer | Explicitly configured, read-only against its assigned run. Reviews the specification and retained evidence; it does not operate hardware or repair the server. | The assigned run's review/evidence records and routed subagent log. |
| Server-repair roles | Explicitly configured roles used only after the main orchestrator independently validates a production-server defect and opens one serialized change-loop. | The owning run's server-repair records and `.agent-workspace/SERVER_REPAIR_QUEUE.md`. |

Every component logs actions rather than relying on chat memory. The main orchestrator records each
launch, instruction, notification, decision, lease change, checkpoint, acknowledgement, repair
transition, and completion decision. The harness records observations and durable events. Each
subagent records task/phase, progress, requests, checkpoints, and result. The watcher records the
exact source packet it evaluated, evaluator outcome, alert lifecycle, and manager recovery
transitions.

## Log routing

The logger routes records by declared source:

```text
runtime/harness-watcher/<epoch>/
|-- orchestrator/
|-- harness/
|-- watcher/
`-- subagents/
    `-- <subagent-name-or-id>/
```

The production configuration must observe all relevant source classes:

- the manager JSONL as role `orchestrator`;
- the active `orchestrator_harness` epoch's event JSONL as role `harness`;
- lane/subagent JSONL as role `subagent`.

Harness events that identify a lane are also mirrored into that lane's subagent directory while the
original harness copy is retained. Source IDs are sanitized for filesystem use. Logs are append-only
JSONL; cursor/state files use atomic replacement.

## Feature flag and passive behavior

The global gate is:

```python
# harness_watcher_implementation/settings.py
harness_watcher_active = True
```

Watcher CLI, polling and diagnostic logging integrations check
this flag at runtime. With `False`, watcher operations are no-ops and the original
`orchestrator_harness` path continues unchanged. The mandatory managed harness remains active in
either mode.

Healthy operation is silent. Unchanged logs are not repeatedly evaluated before the configured
no-progress threshold, and one unchanged generation is not evaluated over and over. A normal long
build, a provider/resource wait, a single retry, a provider failure, a test failure,
or a server defect is **not** by itself a harness defect.

## What qualifies as a harness problem

A watcher alert requires cited evidence of the orchestration machinery being defective, such as:

- a subagent repeating the same stage or work without useful progress;
- oscillation, regression, or redundant work caused by orchestration;
- a lane starved even though its prerequisites and leases are satisfied;
- the manager failing to inspect, notify, coordinate, or instruct active lanes;
- duplicate ownership, broken event delivery, or another clearly inefficient harness behavior.

When the optional evaluator is enabled, it must return strict schema output bound to the exact
source path, SHA-256, and offset packet. Missing, malformed, stale, or mismatched evidence is
rejected rather than converted into an alert. Alerts are deduplicated while unresolved. With the
evaluator disabled, this entire evaluation/alert path is a no-op; deterministic logging and health
evidence continue.

## Retained diagnostics and manager authority

Watcher observations and optional evaluator alerts are retained local evidence. They never
enter the managed notification queue, acknowledge a native request, wake ROOT, release a
resource, or impose a completion/termination obligation. ROOT discovers actionable requests
through the native blocking wait and decides how to handle diagnostic evidence.

The optional local alert APIs preserve stable IDs, deduplication, acknowledgement and the
ordered historical recovery ledger. Those records are operator bookkeeping; they do not
operate lanes or drive managed acceptance. The native diagnostic recovery projection has
only `open`, `acknowledged` and `resolved` states. Neither projection nor the historical
six-step ledger is a required managed shutdown gate. Evaluation remains disabled unless
its separate topology is explicitly authorized and configured.

## Evaluator modes and diagnostic-only operation

`evaluator_enabled` is independent of the global watcher feature flag. The normal default is
`false`: the watcher remains deterministic and diagnostic-only unless a configuration explicitly
sets the field to `true`. In passive mode it still reads sources, advances cursors, ingests
attention, writes reports and health/POLL evidence, and detects owner loss, but it never launches
or invokes an evaluator and emits no evaluator-derived alert. A post-sprint reviewer examines
retained evidence only after cleanup.

Diagnostic attention analysis also consumes passive `HARNESS_EVENT_INELIGIBLE` records for
`ALREADY_ANSWERED`, `INVALID_LANE_ID`, and `LANE_NOT_LIVE`. These records explain why a signal was
correctly non-actionable; they never notify the manager. Exact correlation, ordering, uniqueness,
and absence of an actionable contradiction are mandatory, so malformed evidence fails closed.

```json
{"evaluator_enabled": false, "runtime_root": "runtime/harness-watcher/<epoch>", "poll_interval_seconds": 300, "observed_sources": [{"path": "runtime/orchestrator-harness/<epoch>/events.jsonl", "role": "harness", "source_id": "managed-harness"}]}
```

Start using the owner process identity, then inspect status and stop cooperatively:

```powershell
python -m harness_watcher_implementation --config <epoch-config> start --owner-pid <pid>
python -m harness_watcher_implementation --config <epoch-config> status
python -m harness_watcher_implementation --config <epoch-config> stop
```

READY is published after startup initialization completes and before the first poll, so it marks
the service-ready boundary rather than useful activity; the first poll and its evidence follow.

Proof is the service JSON's `evaluator_enabled:false` through READY and terminal state plus
`SERVICE_STARTED`, `POLL` (`evaluator_enabled:false`), and `EVALUATOR_SKIPPED` records under the
watcher runtime. If READY is not published, confirm the owner PID/creation identity and config
paths; if stop does not complete, inspect the exact service identity and wait for its cooperative
terminal record—do not kill an unrelated process.

For diagnostic scoring, 90 seconds is a diagnostic target rather than an automatic failure. A longer
request is acceptable only when the canonical attention timeline contains one complete, gap-free
chain proving genuine manager work or handling an earlier real request. Late harness observation,
native-wait delay, otherwise-idle delay, and partial or contradictory activity are not valid
reasons for an overrun.

For each counted blocked request, worker preparation, publication, and waiting are distinct passive
evidence boundaries. The worker records `AGENT_SIGNAL_CREATED`, captures exact UTC immediately
before the atomic final rename, records `AGENT_SIGNAL_PUBLISHED` with that captured source time,
then records `AGENT_WAIT_STARTED`. The watcher measures native observation latency from publication,
not creation. When blocked-signal harness-delay attribution depends on this boundary, missing,
duplicate, mismatched, reversed, or post-deadline publication evidence is
`INSUFFICIENT_EVIDENCE`; the watcher must not guess harness delay. None of these records discovers,
notifies, or wakes the manager.

The attention report's explicit deferral duration uses only the latest deferral at or before the
selected pending endpoint. A later deferral produces no duration rather than a negative value;
other reversed causal stages remain insufficient evidence.

## Starting and stopping

Use an explicit configuration file, normally copied from `examples/watcher.diagnostic.example.json`
into `local-config/`, with a fresh selected epoch. Omitted configuration, source-root output and
output overlapping configured observations/configuration are rejected before publication.
A used service epoch cannot be restarted; choose a fresh runtime. Manual diagnostic polling
before the first service start does not discard its existing cursor.

Startup admission uses a persistent kernel-owned lock, released by process exit rather than
by deleting a claim marker. Startup checks existing ownership under that lock. An exact live
watcher remains running while a stop request is pending; a new start preserves that request.
An unobservable or damaged owner is a reconciliation error, never proof of absence. Status
reports `identity_unproven` separately. Service sleeps and terminal publication check the
startup token and watcher identity, and readiness must identify the launched native child.

Polling serializes cursor publication, reads only the sampled byte interval, and advances
through complete newline-terminated records. Partial UTF-8/JSON bytes remain for the next
poll; intentional bounded-tail loss is logged. Routed records have stable raw-byte identities
so replay after output persistence but before cursor publication does not duplicate events.
Corrupt cursors and uncertain source reads preserve committed evidence. All output families
use the harness's guarded file primitives; native mutation enforcement currently has a
Windows backend. POSIX write support remains an implementation/compatibility gap and must
not be credited as verified portable support.

First configure and validate the required `orchestrator_harness` exactly as described in the
portable root `README.md` and `orchestrator_harness/README.md`. Then copy
`examples/watcher.diagnostic.example.json` to a fresh epoch-specific local config and update its
source paths to the current manager, harness, and lane logs. For the minimal no-relay topology keep:

- `poll_interval_seconds: 300`;
- a fresh isolated `runtime_root` under `runtime/harness-watcher/<epoch>`;
- `evaluator_enabled: false`, so no AI evaluator or watcher subagent is launched;
- a no-progress threshold longer than one ordinary review interval.

Use a durable owner identity rather than a transient shell:

```powershell
python -m harness_watcher_implementation --config <watcher-config> start --owner-pid <main-owner-pid>
python -m harness_watcher_implementation --config <watcher-config> status
```

Useful operator commands:

```powershell
python -m harness_watcher_implementation --config <watcher-config> poll
python -m harness_watcher_implementation --config <watcher-config> stop
```

`start` rejects a duplicate live watcher. `stop` is cooperative. The service also terminates when
its exact owner identity disappears and records the terminal reason/time. At epoch end, stop the
harness and watcher cooperatively and confirm their exact PID-plus-creation identities are absent; do not kill
processes by broad command-line matching.

## Validation

Historical run evidence was intentionally not copied and no stored test counts are shipped.
Re-run the focused commands named in the portable root `README.md` after changing watcher or
harness code. Fixture and example configurations are validation scaffolding, not product examples,
and static checks never prove live provider or OS-enforcement behavior.
