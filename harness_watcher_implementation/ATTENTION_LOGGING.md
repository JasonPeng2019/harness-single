# Manager attention logging (host-only)

Attention logging is optional observation, disabled by default. It never wakes or controls a manager, acknowledges work, or accesses hardware, MCP, providers, boards, or suite execution.

## Enable and write records

Enable `attention_logging_enabled` in both watcher and harness configuration, configure the stable `attention_epoch_id`, add the harness `attention-events.jsonl` source, and allowlist producer `(role, source_id)` pairs. Producers write only through:

```powershell
python -m harness_watcher_implementation --config CONFIG record-attention --role orchestrator ...
```

Manager-signal publishers must expose only the final ordinary `*.json` file. Hidden JSON names
and `*.tmp.json` atomic staging files are non-final and the native harness deliberately ignores
them. This prevents one logical `signal_id` from acquiring separate path-derived native events
during an atomic publish.

For a counted causal chain, publishing is ordered: prepare the payload at a hidden or
`*.tmp.json` staging path; successfully append `AGENT_SIGNAL_CREATED`; capture exact UTC immediately
before atomically renaming the staged payload to its final ordinary JSON path; then append
`AGENT_SIGNAL_PUBLISHED` with `--source-timestamp-utc` set to that captured value and append
`AGENT_WAIT_STARTED`. Verify all three returned `record_id` values. The publication record is
passive evidence: it neither discovers, wakes, notifies, nor assists the manager.

When disabled, this command returns `{"disabled": true}` before reading configuration or creating paths.

### Portable Windows producer pattern

Use either inline `--metadata` or `--metadata-file`; they are mutually exclusive and both must contain exactly one JSON object. For interruption-safe Windows producers, write the metadata to a unique temporary file in the same directory, atomically replace the final temporary metadata path, invoke the recorder, check for exit code `0`, and only then remove that temporary metadata file:

```powershell
$metadataPath = Join-Path $env:TEMP "attention-$PID-$([guid]::NewGuid()).json"
$pendingPath = "$metadataPath.pending"
$metadataJson = '{"lane_id":"20260801-attention-r6:Atlas:A22","agent_blocked":true}'
[IO.File]::WriteAllText($pendingPath, $metadataJson, [Text.UTF8Encoding]::new($false))
Move-Item -LiteralPath $pendingPath -Destination $metadataPath
python -m harness_watcher_implementation --config CONFIG record-attention `
  --role subagent --source-id lane --epoch-id 20260801-attention-r6 `
  --event-id signal-001 --kind AGENT_SIGNAL_CREATED --metadata-file $metadataPath
if ($LASTEXITCODE -ne 0) { throw "attention recorder failed" }
$publishedUtc = [DateTime]::UtcNow.ToString('o') # capture immediately before final atomic rename
Move-Item -LiteralPath $stagedSignalPath -Destination $finalSignalPath
python -m harness_watcher_implementation --config CONFIG record-attention `
  --role subagent --source-id lane --epoch-id 20260801-attention-r6 `
  --event-id signal-001 --kind AGENT_SIGNAL_PUBLISHED --metadata-file $metadataPath `
  --source-timestamp-utc $publishedUtc
if ($LASTEXITCODE -ne 0) { throw "publication recorder failed" }
Remove-Item -LiteralPath $metadataPath
```

Do not remove the temporary metadata file after a failed invocation; retain it for retry or evidence. Missing, malformed, non-object, or conflicting metadata inputs fail without appending a record.

### Resumed root turns

Every resumed root turn has a new `manager_invocation_id`. Before it scans, claims work, or responds, write a `MANAGER_INVOCATION_STARTED` record with that new invocation ID and the required pending-work snapshot. Never infer or backfill an unobserved interval under an old invocation identity; leave it unknown and begin the new boundary explicitly.

Source records use `manager-attention-timeline/v1`: UUIDv4 `record_id`, UTC `source_timestamp_utc`, `epoch_id`, `event_id`, `kind`, and recorder identity. Vocabulary includes agent signal creation/publication/wait/response/resume, watcher notification delivery; harness observed/actionable/deferred/pending/ack/scan; manager invocation/wait/activity/claim/review/tool/decision/response/checkpoint; formal baseline; and supervisor absence boundaries. `record-attention --source-timestamp-utc` preserves a timestamp captured at the real source boundary instead of substituting recorder execution time. Canonical watcher records additionally bind source path, role/ID/generation, bytes, host observation time, and source hash.

At the origin of an `AGENT_SIGNAL_CREATED`, each declared delivery or response deadline must be at or after that signal's `source_timestamp_utc`; equality is allowed. The rule deliberately does not reject a later record that carries the original deadline. An historically ingested canonical signal that violates this origin rule is retained as contradictory evidence and diagnoses `INSUFFICIENT_EVIDENCE`; polling continues and it never becomes a harness, busy, or idle attribution.

For a blocked request, harness-observation latency is measured from the single matching
`AGENT_SIGNAL_PUBLISHED`, not from creation. The publication must have the same epoch, event,
`lane_id`, `signal_id`, blocked state, and deadlines, and must be causally ordered
`creation <= publication <= harness observation`. When a blocked-signal harness-delay attribution
depends on this boundary, missing, duplicate, mismatched, reversed, or post-deadline publication
evidence fails closed as `INSUFFICIENT_EVIDENCE`; it must never be reported as harness delay.

## Current host-adapter delivery

The harness CLI no longer accepts the retired manager-session and
manager-invocation flags on `watch`, and the diagnostic watch no longer emits
`wake_id`, `wake_transport`, `wake_component`, or `MANAGER_WAKE_*` records.
Those manager-flag and diagnostic-watch wake semantics were retired with the
attention-sprint policy they served.

Current production delivery is host-adapter based and payload-free.  A
`ManagerEventRouter` queue is the active-queue authority for one exact manager
binding; the harness-owned `DeliveryCoordinator` consumes the queue's wake edge
and produces one sparse `DeliveryNotice` (schema
`orchestrator-delivery-notice/v1`) that contains no event ID, event IDs, data,
or payload.  The implemented `CodexAdapter` (or another `HostAdapter`) delivers
that notice at a safe lifecycle boundary and returns a `DeliveryReceipt`
(schema `orchestrator-delivery-receipt/v1`) whose `outcome` is `DELIVERED`.
A transport receipt is evidence only: it never acknowledges queue work.  The
event stays pending in the router until the manager explicitly acknowledges it
through `acknowledge_event`; a quiet empty queue produces no notice and no
transport call.

The retained attention-timeline decoder in `harness_watcher_implementation`
still reads historical `MANAGER_WAKE_*`, `wake_id`, and `wake_transport`
records for preserved pre-adapter evidence, but current production does not
generate them.  Historical `WATCHER_NOTIFICATION_SENT` relay records remain
readable but are not production delivery proof.

## Pending-work snapshots

`MANAGER_INVOCATION_STARTED`, `MANAGER_INVOCATION_FINISHED`, `HARNESS_EVENT_PENDING`, and `FORMAL_REVIEW_BASELINE_ADVANCED` require `pending_work_snapshot`. A complete snapshot is bounded to 128 metadata-only events:

```json
{"complete":true,"events":[{"event_id":"...","type":"...","priority":2,"age_seconds":4.5,"agent_blocked":true,"response_deadline_utc":"...","lease_deadline_utc":"..."}],"selected_event_id":"...","selection_reason":"SELECT_ACTIONABLE"}
```

Deadlines are optional; all other event fields are required. Transcript, evidence, prompt, reasoning, credential, and arbitrary fields are rejected. If the harness or logger cannot prove every required field, it emits only `{"complete":false,"events":[],"selected_event_id":null,"selection_reason":"UNKNOWN"}`. It never invents age, blocked state, priority, or a selected event. The record-attention CLI validates manager boundary snapshots; malformed or oversized snapshots fail closed.

Harness pending emissions derive the best complete snapshot from selected/deferred notification candidates; formal-baseline acknowledgement emits explicit unknown when the retained notification state cannot prove complete candidate metadata.

## Deferring a known pending event

While a known pending event is deferred, record a paired `MANAGER_WAIT_STARTED`/`MANAGER_WAIT_FINISHED` or `MANAGER_TOOL_STARTED`/`MANAGER_TOOL_FINISHED` interval that covers the deferral. If the manager handles another event, write `MANAGER_EVENT_CLAIMED` for that exact other event, then its matching `MANAGER_RESPONSE_PUBLISHED`; both records use the same manager session/invocation and `HANDLING_OTHER_EVENT` plus that event's `related_event_id`. A decision or checkpoint can close the handling interval only when it has `terminal_for_activity:true`. The analyzer may join touching intervals from one manager invocation, but it never bridges a timestamp gap, accepts an unpaired/cross-invocation endpoint, or infers state from silence. Any gap is `INSUFFICIENT_EVIDENCE`.

A `MANAGER_REVIEW_STARTED` record must include `formal_review_due_utc`. For a late start, the analyzer diagnoses the complete explicit interval chain from that due time to the start: all wait/validated-absence coverage is idle/absent, and any tool or other-event/review work is busy. Formal reviews are self-scheduled, so they need no watcher notification. This late-start diagnosis takes precedence over a later baseline acknowledgement; a late baseline is acknowledgement-only only when review, decision, and response already completed on time.

### Explicit continuity between separate recorder calls

Do not depend on equal timestamps between CLI calls. After recording a terminal (`MANAGER_WAIT_FINISHED`, `MANAGER_TOOL_FINISHED`, `MANAGER_RESPONSE_PUBLISHED`, or an explicitly terminal decision/checkpoint), retain the JSON result's `record_id`. Put it in `continuous_from_record_id` in the metadata file for exactly one next opening (`MANAGER_EVENT_CLAIMED`, `MANAGER_REVIEW_STARTED`, `MANAGER_WAIT_STARTED`, or `MANAGER_TOOL_STARTED`) with the same manager session and invocation. This declares only that exact transition; it does not infer a duration from adjacency. The watcher accepts the bridge only when the named canonical record is a prior terminal in the same invocation and no second successor names it. Missing, nonterminal, cross-invocation, or forked references remain insufficient.

```powershell
# `$finished.record_id` is parsed from the successful prior record-attention output.
@{ manager_session_id = $session; manager_invocation_id = $invocation; manager_state = 'READING_EVENT'; continuous_from_record_id = $finished.record_id } | ConvertTo-Json -Compress | Set-Content -NoNewline $metadataPath
python -m harness_watcher_implementation --config CONFIG record-attention --role orchestrator --source-id root --epoch-id EPOCH --event-id EVENT --kind MANAGER_EVENT_CLAIMED --metadata-file $metadataPath
```


For a blocked signal with a delivery deadline, source creation and harness observation are not proof
that the manager received the event. In the retained attention-timeline decoder, the historical
production proof was the native harness chain: `MANAGER_WAKE_ATTEMPTED` ->
`MANAGER_WAKE_DELIVERED` -> `MANAGER_WAKE_RECEIVED` -> matching `MANAGER_WAIT_FINISHED`, all
bound to the exact event, wake, session, invocation, component, and
`blocking_harness_wait_stdout` transport. Current production no longer generates that chain (see
"Current host-adapter delivery" above); the decoder still applies this rule to preserved
historical records. Without a complete successful native chain, the analyzer reports
`INSUFFICIENT_EVIDENCE` or an explicit harness delivery failure; it never invents busy or
idle manager state.

Historical `WATCHER_NOTIFICATION_SENT` records using `collaboration.send_message` remain readable
for preserved pre-native evidence only. They require successful delivery and subagent provenance,
but they are not generated or required by the production M5 path and must not appear in a counted
M5 sprint.

A late harness observation is always `HARNESS_DELIVERY_DELAY`. When observation was on time but
`HARNESS_EVENT_ACTIONABLE` is late, the analyzer changes that attribution only if a gap-free
explicit manager interval covers the actual overrun from the delivery deadline through
actionability and contains genuine busy work; time before the target is not part of the overrun. An
explicit native harness wait remains `HARNESS_DELIVERY_DELAY`. Partial or contradictory
manager-activity evidence is `INSUFFICIENT_EVIDENCE`. Other missed earlier delivery deadlines retain
harness-delivery precedence. `deadline_lateness_seconds` is populated only from the actual late
endpoint used by the selected diagnosis.

For an observed manager signal that native liveness rules intentionally exclude, the harness may
emit `HARNESS_EVENT_INELIGIBLE` with exactly one of `ALREADY_ANSWERED`, `INVALID_LANE_ID`, or
`LANE_NOT_LIVE`. The analyzer accepts this explanation only from harness provenance, for the exact
signal and exact harness event, at or after observation, with exactly one supported record and no
actionable transition. A valid record produces `INSUFFICIENT_EVIDENCE` rather than the false causal
claim `HARNESS_DELIVERY_DELAY`. Missing, stale, duplicated, mismatched, unsupported, or
actionability-contradicted evidence cannot suppress a supported delivery-delay diagnosis.

A healthy blocked path is `NO_BLOCKING_IMPACT` when the exact event has the canonical successful native harness wake chain, manager claim, and manager response at or before its response deadline. This is a retained attention-timeline decoder rule for preserved historical records; current production delivery is the host-adapter notice/receipt path described under "Current host-adapter delivery" above. The claim must precede or equal the response; both must use the same session/invocation; a matching `MANAGER_INVOCATION_STARTED` must precede the claim; and no matching invocation finish may precede the response. Agent receipt and work-resume may occur later: they stay visible as metrics but do not change the manager-response result. A missing or late native delivery cannot use this healthy branch; late delivery keeps harness-delivery precedence, while a late or missing claim follows the explicit busy/idle/insufficient causal analysis.

## Outputs, report, and recovery

The watcher writes canonical records to `watcher/attention-timeline.jsonl`, durable parse/collision/torn-line observations to `watcher/attention-observation-errors.jsonl`, a byte cursor to `watcher/attention-cursor.json`, and the derived `watcher/attention-report.json`. Timeline records fsync before cursor advancement. On restart, valid canonical records are re-indexed by record ID and source hash, preserving first host-observed time; corrupt neighboring timeline lines are retained as durable observation errors rather than discarding valid history.

The report contains per-event metrics (creation→publication, publication→observation,
observation→actionable, pending→claim, claim→decision, response→receipt, receipt→resume, blocked
duration, deadline lateness, response→ack, and deferral duration), classification counts, trusted
coverage, cursor state, and durable observation errors. Classifications are
`IDLE_OR_ABSENT_MANAGER_DELAY`, `BUSY_MANAGER_DELAY`, `HARNESS_DELIVERY_DELAY`,
`ACKNOWLEDGEMENT_ONLY_DELAY`, `NO_BLOCKING_IMPACT`, and `INSUFFICIENT_EVIDENCE`.

`explicit_deferral_seconds` measures from the latest explicit `HARNESS_EVENT_DEFERRED` whose
source timestamp is at or before the selected pending endpoint. A deferral recorded only after
pending is a later event, so this metric is absent; the watcher never fabricates a negative
deferral duration. Other reversed causal endpoints continue to fail closed.

For M5, 90 seconds is a diagnostic target rather than an automatic sprint-failure threshold. An
overrun is acceptable only when this analyzer's canonical, gap-free evidence supports
`BUSY_MANAGER_DELAY`, including genuine manager work or handling an earlier genuine request. Late
harness observation, a native blocking wait, otherwise-idle delay, and partial, overlapping, or
contradictory activity do not become valid reasons merely because the target was exceeded.

Partial lines, backlog, rotation ambiguity, malformed records, corruption, unknown process/activity identity, or an unresolved exact epoch/event observation error force conservative insufficient evidence. Errors remain effective across source rotations until an explicit durable resolution protocol exists (none is currently defined). `HARNESS_SCAN_COMMITTED` chains require continuous coverage, exact process/output identity, and durable prior linkage; gaps or contradiction do not prove non-delivery.

Run the host-only practical check:

```powershell
python -m harness_watcher_implementation.tests.run_attention_practical
```

The practical admits one actionable event into a fresh manager queue, produces
one sparse delivery notice, obtains one `DELIVERED` boundary receipt from the
synthetic Codex adapter, proves delivery leaves the event pending, and proves a
fresh empty queue emits no notice and no transport call, alongside the retained
attention-analysis scenarios and the CLI disabled-producer gate.  It prints the
`attention practical host-only check: PASS` marker.

## Retired sprint-boundary policy helpers

The attention-sprint boundary policy helpers (boundary validation, snapshot
building, and finalize validation) were retired with the product policy they
served.  They are intentionally absent from current source and are not called,
imported, or advertised by any current workflow or verification asset.

The surviving attention verification asset is the host-only practical
(`harness_watcher_implementation/tests/run_attention_practical.py`), invoked as
shown above under "Outputs, report, and recovery".  It exercises only current
production behavior: the host-adapter wake and quiet delivery paths, the
retained attention-analysis scenarios, and the CLI disabled-producer gate, and
it prints the `attention practical host-only check: PASS` marker.
