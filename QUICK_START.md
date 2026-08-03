# Quick Start

This is the shortest safe path for an agent to use the native harness and deterministic watcher
without an AI watcher subagent, relay, runner, or wrapper.

## 1. Read the rules

Read `QUICK_RULES.md`. The important boundary is simple: the harness delivers events, the watcher
records diagnostics, and the persistent orchestrator makes every decision.

## 2. Prepare local configuration

From this folder:

```powershell
New-Item -ItemType Directory -Force local-config | Out-Null
Copy-Item examples/harness.example.json local-config/harness.json
Copy-Item examples/watcher.diagnostic.example.json local-config/watcher.json
```

Edit `local-config/harness.json`:

- `suite_root`: target repository containing the worker runs;
- `run_globs`: every run the harness should observe;
- `output_dir`: a fresh `runtime/orchestrator-harness/<epoch>` directory; and
- `attention_epoch_id`: a new unique epoch ID.

Edit `local-config/watcher.json`:

- point `observed_sources` at that epoch's manager, harness, and worker JSONL files;
- use a fresh `runtime/harness-watcher/<epoch>` directory; and
- keep `evaluator_enabled: false`.

Never reuse another run's config, state directory, pending notification, or watcher cursor.

## 3. Confirm the target is discoverable

```powershell
python -m orchestrator_harness --config local-config/harness.json scan --no-write
python -m harness_watcher_implementation --config local-config/watcher.json poll
```

Inspect the output. Do not start live workers until the intended runs and lanes are present and no
unexpected run is included.

## 4. Start the diagnostic watcher

Use the exact PID of a durable owner that will remain alive for the run—not a temporary shell:

```powershell
python -m harness_watcher_implementation --config local-config/watcher.json start --owner-pid <durable-owner-pid>
python -m harness_watcher_implementation --config local-config/watcher.json status
```

Confirm the watcher reports ready and `evaluator_enabled: false`.

## 5. Launch real task workers

The persistent orchestrator launches the external workers using the target project's normal worker
controller and authorization rules. The harness and watcher do not launch or manage workers.

Do not launch an AI watcher, notification relay, harness helper, scheduler, or retry process.

## 6. Run the manager loop

The orchestrator directly performs this bounded wait whenever it is ready for the next event:

```powershell
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60 --manager-session-id <session-id> --manager-invocation-id <invocation-id>
```

- Exit `0`: handle the returned JSON event.
- Exit `3`: quiet timeout; call the native wait again when appropriate.
- Exit `1`: preserve the error and diagnose it. Do not build a workaround around the harness.

The agent must stay active and keep returning to this wait. This system cannot restart or wake a
closed agent conversation.

## 7. Handle one returned event correctly

1. Preserve the returned `wake_id`.
2. If attention logging is enabled, make `MANAGER_WAKE_RECEIVED` the first manager record after
   delivery, then record the matching `MANAGER_WAIT_FINISHED`.
3. Use `data.signal_id` as the worker/source event ID.
4. Inspect the durable worker request and make the management decision.
5. Validate non-empty exact epoch, source-event, lane, session, and invocation IDs before publishing
   a response.
6. Publish the response atomically and confirm the intended bytes exist.
7. Acknowledge only after successful handling, using the envelope's top-level native `event_id`:

```powershell
python -m orchestrator_harness --config local-config/harness.json ack --event-id <native-event-id>
```

Never acknowledge with `data.signal_id`.

## 8. Stop and clean up

Let active task work reach a safe natural boundary, then stop cooperatively:

```powershell
python -m harness_watcher_implementation --config local-config/watcher.json stop
python -m orchestrator_harness --config local-config/harness.json watch stop
```

Confirm the watcher and harness stopped, their exact PID-plus-creation identities are absent, and no
worker, controller, lease, debugger, provider, or hardware process remains unintentionally active.

Runtime data belongs under `runtime/`; never move it into a source package.

## If something fails

- Preserve the event, error, watcher diagnostics, and current state.
- Do not add support machinery or edit code during live work.
- Finish or stop at the next safe boundary.
- Decide whether the problem is configuration, operator procedure, worker behavior, harness code,
  or watcher diagnostics.
- Repair only a verified defect, then run the tests in `README.md` before the next fresh epoch.

