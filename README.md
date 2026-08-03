# Portable Harness + Deterministic Watcher

This folder is a clean, standalone copy of the native orchestration harness and deterministic
watcher. It contains no historical run logs, M5 evidence, worker outputs, or watcher runtime state.

**New agent:** read `QUICK_RULES.md`, then follow `QUICK_START.md`.

## Intended minimal topology

```text
external workers -> native orchestrator harness -> persistent orchestrator
                              |
                              v
                    deterministic watcher
                    (diagnostics only)
```

- There is **no AI watcher subagent or collaboration relay**.
- The orchestrator calls the harness's native blocking wait directly.
- The deterministic watcher records diagnostics; it does not wake the orchestrator, manage workers,
  repair code, or make decisions.
- The orchestrator remains responsible for launching workers, responding to them, managing
  resources, and acknowledging exact harness events.

## Requirements

- Python 3.11 or newer.
- No third-party Python runtime dependencies.
- A target suite whose worker/run state follows the contracts in
  `orchestrator_harness/SPEC.md`.
- A persistent orchestrator that repeatedly returns to the native blocking wait. This package
  cannot wake a closed or terminated agent conversation.

Run commands from this folder so Python can import `orchestrator_harness`,
`harness_watcher_implementation`, and `harness_common`.

## First-time setup

1. Copy `examples/harness.example.json` to `local-config/harness.json`.
2. Set `suite_root` to the target repository and set `run_globs` to its run directories.
3. Give every live epoch a fresh `output_dir` under `runtime/orchestrator-harness/<epoch>`.
4. Copy `examples/watcher.diagnostic.example.json` to `local-config/watcher.json`.
5. Update its `observed_sources` to the harness, orchestrator, and worker JSONL files for that epoch.
6. Give it a fresh `runtime_root` under `runtime/harness-watcher/<epoch>`.

The supplied watcher example has `evaluator_enabled: false`. Keep it false for the minimal,
deterministic-only system. Enabling it launches an AI evaluator and is a different runtime topology.

## Validate configuration

```powershell
python -m orchestrator_harness --config local-config/harness.json scan --no-write
python -m harness_watcher_implementation --config local-config/watcher.json poll
```

`scan --no-write` must accurately discover the intended lanes before live operation. If it does
not, fix the configuration or target-suite records; do not surround the harness with a relay,
runner, or wrapper.

## Normal manager loop

Start the deterministic watcher with the exact PID of a durable owner process:

```powershell
python -m harness_watcher_implementation --config local-config/watcher.json start --owner-pid <durable-owner-pid>
```

Then the persistent orchestrator repeatedly performs a bounded native wait:

```powershell
python -m orchestrator_harness --config local-config/harness.json watch --until-actionable --timeout 60 --manager-session-id <session-id> --manager-invocation-id <invocation-id>
```

- Exit `0`: one actionable event was returned as JSON.
- Exit `3`: quiet timeout; no event was delivered.
- Exit `1`: configuration, observation, or safety failure.

For a returned event:

1. Log `MANAGER_WAKE_RECEIVED` first when attention logging is enabled.
2. Record the matching `MANAGER_WAIT_FINISHED`.
3. Inspect and handle the request.
4. Validate the response's exact non-empty epoch, source event, lane, session, and invocation IDs.
5. Publish the response atomically.
6. Acknowledge using the envelope's top-level native `event_id`:

```powershell
python -m orchestrator_harness --config local-config/harness.json ack --event-id <native-event-id>
```

Do not use `data.signal_id` for acknowledgement. That is the worker/source event identity.

## Status and shutdown

```powershell
python -m harness_watcher_implementation --config local-config/watcher.json status
python -m harness_watcher_implementation --config local-config/watcher.json stop
python -m orchestrator_harness --config local-config/harness.json watch stop
```

Stops are cooperative. Confirm exact PID plus creation identity rather than killing processes by
name or broad command matching.

## Tests

```powershell
python -m compileall -q orchestrator_harness harness_common harness_watcher_implementation
python -m unittest discover -s orchestrator_harness/tests -t . -v
python -m unittest discover -s harness_watcher_implementation/tests -t . -v
python harness_watcher_implementation/tests/run_attention_practical.py
```

The optional WSL real-agent test has additional isolation requirements documented in
`orchestrator_harness/README.md`. Ordinary operation does not require WSL or Codex CLI access.

## Documentation map

- `orchestrator_harness/README.md` - harness commands and operating behavior.
- `orchestrator_harness/SPEC.md` - authoritative harness contracts and non-goals.
- `docs/HARNESS_WATCHER_GUIDE.md` - detailed deterministic-watcher operating guide.
- `harness_watcher_implementation/SPEC.md` - watcher interfaces and authority.
- `harness_watcher_implementation/ATTENTION_LOGGING.md` - optional six-stage attention evidence.
- `harness_watcher_implementation/TESTING_GUIDE.md` - focused watcher verification.
- `PORTABLE_CONTENTS.md` - exactly what was copied and excluded.
