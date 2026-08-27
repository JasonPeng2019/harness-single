# Codex headless PostToolUse proof

This is the live-proof contract for firmware-v2 Codex lane hooks. It documents
an actual successful run, not a synthetic adapter self-test.

## Preconditions

1. Prepare the exact lane and install the Codex adapter.
2. The manager creates and owns the real `ManagerEventRouter`.
3. Bind the lane to that already-registered queue before launching:

   ```powershell
   python -m orchestrator_harness adapter bind --host codex --project-root $lane --queue-root $managerQueue
   ```

4. Admit a real pending manager event, then start a fresh lane through the
   native `operator_launch` followed by `lane_controller` path.

The binding is deliberately manager-owned: it carries the exact queue,
registration, manager-session, and coordinator identities. `adapter bind` does
not make a synthetic queue or choose those values itself.

## Live result

On 2026-08-27, Windows Codex CLI `0.150.1` launched GPT-5.6 Luna at medium
reasoning through the firmware-v2 `operator_launch -> lane_controller` path.
The disposable worker's only tool action was `git status --short`; it ended
normally with its expected proof marker.

The bound coordinator then persisted a new delivery receipt with:

```json
{
  "boundary": "post_tool_use",
  "outcome": "DELIVERED",
  "observed_queue_revision": 11
}
```

The manager delivery journal recorded the same fresh wake revision. The pending
event remained pending, which is correct: a hook delivery is a sparse notice,
not manager acknowledgement or queue mutation.

## Failure and recovery

If a Codex hook reports `installed Codex hook has no harness binding`, the
hook command did execute, but the manager omitted the binding step. Bind the
prepared lane to the real registered queue and start a fresh provider session.
If the queue is missing, stale, or belongs to a different binding, `adapter
bind` fails rather than redirecting the lane to a guessed queue.
