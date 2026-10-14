# Codex headless PostToolUse proof

This is the live-proof contract for firmware-v2 Codex lane hooks. It documents
an actual successful run, not a synthetic adapter self-test.

## Preconditions

1. Prepare the exact lane with the native v2 `lane bootstrap` command.
2. The manager creates and owns the real managed queue.
3. Admit a real pending manager event, then start the lane with the native
   `lane launch` command. The controller loads the shipped direct binding.

The binding is shipped/read-only and provider-owned: it carries only the strict
provider identity/version/argv/parser contract. The v2 controller owns queue,
registration, and lifecycle state.

## Live result

On 2026-08-27, Windows Codex CLI `0.150.1` launched GPT-5.6 Luna at medium
reasoning through the firmware-v2 operator/lane route.
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
hook command did execute without the lane's shipped binding record. Re-run
bootstrap with the catalog intact and start a fresh provider session. The v2
route does not create or guess a manager queue from a provider command.
