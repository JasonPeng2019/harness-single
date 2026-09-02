# Claude Code headless PostToolUse proof

This proof establishes that Claude Code invokes the installed project-local
PostToolUse hook in a harness-managed headless session and that the hook
delivers a content-free notice to its explicitly bound manager queue.

## Proven run

On Windows, Claude Code 2.1.239 was launched through the native v2 operator
and lane route in a fresh Git lane. The provider command was:

```text
claude --print --output-format stream-json --verbose --model sonnet --permission-mode bypassPermissions --allowedTools Bash
```

The one-line task asked Claude to run `git status --short` once without editing
files. The stream contains that Bash call, ends with
`CLAUDE_HOOK_PROOF_COMPLETE`, and records a successful result.

Before launch, the proof created a real managed queue and admitted one pending
`MANAGER_SIGNAL`; the controller loaded the shipped direct Claude binding.

The durable manager `DELIVERY.jsonl` recorded `outcome: "DELIVERED"`. The
coordinator receipt identifies `boundary: "post_tool_use"`, reports one
delivery attempt, and leaves the manager event pending. Delivery is transport
evidence only; it is not acknowledgement or completion of queue work.

## Limits and recovery

- The proof covers project-local PostToolUse. It does not claim every Claude
  lifecycle mode will invoke Stop.
- The harness does not supply credentials or provider configuration. Claude
  uses its normal existing configuration and the caller-selected model.
- A lane missing its shipped binding fails loudly with `installed Claude hook
  has no harness binding`. Re-bootstrap the lane with the catalog intact and
  start a fresh Claude session.
- A source-checkout proof grants `PYTHONPATH` so the hook can import the
  uninstalled harness package. Normal installed use does not require that
  temporary grant.
