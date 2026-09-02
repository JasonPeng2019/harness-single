# Claude Code shipped machinery

Notes for adapter authors maintaining the Claude Code payload.

- Launch transport: `claude --print --output-format stream-json --verbose`
  with `--model <model>`, `--permission-mode bypassPermissions`, and the
  prompt on stdin.
- Transcript facts: JSON lines with `type` `system` (`subtype` `init` or
  `permission_denied`) or `result`; the session id is `session_id` or
  `sessionId`.
- Hooks: `settings.json` declares the PostToolUse hook; the hook script
  appends liveness receipts and, for ROOT with `HARNESS_EVENT_ID`, managed
  delivery receipts.
- The binding delegates to
  `orchestrator_harness.provider.ClaudeCodeProviderAdapter` so shipped flags
  and parsing stay conformant with the runtime adapter.
