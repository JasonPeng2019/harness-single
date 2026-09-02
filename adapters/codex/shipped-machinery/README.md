# Codex shipped machinery

Notes for adapter authors maintaining the Codex payload.

- Launch transport: `codex exec` with
  `--dangerously-bypass-approvals-and-sandbox`, `--skip-git-repo-check`,
  `-c approval_policy=...`, `-m <model>`, `-c model_reasoning_effort=...`,
  `-c service_tier=...`, `--json`, `--output-last-message <path>`, and the
  prompt on stdin (`-`).
- Transcript facts: JSON lines with `type` `thread.started`,
  `turn.completed`, `turn.failed`, or `turn.cancelled`; the thread id is
  `thread_id` or `threadId`.
- Hooks: `hooks.json` declares the PostToolUse hook; the hook script appends
  liveness receipts and, for ROOT with `HARNESS_EVENT_ID`, managed delivery
  receipts.
- The binding directly owns the strict provider contract, so shipped flags and
  parsing stay conformant with the v2 controller.
