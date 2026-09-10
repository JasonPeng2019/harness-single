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
- Hooks: `hooks.json` declares the PostToolUse hook; the ROOT hook script
  appends liveness receipts and managed receipts for unresolved top-level
  queue event IDs.  The worker payload declares PostToolUse and Stop hooks
  that run the thin wrappers in `.codex/hooks/`; each wrapper asks the
  worktree-local `.agent-workspace/hook-dispatch.py` for a boundary decision
  and translates it into the Codex hook output contract.
- The binding directly owns the strict provider contract, so shipped flags and
  parsing stay conformant with the v2 controller.
