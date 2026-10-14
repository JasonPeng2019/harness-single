# Qwen Code shipped machinery

Notes for adapter authors maintaining the Qwen Code payload.

- Launch transport: `qwen --approval-mode=yolo --model <configured-model>
  --output-format stream-json` with the prompt on stdin; resume adds
  `--resume <session-id>`.
- The model is required bootstrap configuration; the adapter supplies no model or fallback.
- Transcript facts: JSON lines with `type` `interrupt`, `system` (`subtype`
  `init`), or `result`; the session id is `session_id` or `sessionId`.
- Hooks: the ROOT payload declares the PostToolUse hook in `hooks.json`
  (`hooks/post-tool-use.py` appends liveness receipts and managed receipts
  for unresolved top-level queue event IDs).  The worker payload
  declares PostToolUse and Stop hooks in `settings.json` that run the thin
  wrappers in `.qwen/hooks/`; each wrapper asks the worktree-local
  `.agent-workspace/hook-dispatch.py` for a boundary decision and translates
  it into the Qwen Code hook output contract.
- The binding directly owns the strict provider contract, so shipped flags and
  parsing stay conformant with the v2 controller.
