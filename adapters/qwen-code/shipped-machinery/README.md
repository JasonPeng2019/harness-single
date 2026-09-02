# Qwen Code shipped machinery

Notes for adapter authors maintaining the Qwen Code payload.

- Launch transport: `qwen --approval-mode=yolo --model <model>
  --output-format stream-json` with the prompt on stdin; resume adds
  `--resume <session-id>`.
- Transcript facts: JSON lines with `type` `interrupt`, `system` (`subtype`
  `init`), or `result`; the session id is `session_id` or `sessionId`.
- Hooks: `hooks.json` declares the PostToolUse hook; the hook script appends
  liveness receipts and, for ROOT with `HARNESS_EVENT_ID`, managed delivery
  receipts.
- The binding directly owns the strict provider contract, so shipped flags and
  parsing stay conformant with the v2 controller.
