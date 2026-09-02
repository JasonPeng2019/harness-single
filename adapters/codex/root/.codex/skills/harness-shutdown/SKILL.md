---
name: harness-shutdown
description: End the whole runtime via `operator_launch harness shutdown`.
---

# harness-shutdown

Run the public shutdown command when intentionally ending the run.

## When to use

- ROOT is intentionally ending the whole runtime.

## Command

    operator_launch harness shutdown

## Rules

- Do not close terminals, kill by broad process name, or edit process/lease
  records.
- Preserve failed-shutdown evidence and do not retry with broad kills.

## After

Confirm the runtime state; report any failed-shutdown evidence as-is.
