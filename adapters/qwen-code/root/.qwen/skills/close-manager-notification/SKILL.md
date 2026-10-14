---
name: close-manager-notification
description: Close an acknowledged ordinary manager event via `manager close --event-id --outcome --summary`.
---

# close-manager-notification

Finish an acknowledged ordinary ROOT event with the public close command.

## When to use

- ROOT handled an acknowledged ordinary manager event and must record the
  outcome.

## Command

    operator_launch manager close --event-id <event-id> --outcome COMPLETE|BLOCKED --summary "<summary>"

## Rules

- Only close events that are already ACKNOWLEDGED.
- Choose COMPLETE when the underlying condition was handled; BLOCKED when it
  cannot be handled.
- Never edit the queue and never restart a lane from this skill.
- Completion review closes its own event; do not double-close reviewed
  events.
- Supply a nonblank summary for both `COMPLETE` and `BLOCKED`; the summary is
  retained in the event history.

## After

Confirm the close result, then continue with the next actionable event.
