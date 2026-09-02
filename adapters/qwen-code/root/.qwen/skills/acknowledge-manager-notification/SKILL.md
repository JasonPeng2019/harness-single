---
name: acknowledge-manager-notification
description: Acknowledge manager-queue events ROOT actually read via `manager acknowledge --event-id`.
---

# acknowledge-manager-notification

After finishing the current ROOT task, inspect the fixed manager queue
read-only, then acknowledge every top-level event ROOT actually read.

## When to use

- ROOT read one or more PENDING manager-queue events and is ready to record
  that it has seen them.

## Command

For each event ID ROOT actually read, run:

    operator_launch manager acknowledge --event-id <event-id>

## Rules

- Inspect the manager queue read-only first.  Never edit `QUEUE.json`.
- Acknowledge only events ROOT actually read; never acknowledge on behalf of
  another role.
- Acknowledging does not close the event; handle it, then close it.

## After

Handle the acknowledged event, then close it with
`close-manager-notification` (or complete the review flow for
`COMPLETION_REVIEW_REQUIRED` events).
