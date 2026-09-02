---
name: lane-assignment
description: Acknowledge, complete, or block a ROOT assignment via .agent-workspace/lane-queue.py.
---

# lane-assignment

For a ROOT assignment, use the provider-neutral queue helper to advance its
state.

## When to use

- The worker inbox contains a PENDING or ACKNOWLEDGED ROOT assignment.

## Commands

    python .agent-workspace/lane-queue.py acknowledge --event-id <event-id>
    python .agent-workspace/lane-queue.py complete --event-id <event-id> --summary "<summary>"
    python .agent-workspace/lane-queue.py block --event-id <event-id> --summary "<summary>"

## Rules

- Acknowledge the assignment when you start it; complete it when done; block
  it when ROOT input is required.
- Escalate a block with .agent-workspace/manager-notify.py.
- Never hand-edit either queue.

## After

Confirm the state change; for a block, escalate with manager-notify and stop
at a safe boundary.
