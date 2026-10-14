---
name: send-lane-notification
description: Send a ROOT assignment to a running managed lane via `send-lane-notification`.
---

# send-lane-notification

Use the public send command with the lane ID and prompt.

## When to use

- ROOT must append a new assignment to a running managed lane.

## Command

    operator_launch send-lane-notification --lane-id <lane-id> --prompt "<prompt>"

## Rules

- The lane must be a running managed lane.
- The prompt is a new assignment for the worker; keep it self-contained.
- Never write the worker inbox directly.

## After

Confirm the assignment was appended; the worker acts via its
`lane-assignment` skill.
