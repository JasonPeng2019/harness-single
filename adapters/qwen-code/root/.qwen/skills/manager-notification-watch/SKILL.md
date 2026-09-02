---
name: manager-notification-watch
description: Wait for actionable manager activity with `watch --until-actionable` when ROOT is deliberately idle.
---

# manager-notification-watch

Use this skill only when ROOT has finished its current task and is
deliberately waiting for worker activity.  It is an operational instruction,
not a background process and not a replacement queue.

## When to use

- ROOT has no active work and is intentionally idle, waiting for a worker to
  report an actionable condition.
- ROOT needs a single blocking wait that returns an actionable report.

## Command

Run the public watch command in the current ROOT CLI session:

    operator_launch watch --until-actionable

The command occupies the current session while it waits.  Do not start it
while actively working, and do not run it in the background.

## Rules

- Never start `watch --until-actionable` while ROOT still has work in
  progress.
- Handle the returned actionable report before starting another wait.
- Do not poll, edit the manager queue, or replace the queue with this skill.

## After

Read the actionable report, then act on it with the matching skill
(`acknowledge-manager-notification`, `close-manager-notification`, or
`review-lane-completion`).
