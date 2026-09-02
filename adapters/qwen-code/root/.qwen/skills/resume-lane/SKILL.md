---
name: resume-lane
description: Resume a stopped, unaccepted lane with a new resume task card via `resume-lane`.
---

# resume-lane

For a stopped, unaccepted lane, use the public resume command with a new
resume task card.

## When to use

- A lane is stopped and unaccepted, and ROOT wants to continue it with
  current instructions.

## Command

    operator_launch resume-lane --lane-id <lane-id> --resume-task-card "<task card>" --rationale "<truthful rationale>"

## Rules

- Only resume a stopped, unaccepted lane.
- The resume task card carries the current instructions; the rationale is
  truthful.
- Do not reconstruct a session, PID, worktree, invocation, or
  amendment/hash record.

## After

Confirm the resume result, then run `lane launch --lane-id <lane-id>` to
start the resumed run when the result reports that next step.
