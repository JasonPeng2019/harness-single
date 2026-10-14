---
name: force-stop-lane
description: Hard-stop one stuck lane via `lane force-stop --lane-id`.
---

# force-stop-lane

To hard-stop one stuck lane, use the public force-stop command.

## When to use

- One lane is stuck and cannot be stopped cooperatively.

## Command

    operator_launch lane force-stop --lane-id <lane-id>

## Rules

- Use for one stuck lane only; use `harness-shutdown` for the whole runtime.
- The command terminates that lane's provider/helper/controller,
  force-releases its lease, and marks it retired.
- Never kill by broad process name and never edit lease/process records.
- For an orphaned resource that is not part of a force-stop, use only
  `operator_launch lease force-release --resource-id <id>`. The command writes
  locked STARTED and terminal audit records and reports any partial success.

## After

Confirm the lane is retired, then decide whether to resume or retire it.
