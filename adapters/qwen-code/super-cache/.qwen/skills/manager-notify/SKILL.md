---
name: manager-notify
description: Raise a worker escalation via .agent-workspace/manager-notify.py.
---

# manager-notify

When ROOT intervention is needed, run the provider-neutral escalation helper.

## When to use

- The worker needs a ROOT decision, authority, missing input, or help.
- A lane assignment is blocked and must be escalated.

## Command

    python .agent-workspace/manager-notify.py --severity blocking --summary "<decision or action needed>"

## Rules

- Include the decision/action ROOT needs plus relevant local evidence.
- Do not hand-edit manager queue files.
- After a blocking escalation, stop at a safe boundary rather than inventing
  the missing decision.

## After

Wait for ROOT's response at the safe boundary; do not fabricate the missing
decision.
