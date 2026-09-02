---
name: review-lane-completion
description: Review a lane completion with `lane completion-review` (finding and approval).
---

# review-lane-completion

Profile-aware completion review.  Managed lanes handle an acknowledged
`COMPLETION_REVIEW_REQUIRED` event; plain lanes identify the controlled
terminal `review_pending` lane.

## When to use

- A managed lane produced a `COMPLETION_REVIEW_REQUIRED` event that ROOT
  acknowledged.
- A plain lane is in the controlled terminal `review_pending` state.

## Command

Managed:

    operator_launch lane completion-review --event-id <event-id> --review-outcome PASS|FAIL|BLOCKED --approval ACCEPTED|REJECTED --review-summary "<summary>"

Plain:

    operator_launch lane completion-review --lane-id <lane-id> --review-outcome PASS|FAIL|BLOCKED --approval ACCEPTED|REJECTED --review-summary "<summary>"

## Rules

- `--review-outcome` and `--approval` are independent and both required;
  ACCEPTED requires PASS.
- Compare the copied task criteria, result, and evidence before deciding.
- A stale-source error normally means resume; do not force-accept it.
- `--force-accept --force-reason` only after inspecting a harmless
  current-record difference.
- REJECTED creates a `LANE_RESUME_REQUIRED` event (managed) or direct
  resume-required output (plain).
- Never edit review, acceptance, or lane files directly; never restart a
  provider yourself.

## After

Report the review result.  For REJECTED, prepare a resume task card for
`resume-lane`.
