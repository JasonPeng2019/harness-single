# Required task-card outcome fields

## Outcome

Make three outcome-defining fields mandatory in every
`project-task-card/v1` record:

- `acceptance_criteria`: a non-empty JSON array of non-empty strings.
- `deliverables`: a non-empty JSON array of non-empty strings.
- `reason_for_acceptance_and_deliverables`: a non-empty string.

This is an intentional breaking change to the existing v1 contract. There is no
legacy-card fallback or schema migration. Callers recover by adding valid values
for all three fields and retrying the rejected bootstrap, resume, or review.

## Observable contract

Task cards are supplied by ROOT to lane bootstrap and resume, copied unchanged to
`.agent-workspace/task-card.json`, and later read during completion review. Every
reader validates the same contract from one shared validator.

A valid card looks like:

```json
{
  "schema": "project-task-card/v1",
  "task": "Add the requested task-card contract.",
  "acceptance_criteria": [
    "Bootstrap rejects a card missing any required outcome field.",
    "Workers receive all outcome fields in their prompt."
  ],
  "deliverables": [
    "Shared task-card validation",
    "Regression tests and updated documentation"
  ],
  "reason_for_acceptance_and_deliverables": "These conditions define both completion and the evidence ROOT expects to review.",
  "branch": "lane/task-card-contract",
  "base_commit": "HEAD"
}
```

Validation rejects a missing field, an empty list, a list containing a blank or
non-string item, or a blank/non-string reason. Bootstrap reports
`BOOTSTRAP_REQUEST_INVALID`; resume reports `INVALID_RESUME_TASK_CARD`; review
reports `COMPLETION_REVIEW_STALE_SOURCE`, using an actionable validation message.

The worker prompt renders `task`, acceptance criteria, deliverables, and the
reason as separate sections. The complete task-card object continues to be copied
unchanged and covered by the existing task-card content hash.

## Compatibility and non-goals

- Existing v1 cards without these fields become invalid immediately.
- `branch`, `base_commit`, `card_id`, and other existing behavior remain unchanged.
- This does not change result, review, acceptance, lane, or invocation schemas.
- This does not add defaults, infer missing values, migrate stored cards, or alter
  scheduling and launch behavior.

## Acceptance criteria

1. Bootstrap, resume, and completion review all reject cards missing or
   mis-typing any new required field.
2. A valid card is accepted and preserved value-for-value through materialization.
3. Generated and resumed worker prompts contain all three fields with stable
   headings and list formatting.
4. Existing task-card identity and content-hash behavior includes the new fields
   without changing downstream schemas.
5. Focused task-card, bootstrap, resume, and review tests pass.
