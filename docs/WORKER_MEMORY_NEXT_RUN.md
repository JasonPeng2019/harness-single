# Task-configurable memory for worker lanes

The harness recalls from each ROOT-assigned worker task and delivers the
result only to that worker.

For any task, prepare a fresh ROOT workspace, run ID, and harness runtime. Set
`harness-config.json` to that workspace with managed coordination enabled. The
ROOT prompt and `AGENTS.md` must not direct ROOT to read an operator memory file.
Use a stable task ID and an Atlas collection assigned to that task:

```powershell
.\tools\Invoke-WorkerMemoryRun.ps1 -RunId <fresh-run-id> `
    -Task <task-id> -Workspace <fresh-root-workspace> `
    -PromptFile <fresh-root-prompt> -AtlasDatabase <database-name> `
    -AtlasCollection <collection-name> -AtlasIndex <vector-index-name> `
    -Model <codex-model> -ReasoningEffort <effort> `
    -ServiceTier <tier> -WatchdogHours <hours>
```

The launcher reads Atlas and embedding credentials from `.secrets/creds/`.
Supply the Atlas database and index that belong to the selected collection;
the index must match its embedding and text fields. The launcher sets the
EverOS project and namespace from the task ID. Use `-AtlasTextKey`,
`-AtlasEmbeddingKey`, and the `-Everos*` scope options if the store uses other
field names or ownership. Atlas documents use the `task` metadata field for
isolation. `-Sandbox` defaults to `workspace-write`; select a different
Codex sandbox only when the task needs it. A new task with no stored history
receives an explicit “no similar experience” message. A backend failure is
recorded separately from an empty search; bootstrap stops if both backends
are unavailable.

The launcher requires a distinct native planner before implementation, at
least one substantive native implementer after the planner's accepted result,
and separate native reviewer(s)/validator(s) after integration. The planner
identifies independent, substantial implementation slices. Use as many
implementers as the work justifies, including three or more when each has
clear source custody and useful parallel progress. Use one when splitting
adds more coordination than useful progress, and briefly record why.

Each worker receives memory selected for its task card. ROOT coordinates,
integrates, and records the formal harness acceptance. Reviewer(s) have clear,
complementary specialties: for example, one reviews code and edge cases while
another writes and runs tests and builds. Prefer separate reviewers when both
jobs are substantial. Additional reviewers can cover security, performance,
or integration when that is substantial work; avoid duplicate or trivial
lanes. At least one reviewer runs task-specific builds and tests where it has
access, and each gives an evidence-based PASS/FAIL finding on the final
integrated revision. Where only ROOT can execute a host-only test command, it
relays the exact command and output for independent assessment.
ROOT cannot claim task completion on its own judgment or on failed/missing
validation. The important log records recall queries, results, selection,
and final worker prompts.

Credentials are removed from worker and controller environments.
