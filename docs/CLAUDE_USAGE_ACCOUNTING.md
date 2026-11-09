# Claude usage accounting

F3 extends the general usage ledger to Claude Code ROOT and worker invocations.
The collector reads retained transcripts and protected harness records only; it
captures ROOT provider output through its native process boundary and reads
protected worker records; reporting never changes native task/session history.

## Provider aware report command

Use the provider-aware companion from the harness source or installed data tree:

```powershell
python scripts/prepare_root_launch.py --harness-dir . --workspace <manager-workspace> --run-id <run-id> --provider claude-code
python scripts/collect_usage.py run --provider claude-code --run-id <run-id> --task "<task>" --arm harness --cwd <manager-workspace> --prompt-file <prompt-file> --results-dir <results-dir> --model <claude-model> --effort <effort> --harness-runtime <manager-workspace>/.harness-runtime --harness-dir . --trust-project-hooks --watchdog-hours 4 --interrupted-run
python scripts/collect_usage.py snapshot --task-file <root-run-dir>/task.json
python scripts/collect_usage.py gate --task-file <root-run-dir>/task.json
python scripts/collect_usage.py finalize --task-file <root-run-dir>/task.json --output <archive-dir>
python scripts/collect_usage.py summarize --provider claude-code --events <claude-events.jsonl>
```

After a planned, sealed interruption, continue the same Claude session with:

```powershell
python scripts/collect_usage.py resume --provider claude-code --task-file <root-run-dir>/task.json --prompt-file <continuation-prompt-file>
```

The task's ROOT `launch.json` records `provider_id`; each native worker's
protected `invocation.json` records `provider.id`. Those identities select the
extractor. Existing `collect_codex_usage.py run`, `resume`, `snapshot`, `gate`,
`finalize`, and `summarize` interfaces remain available for Codex users and
consumers. The new provider-aware `collect_usage.py run` and `resume` entry
points preserve the existing watchdog, process-boundary, preparation, and
cleanup checks.

Claude ROOT transcripts are retained as `claude-events.jsonl`; native worker
transcripts remain under the attempt path recorded by the protected invocation
and attempt records. A resumed appended transcript is split at each top-level
`system/init` segment. Protected attempt `session_id` and argv establish native
resume identity. A fresh session starts a new cumulative chain; F2 profile
migration history remains part of the immutable source evidence.

## Metric and evidence meaning

The counted Claude value is the genuine top-level terminal `modelUsage` rollup,
which includes Claude's built-in subagents. Nested result events, assistant
message placeholders, and ordinary `result.usage` values are ignored. The
comparison total is `input_tokens + output_tokens` from that rollup; cache read
and cache creation counts are retained as separate fields and never subtracted
or added a second time. Missing cache counters remain `null`. Per-model input,
output, cache values, and evidence hashes are retained.

Claude's result is a cumulative session total. A resumed segment is charged the
nonnegative delta from the preceding validated segment with the same provider
and native session ID. Missing predecessors, changed session identity,
decreasing counters, malformed completed transcripts, failed terminal results,
missing whole-tree rollups, duplicate evidence, or inconsistent protected
attempt records leave an error or unknown usage state. The collector does not
turn those conditions into zero tokens or an exact total. A partial final JSONL
record is tolerated only for an active invocation and does not make a completed
invocation countable.

`token_total_is_exact` and `gate_allows_new_work` retain the ledger's existing
meaning. Claude and Codex totals can have different provider definitions; the
source field and `usage_scope` identify the actual measure. Reports expose
`provider_metric_totals` and `aggregate_metric_scope`; use the provider subtotals
when comparing providers. In a mixed provider task, the legacy `known_total_tokens`
sum supports the task's operational budget gate and is marked
`mixed_provider_metrics`; it is not a cross-provider comparison metric. The
ledger does not estimate cost from token counts.

## Provider preparation

Prepare a managed Claude ROOT with the installed provider-aware preparation
command and `--provider claude-code`. Preparation binds the Claude project
hooks, session identity, runtime, and provider configuration. Codex-only Windows
sandbox readiness is not applied to Claude; any existing run-wide preparation
failure and public-check ownership validation still apply. Before relying on a
live Claude usage claim, qualify the installed CLI's terminal `modelUsage`
behavior against retained root and built-in-agent evidence. Fixture coverage
does not establish live provider qualification.
