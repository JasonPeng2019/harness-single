# Collector recovery: sealed worker usage gaps, shutdown and retry

This page is the supported recovery when a sealed ROOT Codex run cannot continue because a
native worker transcript is malformed or otherwise unmetered. It documents behavior that already
exists in `scripts/collect_codex_usage.py`, `scripts/token_ledger.py` and the public
`operator_launch` commands. It adds no new flag and no new schema, never changes transcripts,
never fabricates token counts, and never edits native runtime, cleanup, lease or queue records.

## Symptom

`resume`, `snapshot`, `gate` or `finalize` refuses the run, for example:

```text
error: malformed transcript at <worker transcript path>:<physical line>: <decode error>
```

Exit code 2. Strict accounting counts only exact terminal `turn.completed` usage. A sealed run
with an unmetered worker stays blocked until an operator supplies one explicit receipt for that
exact worker invocation. The same receipt covers a sealed worker attempt that ended without
terminal usage. A malformed ROOT transcript is always refused: there is no receipt for it, and no
automatic recovery flag exists.

## The receipt: `<root_run_dir>/usage-gaps.json`

`root_run_dir` is the run directory named by `root_run_dir` in the run's `task.json` (created by
`collect_codex_usage.py run` under `--results-dir/<run-id>`; it already holds `task.json`,
`launch.json`, `manifest.json` and `codex-events.jsonl`). Write `usage-gaps.json` there in the
existing `codex-usage-gaps/v1` schema:

```json
{
  "schema": "codex-usage-gaps/v1",
  "run_id": "<root run id>",
  "gaps": [
    {
      "invocation_id": "<root run id>.<epoch id>.<lane id>.<lane run id>.attempt-<attempt number>",
      "epoch_id": "<epoch id>",
      "lane_id": "<lane id>",
      "lane_run_id": "<lane run id>",
      "epoch_closed_at": "<closed_at from the epoch state>",
      "transcript_sha256": "<sha256 of the exact damaged transcript bytes>",
      "reason": "sealed worker transcript is malformed at physical line <n>; token usage is unknown",
      "operator_observed_absent_at_utc": "<UTC time you observed the worker absent>"
    }
  ]
}
```

Every `gaps` entry needs all eight fields; the receipt contains no token numbers at all. `gaps`
must be nonempty, with one entry per distinct invocation. The receipt is the operator's own
acknowledgement file: write it once with exact values, and correct only that file if a value is
wrong. Never edit the transcript or a native runtime, cleanup, lease or queue record to make a
receipt fit.

### Where the exact values come from

- `run_id`: the task run id in `task.json` / `launch.json`.
- `invocation_id`: the ledger-composed worker identity
  `<run_id>.<epoch_id>.<lane_id>.<lane_run_id>.attempt-<n>`. `epoch_id` is the runtime epoch
  directory name; `lane_id` and `lane_run_id` come from the lane's protected record; `<n>` is the
  1-based provider attempt. When a strict read refuses, compose it from those protected records;
  never guess.
- `epoch_closed_at`: the exact `closed_at` string in
  `<runtime>/epochs/<epoch id>/epoch-state.json` after native shutdown closed the epoch.
- `transcript_sha256`: SHA-256 (lowercase hex) of the malformed transcript bytes named in the
  refusal; the collector re-hashes the file on every read.
- `operator_observed_absent_at_utc`: your UTC observation that the worker process is absent.
- `reason`: a short operator description; it is stored verbatim and never derives tokens.

After acceptance, the same invocation appears in `snapshot` rows as `terminated_usage_unknown`
with its `transcript_sha256`, the sealed `usage_gap` record and null charges, so the receipt can
be verified against the collector's own read.

### What the ledger verifies before accepting

- The named invocation is a worker attempt that native discovery actually found; an unknown
  invocation, a duplicate, or an unused entry is refused.
- The evidence is genuinely unmetered: a malformed transcript, or a not-completed attempt with no
  terminal usage record. Countable usage refuses the receipt ("count it normally"); invalid usage
  values and conflicting session ids stay errors.
- `invocation_id`, `epoch_id`, `lane_id` and `lane_run_id` equal the discovered worker row exactly.
- `transcript_sha256` equals the current digest of that transcript.
- The epoch lifecycle is `closed` and its `closed_at` equals `epoch_closed_at`.
- The lane lifecycle is `retired` with the same lane run id, and native cleanup is proven: the
  exact recorded controller and provider PID-plus-creation identities, and their recorded
  boundary, must be proven exited. A retired label is not cleanup proof, and the receipt does not
  create it; native shutdown or force-stop does.

Wrong hash, wrong identity, open epoch, unretired lane and unproven cleanup are refused, and so
is an unknown original identity or an exact original incarnation that is still live. Numeric PID
reuse is not an owner match: an unrelated new process that happens to reuse a recorded PID is
not the original incarnation, must survive (it is never terminated and it is not cleanup
proof), and the original exact identity can still be proven gone by its recorded creation
identity. The damaged bytes are never rewritten: `finalize` copies the exact transcript bytes,
re-verifies the recorded digest, and reports the inexact total honestly.

## Honest unknown counts, caps and continuation

An accepted receipt marks that worker `terminated_usage_unknown`:

- `charged_tokens` and every per-field charge stay null; no token count is invented.
- `unknown_usage_invocations` counts the acknowledged gaps and `token_total_is_exact` is false.
- With a `budget_tokens` cap, `remaining_tokens` and `overshoot_tokens` are null,
  `gate_allows_new_work` is false, `gate` exits 3 and `resume` refuses ("token budget cannot
  authorize resume"). Capped unknown work stays refused; the receipt never unlocks a cap.
- Uncapped (`budget_tokens: null`, launched without `--budget-tokens`): the explicit
  acknowledgement allows continuation once no other error remains. That is the explicitly
  acknowledged uncapped continuation, an operator decision recorded in the run's own evidence
  directory rather than an automatic waiver.

The receipt is not broad permission to skip evidence. Continuation still requires the ROOT
pre-launch setup receipt for the same run id, the saved native ROOT session, the remaining
active-time allowance, the original model, reasoning effort, service tier, sandbox and workspace,
cleanup proof for every prior ROOT segment and its boundary, no in-flight invocations, and the
task declared as `planned_resume`.

## Recovery sequence

1. Preserve the failure: keep the transcript, manifests and runtime records exactly as produced.
2. Seal the runtime natively; this closes the epoch, retires lanes and proves cleanup:

   ```powershell
   python -m orchestrator_harness.operator_launch --json harness shutdown
   ```

   Follow the returned `code` and `next_action`. On `SHUTDOWN_LANE_CLEANUP_UNPROVEN`, force-stop
   the named lane and retry shutdown (below). In managed mode, shutdown also refuses with
   `SHUTDOWN_UNRESOLVED_MANAGER_OBLIGATIONS` while a queue event is unresolved: handle and close
   the event with `manager acknowledge` / `manager close`; never hand-edit the queue.
3. Record the exact identities and the epoch `closed_at` from the protected runtime records.
4. Hash the malformed transcript bytes with SHA-256.
5. Write `<root_run_dir>/usage-gaps.json`. A receipt written before the lane is retired and the
   epoch closed is refused until sealing completes; it never bypasses the seal checks.
6. Verify with the read-only commands:

   ```powershell
   python scripts/collect_codex_usage.py snapshot --task-file "<root_run_dir>/task.json"
   python scripts/collect_codex_usage.py gate --task-file "<root_run_dir>/task.json"
   ```

7. Confirm the ROOT pre-launch prerequisites for the same run id. The original run must have been
   launched with `--interrupted-run` (planned resume); otherwise `resume` refuses immediately. A
   CLOSED runtime revalidates the existing pre-launch receipt; if the runtime was reopened with
   `harness setup`, run pre-launch preparation with the SAME run id first, as described in
   [Quick Start](../QUICK_START.md) and the [integration guide](INTEGRATED_RUN_FIXES.md).
8. Resume the saved ROOT session. `resume` has exactly three options; model, effort, tier,
   sandbox, workspace, harness runtime and the remaining watchdog are re-derived from the
   original launch record, and the same native session is continued:

   ```powershell
   python scripts/collect_codex_usage.py resume `
     --task-file "<root_run_dir>/task.json" `
     --prompt-file "<continuation prompt>"
   ```

   Add `--continue-open-runtime` only when the runtime is OPEN (for example after reopening with
   `harness setup`); it is accepted only while every retained lane is retired or a
   cleanup-proven, exited `review_pending` lane.

`finalize` can archive the sealed run afterwards; with the receipt it copies the exact damaged
bytes and reports the inexact total.

## Safe shutdown, stuck lanes and retry

`python -m orchestrator_harness.operator_launch harness shutdown` ends the whole runtime:
OPEN -> SHUTTING_DOWN, each lane's controller cleans its own processes, the monitor stops, the
active-epoch marker clears, and the runtime closes to CLOSED. CLOSED proves operational cleanup,
not task success. Repeated shutdown is safe and idempotent in effect: an already CLOSED runtime
returns `SHUTDOWN_OK` ("runtime is already CLOSED") after re-checking the closed inventory.
Follow the actual returned error; never delete records or invent state.

`lane force-stop --lane-id <id>` is only for a genuinely stuck lane, for example one named by a
`SHUTDOWN_LANE_CLEANUP_UNPROVEN` refusal:

- It acts only on the exact recorded PID-plus-creation identity of the original incarnation. An
  unrelated new process that happens to reuse a recorded numeric PID is not the original owner:
  it must survive untouched, and its presence alone does not make the original owner live. The
  original exact identities can still be proven gone by their recorded creation identities.
  Force-stop refuses when the original identity is unknown or unobservable, when records
  disagree or ownership changed, and when the exact original incarnation is still live
  (`FORCE_STOP_PROCESS_SURVIVED`). A numeric PID alone, absence by process name or a reused PID
  is never proof; never kill by name, broad matching or a bare PID.
- A genuinely never-launched lane (exactly empty process record and genuinely absent controller
  status) can be cancelled without a process; missing or uncertain cleanup proof returns
  `FORCE_STOP_CLEANUP_UNPROVEN`, and an exact recorded process still alive returns
  `FORCE_STOP_PROCESS_SURVIVED`.
- It publishes durable forced-cleanup proof, retires the lane and releases only that captured
  lane/run's leases. Then retry `harness shutdown`.

After `SHUTDOWN_OK` (CLOSED), `harness setup` opens a fresh runtime. A retained stopped lane from
a closed epoch can then use the supported saved-session retry:

```powershell
python -m orchestrator_harness.operator_launch resume-lane --lane-id <id> `
  --resume-task-card <fresh-card.json> --rationale "<reason>"
python -m orchestrator_harness.operator_launch lane launch --lane-id <id>
```

`resume-lane` requires the OPEN runtime produced by setup, the retained worktree and the saved
provider session; it prepares a fresh run id and starts nothing until `lane launch`. A lane
retired in the current epoch refuses resume ("resume is not a cleanup tool"), as do an accepted
lane and a lane that already used its initial assignment plus one explicit retry. There is no
automatic retry, no model switch, and no session, PID or worktree reconstruction.

## Where this contract lives

- `scripts/token_ledger.py` - `usage_gap_receipts`, `validate_usage_gap`, `extract_usage`,
  `build_snapshot` and `finalize` accounting.
- `scripts/collect_codex_usage.py` - `prepare_resume` guards and the `run`, `resume`, `snapshot`,
  `gate` and `finalize` commands.
- `orchestrator_harness/shutdown.py` - `run_shutdown` CLOSED/cleanup proof and refusal codes.
- `orchestrator_harness/launch.py` - `run_force_stop` exact-identity cancellation.
- `orchestrator_harness/resume.py` - `resume-lane` retained saved-session retry rules.
- `orchestrator_harness/lane_cleanup.py` - `cleanup_is_proven` native identity checks.

Focused regression coverage is `scripts/tests/test_collector_continuation.py`.
