# Quick Rules

## Authority

1. One persistent manager owns planning, lane assignment, launches, decisions, integration,
   acceptance, promotion, and final cleanup.
2. One coding worker runs in each active branch/worktree lane.
3. A merge worker integrates only the branches assigned by the manager.
4. The native observer reports facts and delivers durable events; it does not schedule or decide.
5. The deterministic watcher is optional and diagnostic-only with `evaluator_enabled: false`.

## Git lanes

- Commit before splitting a lane; branch every child from the recorded stable commit.
- Use a separate worktree for every concurrently active lane.
- Never switch or reuse another live lane's branch or worktree.
- Integrate on a dedicated branch/worktree, not in a worker lane.
- Keep the frozen known-good revision unchanged. Candidate, acceptance, and promotion are distinct.
- Git is the source ownership boundary; do not add a file-ownership database.

## Durable identity

- Bind every coding invocation to lane ID, worker invocation ID, worktree, branch, common Git
  directory, and full base commit.
- Treat `PARALLEL_CHECKPOINT.md` as resumable progress only.
- Treat `RESULT.json` as merge-ready only after schema, invocation, branch-tip, and cleanliness
  validation succeeds.
- A stale, malformed, mismatched, non-tip, or dirty result is not completion.

## Events

- Discover manager work through native `watch --until-actionable`, not transcript inspection,
  custom polling, a second queue, or a notification relay.
- Delivery is at-least-once. Deduplicate with the top-level `event_id`.
- Acknowledge only after handling and verifying the event.
- Acknowledge the top-level `event_id`, never `data.signal_id`.
- Leave an unhandled event pending; an acknowledged event is not redelivered, so watch again for
  new work after handling it.

## Resources and processes

- Declare external exclusivity with exact opaque `exclusive_resources` names.
- Ordinary contention waits without manager intervention.
- Unknown identity is never safe absence. Reclaim only when exact PID-plus-creation evidence proves
  the prior owner absent.
- A controller releases only claims owned by its exact invocation.
- Cancel a prepared or running lane only through `lane force-stop` and its exact recorded
  PID-plus-creation identity; never kill by broad process name or command matching, and treat an
  unobservable identity as a refusal rather than absence.
- Each launcher gets one provider attempt; never automatically retry, refresh sandbox setup,
  automatically switch modes or bypass a health gate. Fresh Windows Codex bootstrap defaults to write-restricted
  `worker-workspace`; resume preserves the current effective mode unless explicitly changed. Strict
  `worker-isolated` and full-access `yolo` are explicit choices. Strict remains the meaning of a
  missing mode in historical invocations. Windows `worker-workspace` readiness uses the native
  one-attempt probe and a mode-specific receipt. This mode does not claim read confidentiality and
  the probe does not qualify strict mode. Preserve and diagnose failures before a separate launch.
- A lane/session permits initial assignment plus at most one explicit manual retry across run IDs.
  Retry explicitly with `resume-lane --lane-id <id> --resume-task-card <fresh-card.json>
  --rationale <reason>` in the same worktree/session, then `lane launch`; there is no automatic
  loop, and a lane retired in the current epoch refuses resume with a preserved error while
  supported retained stopped lanes across closed epochs remain resumable. A complete explicit model
  and provider-option replacement starts a fresh session in the same worktree; preserved history and
  retry/resource counts are not reset. See `docs/RESUME_PROFILES.md`.
- For a fresh managed ROOT, run `scripts/prepare_root_launch.py` first; it performs native setup internally. ROOT sandbox mode is selected and receipt-bound independently from worker permissions. Isolated ROOT requires matching readiness evidence; explicit `danger-full-access` has no OS sandbox and skips readiness for that unused sandbox while retaining any existing run-wide failure block. Fresh Windows Codex bootstrap defaults to `worker-workspace`; resume preserves the current mode. Its one-attempt native probe and mode-specific receipt are separate from ROOT's receipt. `worker-isolated` is an explicit strict choice; `yolo` is explicit full access and is not the normal worker recipe. Historical invocations with no worker mode retain strict meaning. Never delete or fabricate runtime records or receipts.
- Managed ROOT Stop requires runtime CLOSED, not just an empty queue. CLOSED proves cleanup,
  not task success.

## Runtime hygiene

- Use fresh manager-epoch configuration and ignored `runtime/` directories.
- Run `scan --no-write` before launch.
- Keep generated state out of source packages and observed `.agent-workspace` trees except for the
  lane records intentionally written there.
- Freeze product code and live configuration during an acceptance run.
- Preserve failures as evidence; do not hide them with wrappers, schedulers, retry controllers, or
  mid-run automatic repair.

## Release checks

- Use `python -m orchestrator_harness.release_checks` as the one stable-ID registry/selector.
- Credit requires declared-input fingerprints plus exact source root, Git common directory, and
  branch; its origin tip must remain an ancestor of the current tip. Unknown, divergent, mixed,
  or stale credit is not green evidence.
- Fast selection is local; WSL/real-agent and accumulated release assurance are never default-fast.
- `tools/Invoke-CandidateSafeguard.ps1` accepts an exact repository root and reads its branch/tip;
  it can consume selector credit and rechecks root/common-directory/branch/tip, baseline/config,
  and cleanliness after each selector-owned component. It has no developer checkout path. The
  safeguard is candidate-only and release-owned.
