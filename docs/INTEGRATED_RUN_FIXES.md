# Integrated run fixes

The fixes recorded in the bench-testing repository's `maybe_todo.txt` were
consolidated into both reusable harnesses on 2026-10-10. They were ported from
`root_runs/harness-single-updated-pre-generic-20261007/` without copying its run
launchers, prompts, credentials, runtime state, or machine-specific PASS receipt.
The updated harness retains its memory support; the generic harness retains its
strict task-card contract and review/resume recovery guards.

## Implemented behavior

- Managed ROOT Stop rejects every OPEN runtime, including an empty queue with a
  running worker or no current epoch. An unreadable managed runtime fails closed.
  ROOT must handle events, settle lanes, shut down and verify CLOSED before exit.
  Plain mode and an already CLOSED runtime retain their existing behavior.
- Each worker launcher invokes its provider once. An invalid RESULT or startup
  failure preserves transcript, stderr, usage observations and cleanup evidence;
  it never triggers an automatic correction, resume, provider switch or repair.
- A lane/native session has an initial assignment plus at most one explicit
  manual retry. The append-only attempt ledger counts actual provider launches
  across run IDs. Missing/corrupt saved-session history cannot reset that budget.
  Diagnose a failure before preparing a separate launch; use a fresh lane/session
  for further work after the budget is spent.
- Codex ROOT hooks and the native monitor are installed before the ROOT process
  starts. The collector checks the run/workspace/config/hook-bound prelaunch
  receipt before a fresh harness ROOT launch. Setup inside ROOT is too late for
  that session's hook discovery.
- Windows Codex launches require a read-only sandbox health gate by default.
  Missing local policy does not disable it. Marker, CLI binary, Codex home,
  process-local LOCALAPPDATA cache and retained probe evidence must match an
  operator-issued receipt. No gate calls Codex, setup, elevation or fallback.
  A startup setup error or changed marker records a run-wide block and stops only
  the exact owned process boundary, including failures detected after fast exit.
- Worker isolation never denies an ancestor of ROOT/the lane. Linked workers
  receive Git writes for their own metadata and shared object/branch stores;
  ROOT's index, config and hooks remain ungranted. Harness/operator state and
  protected sibling workspaces remain denied.
- An optional deterministic public-check service stages a lane-local client,
  tests the requested committed revision in an offline pinned container and
  returns revision, stdout, stderr, exit status and cleanup evidence directly to
  that worker. Resume refreshes its lane/run binding; shutdown stops the service.
  No ROOT model mediation or direct Docker/harness access is needed by workers.

## Operator preparation

Configure a fresh run copy and ROOT workspace. For Windows Codex, choose a
process-local LOCALAPPDATA cache for that run and keep it unchanged for setup,
ROOT, controllers and workers. Obtain a real isolated-command/protected-path
probe for the exact CLI/home/cache before issuing
`local-config/windows-sandbox-health.json`. Use
`examples/windows-sandbox-health.example.json` only as a schema template; it is
intentionally UNVERIFIED. A copied PASS receipt is not a new probe.

If sandbox setup fails, preserve the error and exact ownership evidence, diagnose
and repair it before any separate launch. At most one separately authorized setup
request is allowed per run; ordinary verified launches need zero. Never run
speculative `codex sandbox ... --help`, retry setup, switch implementations or
broaden worker permissions to bypass a failed gate. The gate does not perform
machine ACL repair. The bench-testing history retains the diagnosed WRITE_DAC /
empty-marker failure and its bounded one-attempt repair/probe evidence.

From the configured harness copy, before starting Codex ROOT:

```powershell
python -B scripts/check_windows_sandbox.py --harness-dir . --workspace <absolute-root>
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <absolute-root> --run-id <id>
```

Preparation calls native setup once, validates installed hooks and writes
`<root>/.agent-workspace/ROOT_PRELAUNCH_SETUP.json`. Repeating preparation for an
already prepared identical run only validates its receipt; it never repeats
setup. Existing runtime without that matching receipt is refused. Use the same
run ID and harness/workspace paths with `scripts/collect_codex_usage.py`. Keep
benchmark-specific PowerShell launchers in the run copy.

Planned ROOT resumes revalidate the same receipt, hooks, configuration and
sandbox/run block without repeating setup. A quiescent OPEN review still needs
its live public-check service. A CLOSED resume requires the original service to
be STOPPED with cleanup proved; it does not restart that service or reopen lanes.

## Direct public build/test route

The shipped task adapter is specifically the public ZSTD decoder benchmark. It
is disabled for other tasks unless a matching task adapter is supplied; it is
not a universal CI framework. Copy `examples/public-check-policy.example.json`
to `local-config/public-check-policy.json` in the configured run copy before
preparation. ROOT must contain committed public task inputs and Docker must
already expose the exact pinned image in `tools/public_task_check.py`.
Preparation starts the service once and records its exact identity/policy hash.
No daemon, image, sandbox or provider startup is automatically retried.

Bootstrap stages `.agent-workspace/public-check.py` and its registered route in
each managed lane. The worker commits task code/scratch before requesting:

```powershell
python .agent-workspace/public-check.py request --check build
python .agent-workspace/public-check.py request --check public
python .agent-workspace/public-check.py wait --request-id <id> --timeout 30
```

Focused commands use `request --check focused -- <container command argv>`.
Exit 75 means pending: wait again on the same request within the active worker;
do not duplicate it or finalize RESULT while waiting. Other nonzero statuses
carry failure feedback. Repair, commit and request a new check for the new tip.
Repeated build/test calls inside that worker are not process-launch retries.
Only committed task-visible files and scratch enter the container; fixed public
inputs, Makefile and public harness source must match the baseline. Containers
have no network or host mounts. Results bind lane/run/request/revision; stale,
foreign, tampered or changed-tip requests are refused and repeats are deduplicated.
After a lane resumes, historical route registrations remain evidence. Each
request is dispatched only to its matching lane/run registration, and foreign
routes cannot publish a result into that request's directory.

## Remaining work

CLOSED proves operational cleanup, not task completion. The task-ID/overview,
registered task-card refresh and harness-owned final-validator design in the
bench-testing `TASK_COMPLETION_IMPLEMENTATION_SPEC.md` is still unimplemented.
`ROOT_TASK_POLICY.md` carries the fresh-validator/public-evidence and incremental
integration/fresh-repair requirements as reusable run-prompt guidance, not a
new completion gate. A run must include them in its actual ROOT assignment.
Cancellation terminal-state/token accounting remains a separate lower-priority
gap. Historical proof is preserved; integrating source does not constitute a
new model run, benchmark score or machine-specific native sandbox health proof.
