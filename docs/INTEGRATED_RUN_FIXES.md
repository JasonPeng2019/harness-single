# Integrated harness fixes

This portable harness integrates launcher safety, ROOT lifecycle enforcement,
worker permissions and a project-configured build/test interface. It retains the
strict task-card contract, watch acknowledgement behavior and review/resume
recovery guards. `harness-config.json` splits the Git product workspace
(`root_workspace`) from the optional ROOT workspace (`manager_workspace`, default
`root_workspace`), which selects where ROOT hooks, skills and `.agent-workspace`
are installed; existing provider configuration fields, hook groups and unrelated
files are preserved.

## Implemented behavior

- Managed ROOT Stop rejects every OPEN runtime, including an empty queue with a
  running worker or no current epoch. Unreadable managed state fails closed.
  Handle events, settle lanes, shut down and verify CLOSED before exit. Plain
  mode and an already CLOSED runtime retain their existing behavior.
- Lane cancellation owns the exact recorded PID-plus-creation boundary and the
  lane's declared resource leases: `lane force-stop` stops prepared and
  busy-running lanes, proves cleanup before retiring the lane, and refuses when
  the identity is unknown or records disagree. Public `harness shutdown` reports
  CLOSED only for proven cleanup; CLOSED is not task-completion proof.
- Each worker launcher invokes its provider once. An invalid RESULT or startup
  failure preserves transcript, stderr, usage and cleanup evidence. It never
  triggers an automatic correction, resume, provider switch or repair.
- A lane/native session permits its initial assignment plus at most one explicit
  manual retry. Protected operator launch history spans run IDs; worker-local
  attempt logs are diagnostic mirrors. Missing/corrupt trusted saved-session
  history cannot reset the budget. Diagnose failures before a
  separate launch and use a fresh lane/session after the budget is spent. The
  retry is `resume-lane --lane-id <id> --resume-task-card <fresh-card.json>
  --rationale <reason>` followed by `lane launch`; it reuses the retained
  worktree/session/history and never becomes an automatic loop. A retired lane in
  the current epoch refuses resume with a preserved error.
- ROOT hooks and the native monitor are installed before the ROOT process starts.
  Fresh launches and resumes validate the run/workspace/config/hook-bound setup
  receipt. Installing hooks inside ROOT is too late for that session's discovery.
- Windows Codex launches require a read-only sandbox health gate by default.
  Missing local policy does not disable it. Marker, CLI binary, Codex home, local
  cache and retained probe evidence must match an operator-issued receipt.
  The gate never calls Codex, setup, elevation or fallback. A setup error or
  changed marker records a run-wide block and stops only the exact owned process
  boundary, including failures detected after fast exit.
- Worker isolation never denies an ancestor of ROOT/the lane. Linked workers
  receive directory writes for their own private Git metadata, shared objects,
  and an exclusive branch/ref/reflog parent. The default branch is
  `lane/<epoch>/<lane>/work`; explicit isolated branches need a dedicated parent
  with no ROOT/peer branches or retained reservation. ROOT retains and checks
  the Git directory, pointer/backlink, branch and origin assignment before use;
  ROOT's index, config and hooks remain ungranted. Harness/operator state and
  protected sibling workspaces remain denied.
- Native Windows public-check adapters and workers share the same read-denial
  policy through `orchestrator_harness.isolation_policy.shared_read_denials`.
  Public checks must not deny live source, worker worktrees or Git metadata that
  workers need. Execute an owned plain snapshot copy outside `operator-only`,
  granting writes only to that copy; see [the adapter contract](BUILD_TEST_INTERFACE.md).
  Isolated Codex worker bootstrap refuses required Git metadata beneath a denied
  ancestor; existing narrow private-Git/object/exclusive-ref write grants are unchanged.
- The optional deterministic build/test service preserves committed snapshots,
  exact request/revision identities, stdout/stderr/exit status, direct worker
  feedback, bounded checks, resume rebinding and shutdown cleanup. Its commands,
  input selection and backend come from operator configuration. No task adapter,
  dataset, fixed container image or evaluation runner ships in the harness.
- Controller, worker Stop, monitor, ROOT review/replay and retirement share the
  complete current-assignment RESULT contract. ROOT uses protected authority;
  a worker mirror cannot authorize acceptance. Acceptance requires clean project
  source at the exact assigned tip, with Git object contents verified and index
  flags unable to hide changes. FAIL/BLOCKED preserve dirty-source diagnostics.
  See the package README for explicit bookkeeping exclusions and unproven
  link/submodule/external-filter cases.
- Codex lanes can run the native OSS transport with `--provider-option
  launcher=ollama`: the launcher passes `--oss --local-provider ollama`, requires
  the operator-owned `local-config/ollama-model-catalog.json` to match the exact
  model slug and reasoning effort, and disables the provider's built-in web
  search because the Ollama 0.40.2 stream path can corrupt the replayed
  conversation. Hooks, worker isolation, approval policy and authentication
  handling are unchanged; built-in web search is unavailable in this launcher,
  nothing falls back to another provider automatically, and provider staffing
  fallbacks remain ROOT planning rather than harness behavior.

## Operator preparation

Configure a fresh harness copy and ROOT workspace, and optionally a separate
manager workspace. A wheel installation is module-only; a runnable harness root
also needs the sibling `adapters/`, `super-cache/`, `scripts/`, `tools/`,
`harness_watcher_implementation/`, `docs/` and `examples/` trees plus operator
configuration. For Windows Codex, choose a process-local LOCALAPPDATA cache and
keep it unchanged for setup, ROOT, controllers and workers. Obtain a real
isolated-command/protected-path probe for the exact CLI/home/cache before
issuing `local-config/windows-sandbox-health.json`.
`examples/windows-sandbox-health.example.json` is an UNVERIFIED schema template;
a copied PASS receipt is not a new probe.

Preserve sandbox errors and exact ownership evidence, diagnose and repair before
a separate launch. The harness makes zero sandbox setup requests and has no setup
retry or fallback loop. The pinned `max_setup_requests_per_run: 1` policy describes
the operator's separately authorized setup budget; it is not a counter over setup
requests made inside the native CLI. Verified launches need zero requests. The gate
and failure latch do not prove that a native CLI cannot attempt internal setup
before emitting a diagnostic. Never run speculative `codex sandbox ...
--help`, retry setup, switch implementations or broaden permissions to bypass
the gate. Machine ACL repair is an operator responsibility.

Native Codex Windows read denials may be applied to a persistent shared sandbox
principal rather than isolated per command. Changing the configured profile does
not itself prove that old denials were removed: SDK versions can leave stale
explicit deny ACEs after their state stops tracking them. If access still fails
with compatible profiles, preserve the exact failing command and compare the
actual ACL/principal with the intended policy before retrying. Any repair belongs
to an explicitly authorized operator procedure that backs up ACLs and removes only
proven stale entries while preserving unrelated protection. This distribution
ships no machine ACL cleanup, automatic permission reset or SDK-state edits.
[Portable Git layout guidance](PATH_PORTABILITY.md#native-worker-git-storage)
describes compatible storage before the first setup.

Before starting Codex ROOT, the sandbox gate reads the product workspace while
preparation selects the configured manager workspace:

```powershell
python -B scripts/check_windows_sandbox.py --harness-dir . --workspace <absolute-root-workspace>
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <configured-manager-workspace> --run-id <id>
```

Preparation invokes native setup once, validates hooks and writes the receipt
under the manager workspace's `.agent-workspace/ROOT_PRELAUNCH_SETUP.json`. Before
setup it writes a run/workspace/config-bound attempt record
(`.agent-workspace/ROOT_PRELAUNCH_ATTEMPT.json`); only that exact attempt authorizes
recovery of an interrupted preparation. If initial preparation fails before a receipt,
preserve every record, use native `harness shutdown`, correct the actual failure, and
explicitly prepare again with the SAME run ID and paths. An unrelated manual runtime
with no matching attempt or receipt still refuses; never delete or fabricate records
or broaden permissions. Repeating preparation for an identical prepared run only
revalidates that receipt; it never repeats setup. Use the same run ID
and paths with `scripts/collect_codex_usage.py`. Supply `--results-dir` explicitly
for a fresh run; the collector does not choose an evidence folder from its caller's cwd.

The collector holds an exact native ROOT process boundary, records its members,
and retains it after ROOT exits. Its existing watchdog also covers descendants
that retain stdout. Windows ROOT starts suspended in a kill-on-close Job before
execution; cleanup targets that held boundary rather than a numeric process tree.
The final manifest records cleanup proof and boundary diagnostics. Unproven
cleanup returns an error, prevents saved-session resume, and prevents new-run
accounting from granting launch credit while retaining countable token usage.
Historical manifests remain readable diagnostics; they cannot supply missing
native cleanup proof for resume.

Configure the optional [build/test interface](BUILD_TEST_INTERFACE.md) before
preparation. No service, backend, sandbox or provider startup is automatically
retried. ROOT resumes revalidate the original hooks, config, sandbox health,
run-wide block and service. Quiescent OPEN review needs the live service;
reopening a prepared CLOSED runtime requires the original service STOPPED with
cleanup proved; native `harness setup` and preparation with the SAME run ID then
revalidate the prior receipt/session/config/hooks and the exact stopped service
owner, explicitly start the enabled service with a fresh startup identity, and
archive the old owner, config and receipt. A lane resume or launch never starts
the service, and nothing retries automatically.

## Remaining work

CLOSED proves operational cleanup, not task completion. Task IDs/overviews,
registered task-card refresh and harness-owned final validation remain separate
unimplemented features.
Cancellation terminal-state/token accounting remains a separate gap. Source
changes alone do not prove a new live sandbox or agent run is healthy.
