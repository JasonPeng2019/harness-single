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
- Windows Codex launches use a read-only readiness gate for the selected worker
  profile. A v2 health receipt binds the permission mode, generated-profile
  policy digest, elevated Windows backend, CLI, Codex home, process-local cache,
  marker, linked-worker probe profile and retained probe evidence. Fresh reduced
  workers do not need the old broad read-denial set. The gate never calls Codex,
  setup, elevation or fallback. A setup error or changed marker records a
  run-wide block and stops only the exact owned process boundary, including
  failures detected after fast exit.
- Codex ROOT preparation and collector launch select the same explicit `--sandbox`:
  `workspace-write` (unchanged default), `read-only`, or `danger-full-access`.
  Preparation records the mode, and launch/resume refuses a mismatch. Explicit full
  access skips readiness for an unused OS sandbox; it retains the run-wide block,
  exact ROOT/session/configuration/service identities and cleanup guards. This is
  an operating compromise, not sandbox isolation. Worker `permission_mode=yolo`
  is selected independently and retains its existing behavior. Nothing falls back
  after startup failure or changes a running/prepared run's mode.
- Fresh Windows Codex workers default to
  `permission_mode=worker-workspace`, which retains the write sandbox but adds
  no harness-generated broad read-denial entries and does not enumerate sibling
  projects. It can read other locations where the remaining OS/native policy
  permits reads. `worker-isolated` explicitly selects the stricter read-denial
  profile; absent mode in historical saved invocations remains strict. `yolo`
  remains a separate explicit full-access choice. Non-Windows defaults are
  unchanged.
- Both restricted Codex profiles grant the lane worktree plus only its private
  Git metadata, shared objects, and exclusive branch/ref/reflog parent. The default branch is
  `lane/<epoch>/<lane>/work`; explicit isolated branches need a dedicated parent
  with no ROOT/peer branches or retained reservation. ROOT retains and checks
  the Git directory, pointer/backlink, branch and origin assignment before use;
  ROOT's index, config and hooks remain ungranted. Strict mode also denies the
  recorded operator and sibling paths; the reduced mode makes no such read
  confidentiality claim.
- Windows restricted Codex controllers receive a copied environment whose TEMP/TMP point
  to `.agent-workspace/tmp/<run_id>` in the lane. Each invocation has its own directory; prior
  artifacts and the parent's environment are preserved. Yolo, other providers and non-Windows
  launches retain their existing environment. This mitigates shared-temp ACL traversal; it does
  not qualify the complete broad read-denial profile or concurrent shared-principal behavior.
- Native Windows public-check adapters execute an owned plain snapshot copy
  outside `operator-only`, with writes limited to that copy; see
  [the adapter contract](BUILD_TEST_INTERFACE.md). Fresh public-check policy v3
  selects `worker-workspace`; a native Windows adapter must declare that mode,
  retain its write sandbox and report no shared read-denial policy. A reduced
  worker cannot be routed to a conflicting strict adapter. Historical policy v2
  retains its strict meaning; strict adapters remain explicit and require
  separate qualification.
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
- Claude hook commands in ROOT and worker payloads resolve from the quoted
  `${CLAUDE_PROJECT_DIR}`. Setup and inherited worker bootstrap migrate only the
  exact known legacy hook groups, deduplicate old-plus-new registrations, and
  preserve other provider settings. Altered or missing inherited owned groups
  refuse without changing settings. See [general harness repairs](GENERAL_HARNESS_REPAIRS.md)
  for the R1–R7 summary and a portable reproduction outline.
- Inherited generated Codex profiles are rebuilt from the supported workspace
  defaults and current lane grants rather than carrying parent grants forward
  (R3). On Windows, worker-isolated Codex gets per-invocation private TEMP/TMP
  directories (R4); this mitigates shared-temp ACL traversal but does not qualify
  the complete isolated profile. Explicit full-access ROOT mode remains an
  operating compromise without OS sandbox enforcement (R7).
- The monitor leaves provisional absent/invalid worker results to the live exact
  controller's finalization and still surfaces explicit failures, dead or
  mismatched controllers, lease issues, transcript contradictions and unproven
  cleanup (R5). Usage accounting rejects duplicate `controller_started` evidence
  within one run before pairing provider events and transcripts (R6). These
  checks report ambiguity or failure; they do not start a retry or authorize a
  second launch. Targeted local evidence is recorded with the repair artifacts;
  no full live demo claim is made here.

## Operator preparation

Configure a fresh harness copy and ROOT workspace, and optionally a separate
manager workspace. A wheel installation is module-only; a runnable harness root
also needs the sibling `adapters/`, `super-cache/`, `scripts/`, `tools/`,
`harness_watcher_implementation/`, `docs/` and `examples/` trees plus operator
configuration. For Windows Codex, choose a process-local LOCALAPPDATA cache and
keep it unchanged for setup, ROOT, controllers and workers. Run the one-attempt
linked-worker probe below; it creates an owned two-worktree fixture, tests lane
and narrow Git writes, rejects protected writes, checks unchanged sentinels, and
records exact cleanup. It publishes a v2 receipt only after those observed checks
pass. The probe preserves prior evidence and never resets machine ACLs.
The example health files are UNVERIFIED templates; a copied PASS receipt is not
a new probe.

```powershell
python -B scripts/probe_windows_worker_isolation.py --harness-dir . `
  --output-dir runtime/windows-worker-probe/<run-id> `
  --local-appdata runtime/windows-worker-probe-cache/<run-id>
python -B scripts/check_windows_sandbox.py --harness-dir . `
  --workspace <absolute-root-workspace> --permission-mode worker-workspace
```

The worker receipt is kept separately at
`local-config/windows-sandbox-health-worker-workspace.json`. ROOT's historical
generic receipt stays at `local-config/windows-sandbox-health.json`; neither
replaces the other. To requalify an existing worker receipt, pass
`--replace-existing-health`; the probe copies the prior receipt into its new
evidence directory before updating the selected mode's health file.

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

For isolated Codex ROOT, the sandbox gate reads the product workspace while
preparation selects the configured manager workspace:

```powershell
python -B scripts/check_windows_sandbox.py --harness-dir . --workspace <absolute-root-workspace>
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <configured-manager-workspace> --run-id <id> --sandbox workspace-write
```

On hosts where that sandbox profile cannot operate, a fresh run can explicitly use
`--sandbox danger-full-access` in preparation and in `scripts/collect_codex_usage.py run`.
Use `--provider-option permission_mode=yolo` for each worker that should also run without
OS sandbox enforcement. This preserves native orchestration while granting commands the
current user's access. It is not a repair for an existing run-wide sandbox block. ROOT
and worker modes are independent; choosing full-access ROOT does not authorize an isolated
worker to skip its own health proof. See the complete command recipe in `QUICK_START.md`.

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
cleanup proved; native `harness setup` and preparation with the SAME run ID and ROOT sandbox mode then
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
