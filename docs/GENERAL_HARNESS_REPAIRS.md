# General harness repairs and operational reproduction

This note summarizes the R1–R7 repairs and gives a portable outline for an operator-run check
with a small, unrelated code project. Commands use placeholders for local paths and provider
settings. They are instructions, not evidence that a live provider demo has run.

## Repair summary

- **R1/R2 — Claude hooks and upgrade migration.** ROOT and worker hook commands quote
  `${CLAUDE_PROJECT_DIR}`, so nested working directories resolve scripts from the project root.
  ROOT setup handles exact legacy-only and old-plus-current registrations, leaving one current
  registration per event. Inherited worker bootstrap migrates only exact known legacy groups.
  Other settings are preserved; altered or missing inherited owned registrations refuse without
  mutation.
- **R3 — inherited generated Codex profile.** Worker bootstrap regenerates only the recognized
  harness-generated suffix for the current worktree and Git identity. It preserves the user's
  profile prefix, current read denials, and narrow exclusive Git grants; custom or malformed owned
  suffixes refuse. The repair and its focused controls do not establish acceptance of the complete
  isolated profile.
- **R4 — private Windows temporary files.** Windows worker-isolated Codex receives per-invocation
  TEMP/TMP directories inside its lane. This mitigates shared-temp ACL traversal; it does not prove
  the full broad-denial profile works in every environment.
- **R5 — monitor finalization.** A provisional absent or invalid result stays nonterminal while the
  exact controller remains alive and owns finalization. Explicit failures, dead or mismatched
  controllers, orphaned leases, transcript contradictions, and unproven cleanup remain actionable.
  The monitor adds no timer, retry, fallback, or second launch authority.
- **R6 — usage evidence ambiguity.** Accounting rejects duplicate `controller_started` evidence
  for one run before pairing provider events and transcripts. This is an evidence-integrity guard;
  it does not replace native dispatch serialization or authorize a retry or second launch.
- **R7 — explicit ROOT sandbox mode.** Codex preparation and collector launch bind the same
  selected mode. Isolated modes keep their readiness gate; explicit `danger-full-access` skips
  readiness for the unused OS sandbox while retaining the run-wide failure block and ownership
  checks. Full access provides no OS sandbox enforcement. This compromise and the other fixes do
  not establish full isolated-profile acceptance.

## Portable reproduction outline

Use a disposable harness copy and a tiny code project that is independent of this harness and its
reference material. Set up a small task such as changing one function and adding a focused test.
Keep source, local configuration, and runtime output in their documented locations. Select a
provider and its model preferences explicitly for the environment; no shipped preference is
implied here.

1. Configure `harness-config.json` with the project's absolute `root_workspace`, optional
   `manager_workspace` for ROOT hooks and skills (defaults to `root_workspace`), and
   `managed_coordination: "enabled"`. Configure `resource-manifest.json` and a complete
   `project-task-card/v1` task card. Review [Quick Start](../QUICK_START.md) for provider-specific
   prerequisites and the chosen ROOT operating mode.
2. For a fresh managed ROOT, prepare hooks and the monitor before starting ROOT:

   ```powershell
   python -B scripts/prepare_root_launch.py --harness-dir . --workspace <manager-workspace> --run-id <run-id> --sandbox <selected-codex-mode>
   ```

   Select the same mode when launching the Codex collector. If using another ROOT provider, select
   it explicitly as documented in [Quick Start](../QUICK_START.md). Native
   `python -m orchestrator_harness.operator_launch harness setup` is for manual integration or
   reopening an already prepared ROOT; follow the documented receipt and service checks.
3. Bootstrap and launch one lane through the public launcher. Supply the provider, its configured
   model, any required provider options, and the task card:

   ```powershell
   python -m orchestrator_harness.operator_launch lane bootstrap --lane-id <lane-id> --provider <provider-id> --model <configured-model> --provider-option <name>=<configured-value> --task-card <task-card.json>
   python -m orchestrator_harness.operator_launch lane launch --lane-id <lane-id>
   ```

   Repeat `--provider-option` for provider-specific preferences and any explicitly selected
   permission mode. Bootstrap and launch are explicit; a failed or invalid run does not trigger an
   automatic retry or provider switch.
4. Wait using native `watch` and acknowledge each event's top-level event ID. For an ordinary
   notification, handle it and close it with `manager close`. For a completion-review event,
   acknowledge it and then use `lane completion-review --event-id`; that command records the
   finding and decision and closes its review event. Do not manually close the review event first.

   ```powershell
   python -m orchestrator_harness.operator_launch watch --until-actionable --timeout <duration>
   python -m orchestrator_harness.operator_launch manager acknowledge --event-id <notification-event-id>
   python -m orchestrator_harness.operator_launch manager close --event-id <notification-event-id> --outcome COMPLETE --summary <handling-summary>
   python -m orchestrator_harness.operator_launch watch --until-actionable --timeout <duration>
   python -m orchestrator_harness.operator_launch manager acknowledge --event-id <review-event-id>
   python -m orchestrator_harness.operator_launch lane completion-review --event-id <review-event-id> --review-outcome PASS --approval ACCEPTED --review-summary <review-summary>
   ```

   Acceptance requires ROOT's PASS finding; worker output alone is not acceptance.
5. Retire the accepted lane, then shut down and confirm the runtime reaches CLOSED:

   ```powershell
   python -m orchestrator_harness.operator_launch lane retire --acceptance-ref <acceptance-reference>
   python -m orchestrator_harness.operator_launch harness shutdown
   ```

### Optional external public checks

An operator may configure the build/test service before ROOT preparation. Pin the check adapter,
its dependencies, and any external tool inputs to reviewed immutable revisions in operator-owned
configuration. The public check should run against the service's committed snapshot and report its
command, revision, output, and exit status. Keep the check implementation and dependencies outside
the harness source and worker-editable project; grant writes only to its owned snapshot copy. The
harness ships no fixed check, dataset, container image, or evaluation runner. See the
[build/test interface](BUILD_TEST_INTERFACE.md) for its contract and limits.

## Evidence boundary

Targeted implementation evidence and its limitations are recorded in the local repair results.
Product documentation describes behavior and reproduction steps only. A live demonstration still
requires the operator to run the complete native workflow and inspect its retained evidence; no
provider, broad isolated-profile, or full live-run acceptance is claimed here.
