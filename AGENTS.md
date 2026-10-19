# Portable harness operating rules

Before any live use, read `QUICK_RULES.md` and follow `QUICK_START.md`. Consult `README.md`,
`orchestrator_harness/SPEC.md`, and `docs/HARNESS_WATCHER_GUIDE.md` for the full contracts.
Implementation workers follow the current scoped task card supplied by ROOT;
archived plans and historical ownership records do not grant additional work.

- Use the native harness directly. Do not add a runner, wrapper, scheduler, retry controller,
  collaboration relay, or AI watcher subagent.
- Keep the deterministic watcher diagnostic-only with `evaluator_enabled: false` unless the user
  explicitly authorizes a different topology.
- The harness and watcher never decide, schedule, repair, operate hardware, or replace the root
  orchestrator.
- The orchestrator must discover requests only through the native blocking wait.
- Keep source directories free of runtime output. Put local configuration under `local-config/`
  and all portable runtime output under `runtime/`.
- Keep the entire product portable: do not hardcode absolute machine, user,
  installation, workspace, or project paths in code, scripts, tests, or shipped
  configuration. Relative path constants are allowed only for files internal to
  the harness. Obtain external paths from explicit configuration, arguments,
  environment, operating-system discovery, or owned temporary fixtures. Never
  ship a configuration that silently selects a developer's external workspace.
- Prioritize the everyday user workflows and preserve working features. Before
  fixing a possible problem, weigh its likelihood in normal use and consequence.
  Fix common workflow failures and credible rare risks of lost work, security
  failures, silent corruption, or damage that is hard to undo.
- Prefer the smallest change that gets the real workflow working. Review affected
  callers and meaningful safety boundaries; private code needs to support its
  actual callers. Do not add impossible-input checks, hypothetical abstractions,
  or broad rewrites just to satisfy improbable tests.
- A clear error is usually enough for an uncommon unsupported use. Record
  lower-priority concerns briefly for the user rather than fixing them unasked.
  Finish when the everyday workflows work end to end and serious risks are
  covered; exhaustive internal-path audits and repeated whole-product review
  cohorts are not required.
- Assign product implementation and substantive integration repairs to fresh,
  scoped implementation workers, including shared-file changes. ROOT approves
  scopes/results, evaluates criticisms, integrates approved work, and manages
  agents and shutdown. Transfer shared paths with one implementation writer at a
  time; ROOT does not become the default coder or edit worker-owned files.
- Testing, validation, and review subagents are advisory only. They can NEVER
  block progress, veto acceptance, require fixes, or prevent shutdown. ROOT alone
  judges their criticisms and assigns justified repairs. Pending reports and
  reviewer verdicts are never completion dependencies; actual checks are reported
  honestly. Advisory agents have read-only product source and private evidence,
  with no product writing/resource lease or required managed-lane acceptance/Stop
  obligation.
- Deploy fresh independent testers for each completed implementation step and
  fresh semantic reviewers after the first full coding workflow, after retry/
  cancellation/service work, and with final testing before delivery. Use fixed
  completed source and private test copies so coding can continue. Scheduled
  advisory reports and verdicts never gate progress, acceptance or shutdown.
- Use a fresh epoch and fresh runtime directories for every live run.
- Preserve exact event identities: `data.signal_id` identifies the worker request; the envelope's
  top-level `event_id` is used for native acknowledgement.
- Validate response identities before atomic publication. Acknowledge only after successful
  handling.
- Stop cooperatively and verify exact PID plus creation identity. Never kill by broad name or
  command matching.
- Treat harness or watcher difficulty as a defect/configuration signal; do not hide it with support
  machinery.
