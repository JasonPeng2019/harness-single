# ROOT task policy for benchmark run prompts

Include these requirements in the actual ROOT assignment for a benchmark run.
They guide ROOT; the native Stop gate currently enforces cleanup rather than
successful task completion.

1. Keep the objective, task constraints and required planner/implementer/reviewer
   roles explicit. Launch each worker through the native harness with the run's
   recorded model, reasoning effort and service tier. Preserve those settings on
   resume. If the experiment uses a different worker effort from ROOT, record
   that explicit override for every worker.
2. Have workers build and test their committed code during implementation using
   the prepared public-check route. Read compiler diagnostics and public case
   results, repair and retest before returning their contribution. Preserve the
   exact checked revision, commands and request/result evidence.
3. Keep a lane ACCEPT decision before integration and final validation. Accept
   and integrate a useful partial contribution when evidence shows an improvement
   over the current candidate without regressions or task-constraint violations.
   Commit it into ROOT and retain remaining failures. Incomplete overall coverage
   alone must not discard useful progress or leave it only on a worker branch.
4. While failures remain, launch a fresh repair lane/native session from the best
   integrated checkpoint. A rejected review, settled queue or cleanup is not a
   reason to finish the task. Each lane/session permits its initial assignment
   and at most one explicit manual retry; launch failures require diagnosis and
   never automatic retries or fallback.
5. Before reporting success, require a fresh independent validator against the
   exact integrated ROOT revision. Its initial task card must include the user's
   objective/rules, candidate commit, acceptance criteria, deliverables, existing
   public build/test results (commands, checked revision, exit statuses, stdout /
   stderr or exact evidence paths), and known remaining failures. Do not wait for
   it to discover that evidence. It must inspect/test integrated code, not an
   unintegrated implementer's checkout. Validation failure means fresh repair
   followed by another fresh validation.
6. Handle native events and worker results, settle/retire lanes, shut down and
   verify CLOSED before exiting. Report timeout/cancellation/incomplete work
   truthfully. Neither lane ACCEPT nor CLOSED is final task acceptance.

Do not add an interface-agent supervisor, AI watcher, ROOT relaunch loop, hidden
benchmark-test access, broad Docker permissions or new memory infrastructure to
implement this guidance. Harness-owned final validation is a separate specified
feature; it has not been added by the fix consolidation.
