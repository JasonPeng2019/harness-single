# Quick Rules for Any Agent

These rules preserve the minimal working system and prevent helpers from silently replacing the
capability being used.

## Runtime roles

1. **One persistent orchestrator is the only manager.** It launches workers, makes decisions,
   manages resources, and responds to requests.
2. **Workers perform the real task.** They do not manage other workers or control the harness.
3. **The native harness only observes and delivers durable events.** Use its blocking wait directly.
4. **The deterministic watcher is diagnostic-only.** Keep `evaluator_enabled: false`. It does not
   wake the orchestrator, notify through collaboration, repair code, acknowledge events, or act.
5. **No AI watcher subagent.** Do not spend a subagent slot or model compute on notification relay.

## Forbidden assistance

Do not create or use:

- a runner around the harness;
- watcher or harness wrappers;
- notification relays or collaboration-message wakeups;
- custom polling loops, schedulers, retry controllers, or event mirrors;
- a second queue or alternate request-discovery path;
- transcript inspection, direct worker-message inspection, or user messages to discover requests;
- a helper subagent that monitors, prompts, or compensates for the orchestrator; or
- mid-run code repair or automatic repair machinery.

If the native harness is difficult or fails, preserve that as evidence. Do not hide it with another
layer.

## Required event discipline

- Discover actionable work only through the native blocking wait.
- Preserve the returned `wake_id`.
- `data.signal_id` is the worker/source event identity.
- The top-level `event_id` is the native harness identity used for acknowledgement.
- Validate every required response identity before atomic publication.
- Acknowledge only after the response or management action is complete and verified.
- Never repair malformed published data in place and then pretend the original publication passed.
- An unhandled event remains pending; do not discard or silently skip it.

## Run discipline

- Use a fresh epoch, config, output directory, watcher runtime, and cursor for every run.
- Confirm configuration and discovery with `scan --no-write` before launching workers.
- Freeze code and configuration during live work.
- Let a safe live run reach its natural boundary even when an agent makes a mistake. Stop early only
  for a real safety, authorization, resource-ownership, or evidence-path failure.
- Make changes only after workers are safe, the harness and watcher are stopped, evidence is
  preserved, and exact cleanup is proven.
- Do not treat an orchestrator or worker mistake as a harness/watcher bug without evidence.
- Do not treat watcher analysis as execution truth; it is diagnostic evidence for the orchestrator
  to audit.

## Process, resource, and hardware safety

- Use exact PID plus provider creation identity. Never kill by process name or broad command match.
- Stop services cooperatively before considering exact targeted termination.
- The harness and watcher grant no hardware, flashing, deployment, lease, or repair authority.
- Follow the target project's existing authorization, lease, checkpoint, and hardware rules.
- Never commit, push, deploy, or flash merely because the harness returned an event.

## Files and logs

- Keep code under `orchestrator_harness/`, `harness_common/`, and
  `harness_watcher_implementation/`.
- Keep local configs under ignored `local-config/`.
- Keep all portable runtime output under ignored `runtime/`.
- Never write logs, generated evidence, caches, pending state, or canary output into source folders.
- Do not reuse runtime data from a previous epoch.

## Important limitation

The no-relay system works while the persistent orchestrator remains active and repeatedly returns to
the blocking wait. The deterministic watcher cannot wake or restart a closed, crashed, or terminated
agent conversation. Do not claim otherwise.

## Before starting checklist

- [ ] I read this file and `QUICK_START.md`.
- [ ] I am the one persistent orchestrator and understand I remain the decision-maker.
- [ ] The watcher is deterministic-only with `evaluator_enabled: false`.
- [ ] No relay, wrapper, runner, helper watcher, scheduler, or alternate discovery path exists.
- [ ] The epoch and all runtime directories are fresh.
- [ ] `scan --no-write` shows exactly the intended runs and lanes.
- [ ] Worker controllers, resources, authorization, and cleanup responsibilities are known.
- [ ] I will validate response identity before publication and acknowledge only the native event ID.
