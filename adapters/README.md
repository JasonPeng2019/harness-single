# Provider adapter catalog

This directory is the shipped adapter catalog.  Each provider has one tree
under `adapters/<provider-id>/`:

- `README.md` — what to put below (per-tree).
- `root/` — installed into the ROOT workspace as the provider's ROOT payload
  (`<provider dotdir>/` with config, hooks, binding, and the eight ROOT
  skills).
- `super-cache/` — installed into `<rt>/super-cache/adapter-payloads/<provider-id>/`
  and copied into every managed worktree for that provider (worker hooks,
  config, binding, and the two worker skills).
- `harness/launcher_binding.py` — registered under
  `orchestrator_harness/provider_adapters/<provider-id>/` by setup.
- `shipped-machinery/` — documentation for the adapter author.

## Binding contract

Each `harness/launcher_binding.py` must define the strict symbols:

- `PROVIDER_ID` — the provider id, matching the catalog directory name.
- `ADAPTER_VERSION` — a version string for the binding.
- `build_argv(*, model, worktree, prompt_path, session_id=None, resume=False)`
  — assemble the provider's headless launch argument vector.
- `parse_line(line)` — read one provider output line into a dict with
  optional `message` and `session_id` keys, or `None`.

The controller loads the single registered binding and checks that its
`PROVIDER_ID` matches before use.

## Skill contract

Each ROOT payload ships exactly eight thin-wrapper skills
(`manager-notification-watch`, `acknowledge-manager-notification`,
`close-manager-notification`, `review-lane-completion`,
`send-lane-notification`, `resume-lane`, `force-stop-lane`,
`harness-shutdown`), each wrapping one existing public command.  Each worker
payload ships exactly two worker skills (`manager-notify`, `lane-assignment`)
that call only the provider-neutral helpers
`.agent-workspace/manager-notify.py` and `.agent-workspace/lane-queue.py`.

## Installation

`harness setup` installs ROOT payloads into the root workspace, stages the
active super-cache (workspace base plus per-provider payloads), and registers
the launcher bindings.  `lane bootstrap` materializes the workspace base and
the selected provider payload into each new worktree.
