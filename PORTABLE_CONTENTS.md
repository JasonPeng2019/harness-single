# Portable contents

Snapshot date: 2026-08-02.

## Included

- Complete production Python code from `orchestrator_harness/`.
- Complete shared `harness_common/` process-identity code.
- Complete production Python code and schema from `harness_watcher_implementation/`.
- Harness and watcher unit/integration tests that are self-contained in this portable tree.
- Example configuration, setup instructions, specifications, attention-logging documentation, and
  testing guides.
- One static harness configuration fixture required by a copied unit test.

## Deliberately excluded

- `multi-agent-logs/`, `harness_watcher/<epoch>/`, and `fresh-experiments/`.
- Canary state directories, generated JSONL, snapshots, pending notifications, caches, and
  `test_results/`.
- Historical M5 plans, reviews, checkpoints, handoffs, verdicts, and repair prompts.
- The AI watcher-subagent relay model and all collaboration-notification machinery.
- Obsolete owner/watcher wrappers and their repository-history-only test.
- Firmware server code and firmware experiment artifacts; they are observed targets, not harness or
  watcher dependencies.

The portable runtime generates new state only under paths selected in local configuration. No
historical runtime evidence was copied into this folder.

