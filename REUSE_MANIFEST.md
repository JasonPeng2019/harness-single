# Reuse manifest

This product seed composes pinned working-tree snapshots. Nested Git metadata is
deliberately excluded; the source repositories remain under the outer
workspace's `references/` directory.

| Component | Source | Source identity | Destination | License/status |
| --- | --- | --- | --- | --- |
| Orchestrator Harness v2 | `references/harness-single` | `0bf05794b1a6ad2ec1ba4916a8b32e42be548c2d` (`working/memory-v1`) | `harness/` | No license file was present in the supplied snapshot; resolve before redistribution. |
| ROOT workflow/skill suite | `references/Codex_Claude_Setup` | `b3b683e6d3396343a8ed68801cd96e7787db1e45` (`Multi-Agent`) | Product worktree root | Complete portable suite content, including its workspace-aid documentation. |
| EverOS | `references/Harness-Memory-Base` | `5076683ab88d714390573d8f88ff3c470e51129a` (`main`), package `everos==1.3.1` | `harness/vendor/everos/` | Apache-2.0; upstream `LICENSE` and `NOTICE` retained. |
| MongoDB/LangChain integrations | `references/Harness-Memory-Planning` | `b37195d794ac0c2cac8f257fae433b852134a41a` (`main`), `langchain-mongodb==0.12.0` | `harness/vendor/langchain-mongodb/` | MIT; upstream license files retained. |
| Product specifications | `new_harness_memory_docs` | Revisions Concepts 1 / Feature 65 / Implementation 67 / Detailed 72 | `harness/docs/product/` | Product-owned specification snapshot. |

## Composition decisions

- The ROOT suite stays at the product workspace root so Codex and Claude can
  discover its instructions, hooks, and skills normally.
- The harness stays in `harness/`. Its setup contract requires the configured
  ROOT workspace to be outside the harness source directory; the parent product
  workspace satisfies that rule.
- EverOS and the MongoDB integration monorepo are vendored intact so their
  source, tests, package metadata, and licenses remain available while the thin
  product integration is built.
- EverOS remains the local experience/case/skill engine. MongoDB Atlas and
  Atlas Vector Search back the shared procedure path. Neither vendored project
  becomes the authority for current task state, plans, review, or acceptance.
- The copied product specifications are the development copy for this product
  tree. Changes should be intentional and reconciled with the outer source set,
  not edited independently by accident.

## Upstream refresh

At the user's request, applied the exact 41-file upstream delta from
`9cbd5b23e94f2e3d3488b68e53881025a2f13f6e` to
`d679c1f792bd46e78a0bfcefc0e4991e43dfb409` (explicit subagent launch configuration).
All affected product files matched the old source before applying the delta.
Product configuration, ROOT payloads/bindings, vendor trees, product specifications
and runtime state were not replaced. This is a source refresh, not setup or a
live-runtime upgrade: existing materialized payloads and records need STEP-01
ownership inspection and fresh setup/bootstrap before using the new contract.

## Intentional run-blocking repairs

On 2026-09-21, synchronized the user-authorized repairs through the canonical
builder identity above: explicit Ollama transport without a duplicate forwarded
model flag, rollback of attempt-created bootstrap branches/worktrees, and setup
manifest refresh that ignores its own persistent lease-lock artifact while still
blocking real `*.lease` records. These are narrow coordination repairs; they do
not add harness capabilities or change direct-Codex model selection.

The copied `harness-config.json` was changed from its stale
`Orchestrator-Harness-2` target to this active product workspace. Recompute the
absolute `root_workspace` value whenever this seed is moved or cloned. No
harness setup or runtime launch was performed during assembly.
