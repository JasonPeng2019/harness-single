# Memory- and Plan-Reuse Coding Harness
## Source, Rationale, and Implementation-Confirmation Ledger

**Revision:** 72  
**Date:** 2026-09-20  
**Supersedes:** `PRODUCT_SPEC_DETAILED_v71.md`  
**Status:** Non-normative. This ledger is neither an alternative implementation specification nor release evidence.  
**Authorities:** [Feature specification](PRODUCT_FEATURE_SPEC_v65.md); [Implementation contract and guide](PRODUCT_IMPLEMENTATION_SPEC_v67.md).

## 0. Reading this ledger

The Feature document owns behavior; the Implementation document owns technical interfaces, engineering discretion, and executable coverage. This ledger owns source observations, why a reference choice was considered, and what to confirm while coding. A source mismatch updates the adapter/ledger under the Implementation decision framework; it does not make a historical recipe authoritative.

Source observations labeled **rechecked** below refer to the preceding v70 consolidation; they are retained evidence, not a fresh repository audit. The v71 scope edit incorrectly let development-test role assignments constrain deployed APC, and the initial v72 text still predetermined the APC model during development verification. This correction applies the user's stricter clarification: the named worker/ROOT/reviewer assignments and nested harness setup govern development verification, but APC always resolves the required `apc_adaptation_binding`; no environment inherits a named APC model from the roster. Later benchmark performance uses the regular deployed product. Provider/source notes below are historical references, not a new availability check. No hosted service, provider task, harness test, benchmark or learner training was executed for this documentation correction.

## 1. Product rationale and preserved scope

The product keeps ROOT/harness execution and review, uses EverOS for experience/case/skill memory, and uses Atlas Vector Search materially for deliberately published procedures. Local durable evidence remains useful even without a remote telemetry replica. A narrow local template registry may be simpler than extending every EverOS kind/search/backend path.

The design separates exact present state from historical evidence and optional procedures. APC accelerates planning only when compatibility, current facts, comparable similarity, and ROOT acceptance support reuse. Empty retrieval and fresh planning are useful behavior, not defects to hide.

Three previously over-prescribed mechanisms remain optional: full first-class EverOS template integration, exclusive use of `langchain-mongodb`, and Atlas replication of every decision/outcome. None is required to maintain the shared Vector Search path. The current implementation scope is the complete fixed-strategy system, including a product-harness-launched lower-capability APC drafting subagent. The permanent behavior is that owned child launch plus explicit `apc_adaptation_binding`; no particular test or deployment model is built in. The learned selector is explicitly not implemented now; its future behavior remains in Feature Section 14.2. Benchmark execution is also deferred under Feature Section 16.1, including every named target below.

## 2. Prior consolidation and current scope changes

The preceding documents accumulated correct safeguards as repeated prose. Repetition eventually produced contradictions, including a test that treated approval as current-revision designation after the Feature document had separated those operations. The current documents keep the distinction once in the behavioral authority and test it by reference.

Other clarifications address implementation limits without weakening the intended protections:

- an internal persisted schema still needs compatibility handling, even before an external consumer exists;
- a local idempotency key does not prove a remote write is safe to replay;
- an off request cannot recall an already-accepted remote operation, so effective-off measurement uses a quiesced/reconciled or isolated boundary;
- provisional development training differs from validated frozen deployment in the retained **future** selector design; neither is implemented or tested in the current plan;
- immutable approved behavior is different from a rebuildable, versioned vector/index projection;
- current designation, content approval, recipient authorization, and revocation are distinct even if one storage object represents several;
- context finalization is not evidence that a worker was dispatched, and a digest is not proof of authorization;
- privacy applies to all product-managed remote content, not only records whose scope label happens to be `shared`.

These are explanations of edits, not a second requirements list. Exact mechanics and test homes are in the companion documents.

## 3. Pinned source provenance

The following hashes were recomputed during the prior v70 consolidation and are retained here without claiming a new hash/source audit. Archive comments are recorded as supplied metadata, not independent proof of a remote repository's present state.

| Mounted archive | SHA-256 | Archive comment |
|---|---|---|
| `harness-single-working-firmware-v2-candidate.zip` | `9c6090ace79f1eb25502976167d8ca8924d13edf7a7dabb001504d49ea7afff3` | `a5ba87314562c71e30232d0f43782313fb5c027b` |
| `Codex_Claude_Setup-Multi-Agent.zip` | `211ca9e03e15f31877e780e5f7459c23b508a1dc04ec3d55fec5793608cf7f05` | `2831860a94ddd3fee7cd736154c481b9b89265f0` |
| `EverOS-main(1).zip` | `674992d0b5085ac53817c49191289cad41b4d37c39d2f3e1166102f811375903` | `5076683ab88d714390573d8f88ff3c470e51129a` |
| `langchain-mongodb-main.zip` | `bb2c22a1e97dfef53838c2fda2d12e59de66a39c5f19d91f0432ba27267073b9` | `b37195d794ac0c2cac8f257fae433b852134a41a` |

The mounted EverOS filename differs from the historical `EverOS-main.zip` name, but the bytes have the previously recorded digest. Source paths below are relative to each archive's repository root; they do not imply that extracted repositories are bundled with these Markdown files.

## 4. Harness source observations

### 4.1 Prompt and execution baseline — rechecked

In `orchestrator_harness/bootstrap.py`, `_write_worker_prompt` starts from `task_card["task"]` (lines 134–141 in this snapshot). Worktree creation uses the resolved task-card base at lines 325–326, and the generated controller invocation has an explicit empty `env` mapping at line 195.

This supports a small common rendering/handoff extension and a test that packet identity matches the actual task/base. The empty mapping is **not** evidence that the eventual process inherits no environment variables: actual launcher/provider inheritance still needs source/runtime verification. Likewise, these lines do not establish a generic protected task-secret channel.

### 4.2 Existing lifecycle — retained observation, implementation confirmation required

Earlier inspection identified `review.py`, `controller.py`, setup/provider payloads, and resume paths as the existing owners of linked review/acceptance, result correction, staging, and cleanup. These responsibilities are retained. This editorial pass did not rerun native launch/review/retirement tests.

Useful implementation questions are: which task/run identifiers survive every correction/resume; how a lost launch response is reconciled without relaunching; where per-invocation native usage boundaries are archived; and which context changes can be added without altering clean-worktree/result checks.

### 4.3 Composition surfaces — retained observation

Earlier source inspection found ROOT-provider file overlaps and a harness-managed worker cache that can restore source payloads. The reference composer therefore reasons about both ROOT discovery files and the actual staged worker payload, not a per-lane patch after launch.

The next implementation task is to enumerate the pinned installer ownership and validate the completed composed output. Replacing several files is not automatically one atomic transaction; staged validation, recoverable commit state, and dispatch gating are a more honest description of the required behavior.

### 4.4 Development-only nested tests; product APC children

For development verification, the implementation harness builds the product and launches a delegated product-test ROOT. That test ROOT invokes the isolated candidate product harness, which owns its test workers, APC child, reviews, receipts and cleanup. The development-test matrix determines worker/ROOT/reviewer assignments; the test run's separate `apc_adaptation_binding` determines the APC child. Shared immutable dependencies are compatible with distinct mutable runtime ownership. Implementation Sections 1.1–1.2 and 15 own the test protocol; these are not instructions to nest harnesses in normal deployment.

In deployment—including later benchmark performance evaluation—the product runs as a regular ROOT-led harness using its configured models/providers/efforts. No implementation harness, delegated product-test ROOT or outer-test receipt is required. Its APC drafting child is still launched and owned by the product harness under Implementation Section 6.1; that genuine product behavior is separate from the development-testing wrapper around the candidate.

Implementation confirmation checks the actual launcher/delegation, test-scoped control access, isolated runtime ownership, child receipts and cleanup. Test orchestration remains separate from normal product startup/configuration. Inability to run the inner candidate is not an outer-harness PASS, and an unverified deployment profile is not made verified merely by allowing it in configuration.

## 5. ROOT skill-suite source observations

### 5.1 Admission, levels, and review — rechecked

`.agents/skills/project-topology/SKILL.md` states that the skill is planning-only, for explicit significant workflow/plan requests. Its admission section describes Level 0 as out of scope, starts admitted planning at Level 1, and rejects project size/file count alone as an escalation reason.

The same file chooses execution tier and document organization separately. Its source describes Levels 1–2 as normally using two independent review assignments, with risk-based splitting, and Levels 3–4 retaining four domain reviewers. These are source-workflow rules when that skill is actually admitted, not a new requirement that routine APC invoke that panel.

The formal framework remains a separately selected workflow. A compiler validates structure; it does not establish semantic approval. This supports the Feature route boundary without reintroducing a binary compact/formal planner for all ordinary work.

### 5.2 Catalog and configuration — retained observation

The supplied suite uses `.agents/skills/` as canonical and `.claude/skills/` as a generated mirror. Prior inspection of the sync scripts found whole-mirror replacement semantics, while the harness also installs command skills in the Claude-visible directory. Prior notes identify `.codex/config.toml`, `.codex/hooks.json`, and `.claude/settings.json` as overlapping ROOT configuration paths.

Those historical counts/paths are useful preflight targets, not a permanent exhaustive allowlist. The standalone suite validator describes its own export; a product composition may intentionally include additional harness behavior. No unverified Qwen or other provider port is implied by copying the directory.

## 6. EverOS source observations

### 6.1 Public wire and ownership — rechecked

`src/everos/entrypoints/api/routes/memorize.py` defines normal `/memory/add` and `/memory/flush` DTOs. Messages carry sender identity, positive Unix-epoch millisecond timestamps, and content. App/project/sender values have path-safety restrictions. Deferred extraction is explicit; the flush response distinguishes extraction from no extraction.

`src/everos/memory/search/dto.py` exposes scoped agent-memory search with case/skill result arrays in the supplied snapshot. This is not a general arbitrary-memory-kind extension point. Application/project/owner scope and the upstream filter DSL should be adapted deliberately rather than receiving arbitrary product compatibility predicates.

Implementation confirmation: construct one source-conformant reviewed trajectory and verify the resulting case owner/session/receipt and every scoped read. A namespace strategy that writes one identity/root and reads another yields plausible empty memory; separate per-trial roots are also a valid isolation option.

### 6.2 Exact provenance and enumeration — rechecked in relevant source

`src/everos/memory/strategies/extract_agent_skill.py` hydrates source-case lineage and uses existing owner/entry repository lookup. A narrow exact resolver is a sensible boundary if public routes do not expose the needed identity operation; a semantic or session-paging reconstruction is not necessary when exact lookup is available.

`src/everos/memory/get/dto.py` caps page size at 100 and documents agent-skill ordering by `updated_at`; `get/manager.py` sets that ordering explicitly. Enumeration code therefore cannot assume a caller-selected sort. Under concurrent generation, pagination completeness/stability must also be verified rather than inferred from receiving one page or one `total_count` value.

### 6.3 Deferred add/flush — partial source evidence, not crash proof

`memory/extract/ingest/service.py` constructs message IDs through `gen_message_id`. In `memory/extract/ingest/id_gen.py`, the function derives identity from session, millisecond timestamp, and message index, with a padded index. This is useful evidence for exact-payload replay, but it does not prove end-to-end exactly-once extraction or flush recovery.

The relevant test must include lost responses, durable buffering, batch ordering, concurrent retries, buffer cleanup, and downstream extraction effects. If an ambiguous commit cannot be reconciled, the product can preserve the trajectory and hold the write without blocking ordinary task execution; it should not pretend the ID formula makes a remote API transactional.

### 6.4 Template extension and backends — retained observation

Earlier inspection found agent-skill behavior across get/search, writer/reader, cascade kind registration, recall/result shaping, strategy/agentic paths, and backend routing. A full template-kind extension follows those paths, including every backend the product advertises. A partial extension may fail only when a different routed backend is selected.

A small local immutable template registry can avoid that cross-cutting work. In either design, APC uses its declared representation; copying the existing skill's small name/description anchor is merely one projection choice, not proof it is appropriate. A dedicated template-selection descriptor remains a useful reference, not a binding format.

### 6.5 Model usage — wrapper rechecked, coverage still to prove

`src/everos/component/llm/_usage_client.py` wraps a configured client, reads response usage, and calls the tracing usage helper. It does not by itself demonstrate complete durable request/task accounting for every product call or background job. Confirm when spans record, what native receipts survive, and how missing/unattributed maintenance is reported.

EverOS's agentic-search/extraction clients and embedding providers remain separate from the product's APC adaptation path: the latter now uses a product-harness CLI subagent, not this LLM wrapper. Agentic search and extraction dependencies, effective embedding dimension, and service-side generation-disable controls remain implementation-confirmation items. Filtering generated output after execution does not disable the source model calls that produced it.

## 7. Atlas package source observations

### 7.1 Hybrid scores and filters — rechecked

In `libs/langchain-mongodb/langchain_mongodb/retrievers/hybrid_search.py`, vector and text branches receive reciprocal-rank processing; `pre_filter` is passed into both helpers, and optional `rerank_path` selects an additional rerank path. Discovery/fusion scores are therefore not automatically the reference raw cosine.

Earlier pipeline inspection places text filtering after `$search`; actual native/vector/text filter semantics deserve fixture tests. A post-fusion filter may be too late to prevent candidate starvation. These are adapter concerns, not reasons to freeze one retrieval algorithm.

### 7.2 Exact reads — rechecked

`vectorstores.py::get_by_ids` in this snapshot extracts the literal `text` field and deletes the literal `embedding` field. It also normalizes input IDs. That helper is a poor fit for a product using another text key or requiring a stored vector for exact APC rescoring.

Direct collection reads can supply exact product metadata/vector/payloads while the package still supplies actual search. A direct Atlas adapter is also allowed. A future source version with a suitable exact-read helper should be used on its actual merits rather than preserving a historical workaround.

### 7.3 Constructor index behavior — rechecked

The hybrid retriever's constructor defaults full-text auto-creation on. In the inspected vector-store constructor, `auto_create_index=False` returns before index creation; other combinations can trigger it. The old prescription to always unset dimensions is not a necessary additional rule when explicit disable already controls the actual code path.

The implementation checks the pinned constructor and owns index creation/repair deliberately. It does not rely on “this is only a search object” to guarantee no infrastructure mutation.

### 7.4 Optional remote services and privacy

The optional Atlas reranker remains off in the initial reference configuration; adopting another model/service is a deliberate measured configuration decision with native-usage coverage and authorized egress, not a side effect of an incidental constructor default.

Project-scoped remote publication is still remote egress. Query, embedding/adaptation, and telemetry payloads can leak the same secrets as a procedure body; the Feature privacy boundary is broader than the `shared` scope label. Authentication transport and arbitrary content payloads are not interchangeable exceptions.

## 8. Reference implementation choices

A compact first implementation can keep EverOS separate, maintain local transactional evidence and a narrow template registry, use a small tested Atlas adapter, and add the smallest ROOT-side prepare/finalize/observe bridge into the existing harness. This is a starting hypothesis, not a new prescribed stack.

A source/reuse ledger may be named `REUSE_MANIFEST.md`; an integration package may be named `memory_plan_integration`. Historical examples used `memory-context/v2` and several objective/decision schema versions. Those names do not define the next implementation's wire schema. `apc_adaptation_binding` is the cross-document configuration concept; its concrete serialized shape belongs to the implemented configuration schema. Once a concrete schema has durable data or consumers, its actual compatibility rules matter even if no outside client exists.

Numeric starting values live only in Implementation Section 16; the user-selected **development-test** worker/ROOT/reviewer role/model/CLI matrix lives only in Implementation Section 1.1. No matrix is duplicated here. APC uses the separately supplied `apc_adaptation_binding` in development tests as well as deployment; it never inherits the lightweight row. Deployed roles use separate supported configuration, not that table by inheritance. The current task is fixed-strategy implementation and synthetic functional product-harness verification, not learner development or benchmark performance evaluation.

### 8.1 Development-test model resolution and APC configuration

The named model families/efforts are the user's development-test selections for worker/ROOT/reviewer roles, not deployment defaults or capability claims. Resolve those names against the installed CLI for the test wave and retain the actual binding evidence; Sonnet's unspecified effort remains an explicitly recorded supported choice. Independently resolve and record the run's required `apc_adaptation_binding`; no named model in the development matrix is its default. The prior edit recorded provider-documentation references below. They are lookup starting points, not proof of current account access, supported IDs, or a native launch in this correction.

Deployment and future benchmark profiles independently select supported ROOT/worker/reviewer bindings and provide their own `apc_adaptation_binding`. All APC runs retain normal validation and receipt/usage traceability and never silently substitute a named development-test model.

- M1 — [OpenAI Codex model selection documentation](https://developers.openai.com/codex/models) (official redirect to ChatGPT Learn): model selection and family identifiers; inspected 2026-09-20.
- M2 — [Claude Code model configuration](https://code.claude.com/docs/en/model-config): explicit model names, effort and effective/fallback configuration; inspected 2026-09-20.

CLI syntax is version-sensitive and not frozen by this reference. Neither these sources nor the role names constitute a comparative benchmark, and no model invocation was made to validate availability in this editing task.

## 9. Implementation-confirmation questions

These questions locate work in the source; they do not add policy beyond the companion contracts.

| Area | Question to answer with source/test evidence |
|---|---|
| Harness handoff | Where do task/base/plan binding, actual dispatch identity, and per-invocation usage survive bootstrap, native correction, resume, and retirement? |
| Development-test runtime | Does the delegated heavyweight product-test ROOT launch the recorded candidate with separate mutable ownership and recoverable cleanup, rather than exercise the outer coding harness? Is that test orchestration absent from required deployment startup? |
| APC child | Does near-match revision launch the product-owned drafting child with parent/template linkage, no recursion and end-to-end bounds, using the exact run-supplied `apc_adaptation_binding` in development and deployment with no named or inherited default? |
| Agent bindings | Which exact CLI IDs/efforts realize the Section 1.1 worker/ROOT/reviewer development-test roles and the separate APC binding, what actually launched, and is the deployment profile independently configurable without hard-coded test models? |
| Phase guard | Can the current plan reject learned-mode activation and keep all benchmark workloads outside the test driver without implementing deferred machinery? |
| Workspace | Which actual source/staged files overlap, and how does completed composition preserve both behavior families after setup is rerun? |
| EverOS scope | Which upstream fields/roots enforce this trial's owner/project boundary consistently across add/flush/get/search? |
| Provenance | Do claimed source-case IDs resolve exactly and retain the reviewed receipt/content identity needed for approval? |
| Writes | What source primitive makes each retry safe, and how are ambiguous effectful commits held/reconciled? |
| Feature control | Can generation/write controls stop product-owned work before effective-off measurement, or is isolated deployment needed? |
| Templates | Is a narrow local registry simpler than a first-class extension; which advertised paths have real tests? |
| Representation | What actual canonical query/projection, effective dimensions, metric, and sanitizer version make APC scores comparable? |
| Atlas | Which exact-read/prefilter/current-designation mechanism works with this package and index; can stale writes bypass eligibility? |
| Usage | Which material calls have native evidence; what remains incomplete rather than free? |
| Snapshots | Are receipts/approvals/tombstones/source dependencies recoverable in the restored identity boundary, and are pre-existing jobs controlled? |
| Network | Which tools and destinations are actually exposed in the launched runtime, independent of desired config? |

## 10. Future deployment benchmark targets — no execution in this plan

The retained future target stack is SWE-Marathon v1.1 or a deliberately selected successor for long-horizon coding; FrontierSWE as a possible full-product comparison; MemoryArena for memory-guided action; LongMemEval-V2 and cleaned LongMemEval for retrieval/regression; and BEAM for scale stress. These names are historical planning context, not current tasks or an authorization to download/execute their test cases.

Feature Sections 1.1 and 16.1 explicitly defer **all** benchmarks, including public/private suites, subsets/smokes, leaderboard runs and scored B0–B3 comparisons. No benchmark-specific integration, execution or post-closeout job belongs in the current release plan. The future protocol remains in Feature Section 16.3; the learned-selector arm additionally depends on future implementation under Section 14.2.

Current development evidence comes from real subagents exercising the candidate product harness on purpose-built synthetic/disposable fixtures and source/service tests. Native timing/usage and all-off checks establish functional behavior and accounting integrity, not benchmark performance. Later authorized benchmarking is use of the deployed product, with its own runner and model/provider/effort configuration. It does not inherit the development-test model matrix or require the nested outer implementation harness. At that later time, the evaluation plan verifies the external runner/rules and actual bindings; this correction makes no new claims about them.

## 11. Risks and practical responses

| Risk | Practical response to evaluate |
|---|---|
| Reuse recipe is more complex than necessary | Prefer the smaller source-compatible mechanism and retain property coverage. |
| Recipe changes reopen compatibility | Keep one implemented schema owner and migrations for real data, rather than copied prose schemas. |
| Mutable eligibility is confused with immutable content | Separate content manifest from authorization/current/tombstone state and versioned search projection. |
| Lost responses or concurrent updates | Persist intent/evidence; reconcile before unsafe replay; use conditional/transactional local mutation. |
| Cost hidden by fallback or missing receipts | Track expected calls and requested/effective behavior; retain incomplete coverage and whole-objective spend. |
| Background work contaminates ablations | Establish effective controls/quiescence or isolate the next trial instead of claiming instantaneous remote cancellation. |
| “Fail closed” breaks ordinary work unnecessarily | Drop optional guidance when safe; stop only the affected mandatory/security-dependent action. |
| Scope expands through trusted-source assumptions | Explicit recipient/issuer policy and exact approved content; no whole-tree auto-trust. |

## 12. Coverage and next implementation step

Implementation Section 15 is the single required coverage matrix, and Section 18 defines release evidence. This ledger does not carry another independent checklist or test count. Source-adapter and integration tests can share fixtures where that preserves their real oracles.

The useful next action is an executable vertical slice tested by a delegated heavyweight product-test ROOT using the isolated candidate product harness: exact state/handoff, reviewed local evidence, a scoped Atlas hit, a real APC child selected through `apc_adaptation_binding` plus direct-fill/fresh fallback, linked outcome, and durable reconciliation. Complete the remaining fixed-strategy capabilities and native role/cleanup tests afterward. Stop before learned-selector implementation and all benchmarks. Refinement follows concrete failed properties and actual provider/source contracts, not an attempt to predict every future failure in prose.

## 13. Historical review limitations

Prior review documents contain proposed findings, accepted edits, rejected candidates, and claims of clean passes. They are historical work records. Their existence or count does not prove an independent review occurred, that every fix was applied correctly, or that the final system works.

The preceding v70 consolidation and v71 edit contain historical source/documentation observations. This v72 correction checked the current companion text against the user's explicit development-testing versus deployment clarification; it did not re-audit repositories or provider documentation. It is document-level self-review, not a separately instantiated or context-erased reviewer and not a claim that runtime defects are absent.

## 14. Updating this ledger

Change source observations when implementation confirms or disproves them; retain the pinned source identity and actual evidence. A renamed helper or a better transaction primitive normally updates the implementation/source ledger, not product scope. A real behavior/trust/retention/evaluation tradeoff follows Implementation Section 0.

## 15. Evidence limits

Archive hashes and identified snippets remain historical evidence. This correction read the three current companions and the Concepts scope clarification, removed the remaining predetermined APC test model, introduced the consistent `apc_adaptation_binding` concept, and checked the resulting text. No product code, model, service, native harness test or benchmark was run. Development release evidence still follows the prescribed synthetic test protocol; every environment supplies its APC binding, deployment uses independently configured roles and a regular product harness, and benchmark execution and the learned selector remain later work.
