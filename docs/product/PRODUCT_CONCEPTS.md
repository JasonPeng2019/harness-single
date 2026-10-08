# Memory- and Plan-Reuse Coding Harness
## Product Concepts — Human-Readable Guiding Star

**Revision:** 1 — Concepts edition  
**Date:** 2026-09-20  
**Status:** Human-readable guiding overview; not a second normative specification.  
**Scope:** What the product is for, how its features fit together, and how they should behave. The original concepts, structure, and plain-language style are retained; detailed implementation contracts live in the companion documents.

**Companions:** [Feature specification v65](PRODUCT_FEATURE_SPEC_v65.md), [Implementation contract v67](PRODUCT_IMPLEMENTATION_SPEC_v67.md), and [Source/rationale ledger v72](PRODUCT_SPEC_DETAILED_v72.md).

Read this first for the product's intent. “Must” and “should” below summarize the governing behavior rather than add a competing set of requirements. The companions own exact contracts, testing obligations, and engineering decisions, subject to the user's latest scope clarification: **the named agent assignments and nested-harness testing setup apply to building and testing the product, but they do not select the APC model.** Every environment supplies the APC child's lower-capability model/provider/CLI/effort through the required `apc_adaptation_binding` configuration. The harness has no built-in APC model and never silently substitutes one. APC near-match revision still uses a product-harness-launched subagent.

**Build now:** the fixed-strategy memory, procedure, and APC product. **Later:** the learned decision selector and all benchmark execution. Their concepts remain here so the direction is clear, not to authorize work on them now.

---

## 1. Purpose

The product is a long-horizon software-engineering system built on the existing ROOT-led multi-agent harness. Its purpose is to help coding agents finish difficult, long-running tasks more reliably and with less repeated work by giving them durable memory, reusable procedural knowledge, reusable planning structure, and bounded adaptive search.

The product is not primarily a research prototype. Its long-term success is measured by whether it improves real software-engineering task completion, continuity, cost, latency, and memory usefulness. This implementation phase proves behavior through functional and native product-harness tests; controlled benchmark measurement happens later.

The core product promise is:

> Preserve what the system has learned, retrieve the right past experience when it matters, reuse planning structure when it is safe, and fall back to normal strong planning whenever reuse is uncertain.

The product must never trade away verified correctness, required review, or execution safety merely to obtain a memory or cache hit.

---

## 2. External product requirements

The product is intended to satisfy the MongoDB agentic-memory hackathon brief. The product therefore must visibly and materially use:

1. **MongoDB Atlas** as part of the real behavior path, not only as a logging sink.
2. **Atlas Vector Search** to help retrieve semantically relevant shared procedures or plan templates.
3. **Agentic memory** that persists across sessions and changes later agent behavior.
4. **Long-running agent behavior** where useful state and experience survive context changes.
5. **Adaptive behavior** where prior experience can influence future search, planning, or procedural guidance.

The demo must make these behaviors observable. A reviewer must be able to see what was remembered, what was retrieved, what was reused, what was rejected, and what happened afterward.

---

## 3. Benchmark goals — future evaluation

### 3.1 Primary coding goal

These are future targets, not current tasks or release gates. Do not run SWE-Marathon, FrontierSWE, or any other public/private benchmark, benchmark subset, scored comparison, or leaderboard evaluation in this implementation plan.

The north-star coding benchmark is **SWE-Marathon v1.1** or its current successor. The product should be optimized for verified completion of realistic long-running software-engineering tasks. **FrontierSWE** is the secondary long-horizon coding benchmark and becomes the primary full-product fallback when SWE-Marathon's execution environment prevents live Atlas-backed memory access.

The long-term ambition is leaderboard-level performance. The product must be designed around the factors that matter for long-horizon coding:

- preserving exact unfinished work;
- avoiding repeated failed investigations;
- recovering correctly after context/session changes;
- reusing useful procedures without forcing bad matches;
- reducing unnecessary repeated planning;
- spending memory/search budget only when useful;
- finishing integration and verification, not merely making partial progress.

### 3.2 Primary memory goals

In the later evaluation phase, the product should also perform strongly on agentic-memory benchmarks, especially:

1. **MemoryArena** — whether remembered experience improves later action across sessions.
2. **LongMemEval-V2** — whether long agent trajectories can be searched for compact, useful evidence with good answer quality and latency.
3. **LongMemEval cleaned** — regression testing for extraction, multi-session reasoning, updates, temporal reasoning, and abstention.
4. **BEAM** — scale stress when histories become too large for full-context delivery.

### 3.3 Benchmark truthfulness

The product must not treat the following as equivalent to task success:

- a cache hit;
- a highly ranked memory;
- a generated skill;
- a valid plan draft;
- an agent claiming success;
- an accepted database write.

Task success is determined by the normal verifier/review/acceptance path for the task.

When benchmark evaluation is separately authorized, the product must keep development, validation, and held-out evidence separate. Independent trials must not silently inherit prior trial answers or outcomes. Comparisons hold the task, verifier, repository baseline, model/provider/effort, budgets, and unrelated tool/network conditions materially constant; any mismatch is reported rather than credited to memory or APC.

### 3.4 Verification in the current plan

Use purpose-built synthetic/disposable tasks, source and contract tests, real scoped service tests, fault injection, and native subagents launched by the product harness. These establish correct behavior and accounting, not a benchmark score or a performance advantage. Benchmark runner work and benchmark execution happen later.

---

## 4. Existing foundation that remains authoritative

The existing harness and ROOT operating suite remain the product's authority for:

- current task ownership;
- active plan and checkpoint state;
- worker creation and execution;
- repository/worktree identity;
- resource ownership;
- worker escalation;
- verification decisions;
- review;
- acceptance or rejection;
- resume/replacement decisions;
- process cleanup and retirement.

Memory and plan reuse are advisory accelerators. They do not replace these responsibilities.

The product must continue to work when all new memory/reuse features are disabled.

EverOS remains the local experience engine. Atlas provides the published procedure library and real Vector Search participation; local decision/outcome evidence remains durable even when optional remote telemetry is unavailable. A local template registry or a suitable EverOS extension may provide template storage. These choices do not create a second planner or replace ROOT authority.

### 4.1 Building the harness versus using it

The **implementation harness** is the system used to build and verify the product. The **product harness** is the candidate system being built and tested, and later deployed for real work.

The named implementation agents in Section 14.4 and the nested testing arrangement in Section 22.8 belong to the build/test workflow only. A deployed product does not require an outer implementation harness, those particular models, or that testing hierarchy. Its configured ROOT and workers still obey the same authority, review, and safety boundaries.

---

## 5. Product information model

The product must keep five kinds of information conceptually separate.

### 5.1 Exact current state

Exact current state describes what is true **now** for the active task. Examples include:

- the user objective;
- mandatory constraints;
- current repository revision;
- active plan/checkpoint;
- completed work;
- unresolved work;
- current failure state;
- valid or invalidated verification credit;
- current lane/session identity.

Exact current state must never depend on semantic search. It must be retrieved by exact task/project identity.

### 5.2 Cases

A **case** is historical experience from previous agent work.

A case answers questions such as:

- What happened on a similar task?
- Which hypothesis was tried?
- What failed?
- What worked?
- What evidence supported that conclusion?

Cases are untrusted historical evidence and context. A case is not a mandatory instruction, even when its text sounds like one.

### 5.3 Skills

A **skill** is reusable procedural guidance distilled from experience or deliberately authored by the project.

A skill answers:

- When this procedure is useful.
- What sequence or method tends to work.
- What warnings or constraints matter.
- How success should be checked.

Skills are optional guidance. They cannot weaken mandatory task requirements, review gates, or verification.

### 5.4 Plan templates

A **plan template** is reusable planning structure.

A plan template contains:

- a stable reusable skeleton;
- the situations where it is useful;
- situations where it should not be used;
- declared task-specific fields that must be filled for the current task;
- fixed planning steps or invariants that should remain stable;
- limited areas where small adaptation is permitted;
- expected validation or completion checks.

A plan template is not a prior task's completed plan copied verbatim. It is a reusable shape with explicit task-specific blanks.

### 5.5 Outcomes

An **outcome** records what happened after one bounded memory/search/reuse decision and its actual execution.

An outcome links:

- the task or bounded objective;
- the search strategy used;
- the cases/skills/templates selected;
- the initial planning/reuse choice and the final accepted plan that actually executed;
- the verified result;
- failure class when applicable;
- cost and latency when available.

A decision has at most one canonical terminal outcome. Review/acceptance evidence must belong to that same task/execution; valid evidence from another run is not valid here. Missing or invalid terminal evidence leaves the decision unobserved rather than creating a synthetic result.

Outcomes exist primarily for evaluation, debugging, and possible future learning. They are not ordinary prompt memory and should not normally be injected into worker context. Usage evidence is retained separately so late accounting does not rewrite the verified outcome. No learned-policy update occurs in this plan.

---

## 6. Persistent experience memory

### 6.1 Capture

The product must retain useful agent experience after reviewed work, including both success and failure.

It should preserve at least:

- the task goal;
- the relevant environment/repository context;
- meaningful actions or approaches;
- verified findings;
- disproved hypotheses;
- important failures;
- the final reviewed result;
- references to supporting evidence.

The product must not turn a failed attempt into a successful lesson merely because the worker described it confidently.

### 6.2 Recent findings

Recently verified findings must remain available immediately to the active task even if background memory extraction or indexing has not finished yet. Task closure alone does not discard reviewed evidence that has not reached long-term memory.

The active task must never be forced to rediscover a fact solely because the long-term memory pipeline is still processing it.

### 6.3 Original evidence

Distilled memory must not be the only retained representation.

Where permitted, the product preserves durable references to the original evidence that supports a case, including relevant test output, decisions, task identity, or reviewed result. A missing or unavailable reference is stated honestly; it does not become verified provenance.

An agent or ROOT must be able to inspect the original evidence when a summary is ambiguous or potentially stale.

---

## 7. Automatic case and skill creation

### 7.1 Case generation

Substantive completed or failed trajectories may automatically become cases.

Thin, meaningless, or purely administrative runs should not be promoted into reusable cases.

### 7.2 Skill generation

Related cases may automatically produce a proposed reusable skill.

Generated skills must preserve provenance to the cases from which they were derived.

### 7.3 Generated-skill trust levels

Generated skills have these human-facing trust states. They describe eligibility, not a required database state machine:

1. **Proposed** — created by memory processing; visible for review and analysis but not delivered as procedural guidance.
2. **Approved** — trusted approval binds the exact content and permitted recipients. Delivery still requires current eligibility and compatibility.
3. **Revocation pending** — blocked at the source while withdrawal from managed remote copies is being reconciled.
4. **Revoked** — retained for history but cannot be delivered to future tasks.

Generated skills must not approve themselves. Complete provenance is necessary but not sufficient: every claimed source case must resolve exactly to reviewed evidence, and trusted ROOT/operator approval must authorize the exact revision before procedural delivery or publication. Model confidence is not that approval.

Curated project skills may begin approved under an explicit trusted procedure-selection policy. Simply being in the ROOT skill suite does not convert governing instructions into optional memory. Approving a generated skill does not change its generated origin.

Approval, choosing the current revision, and authorizing sharing are separate decisions. Changed procedural meaning requires a new approved revision; an old approval never silently covers edited content. A generated helper script is not installed or executed merely because its skill is approved.

### 7.4 Skill use

When an approved skill is retrieved, the product may provide it as bounded procedural guidance.

The product should normally deliver no more than a small number of highly relevant, non-conflicting skills. It must not flood the context with every remotely related instruction. A skill is delivered coherently or omitted, not trimmed in ways that lose prerequisites or verification.

---

## 8. Plan-template memory

### 8.1 Template concept

Plan templates use the same general memory/search experience as cases and skills, but they have a different job. The local template capability remains usable without live Atlas; it need not force a full EverOS extension when a narrow versioned registry is simpler.

A template is a reusable skeleton such as:

- regression diagnosis and repair;
- public API/interface change;
- dependency upgrade;
- data/schema migration;
- integration-failure investigation.

The template contains named fields representing facts that vary from task to task, such as the affected component, expected behavior, validation command, target interface, or migration target.

### 8.2 Template invariants

The product must distinguish:

- **fixed structure**, which reuse is not allowed to silently rewrite;
- **fillable fields**, which are deliberately task-specific;
- **allowed adaptation areas**, where a small structural change is permitted;
- **forbidden carry-over**, such as old task identities, test credit, approvals, or repository-specific transient state.

### 8.3 Template creation

The first product version uses deliberately authored/reviewed plan templates.

Automatic plan-template mining is not required for the initial product.

A future version may learn or extract templates automatically, but any automatically created template would still need provenance, compatibility rules, and approval before trusted reuse.

### 8.4 Template versioning

An approved template revision is immutable, whether used locally or published.

Changing the reusable structure or declared fields creates a new revision.

A bad revision is revoked rather than silently rewritten in place. Rebuilding a search index or changing an embedding representation does not itself change the approved procedural meaning; the rebuilt representation must still identify the correct revision.

### 8.5 Similarity and the no-template result

The highest-ranked template is not automatically a safe match. Reuse requires current eligibility, structured compatibility, resolvable fields, and sufficiently strong comparable similarity. Consider the bounded shortlist rather than stopping at an incompatible first hit.

Similarity uses one compatible, versioned query/candidate representation and metric; search-ranking scores are not automatically reuse scores. Thresholds are conservative recorded configuration, not universal constants. Poor, incompatible, or incomparable matches produce an explicit no-template result and normal planning.

---

## 9. Shared procedure library

### 9.1 Purpose

The product must support a shared library of approved skills and plan templates backed by MongoDB Atlas.

The shared library exists so useful procedures can be found across sessions, workers, and deliberately permitted projects.

### 9.2 Publication

Only eligible procedure revisions may enter the shared library.

Publication must preserve:

- procedure identity;
- immutable revision;
- procedure type;
- applicability and scope;
- provenance;
- integrity identity;
- approval, current-revision, and revocation evidence.

Publishing a procedure does not make it universally applicable. Approval must authorize the actual recipients and scope; publishing more broadly needs a trusted authorization decision. Receiving deployments must be able to verify that approval, not merely read an “approved” flag or a publisher-local path.

An explicit current designation selects the deliverable revision within a trust partition. Approval alone does not replace narrower overrides. Shared changes are complete only when acknowledged by the shared store; retries cannot restore an older designation. Superseded history remains available for audit, and revocation never silently reactivates it.

### 9.3 Scope

The product must support at least these sharing scopes:

1. **Private/local** — only the owning logical agent within the permitted application/project/namespace; not remotely published in this version.
2. **Project** — usable by authorized agents inside that specific project boundary.
3. **Shared** — deliberately published for authorized cross-project use, still respecting application and namespace isolation.

A procedure outside the current allowed scope must not be delivered even if it is highly similar.

### 9.4 Atlas role

Atlas must materially participate in finding shared skills and plan templates in product/demo mode.

Semantic similarity through Vector Search must be part of the retrieval path. Lexical/keyword matching may complement semantic search.

Atlas is not the authority for current task state, review, acceptance, or live worker status. Its mandatory role is shared procedural retrieval, not replication of every local outcome or transcript.

---

## 10. Unified memory search

### 10.1 One product search experience

The rest of the product should see one conceptual search system even though results may come from local memory and the shared Atlas library.

Search may return three candidate types:

- cases;
- skills;
- plan templates.

Each result must retain its type and provenance. The same logical procedure found locally and in Atlas is still one candidate; replication alone must not improve its relevance.

### 10.2 Search is advisory

Search results do not automatically become context or plans.

After retrieval, the product must:

1. enforce allowed scope;
2. reject revoked or ineligible procedures;
3. reject incompatible procedures;
4. remove duplicates;
5. keep candidate counts bounded;
6. preserve provenance;
7. treat cases, skills, and templates according to their distinct roles.

Keep useful capacity for both skills and templates so one result kind cannot crowd the other out. Enabled participating stores receive a bounded opportunity within the shared search budget; a slow local store must not silently prevent every Atlas attempt.

### 10.3 Semantic and lexical retrieval

Search can combine:

- semantic similarity, for conceptually similar work described differently;
- lexical/keyword matching, for exact error strings, symbols, components, commands, or technology names.

Atlas Vector Search is part of the live product path. Lexical matching and hybrid fusion may complement it when useful; one exact retrieval recipe or client library is not required.

### 10.4 Mandatory information is not searched

Current task requirements, active checkpoints, current repository identity, and required verification state are never optional search results.

They are always supplied independently of memory search.

---

## 11. Search strategies

The product provides three fixed search behaviors.

### 11.1 Standard

Standard is the default.

It searches using the current bounded objective and obvious task context. It performs one bounded retrieval pass and returns the best eligible memory/procedure candidates.

Standard should be inexpensive and predictable.

### 11.2 Problem-focused

Problem-focused is available when there is concrete failure information.

It emphasizes:

- the actual error or failing behavior;
- affected component;
- environment/tool/runtime identity;
- recently disproved hypotheses;
- the immediate recovery objective.

If meaningful problem context does not exist, the product must fall back to Standard rather than manufacture a fake failure query.

### 11.3 Deeper

Deeper is used when the task appears to need more investigation before implementation.

It may inspect first-pass results, decide whether evidence is sufficient, and issue bounded follow-up searches for missing aspects.

Deeper must stop when:

- sufficient useful evidence is found;
- its follow-up limit is reached;
- its time/cost budget is exhausted;
- or required search services become unavailable.

Deeper must not search indefinitely simply because more memory exists. Shared-library participation is bounded and conditional on availability; a healthy local adaptive path can still operate in restricted-local mode.

### 11.4 Common rule

All three strategies operate under explicit budgets and must leave enough task budget for actual implementation and verification. The bound covers the complete optional stage, not just its database call. Before search or adaptation, preserve a configured execution reserve; if optional work cannot fit, skip it rather than spend the remaining implementation time.

One bounded objective preparation has at most one primary strategy/search decision. Follow-up queries within Deeper remain part of that decision. Tool calls, notifications, and result corrections do not independently restart the search budget or create a new strategy choice.

---

## 12. Context construction

### 12.1 Context layers

When memory is used, the context supplied to ROOT or a worker must clearly separate:

1. **Current task and mandatory requirements** — authoritative.
2. **Current plan/checkpoint and unresolved work** — authoritative.
3. **Relevant cases** — historical evidence.
4. **Selected skills** — optional procedural guidance.
5. **Current task-specific plan** — the ROOT-accepted plan, whether preserved from exact current state, freshly planned, or produced from a template.
6. **Required output/review contract** — authoritative.

Historical content must be labeled as untrusted historical evidence rather than current fact or instructions. Ordinary execution workers receive an accepted plan, never an unreviewed reusable draft presented as authority.

### 12.2 Bounded delivery

The product must limit optional memory context. Exact current task state and the accepted plan are mandatory; they are not charged to the optional-memory allowance.

Within the optional allowance, priority is:

1. evidence directly relevant to an unresolved issue;
2. selected complete skills;
3. extra historical examples.

Mandatory state must never be truncated merely to fit optional memory. The complete input also accounts for runtime instructions, available session history, and output headroom. If mandatory content cannot fit, use ROOT's normal compaction/recovery path or fail preparation explicitly.

The finalized context belongs to the exact task/objective, repository baseline, decision, and accepted plan. A changed or mismatched packet is rejected before dispatch. A finalized packet is not proof that a worker launched; uncertain launch results are reconciled before launching again.

### 12.3 Worker-specific context

Workers should receive only information relevant to their assigned objective.

ROOT may receive broader project-level memory and alternative candidates because ROOT owns planning and review.

---

## 13. Agentic Plan Caching behavior

### 13.1 Purpose

Plan reuse exists to avoid paying for full planning again when a proven planning structure already fits the new task.

The system must prefer correctness over reuse rate.

### 13.2 Reuse branches

Plan reuse has three branches. Record both the initial attempt and the final accepted plan source so a rejected reuse followed by fresh planning is not counted as a clean cache adoption.

#### Direct fill

Use when the reusable structure fits the current task and all required custom fields can be filled without changing the structure.

The system fills the declared fields and creates a new task-specific plan draft.

#### Light adapt

Use when the template is clearly relevant but needs a small allowed structural adjustment.

The product harness launches a configured lower-capability **drafting subagent** to revise the near-match plan. The child returns a proposed task-specific draft; a direct model/API call or ROOT rewriting it in its own context is not this adaptation path.

The child receives ROOT-authorized drafting instructions, the selected template, trusted current facts, and allowed edits. It does not execute the parent's implementation, accept the parent plan, launch more agents, or recursively invoke APC. Launch, corrections, model work, result validation, and cleanup use the harness's bounded lifecycle; the child's usage is charged to the parent adaptation.

In development tests, deployment, and later evaluation, this child resolves the required `apc_adaptation_binding`. That configuration selects its supported model/provider/CLI/effort and designates it as lower-capability for the run; the harness does not infer a model ranking or inherit a named model from another role. A missing or invalid binding disables light adaptation for that decision and sends the work to fresh ROOT planning. ROOT accepts or rejects the resulting plan.

#### Fresh plan

Use when:

- no suitable template exists;
- a required field cannot be resolved safely;
- the task requires architectural judgment outside the template;
- the template's structure is incompatible;
- the configured adaptation subagent cannot be launched, or adaptation fails, exceeds its allowed edits, or times out;
- the result is malformed;
- or normal plan review rejects the result.

Fresh planning is a normal successful fallback, not a cache failure that must be hidden.

### 13.3 Field resolution order

Task-specific template fields should be filled using this priority:

1. exact current task/ROOT state;
2. already retrieved source-backed evidence that ROOT can establish applies to the current task;
3. bounded subagent adaptation of permitted plan structure from those facts;
4. otherwise stop reuse and plan normally.

The plan-reuse layer must not launch an open-ended repository investigation or invent missing current facts, policy, credentials, or authorization merely to preserve a cache hit.

### 13.4 Review

A reused/adapted ordinary plan must pass the same applicable validation and ROOT acceptance/review as a freshly created ordinary plan. A drafting child's successful closeout does not mean the parent's plan has been accepted.

The separate `project-topology` workflow remains admission-gated to significant explicit planning requests. APC does not invoke or replace it. A Level 0 verdict means ordinary execution; any stale topology-routed optional preparation is discarded before ordinary dispatch, with bounded re-preparation or normal planning as appropriate.

A syntactically valid plan is not automatically an approved plan.

### 13.5 Forbidden carry-over

Plan reuse must never carry forward old:

- completion claims;
- test results or verification credit;
- approvals;
- commits;
- task IDs;
- lane/run/session IDs;
- resource leases;
- worker assignments;
- destructive authorization;
- claims that were true only for the previous repository state.

---

## 14. ROOT and worker behavior

### 14.1 Before a bounded objective begins

The product should:

1. load exact current state and the intended planning route;
2. choose a fixed/configured eligible search strategy and search when budget permits;
3. filter candidates and expose relevant cases and approved skills to ROOT;
4. preserve a verified accepted plan for this same objective, or continue review of an existing draft;
5. only when a new plan is needed, attempt ordinary plan-template reuse or fresh planning;
6. apply normal ROOT acceptance and any independently required review;
7. finalize bounded context around the accepted plan and dispatch through the product harness.

A stale or wrong-objective current plan is a recovery problem, not permission to silently replace it with a memory hit. An accepted current plan does not depend on repeatedly fetching its historical source template; known unsafe provenance is referred to ROOT.

### 14.2 During execution

The product should not rerun full memory search after every tool call.

ROOT may create a new bounded objective and linked search decision at a meaningful boundary such as:

- new module/subtask;
- concrete failure or changed hypothesis;
- ROOT-directed recovery;
- context/session replacement;
- substantial environment/repository change.

### 14.3 After work completes or fails

After the normal review/acceptance chain is verified, the product should:

1. preserve the reviewed result and supporting evidence;
2. fix the one canonical terminal outcome for that decision;
3. retain recent evidence and queue enabled experience ingestion;
4. allow enabled background case/skill extraction;
5. publish only authorized, eligible procedure revisions;
6. retain usage and provenance for later analysis, without running a learned selector or policy update.

Optional bookkeeping is reconciled safely after restart and does not reverse a valid task acceptance. Uncertain effectful operations are not blindly replayed when the underlying service cannot prove replay safe.

### 14.4 Agent assignments for implementation and verification only

These are the chosen agents for **building and testing this product**, including the native product-harness test runs. They are not a fixed roster for deployed use.

| Role | Agent and CLI | Work in this implementation/test plan |
|---|---|---|
| **Lightweight / low capability** | **GPT 5.6 Luna — High, Codex CLI** | Routine tests and straightforward bounded implementation/fixture work. |
| **Middleweight** | **GPT 5.6 Terra — High, Codex CLI**, or **Claude Sonnet 5 — Claude Code CLI** | Substantive coding and relatively more important implementation/integration work. |
| **Heavyweight** | **GPT 5.6 Sol — Medium, Codex CLI** | Implementation ROOT, delegated product-test ROOT, consequential/complex work, and important step-closeout review. |
| **APC drafting child** | Required `apc_adaptation_binding`; **no named default** | Near-match plan revision through the product harness. The run configuration supplies a supported lower-capability model/provider/CLI/effort. |

Use Terra as the default middleweight choice; Sonnet is an explicit alternative. Record a supported Sonnet effort/default rather than inventing one. Resolve and record the requested CLI/model/effort before tests; an unavailable requested binding is not silently replaced and called a passing test.

**ROOT and worker choices remain environment-specific, and APC always uses the explicit `apc_adaptation_binding`.** The development roster does not provide an implicit APC default. The product's authority and review boundaries remain the same. The learned selector discussed later is about search strategy, not choosing agent models.

---

## 15. Continuity across long tasks

### 15.1 Checkpoint continuity

The existing checkpoint/handoff remains the concise source of current unfinished work.

Memory supplements the checkpoint with historical evidence, such as why an earlier approach failed.

### 15.2 Session/context changes

When context changes, the product must reconstruct:

- the exact current objective;
- verified completed work;
- unresolved requirements;
- disproved hypotheses and supporting evidence;
- applicable constraints;
- current plan/checkpoint;
- the first useful next action.

### 15.3 Native resume versus memory recovery

If the original provider session exists, the existing native resume behavior may continue it.

If the provider session does not exist, memory may help start a new session with correct lineage, but the product must not pretend that this is a native resume.

### 15.4 Stale verification

If code, environment, or relevant inputs changed, old verification credit must be invalidated according to the existing ROOT rules even if memory says that a check previously passed.

---

## 16. Procedure compatibility and eligibility

Before a skill or plan template is delivered, the product must verify:

- it is in an allowed scope;
- its revision is currently eligible;
- it is not revoked, revocation-pending, withdrawn, or superseded for this delivery;
- its stated applicability matches the current task sufficiently;
- any required environment/tool/language constraints are satisfied;
- required capabilities and dependencies are available from trusted current facts;
- known conflicts are absent;
- the stored content matches its approved integrity identity;
- its approval and current designation authorize this task's actual recipients and scope.

If any required compatibility check fails, the procedure is rejected for that decision.

A rejected candidate may remain in the library for other tasks. Unknown required facts fail closed rather than being treated as no restriction.

For newly selected live shared guidance, verify authoritative eligibility again at the handoff boundary. If that cannot be established within budget, omit the shared guidance or return an affected reused plan to ROOT. A cached copy cannot pretend to be freshly approved local content. Explicitly frozen-snapshot tests use their declared frozen state, not a claim of live freshness.

Deliver at most one revision of each logical procedure and no declared conflict pair. By default, eligible private/local guidance takes precedence over project guidance, then shared guidance; a trusted explicit policy may choose differently without widening authorization or reviving revoked content.

---

## 17. Outcome labels and future learning

Outcome labels and accounting are implemented now. Their use as learning feedback is future work; no learner is built or updated in this plan.

### 17.1 Verified success

A future positive learning outcome requires substantive verified success through the normal review/acceptance path.

Worker self-report alone is insufficient.

A forced or exceptional acceptance is not automatically a positive learning label.

### 17.2 Failure

A verified task/subtask failure may become a negative outcome when the result is actually attributable to the attempted work or strategy.

### 17.3 Blocked or unknown

External outages, missing authorization, and genuine authoritative blocked/unknown outcomes are kept separate rather than forced into success/failure. Missing or mismatched review evidence leaves the decision unobserved; it is not an immutable unknown outcome.

### 17.4 Cost and latency

Cost and latency are recorded separately from correctness.

The product must not hide quality requirements inside a future reward that allows enough cost savings to compensate for incorrect work.

Use provider-native usage where exposed and show incomplete coverage rather than guessed zeroes. Count search, harness-launched APC drafting, ROOT planning/review, execution, retries, and abandoned preparation. Separate online work, background maintenance, and embeddings. During nested tests, distinguish outer implementation/test orchestration from inner product execution rather than double-counting either.

---

## 18. Optional contextual search selector — deferred design

**Do not implement this selector in the current plan**, even as a disabled feature. No training, exploration, learned inference, policy loader/store, or update machinery is built now. Fixed/configured search and ordinary outcome/usage evidence are implemented; requests to enable learned behavior report deferred/not implemented.

### 18.1 Purpose

In a later development phase, the optional learned selector would decide which fixed search strategy to use for the current bounded objective:

- Standard;
- Problem-focused;
- Deeper.

It is intended to learn when deeper memory investigation is worth the extra cost.

### 18.2 What it does not control

The future selector would not decide:

- whether mandatory checks apply;
- whether ROOT review is required;
- whether a worker may ignore constraints;
- free-form tool actions;
- individual rule wording;
- arbitrary query text;
- arbitrary procedure combinations.

### 18.3 Safe fallback

Current operation uses a fixed tested strategy, with Standard as the default and the same availability/budget gates for every choice. A future unavailable, invalid, incompatible, or insufficiently validated selector falls back to that fixed behavior.

### 18.4 Evaluation behavior

After a separate future implementation is authorized, development training may use a recorded provisional policy and bounded exploration. Validated deployment needs a policy compatible with its feature definitions, search recipes, and operating regime.

Future held-out scored evaluation uses a frozen validated policy or a fixed strategy. No held-out online updates or cross-trial leakage are permitted. Feedback follows the behavior actually executed, preserves correctness over cost, and applies each accepted observation once to a compatible policy history. These are future design constraints, not current implementation or benchmark tasks.

---

## 19. Failure and degradation behavior

### 19.1 No useful memory

Continue with exact current state and normal planning.

### 19.2 Local memory unavailable

Continue without optional historical memory. Preserve any pending ingestion for later retry.

### 19.3 Atlas unavailable

Continue using eligible local memory and normal planning. Do not deliver a stale shared cache as though live eligibility had been checked. A valid task may still complete; an accepted current plan remains ROOT-owned exact state.

### 19.4 Search index lag

Recent exact findings remain available. The task must not block waiting for background indexing.

### 19.5 Malformed memory/procedure

Reject it and continue with other eligible candidates or baseline planning.

### 19.6 Revoked procedure discovered after search

Recheck status before delivery. Do not use it.

### 19.7 Template mismatch or invalid adaptation

Fall back to fresh planning.

### 19.8 Search budget exhausted

Stop searching. Use already valid results if useful, otherwise continue with baseline planning.

### 19.9 Learned selector unavailable

It is deliberately unimplemented in this phase. Run fixed selection; reject explicit requests to enable learned behavior as deferred/not implemented. A future selector failure uses the fixed safe fallback.

### 19.10 Optional logging failure

A completed task's legitimate ROOT acceptance remains valid. Logging is retried separately.

### 19.11 Missing mandatory state

This is not an optional-memory failure. The product must stop and use the existing ROOT recovery path rather than guess the missing task truth.

---

## 20. Trust, security, and isolation behavior

1. Workers must not approve their own skills/templates or receive product-memory control credentials or authority to publish, revoke, or mutate policies. ROOT/operator owns those decisions.
2. Generated or externally sourced procedures remain untrusted until the approval, scope, integrity, and current-eligibility checks pass.
3. Shared search enforces project/agent/publication scope. A search hit never broadens its own authorized recipients.
4. Reusable memory, remote publications at any scope, and derived search/embedding/telemetry payloads must not leak known secrets or unrelated confidential material. Preserve protected source evidence and sanitize derived content without silently changing approved procedural meaning.
5. A retrieved procedure cannot grant permissions or weaken the harness's process, resource, Git, review, or acceptance rules.
6. Arbitrary generated scripts do not become executable merely because a skill contains them.
7. Procedure integrity and current eligibility are checked before use. A digest detects changes; it does not prove authorization by itself.
8. Revision revocation blocks future local delivery and withdraws all tracked managed publications; remote completion is not claimed before acknowledgement. Withdrawing one publication is different from revoking its content everywhere.
9. Previously delivered guidance remains in that session. ROOT decides correction, restart, or abort; neither revocation nor a feature switch can retroactively erase it.
10. Task-specific credentials use an actually available, validated protected runtime path. A protected channel is not permission to pass forbidden memory-control credentials to a worker. Unsafe mandatory content blocks dispatch rather than being silently rewritten.

These are operational authority boundaries, not a claim that same-user coding processes are a hardened sandbox. Deployment security and model selection remain configuration choices within the governing guarantees.

---

## 21. Feature switches and ablation behavior

The product must independently support enabling/disabling:

- experience ingestion;
- experience retrieval;
- generated-skill creation;
- generated-skill use;
- shared procedure publication;
- Atlas shared retrieval;
- plan-template memory;
- plan reuse;
- light adaptation;
- Deeper search.

Learned selection is a reserved future capability, not an implemented switchable learner in this phase. Explicit enable/load/update requests are rejected as deferred.

Disabling one enhancement must not silently disable unrelated required behavior. Generated-skill use off blocks both local and shared generated guidance without blocking curated procedures. Template memory off removes templates from the execution path; APC off prevents automatic plan conversion; adaptation off leaves direct fill and fresh planning available.

Light adaptation requires APC, and APC requires template memory. Skill creation requires experience-write permission. A switch cannot undo a remote operation already accepted: pause new work, reconcile or isolate outstanding effects, and record when the disabled state is effective. Explicit authorized safety revocation remains available outside the ordinary task path.

With all enhancements disabled, the ordinary task path behaves like the existing harness + ROOT suite, with no optional memory calls or APC context. Local evidence remains durable, and required implementation/test security controls still apply.

---

## 22. Evaluation modes and current product testing

The modes below describe future comparisons and useful functional configurations. Current tests may use baseline/memory/APC settings on synthetic fixtures to verify behavior, **not** to run benchmarks or claim comparative performance. Learned-search mode is future-only.

### 22.1 Baseline mode

Existing harness + ROOT suite only.

Used now for functional baseline checks and later as the real benchmark comparison baseline.

### 22.2 Memory mode

Adds persistent cases and eligible skills, including shared procedure retrieval when permitted.

### 22.3 Memory + plan reuse mode

Adds plan-template reuse to Memory mode.

### 22.4 Learned-search mode

In a separately authorized future phase, adds a validated frozen search-strategy selector to Memory + plan reuse mode. Neither that selector nor its benchmark comparison is implemented or run now.

### 22.5 Cold-start evaluation

No prior task-specific learned memory beyond declared curated seed procedures.

### 22.6 Warm evaluation/demo

Uses a declared frozen memory/procedure snapshot. The snapshot must be named and reproducible.

### 22.7 Network modes and future benchmarks

The live product uses Atlas for its own memory/procedure data. That does not authorize general internet research. Distinguish three reported modes:

- **`soft_guardrail_network`** — Atlas is enabled; provider-native general web/search/fetch tools are suppressed where supported. Any unsuppressed tools or shell egress are reported; this is not hard isolation.
- **`atlas_memory_only`** — Atlas is enabled, general web/search/fetch tools are absent or suppressed, and independently enforced egress controls block unrelated outbound access. Tool disabling alone is insufficient.
- **`restricted_local`** — no live Atlas task path; local memory/procedures or an explicitly declared frozen snapshot only.

Product-memory credentials stay ROOT/integration-side. Runtime restrictions cannot be bypassed for fallback or administrative convenience. In later benchmarks, verify the actual permitted network configuration and keep modes separate in reporting. The targets in Section 3 remain future work; no benchmark runs are authorized now.

### 22.8 Native testing of the product harness — current implementation phase

Tests need actual subagents running through the **candidate product harness**, not merely successful activity in the harness used to write its code:

```text
Implementation harness (builds the product)
    |
    +-- launches heavyweight product-test ROOT
            |
            +-- sets up the isolated candidate PRODUCT HARNESS
                    |
                    +-- launches test workers, APC drafting children,
                        and closeout reviewers through that candidate
```

Use the implementation/test agent assignments in Section 14.4 and supply the candidate run's separate `apc_adaptation_binding`. Pin the candidate build and keep its mutable state, worktrees, queues, memory, evidence, and process ownership separate from the outer implementation harness. Retain actual product-side launch/result/review/cleanup evidence; test-ROOT narration or an outer-harness run is not equivalent.

Use purpose-built synthetic/disposable tasks, scoped live services, source/contract tests, and restart/failure fixtures. Important closeout review uses the implementation heavyweight assignment. Changes to product code return to the outer build workflow and start a newly identified test wave; cleanup affects only the owned test resources.

**This nested arrangement is a way to build and test the product, not its deployment architecture.** Once deployed, the product harness runs with its own configured ROOT/workers and need not be launched by another harness. Benchmark testing still happens later.

---

## 23. Observability and explainability

For every memory/reuse decision, the product should make it possible to answer:

- What exact task/objective was active?
- Which search strategy was used?
- What queries were issued?
- Which stores were searched?
- Which cases, skills, and templates were candidates?
- Which candidates were rejected and why?
- Which candidates were delivered?
- What initial plan-reuse branch was attempted?
- What accepted task-specific plan actually executed, including any fallback to fresh planning?
- What verified result followed?
- What did the memory/search/reuse step cost in time/tokens when known?
- Is the evidence complete and verified enough for later analysis or possible future learning?

This traceability supports current debugging, functional validation, and the demo, as well as later benchmark claims. During implementation tests it also identifies the candidate product build and separates outer orchestration from inner product execution.

---

## 24. Demo behavior

A successful product demo can use a purpose-built disposable task to show one end-to-end story. It is not a benchmark:

1. An initial coding task produces reviewed evidence.
2. The product preserves that experience.
3. The memory system produces or retrieves a useful case/skill.
4. An approved skill or plan template is visible in the shared Atlas-backed library.
5. A later related task begins in a new context/session.
6. The system retrieves the relevant shared procedure using semantic/lexical search.
7. A template is directly filled, a product-harness-launched drafting subagent lightly adapts it, or an approved skill supplies guidance.
8. ROOT accepts the current task-specific plan under the applicable review path.
9. The worker executes through the product harness.
10. The normal verifier/review path decides the result.
11. The product records the decision and outcome.
12. Ideally, a deliberately mismatched procedure is also shown being rejected in favor of normal planning.

---

## 25. Product acceptance — human-readable summary

This is a reading aid, not another release checklist. The companion specifications own the exact current requirements and executable verification. In human terms, the current product is ready when:

- Exact current task state is always available independently of semantic retrieval.
- Reviewed experience persists across sessions.
- Cases can be searched and delivered as historical evidence.
- Automatic case/skill generation can operate on suitable trajectories.
- Generated skills remain proposed until approved for procedural use.
- Curated and approved generated skills can be searched and delivered as bounded guidance.
- Plan templates are a distinct usable local memory capability with fixed structure and declared fillable fields, without requiring one storage design.
- Plan templates can be retrieved through the same overall memory-search experience.
- Shared approved skills/templates can be found through Atlas Vector Search in live product mode, with lexical/hybrid support when selected.
- Shared scope, eligibility, revocation, and compatibility are enforced before delivery.
- Standard, Problem-focused, and Deeper search strategies all work and obey bounded search effort.
- Plan reuse supports direct fill, bounded product-harness subagent adaptation, and clean fresh-planning fallback.
- Reused plans receive the same applicable ROOT acceptance/review as fresh ordinary plans; an already accepted same-objective plan is preserved.
- Old task IDs, approvals, test credit, and runtime state never become current state through reuse.
- Workers receive bounded, role-appropriate context with clear separation of current truth, evidence, guidance, and plan.
- Long-task continuity preserves unresolved work and disproved hypotheses through context/session change.
- Outcomes connect decisions to verified results, failure classes, cost, and latency.
- Fixed/configured strategy selection works without learned-policy code, storage, or training; the learner remains future design only.
- Optional memory/search service failure cannot redefine success or corrupt harness state.
- All enhancements can be disabled independently.
- Baseline harness behavior remains intact with enhancements disabled.
- Synthetic fixture tests can isolate baseline, memory, and APC behavior without running a benchmark; learned-search tests wait for its future implementation.
- Snapshot, restore, provenance, and isolated fixture state are functionally tested now; future benchmark protocols retain explicit trial boundaries.
- Network/tool controls are tested and described honestly, without claiming hard isolation from tool disabling alone.
- Restricted-network results and full Atlas-backed results are labeled separately.
- Native validation uses a delegated test ROOT and inner product-harness workers under the implementation/test assignments, not the outer coding harness as a substitute.
- The synthetic-task demo visibly uses Atlas, Vector Search, persistent memory, and adaptive reuse on the real behavior path. No benchmarks are executed in this phase.

---

## 26. Explicit non-goals

This implementation plan **does not implement the learned decision/search selector or run any benchmarks**, including SWE-Marathon, FrontierSWE, other suites, subsets, and scored comparisons. Their future designs remain documented.

The initial product also does not require:

- replacing ROOT or the harness with another agent framework;
- automatic plan-template mining;
- a general-purpose learned rule generator;
- learned ranking of every individual memory;
- a learned stop/continue decision after every retrieval step;
- a graph database;
- migrating all local memory internals into Atlas;
- reimplementing vector search;
- a separate general-purpose APC database/service; a narrow local template registry remains permitted;
- cached model answers as a substitute for plan reuse;
- inference-prefix caching as part of APC;
- copying the full RRCv3 system;
- arbitrary generated scripts becoming executable skills;
- a dashboard as a prerequisite for correctness;
- claiming benchmark superiority without separately authorized reproducible evidence;
- fixing deployed ROOT/worker/adaptation models to the implementation roster or requiring the nested test topology in deployment.

---

## 27. External benchmark constraints — future phase

Live Atlas availability and benchmark rules are controlled by each future evaluation environment rather than by the product. When benchmark work is separately authorized:

- use `restricted_local` when live Atlas is forbidden or unavailable under the rules;
- use `atlas_memory_only` only when both provider-tool suppression and independently enforced unrelated-egress isolation are established;
- otherwise describe permitted live-Atlas operation as `soft_guardrail_network` and report its limits;
- use FrontierSWE or another permitted long-horizon benchmark later when the primary benchmark cannot exercise the full product, without bypassing restrictions or pooling unlike configurations.

Verify external rules and availability at that later time. This document preserves the direction; it does not authorize benchmark setup, runner integration, or execution in the current plan.

---

## 28. Feature relationship summary

```text
Exact current task state
        |
        +------------------------------+
        |                              |
Historical experience             Current objective
        |                              |
   Cases generated                     |
        |                              |
   Skills derived                      |
        |                              |
        +----> Unified bounded search <-+---- Shared Atlas procedures
                        |
         +--------------+---------------+
         |              |               |
       cases          skills       plan templates
      evidence       guidance     reusable structure
         |              |               |
         +--------------+-------+-------+
                                |
                       Context + APC decision
                                |
                 current ROOT-accepted task-specific plan
                                |
                         ROOT + harness execution
                                |
                       verification + acceptance
                                |
                             outcome
                                |
                    future memory
                    (learned selector: later development)
```

The product works when these features reinforce each other without confusing their authority: current task state remains exact, memory remains evidence, procedures remain optional, plans remain reviewable, and success remains verified.
