# Memory- and Plan-Reuse Coding Harness
## Product Feature Specification

**Revision:** 65  
**Date:** 2026-09-20  
**Status:** Normative current-phase product behavior. Sections 14.2 and 16.3 retain future-phase designs, not current implementation or test obligations. Section 18 is a navigation aid.  
**Supersedes:** `PRODUCT_FEATURE_SPEC_v64.md`  
**Companions:** [Implementation contract and guide](PRODUCT_IMPLEMENTATION_SPEC_v67.md); [source and rationale ledger](PRODUCT_SPEC_DETAILED_v72.md).

## 0. Authority and interpretation

This document owns observable behavior, trust boundaries, scope, fallbacks, and evaluation rules. The Implementation document owns shared technical contracts, engineering change authority, and required verification. The Detailed document is non-normative source evidence and rationale. Earlier revisions and review reports are historical, not additional authorities.

“Must” identifies a requirement. A default applies unless a permitted, explicitly recorded alternative is selected. A reference implementation is a starting point, not an additional requirement. Names used to explain concepts do not freeze internal field names, storage formats, classes, provider syntax, or algorithms. Actual implemented interfaces and persisted records nevertheless require coherent compatibility handling under Implementation Section 2.

A requirement has one canonical home. Cross-references and tests exercise that requirement; they do not create a competing policy. The current-phase scope in Section 1.1 controls timing: future selector and benchmark provisions do not authorize implementing or executing those deferred activities now. An incompatible test oracle is corrected rather than used to weaken the owning requirement. Behavior-preserving engineering choices follow Implementation Section 0 without user approval; genuinely consequential changes follow its escalation rule.

## 1. Purpose and success criterion

The product adds persistent experience, optional procedural guidance, reusable planning, and bounded search to the existing ROOT-led coding harness. Its purpose is to reduce repeated investigation and planning while preserving verified task completion, continuity, and execution safety.

A retrieval hit, high similarity, generated skill, valid draft, or successful database write is not task success. Product improvement requires measured completion or resource benefits under the comparison rules in Section 16; implementation conformance alone establishes neither.

The product/demo path must visibly connect persistent experience or procedures, Atlas Vector Search discovery, a reuse or rejection decision, normal ROOT/harness execution, and a verified outcome. Restricted-local behavior is a supported fallback, not evidence that the live Atlas path was demonstrated.

### 1.1 Current implementation scope

Implement the complete fixed-strategy memory/procedure/APC product described here: persistent reviewed experience and case/skill generation, local and Atlas procedure retrieval, Standard/Problem-focused/Deeper recipes, templates, harness-launched light adaptation, trust/publication/revocation, context handoff, durable outcomes/usage, switches, and snapshot/isolation support. Mechanism choices already left optional remain optional; core conformance is not satisfied by omitting a required capability or testing only a stub.

**Do not implement the learned decision/search-strategy selector in this plan**, even as an optional or development-mode feature. Its intended behavior is retained in Section 14.2 for future development. Current selection is fixed/configured with deterministic eligibility and fallback; a capability boundary rejects attempts to enable learned selection or policy updates. No learner algorithm, training/exploration loop, policy loader/store, learned inference, or update/replay machinery is built now. Keep ordinary decision/outcome/usage evidence without adding learner-specific infrastructure solely for future use.

**Do not run benchmarks in this development/verification phase.** SWE-Marathon, FrontierSWE, MemoryArena, LongMemEval, BEAM, and any other public or private benchmark, benchmark subset/smoke, leaderboard run, or scored performance comparison are deferred. Use purpose-built synthetic/disposable fixtures, source tests, service/contract tests, fault injection, and native subagent product-harness tests under Section 16.1. Functional baseline/enhanced fixture runs may verify behavior; they are not benchmark cohorts or evidence of a performance advantage. Future benchmark protocol remains in Section 16.3; benchmark-specific runner work and benchmark execution are not release gates now.

**Development verification is not deployment.** The named model/CLI/effort assignments and the nested two-harness protocol apply only to the development-test workflow used to establish that the product works, including implementation and fixture work. They are not product deployment defaults, supported-model restrictions, or a required deployment topology. APC is the exception to the named development roster: development tests, ordinary deployment, and later evaluation each must supply a supported lower-capability model/provider/CLI/effort through `apc_adaptation_binding`. The harness has no named APC default and does not inherit one from the lightweight worker role. In ordinary deployment the product operates as a regular ROOT-led harness without an outer implementation harness or delegated product-test ROOT. Later benchmark performance evaluation is use of that deployed product, not the development-testing protocol. The permanent APC behavior is still a bounded lower-capability drafting subagent launched by the product harness (Section 6.3).

## 2. Existing authority and baseline

ROOT and the existing harness remain authoritative for the user objective, exact current plan/checkpoint, repository/worktree identity, verification, review, acceptance, worker lifecycle, resume/replacement, resource ownership, and cleanup. Optional memory cannot redefine those responsibilities or grant additional authority.

Preserve an accepted same-objective plan unless ROOT explicitly invalidates it or starts a replan. Preserve verified work and unresolved obligations through context replacement; rerun checks when ROOT's existing invalidation rules require it. Memory recovery does not count as native provider-session resume when that session is unavailable.

Optional integration failure may remove an enhancement, but must not remove mandatory task/plan state or overturn a valid harness acceptance. Missing or inconsistent mandatory state takes the existing ROOT recovery path, not semantic guessing.

The all-enhancements-off task path retains inherited behavior as specified in Section 15. A pristine baseline host smoke is a current functional test; a measured B0 benchmark comparison belongs to the later evaluation phase in Section 16.3. Installing the combined system preserves both harness and ROOT-suite behavior under Implementation Section 10.

Development testing distinguishes the **implementation harness** that builds the product from the **product harness** being built and exercised. The implementation harness launches a delegated heavyweight **product-test ROOT**, which sets up the isolated candidate product harness and launches test workers through it. Successful activity in the outer coding harness is not evidence that the product harness works. Implementation Sections 1.1–1.2 own these development-test assignments and boundaries; Implementation Section 15 owns their verification protocol. This outer/inner arrangement is test infrastructure only, not part of normal deployed operation.

## 3. Information and execution units

| Concept | Meaning and authority |
|---|---|
| Exact current state | Current objective, constraints, repository identity, plan/checkpoint, completed/remaining work, failure context, and verification state. Loaded by exact identity from ROOT/harness, never by semantic search. |
| Case | Historical experience, including failures and disproved hypotheses. Evidence rather than instructions or current truth. |
| Skill | An eligible, approved procedure supplied as optional guidance. Approval does not override task constraints or permissions. |
| Plan template | Reusable structure, typed task-specific fields, applicability, verification/stop intent, and bounded adaptation rules. Not a completed prior plan copied into a new task. |
| Bounded objective | A ROOT-defined execution unit with explicit scope, budget when known, and an identifiable review/acceptance chain. A tool call or notification is not automatically a new objective. |
| Decision | One logical preparation/selection for an objective. Restarting a process does not create a new decision. An explicit replacement records lineage. |
| Outcome | The immutable local record of valid terminal evidence for that execution. There is at most one canonical terminal outcome per decision; absence of valid evidence is not an outcome. |
| Usage evidence | Separately retained observations of resource consumption. It can arrive later without changing the outcome. |

One task may contain several bounded objectives. Objective, task, decision, run, and native provider-session identities are not interchangeable. The implementation retains the exact joins described in Implementation Section 2. Outcome validation is owned by Section 11, selection frequency by Section 9, and accounting by Section 17.

## 4. Persistent experience and original evidence

When experience capture is enabled, retain useful reviewed successes and failures: objective, relevant environment/repository state, meaningful actions, verified findings, disproved hypotheses, review/acceptance, and supporting evidence references. Do not turn a confident worker narrative into a verified lesson.

Recent reviewed findings must remain recoverable from exact local evidence while extraction/indexing is pending. Task closure alone does not discard unindexed evidence. Once long-term searchable representation and its receipt linkage are confirmed, a redundant recent-evidence payload may be compacted while retaining provenance.

Cases retain their historical outcome and environment. An unlinked case may be displayed as explicitly unverified historical evidence; it cannot supply verified provenance or automatically establish a current factual template binding. Generated-case scores do not override a ROOT-reviewed result.

Retain original evidence or stable protected references where permitted. An unavailable reference is marked unavailable with a reason, not fabricated or silently treated as verified. Local source/evidence retention follows the privacy rule in Section 13. In V1, there is no automatic deletion of approved/revoked procedure history, verified outcomes, or evidence needed for declared reproducibility. Explicit operator retention/cleanup is permitted after required evidence is preserved and any loss is recorded.

## 5. Case generation and procedure approval

Suitable reviewed trajectories may generate cases and proposed skills through EverOS when the corresponding capabilities are enabled. Thin administrative activity need not become reusable experience. Automatic template mining is outside V1.

A generated skill begins proposed. Complete provenance is necessary, but never sufficient, for automatic procedural delivery. Approval requires all of the following:

- a nonempty set of source-case identities, each resolved exactly under its source/owner boundary;
- retained reviewed receipts and sufficient integrity evidence for every claimed source case, including failures when relevant;
- explicit trusted ROOT/operator approval of the exact procedure revision and authorized recipients/scope under Section 12.

Missing, ambiguous, unverified, or unverifiable source provenance blocks approval. Similarity, list order, model confidence, or a high extraction score is not an identity join. Approval does not change a generated skill's origin class. Useful material may instead be deliberately reauthored as a distinct curated procedure with new provenance; relabeling the same generated object is not reauthoring.

Curated/builtin auto-approval uses an explicit trusted selection policy. Mere presence in a ROOT/harness/repository skill tree does not authorize optional injection or publication of governing instructions. The same content-binding and scope rules apply to locally approved and remotely published procedures.

A generated skill containing or referencing a script does not automatically install, execute, or authorize that script. A later task may separately authorize a reviewed action through the normal harness authority.

## 6. Plan-template memory and Agentic Plan Caching

### 6.1 Initial library

V1 ships five reviewed ordinary-execution template families. Exact prose and asset layout are implementation choices; these purposes and fallback boundaries are retained.

| Family | Reusable intent | Stop reuse and fresh-plan when |
|---|---|---|
| Regression repair | Establish the concrete failure and component; investigate with a discriminating check; make a bounded repair; verify. | Failure/component cannot be established, or the work is architectural redesign. |
| Public interface change | Establish requested contract and consumers; change interface/implementation coherently; preserve required compatibility/migration; verify. | Essential compatibility policy is unknown, or coordinated redesign is required. |
| Dependency upgrade | Establish dependency and target; update minimum dependency/lock surfaces; repair concrete fallout; verify. | Target is unspecified, or the task is an architectural migration. |
| Schema/data migration | Establish current/required data states; preserve required compatibility and recovery; change related surfaces; verify before/after/failure cases. | Destructive authorization, data semantics, or required rollback behavior is unknown. |
| Integration-failure investigation | Reproduce/localize the boundary failure; discriminate between hypotheses; repair after localization; rerun integration checks. | Boundary cannot be localized, required external state is unavailable, or coordinated redesign is needed. |

Templates must work through the chosen local capability in restricted-local mode. Full first-class EverOS template integration is an option, not a prerequisite; a narrow local versioned registry is also conforming. Advertised template backends must actually support the chosen path.

### 6.2 Reuse gate and field trust

Search rank alone does not authorize reuse. A candidate must pass current eligibility and structured compatibility under Section 12, have a comparable calibrated dense similarity, and have safely resolvable required fields. The bounded shortlist is considered before selecting a candidate; an incompatible top-ranked hit must not prevent consideration of lower-ranked eligible hits.

For one selection, candidate scores compared under one threshold use the same canonical query semantics and compatible representation/fingerprint/metric. Any secret sanitization occurs before this gating representation is produced for all stores. Discovery queries may differ by adapter; unrelated discovery/fusion scores must not be substituted for calibrated reuse similarity. Exact interoperability is defined in Implementation Section 6.

Thresholds are conservative, versioned configuration, not universal constants. A poor/incomparable score or an unestablished hard requirement produces no reusable template. Development tuning is allowed; the scored configuration is frozen and recorded.

Required current facts come from exact trusted state or structured evidence whose applicability to the current task ROOT can establish. Historical fact values are not current facts merely because their source was reviewed. An adaptation model may propose permitted plan edits; it cannot invent credentials, destructive authorization, current repository facts, verification credit, or missing critical policy. Unresolved required current facts cause fresh planning. Future investigation steps may remain future work, but cannot masquerade as already-resolved preconditions.

### 6.3 Branches and review

**Direct fill** changes only declared task-specific bindings while preserving fixed structure. **Light adapt** performs a small bounded adaptation stage within the template's allowed edits. **Fresh plan** is the normal fallback for no match, unresolved facts, unavailable adaptation, exhausted optional budget, invalid output, or review rejection.

Light adaptation—including revision of a near-match plan template—is performed by an actual **bounded lower-capability drafting subagent launched through the product harness**. In every environment, `apc_adaptation_binding` chooses its supported model/provider/CLI/effort. The harness does not contain a named default, infer which installed model is “lower,” inherit the ordinary lightweight-worker binding, or silently substitute another binding. It is not a direct SDK/API call, an EverOS adaptation-client call, a provider-internal delegation without a product-harness run receipt, or ROOT doing the same revision in its own context. Direct fill remains deterministic and does not need an adaptation subagent.

ROOT authorizes a bounded drafting support task containing the exact template revision, current permitted bindings/evidence, allowed edits, and result contract. The child produces a proposed plan artifact, not an accepted parent plan. It cannot approve/publish procedures, execute the parent's implementation, or recursively invoke APC/search preparation or launch its own planning children. The drafting child is not promoted to ROOT merely because it drafts a plan; ROOT retains acceptance and applicable review under the run's configured role assignments. The precise child handoff/lifecycle contract is Implementation Section 6.1.

Each adaptation stage has declared total time, output, and child-attempt limits, plus provider token limits where enforceable. The time bound includes launch/queue delay, CLI/model work, native corrections, result collection, and validation; a retry never resets it. A missing configured adaptation-child binding, launch failure, invalid output, or exhausted stage returns to fresh ROOT planning, without silently switching model or using a direct-call shortcut. Cancel/retire only the owned child through the product harness; unresolved cleanup remains visible and blocks conflicting resource reuse. Do not wait indefinitely for optional adaptation or start open-ended repository investigation merely to preserve a cache hit.

A draft is never execution authority. Reused and fresh ordinary plans receive the same applicable ROOT acceptance/review: routine acceptance where appropriate and independent review when the existing workflow requires it. ROOT may revise a draft and accept the resulting exact revision. A rejected reused draft falls back to normal planning without hiding the attempted reuse. Initial and terminal provenance are recorded under Section 17.

### 6.4 Current-plan precedence and forbidden carry-over

A digest-verified, execution-accepted plan for the same objective remains authoritative. A candidate/reviewed plan continues its existing review unless ROOT explicitly replans. A missing, stale, unreadable, wrong-objective, or wrong-route nonempty plan is mandatory-state inconsistency, not “no plan.”

Reuse never transfers old completion claims, verification credit, approvals, runtime/worker/lease identities, destructive authorization, or repository-state claims. This prohibition does not discard valid current state recovered by exact ROOT identity; that is continuity, not historical plan reuse.

## 7. ROOT planning-suite boundary

`project-topology` is a separate admission-gated planning workflow, not the universal coding planner or APC reviewer. It applies to explicit significant workflow/plan requests. Its execution level, document organization, and formal-framework choice are separate concepts. The formal route is selected only by its documented explicit framework/project-workflow trigger, not by task size, file count, APC failure, or a desire for more review.

V1 APC targets ordinary execution planning. Memory cases/skills may inform admitted topology work, but templates do not replace its grammar, review panel, or compiler. Structural compiler success is not semantic approval.

Level 0 means the topology skill was out of scope; the effective route is ordinary execution. Resolve admission before optional preparation normally. A late Level 0 invalidates the topology-routed preparation for ordinary dispatch. ROOT may boundedly re-prepare under the corrected route or discard that optional preparation and continue ordinary planning. It never relabels stale filtered results as ordinary APC results or silently clears an inconsistent current plan.

The replaced preparation remains non-dispatchable, non-learning, and accounted for. A no-memory continuation still has exact execution/outcome lineage. Reclassification has a finite retry limit; exhaustion uses ROOT recovery, not an endless search loop. The default implementation choice is in Implementation Section 16.

## 8. Unified search behavior

Provide one typed search experience over enabled local cases, eligible skills, local templates, and published Atlas procedures. Every result retains source and provenance; history, guidance, and reusable structure remain distinct.

Apply scope, approval, current-revision, compatibility, integrity, and freshness rules from Section 12. Cases follow Section 4. Reject invalid candidates individually when possible; do not infer that one malformed result invalidates another store's valid result.

Bound candidate discovery and delivered context. Give skills and templates independent candidate capacity so one kind does not crowd the other out before applicability/reuse evaluation. A logical procedure replicated locally and remotely receives no relevance bonus solely from storage duplication; retain both provenances without counting it twice. The algorithm is replaceable if this property holds.

When a strategy includes multiple available stores, give each a bounded opportunity to be attempted within its outer deadline. A slow store cannot silently consume the whole budget before another participating store starts. No store is guaranteed to respond. Record unavailable, disabled, unattempted-by-budget, timed-out, and successful participation distinctly; return valid completed results or the exact-state baseline.

Semantic retrieval is required on the Atlas-enabled shared path under Section 12. Lexical or hybrid retrieval is useful but not an additional immutable architecture requirement. No eligible result is a legitimate `no_optional_memory` outcome; do not weaken gates to avoid it.

## 9. Search frequency and resource budgets

| Strategy | Required behavior |
|---|---|
| Standard | One inexpensive bounded retrieval based on the objective and known context; no separate model call solely to invent missing task classifications. |
| Problem-focused | One bounded retrieval using real failure/recovery context. Without it, request Standard subject to its own gates. |
| Deeper | Bounded adaptive local investigation using existing tested search machinery where practical, with bounded Atlas participation when enabled/available. Healthy local-only Deeper is allowed in restricted-local mode; an unavailable local adaptive path masks Deeper. |

One bounded objective preparation has at most one primary strategy selection and corresponding search decision. Internal queries/rounds of that recipe are not separate decisions. Tool calls, notifications, or native result-correction attempts do not silently reset the search budget. A genuine new ROOT execution/recovery boundary or a bounded explicit supersession may create a new decision with lineage.

Every optional stage has finite time and applicable call/round/token/candidate limits. With known remaining time, admit a stage only when its maximum wall budget plus the configured positive execution reserve fits. Recompute remaining time before each stage from a trusted elapsed-time/deadline source; do not reuse stale remaining time after a restart or route correction. Do not reserve all hypothetical later stages up front.

A search-stage deadline covers its query construction, optional embeddings, retrieval, payload/freshness reads needed for selection, and scoring—not just the database query. A later optional final-delivery freshness check has its own bounded allowance and uses the same reserve rule. If time is unavailable, omit the dependent optional content rather than perform an unbounded check. Mandatory ROOT planning/review/execution remains separate.

Unknown remaining time permits only a configured cheap fixed pass; Deeper and harness-launched adaptation are masked until a usable bound exists. Current fixed/fallback selection uses these gates; the future learned selector must use them too. If nothing fits, perform no optional search. The reserve protects execution opportunity; it is not a promise that an arbitrary task can finish within it.

At a deadline, stop scheduling and stop waiting for optional results. Remote work that cannot be cancelled may still incur usage; record late evidence or mark coverage incomplete. Configuration values are owned by Implementation Section 16 and the resolved run manifest.

## 10. Context construction and dispatch

Finalize execution-worker context only around a ROOT-accepted plan. An APC drafting child is a separate bounded support task under Section 6.3: it receives ROOT-approved drafting instructions, not a finalized parent execution envelope or a claim that the parent's proposed plan is accepted. This narrow distinction does not waive child identity, credential, budget, result, or cleanup checks. Keep authoritative task/constraints, current plan/checkpoint and remaining work, historical evidence, approved optional guidance, and the output/review contract semantically separate. A case containing instruction-like text remains labeled evidence; the renderer must not promote it into the procedural-guidance section.

Mandatory task/accepted-plan state is not charged to the optional-memory allowance and is not truncated to make room for memory. The complete rendered input must fit the known usable context budget; use ROOT's established compaction/recovery path or fail preparation if mandatory content cannot fit. Estimates used for packing are not native-usage evidence.

A selected skill is delivered coherently or omitted, including approved references needed to preserve prerequisites, warnings, verification, and fallback. A separately approved compact representation may substitute; arbitrary truncation/summarization may not. Redaction that changes approved procedural meaning requires a new approved representation or omission, not reuse under the old approval.

The finalized handoff binds the exact task/objective, intended repository/worktree base, decision, accepted-plan identity/integrity, and actual rendered payload. Check these at dispatch, including retry and native resume. Resolve moving repository references to the intended concrete execution baseline; normal code changes during a run do not retroactively invalidate its original receipt. New preparation uses the new intended baseline.

Finalized means prepared for dispatch, not proof of delivery. Retain the harness's actual dispatch/task-card linkage. On an ambiguous launch response, reconcile existing execution by exact identity before launching again; unresolved ambiguity invokes ROOT recovery instead of risking a duplicate worker. Bootstrap and native resume implement equivalent validation semantics, without requiring one specific helper.

If optional content is rejected before dispatch, it may be omitted and the envelope rebuilt without a new semantic search, provided the accepted plan and mandatory state are preserved and the delivered-content trace is corrected. If rejection affects the accepted reused plan or mandatory binding, return to ROOT; never silently drop the plan with the optional packet. Freshness and revocation policy is Section 12.

## 11. Outcomes, durability, and recovery

### 11.1 Authoritative execution records

Persist enough state to distinguish preparation, finalized-but-not-yet-confirmed dispatch, confirmed execution, abandoned pre-dispatch preparation, and fixed local outcome. Exact names and persistence technology are not requirements.

An abandoned preparation cannot later dispatch, receive an execution outcome, or train the selector. Its successor or no-memory continuation retains the relationship. Retries reuse the same logical decision where the inputs and captured policy/configuration are unchanged; drift in mandatory input requires explicit recovery or new preparation, not a second decision hidden as a retry.

Before fixing an outcome, validate that review and acceptance evidence are internally linked to the same task/execution and, for enhanced dispatch, its actual decision/context. A valid PASS from another run is invalid here. Missing/unreadable/mismatched evidence leaves the decision unobserved or raises an integrity error. A genuine authoritative terminal unknown/blocked result is distinct from absent evidence. B0 evidence uses its actual harness execution identity, not an invented optional packet.

An identical outcome replay is idempotent; a contradictory terminal record under the same identity is an integrity conflict. Legitimate upstream corrections require explicit linked correction/supersession evidence and reconciliation of affected reporting; never silently overwrite history or learn from both versions.

### 11.2 Side effects and ambiguous external operations

After the local outcome/evidence is durable, enabled current-phase bookkeeping can reconcile independently: reviewed receipt/recent evidence, ingestion, and optional remote telemetry. Learned-policy updates are not a current side effect; the future design is Section 14.2. “Local outcome fixed” never means every side effect finished. Failure of those effects does not reverse harness acceptance. A bookkeeping failure is reported and retried or held for reconciliation, not hidden as success.

Retain stable operation identity, intended payload/integrity, observed phase/status, retry/error evidence, and any supersession. Completed authoritative effects must not duplicate. The same property applies to learner updates only when that future capability is separately implemented. Corrected blocked input uses an explicit replacement while the historical failure remains auditable. A successfully superseded failure is not still unresolved merely because its old record exists.

Do not promise exactly-once external calls where the source provides no such primitive. Read-only/model calls may repeat after an ambiguous crash within the remaining stage/attempt budget; account for both or report incomplete usage. For ingestion, publication, dispatch, or another effectful operation, replay only when a source-backed idempotency or reconciliation mechanism proves it safe. Otherwise retain an uncertain/blocked operation, keep evidence available, and reconcile or request operator recovery; do not blindly repeat a possibly committed effect. A locally generated operation ID alone does not make a remote API idempotent.

Serialize or conditionally coordinate conflicting local mutations so concurrent retries cannot overwrite each other's accepted work. Network/model calls need not hold local locks; the mechanism is an implementation decision. Feature-disable transitions are governed by Section 15.

## 12. Procedure trust, publication, and eligibility

### 12.1 Identity, scope, and authorization

A procedure has a stable logical identity and immutable behavior revision. Identity is origin-qualified so unrelated authors' same-name skills do not merge. A publication belongs to an application, memory/evaluation namespace, scope/recipient partition, logical procedure, and revision. Equal revisions in different permitted partitions must not collide.

| Scope | Permitted automatic recipients |
|---|---|
| Private/local | The owning logical agent within the specified application/project/namespace boundary. Private procedures are not published to the remote procedure library in V1. |
| Project | Authorized agents within the specified application/project/namespace boundary. |
| Shared | Explicitly authorized cross-project recipients under the configured sharing policy, still bounded by application/namespace. |

Approval authorizes immutable content and a recipient set, not merely a word such as `shared`. Delivery/publication may narrow that set; it cannot change project, owner, application, namespace, or broaden recipients without authorization. A project approval for project A does not authorize project B just because both have scope `project`.

Trusted approval and current designation are separate decisions. Broad approval does not replace narrower overrides. An operator may combine approval and designation for one explicit partition, provided both are recorded. Shared approval evidence must bind content, issuer, and authorized scope/recipients and remain verifiable by the receiving deployment through its configured trust policy. A hash or self-asserted `approved` flag is not proof of issuer authority. No new public-key infrastructure is required: authenticated trusted writers plus retained integrity-checkable approval evidence may suffice.

### 12.2 Content and search projections

Approved behavior includes procedural text, included references, compatibility predicates, declared fields/adaptation, and other meaning-affecting content. Changing it creates a new revision requiring the applicable approval policy, locally as well as remotely. Origin class survives approval/publication; a generated skill cannot become curated merely by being approved.

Derived indexes/vectors are not themselves procedural authority. They are bound to the exact content revision and representation fingerprint. Rebuilding an index or changing an embedding model need not invent a new behavior revision or repeat content approval when the approved meaning is unchanged; produce a separately identified compatible projection and record its configuration. Do not overwrite a frozen projection/snapshot or reuse a predecessor's incompatible vector for changed content. Retrieval of historical versions does not make them automatically eligible.

### 12.3 Current designation, supersession, and rollback

There is an explicit current designation per logical procedure and full trust partition, including owner for private scope. Designation—not approval alone—supersedes the prior automatic revision in that domain. Local approval/designation does not claim remote supersession before the remote current designation is durably accepted. The prior remote current may remain eligible during ordinary publication delay; unsafe content uses revocation instead.

Designation operations are ordered independently of revision hashes. A delayed older operation cannot replace a newer accepted designation. A trusted rollback to a still-approved, non-revoked historical revision is a new designation operation, not replay of an old publish. Revocation, withdrawal, or supersession never automatically reactivates an older version. Partition withdrawal also fences stale designation/publication retries from restoring that withdrawn publication unintentionally.

### 12.4 Revocation and distributed limits

Revision revocation blocks future local/source delivery immediately and targets every managed publication of that exact origin-qualified revision. Retain an authoritative revoked status/tombstone and the tracked publication set so an in-flight or future stale publish cannot escape the operation. Global completion within that managed application/namespace is reported only after every tracked remote publication/designation is blocked or withdrawn and pending stale publication cannot restore it. The product does not claim control over unknown external copies or unauthorized deployments.

A publication withdrawal removes one partition without declaring the revision revoked elsewhere and is recorded distinctly. Trusted credentials are required for either operation; no worker text authorizes them.

Revocation cannot erase instructions already delivered to a model. Record known exposure; ROOT decides correction, replacement/restart, or abort for affected execution. Frozen snapshots are historical inputs, not live state. Do not secretly hot-edit a scored trial after an external revocation. Record its snapshot age and known revocation; under the declared safety policy, continue only when safe and authorized as an explicitly historical trial, otherwise stop/invalidate it. Reproducibility does not require continuing an unsafe task.

### 12.5 Automatic eligibility and freshness

Before procedural use, establish approved content, authorized recipients, current designation, non-revoked status, integrity, and every declared structured compatibility predicate from trusted current facts. Unknown required facts fail closed; empty optional constraints are different from malformed/missing required metadata. Check one-way as well as mutual conflicts. Cases never self-authorize through this procedure gate.

For live remote-origin procedures selected for new use in this decision, check authoritative current/status state at use and at the final delivery boundary after intervening planning/review. A prior cache hit or search-index status alone is insufficient. If freshness cannot be checked within the optional budget, omit that shared guidance; if a reused plan depends on it, return the affected plan to ROOT for fresh planning or explicit reacceptance from current evidence. A cached shared copy is not reclassified as locally approved to bypass this rule. Independent local approval is a separate trusted decision, and known revocation is never ignored.

An already execution-accepted current plan is exact ROOT state, not a fresh delivery of its historical source template. Do not make continuity depend on re-fetching every old template. A known unsafe source exposure is referred to ROOT under Section 12.4; memory never silently replaces the accepted plan.

A freshness check establishes status at that check, not a distributed lease against a later change. Previously delivered content follows Section 12.4. Explicit frozen-snapshot runs use their declared frozen eligibility instead of claiming live freshness.

### 12.6 Final selected set

Deliver at most one revision per logical procedure. Among eligible versions visible at nested scopes, default to private/local, then project, then shared. The same specificity order resolves declared conflicts between different otherwise-eligible procedures; ties use a deterministic documented ranking rule. An explicit trusted selection policy may change that ordering, but cannot widen authorization, revive a revoked item, or deliver a declared conflict pair.

Preserve selected and rejected alternatives in the trace. Deduplication may combine provenance, but never erase the trust partition or freshness obligation of the payload actually used.

## 13. Security, privacy, and network mode

### 13.1 Control-plane authority and credentials

Product-managed workers, including measured B0, must not receive product-memory/control-plane credentials or endorsed approval/publication/revocation/policy-mutation authority through prompts, tools, configuration, or inherited environment. A safe channel is not permission to give a worker the same forbidden credential. Where a task needs an integration operation, ROOT performs the authorized operation or supplies a separate task-scoped credential that does not confer the prohibited authority.

Task-authorized credentials use a validated runtime/task-protected mechanism when one actually exists. The product does not assume the harness ships a generic secret manager and need not invent one. Without an approved mechanism, block/escalate the credential-dependent action instead of putting the secret in ordinary prompt text. Preserve the authoritative task source; a prohibited control credential in mandatory context blocks dispatch until a sanitized task/reference or a safe ROOT-mediated arrangement is supplied.

This is an operational boundary. Same-user native coding processes are not a hardened adversarial sandbox; a digest is not authorization, and prompt/tool restrictions alone do not prove operating-system containment. Preserve the existing task/runtime permissions without advertising stronger isolation than tested.

### 13.2 Reusable content and remote egress

Known credentials/secrets are not intentionally copied into reusable cases, skills, templates, model context, or diagnostic logs. Use redacted markers/summaries and protected evidence references. Authoritative raw evidence may remain with its protected owner where required; this is not authority to rewrite user task truth. Approved content must not be silently edited under its old integrity identity to accomplish redaction.

Before any remote publication, including project scope, check the full payload, search representation, and metadata for known secrets and unrelated confidential repository material. Reject or explicitly sanitize/re-author and, where meaning changes, reapprove before publication.

Apply the same local-to-remote privacy boundary to queries, embedding/adaptation inputs, telemetry, and replication. Minimize telemetry to declared-purpose fields rather than sending complete local evidence by default. Send repository-derived content only to trusted configured recipients authorized for that purpose; being relevant to a query is not authorization to disclose it. Authentication secrets travel only through the intended provider's approved authentication mechanism, never as incidental query/embedding/telemetry text.

The scanner/redaction design is replaceable. It needs a documented detection policy, configured known-secret fixtures, safe failure behavior, and tests; it need not claim perfect detection of every unknown secret or infer confidentiality from arbitrary prose. On an unresolved sensitive-content decision, omit the optional remote payload or ask the authorized operator rather than guess permission.

### 13.3 Network modes

| Mode | Claim permitted |
|---|---|
| `soft_guardrail_network` | Live Atlas is permitted; supported provider-native general web/search/fetch tools are suppressed. Record unsuppressed capabilities and remaining shell egress. This is not hard isolation. |
| `atlas_memory_only` | Provider-native general web/search/fetch is absent/suppressed, and independently enforced egress policy blocks unrelated destinations while permitting required services. Record the enforcement source and verify the actual launched tool/network surface. |
| `restricted_local` | No live Atlas operation on the task path, including optional telemetry or publication retries. Use declared local/frozen memory. This label alone does not say whether model-provider traffic or other benchmark-permitted networking exists. |

The mode describes the run's permission/enforcement profile, not a promise that every service responds or that a later B0 comparison uses memory. Resolve it before a product test/run; future scored evaluation freezes the same profile; retain service outages and effective store participation separately rather than relabeling failures to improve a cohort. Suppression syntax is provider-version-specific. If tools cannot be suppressed or egress enforcement is unverified, do not claim `atlas_memory_only`.

Ordinary fallback and admin safety operations never bypass benchmark/network restrictions. A revocation unavailable in a restricted environment remains pending for an authorized management environment. Matched-arm controls are governed by Section 16.

## 14. Learned selector: explicitly deferred

### 14.1 Current-phase boundary

Implement fixed/configured Standard, Problem-focused, and Deeper selection and deterministic availability/budget fallback. The current release does not implement learned selection, provisional training, exploration, frozen-policy inference, policy persistence, or policy updates. An explicit request to enable learned mode, load a learned policy for use, or enable online policy updates returns an actionable `deferred/not implemented` configuration result before work starts; it is not silently accepted as enabled. A normal fixed-mode run has no dependency on a policy artifact or learning service.

Record the selected/effective recipe, known inputs, outcome, and native usage because they explain the current product. Future development may derive training observations from that evidence. Do not add policy-family state or a training pipeline solely to claim future readiness. Current verification checks absence/non-activation; it does not implement the learner to test it.

### 14.2 Future design — not implemented or exercised in this plan

The following design is retained for a separately authorized future implementation. It adds no current-phase learner work or benchmark obligation.

The selector chooses only Standard, Problem-focused, or Deeper within Section 9. It cannot alter review, permissions, task constraints, free-form actions, or arbitrary procedure text. Fixed and learned selection share all availability/budget gates.

Separate **development training** from **validated deployment**. Explicit development mode may start from a recorded provisional policy, use bounded exploration, and collect eligible updates; it is not advertised as validated. Held-out/scored runs use a frozen policy validated for the recorded feature schema/action definitions and relevant budget/store/network regime, or a tested fixed strategy. Supported availability masking counts as compatible only when the declared policy regime covers it. Unavailable/corrupt/incompatible deployment policy falls back fixed.

Record requested strategy, effective executed behavior, fallback, and policy/configuration identity at decision time. Do not assign a normal outcome reward to an arm that never executed its defined behavior. The default for an incomplete/degraded recipe with ambiguous credit is no ordinary update. An explicit predeclared mapping may credit a well-defined effective arm; retain the original request and all failures in end-to-end reporting so exclusions do not hide an unreliable selector.

Ordinary positive feedback requires verified, non-forced success through the correct review/acceptance chain. Verified task-quality failure or genuine task-budget exhaustion may be negative when attributable; blocked/external/infrastructure, contradictory, forced, or unobserved evidence is not silently coerced into either. In feedback encoding, verified success ranks above eligible failure; effort/cost shaping does not invert that ordering. This is not a mathematical guarantee that a learned policy will always choose the best action.

Each observation retains its compatibility/policy family. A late outcome updates only a compatible, still-enabled policy history, not whatever incompatible policy happens to be current. Retired/unavailable history leaves audit evidence without an update. No online updates occur in held-out/scored mode, when learned selection is disabled, or when update permission is off.

Accepted updates have an exactly-once durable effect in a serialized/versioned history; concurrent outcomes cannot overwrite or lose one another's accepted updates. Replays and crash recovery preserve this property. The learner algorithm and transaction technology remain implementation choices. This is a future capability; none of its execution, training, validation, or update machinery is part of the current release.

## 15. Feature switches and effective transitions

The following table is the single behavioral definition of the switches. It describes effective disabled state, not a request to cancel already-submitted work. The learned-selector row is a reserved unavailable capability in this release, not an option to implement it under the current plan. Resolve each to one effective value and reason before an objective; a subordinate provider setting cannot silently re-enable a disabled feature.

| Switch disabled | Effect on the ordinary task path |
|---|---|
| Experience read | No optional local historical-case or experience-derived-skill retrieval, including recent-evidence lookup as optional memory. Exact current checkpoint/evidence stays available. Deliberately curated guidance and remote procedures follow their own controls. |
| Experience write | No new product-owned long-term experience writes; eligible pending non-safety writes are paused. Local decision/outcome and exact reviewed evidence remain durable. |
| Generated-skill creation | No new product-triggered skill generation from current or queued experience. Case capture may continue; existing skills are governed by the use switch. |
| Generated-skill use | No generated skill delivery from local or remote sources. Curated procedures remain eligible. |
| Shared publication | No new non-safety publish/promote/designate commits, including pending retries. Explicit revoke/withdraw operations remain permitted subject to actual authorization/network policy. |
| Atlas shared retrieval | No task-time remote procedure reads; local procedures remain available. This does not itself enable or disable separately configured administrative writes/telemetry. Restricted-local mode is the stronger all-Atlas-task-path restriction. |
| Plan-template memory | No execution-path template retrieval or APC. Offline/admin inspection is not a scored decision. |
| APC | No automatic template-to-plan conversion. Separately requested diagnostics may inspect templates, but do not alter the execution plan or masquerade as APC use. |
| Light adaptation | No adaptation stage. Direct fill and normal fresh planning remain available. |
| Deeper | That strategy is ineligible. |
| Learned strategy selection (deferred; unavailable now) | Fixed selection only. Explicit enable/load/update requests are rejected under Section 14.1; no learned-policy code or state is required. |

Light adaptation requires APC, which requires template memory. Generated-skill creation also requires experience-write permission because its results are long-term memory writes; disabling creation alone still permits case ingestion. Read/use controls do not require write/creation to be enabled, so already-approved procedures can remain usable without new generation. Runtime/benchmark permissions and the no-online-update rule can restrict a requested feature further; they never turn a disabled value on. Unsupported configurations are reported, not silently called enabled.

Feature configuration is captured for an objective; ordinary hot switching mid-call is not required. A change to “off” is effective for new work immediately, but cannot recall a remote effect already accepted. Pause new/pending submissions, drain or reconcile in-flight work, or isolate the next run from it before claiming a fully disabled measured state. Until that boundary is established, report transitioning/unconfirmed, not “off with no effects.” Late acknowledged work retains its original configuration attribution. Shared services need per-product control or isolation sufficient to enforce the declared ablation; filtering generated outputs after paying to generate them is not generation-off.

With all enhancements off, bypass the optional memory pipeline entirely: no EverOS/Atlas task calls, memory/APC context, or implicit curated-memory retrieval. Preserve inherited lifecycle plus declared uniform measurement/security controls. Explicit admin safety/maintenance is outside that task path, authorized and labeled separately, and cannot mutate a scored cohort's inputs unnoticed. Persistent pending work is not deleted merely to make a feature look disabled.

Optional service outages remove only dependent features where safe. Missing mandatory state or a failed security boundary is an explicit preparation/recovery failure, not an optional-memory fallback that silently proceeds unsafely.

## 16. Development verification now; deployment benchmarks later

### 16.1 Development verification — no benchmark runs

During development verification, test the actual candidate product harness using the nested harness/subagent protocol in Implementation Sections 1.1–1.2 and 15. The outer implementation harness may compile, lint, run unit/source/contract tests, and prepare fixtures, but those results do not replace native inner product-harness evidence. Product-test ROOT and its product-harness-launched test workers use the specified development-test agent tiers and CLI providers; APC uses the separately supplied `apc_adaptation_binding`. Heavyweight test review closes out important development steps from actual artifacts, exit status, linked review/acceptance, and cleanup evidence rather than an agent's unsupported PASS narrative. These test assignments do not constrain deployed ROOT, workers, or reviewers, and the APC binding remains configuration in every environment.

Allowed current tests include isolated synthetic/disposable coding tasks, baseline all-off lifecycle smoke, real local EverOS/Atlas integration with synthetic scoped data, APC direct-fill/light-adapt/fresh-plan cases, generated-skill provenance, resume/restart/fault recovery, snapshot restoration, resource ownership, and model-binding validation. Exercise every required core capability; neither the learned selector nor benchmark execution is among those capabilities. Tests may record native usage and latency to verify accounting and bounds, not to report a benchmark score or comparative performance claim.

No SWE-Marathon, FrontierSWE, MemoryArena, LongMemEval, BEAM, or other benchmark runner/task/subset is executed now. Do not substitute a benchmark sample for a synthetic fixture or silently schedule benchmarking after implementation closeout. No B0/B1/B2/B3 scored comparison or leaderboard submission is a current task. Later benchmark testing needs a separate plan/authorization; the prohibition is about current work, not removal of the future evaluation design.

### 16.2 Current snapshot and test-isolation support

Independent test fixtures use declared isolated local/remote state. Do not import implementation-harness memory, worktrees, approvals, learned artifacts, or previous test results into the product harness as undeclared inputs. Pre-existing work that could change fixture inputs is drained, paused, excluded, or explicitly controlled; new within-fixture memory work follows the selected feature settings. Implement snapshot/export/restore and evidence integrity on synthetic fixtures now so later evaluation has a tested foundation.

A snapshot captures a consistent logical state, not necessarily a universal instant across independent services. Concurrent changes must not tear captured state. It may explicitly preserve pending work if restore/replay semantics and dependencies are captured; it may not claim that unresolved external synchronization completed. Restore includes or can resolve the receipts, approval/current/revocation state, source/evidence references, representation/configuration, and any future policy needed for the capabilities claimed. Missing dependencies make that capability incomplete, not trusted by default.

Verify exported integrity and explicit application/project/namespace identity. Namespace-bound restore is valid; tested remapping is optional, but a filesystem copy with unchanged identity-bearing rows is not automatically a new isolated trial. Never overwrite production or merge independent trial outcomes implicitly. Historical failures that were successfully superseded do not invalidate an otherwise consistent snapshot.

### 16.3 Future deployment benchmark/evaluation protocol — execution deferred

This protocol is documentation for a later deployment/performance-evaluation phase. It does not authorize benchmark setup/runs, benchmark-specific runner integration, selector development, tuning on benchmark tasks, or performance comparisons in the current plan. That later phase runs the regular deployed product under its declared model/provider/effort configuration and benchmark runner constraints. It does not inherit the development-test role matrix or require the implementation-harness → product-test ROOT → candidate-harness topology. The later evaluation plan selects and freezes its own bindings; it may explicitly reuse a development-test model, but none is mandatory because it appeared in that test profile.

Define an experiment before scoring: tasks/verifier, repository baseline, models/providers/effort, total budgets, source/runtime versions, tool/network profile, feature values, memory/procedure snapshots, and selector policy/regime. Hold non-feature conditions materially matched when attributing an effect to memory/APC/search. Report unavoidable mismatches and do not attribute their effects to the feature. Memory-added context need not be identical to baseline prompts.

B0 is the harness + ROOT comparison; B1 adds memory/procedures with fixed search; B2 adds APC; B3 adds a validated frozen selector. B0/B1/B2 performance comparisons are deferred; B3 additionally depends on separately implementing the future selector. These labels are not current benchmark tasks. Each comparison declares its actual switches, not just a label. Before claims on a host/runtime class, run a pristine baseline launch/work/review/retire-or-cleanup smoke on that class. Measured B0 uses the same unrelated provider/network controls as enhanced arms and is not substituted for the pristine smoke.

Future independent trials begin from declared isolated state; development, validation, and held-out evidence remain separated. Pre-existing jobs that could change scored inputs are drained, paused, excluded, or governed by a frozen declared protocol. Within-trial memory evolution is permitted only as declared; held-out learned-policy updates remain disabled. Capture scope, receipt/approval/current/revocation/representation dependencies and any future policy/regime. Safety intervention follows Section 12.4 rather than silently hot-editing a frozen run.

Report quality, usage coverage, latency, failures, and effective degradation separately. Missing native usage may support quality evidence but not an unqualified efficiency percentage. Verify actual benchmark runners/rules at that later time. Current release conformance comes from Implementation Sections 15 and 18; no positive performance gain or benchmark result is claimed from the present functional test phase.

## 17. Observability and complete accounting

Retain enough durable evidence to reconstruct the exact objective/context and answer: which route, fixed configuration, strategy and stores were used (and which policy family only after the future selector exists); which candidates were rejected/selected/delivered; which approval/current-designation/representation evidence authorized them; what ROOT accepted; and which execution/outcome followed. Redacted records may preserve protected references/digests rather than copying secrets.

Record both initial planning disposition/APC branch and final accepted/executed-plan source. Existing-plan preservation is not an APC hit. Initial reuse followed by fresh planning is not clean cache adoption. Keep costs and review/rework even if the abandoned decision has no successor or no terminal outcome. Avoid counting the same usage again when it is referenced by both parent and successor.

Use provider-native usage where exposed and retain source/invocation identity. Distinguish retries, corrections, resumes, and genuine model calls even within one session; repeated parsing of the same native receipt is idempotent. Do not sum cumulative/intermediate totals twice or count cache/reasoning fields again when already included in a provider total. Unknown fields and unobserved calls are unknown, not zero. Record expected call/coverage evidence so absence of a returned usage event cannot look like free work.

Separate online work (search/query/refinement/rerank, harness-launched adaptation and its corrections, ROOT planning and applicable review, worker execution and terminal review), maintenance (ingestion/extraction/skill work and publication-related model work), and embedding usage. An APC support child's invocation is linked to its parent decision and counted once as adaptation cost; its draft-task closeout is not an independent success of the parent's coding objective. In nested product tests, preserve outer implementation orchestration, product-test ROOT, and inner product worker/APC/reviewer attribution; report outer testing overhead separately rather than hiding it inside product execution or counting it twice. Retain per-provider/model units rather than pretending embedding tokens equal completion-model tokens. Report combined cost only under a declared compatible unit/cost policy. An APC savings claim includes ROOT planning/review in both arms, attempted reuse, fallback, and any repeated/late calls.

State the measurement window and maintenance cutoff before comparison; asynchronous unobserved work makes that aggregate incomplete. Retain late evidence without changing the immutable task outcome. Measure stage latency and objective wall time separately; concurrent stage durations are not summed as elapsed wall time. Currency estimates require a pinned disclosed rate/model mapping when not natively reported.

## 18. Acceptance evidence index — non-normative

This section adds no requirements. Canonical current behavior is Sections 1–17 subject to Section 1.1; Sections 14.2 and 16.3 are future-only designs. Scope exclusions are Section 19; required executable coverage and release evidence are Implementation Sections 15 and 18. The implementation's coverage matrix maps tests back to those homes. Do not turn this index into a second acceptance checklist.

## 19. Explicit non-goals

The learned decision/search-strategy selector is **deferred and must not be implemented in this plan**, including development-mode training or a disabled shipped learner. All benchmark execution and scored performance comparisons are **deferred**, including SWE-Marathon, FrontierSWE, and every other benchmark. Their future specifications are Sections 14.2 and 16.3. The named model assignments and two-harness protocol are explicit development-verification requirements only, not deployment architecture, deployment defaults, or a learned routing feature.

V1 does not require replacing ROOT/harness, a second general-purpose memory engine, automatic template mining, all-EverOS migration to Atlas, a graph database, a separate APC service, semantic answer caching, inference-prefix caching, arbitrary executable generated scripts, a dashboard, or a learner for every retrieval/tool action.

A narrow local template registry is permitted; a full EverOS template extension, `langchain-mongodb`, and optional Atlas telemetry are available implementation choices, not mandatory architecture. No new generic secret manager or proof of adversarial same-user sandboxing is required. Do not add those systems to satisfy a prose mechanism when the declared behavior has a simpler implementation.

## 20. Implementation freedom

Use the engineering decision framework in Implementation Section 0. Implement, test, and document ordinary behavior-preserving choices without repeatedly seeking approval or running open-ended prose audits. Escalate only a consequential scope, authority, privacy/retention, compatibility, evaluation, or behavior change that cannot be resolved within the governing guarantees. A failing test can reveal a bad prescription as well as bad code; correct the responsible owner rather than preserving an obsolete recipe.
