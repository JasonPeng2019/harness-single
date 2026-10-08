# Memory- and Plan-Reuse Coding Harness
## Implementation Contract and Engineering Guide

**Revision:** 67  
**Date:** 2026-09-20  
**Supersedes:** `PRODUCT_IMPLEMENTATION_SPEC_v66.md`  
**Behavioral authority:** [Feature specification](PRODUCT_FEATURE_SPEC_v65.md)  
**Source evidence:** [Detailed ledger](PRODUCT_SPEC_DETAILED_v72.md)  
**Status:** Only sections marked **[BINDING]** are normative implementation contracts. **[REFERENCE]** sections provide replaceable engineering starting points.

## 0. Engineering decision framework [BINDING]

### Authority and changes

The Feature document owns product behavior. This document adds cross-component data/operation contracts, implementation governance, and verification—not a second behavioral specification. The Detailed ledger and historical review reports cannot impose requirements. Feature Section 1.1 excludes learner implementation and every benchmark run from this plan. The development-test agent assignments and two-harness protocol in Sections 1.1–1.2 are explicit user decisions for this verification workflow only, not deployment defaults or architecture. Those named assignments do not choose the APC child: every test or deployed run supplies `apc_adaptation_binding` independently. Within the named-role tests, changing CLI syntax to obtain the same requested model/effort is allowed; silently substituting a different requested binding or doing deferred work is not. Ordinary deployment and later benchmark use select supported model/provider/CLI/effort bindings independently under Feature Section 1.1.

A heading label applies at any heading depth and is inherited until a nearer labeled heading overrides it. Unlabeled subsection headings inherit their parent; an unmarked top-level section does not create requirements. Examples in reference sections are not exact acceptance oracles.

Use this decision sequence when source, tests, or implementation contradict a recipe:

1. Identify the owning Feature requirement and the actual source/runtime evidence.
2. Select the smallest supported implementation preserving scope, authority, privacy, durability, interoperability, and evaluation semantics.
3. Change the code and its canonical schema/adapter together; add or update the relevant property test. Correct a stale test or reference, not the requirement it misstates.
4. Record the material choice, source symbols/revision, affected artifacts, tests, and license obligations in a concise decision/reuse ledger. A small local refactor does not require a separate design memo.
5. Continue without user approval unless no behavior-preserving resolution exists or a consequential tradeoff changes the product's required scope, trust/recipients, retention/privacy, deployed compatibility, or evaluation validity.

Prefer `USE_AS_IS > WRAP > EXTEND > COPY_ADAPT > NEW_CODE`. This is a reuse preference, not permission to force an unsuitable upstream abstraction. A simpler tested adapter or narrow local template registry is legitimate; replacing a working subsystem merely for aesthetic uniformity is not. Preserve licenses/NOTICE and applicable upstream tests. Record why copying or new code is needed when a suitable existing implementation appeared available.

Mechanisms may change without a product redesign: persistence technology, transaction/locking approach, provider syntax, call scheduling, internal schemas, package/CLI names, retrieval adapter, and calibrated representation/tuning are examples. Before changing an already-used interface or persisted representation, apply Section 2 compatibility handling; “internal” does not mean disposable.

Source observations are hypotheses until verified against the pinned implementation. Implementation-complete, service-tested, native-runtime-tested, and benchmark-improved are separate claims. Do not describe self-review as a separate reviewer or a clean prose pass as proof that implementation is defect-free.

## 1. Ownership and integration boundaries [BINDING]

| Responsibility | Implementation owner |
|---|---|
| Task/current-plan authority, applicable review and acceptance | Existing ROOT/harness and repository instructions |
| Worktrees, launch/resume/corrections, result protocol, resource/process cleanup | Existing harness |
| Local experience ingestion, case/skill extraction and recall | Supplied EverOS, reused or minimally extended |
| Local versioned plan templates | Product integration: an EverOS extension or a narrow local registry, selected for simplicity and tested behavior |
| Shared procedure storage and Atlas Vector Search | MongoDB Atlas, through a tested adapter |
| Exact local decision/outcome/approval/usage evidence and reconciliation | Product integration; remote telemetry is optional |
| Cross-store normalization, fixed strategies, APC and context assembly | Product integration; APC adaptation delegates through the product harness |
| Learned search-strategy selector | Future design only; no current implementation, training, inference, or update owner to build |
| Admitted topology planning/formal workflow | Supplied ROOT skill suite |

Keep optional memory service work outside the harness controller's correction/lifecycle loop. APC drafting nevertheless uses the existing product-harness child launcher/lifecycle under Section 6.1, not a parallel API adapter. Reusing a common prompt/handoff seam is preferred; another small supported seam is allowed. Do not introduce a second general-purpose memory engine, scheduler, review system, or Atlas search engine. The two harness instances below are separate owners/runtimes used for development testing, not a requirement for a second deployment harness or scheduling technology. Narrow test-orchestration glue for that ownership/delegation boundary is permitted; normal product startup does not require it.

### 1.1 Required development-test agent assignments and APC binding — not deployment defaults

This is the binding model/CLI/effort matrix for **development verification of the product harness**, including bounded implementation/fixture work and closeout review performed within that development/test workflow. It does not prescribe deployed ROOT, worker, or reviewer models. APC plan revision is deliberately separate: `apc_adaptation_binding` is a required run configuration variable and has no named model default. Role names express the user's test allocation, not a measured capability ranking or a runtime requirement to implement a three-tier routing system.

| Development-test role | Selected model | CLI and effort | Assigned development/test work |
|---|---|---|---|
| Lightweight / low capability | **GPT 5.6 Luna** | **Codex CLI; High** | Routine test execution and straightforward bounded implementation/fixture work. |
| Middleweight | **GPT 5.6 Terra**, or **Claude Sonnet 5** | **Codex CLI; High** for Terra. **Claude Code CLI** for Sonnet; use and record a supported pinned effort/default because no Sonnet effort was prescribed. | Substantive coding and comparatively important implementation/integration work within the development/test workflow. |
| Heavyweight | **GPT 5.6 Sol** | **Codex CLI; Medium** | ROOT assignments in this development/test workflow, consequential/complex implementation, and important development-step closeout review. |
| APC drafting child | `apc_adaptation_binding` (**no named default**) | The test run supplies a supported provider/CLI/model/effort binding. | Near-match APC plan revision exercised through the candidate product harness. The supplied binding is designated lower-capability for that run. |

ROOT assignments within this development/test workflow—including its implementation/test-orchestration ROOT and delegated product-test ROOT—use the heavyweight test binding. Routine test assignments use the smallest appropriate prescribed role; consequential test reviews/closeouts use heavyweight. This is explicit test configuration, not the deferred learned search selector, and does not replace EverOS extraction/embedding services with agent launches.

Use Terra as the default middleweight test choice; Sonnet is an explicitly selected test alternative, not an unreported provider fallback. A native development verification campaign needs Luna, one selected middleweight alternative, Sol, and one explicitly configured APC adaptation binding; these may name the same model only when the resolved run configuration deliberately does so. It need not run every scenario with both middleweight alternatives; claiming both were verified requires evidence for both. A task promoted to heavyweight is a new recorded test assignment, not a relabeled middleweight result.

Resolve the requested test names and `apc_adaptation_binding` to exact installed-provider model IDs and supported effort settings before launch. Record CLI version, requested/resolved binding, and provider-reported model when available. Aliases, upgrades, or fallback chains must not silently replace a requested model/effort. Unavailable or mismatched named-role bindings leave their affected development tests blocked/nonconforming. A missing or invalid `apc_adaptation_binding` may exercise the APC-to-fresh-plan failure path, but does not prove successful light adaptation. Exact flags/IDs are version-checked adapter configuration, not assumed permanent syntax.

**Configuration boundary:** deployed ROOT, workers, and reviewers use supported bindings selected in the deployment's own configuration. The APC drafting child always uses the run's explicit `apc_adaptation_binding`, including during development tests; there is no product or test default. Do not hard-code this development-test matrix into product eligibility, ordinary startup, dispatch, or fallback checks; do not make access to these named test models a prerequisite for a differently configured deployment. Any lack of native evidence for a supplied binding is reported honestly, not solved by declaring that binding forbidden. Later benchmark/performance runs use their separately declared deployment/evaluation profile and APC binding, not this test matrix by inheritance.

### 1.2 Development-only two-harness test protocol

This protocol governs functional development verification, not normal deployment or later benchmark performance evaluation. A deployed product runs its own ordinary ROOT/worker hierarchy and its APC support children without an outer implementation harness, delegated product-test ROOT, or outer-test receipts. Test runners remain separable tooling; the installed product does not recursively create another harness to perform normal work.


```text
Implementation ROOT / implementation harness (builds the product)
    -> launches heavyweight product-test ROOT as a delegated subagent
        -> sets up and invokes the recorded candidate PRODUCT HARNESS
            -> launches lightweight, middleweight, and heavyweight test workers
            -> launches the APC support child from `apc_adaptation_binding` when light adaptation runs
            -> records its own reviews, outcomes, resume/cleanup and native receipts
        -> returns product-harness evidence to implementation ROOT
```

The outer **implementation harness** coordinates code changes and test setup. The inner **product harness** is the actual candidate system under test, even if both reuse the same harness source or CLI installation. Product-test ROOT is launched by the outer harness but is explicitly authorized as ROOT **only for the isolated development-test instance**. It is not an ordinary product worker with accidental publication credentials. Use the existing supported ROOT/delegation configuration; if the outer launcher only supports ordinary workers, add a narrow trusted test-ROOT assignment at task creation rather than weakening every worker's restrictions or introducing another scheduler. Its product-side control access is scoped to test data/services and delivered through validated control configuration, never as secrets pasted into prompts. Ordinary inner workers retain the Feature security boundary.

Pin the candidate build/revision or equivalent source manifest, entry point, configuration, and installed provider bindings before a native test wave. Product-test ROOT calls that candidate's setup/launch/review/resume/cleanup interfaces. Running equivalent commands directly in the outer harness, invoking a raw provider CLI instead of the product launcher, or merely asking a coding agent to narrate a test is not product-harness execution evidence.

Use distinct instance/run ownership and separate runtime directories, worktrees/task roots, queues/leases, evidence logs, generated provider configuration, and local-memory/Atlas test partitions where applicable. Disjoint ownership can be provided by namespaces under shared infrastructure; two hosts or duplicated immutable dependencies are not required. The inner harness cannot overwrite or retire the outer harness's files/processes/resources. Do not use broad process-name cleanup. Nested-launch capacity and budgets must allow the child wave or fail preflight instead of deadlocking on a resource held by its parent.

Each instance manages its own children. Product-test ROOT drains/stops/retires its inner workers and archives their evidence before its outer task closes. On test-ROOT failure, the outer owner may recover/clean only the recorded inner instance/subtree through an explicit ownership-checked path; unresolved children are reported, not hidden behind a successful ROOT exit. Do not recursively create further test harnesses. Product code fixes go back to the outer implementation workflow; a changed build starts a new identified test wave rather than silently changing the running candidate.

## 2. Shared record and compatibility contracts [BINDING]

### 2.1 One implemented schema owner

Before a cross-component exchange or durable write is used, define its concrete schema, identity rules, integrity serialization, and supported version in one owning code module/schema artifact. Consumers import it or validate against the same artifact. Other documentation references it rather than maintaining independent field lists. These prose contracts specify required meaning; the implementer chooses the initial field names and wire layout coherently.

Reject malformed required fields, unknown critical versions, contradictory identities, non-finite vectors/numeric budgets, and missing authorization/current-state evidence. Do not interpret an absent required compatibility field as “no restriction.” Additive extensions or older versions are supported only through an explicit tested compatibility rule; do not silently duck-type them into the current schema.

Schema changes affecting persisted data require a declared supported read path or migration and preservation of audit evidence. This applies even if only the product itself reads the data. Historical illustrative schema IDs in earlier specs are not a deployed migration obligation unless actual data used them.

Integrity canonicalization must be deterministic and unambiguous: structured/domain-separated identity inputs, explicit text normalization, stable map/set handling, and exact payload references. Do not concatenate variable-length IDs without delimiters/length encoding. Do not derive identity from volatile timestamps, mutable display names, or guessed semantic equivalence. An integrity digest detects changes; authority still comes from the trusted source/control path.

### 2.2 Logical identities and lineage

Represent application, project, memory/evaluation namespace, logical agent owner, task, bounded objective, decision, execution invocation, procedure origin/logical identity, immutable behavior revision, and external operation identity. They need not use those literal field names, but their meanings and joins cannot be collapsed.

A decision is unique within its actual namespace/application/project/task/objective context. A retry resumes the captured preparation identity and fixed configuration. A future learned-policy binding is relevant only after that deferred capability is implemented; current records need no loaded policy or placeholder policy hash. An explicit supersession records the predecessor; intentional parent/child task lineage must resolve exactly and cannot be guessed from session, time, lane adjacency, or similar text. Parents are earlier acyclic decisions for the same task unless an explicitly authorized cross-task provenance relation is separately modeled.

For nested development tests only, include harness-instance ownership and retain the outer delegation → product-test ROOT → candidate product instance → product worker/APC child relation. Native CLI session IDs alone do not identify which harness ran a test. Those outer-test links are conditional test evidence, not required fields for ordinary deployed executions; deployment records use their actual product ROOT/run ownership. In every mode, APC support-child identity links to its parent objective/decision but is not a second primary search-strategy observation. Do not collapse the child's draft-task receipt into the parent's implementation acceptance.

Procedure identity is stable across content edits and origin-qualified. Approval, current designation, publication partition, and revoked state remain distinct from content revision. Reference formulas are in Section 4, not binding encodings.

### 2.3 ROOT-to-integration objective contract

Convey exact task/objective identity and text, repository/intended execution baseline, current constraints, and current-plan identity/state/route/objective binding. Also convey known phase/task-family/flags, failure context, environment/languages/capabilities, failed hypotheses, checkpoint, completed/remaining work, verification references, trusted field bindings, budget/deadline information, and usable executor context size when known.

Unknowns remain explicit; no extra classifier call fabricates them. Current-plan meanings distinguish no plan, candidate, reviewed-but-not-accepted, and execution-accepted. Any nonempty plan has a loadable integrity-checked reference or equivalent inline identity tied to the active objective and route. The contract supports ROOT explicitly invalidating/revising a plan; memory cannot do that implicitly.

Keep topology verdict evidence separate from the chosen route. Level 0 resolves to ordinary execution; formal selection requires the actual formal trigger. Internal class/enum names remain flexible.

### 2.4 Finalization and dispatch receipt contract

This is the parent implementation/execution handoff. APC drafting support requests use Section 6.1 instead of requiring a not-yet-accepted parent plan; they still retain the ordinary harness's identity, result and ownership checks.

The final handoff contains or exactly references: decision and objective/task binding; intended repository/worktree base; accepted plan and its review/acceptance evidence; actual rendered mandatory/optional content; delivered item origin/revision/provenance; role separation; and an integrity value covering the complete envelope.

Validate against the actual task card/execution target at dispatch, not only against the envelope itself. A caller changing the task or base while copying an otherwise valid packet must fail. A changed rendered package gets a new integrity value and updated delivery evidence. Preserve the distinction between selected, packed, and actually delivered items.

Record dispatch intent and the returned/observed harness run/invocation identity without implementing a second launcher. Recovery uses that exact identity to reconcile an uncertain launch. Finalized context is not a delivery acknowledgment. Native corrections remain in the original execution chain; an explicit new resume objective receives its own preparation identity.

### 2.5 Procedure, approval, and compatibility contract

A normalized candidate/record carries type, origin class, origin-qualified logical identity, revision/content integrity, source/payload reference, scope/recipient/owner boundary, approval evidence, current/revocation state, and structured applicability/conflict/capability/route predicates. It retains both discovery signals and any separately calibrated APC similarity/fingerprint.

Concrete predicate encoding is chosen once and has explicit absent/empty/unknown/AND/OR semantics. All adapters preserve the same meaning before and after publication. Unsupported required predicates reject automatic eligibility; aliases do not become empty constraints. Origin distinguishes generated from curated/builtin after approval, publication, and retrieval.

Approval evidence binds the exact approved content and authorized recipients to a trusted issuer/policy. For cross-deployment use, retain a verifiable approval record or trusted attestation plus durable supporting references; a machine-local pathname or digest alone is insufficient evidence for a receiver unable to resolve it. The receiver applies its configured issuer/recipient trust, not candidate-supplied authority claims. Exact source-case-to-reviewed-receipt validation occurs at generated-skill approval; keep that evidence auditable without requiring every receiving worker to access the origin's private filesystem.

### 2.6 Outcomes, receipts, usage, and operation records

The execution outcome binds actual dispatch/run/task identity, accepted plan, linked review/acceptance evidence, force/exception/failure status, and observation time. A reviewed-memory receipt binds that outcome/execution to the exact ingestion trajectory/source cases. All enabled current-phase side effects can be rediscovered from durable evidence after an interrupted reconciliation. No learner observation/update store is required now. Preserve neutral strategy/outcome/usage facts for later development without importing future policy machinery.

Usage records retain invocation/source identity, decision/task/objective attribution, provider/model, stage, native components/total/cost when exposed, and completeness. Coverage evidence includes calls known to have started even if no native usage returned. Re-parsing a receipt does not create a second call; identical counts from different invocations are not the same event.

External operations retain immutable intent/payload integrity and a stable identity, observed status, uncertainty/error, and supersession. Reference labels such as pending, committed, blocked, or uncertain are not a mandated state-machine encoding. A receipt of local intent is not a remote commit receipt.

## 3. Execution sequence [REFERENCE]

A practical ROOT-side cycle is:

| Boundary | Work | Next action on failure |
|---|---|---|
| Resolve | Read exact task/plan/repository/configuration; validate route and gates; record preparation identity. | Mandatory inconsistency goes to ROOT recovery; no semantic substitution. |
| Prepare | Run at most one primary bounded search recipe; normalize candidates and record trace. | Keep valid partial results or no optional memory. |
| Plan | Preserve accepted current plan; continue current review; otherwise direct-fill, launch the lightweight product-harness adaptation child under Section 6.1, or fresh-plan. | Invalid/rejected reuse returns to normal ROOT planning. |
| Finalize | Obtain accepted plan; refresh required procedural eligibility within budget; pack content and bind the handoff. | Omit invalid optional guidance; return plan-affecting changes to ROOT. |
| Dispatch | Call the existing harness with scrubbed control credentials; retain intent and actual execution receipt. | Reconcile uncertain launch before any repeat. |
| Observe | Load actual linked terminal evidence and fix one local outcome. | Invalid/absent evidence stays unobserved; retain diagnostics. |
| Reconcile | Ensure enabled local receipts/ingestion and optional remote copies; no policy updates in this phase. | Retry only safe operations, retain uncertainty, and preserve task acceptance. |

For a late Level 0, invalidate the prepared topology result; default to one bounded ordinary-route re-prepare when useful, otherwise continue with an explicit no-memory successor/continuation receipt. Never reuse the invalid packet merely because the next path omits optional search. The original cost remains attributed.

Useful internal lifecycle labels separate `prepared`, `dispatch_pending`, `dispatched`, `outcome_recorded`, and `abandoned`. Use fewer/different states if the implementation can still distinguish those facts. A persisted state name is less important than recoverable evidence of what actually happened.

## 4. Durability and recovery design [REFERENCE]

Start with an existing local transactional primitive when it simplifies the product evidence/outbox. SQLite transactions, or well-tested append-only records with locks, are alternatives—not competing required implementations. A compact transaction can persist a canonical outcome and the deterministic side-effect intents together; remote calls occur outside that transaction and are acknowledged separately.

Use unique logical operation/update keys and conditional versioning to prevent two retry processes from owning the same mutation. Retain append-only audit events or equivalent immutable history; mutable projections can point to current state. Transactional storage need not emulate JSONL sequence gaps, and JSONL need not pretend several file replacements are one atomic commit.

A reference identity uses a domain tag plus canonical structured inputs, for example a hash of `["decision", namespace, app, project, task, objective, preparation_nonce]`. Capture the input/configuration/policy digests as replay bindings. A monotonically allocated preparation sequence or a stored random nonce can both supply the nonce. Source identity and behavior revision use exact normalized manifests, not a task name or timestamp. Integrity inputs never include raw secret values as public diagnostics.

The EverOS deferred-add/flush recipe is a candidate only after source tests prove buffering, deduplication, extraction, and concurrent retry behavior across the relevant crash points. A message-ID formula by itself is insufficient proof. If a write's commit is uncertain and exact reconciliation/idempotency is unavailable, keep the durable reviewed trajectory and hold the write for recovery instead of duplicating it. Optional memory outages should not stall valid task work.

Future selector implementation can reuse single-owner/conditional-commit discipline for an ordered policy history. That is a retained design note, not current work: do not add observation-update storage, replay code, or training jobs now.

## 5. Publication interoperability [BINDING]

The remote procedure contract preserves the normalized record in Section 2.5 plus its publication partition, approval evidence, content/payload manifest, and representation identity. Receiving deployments can resolve the payload and approval evidence using trusted configured sources. Local-only paths cannot stand in for remotely retrievable payloads.

Keep four facts distinguishable: immutable approved behavior, scope authorization, current designation, and revocation/withdrawal. A storage design may combine these in documents, but its conditional updates must implement Feature Section 12. Approval alone never changes another partition's current designation. An old publish retry never counts as a new authorized rollback.

Represent current-designation order independently of target revision hashes; use a generation/conditional predecessor or another equivalent causal token. Publication/withdrawal/revocation and new-current commits coordinate against current lifecycle state, so publishing approved bytes cannot bypass a later revocation or withdrawal. Retain sufficient publication tracking/tombstones for the managed revocation completion claim. No distributed transaction across the origin filesystem and Atlas is required.

Distinguish source-local commit, remote durable commit, and local acknowledgment. If a response is lost, remote state may already have changed; the origin reports uncertainty and reconciles by exact operation/current identity. Search-index visibility can lag a durable write and cannot substitute for authoritative eligibility lookup.

Immutable behavior and rebuildable search projections are separately identified. Projection changes preserve content approval only when approved meaning is unchanged; otherwise create a new behavior revision. A candidate's score, payload, approval, and current designation must all resolve to the same intended revision/domain before use.

## 6. Template and similarity interoperability [BINDING]

A canonical template conveys identity/revision/provenance, fixed structure and verification/stop intent, typed fields and permitted source/trust policy, structured compatibility, and allowed structural edits. Field names, JSON/Markdown layout, and number of descriptive fields are not fixed here. The local registry, publisher, retrieval adapter, and APC consumer use the same implemented schema.

The validator distinguishes current facts from future investigation and proposed edits. Declared substitutions cannot execute commands merely to validate syntax. Paths and artifact references respect the configured source/repository boundaries. Adapter output is untrusted until schema, allowed edits, required bindings, integrity, and ROOT review have been checked.

Similarity comparability is an explicit representation contract: model/provider identity as needed, effective dimensions/projection, canonical query/candidate preprocessing, sanitization policy version, and metric. For one comparison all candidates use the same canonical sanitized query semantics. Compatible source representations or exact centralized rescoring are acceptable; an unknown fingerprint, malformed vector, zero norm where relevant, or uncalibrated source score makes automatic reuse unavailable rather than approximately comparable.

Deterministic preprocessing means the same input/recipe is constructed; it does not promise a remote model returns bit-identical floating-point values forever. Pin the model/representation revision and retain the actual score/projection evidence used. Re-embedding cannot silently contaminate a frozen comparison.

A changed template revision must not inherit a stale predecessor representation. Unchanged historical behavior may retain its historical projection; rebuilding a compatible derived index is not editing approved procedure text.

### 6.1 Harness-launched APC drafting support contract

Feature Section 6.3 owns the behavior; the implementation supplies a thin harness delegation adapter, not a standalone adaptation API client. In every environment the **product harness** launches the configured lower-capability drafting subagent by resolving `apc_adaptation_binding` to the selected model/provider/CLI/effort. The harness must not infer a lower model from installed providers, copy another role's binding, or supply a named fallback. In development tests the implementation harness must not service the APC child request on the candidate product's behalf. Outside testing, no outer implementation harness is required.

A child task carries or exactly references the parent decision/objective, immutable selected template and draft if present, trusted current bindings/evidence, edit/field limits, a fixed result-artifact contract, and the remaining adaptation-stage deadline/limits. ROOT authorizes those drafting instructions as the child's bounded support task; the parent's not-yet-accepted plan is explicitly proposed data, not authority. The support child uses normal harness task/run ownership and result validation without recursively running the full memory/APC preparation cycle.

The child may read the supplied scoped material and write its draft/result artifact. It may not implement the parent's code changes, certify parent acceptance, approve/publish memory, spawn more agents, run new open-ended retrieval/investigation, or create another harness. Its capability profile disables APC/template preparation and additional memory search. Native result-correction attempts remain owned by the existing controller and share the same absolute stage deadline and declared limit; no second retry loop replenishes them.

A child completion/result receipt proves only that a drafting support task completed. Retrieve and validate the exact result schema, parent/template binding, field types and allowed edits before passing the proposal to ROOT under the run's configured assignment. Parent plan acceptance and any required independent review remain separate. Retain child invocation/result/review/cleanup references, native model/effort and usage; attribute its cost once to parent online adaptation, not a second successful coding outcome or reusable solved case.

Launch intent, timeout/cancellation and uncertain acknowledgement use the existing harness recovery/cleanup contract. Do not relaunch after ambiguous receipt without reconciliation; do not release a workspace/lease for conflicting work while its child may still use it. A bounded failure goes to fresh ROOT planning. Standalone SDK/EverOS model calls, ROOT self-rewriting, and provider-internal delegations lacking product-harness ownership/receipts do not satisfy this contract.

## 7. Retrieval implementation [REFERENCE]

For a small seed library, an immutable local template manifest plus cached vectors and bounded lexical/dense scoring is a reasonable starting point. It does not need a new general-purpose memory service. Extend EverOS instead when source-supported integration is actually simpler. Put only the backends genuinely supported by that chosen route in the advertised support matrix.

Use the supplied `langchain-mongodb` search/index primitives when they fit; a tested direct Atlas adapter is also allowed. Exact metadata/vector reads may use normal collection operations. The Detailed ledger records the pinned package's constructor, score, filtering, and exact-get caveats; verify them before choosing a wrapper.

Start independent store/kind calls concurrently, or reserve per-store sub-budgets. Both can satisfy Feature Sections 8–9. Cap physical requests and shortlist size as well as final context. A mixed top-k response that starves all templates is not independent kind capacity. Type-specific calls or an equivalent tested quota scheme solve that problem.

Normalize and gate before final selection. Deduplicate one logical revision across stores without adding a duplicate-storage ranking contribution. Resolve visible revisions and declared conflicts using the Feature policy. Preserve exact provenance and live/frozen eligibility mode even if a local cached payload supplies the bytes. Retry/requery solely to fill a quota is unnecessary.

Health checks should not become another unbounded model/search stage. Readiness can be established before a run and failures revalidated narrowly on use. Runtime retrieval does not opportunistically alter index infrastructure: create/repair/wait through the explicit setup/administration path.

## 8. APC branch implementation [REFERENCE]

Check exact current-plan precedence first. Existing accepted or in-review plans need no template scoring/adaptation; evidence/skills may still be useful around them. For planning-needed ordinary objectives, evaluate the bounded eligible shortlist and feasible branches, then prefer the highest calibrated similarity; ties use a deterministic recorded ordering. An unavailable adaptation branch should not force choosing an unusable high-ranked candidate over a viable candidate.

Apply the reference thresholds in Section 16 only to their calibrated metric. Above the direct-fill threshold, direct fill still needs all required bindings and unchanged fixed structure; otherwise only a permitted adaptation branch is considered. Below the lower threshold, use fresh planning. Configuration may be tuned on development evidence but does not waive compatibility.

A structured template/step representation with bounded edit operations is easier to validate than unconstrained free-form rewriting. Wrap the product harness's existing task/launch/result lifecycle for the Section 6.1 drafting child. Require and resolve `apc_adaptation_binding` explicitly; do not call the provider SDK or EverOS client to bypass the required harness path. The child's proposal does not become a verified fact or accepted parent plan. Reject invalid or over-budget output and return to ROOT; do not invent a second strong planner inside APC.

ROOT review can legitimately revise a candidate. Preserve original template/draft provenance, the final accepted artifact, review evidence, and any fresh-planning fallback. No text-diff percentage is required to classify every editorial change as a cache failure; a rejected/abandoned reuse path is never reported as clean adoption.

## 9. Context boundary implementation [REFERENCE]

Section 2.4 is the binding handoff contract. The reference adapter implements it for both bootstrap and native resume and keeps legacy task cards without optional memory valid. The producer and consumer share one validator/format owner or equivalent round-trip contract. Packaging storage location and field name remain implementation choices.

Verify the intended task/objective/repository and accepted-plan binding at dispatch. The actual prompt construction preserves mandatory content and historical-versus-procedural roles. Validate the final envelope rather than only its source template or a digest of the Markdown subsection. Authentication/authorization comes from trusted ROOT execution, not a worker's ability to compute the same digest.

Do not make prompt rendering depend on a late uncontrolled database call inside a worker/controller. Resolve optional content at the ROOT boundary under budget, then render the validated result. Any pre-dispatch omission/refinalization updates the receipt. Missing optional evidence must not accidentally remove the mandatory accepted plan.

An oversized mandatory context, prohibited control credential, invalid plan binding, or unresolved dispatch identity is surfaced before unsafe execution. Benign optional candidate rejection follows the Feature fallback instead of becoming a new mandatory dependency.

## 10. Workspace and provider composition [BINDING]

Preserve required harness command skills/provider lifecycle behavior and ROOT-suite skills/provider behavior. Existing repository instructions are not overwritten merely because a portable starter file exists. Unknown ownership collisions fail preflight rather than choosing the last writer.

Identify pinned input ownership, inspect actual overlapping destinations, stage a deterministic composed view, validate it, and commit with a completion receipt or equivalent recoverable boundary. After an interrupted partial commit, enhanced dispatch is blocked until validation/recovery finishes. This does not require pretending multiple filesystem files can be atomically replaced together. Conflicts discovered in preflight do not mutate live files.

Idempotent setup recognizes its already-correct composed output. It must also detect a later upstream setup/overwrite that restores raw payloads and require recomposition or a valid baseline path. Validate the actual launched worker payload/tool configuration, not just a source file that staging can replace. Provider-specific suppression mechanisms are confirmed for the pinned installed version.

A standalone suite validator describes its own export; preserve it for pristine-source checks. A composed-workspace check tests the required union and ownership, not accidental equality with either standalone tree. No per-provider mirroring port is assumed supported without source/runtime evidence.

## 11. Outcome and usage implementation [REFERENCE]

Normalize source review/acceptance evidence only after exact execution linkage. Record authentic PASS/FAIL/BLOCKED/unknown facts, acceptance and forced/exception status separately. These neutral facts support current reporting and future learning; current code does not compute policy updates or require learner-specific eligibility state. Do not label a missing artifact as terminal unknown. Reconciliation can retry after the local outcome is fixed.

Use retained per-invocation structured provider output where available. Record invocation boundaries across corrections/resumes instead of slicing a cumulative session arbitrarily. API integrations retain native response/request usage independent of whether an optional tracing span happened to exist. A receipt with missing fields is incomplete, not zero-cost.

Capture call intent or equivalent expected-coverage evidence, source identity, native usage, and current fixed strategy/configuration. A policy-family binding belongs only to the future selector implementation. Aggregate by actual invocation and task/objective ownership, not by summing every reference to an event. Include ROOT planning and reviewers, wasted/repeated preparation, and asynchronous costs under their declared windows. Resource history and the immutable quality outcome have different update lifecycles.

For future selector development only, a candidate feedback mapping is clean linked PASS+ACCEPTED → eligible success and clean linked FAIL+REJECTED → negative only for attributable task-quality/budget failure. Forced/contradictory, missing, or unrelated-outage evidence stays excluded. This documents a future mapping under Feature Section 14.2; it is not a current reward function or update path to implement.

## 12. Configuration and feature resolution [BINDING]

Feature Section 15 is the sole disabled-state table. The implementation exposes one resolved value per semantic feature plus requested value, prerequisites, availability/permission reason, and configuration identity. Names and config layout may differ; duplicate switches cannot produce contradictory effective state.

Resolve configuration before preparation, and retain it with decisions/operations. A missing or invalid required value must not silently enable an expensive stage or weaken a trust gate. Search defaults to fixed Standard, with explicit fixed Problem-focused/Deeper configurations and deterministic gates. Learned selection and policy updates are **not implemented**, including provisional development mode. An explicit learned-mode/policy-load/update request returns a deferred/not-implemented error before execution. Do not ship a disabled learner, install training dependencies, create a policy store, or run policy compatibility/update machinery to enforce this boundary; a capability/configuration guard is sufficient. Other reference defaults are centralized in Section 16.

Read/write/creation controls must reach the actual service path, not just filter response objects after forbidden work happened. Where an upstream service cannot isolate flags for concurrent products/trials, use an isolated service/root or a documented supported configuration boundary rather than claiming independent controls.

Off transitions use the Feature effective-state rule: stop new submissions, pause pending operations, reconcile/drain or isolate in-flight effects, and publish a truthful effective status. A lost acknowledgment or unreachable worker is not proof that the old work stopped. Measured initialization rejects an unconfirmed off state that could change the trial's inputs. There is no policy-update commit path in the current release. Future commit-time permission/regime checks remain specified in Feature Section 14.2.

## 13. Snapshot and evaluation implementation [REFERENCE]

The simplest initial snapshot path may stop/quiesce owned writers and copy/export their consistent durable state. Verify actual ownership/process/job quiescence; an unreachable HTTP endpoint alone does not prove a service stopped. A transactional logical snapshot can instead capture a consistent version while live writers continue elsewhere. Exporting a remote index requires that backend's supported consistency/export path, not an assumed local directory copy.

Include the memory/procedure content and all receipts, approvals, current designations, tombstones, policies/representation configuration, and protected provenance dependencies needed for the capabilities claimed after restore. A partial diagnostic export is allowed when labeled partial; it is not a complete scored initial state. Pending operation state may be preserved without claiming it completed, but scored initialization obeys the stricter declared-background-work rule.

Namespace-bound restoration into a fresh root/remote partition is valid. To populate another trial, prepare a distinct namespace-bound seed before scoring or implement a tested identity/reference remapping. Verify content hashes before transforms and record any transformed identity manifest; never treat replacing a directory name as complete isolation.

Use the snapshot path on synthetic fixtures now. Future benchmark cohorts and any development-training cohorts belong to separately authorized later phases; do not create or execute them in this plan. The retained future protocol freezes inputs, records actual outage/degradation, and verifies the then-current external runner/rules. Current functional tests validate isolation/accounting mechanics, not comparative performance.

## 14. Source confirmation [REFERENCE]

The [Detailed ledger](PRODUCT_SPEC_DETAILED_v72.md) is the sole catalog of pinned source observations and remaining source-confirmation questions. Read it before adapting the corresponding subsystem, then verify the actual symbols and tests in the pinned checkout. No prose source observation should force a wrong call signature or a duplicate implementation.

## 15. Required verification [BINDING]

### 15.1 Execution protocol and scope

For functional development verification only, use the test topology and role matrix in Section 1. These are not deployment or benchmark-runtime requirements. Before a native test wave, the implementation harness launches a fresh heavyweight product-test ROOT with a bounded synthetic test brief, candidate build identity, selected middleweight alternative, explicit `apc_adaptation_binding`, allowed fixture/service partitions, and artifact/cleanup contract. It receives test-ROOT authority only for that isolated candidate test instance. It is a real launched delegation, not the implementation agent declaring itself an independent ROOT.

Product-test ROOT sets up the isolated candidate product harness and delegates routine checks to lightweight workers, substantive fixture/integration work to the selected middleweight, and consequential/complex work or step-closeout review to heavyweight. For important test closeouts, launch a separate heavyweight reviewer through the product harness; ROOT adjudicates its findings against actual receipts/artifacts. This does not add a universal review ceremony to every routine plan. The native campaign exercises Luna, the chosen middleweight alternative, and Sol via their specified CLIs, plus the separately configured APC binding; mock provider responses alone cannot establish this coverage.

Unit/contract/source tests can run in the implementation harness during development. To count as native product-harness coverage, retain the outer delegation receipt, product-test ROOT invocation, candidate entry point/build, inner instance/task/run/CLI identities, requested/resolved roles, actual results, product review/acceptance and cleanup status. Deliberately run at least one real near-match adaptation through the inner child resolved from `apc_adaptation_binding`; a successful fallback when that binding is missing, invalid, or unavailable is fallback evidence, not successful adaptation evidence.

Keep the candidate source/configuration stable during a wave. Product-test ROOT may mutate disposable fixture repositories as the tests require, but returns product-code defects to the outer implementation workflow. After a fix, identify the new candidate and rerun affected coverage. Before closing the outer ROOT task, archive inner evidence and close or explicitly reconcile every owned inner child/resource. Failures remain failures even if the outer author reports success.

**No benchmark execution is part of this protocol.** The allowed task set is purpose-built synthetic/disposable fixtures and relevant existing source tests, never benchmark tasks/subsets or B0–B3 scored comparisons. Do not schedule a benchmark as an automatic next step. Timing/native-usage checks validate functionality and budgets; they are not a benchmark study. Current acceptance cannot depend on learned-selector behavior or a performance gain.

### 15.2 Coverage and proportionality

Tests establish the Feature guarantees and Sections 1–2, 5–6, 10, and 12 contracts. They do not require the reference persistence/adapter/field names. Every current-phase row below is a coverage obligation for its relevant implemented capability; rows may share fixtures. Core requirements must be implemented/tested rather than skipped by leaving them disabled. Only design-selected optional mechanisms and supported environments are conditional. T21 tests the absence/non-activation of the deferred selector, not a learner implementation. Future selector coverage is separately labeled in Section 15.3 and is not a release gate. Unsupported environments are reported honestly, not counted as passing native coverage.

Use focused unit/contract tests for deterministic branches, source-adapter tests for upstream wires, fault injection for persistence boundaries, and representative real service/native tests for actual interoperability. Avoid an exhaustive product of providers/backends/scenarios when representative coverage plus source-specific tests proves the boundary. Each advertised provider/backend and current target host needs its relevant conformance evidence; Section 1.1 defines the required native tier coverage. Benchmark-host validation and benchmark cohorts are later work. Preserve applicable upstream tests; explain unavailable environments rather than silently dropping them.

| Test ID | Canonical home | Required scenario and oracle |
|---|---|---|
| T01 Baseline and workspace | Feature 2, 15–16; Implementation 1, 10 | All-off ordinary task path has no memory calls/context. Native synthetic baseline lifecycle succeeds without a benchmark. Compose pinned harness+suite twice; preserve both families and repository instructions, detect ownership conflicts before mutation and interrupted/restored composition before enhanced dispatch. |
| T02 Exact plan and task | Feature 6.4, 10; Implementation 2.3–2.4 | Preserve accepted same-objective plan; continue candidate review. Reject missing/stale/wrong-objective/wrong-route refs. Reject a valid packet attached to another task or execution base. An explicitly ROOT-revised plan can be accepted under its new exact identity. |
| T03 Delivery and launch | Feature 10–11 | No draft-as-authority. Distinguish finalized from dispatched. Bootstrap/resume validate the same envelope meaning. Mutations to any bound field fail. Lost launch acknowledgment reconciles existing run or blocks duplicate dispatch. Optional omission preserves mandatory plan and updates actual-delivery evidence. |
| T04 History and receipts | Feature 3–5 | Reviewed successes/failures and disproved hypotheses survive restart/retirement with references. Recent evidence remains until searchable representation is confirmed. Unlinked history is visibly unverified. Instruction-like case text stays evidence, not approved guidance. |
| T05 Generated/curated trust | Feature 5, 12.1 | Complete source provenance without approval remains ineligible. Resolve every generated source case/receipt exactly; missing/ambiguous/unreviewed IDs block approval. Explicit curated selection excludes unlisted governing skills. Approval never changes generated origin. Generated helper scripts are not auto-installed/executed. |
| T06 Content and compact payloads | Feature 10, 12.2 | Locally and remotely changed behavior/references/compatibility require a new approved revision. A skill that cannot fit coherently is omitted or uses a separately approved compact version, not truncated. Redaction cannot silently mutate approved meaning under the old digest. |
| T07 Scope and identity | Feature 12.1; Implementation 2.2, 5 | Cross-owner/project/app/namespace negatives cannot deliver. A project-A approval cannot authorize project B. A receiver validates issuer, exact content, and recipient authorization; an unresolvable origin-local path or self-asserted hash is insufficient. Equal revisions in distinct authorized partitions do not collide. |
| T08 Approval vs designation | Feature 12.3 | **Approval alone does not supersede anything.** Explicit designation changes only its target partition. Shared current changes are not claimed before durable remote acceptance; lost acknowledgment is uncertain and reconciled. Older delayed designation fails after a newer one; authorized rollback is a new operation. |
| T09 Revocation/withdrawal | Feature 12.3–12.4 | Multi-partition revision revocation blocks every tracked active copy before managed global completion; a stale/in-flight publish cannot restore it. Partition withdrawal is narrower and fences stale replay. Neither implicitly selects old history. Already-delivered exposure is recorded, not claimed erased; unsafe frozen-trial handling does not silently mutate inputs. |
| T10 Final eligibility/selection | Feature 8, 12.5–12.6 | Unknown hard requirements, malformed predicates, incompatible payloads, and revoked/superseded status fail. Round-trip predicate meaning is unchanged. No duplicate-storage relevance boost, competing revisions, or one-way/mutual conflict pair. Specificity/tie policy is deterministic and cannot broaden authorization. |
| T11 Live/frozen freshness | Feature 9, 12.5 | Revoke/change current after search and before use/final dispatch: reject affected guidance or return affected plan to ROOT. Unavailable live freshness cannot be bypassed by a cached shared copy or relabeling it local. An explicit frozen snapshot uses frozen eligibility and is labeled accordingly. An accepted current plan is not made unavailable merely because its historical template cannot be re-fetched. |
| T12 Templates and APC | Feature 6–7; Implementation 6 | Ship all five seed families; local template storage/search works in restricted-local mode and on every advertised template backend. Lower-ranked eligible candidate remains considered. Low/incomparable scores fresh-plan; forbidden fact sourcing, substitutions, removals/reordering/invariant edits are rejected. ROOT rejection is recorded as initial reuse→terminal fresh plan. |
| T13 Representation | Feature 6.2, 12.2; Implementation 6 | Same comparison uses the same sanitized query/fingerprint/metric across stores. Malformed/incompatible vectors do not pass via high hybrid scores. Changed behavior cannot reuse a stale projection. Reindexing unchanged approved content does not require a fictitious content change; frozen projection evidence remains reproducible. |
| T14 Route correction | Feature 7 | Ordinary work never auto-invokes topology. Known Level 0 corrects the route; late Level 0 makes the old preparation permanently non-dispatchable/non-learning. Both bounded re-prepare and no-memory continuation preserve exact state, lineage, fresh budget, and all spent cost. Repeated correction cannot reset budgets indefinitely. |
| T15 Search and stage budgets | Feature 6.3, 8–9 | Test Standard, real-problem query, no-problem fallback, bounded Deeper, and local-only Deeper. Slow stores have fair attempt opportunity and kind capacity cannot starve templates. Embedding/fetch/scoring delays count inside bounds. Fixed fallback cannot bypass reserve; unknown budget masks expensive stages. APC launch/queue/CLI/correction/collection/validation all share its enclosing limit. |
| T16 Feature matrix | Feature 14.1, 15; Implementation 12 | Parameterize each current switch and meaningful dependencies using the Feature oracle. Assert actual service-side suppression, not output filtering. Pending/in-flight writes exercise truthful transitioning/off state. Test deferred-selector enable/update requests fail without loading learner code. Admin safety respects restrictions; all-off bypasses implicit curated memory. |
| T17 Control credentials/authority | Feature 13.1 | Every product-managed arm's worker prompt/environment/tools lacks control credentials and endorsed trust/policy mutation. Prohibited mandatory secrets block without rewriting user truth. A validated task-only credential path works when present; its absence is reported, not replaced by an invented secret manager. No safe channel exception grants forbidden control authority. |
| T18 Content and egress privacy | Feature 13.2 | Known secret/confidential fixtures cover ingestion, case/skill/template generation, template-derived context, project/shared publication, query/embedding/adaptation requests, telemetry, and logs. Sanitize or omit derived content; retain protected authoritative source unchanged. Recipient authorization is checked; provider authentication stays in its intended transport, not prompt/query payloads. |
| T19 Outcome evidence | Feature 11, 17 | Wrong-run/task/decision review/acceptance cannot create an outcome. Missing evidence remains unobserved; genuine terminal unknown differs. Identical replay is idempotent; conflicts are explicit. Post-outcome retries complete missing enabled receipts/ingestion/telemetry, never learner updates. APC child completion is not parent coding acceptance. |
| T20 Faults and concurrency | Feature 10–11, 15–16; Implementation 1.2, 6.1 | Inject crashes before/after preparation, finalization, child/worker launch intent/ack, outcome fixation, side-effect phases, designation/revocation, and snapshot capture. Reconcile ambiguous unsafe effects; concurrent retries cannot duplicate/overwrite accepted work. Include adaptation-child timeout and product-test ROOT failure with owned-subtree cleanup. No selector commit path is implemented or tested now. |
| T21 Deferred selector boundary | Feature 1.1, 14.1; Implementation 12 | Fixed Standard/Problem-focused/Deeper selection works without policy files, learner dependencies or updates. Explicit requests for learned selection, provisional training, policy loading or updates return deferred/not implemented. Retain neutral strategy/outcome/usage facts; do not implement training, inference, persistence or replay merely to test their absence. |
| T22 Native usage completeness | Feature 17; Implementation 1.2, 6.1 | Count actual CLI invocations once across cumulative receipts/corrections/resumes. Missing/late usage remains incomplete. Include ROOT, reviewers and lightweight adaptation; link child cost once to parent adaptation. Keep outer implementation orchestration distinct from inner product execution and preserve both. Check accounting on synthetic fixtures, not benchmark comparisons. |
| T23 Snapshot/test isolation | Feature 16.2 | Synthetic restore includes required approval/receipt/revocation/representation dependencies and verifies integrity. Namespace-bound restore or tested remapping cannot contaminate other tests/production/outer harness. Pending state is explicit; superseded failures are not live blockers. No undeclared jobs mutate fixture inputs; future policy artifacts are not a current dependency. |
| T24 Network and future-evaluation readiness | Feature 13.3, 16 | On synthetic fixtures verify actual launched tools/egress for the claimed mode and no restricted-local Atlas task call. Outages remain reported. Validate configuration/snapshot/accounting metadata needed by future comparisons using fixtures only; do not run a benchmark or comparative cohort now. |
| T25 Native product-harness integration | Feature 1–17; Implementation 1, 15.1 | The outer harness launches heavyweight product-test ROOT; that ROOT launches real test workers through the isolated candidate product harness. Synthetic baseline/enhanced tasks exercise exact state → memory/APC or fallback → ROOT acceptance → product dispatch → linked outcome → durable memory/cleanup, including actual scoped Atlas Vector Search. Outer-only work or mock-only execution is not a native product pass. |
| T26 Harness APC child | Feature 6.3; Implementation 6.1 | In this development test, near-match adaptation invokes the actual candidate product-harness child using the run's explicit `apc_adaptation_binding`, with parent/template identity and permitted edits. Prove that two supported configured bindings can be selected without source changes, while no binding or an invalid binding takes the fresh-plan fallback and never inherits the lightweight-worker model. Its ROOT-approved drafting task does not require a falsely accepted parent plan. Capture requested/resolved binding, result/native usage, and cleanup; ROOT accepts/rejects the parent plan separately. Direct fill launches no adaptation child. Invalid/no-model/timeout paths fresh-plan without direct API fallback, recursion, parent-code mutation, false parent outcome, or leaked resource. |
| T27 Development-test harness ownership | Feature 2, 16.1; Implementation 1.2 | In the nested development test, outer and inner instances have disjoint owned state/resources and an explicit delegation chain. Assert test entry points resolve to the recorded candidate product build, not the coding harness. Inner cleanup/collision tests leave outer work untouched. Product-test ROOT failure exposes/reconciles its inner children before outer closeout; capacity checks prevent parent/child deadlock. A normal product-startup/record-contract fixture requires no outer test harness, delegated test ROOT, or outer-test lineage. |
| T28 Development-test bindings and deployment separation | Feature 1.1, 6.3; Implementation 1.1, 6.1, 15.1 | Native development tasks use the prescribed worker/ROOT/reviewer model/CLI/effort matrix, including heavyweight test ROOT/closeout review and the chosen middleweight alternative; record actual bindings and reject silent substitutions. Verify both alternatives only if both are claimed. APC independently honors the run's `apc_adaptation_binding` in development and deployment and has no named or inherited default. Review actual receipts/artifacts. Configuration/launch-contract fixtures may use provider stubs and do not claim additional native-model coverage or run benchmarks. |
| T29 No-benchmark release scope | Feature 1.1, 16.1; Implementation 18 | Current test manifest/driver contains only authorized synthetic/disposable and source test workloads; no benchmark tasks, benchmark subsets/runners, scored B0–B3 comparisons, leaderboard submissions or automatic post-closeout benchmark job. Deferred benchmarks/learner tests are marked future scope, not passing or missing current coverage. |

A code implementation may use fewer suites than rows. A fixture that asserts a reference field name, file layout, storage technology, or exact callback order is not a sufficient substitute for the stated property. New test failures are adjudicated against the canonical home, not automatically patched by expanding the spec.

### 15.3 Future selector verification [REFERENCE]

Retain these scenarios for a later selector implementation: provisional development cold start; compatible frozen deployment and fixed fallback; requested/effective action credit; correctness-dominant feedback; disabled/held-out update gates; late regime-A outcomes after a move to regime B; and concurrent/crash-replayed accepted observations appearing once in versioned history. These exercise Feature Section 14.2 only after that capability is separately authorized. They are not T21's current absence check, do not justify implementing a learner now, and do not authorize a benchmark run.

## 16. Starting configuration [REFERENCE]

This is the only numeric/default table in the document set. Use it to start implementation; record explicit development overrides and freeze the resolved settings for scored runs. Changes that preserve the Feature gates do not require user approval.

| Setting | Starting value or policy |
|---|---|
| Ordinary product features | Core memory/procedure/template/APC capabilities enabled when configured; no silent service substitutes |
| Search selection | Fixed Standard; explicit fixed Problem-focused/Deeper permitted under gates |
| Development-test worker/ROOT/reviewer model/CLI/effort bindings | Prescribed by Section 1.1 for development verification only; not deployment defaults |
| APC adaptation binding | Required `apc_adaptation_binding` in every environment; explicit supported model/provider/CLI/effort; no named, inherited, or silently substituted default |
| Deployment ROOT/worker/reviewer bindings | Explicit supported deployment configuration, independent of Section 1.1; no mandatory named test model or three-tier routing scheme |
| Current test workloads | Synthetic/disposable and source tests only; delegated product-harness subagents; no benchmark execution |
| Learned selection / policy updates | Not implemented in this phase; fixed search only and explicit enable/load/update requests rejected |
| Standard / Problem-focused outer stage | 8 seconds each |
| Deeper outer stage | 25 seconds; at most two local rounds and three follow-up queries in the follow-up round |
| Execution reserve | 120 seconds; positive in scored runs |
| Light-adaptation stage | 120 seconds total including harness/CLI startup and result collection; one lightweight support-child launch initially; result-artifact cap 6,000 estimated tokens, with native usage recorded separately |
| APC metric and thresholds | Raw cosine initially; direct-fill candidate ≥0.93, light-adapt candidate ≥0.87; incompatible/incomparable always rejected |
| Delivered cases / skills | Up to 4 / up to 2 |
| APC shortlist | Up to 8 templates, then at most one selected template |
| Optional context cap | At most 10% of known usable window and 12,000 estimated tokens; 8,000 when window is unknown, with mandatory-context/provider-limit handling retained |
| Late Level-0 automatic re-prepare | At most one; otherwise explicit no-memory continuation or ROOT recovery |
| Extra final-delivery optional freshness stage | At most 5 seconds total, still subject to the execution reserve; batch/concurrent reads where useful |
| Maintenance measurement cutoff | 120 seconds after reviewed flush/commit, or earlier confirmed job completion; pending/unobservable jobs remain incomplete |
| Network profile | Soft guardrail when permitted; restricted-local when live Atlas is forbidden/disabled; strongest label only with verified enforcement |

The context/adaptation/cutoff values are implementation starting choices, not measured optima. The adaptation allowance now covers a real launched CLI child rather than an assumed direct API request. Verify feasibility with synthetic smoke tasks and adjust before a test wave when needed, without running benchmarks. The output cap governs the returned artifact; use provider hard token limits only when supported. Harness deadlines, child-attempt/correction bounds and ownership remain enforceable even when the CLI does not expose a hard native-token ceiling. A cheap deterministic directly filled plan still requires normal ROOT acceptance. Configure finite per-service timeouts below the enclosing stage bound; the outer deadline remains authoritative. SDK connection/socket timeouts alone are not proof of end-to-end bounded latency.

## 17. Operator surface and implementation order [REFERENCE]

Provide operations equivalent to setup/readiness, prepare/finalize, normal harness dispatch with control-credential scrubbing, record/reconcile outcome, procedure sync/approval/designation/publication/withdrawal/revocation, pending-operation inspection/retry, seed/repair templates, snapshot import/export, and native usage ingestion where needed. Return actionable status, evidence references, and next step without secrets. Choose concrete command names once automation depends on them.

Build a thin executable vertical slice first: baseline host lifecycle and isolated candidate deployment; exact task/plan handoff; reviewed local experience; scoped Atlas retrieval; direct-fill/fresh fallback and a real harness adaptation child selected through `apc_adaptation_binding`; outcomes, reconciliation and native usage. Then complete all remaining core functions, switches and fault coverage through the delegated product-test ROOT protocol. Within the development-test workflow, heavyweight ROOT reviews important closeouts from actual evidence. Stop this implementation plan before the learned selector: leave its future design documented, with no learner code. Do not add benchmark setup/runs, scored comparison stages or automatic follow-up jobs. This sequence does not authorize claiming completion while a required fixed-strategy capability is missing.

## 18. Current-phase release evidence [BINDING]

Current development-verification release claims cite actual evidence against the pinned candidate source/configuration, the test-role bindings, and the explicit APC binding in Section 1.1. These evidence requirements do not become fixed role assignments for the deployed product; an APC binding remains required runtime configuration rather than a named model requirement. Complete the applicable current-phase Section 15 coverage for the fixed-strategy product. Record unit/contract, source-adapter, service, native model/provider/backend/host, nested product-harness and fault results. Unavailable required native bindings/environments are limitations or blockers, never passing coverage or permission for silent substitutions.

Acceptance of this development work requires evidence from the candidate product harness under a heavyweight product-test ROOT launched by the implementation harness, including inner test workers, actual APC adaptation through the configured binding, heavyweight closeout review, linked outcomes and owned cleanup. An outer coding-harness test run alone does not satisfy this. Preserve the ordinary all-off baseline and trust/recovery boundaries; an Atlas-backed demonstration uses actual Vector Search on synthetic scoped data.

Later benchmark performance evaluation uses the ordinary deployed product with its own declared model/provider/effort and runner configuration; it does not require this development-test matrix or the outer/inner test arrangement.

**No benchmark run, benchmark score, scored B0–B3 comparison, performance-gain demonstration or learned-selector implementation is required or authorized for this release.** Explicitly record `benchmark execution: deferred/not run` and `learned selector: deferred/not implemented`, separately from failed/skipped current test coverage. Do not execute deferred work as an automatic post-release or background step. Generic snapshot, budget and usage support is tested with synthetic fixtures now; the future evaluation/selector designs remain documented for later development.

Do not claim universal correctness, perfect security, independent review where only self-review occurred, or absence of undiscovered defects from this evidence.

## 19. Scope and change control [REFERENCE]

Feature Section 19 is the only non-goal list; Section 0 here is the only engineering escalation framework. Update the single implemented contract and relevant source note when an ordinary compatibility fix is needed. Do not create another normative checklist or restart open-ended review merely because a helper, serialization, or locking primitive changed.
