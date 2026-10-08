"""Durable, worktree-independent native evidence for one enhanced parent review.

The lane-1 outcome transaction can read this record after worktree retirement.
This module validates the supplied evidence; it does not fix an outcome or run
follow-on effects. Missing evidence returns ``None`` from the reader, while a
present but contradictory record raises ``TerminalEvidenceError``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .core import content_hash, read_json, sha256_hex
from .epochs import lane_record_dir
from .records import atomic_write_json, RecordLock

TERMINAL_EVIDENCE_SCHEMA = "native-terminal-evidence/v1"
TERMINAL_EVIDENCE_NAME = "NATIVE_TERMINAL_EVIDENCE.json"
REJECTED_ATTEMPT_NAME = "REJECTED_NATIVE_ATTEMPT.json"
ACCEPTED_OUTCOME_NAME = "NATIVE_OUTCOME_RECORDED.json"


class TerminalEvidenceError(ValueError):
    """A present native terminal evidence record is invalid or contradictory."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TerminalEvidenceError(message)


def _hashed(record: Any, schema: str, name: str) -> dict[str, Any]:
    _require(isinstance(record, dict), f"{name} is not an object")
    _require(record.get("schema") == schema, f"{name} schema mismatch")
    _require(record.get("content_hash") == content_hash(record), f"{name} hash mismatch")
    return record


def run_publication_dir(rt: Path, epoch_id: str, lane_id: str, run_id: str) -> Path:
    """Non-conflicting publication slot for a resumed run."""
    return lane_record_dir(rt, epoch_id, lane_id) / "review-runs" / sha256_hex(run_id)


def publication_dir(rt: Path, epoch_id: str, lane: Mapping[str, Any]) -> Path:
    """Keep the inherited root slot for the first run and legacy lanes."""
    if lane.get("memory_plan_state") == "execution_accepted" and lane.get("resume_from_run_id"):
        return run_publication_dir(rt, epoch_id, str(lane["lane_id"]), str(lane["run_id"]))
    return lane_record_dir(rt, epoch_id, str(lane["lane_id"]))


def record_domain_review(
    rt: Path, epoch_id: str, lane: Mapping[str, Any], evidence: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Replay the domain transaction and retain only its returned rejection ID."""
    from . import memory_handoff

    validate_terminal_evidence(evidence, lane_id=lane["lane_id"], run_id=lane["run_id"])
    _require(evidence["epoch_id"] == epoch_id, "terminal evidence epoch mismatch")
    folder = publication_dir(rt, epoch_id, lane)
    root_folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    if not (folder / TERMINAL_EVIDENCE_NAME).is_file() and (root_folder / TERMINAL_EVIDENCE_NAME).is_file():
        _require(read_json(root_folder / TERMINAL_EVIDENCE_NAME) == dict(evidence),
                 "retained root native evidence differs from the reviewed run")
        folder = root_folder
    rejected = evidence["acceptance"]["approval"] == "REJECTED"
    path = folder / (REJECTED_ATTEMPT_NAME if rejected else ACCEPTED_OUTCOME_NAME)
    if not Path(lane["worktree_path"]).is_dir():
        _require(lane.get("lifecycle") == "retired" and path.is_file(),
                 "domain review cannot be recovered without its worktree")
        retained = read_json(path)
        _require(retained.get("schema") == (
            "rejected-native-authorization/v1" if rejected else "native-outcome-recorded/v1"
        ) and retained.get("content_hash") == content_hash(retained)
                 and isinstance(retained.get("rejected_attempt_id" if rejected else "outcome_id"), str)
                 and bool(retained["rejected_attempt_id" if rejected else "outcome_id"])
                 and retained.get("run_id") == evidence["run_id"]
                 and retained.get("decision_id") == evidence["decision_id"]
                 and retained.get("evidence_digest") == evidence["content_hash"],
                 "retained domain review provenance mismatch")
        return retained if rejected else None
    result = memory_handoff.record_native_review(
        worktree_path=lane["worktree_path"], evidence=evidence,
    )
    authorization = ({
        "schema": "rejected-native-authorization/v1",
        "rejected_attempt_id": result["rejected_attempt_id"],
        "run_id": evidence["run_id"],
        "decision_id": evidence["decision_id"],
        "operation_id": evidence["dispatch"]["operation"]["operation_id"],
        "evidence_digest": evidence["content_hash"],
    } if rejected else {
        "schema": "native-outcome-recorded/v1",
        "outcome_id": result["outcome_id"],
        "run_id": evidence["run_id"],
        "decision_id": evidence["decision_id"],
        "evidence_digest": evidence["content_hash"],
    })
    authorization["content_hash"] = content_hash(authorization)
    with RecordLock(path):
        if path.exists():
            _require(read_json(path) == authorization, "rejected attempt authorization conflicts")
        else:
            atomic_write_json(path, authorization)
    return authorization if rejected else None


def _validate_retained_sources(
    evidence: dict[str, Any], card: dict[str, Any], plan: dict[str, Any],
    lane: str, run: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Recheck source meaning without opening the retired worktree or store."""
    from memory_harness import contracts
    from . import memory_handoff

    decision = _hashed(evidence.get("decision"), contracts.DECISION_SCHEMA, "decision")
    _require(isinstance(decision.get("created_at"), str) and bool(decision["created_at"]), "decision creation time missing")
    dispatch = evidence.get("dispatch")
    _require(isinstance(dispatch, dict), "dispatch evidence missing")
    envelope = _hashed(dispatch.get("envelope"), contracts.FINAL_ENVELOPE_SCHEMA, "dispatch envelope")
    context = _hashed(evidence.get("final_context"), contracts.FINAL_CONTEXT_SCHEMA, "final context")
    configuration = evidence.get("configuration")
    _require(isinstance(configuration, dict), "resolved configuration missing")
    _require(configuration == decision.get("configuration") == envelope.get("configuration") == context.get("configuration"), "resolved configuration mismatch")
    _require(evidence.get("configuration_digest") == decision.get("configuration_digest") == envelope.get("configuration_digest") == context.get("configuration_digest") == sha256_hex(configuration), "configuration digest mismatch")
    expected_decision = sha256_hex({
        "task_card_digest": card["content_hash"],
        "objective_id": plan["objective_id"], "route": plan["route"],
        "plan_id": plan["plan_id"], "strategy": envelope.get("strategy"),
        "configuration_digest": envelope.get("configuration_digest"),
    })
    _require(decision.get("decision_id") == evidence.get("decision_id") == envelope.get("decision_id") == context.get("decision_id") == expected_decision, "decision identity mismatch")
    for field, value in (
        ("task_card_digest", card["content_hash"]), ("objective_id", plan["objective_id"]),
        ("route", plan["route"]), ("plan_id", plan["plan_id"]),
        ("plan_state", plan["state"]), ("plan_digest", plan["content_hash"]),
        ("strategy", envelope.get("strategy")),
    ):
        _require(decision.get(field) == value, f"decision {field} mismatch")
    _require(decision.get("state") == "prepared", "decision is not prepared")
    _require(envelope.get("lane_id") == lane and envelope.get("run_id") == run, "envelope lane or run mismatch")
    _require(context.get("strategy") == envelope.get("strategy"), "final context strategy mismatch")
    _require(isinstance(envelope.get("delivery"), dict), "envelope delivery missing")
    _require(context.get("omitted") == envelope.get("delivery", {}).get("omitted"), "final context omissions mismatch")
    _require(isinstance(envelope.get("worktree_path"), str) and bool(envelope["worktree_path"]), "envelope worktree missing")
    _require(envelope.get("worktree_path") == context.get("worktree_path"), "final context worktree mismatch")
    _require(dispatch.get("envelope_digest") == envelope["content_hash"], "dispatch envelope digest mismatch")
    try:
        contracts.validate_decision(decision)
        memory_handoff.validate_envelope_for_launch(
            envelope=envelope, task_card=card, lane_id=lane, run_id=run,
            worktree_path=envelope["worktree_path"], base_commit=card["base_commit"],
        )
        memory_handoff.validate_final_context_for_launch(
            context=context, envelope=envelope, task_card=card,
            lane_id=lane, run_id=run, worktree_path=envelope["worktree_path"],
            base_commit=card["base_commit"],
        )
        rebuilt = contracts.make_finalized_context(
            lane_id=lane, run_id=run, decision_id=decision["decision_id"],
            task=card["task"],
            task_card_digest=card["content_hash"], objective_id=plan["objective_id"],
            route=plan["route"], plan_id=plan["plan_id"],
            plan_revision=plan["revision"], accepted_by=plan["accepted_by"],
            accepted_plan_content=plan["content"],
            plan_digest=plan["content_hash"], base_commit=card["base_commit"],
            worktree_path=envelope["worktree_path"], strategy=envelope["strategy"],
            checkpoint=envelope["checkpoint"], configuration=configuration,
            execution_role=envelope["execution_role"],
            invocation_target=envelope["invocation_target"], recipient=envelope["recipient"],
            mandatory_content=envelope["mandatory_content"],
            optional_content=envelope["optional_content"],
            delivery_trace=envelope["delivery_trace"],
            role_separation=context["role_separation"], freshness=context["freshness"],
            context_limit=context["context_limit"], context_id=context["context_id"],
            created_at=context["created_at"],
        )
    except Exception as exc:
        raise TerminalEvidenceError(f"retained decision, envelope, or context is invalid: {exc}") from exc
    _require(context == rebuilt, "final context rebuilt meaning mismatch")
    return envelope, context


def _validate_terminal_evidence(
    record: Mapping[str, Any], *, lane_id: str | None = None, run_id: str | None = None,
) -> None:
    """Validate v1 structure, hashes, and exact internal source links.

    This checks the self-contained durable record. The reader additionally
    checks that its sibling review and acceptance files still match.
    """
    evidence = _hashed(record, TERMINAL_EVIDENCE_SCHEMA, "terminal evidence")
    lane = evidence.get("lane_id")
    run = evidence.get("run_id")
    _require(isinstance(evidence.get("epoch_id"), str) and bool(evidence["epoch_id"]), "epoch identity missing")
    _require(isinstance(lane, str) and bool(lane), "lane identity missing")
    _require(isinstance(run, str) and bool(run), "run identity missing")
    _require(lane_id is None or lane == lane_id, "terminal evidence lane mismatch")
    _require(run_id is None or run == run_id, "terminal evidence run mismatch")

    card = _hashed(evidence.get("task_card"), "project-task-card/v1", "task card")
    plan = _hashed(evidence.get("accepted_plan"), "memory-plan/v1", "accepted plan")
    handoff = card.get("memory_handoff")
    _require(isinstance(card.get("task"), str) and bool(card["task"].strip()), "parent task missing")
    _require(isinstance(handoff, dict) and handoff.get("plan_state") == "execution_accepted", "not an accepted parent handoff")
    _require(handoff.get("plan") == plan, "accepted plan does not match task card")
    _require(plan.get("state") == "accepted" and plan.get("accepted_by") == "ROOT", "plan lacks ROOT acceptance")
    objective = evidence.get("objective_id")
    decision = evidence.get("decision_id")
    _require(isinstance(objective, str) and bool(objective), "objective identity missing")
    _require(isinstance(decision, str) and bool(decision), "decision identity missing")
    _require(handoff.get("objective_id") == objective == plan.get("objective_id"), "objective mismatch")
    _require(handoff.get("route") == plan.get("route"), "route mismatch")

    envelope, context = _validate_retained_sources(evidence, card, plan, lane, run)

    configuration = evidence.get("configuration")
    _require(isinstance(configuration, dict), "resolved configuration missing")
    configuration_digest = evidence.get("configuration_digest")
    _require(configuration_digest == sha256_hex(configuration), "configuration digest mismatch")
    dispatch = evidence.get("dispatch")
    _require(isinstance(dispatch, dict), "dispatch evidence missing")
    operation = dispatch.get("operation")
    _require(isinstance(operation, dict), "dispatch operation missing")
    _require(set(operation) == {
        "operation_id", "decision_id", "envelope_digest", "run_id", "kind",
        "status", "observed_invocation", "created_at", "updated_at",
    }, "dispatch operation fields malformed")
    for field in ("created_at", "updated_at"):
        _require(isinstance(operation.get(field), str) and bool(operation[field]), f"dispatch operation {field} missing")
    _require(dispatch.get("operation_digest") == sha256_hex(operation), "dispatch receipt digest mismatch")
    observed = dispatch.get("observed_invocation")
    _require(isinstance(observed, dict), "native invocation missing")
    _require(operation.get("kind") == "dispatch" and operation.get("status") == "delivered", "dispatch was not delivered")
    _require(operation.get("observed_invocation") == observed, "operation invocation mismatch")
    envelope_digest = dispatch.get("envelope_digest")
    _require(isinstance(envelope_digest, str) and bool(envelope_digest), "envelope digest missing")
    _require(operation.get("envelope_digest") == envelope_digest, "dispatch envelope mismatch")
    _require(operation.get("operation_id") == sha256_hex({"kind": "dispatch", "envelope_digest": envelope_digest}), "dispatch operation identity mismatch")
    _require(operation.get("decision_id") == decision and operation.get("run_id") == run, "dispatch decision or run mismatch")
    expected_observation = {
        "task_card_digest": card["content_hash"],
        "decision_id": decision,
        "plan_id": plan.get("plan_id"),
        "plan_digest": plan["content_hash"],
        "envelope_digest": envelope_digest,
        "lane_id": lane,
        "run_id": run,
        "base_commit": card.get("base_commit"),
        "route": plan.get("route"),
        "configuration_digest": configuration_digest,
    }
    for field, value in expected_observation.items():
        _require(observed.get(field) == value, f"native observation {field} mismatch")
    _require(observed.get("context_id") == context["context_id"] and observed.get("context_digest") == context["content_hash"], "native final context mismatch")
    _require(isinstance(observed.get("context_id"), str) and bool(observed["context_id"]), "native context identity missing")
    _require(isinstance(observed.get("context_digest"), str) and bool(observed["context_digest"]), "native context digest missing")
    pid = observed.get("pid")
    creation = observed.get("creation_time")
    _require(isinstance(pid, int) and not isinstance(pid, bool) and pid > 0, "native process ID missing")
    _require(isinstance(creation, str) and bool(creation), "native creation time missing")
    _require(observed.get("invocation_id") == f"controller:{pid}:{creation}", "native invocation identity mismatch")

    review = _hashed(evidence.get("review"), "completion-review/v1", "review")
    acceptance = _hashed(evidence.get("acceptance"), "orchestrator-acceptance/v1", "acceptance")
    _require(review.get("lane_id") == lane == acceptance.get("lane_id"), "review lane mismatch")
    _require(review.get("run_id") == run == acceptance.get("run_id"), "review run mismatch")
    _require(acceptance.get("review_ref") == review["content_hash"], "acceptance review link mismatch")
    _require(acceptance.get("accepted_by") == "ROOT", "ROOT acceptance identity missing")
    _require(acceptance.get("approval") in {"ACCEPTED", "REJECTED"}, "approval invalid")
    task_card_id = str(card.get("card_id") or card.get("id") or card["content_hash"])
    for field, value in (
        ("task_card_id", task_card_id), ("task_card_hash", card["content_hash"]),
    ):
        _require(review.get(field) == value == acceptance.get(field), f"{field} mismatch")
    for field in ("result_id", "result_hash", "commit"):
        _require(review.get(field) == acceptance.get(field), f"{field} mismatch")
    _require(isinstance(review.get("commit"), str) and bool(review["commit"]), "review commit missing")

    result = evidence.get("result")
    proof = evidence.get("terminal_proof")
    outcome = review.get("review_outcome")
    if outcome == "UNKNOWN":
        _require(result is None, "UNKNOWN cannot carry a result")
        _require(review.get("result_id") is None and review.get("result_hash") is None, "UNKNOWN has a result identity")
        _require(acceptance.get("approval") == "ACCEPTED", "UNKNOWN lacks exceptional ROOT acceptance")
        reason = acceptance.get("force_accept_reason")
        _require(isinstance(reason, str) and bool(reason.strip()), "UNKNOWN lacks exceptional acceptance reason")
        _require(isinstance(proof, dict) and proof.get("schema") == "controller-status/v1", "UNKNOWN lacks controller proof")
        _require(proof.get("lane_id") == lane and proof.get("run_id") == run, "controller proof run mismatch")
        _require(proof.get("controller_state") == "exited", "controller exit unproven")
        _require(isinstance(proof.get("provider_state"), dict) and proof["provider_state"].get("state") == "exited", "provider exit unproven")
        _require(proof.get("result_state") in {"absent", "invalid"}, "no-result state unproven")
        _require(proof.get("recorded_status") == "provider_exited_no_result", "exact no-result status unproven")
        _require(proof.get("cleanup_proven") is True, "cleanup unproven")
        proof_digest = sha256_hex(proof)
        _require(review.get("terminal_proof_digest") == proof_digest == acceptance.get("terminal_proof_digest"), "controller proof digest mismatch")
    else:
        _require(outcome in {"PASS", "FAIL", "BLOCKED"}, "review outcome invalid")
        result = _hashed(result, "result/v1", "result")
        _require(result.get("lane_id") == lane and result.get("run_id") == run, "result run mismatch")
        _require(result.get("outcome") in {"PASS", "FAIL", "BLOCKED"}, "result outcome invalid")
        _require(review.get("result_id") == run and review.get("result_hash") == result["content_hash"], "review result link mismatch")
        _require(proof is None, "ordinary result carries UNKNOWN proof")
        _require("terminal_proof_digest" not in review and "terminal_proof_digest" not in acceptance, "ordinary result carries UNKNOWN digest")
        if acceptance["approval"] == "ACCEPTED" and outcome != "PASS":
            reason = acceptance.get("force_accept_reason")
            _require(isinstance(reason, str) and bool(reason.strip()), "forced acceptance reason missing")


def validate_terminal_evidence(
    record: Mapping[str, Any], *, lane_id: str | None = None, run_id: str | None = None,
) -> None:
    """Validate retained evidence, including malformed nested source records."""
    try:
        _validate_terminal_evidence(record, lane_id=lane_id, run_id=run_id)
    except TerminalEvidenceError:
        raise
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        raise TerminalEvidenceError(f"malformed native terminal evidence: {exc}") from exc


def make_terminal_evidence(
    *, epoch_id: str, lane_id: str, run_id: str, task_card: dict[str, Any],
    accepted_plan: dict[str, Any], objective_id: str, decision_id: str,
    decision: dict[str, Any], envelope: dict[str, Any], final_context: dict[str, Any],
    dispatch_operation: dict[str, Any], envelope_digest: str,
    observed_invocation: dict[str, Any], configuration: dict[str, Any],
    configuration_digest: str, result: dict[str, Any] | None,
    review: dict[str, Any], acceptance: dict[str, Any],
    terminal_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    record = {
        "schema": TERMINAL_EVIDENCE_SCHEMA,
        "epoch_id": epoch_id, "lane_id": lane_id, "run_id": run_id,
        "task_card": task_card, "objective_id": objective_id,
        "decision_id": decision_id, "decision": decision,
        "accepted_plan": accepted_plan, "final_context": final_context,
        "dispatch": {
            "operation": dispatch_operation,
            "operation_digest": sha256_hex(dispatch_operation),
            "envelope_digest": envelope_digest, "envelope": envelope,
            "observed_invocation": observed_invocation,
        },
        "configuration": configuration,
        "configuration_digest": configuration_digest,
        "result": result, "review": review, "acceptance": acceptance,
        "terminal_proof": terminal_proof,
    }
    record["content_hash"] = content_hash(record)
    validate_terminal_evidence(record, lane_id=lane_id, run_id=run_id)
    return record


def read_terminal_evidence(
    rt: Path, epoch_id: str, lane_id: str, *, run_id: str | None = None,
) -> dict[str, Any] | None:
    """Read one retained lane record, or ``None`` when it was never emitted."""
    folder = lane_record_dir(rt, epoch_id, lane_id)
    scoped = run_publication_dir(rt, epoch_id, lane_id, run_id) if run_id is not None else None
    if scoped is not None and (scoped / TERMINAL_EVIDENCE_NAME).exists():
        folder = scoped
    elif run_id is not None and (folder / "COMPLETION_REVIEW.json").is_file():
        try:
            root_review = read_json(folder / "COMPLETION_REVIEW.json")
        except (OSError, ValueError) as exc:
            raise TerminalEvidenceError(f"cannot inspect root review slot: {exc}") from exc
        if root_review.get("run_id") != run_id:
            root_evidence = folder / TERMINAL_EVIDENCE_NAME
            if not root_evidence.is_file():
                return None
            try:
                claimed_run = read_json(root_evidence).get("run_id")
            except (OSError, ValueError) as exc:
                raise TerminalEvidenceError(f"cannot inspect root evidence slot: {exc}") from exc
            if claimed_run != run_id:
                return None
    path = folder / TERMINAL_EVIDENCE_NAME
    if not path.exists():
        return None
    try:
        record = read_json(path)
        validate_terminal_evidence(record, lane_id=lane_id, run_id=run_id)
        _require(record["epoch_id"] == epoch_id, "terminal evidence epoch mismatch")
        for name, field in (
            ("COMPLETION_REVIEW.json", "review"),
            ("ORCHESTRATOR_ACCEPTANCE.json", "acceptance"),
        ):
            sibling = read_json(folder / name)
            _require(sibling == record[field], f"{name} conflicts with terminal evidence")
        if scoped is not None and folder == scoped:
            validate_root_siblings(rt, epoch_id, lane_id, run_id, record)
    except (OSError, ValueError) as exc:
        if isinstance(exc, TerminalEvidenceError):
            raise
        raise TerminalEvidenceError(f"cannot validate terminal evidence at {path}: {exc}") from exc
    return record


def validate_root_siblings(
    rt: Path, epoch_id: str, lane_id: str, run_id: str, record: dict[str, Any],
) -> None:
    """Reject contradictory root publications for the same scoped run."""
    root = lane_record_dir(rt, epoch_id, lane_id)
    for name, field, schema in (
        ("COMPLETION_REVIEW.json", "review", "completion-review/v1"),
        ("ORCHESTRATOR_ACCEPTANCE.json", "acceptance", "orchestrator-acceptance/v1"),
        (TERMINAL_EVIDENCE_NAME, None, TERMINAL_EVIDENCE_SCHEMA),
    ):
        root_path = root / name
        if not root_path.exists():
            continue
        try:
            historical = read_json(root_path)
        except (OSError, ValueError) as exc:
            raise TerminalEvidenceError(f"cannot inspect root {name}: {exc}") from exc
        _require(isinstance(historical, dict), f"root {name} is not an object")
        if historical.get("run_id") != run_id:
            continue
        _hashed(historical, schema, f"root {name}")
        if field is None:
            validate_terminal_evidence(historical, lane_id=lane_id, run_id=run_id)
        _require(historical == (record if field is None else record[field]),
                 f"root {name} conflicts with scoped terminal evidence")
