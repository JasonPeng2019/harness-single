"""Trusted procedure lifecycle coordination.

The module owns product policy around immutable procedure evidence.  It does
not create a scheduler, review system, or launcher: callers supply their
trusted approval and, when enabled, a narrow Atlas adapter.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable, Mapping

from . import atlas, contracts
from .config import MemoryConfig, NetworkResolution, effect_submission_enabled
from .privacy import (
    PrivacyPolicy,
    RemotePayloadPrivacyError,
    guard_remote_payload,
    guard_worker_bound_remote,
    sanitize_payload,
)
from .store import MemoryStore, OperationConflictError, ProcedureConflictError, StoreError


class ProcedureError(ValueError):
    """A trusted-procedure lifecycle request is invalid or unsafe."""


class ProcedureAuthorizationError(ProcedureError):
    """A caller or receiver lacks configured procedure authority."""


class ProcedureRemoteAmbiguityError(ProcedureError):
    """A remote operation must be exact-read before it can be retried."""


class ProcedureIneligibleError(ProcedureError):
    """A discovered procedure did not pass authoritative delivery checks."""


class ProcedureNetworkDeniedError(ProcedureIneligibleError):
    """The captured effective profile forbids optional Atlas task work."""


class TrustedProcedureService:
    """Coordinate local approval, designation, and durable procedure state.

    Trusted issuer configuration is intentionally explicit and fail-closed.
    Approval evidence is stored verbatim and checked again by remote receivers;
    a candidate-supplied issuer string is never treated as authority by itself.
    """

    def __init__(
        self,
        memory_store: MemoryStore,
        *,
        trusted_issuers: Iterable[str],
        privacy_policy: PrivacyPolicy | None = None,
    ) -> None:
        issuers = frozenset(
            issuer for issuer in trusted_issuers if isinstance(issuer, str) and issuer
        )
        if not issuers:
            raise ProcedureAuthorizationError("trusted procedure issuers must be configured")
        self.store = memory_store
        self.trusted_issuers = issuers
        self.privacy_policy = privacy_policy or PrivacyPolicy()

    def _require_trusted_issuer(self, issuer: Any) -> str:
        if not isinstance(issuer, str) or not issuer:
            raise ProcedureAuthorizationError("procedure issuer must be a nonempty identity")
        if issuer not in self.trusted_issuers:
            raise ProcedureAuthorizationError("procedure issuer is not trusted by this deployment")
        return issuer

    @staticmethod
    def _require_task_network(
        network_resolution: NetworkResolution | Mapping[str, Any] | None,
    ) -> None:
        if not atlas.atlas_task_network_allowed(network_resolution):
            raise ProcedureNetworkDeniedError("effective restricted_local forbids Atlas task work")

    @staticmethod
    def _publication_admission(
        network_resolution: NetworkResolution | Mapping[str, Any] | None,
        shared_publication_enabled: bool,
        effects: list[dict[str, Any]],
        operations: list[dict[str, Any]],
        publication: Mapping[str, Any] | None = None,
    ) -> bool:
        """Admit new task work, or exact-only recovery of one submitted effect.

        The returned value permits a new claim. Submitted or confirmed effects
        with unfinished local acknowledgement may be read while off; absence
        must not turn that read into a retry.
        """
        new_work_allowed = (atlas.atlas_task_network_allowed(network_resolution)
                            and shared_publication_enabled)
        submitted = (len(effects) == 1 and effects[0]["status"] in {"uncertain", "in_flight"})
        unresolved = {"intent", "ambiguous", "remote_committed"}
        local_states = unresolved | {"acknowledged"}
        confirmed_local = (
            len(effects) == 1 and effects[0]["status"] == "confirmed"
            and publication is not None and len(operations) == 1
            and operations[0]["kind"] == "publication"
            and operations[0]["payload_id"] == effects[0]["source_id"]
            and publication["publication_id"] == effects[0]["source_id"]
            and publication["status"] in local_states
            and operations[0]["status"] in local_states
            and (publication["status"] in unresolved or operations[0]["status"] in unresolved)
        )
        legacy_submitted = (not effects and len(operations) == 1
                            and operations[0]["kind"] == "publication"
                            and operations[0]["status"] in {"ambiguous", "remote_committed"})
        if not new_work_allowed and not (submitted or confirmed_local or legacy_submitted):
            TrustedProcedureService._require_task_network(network_resolution)
            raise ProcedureIneligibleError("shared publication feature is off")
        return new_work_allowed

    def record_approved_revision(
        self,
        procedure: Mapping[str, Any],
        approval: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist an approved immutable revision without changing current state."""

        try:
            contracts.validate_procedure_revision(procedure)
            contracts.validate_procedure_approval(approval, procedure=procedure)
        except contracts.ContractError as exc:
            raise ProcedureError("procedure approval does not bind exact content") from exc
        self._require_trusted_issuer(approval["issuer"])
        try:
            persisted_procedure = self.store.record_procedure_revision(procedure)
            return self.store.record_procedure_approval(
                contracts.make_procedure_approval(
                    approval_id=approval["approval_id"],
                    procedure=persisted_procedure,
                    issuer=approval["issuer"],
                    recipients=approval["recipients"],
                    authority_evidence=approval["authority_evidence"],
                    approved_at=approval["approved_at"],
                )
            )
        except (contracts.ContractError, ProcedureConflictError) as exc:
            raise ProcedureError("could not durably record approved procedure revision") from exc

    def record_representation(
        self, representation: Mapping[str, Any]
    ) -> dict[str, Any]:
        try:
            return self.store.record_procedure_representation(representation)
        except (contracts.ContractError, ProcedureConflictError) as exc:
            raise ProcedureError("representation does not bind durable procedure content") from exc

    def designate(
        self,
        *,
        procedure: Mapping[str, Any],
        approval: Mapping[str, Any],
        partition: Mapping[str, Any],
        issuer: str,
    ) -> dict[str, Any]:
        """Explicitly select one revision as local current for one partition."""

        selected_issuer = self._require_trusted_issuer(issuer)
        self.record_approved_revision(procedure, approval)
        current = self.store.get_current_procedure_designation(
            str(procedure["logical_id"]), partition
        )
        if current is not None and current["state"] == "active":
            try:
                active_designation = self.store.get_procedure_designation(
                    str(current["designation_id"])
                )
            except StoreError as exc:
                raise ProcedureError("current procedure designation is not durable") from exc
            normalized_partition = contracts.normalize_procedure_partition(partition)
            if (
                active_designation["revision_id"] == procedure["revision_id"]
                and active_designation["procedure_digest"] == procedure["content_hash"]
                and active_designation["approval_id"] == approval["approval_id"]
                and active_designation["approval_digest"] == approval["content_hash"]
                and active_designation["partition"] == normalized_partition
                and active_designation["issuer"] == selected_issuer
            ):
                # The exact local current operation survived a caller crash;
                # return it instead of allocating a meaningless successor.
                return active_designation
        generation = 1 if current is None else int(current["generation"]) + 1
        predecessor = None if current is None else int(current["generation"])
        try:
            designation = contracts.make_procedure_designation(
                procedure=procedure,
                approval=approval,
                partition=partition,
                generation=generation,
                predecessor_generation=predecessor,
                issuer=selected_issuer,
            )
            return self.store.record_procedure_designation(designation)
        except (contracts.ContractError, ProcedureConflictError) as exc:
            raise ProcedureError("could not record ordered procedure designation") from exc

    def procedure_from_generated_skill(
        self,
        *,
        candidate_id: str,
        skill_approval_id: str,
        logical_name: str,
        references: Iterable[Mapping[str, Any]],
        predicates: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a revision that retains exact Step-02 candidate provenance."""

        try:
            candidate = self.store.get_generated_skill_candidate(candidate_id)
            source_approval = self.store.get_skill_approval(skill_approval_id)
        except StoreError as exc:
            raise ProcedureError("generated procedure source cannot resolve Step-02 approval") from exc
        if source_approval["candidate_id"] != candidate["candidate_id"]:
            raise ProcedureError("generated procedure source approval names another candidate")
        source = {
            "kind": "generated_skill",
            "candidate_id": candidate["candidate_id"],
            "candidate_digest": candidate["content_hash"],
            "skill_approval_id": source_approval["approval_id"],
            "skill_approval_digest": source_approval["content_hash"],
            "source_cases": [dict(item) for item in candidate["source_cases"]],
        }
        try:
            return contracts.make_procedure_revision(
                logical_name=logical_name,
                origin="generated",
                origin_scope=candidate["scope"],
                body=candidate["content"],
                references=references,
                predicates=predicates,
                source=source,
                metadata=metadata,
            )
        except contracts.ContractError as exc:
            raise ProcedureError("generated procedure source could not form a revision") from exc

    def approve_revision(
        self,
        *,
        procedure: Mapping[str, Any],
        approval_id: str,
        issuer: str,
        recipients: Iterable[Mapping[str, Any]],
        authority_evidence: Mapping[str, Any],
        approved_at: str | None = None,
    ) -> dict[str, Any]:
        """Create and persist an explicit trusted approval for a revision."""

        selected_issuer = self._require_trusted_issuer(issuer)
        try:
            approval = contracts.make_procedure_approval(
                approval_id=approval_id,
                procedure=procedure,
                issuer=selected_issuer,
                recipients=recipients,
                authority_evidence=authority_evidence,
                approved_at=approved_at,
            )
        except contracts.ContractError as exc:
            raise ProcedureError("could not create content-bound procedure approval") from exc
        return self.record_approved_revision(procedure, approval)

    def _create_remote_operation(
        self,
        *,
        kind: str,
        logical_id: str,
        revision_id: str,
        payload_id: str,
        payload_digest: str,
        partition_id: str | None,
    ) -> dict[str, Any]:
        operation = contracts.make_procedure_remote_operation(
            kind=kind,
            logical_id=logical_id,
            revision_id=revision_id,
            payload_id=payload_id,
            payload_digest=payload_digest,
            partition_id=partition_id,
        )
        persisted, _ = self.store.create_procedure_remote_operation(operation)
        return persisted

    def _update_remote_operation(
        self,
        operation: Mapping[str, Any],
        *,
        status: str,
        remote_receipt: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        if (
            operation["status"] == status
            and operation.get("remote_receipt") == remote_receipt
            and operation.get("error") == error
        ):
            return dict(operation)
        try:
            return self.store.update_procedure_remote_operation(
                str(operation["operation_id"]),
                status=status,
                expected_version=int(operation["version"]),
                remote_receipt=remote_receipt,
                error=error,
            )
        except ProcedureConflictError as exc:
            raise ProcedureError("procedure remote operation changed concurrently") from exc

    def _update_publication(
        self,
        publication: Mapping[str, Any],
        *,
        status: str,
        remote_receipt: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        if (
            publication["status"] == status
            and publication.get("remote_receipt") == remote_receipt
            and publication.get("error") == error
        ):
            return dict(publication)
        try:
            return self.store.update_procedure_publication(
                str(publication["publication_id"]),
                status=status,
                expected_version=int(publication["version"]),
                remote_receipt=remote_receipt,
                error=error,
            )
        except ProcedureConflictError as exc:
            raise ProcedureError("procedure publication changed concurrently") from exc

    @staticmethod
    def _receipt(document: Mapping[str, Any]) -> dict[str, Any]:
        identifier = document.get("_id")
        digest = document.get("content_hash")
        if not isinstance(identifier, str) or not identifier or not isinstance(digest, str) or not digest:
            raise ProcedureError("remote receipt lacks an exact identity")
        return {"remote_id": identifier, "remote_digest": digest}

    @staticmethod
    def _current_matches_designation(
        current: Mapping[str, Any] | None, designation: Mapping[str, Any]
    ) -> bool:
        return bool(
            current
            and current.get("state") == "active"
            and current.get("logical_id") == designation.get("logical_id")
            and current.get("partition_id") == designation.get("partition_id")
            and current.get("revision_id") == designation.get("revision_id")
            and current.get("designation_id") == designation.get("designation_id")
            and current.get("generation") == designation.get("generation")
            and current.get("source_digest") == designation.get("content_hash")
        )

    def publish_designation(
        self,
        designation: Mapping[str, Any],
        adapter: atlas.AtlasProcedureAdapter,
        *,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
        shared_publication_enabled: bool = True,
    ) -> dict[str, Any]:
        """Durably accept a shared current designation before publication claim."""

        self._require_task_network(network_resolution)
        if not shared_publication_enabled:
            raise ProcedureIneligibleError("shared publication feature is off")

        try:
            stored = self.store.get_procedure_designation(str(designation["designation_id"]))
        except StoreError as exc:
            raise ProcedureError("designation must be durable before remote publication") from exc
        if stored["content_hash"] != designation.get("content_hash"):
            raise ProcedureError("designation does not match durable local evidence")
        self._require_trusted_issuer(stored["issuer"])
        if stored["partition"]["scope"] == "private":
            raise ProcedureIneligibleError(
                "private procedure partitions remain local and cannot be published"
            )
        operation = self._create_remote_operation(
            kind="designation",
            logical_id=stored["logical_id"],
            revision_id=stored["revision_id"],
            payload_id=stored["designation_id"],
            payload_digest=stored["content_hash"],
            partition_id=stored["partition_id"],
        )
        try:
            current = adapter.exact_current(stored["logical_id"], stored["partition_id"])
        except atlas.AtlasProcedureError as exc:
            if operation["status"] == "intent":
                self._update_remote_operation(
                    operation, status="blocked", error="remote designation exact read failed"
                )
            if operation["status"] in {"ambiguous", "remote_committed"}:
                raise ProcedureRemoteAmbiguityError(
                    "remote designation remains uncertain until exact current can be read"
                ) from exc
            raise ProcedureError("remote designation exact read failed") from exc
        if operation["status"] in {"acknowledged", "remote_committed"}:
            if self._current_matches_designation(current, stored):
                if operation["status"] == "remote_committed":
                    self._update_remote_operation(
                        operation,
                        status="acknowledged",
                        remote_receipt=self._receipt(current),
                    )
                return current  # type: ignore[return-value]
            if operation["status"] == "remote_committed":
                if current is not None:
                    self._update_remote_operation(
                        operation, status="fenced", error="remote current changed"
                    )
                    raise ProcedureIneligibleError(
                        "remote current designation is no longer exact"
                    )
                raise ProcedureRemoteAmbiguityError(
                    "remote designation commit lacks exact current evidence"
                )
            raise ProcedureIneligibleError("remote current designation is no longer exact")
        if operation["status"] == "ambiguous":
            if self._current_matches_designation(current, stored):
                operation = self._update_remote_operation(
                    operation, status="remote_committed", remote_receipt=self._receipt(current)
                )
                self._update_remote_operation(
                    operation, status="acknowledged", remote_receipt=self._receipt(current)
                )
                return current  # type: ignore[return-value]
            raise ProcedureRemoteAmbiguityError(
                "ambiguous designation must not be retried until its exact current state resolves"
            )
        if operation["status"] != "intent":
            raise ProcedureIneligibleError("designation remote operation is no longer publishable")
        try:
            remote = adapter.write_designation(stored)
        except atlas.AtlasProcedureFencedError as exc:
            self._update_remote_operation(operation, status="fenced", error="remote designation fenced")
            raise ProcedureIneligibleError("remote designation was fenced") from exc
        except atlas.AtlasProcedureAmbiguityError as exc:
            self._update_remote_operation(operation, status="ambiguous", error="remote acknowledgement lost")
            raise ProcedureRemoteAmbiguityError(
                "remote designation acknowledgement is ambiguous; exact reconciliation is required"
            ) from exc
        except atlas.AtlasProcedureError as exc:
            self._update_remote_operation(operation, status="blocked", error="remote designation failed")
            raise ProcedureError("remote designation failed") from exc
        try:
            current = adapter.exact_current(stored["logical_id"], stored["partition_id"])
        except atlas.AtlasProcedureError as exc:
            self._update_remote_operation(
                operation, status="ambiguous", error="remote designation exact read after write failed"
            )
            raise ProcedureRemoteAmbiguityError(
                "remote designation write lacks authoritative exact readback"
            ) from exc
        if current is None:
            self._update_remote_operation(
                operation, status="ambiguous", error="remote designation lacks exact current evidence"
            )
            raise ProcedureRemoteAmbiguityError(
                "remote designation write lacks authoritative exact readback"
            )
        if not self._current_matches_designation(current, stored):
            self._update_remote_operation(operation, status="fenced", error="remote current changed")
            raise ProcedureIneligibleError("remote designation did not become exact current")
        operation = self._update_remote_operation(
            operation, status="remote_committed", remote_receipt=self._receipt(remote)
        )
        self._update_remote_operation(
            operation, status="acknowledged", remote_receipt=self._receipt(remote)
        )
        return remote

    def _snapshot_matches_publication(
        self,
        snapshot: atlas.AtlasExactProcedureSnapshot | None,
        publication: Mapping[str, Any],
    ) -> bool:
        if snapshot is None:
            return False
        try:
            atlas.validate_atlas_procedure_document(snapshot.document)
        except atlas.AtlasProcedureError:
            return False
        remote_publication = snapshot.document["publication"]
        if remote_publication.get("payload_digest") != publication.get("payload_digest"):
            return False
        current = snapshot.current
        state = snapshot.publication_state
        designation = publication["designation"]
        return bool(
            snapshot.revocation is None
            and current is not None
            and state is not None
            and self._current_matches_designation(current, designation)
            and state.get("state") == "active"
            and state.get("publication_id") == publication.get("publication_id")
            and state.get("source_digest") == publication.get("payload_digest")
            and state.get("designation_id") == designation.get("designation_id")
            and state.get("designation_generation") == designation.get("generation")
        )

    def _governed_effect_committed(
        self, snapshot: atlas.AtlasExactProcedureSnapshot | None,
        publication: Mapping[str, Any], payload: Mapping[str, Any],
    ) -> bool:
        if snapshot is None or snapshot.publication_state is None:
            return False
        state = snapshot.publication_state
        original_state = payload["publication_state"]
        if snapshot.document.get("content_hash") != payload["document"]["content_hash"]:
            return False
        return (state.get("content_hash") == original_state["content_hash"] and
                self._snapshot_matches_publication(snapshot, publication))

    @staticmethod
    def _snapshot_matches_publication_state(
        snapshot: atlas.AtlasExactProcedureSnapshot | None,
        publication: Mapping[str, Any],
        *,
        state: str,
    ) -> bool:
        """Require the exact durable state document for one tracked copy."""

        if snapshot is None or snapshot.publication_state is None:
            return False
        try:
            expected = atlas.make_atlas_publication_state_document(
                publication, state=state
            )
            atlas.validate_atlas_publication_state_document(snapshot.publication_state)
        except (atlas.AtlasProcedureError, contracts.ContractError):
            return False
        return snapshot.publication_state.get("content_hash") == expected["content_hash"]

    def _publication_state_operation_is_acknowledged(
        self, publication: Mapping[str, Any], *, kind: str
    ) -> bool:
        """A pre-existing lifecycle operation must not be hidden by local state."""

        operations = [
            operation
            for operation in self.store.list_procedure_remote_operations(
                payload_id=str(publication["publication_id"])
            )
            if operation["kind"] == kind
        ]
        return not operations or (
            len(operations) == 1 and operations[0]["status"] == "acknowledged"
        )

    def _record_publication_state_failure(
        self,
        operation: Mapping[str, Any],
        *,
        ambiguous: bool,
        error: str,
    ) -> None:
        """Retain uncertainty or a block without inventing a completed state."""

        if operation["status"] == "intent":
            self._update_remote_operation(
                operation,
                status="ambiguous" if ambiguous else "blocked",
                error=error,
            )

    def _reconcile_publication_state_operation(
        self,
        publication: Mapping[str, Any],
        *,
        kind: str,
        state: str,
        adapter: atlas.AtlasProcedureAdapter,
    ) -> bool:
        """Advance one state operation only from an authoritative exact read."""

        operation = self._create_remote_operation(
            kind=kind,
            logical_id=publication["logical_id"],
            revision_id=publication["revision_id"],
            payload_id=publication["publication_id"],
            payload_digest=publication["payload_digest"],
            partition_id=publication["partition_id"],
        )
        try:
            snapshot = adapter.exact_read(publication["publication_id"])
        except atlas.AtlasProcedureError:
            self._record_publication_state_failure(
                operation,
                ambiguous=operation["status"] == "ambiguous",
                error=f"remote {state} exact read failed",
            )
            return False

        if not self._snapshot_matches_publication_state(
            snapshot, publication, state=state
        ):
            if operation["status"] not in {"intent", "ambiguous", "blocked"}:
                return False
            try:
                adapter.write_publication_state(publication, state=state)
            except atlas.AtlasProcedureAmbiguityError:
                self._record_publication_state_failure(
                    operation,
                    ambiguous=True,
                    error=f"{state} acknowledgement lost",
                )
                return False
            except atlas.AtlasProcedureFencedError:
                if operation["status"] in {
                    "intent",
                    "ambiguous",
                    "remote_committed",
                    "acknowledged",
                    "blocked",
                }:
                    self._update_remote_operation(
                        operation,
                        status="fenced",
                        error=f"remote {state} fenced",
                    )
                return False
            except atlas.AtlasProcedureError:
                self._record_publication_state_failure(
                    operation,
                    ambiguous=False,
                    error=f"remote {state} failed",
                )
                return False
            try:
                snapshot = adapter.exact_read(publication["publication_id"])
            except atlas.AtlasProcedureError:
                # The write returned, but the following authoritative read did
                # not.  Its durable effect is uncertain rather than blocked.
                self._record_publication_state_failure(
                    operation,
                    ambiguous=True,
                    error=f"remote {state} exact read after write failed",
                )
                return False
            if not self._snapshot_matches_publication_state(
                snapshot, publication, state=state
            ):
                self._record_publication_state_failure(
                    operation,
                    ambiguous=True,
                    error=f"remote {state} lacks exact state evidence",
                )
                return False

        assert snapshot is not None and snapshot.publication_state is not None
        receipt = self._receipt(snapshot.publication_state)
        if operation["status"] in {"intent", "ambiguous", "blocked"}:
            operation = self._update_remote_operation(
                operation,
                status="remote_committed",
                remote_receipt=receipt,
            )
        if operation["status"] == "remote_committed":
            operation = self._update_remote_operation(
                operation,
                status="acknowledged",
                remote_receipt=receipt,
            )
        if operation["status"] != "acknowledged":
            return False

        current = self.store.get_procedure_publication(publication["publication_id"])
        if current["status"] == state:
            return True
        if current["status"] == "blocked":
            return False
        self._update_publication(current, status=state, remote_receipt=receipt)
        return True

    def publish(
        self,
        *,
        procedure: Mapping[str, Any],
        approval: Mapping[str, Any],
        representation: Mapping[str, Any],
        designation: Mapping[str, Any],
        adapter: atlas.AtlasProcedureAdapter,
        captured_config: Mapping[str, Any] | MemoryConfig | None = None,
        current_config: Mapping[str, Any] | MemoryConfig | None = None,
        atlas_scope: Mapping[str, Any] | None = None,
        claimant: Mapping[str, Any] | None = None,
        claim_fence: Mapping[str, Any] | None = None,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
        shared_publication_enabled: bool = True,
    ) -> dict[str, Any]:
        """Publish after trust/privacy checks.

        To govern a new Atlas effect, supply both configurations, exact scope,
        and a content-bound claimant from trusted native composition. For
        example, ``captured_config={"shared_publication": True}``,
        ``current_config={"shared_publication": True}``, and ``atlas_scope``
        with database, collection, index, namespace, and exact recipients.
        Repeat the same call after a lost acknowledgement: exact readback
        resolves it; complete absence permits a new claim generation. A
        reopened in-flight claim additionally needs a trusted claim fence
        verified by the store's constructor-bound verifier.
        """

        self._require_trusted_issuer(approval.get("issuer"))
        try:
            publication = contracts.make_procedure_publication(
                procedure=procedure,
                approval=approval,
                representation=representation,
                designation=designation,
            )
            if publication["representation"]["metric"] != adapter.metric:
                raise ProcedureIneligibleError(
                    "procedure representation metric does not match the configured Atlas index"
                )
            try:
                guard_worker_bound_remote(procedure["behavior"]["body"], self.privacy_policy)
                guard_worker_bound_remote(representation["search_text"], self.privacy_policy)
                guard_remote_payload(publication, self.privacy_policy)
            except RemotePayloadPrivacyError as exc:
                raise ProcedureError("privacy scan rejected procedure publication") from exc
            wants_governance = any(value is not None for value in (
                captured_config, current_config, atlas_scope, claimant, claim_fence))
            prior_effects = [row for row in self.store.list_effect_operations()
                if row["outcome_id"] is None and row["kind"] == "procedure_publication"
                and row["source_id"] == publication["publication_id"]]
            operations = self.store.list_procedure_remote_operations(
                payload_id=publication["publication_id"])
            stored_publication = None
            if len(prior_effects) == 1 and prior_effects[0]["status"] == "confirmed":
                try:
                    stored_publication = self.store.get_procedure_publication(
                        publication["publication_id"])
                except StoreError:
                    pass
            new_work_allowed = self._publication_admission(
                network_resolution, shared_publication_enabled,
                prior_effects, operations, stored_publication)
            if wants_governance:
                if (captured_config is None or current_config is None or atlas_scope is None
                        or claimant is None):
                    raise ProcedureError("governed publication needs a trusted claimant, configurations and scope")
                try:
                    self.store._validate_external_claimant(claimant)
                except contracts.ContractError as exc:
                    raise ProcedureError("governed publication needs a valid native claimant") from exc
                expected_keys = {"database", "collection", "index", "namespace", "recipients"}
                if (set(atlas_scope) != expected_keys or
                        any(not isinstance(atlas_scope[key], str) or not atlas_scope[key]
                            for key in ("database", "collection", "index", "namespace")) or
                        atlas_scope["namespace"] != publication["partition"]["namespace"] or
                        atlas_scope["recipients"] != publication["partition"]["recipients"]):
                    raise ProcedureError("Atlas scope must name exact collection, index, namespace, and recipients")
                if getattr(adapter, "location", None) != {
                    key: atlas_scope[key] for key in ("database", "collection", "index")
                }:
                    raise ProcedureError("Atlas scope differs from the adapter's bound location")
                if not effect_submission_enabled(captured_config, "procedure_publication"):
                    raise ProcedureIneligibleError("captured shared publication feature is off")
                if not prior_effects and not effect_submission_enabled(
                    current_config, "procedure_publication"
                ):
                    raise ProcedureIneligibleError("current shared publication feature is off")
                scope_key = contracts.sha256_hex(atlas_scope)
                effect_id = contracts.external_effect_operation_id(
                    publication["publication_id"], "procedure_publication", scope_key)
                if any(row["operation_id"] != effect_id for row in prior_effects):
                    raise ProcedureError("publication is already bound to another Atlas scope")
                if prior_effects:
                    source = prior_effects[0]["source_record"]
                    expected_source = contracts.make_procedure_publication(
                        procedure=procedure, approval=approval,
                        representation=representation, designation=designation,
                        created_at=source["created_at"])
                    if source != expected_source:
                        raise ProcedureError("governed publication source differs from original intent")
                    publication = source
                else:
                    try:
                        self.store.get_procedure_publication(publication["publication_id"])
                    except StoreError:
                        pass
                    else:
                        raise ProcedureError("legacy publication cannot acquire a retrospective governed effect")
                payload = {
                    "atlas_scope": dict(atlas_scope),
                    "document": atlas.make_atlas_procedure_document(publication),
                    "publication_state": atlas.make_atlas_publication_state_document(
                        publication, state="active"),
                }
                try:
                    self.store.create_external_effect_operation(
                        kind="procedure_publication", scope_key=scope_key,
                        source_id=publication["publication_id"], source_record=publication,
                        payload=payload, captured_config=captured_config,
                        current_config=current_config)
                except (contracts.ContractError, OperationConflictError) as exc:
                    raise ProcedureIneligibleError("governed publication intent is off or conflicts") from exc
            publication, _ = self.store.create_procedure_publication(publication)
        except (contracts.ContractError, ProcedureConflictError) as exc:
            raise ProcedureError("could not create a durable publication intent") from exc
        governed_effects = [row for row in self.store.list_effect_operations()
            if row["outcome_id"] is None and row["kind"] == "procedure_publication"
            and row["source_id"] == publication["publication_id"]]
        if governed_effects and not wants_governance:
            raise ProcedureError("governed publication recovery needs original scope and configuration")
        if not wants_governance and publication["status"] == "acknowledged":
            return publication
        operation = self._create_remote_operation(
            kind="publication",
            logical_id=publication["logical_id"],
            revision_id=publication["revision_id"],
            payload_id=publication["publication_id"],
            payload_digest=publication["payload_digest"],
            partition_id=publication["partition_id"],
        )
        if (publication["status"] not in {"intent", "ambiguous", "remote_committed", "acknowledged"}
                or operation["status"] not in {"intent", "ambiguous", "remote_committed", "acknowledged"}):
            raise ProcedureIneligibleError("publication is no longer eligible for remote submission")
        if wants_governance:
            return self._publish_governed(
                publication, operation, designation, adapter,
                captured_config=captured_config, current_config=current_config,
                atlas_scope=atlas_scope, claimant=claimant, claim_fence=claim_fence,
                new_work_allowed=new_work_allowed,
            )
        if new_work_allowed:
            self.publish_designation(designation, adapter)
        if publication["status"] in {"ambiguous", "remote_committed"} or operation[
            "status"
        ] in {"ambiguous", "remote_committed"}:
            reconciled = self.reconcile_publication(
                publication["publication_id"], adapter,
                network_resolution=network_resolution,
                shared_publication_enabled=shared_publication_enabled)
            if reconciled["status"] == "acknowledged":
                return reconciled
            raise ProcedureRemoteAmbiguityError(
                "remote publication remains unresolved after exact read"
            )
        if publication["status"] != "intent" or operation["status"] != "intent":
            raise ProcedureIneligibleError("publication is no longer eligible for remote submission")
        try:
            remote = adapter.write_publication(publication)
        except atlas.AtlasProcedureFencedError as exc:
            self._update_publication(publication, status="fenced", error="remote publication fenced")
            self._update_remote_operation(operation, status="fenced", error="remote publication fenced")
            raise ProcedureIneligibleError("remote publication was fenced") from exc
        except atlas.AtlasProcedureAmbiguityError as exc:
            self._update_publication(publication, status="ambiguous", error="remote acknowledgement lost")
            self._update_remote_operation(operation, status="ambiguous", error="remote acknowledgement lost")
            raise ProcedureRemoteAmbiguityError(
                "remote publication acknowledgement is ambiguous; exact reconciliation is required"
            ) from exc
        except atlas.AtlasProcedureError as exc:
            self._update_publication(publication, status="blocked", error="remote publication failed")
            self._update_remote_operation(operation, status="blocked", error="remote publication failed")
            raise ProcedureError("remote publication failed") from exc
        try:
            snapshot = adapter.exact_read(publication["publication_id"])
        except atlas.AtlasProcedureError as exc:
            self._update_publication(
                publication, status="ambiguous", error="remote exact read after write failed"
            )
            self._update_remote_operation(
                operation, status="ambiguous", error="remote exact read after write failed"
            )
            raise ProcedureRemoteAmbiguityError(
                "remote publication write lacks authoritative exact readback"
            ) from exc
        if not self._snapshot_matches_publication(snapshot, publication):
            remote_publication = (
                snapshot.document.get("publication") if snapshot is not None else None
            )
            incomplete_readback = snapshot is None or (
                isinstance(remote_publication, Mapping)
                and remote_publication.get("payload_digest") == publication["payload_digest"]
                and snapshot.revocation is None
                and (
                    snapshot.current is None
                    or self._current_matches_designation(
                        snapshot.current, publication["designation"]
                    )
                )
                and (
                    snapshot.publication_state is None
                    or self._snapshot_matches_publication_state(
                        snapshot, publication, state="active"
                    )
                )
                and (snapshot.current is None or snapshot.publication_state is None)
            )
            if incomplete_readback:
                self._update_publication(
                    publication, status="ambiguous", error="remote publication lacks exact readback"
                )
                self._update_remote_operation(
                    operation, status="ambiguous", error="remote publication lacks exact readback"
                )
                raise ProcedureRemoteAmbiguityError(
                    "remote publication write lacks authoritative exact readback"
                )
            self._update_publication(publication, status="fenced", error="remote lifecycle changed")
            self._update_remote_operation(operation, status="fenced", error="remote lifecycle changed")
            raise ProcedureIneligibleError("remote publication failed exact lifecycle validation")
        if operation["status"] == "intent":
            operation = self._update_remote_operation(
                operation, status="remote_committed", remote_receipt=self._receipt(remote)
            )
        if publication["status"] == "intent":
            publication = self._update_publication(
                publication, status="remote_committed", remote_receipt=self._receipt(remote)
            )
        current = self.store.get_current_procedure_designation(
            publication["logical_id"], publication["partition"]
        )
        if (
            self.store.is_procedure_revoked(publication["revision_id"])
            or current is None
            or current["state"] != "active"
            or current["designation_id"] != publication["designation"]["designation_id"]
        ):
            self._update_publication(publication, status="fenced", error="local lifecycle changed")
            self._update_remote_operation(operation, status="fenced", error="local lifecycle changed")
            raise ProcedureIneligibleError("local lifecycle fenced publication acknowledgement")
        operation = self._update_remote_operation(
            operation, status="acknowledged", remote_receipt=self._receipt(remote)
        )
        return self._update_publication(
            publication, status="acknowledged", remote_receipt=self._receipt(remote)
        )

    def _publish_governed(
        self, publication: Mapping[str, Any], operation: Mapping[str, Any],
        designation: Mapping[str, Any], adapter: atlas.AtlasProcedureAdapter,
        *, captured_config: Mapping[str, Any] | MemoryConfig,
        current_config: Mapping[str, Any] | MemoryConfig,
        atlas_scope: Mapping[str, Any],
        claimant: Mapping[str, Any],
        claim_fence: Mapping[str, Any] | None,
        new_work_allowed: bool,
    ) -> dict[str, Any]:
        scope = dict(atlas_scope)
        scope_key = contracts.sha256_hex(scope)
        effect_id = contracts.external_effect_operation_id(
            str(publication["publication_id"]), "procedure_publication", scope_key
        )
        if any(
            row["outcome_id"] is None and row["kind"] == "procedure_publication"
            and row["source_id"] == publication["publication_id"]
            and row["operation_id"] != effect_id
            for row in self.store.list_effect_operations()
        ):
            raise ProcedureError("publication is already bound to another Atlas scope")
        captured = asdict(captured_config) if isinstance(captured_config, MemoryConfig) else dict(captured_config)
        payload = {
            "atlas_scope": scope,
            "document": atlas.make_atlas_procedure_document(publication),
            "publication_state": atlas.make_atlas_publication_state_document(publication, state="active"),
        }
        try:
            existing = self.store.get_effect_operation(effect_id)
        except StoreError:
            existing = None
        if existing is not None:
            original = existing["source_record"]
            expected_source = contracts.make_procedure_publication(
                procedure=publication["procedure"], approval=publication["approval"],
                representation=publication["representation"], designation=publication["designation"],
                created_at=original["created_at"])
            if (original != expected_source or
                    existing["payload_record"]["atlas_scope"] != scope or
                    existing["configuration"] != captured):
                raise ProcedureError("governed publication replay differs from original intent")
            source = existing["source_record"]
            payload = existing["payload_record"]
        else:
            source = publication
        try:
            effect, _ = self.store.create_external_effect_operation(
                kind="procedure_publication", scope_key=scope_key,
                source_id=str(publication["publication_id"]), source_record=source,
                payload=payload, captured_config=captured_config,
                current_config=current_config,
            )
        except (contracts.ContractError, OperationConflictError) as exc:
            raise ProcedureIneligibleError("governed publication intent is off or conflicts") from exc

        def evidence(proof: Mapping[str, Any], *, absent: bool = False) -> dict[str, Any]:
            result = {key: effect[key] for key in (
                "operation_id", "kind", "scope_key", "source_digest",
                "payload_digest", "configuration_digest",
            )}
            result["adapter_proof"] = dict(proof)
            if absent:
                result["readback_complete"] = True
            return result

        def acknowledge(snapshot: atlas.AtlasExactProcedureSnapshot) -> dict[str, Any]:
            assert snapshot.publication_state is not None
            proof = {"publication_id": publication["publication_id"],
                     "document_digest": snapshot.document["content_hash"],
                     "state_digest": snapshot.publication_state["content_hash"]}
            self.store.confirm_effect_operation(effect_id, evidence(proof),
                claim_id=effect["active_claim_id"])
            current = self.store.get_current_procedure_designation(
                publication["logical_id"], publication["partition"]
            )
            if (self.store.is_procedure_revoked(publication["revision_id"]) or
                    current is None or current["state"] != "active" or
                    current["designation_id"] != publication["designation"]["designation_id"]):
                self._update_publication(publication, status="fenced", error="local lifecycle changed")
                self._update_remote_operation(operation, status="fenced", error="local lifecycle changed")
                raise ProcedureIneligibleError("local lifecycle fenced publication acknowledgement")
            return self.reconcile_publication(str(publication["publication_id"]), adapter)

        if effect["status"] == "in_flight":
            if claim_fence is None:
                raise ProcedureRemoteAmbiguityError("publication claim remains in flight")
            try:
                effect = self.store.isolate_external_effect_claim(
                    effect_id, effect["active_claim_id"], claim_fence)
            except OperationConflictError as exc:
                raise ProcedureRemoteAmbiguityError("publication claim fence is unverified") from exc
        if effect["status"] == "uncertain":
            try:
                snapshot = adapter.exact_read(str(publication["publication_id"]))
            except atlas.AtlasProcedureError as exc:
                raise ProcedureRemoteAmbiguityError("governed publication exact read failed") from exc
            if self._governed_effect_committed(snapshot, publication, payload):
                assert snapshot is not None and snapshot.publication_state is not None
                return acknowledge(snapshot)
            try:
                absent = adapter.publication_effect_absent(str(publication["publication_id"]))
            except atlas.AtlasProcedureError as exc:
                raise ProcedureRemoteAmbiguityError("publication absence is unresolved") from exc
            if not absent:
                raise ProcedureRemoteAmbiguityError("publication effect remains uncertain")
            self.store.reconcile_external_effect_operation(
                effect_id,
                evidence=evidence({"publication_id": publication["publication_id"],
                                   "state_id": atlas.publication_state_document_id(
                                       str(publication["publication_id"]))}, absent=True),
                result="absent", claim_id=effect["active_claim_id"],
            )
            effect = self.store.get_effect_operation(effect_id)
        if effect["status"] == "confirmed":
            return self.reconcile_publication(str(publication["publication_id"]), adapter)
        if effect["status"] != "pending":
            raise ProcedureIneligibleError("governed publication is not pending")
        if not new_work_allowed:
            raise ProcedureIneligibleError("new shared publication work is off")
        # The current gate is checked before any new non-safety remote work.
        if not effect_submission_enabled(current_config, "procedure_publication"):
            raise ProcedureIneligibleError("current shared publication feature is off")
        self.publish_designation(designation, adapter)
        try:
            effect = self.store.claim_effect_operation(
                effect_id, current_config=current_config, claimant=claimant)
        except (OperationConflictError, contracts.ContractError) as exc:
            raise ProcedureIneligibleError("governed publication claim is unavailable") from exc
        claim_id = effect["active_claim_id"]
        try:
            adapter.write_publication(source)
            snapshot = adapter.exact_read(str(publication["publication_id"]))
            if not self._governed_effect_committed(snapshot, publication, payload):
                raise atlas.AtlasProcedureAmbiguityError("publication lacks exact readback")
        except atlas.AtlasProcedureFencedError as exc:
            self.store.mark_effect_uncertain(effect_id, "remote publication fenced after claim",
                claim_id=claim_id)
            self._update_publication(publication, status="fenced", error="remote publication fenced")
            self._update_remote_operation(operation, status="fenced", error="remote publication fenced")
            raise ProcedureIneligibleError("remote publication was fenced") from exc
        except atlas.AtlasProcedureError as exc:
            self.store.mark_effect_uncertain(effect_id, "remote publication acknowledgement unresolved",
                claim_id=claim_id)
            self._update_publication(publication, status="ambiguous", error="remote acknowledgement unresolved")
            self._update_remote_operation(operation, status="ambiguous", error="remote acknowledgement unresolved")
            raise ProcedureRemoteAmbiguityError("remote publication requires exact reconciliation") from exc
        assert snapshot is not None and snapshot.publication_state is not None
        return acknowledge(snapshot)

    def reconcile_publication(
        self, publication_id: str, adapter: atlas.AtlasProcedureAdapter,
        *, claim_fence: Mapping[str, Any] | None = None,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
        shared_publication_enabled: bool = True,
    ) -> dict[str, Any]:
        """Resolve a lost acknowledgement through exact read, never blind replay."""

        publication = self.store.get_procedure_publication(publication_id)
        effects = [
            row for row in self.store.list_effect_operations()
            if row["outcome_id"] is None and row["kind"] == "procedure_publication"
            and row["source_id"] == publication_id
        ]
        if len(effects) > 1:
            raise ProcedureError("publication has conflicting governed scopes")
        operations = [
            operation
            for operation in self.store.list_procedure_remote_operations(payload_id=publication_id)
            if operation["kind"] == "publication"
        ]
        self._publication_admission(network_resolution, shared_publication_enabled,
                                    effects, operations, publication)
        if effects:
            effect = effects[0]
            scope = effect["payload_record"]["atlas_scope"]
            if getattr(adapter, "location", None) != {
                key: scope[key] for key in ("database", "collection", "index")
            }:
                raise ProcedureError("Atlas adapter does not match the governed effect scope")
        if effects and effects[0]["status"] != "confirmed":
            effect = effects[0]
            if effect["status"] == "in_flight":
                if claim_fence is None:
                    raise ProcedureRemoteAmbiguityError("publication claim remains in flight")
                try:
                    effect = self.store.isolate_external_effect_claim(
                        effect["operation_id"], effect["active_claim_id"], claim_fence)
                except OperationConflictError as exc:
                    raise ProcedureRemoteAmbiguityError("publication claim fence is unverified") from exc
            try:
                original = adapter.exact_read(publication_id)
            except atlas.AtlasProcedureError as exc:
                raise ProcedureRemoteAmbiguityError("governed publication exact read failed") from exc
            if (effect["status"] not in {"in_flight", "uncertain"} or
                    not self._governed_effect_committed(
                        original, publication, effect["payload_record"]
                    )):
                raise ProcedureRemoteAmbiguityError(
                    "governed publication lacks exact original submission evidence"
                )
            assert original is not None and original.publication_state is not None
            proof = {key: effect[key] for key in (
                "operation_id", "kind", "scope_key", "source_digest",
                "payload_digest", "configuration_digest",
            )}
            proof["adapter_proof"] = {
                "publication_id": publication_id,
                "document_digest": original.document["content_hash"],
                "state_digest": original.publication_state["content_hash"],
            }
            self.store.confirm_effect_operation(effect["operation_id"], proof,
                claim_id=effect["active_claim_id"])
            if original.publication_state["state"] == "active":
                current = self.store.get_current_procedure_designation(
                    publication["logical_id"], publication["partition"]
                )
                if (self.store.is_procedure_revoked(publication["revision_id"]) or
                        current is None or current["state"] != "active" or
                        current["designation_id"] != publication["designation"]["designation_id"]):
                    self._update_publication(
                        publication, status="fenced", error="local lifecycle changed"
                    )
                    raise ProcedureIneligibleError(
                        "local lifecycle fenced publication acknowledgement"
                    )
        if len(operations) != 1:
            raise ProcedureError("publication has no unique stable remote operation")
        operation = operations[0]
        snapshot = adapter.exact_read(publication_id)
        if self._snapshot_matches_publication(snapshot, publication):
            receipt = self._receipt(snapshot.document)  # type: ignore[union-attr]
            if operation["status"] in {"intent", "ambiguous"}:
                operation = self._update_remote_operation(
                    operation, status="remote_committed", remote_receipt=receipt
                )
            if publication["status"] in {"intent", "ambiguous"}:
                publication = self._update_publication(
                    publication, status="remote_committed", remote_receipt=receipt
                )
            if operation["status"] == "remote_committed":
                operation = self._update_remote_operation(
                    operation, status="acknowledged", remote_receipt=receipt
                )
            if publication["status"] == "remote_committed":
                publication = self._update_publication(
                    publication, status="acknowledged", remote_receipt=receipt
                )
            return publication
        if snapshot is not None and snapshot.revocation is not None:
            target = "revoked"
        elif snapshot is not None and (
            snapshot.current is not None and snapshot.current.get("state") == "withdrawn"
        ):
            target = "withdrawn"
        else:
            # No exact remote evidence means retain the uncertainty as-is.
            return publication
        if operation["status"] in {"intent", "ambiguous", "remote_committed", "acknowledged"}:
            self._update_remote_operation(operation, status=target, error="remote lifecycle fenced")
        if publication["status"] in {"intent", "ambiguous", "remote_committed", "acknowledged", "fenced", "revocation_pending"}:
            return self._update_publication(publication, status=target, error="remote lifecycle fenced")
        return publication

    @staticmethod
    def _current_matches_withdrawal(
        current: Mapping[str, Any] | None, withdrawal: Mapping[str, Any]
    ) -> bool:
        return bool(
            current
            and current.get("state") == "withdrawn"
            and current.get("logical_id") == withdrawal.get("logical_id")
            and current.get("partition_id") == withdrawal.get("partition_id")
            and current.get("revision_id") == withdrawal.get("revision_id")
            and current.get("designation_id") == withdrawal.get("withdrawal_id")
            and current.get("generation") == withdrawal.get("generation")
            and current.get("source_digest") == withdrawal.get("content_hash")
        )

    def _complete_withdrawn_publications(
        self,
        withdrawal: Mapping[str, Any],
        adapter: atlas.AtlasProcedureAdapter,
    ) -> bool:
        """Advance tracked copies only after the remote current fence is exact."""

        complete = True
        for publication in self.store.list_procedure_publications(
            logical_id=withdrawal["logical_id"], partition=withdrawal["partition"]
        ):
            if publication["status"] == "revoked":
                if not self._publication_state_operation_is_acknowledged(
                    publication, kind="withdraw_publication"
                ):
                    complete = False
                continue
            if publication["status"] == "withdrawn" and self._publication_state_operation_is_acknowledged(
                publication, kind="withdraw_publication"
            ):
                continue
            if not self._reconcile_publication_state_operation(
                publication,
                kind="withdraw_publication",
                state="withdrawn",
                adapter=adapter,
            ):
                complete = False
        publications = self.store.list_procedure_publications(
            logical_id=withdrawal["logical_id"], partition=withdrawal["partition"]
        )
        return complete and all(
            publication["status"] in {"withdrawn", "revoked"}
            and self._publication_state_operation_is_acknowledged(
                publication, kind="withdraw_publication"
            )
            for publication in publications
        )

    def _reconcile_withdrawal_record(
        self,
        withdrawal: Mapping[str, Any],
        adapter: atlas.AtlasProcedureAdapter,
    ) -> dict[str, Any]:
        """Exact-read one durable withdrawal before deciding whether it is safe to write."""

        if withdrawal["partition"]["scope"] == "private":
            return {"withdrawal": dict(withdrawal), "managed_complete": True}
        operation = self._create_remote_operation(
            kind="withdrawal",
            logical_id=withdrawal["logical_id"],
            revision_id=withdrawal["revision_id"],
            payload_id=withdrawal["withdrawal_id"],
            payload_digest=withdrawal["content_hash"],
            partition_id=withdrawal["partition_id"],
        )
        try:
            remote_current = adapter.exact_current(
                withdrawal["logical_id"], withdrawal["partition_id"]
            )
        except atlas.AtlasProcedureError as exc:
            if operation["status"] == "intent":
                self._update_remote_operation(
                    operation, status="blocked", error="remote withdrawal exact read failed"
                )
            if operation["status"] in {"ambiguous", "remote_committed"}:
                raise ProcedureRemoteAmbiguityError(
                    "remote withdrawal remains uncertain until exact current can be read"
                ) from exc
            raise ProcedureError("remote withdrawal exact read failed") from exc
        if self._current_matches_withdrawal(remote_current, withdrawal):
            receipt = self._receipt(remote_current)
            if operation["status"] in {"intent", "ambiguous", "blocked"}:
                operation = self._update_remote_operation(
                    operation, status="remote_committed", remote_receipt=receipt
                )
            if operation["status"] == "remote_committed":
                operation = self._update_remote_operation(
                    operation, status="acknowledged", remote_receipt=receipt
                )
        elif operation["status"] in {"intent", "blocked"}:
            try:
                remote = adapter.write_withdrawal(withdrawal)
            except atlas.AtlasProcedureAmbiguityError as exc:
                self._update_remote_operation(
                    operation, status="ambiguous", error="withdrawal acknowledgement lost"
                )
                raise ProcedureRemoteAmbiguityError(
                    "remote withdrawal acknowledgement is ambiguous; exact reconciliation is required"
                ) from exc
            except atlas.AtlasProcedureFencedError as exc:
                if operation["status"] != "fenced":
                    self._update_remote_operation(
                        operation, status="fenced", error="remote withdrawal fenced"
                    )
                raise ProcedureIneligibleError("remote withdrawal was fenced") from exc
            except atlas.AtlasProcedureError as exc:
                if operation["status"] == "intent":
                    self._update_remote_operation(
                        operation, status="blocked", error="remote withdrawal failed"
                    )
                raise ProcedureError("remote withdrawal failed") from exc
            try:
                remote_current = adapter.exact_current(
                    withdrawal["logical_id"], withdrawal["partition_id"]
                )
            except atlas.AtlasProcedureError as exc:
                self._update_remote_operation(
                    operation, status="ambiguous", error="remote withdrawal exact read after write failed"
                )
                raise ProcedureRemoteAmbiguityError(
                    "remote withdrawal write lacks authoritative exact readback"
                ) from exc
            if remote_current is None:
                self._update_remote_operation(
                    operation, status="ambiguous", error="remote withdrawal lacks exact current evidence"
                )
                raise ProcedureRemoteAmbiguityError(
                    "remote withdrawal write lacks authoritative exact readback"
                )
            if not self._current_matches_withdrawal(remote_current, withdrawal):
                self._update_remote_operation(
                    operation, status="fenced", error="remote current changed"
                )
                raise ProcedureIneligibleError("remote withdrawal did not become exact current")
            receipt = self._receipt(remote)
            if operation["status"] in {"intent", "blocked"}:
                operation = self._update_remote_operation(
                    operation, status="remote_committed", remote_receipt=receipt
                )
            if operation["status"] == "remote_committed":
                operation = self._update_remote_operation(
                    operation, status="acknowledged", remote_receipt=receipt
                )
        else:
            if remote_current is not None and int(remote_current["generation"]) >= int(
                withdrawal["generation"]
            ):
                if operation["status"] != "fenced":
                    self._update_remote_operation(
                        operation, status="fenced", error="remote current changed"
                    )
                raise ProcedureIneligibleError("remote withdrawal is fenced by newer state")
            raise ProcedureRemoteAmbiguityError(
                "withdrawal remains uncertain until exact current evidence resolves"
            )
        if operation["status"] != "acknowledged":
            return {"withdrawal": dict(withdrawal), "managed_complete": False}
        return {
            "withdrawal": dict(withdrawal),
            "managed_complete": self._complete_withdrawn_publications(withdrawal, adapter),
        }

    def withdraw_partition(
        self,
        *,
        logical_id: str,
        partition: Mapping[str, Any],
        issuer: str,
        adapter: atlas.AtlasProcedureAdapter,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Withdraw one partition without revoking other authorized copies."""

        selected_issuer = self._require_trusted_issuer(issuer)
        current = self.store.get_current_procedure_designation(logical_id, partition)
        if current is None or current["state"] != "active":
            raise ProcedureIneligibleError("partition has no active designation to withdraw")
        try:
            predecessor = self.store.get_procedure_designation(current["designation_id"])
            withdrawal = contracts.make_procedure_withdrawal(
                predecessor_designation=predecessor, issuer=selected_issuer
            )
            withdrawal = self.store.record_procedure_withdrawal(withdrawal)
        except (contracts.ContractError, ProcedureConflictError, StoreError) as exc:
            raise ProcedureError("could not durably fence the partition withdrawal") from exc
        if not atlas.atlas_task_network_allowed(network_resolution):
            return {"withdrawal": withdrawal,
                    "managed_complete": withdrawal["partition"]["scope"] == "private"}
        return self._reconcile_withdrawal_record(withdrawal, adapter)

    def reconcile_withdrawal(
        self,
        *,
        logical_id: str,
        partition: Mapping[str, Any],
        adapter: atlas.AtlasProcedureAdapter,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Reconcile a locally fenced withdrawal through the exact current id."""

        current = self.store.get_current_procedure_designation(logical_id, partition)
        if current is None or current["state"] != "withdrawn":
            raise ProcedureIneligibleError("partition has no durable withdrawal to reconcile")
        withdrawal = current.get("record")
        try:
            contracts.validate_procedure_withdrawal(withdrawal)
        except contracts.ContractError as exc:
            raise ProcedureError("current withdrawal state is not valid durable evidence") from exc
        self._require_trusted_issuer(withdrawal["issuer"])
        if not atlas.atlas_task_network_allowed(network_resolution):
            return {"withdrawal": dict(withdrawal),
                    "managed_complete": withdrawal["partition"]["scope"] == "private"}
        return self._reconcile_withdrawal_record(withdrawal, adapter)

    def revoke(
        self,
        *,
        procedure: Mapping[str, Any],
        issuer: str,
        reason: str,
        adapter: atlas.AtlasProcedureAdapter,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Tombstone a revision and fence every tracked publication before completion."""

        selected_issuer = self._require_trusted_issuer(issuer)
        try:
            existing = self.store.get_procedure_revocation(str(procedure["revision_id"]))
            if existing is None:
                revocation = contracts.make_procedure_revocation(
                    procedure=procedure, issuer=selected_issuer, reason=reason
                )
                revocation = self.store.record_procedure_revocation(revocation)
            else:
                contracts.validate_procedure_revocation(existing, procedure=procedure)
                if existing["issuer"] != selected_issuer or existing["reason"] != reason:
                    raise ProcedureIneligibleError(
                        "procedure revision already has a different durable revocation"
                    )
                revocation = existing
        except (contracts.ContractError, ProcedureConflictError) as exc:
            raise ProcedureError("could not durably record procedure revocation") from exc
        operation = self._create_remote_operation(
            kind="revocation",
            logical_id=revocation["logical_id"],
            revision_id=revocation["revision_id"],
            payload_id=revocation["revocation_id"],
            payload_digest=revocation["content_hash"],
            partition_id=None,
        )
        if not atlas.atlas_task_network_allowed(network_resolution):
            return {
                "revocation": revocation,
                "managed_complete": False,
                "exposures": self.store.list_procedure_exposures(revocation["revision_id"]),
            }
        remote_ready = False
        try:
            remote = adapter.write_revocation(revocation)
            receipt = self._receipt(remote)
            if operation["status"] in {"intent", "ambiguous", "blocked"}:
                operation = self._update_remote_operation(
                    operation, status="remote_committed", remote_receipt=receipt
                )
            if operation["status"] == "remote_committed":
                operation = self._update_remote_operation(
                    operation, status="acknowledged", remote_receipt=receipt
                )
            remote_ready = operation["status"] == "acknowledged"
        except atlas.AtlasProcedureAmbiguityError:
            if operation["status"] == "intent":
                self._update_remote_operation(
                    operation, status="ambiguous", error="revocation acknowledgement lost"
                )
        except atlas.AtlasProcedureError:
            if operation["status"] == "intent":
                self._update_remote_operation(operation, status="blocked", error="remote revocation failed")
        if remote_ready:
            for publication in self.store.list_procedure_publications(
                revision_id=revocation["revision_id"]
            ):
                if publication["status"] == "blocked":
                    continue
                self._reconcile_publication_state_operation(
                    publication,
                    kind="revoke_publication",
                    state="revoked",
                    adapter=adapter,
                )
        publications = self.store.list_procedure_publications(revision_id=revocation["revision_id"])
        revocation_operation = self.store.get_procedure_remote_operation(operation["operation_id"])
        return {
            "revocation": revocation,
            "managed_complete": (
                revocation_operation["status"] == "acknowledged"
                and all(
                    publication["status"] in {"revoked", "withdrawn"}
                    and self._publication_state_operation_is_acknowledged(
                        publication, kind="revoke_publication"
                    )
                    for publication in publications
                )
            ),
            "exposures": self.store.list_procedure_exposures(revocation["revision_id"]),
        }

    def record_exposure(
        self,
        *,
        publication_id: str,
        recipient: Mapping[str, Any],
        delivery_id: str,
    ) -> dict[str, Any]:
        """Retain already delivered exposure without treating it as revocable."""

        publication = self.store.get_procedure_publication(publication_id)
        try:
            exposure = contracts.make_procedure_exposure(
                publication=publication, recipient=recipient, delivery_id=delivery_id
            )
            return self.store.record_procedure_exposure(exposure)
        except (contracts.ContractError, ProcedureConflictError) as exc:
            raise ProcedureError("could not record known procedure exposure") from exc

    def resolve_atlas(
        self,
        query_text: str,
        *,
        receiver: Mapping[str, Any],
        facts: Mapping[str, Any],
        route: str,
        adapter: atlas.AtlasProcedureAdapter,
        representation: Mapping[str, Any] | None = None,
        limit: int = 8,
        network_resolution: NetworkResolution | Mapping[str, Any] | None = None,
        atlas_shared_retrieval_enabled: bool = True,
    ) -> list[dict[str, Any]]:
        """Discover ids through Vector Search, then exact-read every delivery candidate."""

        if not atlas_shared_retrieval_enabled or not atlas.atlas_task_network_allowed(
            network_resolution
        ):
            return []

        # Search order and duplicate physical copies are discovery artifacts,
        # not authority.  The contract permits one delivered revision per
        # logical procedure, preferring the narrower remote scope; score only
        # orders equally specific, independently authorized partitions.
        scope_rank = {"private": 0, "project": 1, "shared": 2}
        selected: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {}
        if not isinstance(representation, Mapping) or any(
            field not in representation
            for field in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
        ):
            # Atlas guidance is optional.  Do not even begin a discovery query
            # when the caller cannot prove the representation it will compare.
            return []
        sanitized_query = sanitize_payload(query_text, self.privacy_policy)
        if not isinstance(sanitized_query, str) or not sanitized_query.strip():
            raise ProcedureError("Atlas discovery query must be a nonempty string")
        for hit in adapter.discover(sanitized_query, receiver=receiver, limit=limit):
            try:
                snapshot = adapter.exact_read(hit.publication_id)
                if snapshot is None:
                    continue
                delivery = atlas.validate_atlas_snapshot_for_delivery(
                    snapshot,
                    receiver=receiver,
                    facts=facts,
                    route=route,
                    trusted_issuers=self.trusted_issuers,
                    representation=representation,
                )
                if delivery["representation"]["metric"] != adapter.metric:
                    continue
            except (atlas.AtlasProcedureError, contracts.ContractError):
                # A bad vector result is not evidence against a separate hit;
                # it simply cannot become optional procedural guidance.
                continue
            candidate = {**delivery, "discovery_score": hit.score}
            key = (
                scope_rank.get(candidate["partition"]["scope"], 99),
                -hit.score,
                candidate["revision_id"],
                candidate["publication_id"],
            )
            logical_id = candidate["logical_id"]
            prior = selected.get(logical_id)
            if prior is None or key < prior[0]:
                selected[logical_id] = (key, candidate)
        return [
            candidate
            for _, candidate in sorted(
                selected.values(), key=lambda item: (item[0], item[1]["logical_id"])
            )
        ]


__all__ = [
    "ProcedureError",
    "ProcedureAuthorizationError",
    "ProcedureRemoteAmbiguityError",
    "ProcedureIneligibleError",
    "ProcedureNetworkDeniedError",
    "TrustedProcedureService",
]

