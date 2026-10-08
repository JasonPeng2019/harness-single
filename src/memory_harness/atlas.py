"""Deterministic local Atlas-query and representation fixtures.

This module is a fixture adapter only: it never contacts a live service and
provider authentication is accepted only to be explicitly excluded from the
query/representation payload.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from . import contracts
from .config import NETWORK_MODES, NetworkResolution
from .experience import ExperienceRecord
from .privacy import PrivacyPolicy, sanitize_payload
from .activity_log import write_activity, write_detail


@dataclass(frozen=True)
class AtlasQuery:
    text: str
    objective_id: str
    route: str
    filters: tuple[tuple[str, str], ...] = ()

    def to_record(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "objective_id": self.objective_id,
            "route": self.route,
            "filters": [list(item) for item in self.filters],
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "AtlasQuery":
        return cls(
            text=str(record["text"]),
            objective_id=str(record["objective_id"]),
            route=str(record["route"]),
            filters=tuple((str(key), str(value)) for key, value in record.get("filters", [])),
        )


@dataclass(frozen=True)
class AtlasDocument:
    document_id: str
    objective_id: str
    route: str
    text: str
    fingerprint: str

    def to_record(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "objective_id": self.objective_id,
            "route": self.route,
            "text": self.text,
            "fingerprint": self.fingerprint,
        }


def build_atlas_query(
    text: str,
    *,
    objective_id: str,
    route: str,
    privacy_policy: PrivacyPolicy | None = None,
    credentials: Mapping[str, Any] | None = None,
    filters: Mapping[str, str] | None = None,
) -> AtlasQuery:
    """Build a sanitized query. Credentials are never copied into the payload."""

    policy = privacy_policy or PrivacyPolicy()
    sanitized_text = sanitize_payload(text, policy)
    if not isinstance(sanitized_text, str):
        raise ValueError("Atlas query text must be a string")
    normalized_filters = tuple(
        (str(key), str(value)) for key, value in sorted((filters or {}).items())
    )
    return AtlasQuery(
        text=sanitized_text,
        objective_id=objective_id,
        route=route,
        filters=normalized_filters,
    )


def build_representation(
    experience: ExperienceRecord, privacy_policy: PrivacyPolicy | None = None
) -> AtlasDocument:
    policy = privacy_policy or PrivacyPolicy()
    sanitized = sanitize_payload(experience.raw_content, policy)
    if not isinstance(sanitized, str):
        raise ValueError("representation text must be a string")
    fingerprint = _deterministic_fingerprint(sanitized)
    return AtlasDocument(
        document_id=experience.record_id,
        objective_id=experience.objective_id,
        route=experience.route,
        text=sanitized,
        fingerprint=fingerprint,
    )


def _deterministic_fingerprint(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _tokens(text: str) -> set[str]:
    return {item for item in re.findall(r"[a-z0-9_]+", text.lower()) if len(item) > 2}


class LocalAtlasFixture:
    """A deterministic in-memory fixture, not a live Atlas client."""

    def __init__(self, documents: Iterable[AtlasDocument]) -> None:
        self.documents = tuple(documents)

    def search(self, query: AtlasQuery) -> list[AtlasDocument]:
        query_tokens = _tokens(query.text)
        scored: list[tuple[int, str, AtlasDocument]] = []
        for document in self.documents:
            if query.objective_id and document.objective_id != query.objective_id:
                continue
            if query.route and document.route != query.route:
                continue
            score = len(query_tokens & _tokens(document.text))
            if score:
                scored.append((score, document.document_id, document))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [item[2] for item in scored]


# The records below are the narrow production Atlas boundary.  Vector Search is
# only used to obtain publication ids; every deliverable procedure is then read
# from the collection by exact id with its lifecycle control records.

ATLAS_PROCEDURE_DOCUMENT_SCHEMA = "atlas-trusted-procedure-document/v1"
ATLAS_CURRENT_DOCUMENT_SCHEMA = "atlas-trusted-procedure-current/v1"
ATLAS_PUBLICATION_STATE_SCHEMA = "atlas-trusted-procedure-publication-state/v1"
ATLAS_REVOCATION_DOCUMENT_SCHEMA = "atlas-trusted-procedure-revocation/v1"
ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS = (
    "model",
    "dimensions",
    "metric",
    "sanitizer_version",
)

# ``MongoDBAtlasVectorSearch`` uses the Atlas spelling for dot product while
# the product contract keeps a portable, snake-case metric name.
_ATLAS_VECTOR_METRICS = {
    "cosine": "cosine",
    "dot_product": "dotProduct",
    "euclidean": "euclidean",
}


class AtlasProcedureError(RuntimeError):
    """The Atlas procedure adapter cannot establish authoritative state."""


class AtlasProcedureDependencyError(AtlasProcedureError):
    """The optional Atlas dependencies are not installed."""


class AtlasProcedureAmbiguityError(AtlasProcedureError):
    """A write acknowledgement is not enough to prove the remote state."""


class AtlasProcedureFencedError(AtlasProcedureError):
    """A newer designation, withdrawal, or revocation fenced a stale write."""


def atlas_task_network_allowed(
    resolution: NetworkResolution | Mapping[str, Any] | None,
) -> bool:
    """Consume the provider's captured effective profile without resolving it again.

    ``None`` preserves callers that have no captured network profile. A rich
    preparation record and the provider's NetworkResolution have the same
    requested/effective meaning. The caller supplies this captured provider
    record; this boundary cannot create or verify an egress claim. Legacy
    callers with no captured profile retain their established behavior.
    """

    if resolution is None:
        return True
    requested = (resolution.requested_mode if isinstance(resolution, NetworkResolution)
                 else resolution.get("requested_mode") if isinstance(resolution, Mapping) else None)
    effective = (resolution.effective_mode if isinstance(resolution, NetworkResolution)
                 else resolution.get("effective_mode") if isinstance(resolution, Mapping) else None)
    if requested not in NETWORK_MODES or effective not in NETWORK_MODES:
        raise AtlasProcedureError("captured network resolution is incomplete")
    if effective == "atlas_memory_only":
        verified = (resolution.verification_result if isinstance(resolution, NetworkResolution)
                    else resolution.get("verification_result"))
        if requested != "atlas_memory_only" or (
            verified is not True and not isinstance(verified, Mapping)
        ):
            raise AtlasProcedureError("Atlas-only needs a provider-verified effective resolution")
    elif requested == "atlas_memory_only":
        if effective != "soft_guardrail_network":
            raise AtlasProcedureError("Atlas-only downgrade must retain effective soft mode")
    elif effective != requested:
        raise AtlasProcedureError("captured network modes disagree")
    return effective != "restricted_local"


@dataclass(frozen=True)
class AtlasDiscoveryHit:
    publication_id: str
    score: float


@dataclass(frozen=True)
class AtlasExactProcedureSnapshot:
    document: dict[str, Any]
    current: dict[str, Any] | None
    publication_state: dict[str, Any] | None
    revocation: dict[str, Any] | None


def _atlas_record_hash(record: Mapping[str, Any]) -> str:
    return contracts.content_hash(record)


def _deep_copy_record(record: Mapping[str, Any]) -> dict[str, Any]:
    try:
        import json

        value = json.loads(contracts.canonical_json(record).decode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise AtlasProcedureError("Atlas procedure record must be JSON serializable") from exc
    if not isinstance(value, dict):
        raise AtlasProcedureError("Atlas procedure record must be an object")
    return value


def current_document_id(logical_id: str, partition_id: str) -> str:
    return f"trusted-procedure-current:{logical_id}:{partition_id}"


def publication_state_document_id(publication_id: str) -> str:
    return f"trusted-procedure-publication-state:{publication_id}"


def revocation_document_id(revision_id: str) -> str:
    return f"trusted-procedure-revocation:{revision_id}"


def make_atlas_procedure_document(publication: Mapping[str, Any]) -> dict[str, Any]:
    """Serialize one immutable publication for Atlas Vector Search discovery."""

    contracts.validate_procedure_publication(publication)
    partition = publication["partition"]
    representation = publication["representation"]
    record: dict[str, Any] = {
        "_id": publication["publication_id"],
        "schema": ATLAS_PROCEDURE_DOCUMENT_SCHEMA,
        "document_kind": "trusted_procedure_publication",
        "publication_id": publication["publication_id"],
        "logical_id": publication["logical_id"],
        "revision_id": publication["revision_id"],
        "partition_id": publication["partition_id"],
        "application": partition["application"],
        "project": partition["project"],
        "namespace": partition["namespace"],
        "partition_scope": partition["scope"],
        "designation_id": publication["designation"]["designation_id"],
        "designation_generation": publication["designation"]["generation"],
        "representation_id": representation["representation_id"],
        "representation_payload_digest": representation["payload_digest"],
        "search_text": representation["search_text"],
        # Direct exact reads preserve this vector. The public vector-store
        # wrapper is not used for writes because its insert helper cannot
        # conditionally preserve this full immutable product record.
        "procedure_embedding": list(representation["vector"]),
        "publication": _deep_copy_record(publication),
    }
    record["content_hash"] = _atlas_record_hash(record)
    validate_atlas_procedure_document(record)
    return record


def validate_atlas_procedure_document(record: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping):
        raise AtlasProcedureError("Atlas procedure document must be an object")
    if record.get("schema") != ATLAS_PROCEDURE_DOCUMENT_SCHEMA:
        raise AtlasProcedureError("Atlas procedure document schema mismatch")
    if record.get("document_kind") != "trusted_procedure_publication":
        raise AtlasProcedureError("Atlas procedure document kind mismatch")
    if record.get("content_hash") != _atlas_record_hash(record):
        raise AtlasProcedureError("Atlas procedure document content hash mismatch")
    publication = record.get("publication")
    if not isinstance(publication, Mapping):
        raise AtlasProcedureError("Atlas procedure document has no publication evidence")
    try:
        contracts.validate_procedure_publication(publication)
    except contracts.ContractError as exc:
        raise AtlasProcedureError("Atlas procedure document has invalid publication evidence") from exc
    expected = {
        "_id": publication["publication_id"],
        "publication_id": publication["publication_id"],
        "logical_id": publication["logical_id"],
        "revision_id": publication["revision_id"],
        "partition_id": publication["partition_id"],
        "application": publication["partition"]["application"],
        "project": publication["partition"]["project"],
        "namespace": publication["partition"]["namespace"],
        "partition_scope": publication["partition"]["scope"],
        "designation_id": publication["designation"]["designation_id"],
        "designation_generation": publication["designation"]["generation"],
        "representation_id": publication["representation"]["representation_id"],
        "representation_payload_digest": publication["representation"]["payload_digest"],
        "search_text": publication["representation"]["search_text"],
        "procedure_embedding": publication["representation"]["vector"],
    }
    for field, expected_value in expected.items():
        if record.get(field) != expected_value:
            raise AtlasProcedureError(f"Atlas procedure document {field} does not match publication")


def _make_control_document(
    *,
    identifier: str,
    schema: str,
    kind: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "_id": identifier,
        "schema": schema,
        "document_kind": kind,
        **_deep_copy_record(payload),
    }
    record["content_hash"] = _atlas_record_hash(record)
    return record


def make_atlas_current_document(
    designation: Mapping[str, Any],
    *,
    state: str = "active",
) -> dict[str, Any]:
    contracts.validate_procedure_designation(designation)
    if state not in {"active", "withdrawn"}:
        raise AtlasProcedureError("Atlas current state must be active or withdrawn")
    partition = designation["partition"]
    return _make_control_document(
        identifier=current_document_id(designation["logical_id"], designation["partition_id"]),
        schema=ATLAS_CURRENT_DOCUMENT_SCHEMA,
        kind="trusted_procedure_current",
        payload={
            "logical_id": designation["logical_id"],
            "partition_id": designation["partition_id"],
            "partition": partition,
            "revision_id": designation["revision_id"],
            "designation_id": designation["designation_id"],
            "generation": designation["generation"],
            "state": state,
            "source_digest": designation["content_hash"],
        },
    )


def make_atlas_withdrawal_document(withdrawal: Mapping[str, Any]) -> dict[str, Any]:
    contracts.validate_procedure_withdrawal(withdrawal)
    return _make_control_document(
        identifier=current_document_id(withdrawal["logical_id"], withdrawal["partition_id"]),
        schema=ATLAS_CURRENT_DOCUMENT_SCHEMA,
        kind="trusted_procedure_current",
        payload={
            "logical_id": withdrawal["logical_id"],
            "partition_id": withdrawal["partition_id"],
            "partition": withdrawal["partition"],
            "revision_id": withdrawal["revision_id"],
            "designation_id": withdrawal["withdrawal_id"],
            "generation": withdrawal["generation"],
            "state": "withdrawn",
            "source_digest": withdrawal["content_hash"],
        },
    )


def validate_atlas_current_document(record: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping) or record.get("schema") != ATLAS_CURRENT_DOCUMENT_SCHEMA:
        raise AtlasProcedureError("Atlas current document schema mismatch")
    if record.get("document_kind") != "trusted_procedure_current":
        raise AtlasProcedureError("Atlas current document kind mismatch")
    if record.get("content_hash") != _atlas_record_hash(record):
        raise AtlasProcedureError("Atlas current document content hash mismatch")
    for field in ("_id", "logical_id", "partition_id", "revision_id", "designation_id", "source_digest"):
        if not isinstance(record.get(field), str) or not record[field]:
            raise AtlasProcedureError(f"Atlas current document lacks {field}")
    try:
        partition = contracts.normalize_procedure_partition(record.get("partition"))
    except contracts.ContractError as exc:
        raise AtlasProcedureError("Atlas current document has an invalid partition") from exc
    if record.get("partition") != partition or record["partition_id"] != contracts.procedure_partition_id(partition):
        raise AtlasProcedureError("Atlas current document partition identity mismatch")
    if record["_id"] != current_document_id(record["logical_id"], record["partition_id"]):
        raise AtlasProcedureError("Atlas current document id mismatch")
    if record.get("state") not in {"active", "withdrawn"}:
        raise AtlasProcedureError("Atlas current document state is invalid")
    generation = record.get("generation")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise AtlasProcedureError("Atlas current document generation is invalid")


def make_atlas_publication_state_document(
    publication: Mapping[str, Any], *, state: str = "active"
) -> dict[str, Any]:
    contracts.validate_procedure_publication(publication)
    if state not in {"active", "withdrawn", "revoked"}:
        raise AtlasProcedureError("Atlas publication state is invalid")
    return _make_control_document(
        identifier=publication_state_document_id(publication["publication_id"]),
        schema=ATLAS_PUBLICATION_STATE_SCHEMA,
        kind="trusted_procedure_publication_state",
        payload={
            "publication_id": publication["publication_id"],
            "logical_id": publication["logical_id"],
            "revision_id": publication["revision_id"],
            "partition_id": publication["partition_id"],
            "designation_id": publication["designation"]["designation_id"],
            "designation_generation": publication["designation"]["generation"],
            "state": state,
            "source_digest": publication["payload_digest"],
        },
    )


def validate_atlas_publication_state_document(record: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping) or record.get("schema") != ATLAS_PUBLICATION_STATE_SCHEMA:
        raise AtlasProcedureError("Atlas publication state schema mismatch")
    if record.get("document_kind") != "trusted_procedure_publication_state":
        raise AtlasProcedureError("Atlas publication state kind mismatch")
    if record.get("content_hash") != _atlas_record_hash(record):
        raise AtlasProcedureError("Atlas publication state content hash mismatch")
    for field in ("_id", "publication_id", "logical_id", "revision_id", "partition_id", "designation_id", "source_digest"):
        if not isinstance(record.get(field), str) or not record[field]:
            raise AtlasProcedureError(f"Atlas publication state lacks {field}")
    if record["_id"] != publication_state_document_id(record["publication_id"]):
        raise AtlasProcedureError("Atlas publication state id mismatch")
    if record.get("state") not in {"active", "withdrawn", "revoked"}:
        raise AtlasProcedureError("Atlas publication state is invalid")
    generation = record.get("designation_generation")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise AtlasProcedureError("Atlas publication state generation is invalid")


def make_atlas_revocation_document(revocation: Mapping[str, Any]) -> dict[str, Any]:
    contracts.validate_procedure_revocation(revocation)
    return _make_control_document(
        identifier=revocation_document_id(revocation["revision_id"]),
        schema=ATLAS_REVOCATION_DOCUMENT_SCHEMA,
        kind="trusted_procedure_revocation",
        payload={
            "revocation_id": revocation["revocation_id"],
            "logical_id": revocation["logical_id"],
            "revision_id": revocation["revision_id"],
            "source_digest": revocation["content_hash"],
        },
    )


def validate_atlas_revocation_document(record: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping) or record.get("schema") != ATLAS_REVOCATION_DOCUMENT_SCHEMA:
        raise AtlasProcedureError("Atlas revocation document schema mismatch")
    if record.get("document_kind") != "trusted_procedure_revocation":
        raise AtlasProcedureError("Atlas revocation document kind mismatch")
    if record.get("content_hash") != _atlas_record_hash(record):
        raise AtlasProcedureError("Atlas revocation document content hash mismatch")
    for field in ("_id", "revocation_id", "logical_id", "revision_id", "source_digest"):
        if not isinstance(record.get(field), str) or not record[field]:
            raise AtlasProcedureError(f"Atlas revocation document lacks {field}")
    if record["_id"] != revocation_document_id(record["revision_id"]):
        raise AtlasProcedureError("Atlas revocation document id mismatch")


class AtlasProcedureAdapter:
    """Thin Atlas adapter: public Vector Search for discovery, PyMongo for truth.

    ``collection`` is a public PyMongo collection-like object. ``vector_store``
    is a public ``MongoDBAtlasVectorSearch`` instance (or a deterministic test
    double) and is deliberately used only for vector-search discovery/index
    administration. Exact content, vectors, current state, and tombstones are
    always read from ``collection`` by the stable product identifiers.
    """

    def __init__(
        self,
        *,
        collection: Any,
        vector_store: Any,
        metric: str = "cosine",
        privacy_policy: PrivacyPolicy | None = None,
        location: Mapping[str, str] | None = None,
    ) -> None:
        if metric not in _ATLAS_VECTOR_METRICS:
            raise AtlasProcedureError("Atlas procedure metric is unsupported")
        self.collection = collection
        self.vector_store = vector_store
        self.metric = metric
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self.location = None if location is None else dict(location)
        if self.location is not None:
            if (set(self.location) != {"database", "collection", "index"} or
                    any(not isinstance(value, str) or not value
                        for value in self.location.values())):
                raise AtlasProcedureError("Atlas adapter location must name database, collection, and index")
            actual_collection = getattr(collection, "name", None)
            actual_database = getattr(getattr(collection, "database", None), "name", None)
            if ((isinstance(actual_collection, str) and
                 actual_collection != self.location["collection"]) or
                    (isinstance(actual_database, str) and
                     actual_database != self.location["database"])):
                raise AtlasProcedureError("Atlas adapter location differs from its collection")

    @classmethod
    def from_pymongo_collection(
        cls,
        *,
        collection: Any,
        embedding: Any,
        index_name: str,
        metric: str = "cosine",
        privacy_policy: PrivacyPolicy | None = None,
    ) -> "AtlasProcedureAdapter":
        """Build a lazy optional adapter without creating index infrastructure."""

        if metric not in _ATLAS_VECTOR_METRICS:
            raise AtlasProcedureError("Atlas procedure metric is unsupported")

        try:
            from langchain_mongodb import MongoDBAtlasVectorSearch
        except ImportError as exc:
            raise AtlasProcedureDependencyError(
                "Atlas support requires the memory-harness[atlas] optional dependencies"
            ) from exc
        vector_store = MongoDBAtlasVectorSearch(
            collection=collection,
            embedding=embedding,
            index_name=index_name,
            text_key="search_text",
            embedding_key="procedure_embedding",
            relevance_score_fn=_ATLAS_VECTOR_METRICS[metric],
            auto_create_index=False,
        )
        return cls(
            collection=collection,
            vector_store=vector_store,
            metric=metric,
            privacy_policy=privacy_policy,
            location={"database": collection.database.name,
                      "collection": collection.name, "index": index_name},
        )

    def create_vector_search_index(
        self,
        *,
        dimensions: int,
        filter_fields: list[str] | None = None,
        wait_until_complete: float | None = None,
    ) -> None:
        """Explicit administrative index setup; runtime discovery never creates it."""

        write_activity(
            "memory.atlas.index.started", dimensions=dimensions,
            location=self.location,
        )
        self.vector_store.create_vector_search_index(
            dimensions=dimensions,
            filters=list(filter_fields or ["document_kind", "application", "namespace"]),
            wait_until_complete=wait_until_complete,
        )
        write_activity(
            "memory.atlas.index.completed", dimensions=dimensions,
            location=self.location,
        )

    @staticmethod
    def _as_mapping(value: Any, description: str) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise AtlasProcedureError(f"{description} is not an object")
        return dict(value)

    def _find_one(self, identifier: str) -> dict[str, Any] | None:
        result = self.collection.find_one({"_id": identifier})
        if result is None:
            return None
        return self._as_mapping(result, "Atlas exact-read result")

    @staticmethod
    def _local_insert_typeerror(exc: TypeError) -> bool:
        trace = exc.__traceback__
        if trace is not None and trace.tb_next is None and "required positional argument" in str(exc):
            # Python rejected the collection call before entering insert_one.
            return True
        while trace is not None:
            frame = trace.tb_frame
            if (
                frame.f_globals.get("__name__") == "pymongo.common"
                and frame.f_code.co_name == "validate_is_document_type"
            ):
                # PyMongo invokes this validator before its insert command.
                return True
            trace = trace.tb_next
        return False

    def _insert_exact(self, document: Mapping[str, Any], *, description: str) -> dict[str, Any]:
        identifier = document.get("_id")
        if not isinstance(identifier, str) or not identifier:
            raise AtlasProcedureError(f"{description} has no stable id")
        write_activity(
            "memory.atlas.store.started", document_id=identifier,
            description=description, location=self.location,
        )
        write_detail(
            "memory.atlas.store.payload",
            document_id=identifier,
            description=description,
            location=self.location,
            document=dict(document),
        )
        try:
            existing = self._find_one(identifier)
        except Exception as exc:
            raise AtlasProcedureError(f"{description} exact read before write failed") from exc
        if existing is not None:
            if existing.get("content_hash") == document.get("content_hash"):
                write_activity(
                    "memory.atlas.store.completed", document_id=identifier,
                    description=description, deduplicated=True,
                )
                return existing
            raise AtlasProcedureFencedError(f"{description} id already names different content")
        try:
            self.collection.insert_one(dict(document))
        except Exception as exc:
            if isinstance(exc, TypeError) and self._local_insert_typeerror(exc):
                raise AtlasProcedureError(f"{description} local insert validation failed") from exc
            # A lost response may have committed. Exact-read only the same id;
            # never generate a replacement operation identity on retry.
            try:
                reconciled = self._find_one(identifier)
            except Exception as read_exc:
                raise AtlasProcedureAmbiguityError(
                    f"{description} write acknowledgement and readback are ambiguous"
                ) from read_exc
            if reconciled is not None and reconciled.get("content_hash") == document.get("content_hash"):
                return reconciled
            raise AtlasProcedureAmbiguityError(
                f"{description} write acknowledgement is ambiguous"
            ) from exc
        try:
            reconciled = self._find_one(identifier)
        except Exception as exc:
            raise AtlasProcedureAmbiguityError(
                f"{description} did not exact-read after its write"
            ) from exc
        if reconciled is None or reconciled.get("content_hash") != document.get("content_hash"):
            raise AtlasProcedureAmbiguityError(
                f"{description} did not exact-read after its write"
            )
        write_activity(
            "memory.atlas.store.completed", document_id=identifier,
            description=description, deduplicated=False,
        )
        return reconciled

    def _write_ordered_current(self, document: Mapping[str, Any]) -> dict[str, Any]:
        validate_atlas_current_document(document)
        identifier = str(document["_id"])
        try:
            existing = self._find_one(identifier)
        except AtlasProcedureError:
            raise
        except Exception as exc:
            raise AtlasProcedureError("Atlas current exact read before write failed") from exc
        if existing is None:
            persisted = self._insert_exact(document, description="Atlas current designation")
            validate_atlas_current_document(persisted)
            return persisted
        validate_atlas_current_document(existing)
        existing_generation = int(existing["generation"])
        desired_generation = int(document["generation"])
        if existing_generation > desired_generation:
            raise AtlasProcedureFencedError("newer Atlas current designation already exists")
        if existing_generation == desired_generation:
            if existing.get("content_hash") == document.get("content_hash"):
                return existing
            raise AtlasProcedureFencedError("Atlas current generation already has another operation")
        write_error: Exception | None = None
        result: Any = None
        try:
            result = self.collection.replace_one(
                {"_id": identifier, "generation": existing_generation},
                dict(document),
                upsert=False,
            )
        except AtlasProcedureError:
            raise
        except Exception as exc:
            write_error = exc
        try:
            reconciled = self._find_one(identifier)
            if reconciled is not None:
                validate_atlas_current_document(reconciled)
        except Exception as exc:
            raise AtlasProcedureAmbiguityError(
                "Atlas current designation did not exact-read after update"
            ) from exc
        if reconciled is not None and reconciled.get("content_hash") == document.get("content_hash"):
            return reconciled
        if reconciled is not None and int(reconciled["generation"]) >= desired_generation:
            raise AtlasProcedureFencedError("Atlas current designation advanced during update")
        if write_error is not None:
            raise AtlasProcedureAmbiguityError(
                "Atlas current designation acknowledgement is ambiguous"
            ) from write_error
        if getattr(result, "matched_count", None) not in (None, 1):
            raise AtlasProcedureAmbiguityError("Atlas current conditional update was not matched")
        if reconciled is None:
            raise AtlasProcedureAmbiguityError("Atlas current designation disappeared after update")
        raise AtlasProcedureFencedError("Atlas current designation advanced during update")

    def write_designation(self, designation: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_procedure_designation(designation)
        if designation["partition"]["scope"] == "private":
            raise AtlasProcedureError("private procedure partitions stay local in V1")
        return self._write_ordered_current(make_atlas_current_document(designation))

    def write_withdrawal(self, withdrawal: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_procedure_withdrawal(withdrawal)
        if withdrawal["partition"]["scope"] == "private":
            raise AtlasProcedureError("private procedure partitions stay local in V1")
        return self._write_ordered_current(make_atlas_withdrawal_document(withdrawal))

    def write_revocation(self, revocation: Mapping[str, Any]) -> dict[str, Any]:
        document = make_atlas_revocation_document(revocation)
        persisted = self._insert_exact(document, description="Atlas revocation tombstone")
        validate_atlas_revocation_document(persisted)
        return persisted

    def write_publication(self, publication: Mapping[str, Any]) -> dict[str, Any]:
        """Write an immutable publication only while its exact remote fence is active."""

        contracts.validate_procedure_publication(publication)
        if publication["partition"]["scope"] == "private":
            raise AtlasProcedureError("private procedure partitions stay local in V1")
        if publication["representation"]["metric"] != self.metric:
            raise AtlasProcedureError(
                "publication representation metric does not match the Atlas index"
            )
        document = make_atlas_procedure_document(publication)
        current = self._find_one(
            current_document_id(publication["logical_id"], publication["partition_id"])
        )
        if current is None:
            raise AtlasProcedureFencedError("Atlas has no durable current designation")
        validate_atlas_current_document(current)
        designation = publication["designation"]
        if (
            current["state"] != "active"
            or current["designation_id"] != designation["designation_id"]
            or current["generation"] != designation["generation"]
            or current["revision_id"] != publication["revision_id"]
        ):
            raise AtlasProcedureFencedError("Atlas publication is fenced by newer lifecycle state")
        if self._find_one(revocation_document_id(publication["revision_id"])) is not None:
            raise AtlasProcedureFencedError("Atlas publication revision is revoked")
        persisted = self._insert_exact(document, description="Atlas procedure publication")
        validate_atlas_procedure_document(persisted)
        state = make_atlas_publication_state_document(publication, state="active")
        try:
            persisted_state = self._insert_exact(state, description="Atlas publication state")
            validate_atlas_publication_state_document(persisted_state)
        except AtlasProcedureFencedError:
            raise
        except AtlasProcedureError as exc:
            raise AtlasProcedureAmbiguityError(
                "Atlas publication state is uncertain after publication write"
            ) from exc
        return persisted

    def write_publication_state(
        self, publication: Mapping[str, Any], *, state: str
    ) -> dict[str, Any]:
        contracts.validate_procedure_publication(publication)
        if publication["partition"]["scope"] == "private":
            raise AtlasProcedureError("private procedure partitions stay local in V1")
        desired = make_atlas_publication_state_document(publication, state=state)
        identifier = str(desired["_id"])
        existing = self._find_one(identifier)
        if existing is None:
            persisted = self._insert_exact(desired, description="Atlas publication lifecycle state")
            validate_atlas_publication_state_document(persisted)
            return persisted
        validate_atlas_publication_state_document(existing)
        order = {"active": 0, "withdrawn": 1, "revoked": 2}
        if order[existing["state"]] > order[state]:
            raise AtlasProcedureFencedError("Atlas publication lifecycle is already stricter")
        if existing["state"] == state:
            if existing.get("source_digest") != desired.get("source_digest"):
                raise AtlasProcedureFencedError("Atlas publication state names different content")
            return existing
        try:
            result = self.collection.replace_one(
                {"_id": identifier, "content_hash": existing["content_hash"]},
                dict(desired),
                upsert=False,
            )
            matched = getattr(result, "matched_count", None)
            if matched not in (None, 1):
                reconciled = self._find_one(identifier)
                if reconciled is not None:
                    validate_atlas_publication_state_document(reconciled)
                    if reconciled.get("content_hash") == desired.get("content_hash"):
                        return reconciled
                    order = {"active": 0, "withdrawn": 1, "revoked": 2}
                    if order[reconciled["state"]] >= order[state]:
                        raise AtlasProcedureFencedError(
                            "Atlas publication lifecycle is already stricter"
                        )
                raise AtlasProcedureAmbiguityError(
                    "Atlas publication lifecycle conditional update was not matched"
                )
        except AtlasProcedureError:
            raise
        except Exception as exc:
            reconciled = self._find_one(identifier)
            if reconciled is not None and reconciled.get("content_hash") == desired.get("content_hash"):
                return reconciled
            raise AtlasProcedureAmbiguityError("Atlas publication lifecycle acknowledgement is ambiguous") from exc
        reconciled = self._find_one(identifier)
        if reconciled is None or reconciled.get("content_hash") != desired.get("content_hash"):
            raise AtlasProcedureAmbiguityError("Atlas publication lifecycle did not exact-read")
        validate_atlas_publication_state_document(reconciled)
        return reconciled

    def discover(
        self,
        query_text: str,
        *,
        receiver: Mapping[str, Any],
        limit: int = 8,
    ) -> list[AtlasDiscoveryHit]:
        """Use the public Vector Search primitive only to shortlist stable ids."""

        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise AtlasProcedureError("Atlas discovery limit must be positive")
        receiver_scope = contracts.normalize_experience_scope(receiver)
        sanitized = sanitize_payload(query_text, self.privacy_policy)
        if not isinstance(sanitized, str) or not sanitized.strip():
            raise AtlasProcedureError("Atlas discovery query must be a nonempty string")
        write_activity(
            "memory.atlas.recall.started", limit=limit, location=self.location,
        )
        write_detail(
            "memory.atlas.recall.query",
            query_text=sanitized,
            receiver=receiver_scope,
            limit=limit,
            location=self.location,
        )
        try:
            results = self.vector_store.similarity_search_with_score(
                sanitized,
                k=limit,
                pre_filter={
                    "document_kind": "trusted_procedure_publication",
                    "application": receiver_scope["application"],
                    "namespace": receiver_scope["namespace"],
                },
            )
            write_detail(
                "memory.atlas.recall.vector_results",
                query_text=sanitized,
                location=self.location,
                results=[
                    {
                        "id": getattr(item[0], "id", None)
                        if isinstance(item, tuple) and item
                        else None,
                        "text": getattr(item[0], "page_content", None)
                        if isinstance(item, tuple) and item
                        else None,
                        "metadata": getattr(item[0], "metadata", None)
                        if isinstance(item, tuple) and item
                        else None,
                        "score": item[1]
                        if isinstance(item, tuple) and len(item) == 2
                        else None,
                    }
                    for item in results
                ],
            )
        except Exception as exc:
            write_activity(
                "memory.atlas.recall.failed", error_type=type(exc).__name__,
                location=self.location,
            )
            raise AtlasProcedureError("Atlas Vector Search discovery failed") from exc
        hits: list[AtlasDiscoveryHit] = []
        seen: set[str] = set()
        for item in results:
            if not isinstance(item, tuple) or len(item) != 2:
                raise AtlasProcedureError("Atlas Vector Search returned an invalid result")
            document, raw_score = item
            metadata = getattr(document, "metadata", {})
            identifier = getattr(document, "id", None)
            if not isinstance(identifier, str) or not identifier:
                if isinstance(metadata, Mapping):
                    candidate = metadata.get("_id", metadata.get("publication_id"))
                    identifier = str(candidate) if candidate is not None else ""
            if not identifier or identifier in seen:
                continue
            if (
                not isinstance(raw_score, (int, float))
                or isinstance(raw_score, bool)
                or not math.isfinite(raw_score)
            ):
                raise AtlasProcedureError("Atlas Vector Search returned a nonnumeric score")
            seen.add(identifier)
            hits.append(AtlasDiscoveryHit(publication_id=identifier, score=float(raw_score)))
        write_activity(
            "memory.atlas.recall.completed", hits=len(hits),
            location=self.location,
        )
        return hits

    def exact_read(self, publication_id: str) -> AtlasExactProcedureSnapshot | None:
        """Read the selected procedure and all lifecycle evidence by exact id."""

        write_activity(
            "memory.atlas.exact_read.started", publication_id=publication_id,
            location=self.location,
        )

        def read(identifier: str) -> dict[str, Any] | None:
            try:
                return self._find_one(identifier)
            except AtlasProcedureError:
                raise
            except Exception as exc:
                raise AtlasProcedureError("Atlas exact read failed") from exc

        document = read(publication_id)
        if document is None:
            write_activity(
                "memory.atlas.exact_read.completed", publication_id=publication_id,
                found=False, location=self.location,
            )
            return None
        validate_atlas_procedure_document(document)
        current = read(
            current_document_id(document["logical_id"], document["partition_id"])
        )
        if current is not None:
            validate_atlas_current_document(current)
        state = read(publication_state_document_id(publication_id))
        if state is not None:
            validate_atlas_publication_state_document(state)
        revocation = read(revocation_document_id(document["revision_id"]))
        if revocation is not None:
            validate_atlas_revocation_document(revocation)
        result = AtlasExactProcedureSnapshot(
            document=document,
            current=current,
            publication_state=state,
            revocation=revocation,
        )
        write_activity(
            "memory.atlas.exact_read.completed", publication_id=publication_id,
            found=True, location=self.location,
        )
        write_detail(
            "memory.atlas.exact_read.payload",
            publication_id=publication_id,
            location=self.location,
            document=document,
            current=current,
            publication_state=state,
            revocation=revocation,
        )
        return result

    def publication_effect_absent(self, publication_id: str) -> bool:
        """Prove both IDs written by publication are absent by exact reads.

        A missing publication document alone is insufficient: the companion
        lifecycle state may have committed before an acknowledgement was lost.
        """
        try:
            document = self._find_one(publication_id)
            state = self._find_one(publication_state_document_id(publication_id))
        except Exception as exc:
            raise AtlasProcedureError("Atlas publication absence read failed") from exc
        return document is None and state is None

    def exact_current(
        self, logical_id: str, partition_id: str
    ) -> dict[str, Any] | None:
        """Read one current-designation control document by exact identity."""

        try:
            current = self._find_one(current_document_id(logical_id, partition_id))
        except AtlasProcedureError:
            raise
        except Exception as exc:
            raise AtlasProcedureError("Atlas exact current read failed") from exc
        if current is not None:
            validate_atlas_current_document(current)
        return current


def validate_atlas_snapshot_for_delivery(
    snapshot: AtlasExactProcedureSnapshot,
    *,
    receiver: Mapping[str, Any],
    facts: Mapping[str, Any],
    route: str,
    trusted_issuers: Iterable[str],
    representation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return normalized guidance only after full authoritative revalidation."""

    validate_atlas_procedure_document(snapshot.document)
    publication = snapshot.document["publication"]
    procedure = publication["procedure"]
    approval = publication["approval"]
    selected_representation = publication["representation"]
    designation = publication["designation"]
    try:
        contracts.validate_procedure_revision(procedure)
        contracts.validate_procedure_approval(approval, procedure=procedure)
        contracts.validate_procedure_representation(selected_representation, procedure=procedure)
        contracts.validate_procedure_designation(
            designation, procedure=procedure, approval=approval
        )
    except contracts.ContractError as exc:
        raise AtlasProcedureError("Atlas exact read has incoherent procedure evidence") from exc
    receiver_scope = contracts.normalize_experience_scope(receiver)
    recipient_keys = {
        contracts.canonical_json(item).decode("utf-8")
        for item in publication["partition"]["recipients"]
    }
    if contracts.canonical_json(receiver_scope).decode("utf-8") not in recipient_keys:
        raise AtlasProcedureFencedError("receiver is outside the published procedure partition")
    if not contracts.partition_is_authorized(approval, publication["partition"]):
        raise AtlasProcedureError("Atlas publication partition broadens approval")
    if publication["partition"]["scope"] == "private":
        raise AtlasProcedureFencedError("private procedure partitions are never remote deliverables")
    if approval["issuer"] not in frozenset(trusted_issuers):
        raise AtlasProcedureFencedError("Atlas procedure issuer is not trusted by this receiver")
    if snapshot.current is None:
        raise AtlasProcedureFencedError("Atlas procedure has unresolved current designation")
    current = snapshot.current
    validate_atlas_current_document(current)
    if (
        current["state"] != "active"
        or current["logical_id"] != publication["logical_id"]
        or current["partition_id"] != publication["partition_id"]
        or current["revision_id"] != publication["revision_id"]
        or current["designation_id"] != designation["designation_id"]
        or current["generation"] != designation["generation"]
        or current["source_digest"] != designation["content_hash"]
    ):
        raise AtlasProcedureFencedError("Atlas procedure is not the exact current designation")
    if snapshot.publication_state is None:
        raise AtlasProcedureFencedError("Atlas procedure has unresolved publication state")
    state = snapshot.publication_state
    validate_atlas_publication_state_document(state)
    if (
        state["state"] != "active"
        or state["publication_id"] != publication["publication_id"]
        or state["logical_id"] != publication["logical_id"]
        or state["revision_id"] != publication["revision_id"]
        or state["designation_id"] != designation["designation_id"]
        or state["designation_generation"] != designation["generation"]
        or state["source_digest"] != publication["payload_digest"]
    ):
        raise AtlasProcedureFencedError("Atlas procedure publication is no longer active")
    if snapshot.revocation is not None:
        if snapshot.revocation["revision_id"] == publication["revision_id"]:
            raise AtlasProcedureFencedError("Atlas procedure revision is revoked")
        raise AtlasProcedureError("Atlas exact read returned a mismatched revocation document")
    if not contracts.procedure_predicates_match(
        procedure["behavior"]["predicates"], facts, route=route
    ):
        raise AtlasProcedureFencedError("Atlas procedure predicates are not currently eligible")
    if not isinstance(representation, Mapping) or any(
        field not in representation
        for field in ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
    ):
        raise AtlasProcedureFencedError(
            "Atlas procedure delivery requires an exact query representation identity"
        )
    for field in ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS:
        if representation.get(field) != selected_representation.get(field):
            raise AtlasProcedureFencedError("Atlas procedure representation is incompatible")
    return {
        "publication_id": publication["publication_id"],
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure": _deep_copy_record(procedure),
        "approval": _deep_copy_record(approval),
        "representation": _deep_copy_record(selected_representation),
        "designation": _deep_copy_record(designation),
        "partition": _deep_copy_record(publication["partition"]),
    }


__all__ = [
    "AtlasQuery",
    "AtlasDocument",
    "LocalAtlasFixture",
    "build_atlas_query",
    "build_representation",
    "ATLAS_PROCEDURE_DOCUMENT_SCHEMA",
    "ATLAS_CURRENT_DOCUMENT_SCHEMA",
    "ATLAS_PUBLICATION_STATE_SCHEMA",
    "ATLAS_REVOCATION_DOCUMENT_SCHEMA",
    "ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS",
    "AtlasProcedureError",
    "AtlasProcedureDependencyError",
    "AtlasProcedureAmbiguityError",
    "AtlasProcedureFencedError",
    "AtlasDiscoveryHit",
    "AtlasExactProcedureSnapshot",
    "AtlasProcedureAdapter",
    "current_document_id",
    "publication_state_document_id",
    "revocation_document_id",
    "make_atlas_procedure_document",
    "validate_atlas_procedure_document",
    "make_atlas_current_document",
    "make_atlas_withdrawal_document",
    "validate_atlas_current_document",
    "make_atlas_publication_state_document",
    "validate_atlas_publication_state_document",
    "make_atlas_revocation_document",
    "validate_atlas_revocation_document",
    "validate_atlas_snapshot_for_delivery",
]
