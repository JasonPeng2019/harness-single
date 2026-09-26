"""Reviewed execution experience and its deliberately narrow reuse boundary.

The local SQLite trajectory is authoritative. EverOS is introduced below as an
optional representation adapter; it never replaces ROOT review, terminal
outcome evidence, or the exact task/plan/run provenance retained locally.
"""

from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Protocol

from . import contracts
from . import config
from . import privacy as privacy_module
from .privacy import PrivacyPolicy
from .store import ExperienceConflictError, MemoryStore, OperationConflictError, StoreError

SYNTHETIC_SECRET = "synthetic-secret-alpha-1234567890"


# EverOS owns lazy process-global service singletons.  The adapter cannot safely
# switch their backing root, so the first successful public surface claims the
# one root this Python process may use.  A fresh process is the supported
# boundary for a different root.
_everos_process_root: Path | None = None
_everos_process_root_lock = threading.RLock()


@dataclass(frozen=True)
class ExperienceScope:
    """The application/project/namespace/owner boundary for local experience."""

    application: str
    project: str
    namespace: str
    owner: str

    def to_record(self) -> dict[str, str]:
        return {
            "application": self.application,
            "project": self.project,
            "namespace": self.namespace,
            "owner": self.owner,
        }

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> "ExperienceScope":
        try:
            return cls(**contracts.normalize_experience_scope(value))
        except (contracts.ContractError, TypeError) as exc:
            raise ValueError("experience scope is incomplete") from exc


@dataclass(frozen=True)
class ExperienceRecord:
    """Synthetic Stage-A fixture retained for existing deterministic tests."""

    record_id: str
    objective_id: str
    route: str
    status: str
    raw_content: str
    evidence_ref: str
    reviewed_by: str = "ROOT"


_EXAMPLES = (
    ExperienceRecord(
        record_id="experience-regression-001",
        objective_id="objective-regression",
        route="ordinary",
        status="reviewed_success",
        raw_content=(
            "A regression failed after the parser change. The discriminating check was "
            "tests/test_parser.py. The repair preserved the public API. "
            f"Synthetic fixture credential {SYNTHETIC_SECRET} must never leave raw evidence."
        ),
        evidence_ref="review://fixture/regression-001",
    ),
    ExperienceRecord(
        record_id="experience-interface-002",
        objective_id="objective-interface",
        route="ordinary",
        status="reviewed_failure",
        raw_content=(
            "An interface change failed because a consumer was not updated. "
            "The missing consumer was in the release adapter."
        ),
        evidence_ref="review://fixture/interface-002",
    ),
    ExperienceRecord(
        record_id="experience-hypothesis-003",
        objective_id="objective-hypothesis",
        route="problem_focused",
        status="disproved_hypothesis",
        raw_content=(
            "The timeout was not caused by the network adapter; the local lock was held "
            "by the previous worker. This disproved the network-first hypothesis."
        ),
        evidence_ref="review://fixture/hypothesis-003",
    ),
)


def load_experience_examples(policy: PrivacyPolicy | None = None) -> tuple[ExperienceRecord, ...]:
    """Return immutable synthetic reviewed-experience fixtures.

    ``policy`` is accepted for interface symmetry, but raw authoritative evidence
    is intentionally never rewritten here.
    """

    return _EXAMPLES


def derived_optional_content(
    record: ExperienceRecord, policy: PrivacyPolicy | None = None
) -> dict[str, Any]:
    selected_policy = policy or PrivacyPolicy(known_secrets=(SYNTHETIC_SECRET,))
    sanitized = privacy_module.sanitize_payload(record.raw_content, selected_policy)
    return {
        "id": record.record_id,
        "kind": "experience",
        "status": record.status,
        "content": sanitized,
        "evidence_ref": record.evidence_ref,
    }


class ExperienceError(RuntimeError):
    """Base error for reviewed-experience persistence and representation."""


class ScopeBoundaryError(ExperienceError):
    """A recalled object did not resolve inside the requested scope."""


class ProvenanceError(ExperienceError):
    """A case or generated skill lacks exact reviewed source provenance."""


class ApprovalError(ExperienceError):
    """A generated candidate cannot receive the requested approval."""


class EverOSUnavailableError(ExperienceError):
    """The optional EverOS public package is unavailable to this process."""


@dataclass(frozen=True)
class VerifiedApproval:
    """A configured trust policy's authenticated approval result.

    The service verifies every field against the durable candidate before it
    persists this evidence.  Constructing an approval record from a caller's
    ``issuer`` string is deliberately not an authorization path.
    """

    issuer: str
    candidate_id: str
    scope: Mapping[str, Any]
    recipients: tuple[str, ...]
    authority_evidence: Mapping[str, Any]


class TrustedApprovalVerifier(Protocol):
    """Authenticate approval of one exact generated candidate and recipient set."""

    def verify_generated_skill_approval(
        self,
        *,
        issuer: str,
        candidate: Mapping[str, Any],
        scope: Mapping[str, Any],
        recipients: tuple[str, ...],
    ) -> VerifiedApproval:
        """Return retained authority evidence or reject the requested approval."""


@dataclass(frozen=True)
class EverOSPublicSurface:
    """Only the vendored public EverOS seam plus its captured memory root."""

    memorize: Callable[..., Awaitable[Any]]
    search: Callable[[Any], Awaitable[Any]]
    make_search_request: Callable[..., Any]
    memory_root: Path
    resolve_memory_root: Callable[[], str | Path]
    get: Callable[[Any], Awaitable[Any]] | None = None
    make_get_request: Callable[..., Any] | None = None

    @classmethod
    def from_object(
        cls,
        value: object,
        *,
        memory_root: str | Path,
        resolve_memory_root: Callable[[], str | Path],
    ) -> "EverOSPublicSurface":
        memorize = getattr(value, "memorize", None)
        search = getattr(value, "search", None)
        make_search_request = getattr(value, "make_search_request", None)
        get = getattr(value, "get", None)
        make_get_request = getattr(value, "make_get_request", None)
        if not callable(memorize) or not callable(search) or not callable(make_search_request):
            raise TypeError("EverOS public surface requires memorize, search, and SearchRequest")
        if callable(get) != callable(make_get_request):
            raise TypeError("EverOS public get requires both get and GetRequest")
        if not callable(resolve_memory_root):
            raise TypeError("EverOS public surface requires a MemoryRoot resolver")
        return cls(
            memorize=memorize,
            search=search,
            make_search_request=make_search_request,
            memory_root=_normalize_memory_root(memory_root, "captured EverOS root"),
            resolve_memory_root=resolve_memory_root,
            get=get,
            make_get_request=make_get_request,
        )


def _normalize_memory_root(value: str | Path, label: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise ScopeBoundaryError(f"{label} must be a nonempty path")
    return Path(value).expanduser().resolve()


def load_vendored_everos_public_surface(
    *, memory_root: str | Path
) -> EverOSPublicSurface:
    """Load vendored EverOS through its public service and request DTO exports.

    The product package keeps this import lazy so ordinary all-off installs do
    not acquire EverOS's optional runtime dependency set.  The caller must
    configure ``EVEROS_ROOT`` for this exact namespace *before* loading the
    public surface.  We then capture EverOS's own public ``MemoryRoot`` and
    retain its public resolver. Every adapter operation re-resolves that root
    and fails closed if it changed.  EverOS has lazy process-global services,
    so a process may load only one root; use a fresh process to change roots.
    """

    expected_root = _normalize_memory_root(memory_root, "requested EverOS root")
    global _everos_process_root
    with _everos_process_root_lock:
        if (
            _everos_process_root is not None
            and _everos_process_root != expected_root
        ):
            raise ScopeBoundaryError(
                "EverOS is already bound to a different root in this process; "
                "start a fresh process before loading another root"
            )

        configured_root = os.environ.get("EVEROS_ROOT")
        if configured_root is None or not configured_root.strip():
            raise ScopeBoundaryError(
                "EVEROS_ROOT must be configured before loading the EverOS surface"
            )
        if _normalize_memory_root(configured_root, "EVEROS_ROOT") != expected_root:
            raise ScopeBoundaryError(
                "configured EverOS root is outside the requested namespace"
            )
        try:
            from everos.core.persistence import MemoryRoot
            from everos.memory.get import GetRequest
            from everos.memory.search import SearchRequest
            from everos.service import get, memorize, search
        except ModuleNotFoundError as exc:
            raise EverOSUnavailableError(
                "EverOS is unavailable; install the vendored EverOS runtime before "
                "enabling reviewed-experience extraction"
            ) from exc

        def resolve_memory_root() -> Path:
            return MemoryRoot.resolve().root

        captured_root = _normalize_memory_root(
            resolve_memory_root(), "captured EverOS root"
        )
        if captured_root != expected_root:
            raise ScopeBoundaryError(
                "loaded EverOS public surface captured a root outside the requested namespace"
            )
        _everos_process_root = captured_root
        return EverOSPublicSurface(
            memorize=memorize,
            search=search,
            make_search_request=SearchRequest,
            memory_root=captured_root,
            resolve_memory_root=resolve_memory_root,
            get=get,
            make_get_request=GetRequest,
        )


class EverOSAdapter:
    """Translate one exact product scope to EverOS roots and public filters.

    EverOS natively has application, project, and agent-owner fields. A
    product namespace is isolated by a deterministic EverOS root, so one
    adapter is intentionally bound to one four-part scope.  Callers configure
    the EverOS service process before loading the public surface; this adapter
    checks both the captured root and EverOS's current public ``MemoryRoot``
    resolution before every payload or service operation.  One process may use
    only one EverOS root, and changing it requires a fresh process.
    """

    destination = "everos-agent-memory/v1"

    def __init__(
        self,
        *,
        scope: ExperienceScope,
        base_root: str | Path,
        surface: EverOSPublicSurface,
        privacy_policy: PrivacyPolicy | None = None,
    ) -> None:
        self.scope = scope
        self._validate_scope(scope)
        self.base_root = Path(base_root).resolve()
        self.memory_root = self.memory_root_for_scope(self.base_root, scope)
        self.surface = surface
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self._assert_bound_memory_root()

    @staticmethod
    def _validate_scope(scope: ExperienceScope) -> None:
        for field, value in scope.to_record().items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"experience scope {field} must be a nonempty string")

    @classmethod
    def memory_root_for_scope(
        cls, base_root: str | Path, scope: ExperienceScope
    ) -> Path:
        cls._validate_scope(scope)
        namespace_key = contracts.sha256_hex(
            {"application": scope.application, "namespace": scope.namespace}
        )[:32]
        return Path(base_root).resolve() / f"everos-{namespace_key}"

    @property
    def everos_application_id(self) -> str:
        return _everos_identifier("app", self.scope.application)

    @property
    def everos_project_id(self) -> str:
        return _everos_identifier("project", self.scope.project)

    @property
    def everos_owner_id(self) -> str:
        return _everos_identifier("owner", self.scope.owner)

    def session_id_for(self, trajectory_id: str) -> str:
        return contracts.sha256_hex(
            {"trajectory_id": trajectory_id, "destination": self.destination}
        )

    def assert_scope(self, scope: Mapping[str, Any]) -> None:
        if dict(scope) != self.scope.to_record():
            raise ScopeBoundaryError("EverOS adapter is bound to a different scope")

    def _assert_bound_memory_root(self) -> None:
        """Fail closed unless captured, adapter, and public current roots agree."""

        if self.surface.memory_root != self.memory_root:
            raise ScopeBoundaryError(
                "captured EverOS root is outside the adapter namespace"
            )
        try:
            current_root = _normalize_memory_root(
                self.surface.resolve_memory_root(), "current EverOS root"
            )
        except ScopeBoundaryError:
            raise
        except Exception as exc:
            raise ScopeBoundaryError(
                "current EverOS root could not be resolved"
            ) from exc
        if current_root != self.memory_root:
            raise ScopeBoundaryError(
                "current EverOS root differs from the captured adapter root"
            )

    def add_payload(
        self, trajectory: Mapping[str, Any], *, session_id: str
    ) -> dict[str, Any]:
        self.assert_scope(trajectory.get("scope", {}))
        self._assert_bound_memory_root()
        derived = privacy_module.sanitize_payload(
            {
                "task": trajectory["task_text"],
                "evidence": trajectory["raw_evidence"],
                "failed_hypotheses": trajectory["failed_hypotheses"],
            },
            self.privacy_policy,
        )
        if not isinstance(derived, Mapping):
            raise ExperienceError("sanitized EverOS input must remain an object")
        message = "\n".join(
            part
            for part in (
                f"Task: {derived['task']}",
                f"Reviewed trajectory evidence: {derived['evidence']}",
                f"Trajectory receipt: {session_id}",
                "Disproved hypotheses: " + "; ".join(derived["failed_hypotheses"])
                if derived["failed_hypotheses"]
                else "",
            )
            if part
        )
        if self.privacy_policy.detect(message):
            raise ExperienceError("unsafe content remained after EverOS sanitization")
        return {
            "session_id": session_id,
            "app_id": self.everos_application_id,
            "project_id": self.everos_project_id,
            "messages": [
                {
                    "sender_id": self.everos_owner_id,
                    "role": "assistant",
                    "timestamp": _epoch_milliseconds(trajectory["recorded_at"]),
                    "content": message,
                }
            ],
        }

    async def memorize(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_bound_memory_root()
        result = await self.surface.memorize(dict(payload), is_final=True)
        return _model_mapping(result, "EverOS memorize result")

    async def readback_cases(
        self, *, session_id: str, query: str
    ) -> list[dict[str, Any]]:
        """Read one original session through public get when available.

        Older public-surface doubles retain the accepted scoped search path.
        A failed or incomplete get never falls through to ranked search.
        """

        self._assert_bound_memory_root()
        if not isinstance(session_id, str) or not session_id:
            raise ExperienceError("EverOS readback requires the original session_id")
        if self.surface.get is None or self.surface.make_get_request is None:
            result = await self.search_representation(session_id=session_id, query=query)
            return result["agent_cases"]
        cases: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        expected_total: int | None = None
        page = 1
        page_size = 100  # Vendored GetRequest's maximum page size.
        while True:
            self._assert_bound_memory_root()
            request = self.surface.make_get_request(
                agent_id=self.everos_owner_id,
                app_id=self.everos_application_id,
                project_id=self.everos_project_id,
                memory_type="agent_case",
                filters={"session_id": session_id},
                page=page,
                page_size=page_size,
            )
            response = _model_mapping(await self.surface.get(request), "EverOS get response")
            data = response.get("data")
            if not isinstance(data, Mapping):
                raise ExperienceError("EverOS get response has no data object")
            items = data.get("agent_cases")
            count = data.get("count")
            total_count = data.get("total_count")
            if (
                not isinstance(items, list)
                or isinstance(count, bool)
                or not isinstance(count, int)
                or isinstance(total_count, bool)
                or not isinstance(total_count, int)
                or total_count < 0
                or (expected_total is not None and total_count != expected_total)
                or count != len(items)
                or count != min(page_size, total_count - len(cases))
            ):
                raise ExperienceError("EverOS get case listing is incomplete or malformed")
            expected_total = total_count
            for item in items:
                source_case = _model_mapping(item, "EverOS get case")
                self.validate_case(source_case, session_id=session_id)
                if source_case["id"] in seen_ids:
                    raise ExperienceError("EverOS get returned duplicate case identities")
                seen_ids.add(source_case["id"])
                cases.append(source_case)
            if len(cases) == expected_total:
                return cases
            page += 1

    async def search_representation(
        self, *, session_id: str, query: str
    ) -> dict[str, list[dict[str, Any]]]:
        self._assert_bound_memory_root()
        safe_query = privacy_module.sanitize_text(query, self.privacy_policy).strip()
        if not safe_query:
            raise ExperienceError("EverOS search query became empty after sanitization")
        request = self.surface.make_search_request(
            agent_id=self.everos_owner_id,
            app_id=self.everos_application_id,
            project_id=self.everos_project_id,
            # EverOS's public filter DSL fans one filter across both agent-case
            # and agent-skill tables. Agent skills have no ``session_id`` field,
            # so a session filter fails the whole public search. The stable
            # receipt token is part of our own sanitized case text instead;
            # app/project/owner remain upstream hard filters and session is
            # checked exactly below before a receipt can be confirmed.
            query=f"{safe_query} {session_id}",
            method="keyword",
            top_k=100,
        )
        response = _model_mapping(await self.surface.search(request), "EverOS search response")
        data = response.get("data")
        if not isinstance(data, Mapping):
            raise ExperienceError("EverOS search response has no data object")
        result: dict[str, list[dict[str, Any]]] = {}
        for field in ("agent_cases", "agent_skills"):
            items = data.get(field, [])
            if not isinstance(items, list):
                raise ExperienceError(f"EverOS search {field} must be a list")
            result[field] = [_model_mapping(item, f"EverOS {field} item") for item in items]
        exact_cases: list[dict[str, Any]] = []
        for source_case in result["agent_cases"]:
            self._validate_case_scope(source_case)
            if source_case.get("session_id") == session_id:
                exact_cases.append(source_case)
        result["agent_cases"] = exact_cases
        for source_skill in result["agent_skills"]:
            self.validate_skill(source_skill)
        return result

    def validate_case_scope(self, source_case: Mapping[str, Any]) -> None:
        """Fail closed unless one returned case is exactly inside this scope.

        The case query below returns remote records that no ingestion session
        has reconciled yet, so this is the smallest read-only public check the
        scoped query and its callers can apply before any durable rejoin: the
        exact case id plus the bound owner, application, and project.
        """

        self._validate_case_scope(source_case)

    async def search_case_candidates(
        self, *, query: str, top_k: int = 100
    ) -> list[dict[str, Any]]:
        """Search this scope's EverOS case candidates without a session filter.

        This is the smallest read-only scoped query on top of the accepted
        public search surface: the exact bound root is re-checked, the accepted
        ``SearchRequest`` carries the exact application, project, and owner
        filters, and no ingestion-reconciliation receipt token is appended.
        The ingestion session is deliberately not part of this query, because a
        case is discoverable before any local ingestion exists; every hit is
        only a candidate and stays untrusted until its exact case receipt,
        confirmed ingestion, reviewed trajectory, and review receipt resolve in
        this same scope.

        Per-hit scope failures are omitted here so one foreign or malformed
        remote record can never discard an unrelated in-scope hit; a whole
        response or bound-root failure still raises, and the caller isolates
        it.
        """

        self._assert_bound_memory_root()
        if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
            raise ExperienceError("EverOS case query top_k must be a positive integer")
        safe_query = privacy_module.sanitize_text(query, self.privacy_policy).strip()
        if not safe_query:
            raise ExperienceError("EverOS case query became empty after sanitization")
        request = self.surface.make_search_request(
            agent_id=self.everos_owner_id,
            app_id=self.everos_application_id,
            project_id=self.everos_project_id,
            query=safe_query,
            method="keyword",
            top_k=int(top_k),
        )
        response = _model_mapping(
            await self.surface.search(request), "EverOS case search response"
        )
        data = response.get("data")
        if not isinstance(data, Mapping):
            raise ExperienceError("EverOS case search response has no data object")
        items = data.get("agent_cases", [])
        if not isinstance(items, list):
            raise ExperienceError("EverOS case search agent_cases must be a list")
        cases: list[dict[str, Any]] = []
        for item in items:
            try:
                source_case = _model_mapping(item, "EverOS agent case")
                self.validate_case_scope(source_case)
            except (ScopeBoundaryError, ExperienceError):
                # A foreign or malformed hit is omitted, never promoted and
                # never allowed to suppress an unrelated in-scope hit.
                continue
            cases.append(source_case)
        return cases

    async def search_skill_candidates(
        self, *, query: str, top_k: int = 100
    ) -> list[dict[str, Any]]:
        """Search this scope's EverOS skill candidates without a session filter.

        This is the smallest read-only scoped query on top of the accepted
        public search surface: the exact bound root is re-checked, the accepted
        ``SearchRequest`` carries the exact application, project, and owner
        filters, and no session token is appended.  Every returned mapping is
        raw discovery only: EverOS presence, similarity (``score``), and any
        Step-02 generated-skill approval are never procedural guidance here,
        nothing local is written, and no hit is promoted into guidance or a
        plan.  Whether one hit may later rejoin an exact durable trust chain is
        deliberately a separate decision outside this query.

        Per-hit scope failures are omitted here so one foreign or malformed
        remote record can never discard an unrelated in-scope hit; a whole
        response or bound-root failure still raises, and the caller isolates
        it.
        """

        self._assert_bound_memory_root()
        if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
            raise ExperienceError("EverOS skill query top_k must be a positive integer")
        safe_query = privacy_module.sanitize_text(query, self.privacy_policy).strip()
        if not safe_query:
            raise ExperienceError("EverOS skill query became empty after sanitization")
        request = self.surface.make_search_request(
            agent_id=self.everos_owner_id,
            app_id=self.everos_application_id,
            project_id=self.everos_project_id,
            query=safe_query,
            method="keyword",
            top_k=int(top_k),
        )
        response = _model_mapping(
            await self.surface.search(request), "EverOS skill search response"
        )
        data = response.get("data")
        if not isinstance(data, Mapping):
            raise ExperienceError("EverOS skill search response has no data object")
        items = data.get("agent_skills", [])
        if not isinstance(items, list):
            raise ExperienceError("EverOS skill search agent_skills must be a list")
        skills: list[dict[str, Any]] = []
        for item in items:
            try:
                source_skill = _model_mapping(item, "EverOS agent skill")
                self.validate_skill(source_skill)
            except (ScopeBoundaryError, ExperienceError):
                # A foreign or malformed hit is omitted, never promoted and
                # never allowed to suppress an unrelated in-scope hit.
                continue
            skills.append(source_skill)
        return skills

    def validate_case(self, source_case: Mapping[str, Any], *, session_id: str) -> None:
        self._validate_case_scope(source_case)
        value = source_case.get("session_id")
        if not isinstance(value, str) or not value:
            raise ScopeBoundaryError("EverOS case has no verified session_id")
        if value != session_id:
            raise ScopeBoundaryError("EverOS case session_id is outside the requested receipt")

    def _validate_case_scope(self, source_case: Mapping[str, Any]) -> None:
        required = {
            "id": None,
            "agent_id": self.everos_owner_id,
            "app_id": self.everos_application_id,
            "project_id": self.everos_project_id,
        }
        for field, expected in required.items():
            value = source_case.get(field)
            if not isinstance(value, str) or not value:
                raise ScopeBoundaryError(f"EverOS case has no verified {field}")
            if expected is not None and value != expected:
                raise ScopeBoundaryError(f"EverOS case {field} is outside the requested scope")

    def validate_skill(self, source_skill: Mapping[str, Any]) -> None:
        required = {
            "id": None,
            "agent_id": self.everos_owner_id,
            "app_id": self.everos_application_id,
            "project_id": self.everos_project_id,
        }
        for field, expected in required.items():
            value = source_skill.get(field)
            if not isinstance(value, str) or not value:
                raise ScopeBoundaryError(f"EverOS skill has no verified {field}")
            if expected is not None and value != expected:
                raise ScopeBoundaryError(f"EverOS skill {field} is outside the requested scope")


def _everos_identifier(prefix: str, value: str) -> str:
    # EverOS path-backed IDs need only a safe deterministic transport form;
    # raw product scope labels remain in the local reviewed receipt.
    digest = contracts.sha256_hex({"value": value})[:32]
    return f"mh-{prefix}-{digest}"


def _epoch_milliseconds(value: object) -> int:
    if not isinstance(value, str):
        raise ExperienceError("reviewed trajectory recorded_at must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExperienceError("reviewed trajectory recorded_at is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(1, int(parsed.timestamp() * 1000))


def _model_mapping(value: Any, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            dumped = model_dump(mode="json")
        except TypeError:
            dumped = model_dump()
        if isinstance(dumped, Mapping):
            return dict(dumped)
    raise ExperienceError(f"{label} must be a mapping or public model")


def _safe_error(exc: Exception, policy: PrivacyPolicy) -> str:
    return privacy_module.sanitize_text(f"{type(exc).__name__}: {exc}", policy)


def select_curated_guidance(
    visible_items: Iterable[Mapping[str, Any]],
    *,
    scope: ExperienceScope,
    selected_ids: Iterable[str],
) -> list[dict[str, Any]]:
    """Return only explicitly selected curated guidance in one exact scope.

    Visibility is never approval. This helper deliberately has no default
    selection path, so a newly visible repository/builtin item cannot become
    governing guidance by merely appearing in an adapter result.
    """

    requested = list(selected_ids)
    if len(requested) != len(set(requested)):
        raise ApprovalError("curated selection ids must be unique")
    if any(not isinstance(item_id, str) or not item_id for item_id in requested):
        raise ApprovalError("curated selection ids must be nonempty strings")
    visible: dict[str, Mapping[str, Any]] = {}
    for item in visible_items:
        item_id = item.get("id")
        if isinstance(item_id, str) and item_id:
            if item_id in visible:
                raise ApprovalError(f"curated item identity is ambiguous: {item_id}")
            visible[item_id] = item
    selected: list[dict[str, Any]] = []
    expected_scope = scope.to_record()
    for item_id in requested:
        item = visible.get(item_id)
        if item is None:
            raise ApprovalError(f"explicit curated item is not available: {item_id}")
        if item.get("origin") != "curated":
            raise ApprovalError("explicit curated selection cannot select generated guidance")
        item_scope = item.get("scope")
        if item_scope != expected_scope:
            raise ScopeBoundaryError("curated item is outside the requested scope")
        selected.append(dict(item))
    return selected


def _canonical_approval_recipients(recipients: Iterable[str]) -> tuple[str, ...]:
    if isinstance(recipients, str):
        raise ApprovalError("approval recipients must be a collection of identities")
    selected = list(recipients)
    if not selected or any(
        not isinstance(recipient, str) or not recipient.strip()
        for recipient in selected
    ):
        raise ApprovalError("approval recipients must be nonempty identities")
    if len(selected) != len(set(selected)):
        raise ApprovalError("approval recipients must be unique")
    return tuple(sorted(selected))


class ReviewedExperienceService:
    """Persist and query immutable reviewed trajectories before extraction.

    The service only accepts an outcome that is already durable in
    :class:`MemoryStore`. It retains protected raw evidence locally and exposes
    a sanitized historical-evidence projection; that projection is not
    procedural guidance and cannot execute or authorize anything.
    """

    def __init__(
        self,
        memory_store: MemoryStore,
        *,
        privacy_policy: PrivacyPolicy | None = None,
        approval_verifier: TrustedApprovalVerifier | None = None,
    ) -> None:
        self.store = memory_store
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self.approval_verifier = approval_verifier

    def capture(
        self,
        *,
        task_card: Mapping[str, Any],
        plan: Mapping[str, Any],
        decision: Mapping[str, Any],
        outcome: Mapping[str, Any],
        review_receipt: Mapping[str, Any],
        scope: ExperienceScope,
    ) -> dict[str, Any]:
        """Durably capture one reviewed terminal trajectory.

        The durable local outcome is read first so a caller cannot attach a
        syntactically valid receipt to a different decision or run.
        """

        persisted_outcome = self.store.get_outcome(str(decision.get("decision_id", "")))
        if persisted_outcome.get("outcome_id") != outcome.get("outcome_id"):
            raise contracts.ContractError(
                "reviewed trajectory outcome is not the durable outcome for its decision"
            )
        persisted_review_receipt = self.store.record_review_receipt(review_receipt)
        trajectory = contracts.make_reviewed_trajectory(
            task_card=task_card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=persisted_review_receipt,
            scope=scope.to_record(),
        )
        return self.store.record_reviewed_trajectory(trajectory)

    async def extract_trajectory(
        self,
        trajectory_id: str,
        adapter: EverOSAdapter,
        *,
        experience_write: bool = True,
        current_config: Mapping[str, Any] | config.MemoryConfig | None = None,
        claimant: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Submit one optional EverOS extraction without unsafe replay.

        The local intent exists before the networked call. Any existing intent,
        including one left uncertain by a lost response, is reconciled only by
        exact receipt lookup and is never sent to EverOS a second time here.
        When the captured operation disables experience writing, return the
        existing local intent unchanged (or ``None``) without contacting EverOS.
        """

        trajectory = self.get_trajectory(trajectory_id)
        adapter.assert_scope(trajectory["scope"])
        existing = self.store.get_experience_ingestion_for_trajectory(trajectory_id)
        effective = current_config or config.MemoryConfig()
        captured = self.store.get_decision(trajectory["decision_id"])["configuration"]
        if (not experience_write or
                not config.effect_submission_enabled(captured, "experience_ingestion") or
                not config.effect_submission_enabled(effective, "experience_ingestion")):
            return existing
        if claimant is not None:
            return await self._extract_claimed_trajectory(
                trajectory, adapter, claimant=claimant, current_config=effective,
            )
        try:
            self.store.get_effect_operation(self._external_ingestion_id(trajectory, adapter))
        except StoreError:
            pass
        else:
            if existing is None:
                raise ExperienceError("source-owned EverOS claim has no ingestion")
            return await self.reconcile_extraction(
                trajectory_id, adapter, current_config=effective,
            )
        if existing is not None:
            if existing["status"] in {"pending", "uncertain"}:
                return await self.reconcile_extraction(
                    trajectory_id, adapter, experience_write=experience_write,
                    current_config=effective,
                )
            return existing
        session_id = adapter.session_id_for(trajectory_id)
        payload = adapter.add_payload(trajectory, session_id=session_id)
        intent = contracts.make_experience_ingestion(
            trajectory=trajectory,
            destination=adapter.destination,
            session_id=session_id,
            payload_digest=contracts.sha256_hex(payload),
        )
        operation_id = contracts.effect_operation_id(trajectory["outcome_id"], "experience_ingestion")
        try:
            operation = self.store.get_effect_operation(operation_id)
        except StoreError:
            operation = None  # Accepted legacy outcomes have no effect operation.
        if operation is not None:
            self.store.bind_effect_source(operation_id, trajectory_id, trajectory)
            operation = self.store.bind_effect_payload(operation_id, payload)
            if operation["status"] == "pending":
                try:
                    self.store.claim_effect_operation(operation_id, current_config=effective)
                except OperationConflictError:
                    # A peer can win the claim. The ingestion insert below chooses
                    # the sole submitter using the same durable identity.
                    if self.store.get_effect_operation(operation_id)["status"] != "in_flight":
                        raise
        ingestion, created = self.store.create_experience_ingestion(intent)
        if not created:
            return ingestion
        try:
            await adapter.memorize(payload)
        except Exception as exc:
            # The public API does not expose a source-backed idempotency key for
            # this write. Treat every failed/lost result as potentially committed.
            try:
                return self.store.update_experience_ingestion(
                    ingestion["ingestion_id"],
                    status="uncertain",
                    expected_version=ingestion["version"],
                    error=_safe_error(exc, self.privacy_policy),
                )
            except ExperienceConflictError as update_error:
                latest = self.store.get_experience_ingestion(ingestion["ingestion_id"])
                if latest["version"] != ingestion["version"]:
                    return latest
                raise update_error from exc
        return await self.reconcile_extraction(
            trajectory_id, adapter, experience_write=experience_write,
            current_config=effective,
        )

    @staticmethod
    def _external_ingestion_id(trajectory: Mapping[str, Any], adapter: EverOSAdapter) -> str:
        scope_key = contracts.sha256_hex({
            "scope": trajectory["scope"], "destination": adapter.destination,
        })
        return contracts.external_effect_operation_id(
            trajectory["trajectory_id"], "experience_ingestion", scope_key,
        )

    async def _extract_claimed_trajectory(
        self, trajectory: Mapping[str, Any], adapter: EverOSAdapter, *,
        claimant: Mapping[str, Any], current_config: Mapping[str, Any] | config.MemoryConfig,
    ) -> dict[str, Any]:
        """Claim the source-owned effect before one public EverOS submission."""
        trajectory_id = trajectory["trajectory_id"]
        session_id = adapter.session_id_for(trajectory_id)
        payload = adapter.add_payload(trajectory, session_id=session_id)
        captured = self.store.get_decision(trajectory["decision_id"])["configuration"]
        operation_id = self._external_ingestion_id(trajectory, adapter)
        ingestion = self.store.get_experience_ingestion_for_trajectory(trajectory_id)
        try:
            previous = self.store.get_effect_operation(operation_id)
        except StoreError:
            previous = None
        if ingestion is not None and previous is None:
            return await self.reconcile_extraction(
                trajectory_id, adapter, current_config=current_config,
            )
        operation, _ = self.store.create_external_effect_operation(
            kind="experience_ingestion",
            scope_key=contracts.sha256_hex({
                "scope": trajectory["scope"], "destination": adapter.destination,
            }),
            source_id=trajectory_id, source_record=trajectory, payload=payload,
            captured_config=captured, current_config=current_config,
        )
        if (operation["claimant_record"] is not None and
                operation["claimant_record"] != dict(claimant)):
            raise OperationConflictError("EverOS operation belongs to a different claimant")
        if operation["status"] != "pending" or operation["claim_generation"] > 0:
            if ingestion is None:
                raise ExperienceError("claimed EverOS operation has no ingestion")
            return await self.reconcile_extraction(
                trajectory_id, adapter, current_config=current_config,
            )
        try:
            claim = self.store.claim_effect_operation(
                operation_id, current_config=current_config, claimant=claimant,
            )
        except OperationConflictError:
            ingestion = self.store.get_experience_ingestion_for_trajectory(trajectory_id)
            if ingestion is None:
                raise
            return await self.reconcile_extraction(
                trajectory_id, adapter, current_config=current_config,
            )
        intent = contracts.make_experience_ingestion(
            trajectory=trajectory, destination=adapter.destination,
            session_id=session_id, payload_digest=contracts.sha256_hex(payload),
        )
        ingestion, created = self.store.create_experience_ingestion(intent)
        if not created:
            raise OperationConflictError("claimed EverOS ingestion already exists")
        usage = contracts.make_native_usage_start(
            source="everos:memorize", invocation_id=claimant["native_invocation_id"],
            objective_id=trajectory["objective_id"], decision_id=trajectory["decision_id"],
            maintenance_operation_id=None, stage="experience_ingestion",
            category="inner_candidate", window_id=trajectory["run_id"],
            binding={"requested": None, "resolved": None, "native": None},
        )
        self.store.start_native_usage(usage)
        result: Mapping[str, Any] | None = None
        failure: Exception | None = None
        try:
            result = await adapter.memorize(payload)
        except Exception as exc:
            failure = exc
        reason = (_safe_error(failure, self.privacy_policy) if failure is not None
                  else "awaiting exact EverOS readback")
        effect, settled, _ = self.store.settle_external_experience_ingestion(
            operation_id, ingestion["ingestion_id"],
            claim_id=claim["active_claim_id"],
            claim_generation=claim["claim_generation"], mode="uncertain",
            reason=reason, ingestion_uncertain=failure is not None,
        )
        if settled["status"] == "confirmed":
            acknowledgement = effect["acknowledgement"]
            _, settled, _ = self.store.settle_external_experience_ingestion(
                operation_id, ingestion["ingestion_id"],
                claim_id=claim["active_claim_id"],
                claim_generation=claim["claim_generation"], mode="acknowledged",
                evidence=effect["reconciliation"],
                case_receipts=tuple(acknowledgement["case_receipts"]),
            )
        if result is not None:
            self.record_memorize_usage_result(trajectory_id, adapter, claimant, result)
        if settled["status"] == "confirmed":
            return settled
        return await self.reconcile_extraction(
            trajectory_id, adapter, current_config=current_config,
        )

    def record_memorize_usage_result(
        self, trajectory_id: str, adapter: EverOSAdapter,
        claimant: Mapping[str, Any], result: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Accept an authentic source result, including one delivered late, once."""
        operation = self.store.get_effect_operation(self._external_ingestion_id(
            self.get_trajectory(trajectory_id), adapter,
        ))
        if operation["claimant_record"] != dict(claimant):
            raise OperationConflictError("EverOS usage result has a different claimant")
        message_count = result.get("message_count")
        if type(message_count) is not int or message_count < 0:
            return None
        usage = self.store.get_native_usage("everos:memorize", claimant["native_invocation_id"])
        receipt = contracts.make_native_usage_receipt(
            source=usage["source"], invocation_id=usage["invocation_id"],
            receipt_id=contracts.sha256_hex({
                "source": usage["source"], "invocation_id": usage["invocation_id"],
                "measure": "message_count",
            }),
            objective_id=usage["objective_id"], decision_id=usage["decision_id"],
            maintenance_operation_id=None, stage=usage["stage"],
            category=usage["category"], window_id=usage["window_id"],
            binding=usage["binding"], mode="cumulative", complete=False,
            measures={"messages": {"value": message_count, "unit": "messages",
                                    "included_in_total": None}},
        )
        return self.store.record_native_usage_receipt(receipt)

    async def reconcile_extraction(
        self,
        trajectory_id: str,
        adapter: EverOSAdapter,
        *,
        experience_write: bool = True,
        current_config: Mapping[str, Any] | config.MemoryConfig | None = None,
    ) -> dict[str, Any] | None:
        """Confirm exact case receipts, or pause reconciliation while writing is off."""

        trajectory = self.get_trajectory(trajectory_id)
        adapter.assert_scope(trajectory["scope"])
        ingestion = self.store.get_experience_ingestion_for_trajectory(trajectory_id)
        effective = current_config or config.MemoryConfig()
        captured = self.store.get_decision(trajectory["decision_id"])["configuration"]
        try:
            source_operation = self.store.get_effect_operation(
                self._external_ingestion_id(trajectory, adapter)
            )
        except StoreError:
            source_operation = None
        if (not experience_write or
                not config.effect_submission_enabled(captured, "experience_ingestion") or
                (not config.effect_submission_enabled(effective, "experience_ingestion") and
                 (source_operation is None or source_operation["status"] not in
                  {"in_flight", "uncertain", "confirmed"}))):
            return ingestion
        if ingestion is None:
            raise ExperienceError("no reviewed-experience ingestion exists to reconcile")
        if ingestion["status"] == "confirmed":
            return ingestion
        if source_operation is not None and (
            adapter.surface.get is None or adapter.surface.make_get_request is None
        ):
            return ingestion  # Ranked search cannot prove a claimed mutation.
        try:
            source_cases = await adapter.readback_cases(
                session_id=ingestion["session_id"], query=trajectory["task_text"]
            )
        except Exception:
            # Readback is read-only. Leave local state truthful and retain
            # the recent evidence rather than relabeling unknown remote state.
            return ingestion
        receipts: list[dict[str, Any]] = []
        for source_case in source_cases:
            adapter.validate_case(source_case, session_id=ingestion["session_id"])
            sanitized_case = privacy_module.sanitize_payload(source_case, self.privacy_policy)
            if not isinstance(sanitized_case, Mapping):
                raise ExperienceError("sanitized EverOS case must remain an object")
            receipts.append(
                contracts.make_case_receipt(
                    trajectory=trajectory,
                    ingestion=ingestion,
                    source_case=sanitized_case,
                )
            )
        if not receipts:
            return ingestion
        try:
            if source_operation is not None:
                evidence = {key: source_operation[key] for key in (
                    "operation_id", "kind", "scope_key", "source_digest",
                    "payload_digest", "configuration_digest",
                )}
                evidence["adapter_proof"] = {
                    "session_id": ingestion["session_id"],
                    "scope": adapter.scope.to_record(),
                    "case_ids": sorted(receipt["case_id"] for receipt in receipts),
                }
                _, settled, _ = self.store.settle_external_experience_ingestion(
                    source_operation["operation_id"], ingestion["ingestion_id"],
                    claim_id=source_operation["active_claim_id"],
                    claim_generation=source_operation["claim_generation"],
                    mode="acknowledged", evidence=evidence,
                    case_receipts=tuple(receipts),
                )
                return settled
            return self.store.confirm_experience_ingestion(
                ingestion["ingestion_id"], receipts,
                expected_version=ingestion["version"],
            )
        except ExperienceConflictError:
            latest = self.store.get_experience_ingestion(ingestion["ingestion_id"])
            if latest["version"] != ingestion["version"]:
                return latest
            raise

    def resolve_generated_skill_candidate(
        self,
        source_skill: Mapping[str, Any],
        adapter: EverOSAdapter,
        *,
        generated_skill_creation: bool = True,
        current_config: Mapping[str, Any] | config.MemoryConfig | None = None,
    ) -> dict[str, Any] | None:
        """Resolve a returned EverOS skill to exact reviewed source receipts.

        The result remains a generated, non-authoritative candidate. This
        operation neither approves the candidate nor executes any text/script it
        contains. Disabling creation suppresses local candidate persistence;
        EverOS's public memorize call has no per-call skill-only extraction gate.
        """

        if not generated_skill_creation:
            return None
        adapter.validate_skill(source_skill)
        source_case_ids = source_skill.get("source_case_ids")
        if not isinstance(source_case_ids, list) or not source_case_ids:
            raise ProvenanceError("generated EverOS skill has no source case ids")
        if any(not isinstance(case_id, str) or not case_id for case_id in source_case_ids):
            raise ProvenanceError("generated EverOS skill has an invalid source case id")
        if len(source_case_ids) != len(set(source_case_ids)):
            raise ProvenanceError("generated EverOS skill source case ids are ambiguous")
        sources: list[dict[str, str]] = []
        trajectories: dict[str, dict[str, Any]] = {}
        scope = adapter.scope.to_record()
        for case_id in source_case_ids:
            try:
                receipt = self.store.get_case_receipt(scope, case_id)
                trajectory = self.store.get_reviewed_trajectory(receipt["trajectory_id"])
            except Exception as exc:
                raise ProvenanceError(
                    f"generated EverOS skill source case cannot be resolved: {case_id}"
                ) from exc
            if receipt["scope"] != scope or trajectory["scope"] != scope:
                raise ProvenanceError("generated EverOS skill source case crosses scope")
            if not is_reviewed(trajectory) or trajectory["review_state"] not in contracts.REVIEW_STATES:
                raise ProvenanceError("generated EverOS skill source case is not reviewed")
            if receipt["review_receipt_id"] != trajectory["review_receipt_id"]:
                raise ProvenanceError("generated EverOS skill source receipt is inconsistent")
            trajectories[trajectory["trajectory_id"]] = trajectory
            sources.append(
                {
                    "case_id": case_id,
                    "case_receipt_id": receipt["case_receipt_id"],
                    "trajectory_id": trajectory["trajectory_id"],
                    "review_receipt_id": trajectory["review_receipt_id"],
                    "review_receipt_digest": trajectory["review_receipt_digest"],
                }
            )
        skill_id = source_skill.get("id")
        content = source_skill.get("content")
        if not isinstance(skill_id, str) or not skill_id:
            raise ProvenanceError("generated EverOS skill has no exact id")
        if not isinstance(content, str) or not content:
            raise ProvenanceError("generated EverOS skill has no content")
        sanitized_content = privacy_module.sanitize_text(content, self.privacy_policy)
        metadata = privacy_module.sanitize_payload(
            {
                key: source_skill[key]
                for key in ("name", "description", "confidence", "maturity_score")
                if key in source_skill
            },
            self.privacy_policy,
        )
        if not isinstance(metadata, Mapping):
            raise ExperienceError("sanitized generated skill metadata must remain an object")
        candidate = contracts.make_generated_skill_candidate(
            scope=scope,
            skill_id=skill_id,
            content=sanitized_content,
            source_cases=sources,
            metadata=metadata,
        )
        effective = current_config or config.MemoryConfig()
        if not config.effect_submission_enabled(effective, "generated_skill_creation"):
            return None
        operations: dict[str, str] = {}
        for trajectory in trajectories.values():
            captured = self.store.get_decision(trajectory["decision_id"])["configuration"]
            if not config.effect_submission_enabled(captured, "generated_skill_creation"):
                return None
            operation_id = contracts.effect_operation_id(
                trajectory["outcome_id"], "generated_skill_creation"
            )
            try:
                self.store.get_effect_operation(operation_id)
            except StoreError:
                continue  # Accepted legacy outcomes have no effect operation.
            operations[operation_id] = trajectory["trajectory_id"]
            self.store.bind_effect_source(
                operation_id, trajectory["trajectory_id"], trajectory
            )
        candidate_payload = {
            key: candidate[key]
            for key in ("candidate_id", "skill_id", "content_digest", "source_cases", "metadata")
        }
        acknowledgements: dict[str, dict[str, str]] = {}
        for operation_id, trajectory_id in operations.items():
            operation = self.store.get_effect_operation(operation_id)
            if operation["status"] == "confirmed":
                # The candidate store owns each distinct proposed skill.
                continue
            # The effect belongs to the reviewed outcome. Exact generated skill
            # identities and provenance live in the candidate store below.
            payload = {"source_trajectory_id": trajectory_id}
            acknowledgement = payload
            retained = operation["payload_record"]
            if retained is not None and set(retained) == set(candidate_payload):
                # Older writers bound this one outcome slot to one candidate.
                # Another valid candidate must not rebind or confirm that claim.
                if retained != candidate_payload:
                    continue
                payload = retained
                acknowledgement = {"candidate_id": candidate["candidate_id"]}
            operation = self.store.bind_effect_payload(operation_id, payload)
            acknowledgements[operation_id] = acknowledgement
            if operation["status"] == "pending":
                try:
                    self.store.claim_effect_operation(operation_id, current_config=effective)
                except OperationConflictError:
                    if self.store.get_effect_operation(operation_id)["status"] not in (
                        "in_flight", "uncertain", "confirmed"
                    ):
                        raise
        persisted = self.store.record_generated_skill_candidate(candidate)
        for operation_id, acknowledgement in acknowledgements.items():
            self.store.confirm_effect_operation(operation_id, acknowledgement)
        return persisted

    def approve_generated_skill(
        self,
        *,
        candidate_id: str,
        scope: ExperienceScope,
        approval_id: str,
        issuer: str,
        recipients: Iterable[str],
        approved_at: str,
    ) -> dict[str, Any]:
        """Apply explicit approval only after every exact reviewed source rechecks."""

        candidate = self.store.get_generated_skill_candidate(candidate_id)
        expected_scope = scope.to_record()
        if candidate["scope"] != expected_scope:
            raise ApprovalError("generated skill candidate is outside the approval scope")
        if candidate["origin"] != "generated":
            raise ApprovalError("only generated candidates use this approval path")
        if candidate["state"] != "proposed":
            raise ApprovalError("generated skill candidate is not a proposed candidate")
        selected_recipients = _canonical_approval_recipients(recipients)
        for source in candidate["source_cases"]:
            try:
                receipt = self.store.get_case_receipt(expected_scope, source["case_id"])
                trajectory = self.store.get_reviewed_trajectory(source["trajectory_id"])
            except Exception as exc:
                raise ApprovalError("generated skill source receipt is no longer resolvable") from exc
            expected = {
                "case_receipt_id": receipt["case_receipt_id"],
                "trajectory_id": trajectory["trajectory_id"],
                "review_receipt_id": trajectory["review_receipt_id"],
                "review_receipt_digest": trajectory["review_receipt_digest"],
            }
            if any(source.get(field) != value for field, value in expected.items()):
                raise ApprovalError("generated skill source receipt changed or is ambiguous")
            if trajectory["scope"] != expected_scope or not is_reviewed(trajectory):
                raise ApprovalError("generated skill source trajectory is not reviewed in scope")
        verifier = self.approval_verifier
        if verifier is None:
            raise ApprovalError("trusted generated-skill approval verifier is not configured")
        try:
            verified = verifier.verify_generated_skill_approval(
                issuer=issuer,
                candidate=candidate,
                scope=expected_scope,
                recipients=selected_recipients,
            )
        except ApprovalError:
            raise
        except Exception as exc:
            raise ApprovalError("trusted approval verifier rejected the request") from exc
        self._validate_verified_approval(
            verified=verified,
            issuer=issuer,
            candidate=candidate,
            scope=expected_scope,
            recipients=selected_recipients,
        )
        approval = contracts.make_skill_approval(
            approval_id=approval_id,
            candidate=candidate,
            issuer=issuer,
            recipients=selected_recipients,
            approved_at=approved_at,
            authority_evidence=verified.authority_evidence,
        )
        return self.store.record_skill_approval(approval)

    @staticmethod
    def _validate_verified_approval(
        *,
        verified: VerifiedApproval,
        issuer: str,
        candidate: Mapping[str, Any],
        scope: Mapping[str, Any],
        recipients: tuple[str, ...],
    ) -> None:
        if not isinstance(verified, VerifiedApproval):
            raise ApprovalError("trusted approval verifier returned no verified authority")
        if verified.issuer != issuer:
            raise ApprovalError("trusted approval issuer does not match the request")
        if verified.candidate_id != candidate["candidate_id"]:
            raise ApprovalError("trusted approval does not bind the exact candidate")
        try:
            verified_scope = contracts.normalize_experience_scope(verified.scope)
        except contracts.ContractError as exc:
            raise ApprovalError("trusted approval has an invalid scope") from exc
        if verified_scope != scope:
            raise ApprovalError("trusted approval scope does not match the request")
        if tuple(verified.recipients) != recipients:
            raise ApprovalError("trusted approval recipients do not match the request")
        try:
            contracts.make_skill_approval(
                approval_id="authority-validation",
                candidate=candidate,
                issuer=issuer,
                recipients=recipients,
                approved_at="authority-validation",
                authority_evidence=verified.authority_evidence,
            )
        except contracts.ContractError as exc:
            raise ApprovalError("trusted approval has no retained authority evidence") from exc

    def get_trajectory(self, trajectory_id: str) -> dict[str, Any]:
        return self.store.get_reviewed_trajectory(trajectory_id)

    def confirmed_case_evidence(
        self, scope: ExperienceScope, source_case: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Return sanitized evidence for one durable confirmed reviewed case.

        A remote case is discovery, never authority.  This method accepts one
        already scope-validated EverOS case and returns a sanitized
        historical-evidence projection only after the exact durable chain
        resolves in the same scope:

        - the case receipt recorded for this exact case id and scope,
        - its confirmed experience ingestion (same trajectory, same scope),
        - the reviewed, reviewed-state trajectory behind that ingestion, and
        - the durable review receipt the trajectory still binds.

        The stored sanitized source case must still match this remote hit on
        every stable identity/content field, so an altered, unresolved,
        unreviewed, unconfirmed, cross-scope, or stale remote hit returns
        ``None`` instead of being promoted.  Only the query-dependent ranking
        metadata the accepted public surface returns on every hit (the exact
        ``SearchAgentCaseItem.score`` of the ranking query) may differ between
        confirmation and this later search.  The returned projection is
        historical evidence only: it carries no raw evidence, no protected
        references, no procedural steps, and no plan authority.
        """

        expected_scope = scope.to_record()
        case_id = source_case.get("id") if isinstance(source_case, Mapping) else None
        if not isinstance(case_id, str) or not case_id.strip():
            return None
        sanitized_case = privacy_module.sanitize_payload(source_case, self.privacy_policy)
        if not isinstance(sanitized_case, Mapping):
            return None
        try:
            join = self.store.read_confirmed_case_join(expected_scope, case_id)
        except Exception:
            # Missing, unconfirmed, altered, unreviewed, or out-of-scope
            # durable state omits this hit instead of trusting the remote case.
            return None
        receipt = join["case_receipt"]
        ingestion = join["ingestion"]
        trajectory = join["trajectory"]
        if (
            receipt["scope"] != expected_scope
            or ingestion["scope"] != expected_scope
            or trajectory["scope"] != expected_scope
        ):
            return None
        if ingestion["status"] != "confirmed" or case_id not in ingestion["case_ids"]:
            return None
        if ingestion["trajectory_id"] != receipt["trajectory_id"]:
            return None
        if not is_reviewed(trajectory) or trajectory["review_state"] not in contracts.REVIEW_STATES:
            return None
        if (
            receipt["review_receipt_id"] != trajectory["review_receipt_id"]
            or receipt["review_receipt_digest"] != trajectory["review_receipt_digest"]
        ):
            return None
        stored_source_case = receipt["source_case"]
        if not isinstance(stored_source_case, Mapping):
            return None
        if receipt["source_case_digest"] != contracts.sha256_hex(stored_source_case):
            # The durable receipt no longer matches its own retained digest.
            return None
        if _stable_case_content(stored_source_case) != _stable_case_content(
            sanitized_case
        ):
            # A stable identity or content field changed after confirmation and
            # the remote case is omitted as altered; only the query-dependent
            # ranking metadata may differ between the confirming query and this
            # later discovery query.
            return None
        projection = _trajectory_evidence_projection(trajectory, self.privacy_policy)
        projection["case_id"] = case_id
        projection["case_receipt_id"] = receipt["case_receipt_id"]
        return projection

    def search_recent_evidence(
        self, scope: ExperienceScope, query: str
    ) -> list[dict[str, Any]]:
        """Return sanitized local evidence while representation is absent.

        This deliberately simple local lookup is only a recovery/searchability
        bridge. Later fixed-strategy retrieval decides how optional memory is
        ranked or delivered; this method never promotes historical text into a
        skill or accepted plan.
        """

        if not isinstance(query, str) or not query.strip():
            raise ValueError("recent-evidence query must be a nonempty string")
        expected_scope = scope.to_record()
        tokens = _search_tokens(query)
        results: list[dict[str, Any]] = []
        for trajectory in self.store.list_recent_reviewed_trajectories(expected_scope):
            if trajectory.get("scope") != expected_scope:
                # A digest match is not enough authority to cross scope.
                continue
            projection = _trajectory_evidence_projection(trajectory, self.privacy_policy)
            if tokens and not tokens.intersection(_search_tokens(projection["content"])):
                continue
            results.append(projection)
        return results


# The accepted public EverOS case surface returns ``SearchAgentCaseItem.score``
# on every hit, and that value is the ranking result of the exact query that
# produced the hit.  STEP-02 confirmation and a later preparation search use
# different queries, so the same unchanged case legitimately comes back with
# another score; every other returned field is stable case identity or content
# and must still match the durable receipt exactly.
_QUERY_DEPENDENT_CASE_FIELDS = frozenset({"score"})


def _stable_case_content(case: Mapping[str, Any]) -> dict[str, Any]:
    """Return one sanitized case without its query-dependent ranking metadata."""

    return {
        key: value
        for key, value in case.items()
        if key not in _QUERY_DEPENDENT_CASE_FIELDS
    }


def _trajectory_evidence_projection(
    trajectory: Mapping[str, Any], policy: PrivacyPolicy
) -> dict[str, Any]:
    derived = privacy_module.sanitize_payload(
        {
            "task": trajectory["task_text"],
            "evidence": trajectory["raw_evidence"],
            "failed_hypotheses": trajectory["failed_hypotheses"],
        },
        policy,
    )
    assert isinstance(derived, dict)
    hypotheses = derived["failed_hypotheses"]
    rendered_hypotheses = "\n".join(f"- {item}" for item in hypotheses)
    content = "\n".join(
        part
        for part in (
            f"Task: {derived['task']}",
            f"Historical evidence: {derived['evidence']}",
            f"Disproved hypotheses:\n{rendered_hypotheses}" if hypotheses else "",
        )
        if part
    )
    return {
        "id": trajectory["trajectory_id"],
        "kind": "historical_evidence",
        "authority": "reviewed_historical_evidence",
        "status": trajectory["status"],
        "content": content,
        "evidence_refs": list(trajectory["evidence_refs"]),
        "review_receipt_id": trajectory["review_receipt_id"],
        "scope": dict(trajectory["scope"]),
    }


def _search_tokens(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9_]+", value.casefold()))


def is_reviewed(record: Mapping[str, Any]) -> bool:
    return record.get("status") in {
        "reviewed_success",
        "reviewed_failure",
        "disproved_hypothesis",
    }


__all__ = [
    "ApprovalError",
    "EverOSAdapter",
    "EverOSPublicSurface",
    "EverOSUnavailableError",
    "ExperienceRecord",
    "ExperienceError",
    "ExperienceScope",
    "ProvenanceError",
    "ReviewedExperienceService",
    "ScopeBoundaryError",
    "SYNTHETIC_SECRET",
    "TrustedApprovalVerifier",
    "VerifiedApproval",
    "derived_optional_content",
    "is_reviewed",
    "load_vendored_everos_public_surface",
    "load_experience_examples",
    "select_curated_guidance",
]

