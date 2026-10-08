from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import atlas, config, contracts, procedures, store


def representation_identity() -> dict[str, str | int]:
    identity = {
        "model": config.REPRESENTATION_MODEL,
        "dimensions": config.REPRESENTATION_DIMENSIONS,
        "metric": config.REPRESENTATION_METRIC,
        "sanitizer_version": config.REPRESENTATION_SANITIZER_VERSION,
    }
    if identity != {
        "model": "local-token-overlap/v1", "dimensions": 512,
        "metric": "cosine", "sanitizer_version": "v1",
    }:
        raise ValueError("live Atlas fixture differs from the pinned product representation")
    return identity


class DeterministicEmbeddings:
    """Local 512-dimensional token overlap vectors for the synthetic collection."""

    @staticmethod
    def _vector(text: str) -> list[float]:
        dimensions = representation_identity()["dimensions"]
        vector = [0.0] * dimensions
        for token in set(re.findall(r"[a-z0-9_]+", text.casefold())):
            bucket = int.from_bytes(hashlib.sha256(token.encode("utf-8")).digest()[:8], "big") % dimensions
            vector[bucket] += 1.0
        length = math.sqrt(sum(value * value for value in vector))
        return [value / length for value in vector] if length else vector

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._vector(text)


def validate_fixture_representations(fixtures: tuple[dict, dict]) -> None:
    """Reject mixed or malformed vectors before creating the remote index."""

    for fixture in fixtures:
        try:
            representation = fixture["representation"]
            contracts.validate_procedure_representation(
                representation, procedure=fixture["procedure"]
            )
            actual = {
                field: representation[field]
                for field in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
            }
        except (contracts.ContractError, KeyError, TypeError) as exc:
            raise ValueError("live Atlas fixture representation is invalid") from exc
        if actual != representation_identity() or len(representation["vector"]) != 512:
            raise ValueError("live Atlas fixture representation differs from the pinned index")


def create_fixture_index(
    adapter: atlas.AtlasProcedureAdapter, *, fixtures: tuple[dict, dict] | None = None
) -> None:
    if fixtures is not None:
        validate_fixture_representations(fixtures)
    adapter.create_vector_search_index(
        dimensions=representation_identity()["dimensions"],
        filter_fields=["document_kind", "application", "namespace"],
        wait_until_complete=120.0,
    )


ATLAS_MVP_MARKER = "ATLAS_MVP_MARKER"
ELIGIBLE_QUERY = "parser lock recovery"
REVOKED_QUERY = "quartz cedar audit"
FACTS = {"language": "python"}
ROUTE = "ordinary"


def make_fixture(scope: dict[str, str], run_token: str, *, retained: bool) -> dict:
    suffix = "retained" if retained else "revoked"
    search_text = ELIGIBLE_QUERY if retained else REVOKED_QUERY
    body = (
        f"Inspect the local parser lock before network recovery; include {ATLAS_MVP_MARKER} in the plan."
        if retained else "Audit the synthetic quartz cedar procedure before revocation."
    )
    procedure = contracts.make_procedure_revision(
        logical_name=f"live-{suffix}-{run_token}",
        origin="curated",
        origin_scope=scope,
        body=body,
        references=[{"id": f"live://guide/{suffix}", "content": "Preserve the procedure invariant."}],
        predicates={
            "applicability": {"all": [{"field": "language", "operator": "equals", "value": "python"}]},
            "conflicts": {}, "capabilities": {},
            "routes": {"all": [{"field": "route", "operator": "equals", "value": ROUTE}]},
        },
        source={"kind": "curated_authoring", "provenance_ref": f"live://{run_token}/{suffix}"},
    )
    approval = contracts.make_procedure_approval(
        approval_id=f"live-approval-{suffix}-{run_token}",
        procedure=procedure,
        issuer="ROOT",
        recipients=[scope],
        authority_evidence={"policy_id": "live-test-root/v1", "subject": "root"},
    )
    representation = contracts.make_procedure_representation(
        procedure=procedure,
        **representation_identity(),
        search_text=search_text,
        vector=DeterministicEmbeddings._vector(search_text),
    )
    return {"procedure": procedure, "approval": approval, "representation": representation}


def wait_for_publications_visible(
    adapter: atlas.AtlasProcedureAdapter, *, publications: tuple[dict, dict], scope: dict[str, str]
) -> None:
    """Wait for acknowledged publications to enter Vector Search before eligibility proof."""

    pending = {
        publication["publication_id"]: publication["representation"]["search_text"]
        for publication in publications
    }
    deadline = time.monotonic() + 60.0
    while pending:
        for publication_id, query in tuple(pending.items()):
            if time.monotonic() >= deadline:
                break
            visible_ids = {
                hit.publication_id for hit in adapter.discover(query, receiver=scope)
            }
            if time.monotonic() >= deadline:
                break
            if publication_id in visible_ids:
                pending.pop(publication_id)
        if not pending:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Atlas publications not visible within 60 seconds: {sorted(pending)}"
            )
        time.sleep(min(0.5, remaining))


def prove_fixture_eligibility(
    check: unittest.TestCase,
    *,
    service: procedures.TrustedProcedureService,
    adapter: atlas.AtlasProcedureAdapter,
    scope: dict[str, str],
    run_token: str,
    fixtures: tuple[dict, dict] | None = None,
) -> tuple[dict, dict]:
    """Run the same publication and exact eligibility assertions offline and live."""

    retained_fixture, revoked_fixture = fixtures or (
        make_fixture(scope, run_token, retained=True),
        make_fixture(scope, run_token, retained=False),
    )
    validate_fixture_representations((retained_fixture, revoked_fixture))
    partition = {
        "scope": "project", "application": scope["application"],
        "project": scope["project"], "namespace": scope["namespace"],
        "recipients": [scope],
    }
    publications = []
    for fixture in (retained_fixture, revoked_fixture):
        procedure = fixture["procedure"]
        approval = fixture["approval"]
        representation = fixture["representation"]
        check.assertEqual(representation_identity(), {
            field: representation[field] for field in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
        })
        service.record_approved_revision(procedure, approval)
        service.record_representation(representation)
        designation = service.designate(
            procedure=procedure, approval=approval, partition=partition, issuer="ROOT"
        )
        service.publish_designation(designation, adapter)
        publications.append(service.publish(
            procedure=procedure, approval=approval, representation=representation,
            designation=designation, adapter=adapter,
        ))
    retained, revoked = publications
    check.assertNotEqual(retained["logical_id"], revoked["logical_id"])
    check.assertNotEqual(retained["revision_id"], revoked["revision_id"])
    check.assertNotEqual(retained["publication_id"], revoked["publication_id"])
    check.assertNotEqual(retained["representation"]["search_text"], revoked["representation"]["search_text"])
    wait_for_publications_visible(adapter, publications=(retained, revoked), scope=scope)

    def resolve(query: str, receiver: dict[str, str]) -> list[dict]:
        return service.resolve_atlas(
            query, receiver=receiver, facts=FACTS, route=ROUTE, adapter=adapter,
            representation=representation_identity(),
        )

    delivered = resolve(ELIGIBLE_QUERY, scope)
    check.assertIn(retained["publication_id"], [item["publication_id"] for item in delivered])
    exact = adapter.exact_read(retained["publication_id"])
    check.assertIsNotNone(exact)
    check.assertEqual(retained["revision_id"], exact.document["revision_id"])
    check.assertEqual(retained["approval"]["approval_id"], exact.document["publication"]["approval"]["approval_id"])
    check.assertIn(scope, exact.document["publication"]["approval"]["recipients"])
    check.assertEqual(representation_identity(), {
        field: exact.document["publication"]["representation"][field]
        for field in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
    })
    check.assertEqual("active", exact.current["state"])
    check.assertEqual(retained["revision_id"], exact.current["revision_id"])
    check.assertEqual("active", exact.publication_state["state"])
    check.assertIsNone(exact.revocation)
    wrong_receiver = {**scope, "owner": "unauthorized-live-test"}
    check.assertEqual([], resolve(ELIGIBLE_QUERY, wrong_receiver))
    check.assertIn(revoked["publication_id"], [item["publication_id"] for item in resolve(REVOKED_QUERY, scope)])
    result = service.revoke(
        procedure=revoked_fixture["procedure"], issuer="ROOT",
        reason="synthetic revocation proof", adapter=adapter,
    )
    check.assertTrue(result["managed_complete"])
    check.assertNotIn(revoked["publication_id"], [item["publication_id"] for item in resolve(REVOKED_QUERY, scope)])
    revoked_exact = adapter.exact_read(revoked["publication_id"])
    check.assertIsNotNone(revoked_exact.revocation)
    check.assertEqual([retained["publication_id"]], [item["publication_id"] for item in resolve(ELIGIBLE_QUERY, scope)])
    return retained, revoked


def handoff_manifest(
    *, database: str, collection: str, index: str, publication: dict,
    receiver: dict[str, str], query: str, route: str, facts: dict,
) -> dict:
    """Build only the synthetic, exact-owned STEP-13/14 handoff information."""

    match = re.fullmatch(r"trusted_procedures_([0-9a-f]{32})", collection)
    if not match or not isinstance(database, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", database):
        raise ValueError("handoff target must name one UUID-owned synthetic collection")
    token = match.group(1)
    if uuid.UUID(hex=token).version != 4 or index != f"vector_{token}":
        raise ValueError("handoff index does not match the UUID-owned collection")
    try:
        contracts.validate_procedure_publication(publication)
        normalized_receiver = contracts.normalize_experience_scope(receiver)
    except (contracts.ContractError, KeyError, TypeError) as exc:
        raise ValueError("handoff publication or receiver is invalid") from exc
    partition = publication["partition"]
    representation = publication["representation"]
    if (
        publication["status"] != "acknowledged"
        or partition["namespace"] != f"step18-live-{token}"
        or normalized_receiver not in partition["recipients"]
        or normalized_receiver not in publication["approval"]["recipients"]
        or {key: representation[key] for key in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS}
           != representation_identity()
        or representation["search_text"] != query
        or ATLAS_MVP_MARKER not in publication["procedure"]["behavior"]["body"]
        or route != ROUTE or facts != FACTS
    ):
        raise ValueError("handoff publication does not match the eligible synthetic fixture")
    return {
        "schema": "atlas-mvp-handoff/v1",
        "database": database,
        "collection": collection,
        "index": index,
        "namespace": partition["namespace"],
        "partition": partition,
        "partition_id": publication["partition_id"],
        "logical_id": publication["logical_id"],
        "revision_id": publication["revision_id"],
        "publication_id": publication["publication_id"],
        "receiver": normalized_receiver,
        "query": query,
        "route": route,
        "facts": dict(facts),
        "representation": representation_identity(),
        "material_use_marker": ATLAS_MVP_MARKER,
        "cleanup_owner": "Master-ROOT",
        "cleanup_target": {"database": database, "collection": collection},
    }


def owned_atlas_identity(database: str | None, run_token: str | None) -> dict[str, str]:
    """Derive the exact owned target from the caller's predeclared UUIDv4 token."""

    if not isinstance(database, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", database):
        raise ValueError("live Atlas database must be a nonempty simple name")
    if (
        not isinstance(run_token, str)
        or not re.fullmatch(r"[0-9a-f]{32}", run_token)
        or uuid.UUID(hex=run_token).version != 4
    ):
        raise ValueError(
            "MEMORY_HARNESS_ATLAS_RUN_TOKEN must be a canonical lowercase UUIDv4 hex token"
        )
    return {
        "database": database,
        "collection": f"trusted_procedures_{run_token}",
        "index": f"vector_{run_token}",
        "namespace": f"step18-live-{run_token}",
    }


class ManifestHandoffCleanupError(OSError):
    """The manifest exists, so its exact collection remains owned for reconciliation."""

    def __init__(self, path: Path, cleanup_target: dict) -> None:
        self.manifest_path = path
        self.cleanup_target = cleanup_target
        super().__init__(
            f"manifest retained at {path}; reconcile owned Atlas collection "
            f"{cleanup_target['database']}.{cleanup_target['collection']}"
        )


def _write_manifest_atomic(path: Path, manifest: dict) -> None:
    """Create a complete manifest only if the explicit target is still absent."""

    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    linked = False
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        linked = True
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except BaseException as exc:
            if linked:
                raise ManifestHandoffCleanupError(path, manifest["cleanup_target"]) from exc
            raise


def run_owned_collection(collection: object, *, action: object, manifest_path: Path | None) -> dict:
    """Drop exactly this collection unless all assertions and manifest creation finish."""

    retained = False
    try:
        manifest = action()
        if manifest_path is not None:
            try:
                _write_manifest_atomic(manifest_path, manifest)
            except ManifestHandoffCleanupError:
                retained = True
                raise
            retained = True
        return manifest
    finally:
        if not retained:
            collection.drop()


class LiveAtlasTrustedProcedureTests(unittest.TestCase):
    """One synthetic UUID-owned collection; explicit manifest path opts in to handoff."""

    def test_vector_discovery_exact_validation_and_revocation(self) -> None:
        if os.environ.get("MEMORY_HARNESS_RUN_LIVE_ATLAS") != "1":
            self.skipTest("set MEMORY_HARNESS_RUN_LIVE_ATLAS=1 for the owned live Atlas test")
        uri = os.environ.get("MEMORY_HARNESS_ATLAS_URI")
        run_token = os.environ.get("MEMORY_HARNESS_ATLAS_RUN_TOKEN")
        identity = owned_atlas_identity(
            os.environ.get("MEMORY_HARNESS_ATLAS_LIVE_DATABASE"), run_token
        )
        database = identity["database"]
        if not uri:
            self.skipTest("set MEMORY_HARNESS_ATLAS_URI")
        handoff_path = os.environ.get("MEMORY_HARNESS_ATLAS_HANDOFF_MANIFEST_PATH")
        if handoff_path == "":
            raise ValueError("handoff manifest path must be nonempty")
        try:
            from pymongo import MongoClient
        except ImportError as exc:  # pragma: no cover - environment gated
            self.skipTest(f"Atlas optional dependency unavailable: {exc.__class__.__name__}")

        namespace = identity["namespace"]
        collection_name = identity["collection"]
        index_name = identity["index"]
        client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
        collection = client[database][collection_name]
        temporary = tempfile.TemporaryDirectory()
        memory_store = store.MemoryStore(Path(temporary.name) / "memory.sqlite3")

        def action() -> dict:
            memory_store.initialize()
            scope = {
                "application": "memory-harness-live-test",
                "project": "step18-synthetic",
                "namespace": namespace,
                "owner": "root-live-test",
            }
            fixtures = (
                make_fixture(scope, run_token, retained=True),
                make_fixture(scope, run_token, retained=False),
            )
            service = procedures.TrustedProcedureService(
                memory_store, trusted_issuers={"ROOT"}
            )
            adapter = atlas.AtlasProcedureAdapter.from_pymongo_collection(
                collection=collection,
                embedding=DeterministicEmbeddings(),
                index_name=index_name,
            )
            create_fixture_index(adapter, fixtures=fixtures)
            retained, _ = prove_fixture_eligibility(
                self, service=service, adapter=adapter, scope=scope,
                run_token=run_token, fixtures=fixtures,
            )
            return handoff_manifest(
                database=database, collection=collection_name, index=index_name,
                publication=retained, receiver=scope, query=ELIGIBLE_QUERY,
                route=ROUTE, facts=FACTS,
            )

        try:
            run_owned_collection(
                collection, action=action,
                manifest_path=Path(handoff_path) if handoff_path is not None else None,
            )
        finally:
            try:
                client.close()
            finally:
                try:
                    memory_store.close()
                finally:
                    temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
