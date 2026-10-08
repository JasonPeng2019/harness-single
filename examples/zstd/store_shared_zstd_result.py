"""Store and verify one reviewed ZSTD benchmark result in shared memory."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from langchain_core.embeddings import Embeddings
from langchain_mongodb import MongoDBAtlasVectorSearch
from memory_harness.activity_log import write_activity, write_detail
from memory_harness.experience import (
    EverOSAdapter,
    ExperienceScope,
    load_vendored_everos_public_surface,
)
from pymongo import MongoClient


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is absent: {name}")
    return value


def as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        result = dump(mode="json")
        if isinstance(result, dict):
            return result
    raise RuntimeError(f"unsupported EverOS value: {type(value).__name__}")


async def store_everos(text: str) -> tuple[list[float], str, dict[str, Any]]:
    from everos.component.embedding import get_embedding_capability
    from everos.config import load_settings
    from everos.infra.persistence.lancedb import (
        AgentCase,
        agent_case_repo,
        ensure_business_indexes,
        lancedb_manager,
    )

    import everos.component.embedding.accessor as embedding_accessor
    import everos.service.search as search_service

    scope = ExperienceScope(
        application="memory-harness-benchmark",
        project="zstd-decoder",
        namespace="zstd-decoder-shared-v1",
        owner="root-benchmark",
    )
    base_root = Path(required("MEMORY_HARNESS_EVEROS_BASE_ROOT")).resolve()
    memory_root = EverOSAdapter.memory_root_for_scope(base_root, scope)
    memory_root.mkdir(parents=True, exist_ok=True)
    os.environ["EVEROS_ROOT"] = str(memory_root)

    load_settings.cache_clear()
    embedding_accessor._capability = None
    search_service._manager = None
    lancedb_manager._conn = None
    lancedb_manager._tables.clear()

    surface = load_vendored_everos_public_surface(memory_root=memory_root)
    adapter = EverOSAdapter(scope=scope, base_root=base_root, surface=surface)
    provider = get_embedding_capability().require()
    write_activity("memory.shared.everos.embedding.started")
    vector = await provider.embed(text)
    if not vector:
        raise RuntimeError("EverOS embedder returned an empty vector")
    write_activity("memory.shared.everos.embedding.completed", dimensions=len(vector))

    run_id = required("MEMORY_HARNESS_RESULT_RUN_ID")
    token = required("MEMORY_HARNESS_RESULT_TOKEN")
    entry_id = "benchmark-result-" + token
    case_id = adapter.everos_owner_id + "_" + entry_id
    case = AgentCase(
        id=case_id,
        entry_id=entry_id,
        owner_id=adapter.everos_owner_id,
        owner_type="agent",
        app_id=adapter.everos_application_id,
        project_id=adapter.everos_project_id,
        session_id="zstd-shared-results-v1",
        timestamp=dt.datetime.now(dt.UTC),
        parent_type="memcell",
        parent_id=run_id,
        quality_score=0.442,
        task_intent="Complete the from-scratch RFC 8878 ZSTD decoder benchmark.",
        task_intent_tokens="RFC 8878 ZSTD decoder benchmark",
        approach="Use the native harness, but mechanically prevent early partial shutdown.",
        approach_tokens="native harness prevent early partial shutdown entropy lanes",
        key_insight=text,
        md_path=f"benchmark-results/{run_id}.md",
        content_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        vector=vector,
    )
    write_activity("memory.shared.everos.store.started", run_id=run_id, case_id=case_id)
    await agent_case_repo.upsert([case])
    await ensure_business_indexes()
    write_activity("memory.shared.everos.store.completed", run_id=run_id, case_id=case_id)

    request = surface.make_search_request(
        agent_id=adapter.everos_owner_id,
        app_id=adapter.everos_application_id,
        project_id=adapter.everos_project_id,
        query="prior ZSTD decoder benchmark failure entropy early shutdown",
        method="vector",
        top_k=10,
    )
    write_activity("memory.shared.everos.recall.started", run_id=run_id)
    response = as_mapping(await surface.search(request))
    cases = response.get("data", {}).get("agent_cases", [])
    rendered = [as_mapping(item) for item in cases]
    hit = next((item for item in rendered if item.get("id") == case_id), None)
    if hit is None:
        raise RuntimeError("shared EverOS recall did not return the stored result")
    write_detail(
        "memory.shared.everos.recall.results",
        results=rendered,
        selected=hit,
    )
    write_activity("memory.shared.everos.recall.completed", run_id=run_id, case_id=case_id)
    return vector, case_id, hit


class FixedEmbeddings(Embeddings):
    def __init__(self, vector: list[float]) -> None:
        self.vector = list(vector)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [list(self.vector) for _ in texts]

    def embed_query(self, text: str) -> list[float]:
        return list(self.vector)


def store_atlas(text: str, vector: list[float], everos_case_id: str) -> dict[str, Any]:
    uri = required("MEMORY_HARNESS_ATLAS_URI")
    database_name = os.environ.get("MEMORY_HARNESS_ATLAS_DATABASE", "memory-dev")
    collection_name = "benchmark_memories_zstd_decoder_v1"
    index_name = "vector_benchmark_memory_v1"
    run_id = required("MEMORY_HARNESS_RESULT_RUN_ID")
    token = required("MEMORY_HARNESS_RESULT_TOKEN")
    document_id = "benchmark-result-" + token
    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    collection = client[database_name][collection_name]
    vector_store = MongoDBAtlasVectorSearch(
        collection=collection,
        embedding=FixedEmbeddings(vector),
        index_name=index_name,
        text_key="search_text",
        embedding_key="procedure_embedding",
        relevance_score_fn="cosine",
    )
    try:
        indexes = list(collection.list_search_indexes())
        if not any(index.get("name") == index_name for index in indexes):
            write_activity(
                "memory.shared.atlas.index.started",
                collection=collection_name,
                index=index_name,
            )
            vector_store.create_vector_search_index(
                dimensions=len(vector),
                filters=["run_id", "document_kind", "task"],
                wait_until_complete=120.0,
            )
            write_activity(
                "memory.shared.atlas.index.completed",
                collection=collection_name,
                index=index_name,
                dimensions=len(vector),
            )
        existing = collection.find_one({"_id": document_id})
        if existing is None:
            write_activity(
                "memory.shared.atlas.store.started",
                run_id=run_id,
                collection=collection_name,
            )
            vector_store.add_texts(
                [text],
                metadatas=[{
                    "run_id": run_id,
                    "document_kind": "benchmark_run_result",
                    "task": "zstd-decoder",
                    "partial_score": 0.442,
                    "public_passed": 3,
                    "public_total": 6,
                    "hidden_passed": 16,
                    "hidden_total": 37,
                    "everos_case_id": everos_case_id,
                    "stored_at": dt.datetime.now(dt.UTC).isoformat(),
                }],
                ids=[document_id],
            )
            write_activity(
                "memory.shared.atlas.store.completed",
                run_id=run_id,
                collection=collection_name,
                document_id=document_id,
            )
        elif existing.get("search_text") != text:
            raise RuntimeError("shared Atlas result ID already contains different text")

        deadline = time.monotonic() + 90.0
        hits: list[tuple[Any, float]] = []
        while time.monotonic() < deadline:
            write_activity(
                "memory.shared.atlas.recall.attempt",
                run_id=run_id,
                collection=collection_name,
            )
            hits = vector_store.similarity_search_with_score(
                "prior ZSTD decoder benchmark failure entropy early shutdown",
                k=5,
                pre_filter={"task": {"$eq": "zstd-decoder"}},
            )
            if any(str(document.metadata.get("_id")) == document_id for document, _ in hits):
                break
            time.sleep(1)
        rendered = [
            {
                "text": document.page_content,
                "metadata": document.metadata,
                "score": float(score),
            }
            for document, score in hits
        ]
        selected = next(
            (item for item in rendered if str(item["metadata"].get("_id")) == document_id),
            None,
        )
        if selected is None:
            raise RuntimeError("shared Atlas recall did not return the stored result")
        write_detail(
            "memory.shared.atlas.recall.vector_results",
            query="prior ZSTD decoder benchmark failure entropy early shutdown",
            results=rendered,
            selected=selected,
        )
        write_activity(
            "memory.shared.atlas.recall.completed",
            run_id=run_id,
            collection=collection_name,
            score=selected["score"],
        )
        return {
            "database": database_name,
            "collection": collection_name,
            "index": index_name,
            "document_id": document_id,
            "recall": selected,
        }
    finally:
        client.close()


def main() -> None:
    text = required("MEMORY_HARNESS_RESULT_TEXT")
    vector, case_id, everos_hit = asyncio.run(store_everos(text))
    atlas = store_atlas(text, vector, case_id)
    result = {
        "schema": "shared-benchmark-memory/v1",
        "task": "zstd-decoder",
        "run_id": required("MEMORY_HARNESS_RESULT_RUN_ID"),
        "memory_text": text,
        "everos": {
            "namespace": "zstd-decoder-shared-v1",
            "case_id": case_id,
            "recalled_key_insight": everos_hit.get("key_insight"),
        },
        "atlas": atlas,
        "verified_at": dt.datetime.now(dt.UTC).isoformat(),
    }
    path = Path(required("MEMORY_HARNESS_RESULT_MANIFEST"))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
