from __future__ import annotations

import unittest
import asyncio
import tempfile
from types import ModuleType
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from memory_harness import worker_recall


def atlas_test_modules(embedding: MagicMock, vector_search: MagicMock) -> dict[str, ModuleType]:
    """Supply inert optional SDK surfaces while testing our query contract."""
    everos_embedding = ModuleType("everos.component.embedding")
    everos_embedding.get_embedding_capability = lambda: embedding
    langchain_core = ModuleType("langchain_core")
    langchain_core.__path__ = []
    langchain_embeddings = ModuleType("langchain_core.embeddings")
    langchain_embeddings.Embeddings = type("Embeddings", (), {})
    langchain_mongodb = ModuleType("langchain_mongodb")
    langchain_mongodb.MongoDBAtlasVectorSearch = vector_search
    return {
        "everos.component.embedding": everos_embedding,
        "langchain_core": langchain_core,
        "langchain_core.embeddings": langchain_embeddings,
        "langchain_mongodb": langchain_mongodb,
    }


class WorkerRecallTests(unittest.TestCase):
    def test_exact_assignment_queries_both_stores_and_renders_worker_context(self) -> None:
        query = "Implement the API client retry policy"
        atlas = [{"text": "Prior retry timeout defect", "score": 0.82, "metadata": {"task": "api-client"}}]
        everos = [{"key_insight": "Prior retry timeout defect"}, {"key_insight": "Check idempotency"}]
        with (
            patch.dict("os.environ", {"MEMORY_HARNESS_RUN_ID": "run-02", "MEMORY_HARNESS_TASK_ID": "api-client"}),
            patch.object(worker_recall, "_search_atlas", new_callable=AsyncMock, return_value=atlas) as search_atlas,
            patch.object(worker_recall, "_search_everos", new_callable=AsyncMock, return_value=everos) as search_everos,
            patch.object(worker_recall, "write_activity") as activity,
            patch.object(worker_recall, "write_detail") as detail,
        ):
            context = worker_recall.recall_worker_context(query, lane_id="retry-policy")
        search_atlas.assert_awaited_once_with(query)
        search_everos.assert_awaited_once_with(query)
        self.assertEqual(1, context.count("Prior retry timeout defect"))
        self.assertIn("Check idempotency", context)
        completed = next(call for call in activity.call_args_list if call.args[0] == "memory.worker.recall.completed")
        self.assertEqual(["atlas", "everos"], [item["source"] for item in completed.kwargs["selected"]])
        self.assertTrue(all(call.kwargs["recipient"] == "worker" for call in detail.call_args_list))

    def test_backend_failure_is_not_silently_treated_as_empty_memory(self) -> None:
        with (
            patch.dict("os.environ", {"MEMORY_HARNESS_RUN_ID": "run-02", "MEMORY_HARNESS_TASK_ID": "api-client"}),
            patch.object(worker_recall, "_search_everos", new_callable=AsyncMock, side_effect=RuntimeError("offline")),
            patch.object(worker_recall, "_search_atlas", new_callable=AsyncMock, side_effect=RuntimeError("offline")),
            patch.object(worker_recall, "write_activity") as activity,
        ):
            with self.assertRaisesRegex(RuntimeError, "both worker memory stores are unavailable"):
                worker_recall.recall_worker_context("Build retries", lane_id="retries")
        self.assertEqual("memory.worker.recall.failed", activity.call_args_list[-1].args[0])

    def test_one_unavailable_store_keeps_available_experience(self) -> None:
        with (
            patch.dict("os.environ", {"MEMORY_HARNESS_RUN_ID": "run-02", "MEMORY_HARNESS_TASK_ID": "api-client"}),
            patch.object(worker_recall, "_search_everos", new_callable=AsyncMock, return_value=[{"key_insight": "Prior retry backoff defect"}]),
            patch.object(worker_recall, "_search_atlas", new_callable=AsyncMock, side_effect=RuntimeError("offline")),
            patch.object(worker_recall, "write_activity") as activity,
            patch.object(worker_recall, "write_detail"),
        ):
            context = worker_recall.recall_worker_context("Build retries", lane_id="retries")
        self.assertIn("Prior retry backoff defect", context)
        completed = activity.call_args_list[-1]
        self.assertEqual("memory.worker.recall.completed", completed.args[0])
        self.assertEqual(["atlas"], completed.kwargs["unavailable_stores"])

    def test_scope_defaults_are_generic_and_isolate_tasks(self) -> None:
        with patch.dict("os.environ", {"MEMORY_HARNESS_TASK_ID": "api-client"}, clear=True):
            first = worker_recall._scope()
        with patch.dict("os.environ", {"MEMORY_HARNESS_TASK_ID": "rust-rewrite"}, clear=True):
            other = worker_recall._scope()
        self.assertEqual("api-client-shared-v1", first.namespace)
        self.assertEqual("api-client", first.project)
        self.assertEqual("coding-harness", first.application)
        self.assertEqual("worker-memory", first.owner)
        self.assertEqual("rust-rewrite-shared-v1", other.namespace)
        self.assertEqual("rust-rewrite", other.project)

    def test_atlas_uses_configured_collection_and_task_filter(self) -> None:
        client = MagicMock()
        database = client.__getitem__.return_value
        database.__getitem__.return_value.find_one.return_value = {"_id": "prior"}
        search = MagicMock()
        search.similarity_search_with_score.return_value = []
        embedding = MagicMock()
        embedding.require.return_value.embed = AsyncMock(return_value=[0.1, 0.2])
        vector_search = MagicMock(return_value=search)
        environment = {
            "MEMORY_HARNESS_TASK_ID": "rust-rewrite",
            "MEMORY_HARNESS_ATLAS_URI": "mongodb://unused",
            "MEMORY_HARNESS_ATLAS_DATABASE": "test-memory",
            "MEMORY_HARNESS_ATLAS_COLLECTION": "shared_rust_memories",
            "MEMORY_HARNESS_ATLAS_INDEX": "vector_memories_v1",
        }
        with (
            patch.dict("os.environ", environment, clear=True),
            patch("pymongo.MongoClient", return_value=client),
            patch.dict("sys.modules", atlas_test_modules(embedding, vector_search)),
        ):
            self.assertEqual([], asyncio.run(worker_recall._search_atlas("Implement crate parser")))
        vector_search.assert_called_once()
        self.assertEqual("vector_memories_v1", vector_search.call_args.kwargs["index_name"])
        self.assertIs(database.__getitem__.return_value, vector_search.call_args.kwargs["collection"])
        self.assertEqual(
            {"task": {"$eq": "rust-rewrite"}},
            search.similarity_search_with_score.call_args.kwargs["pre_filter"],
        )

    def test_new_task_without_atlas_collection_has_no_prior_matches(self) -> None:
        client = MagicMock()
        client.__getitem__.return_value.__getitem__.return_value.find_one.return_value = None
        embedding = MagicMock()
        embedding.require.return_value.embed = AsyncMock(return_value=[0.1, 0.2])
        with (
            patch.dict("os.environ", {
                "MEMORY_HARNESS_TASK_ID": "new-task",
                "MEMORY_HARNESS_ATLAS_URI": "mongodb://unused",
                "MEMORY_HARNESS_ATLAS_DATABASE": "test-memory",
                "MEMORY_HARNESS_ATLAS_COLLECTION": "new_task_memory",
            }, clear=True),
            patch("pymongo.MongoClient", return_value=client),
            patch.dict("sys.modules", atlas_test_modules(embedding, MagicMock())),
        ):
            self.assertEqual([], asyncio.run(worker_recall._search_atlas("First assignment")))
        embedding.assert_not_called()

    def test_new_task_without_everos_scope_has_no_prior_matches(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            with (
                patch.dict("os.environ", {
                    "MEMORY_HARNESS_TASK_ID": "new-task",
                    "MEMORY_HARNESS_EVEROS_BASE_ROOT": raw,
                }, clear=True),
                patch.object(worker_recall.EverOSAdapter, "memory_root_for_scope", return_value=Path(raw) / "absent"),
            ):
                self.assertEqual([], asyncio.run(worker_recall._search_everos("First assignment")))


if __name__ == "__main__":
    unittest.main()
