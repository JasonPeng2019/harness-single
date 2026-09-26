"""In-process trusted SearchStores reach the real bootstrap worker handoff."""

from __future__ import annotations

import json
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from memory_harness import contracts
try:
    from tests.local.mvp.test_coherent_memory_path import (
        ATLAS_MARKER,
        EVEROS_MARKER,
        RAW_APPROVAL_MATERIAL,
        _CoherentFixture,
    )
except ModuleNotFoundError:
    # This is an optional cross-repository integration suite. The standalone
    # harness package does not vendor the memory product's large fake-store
    # fixture tree; CI can add that checkout to PYTHONPATH to enable it.
    ATLAS_MARKER = "ATLAS_MVP_MARKER"
    EVEROS_MARKER = "EVEROS_MVP_MARKER"
    RAW_APPROVAL_MATERIAL = "RAW_APPROVAL_EVIDENCE_DO_NOT_DELIVER"

    class _CoherentFixture:
        pass

    COHERENT_FIXTURE_AVAILABLE = False
else:
    COHERENT_FIXTURE_AVAILABLE = True

from orchestrator_harness import bootstrap, memory_handoff, resume
from orchestrator_harness.tests.test_step04_launch_boundary import LaunchBoundaryFixture


@unittest.skipUnless(
    COHERENT_FIXTURE_AVAILABLE,
    "memory product fake-store fixtures are not available on PYTHONPATH",
)
class BootstrapSearchStoreTests(_CoherentFixture, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.harness = LaunchBoundaryFixture()
        self.addCleanup(self.harness.close)

    def _bootstrap(self, *, lane_id: str, card: dict, search_stores=None) -> tuple[dict, Path]:
        task_card_path = self.harness.root / f"{lane_id}-task-card.json"
        self.harness.write_json(task_card_path, card)
        worktree = self.harness.runtime / "worktrees" / self.harness.EPOCH / lane_id

        def fake_git_add(root: Path, branch: str, target: Path, base_commit: str) -> None:
            target.mkdir(parents=True, exist_ok=True)

        def fake_git_identity(root: Path, branch: str, base_commit: str) -> dict:
            return {
                "source_root": str(root.resolve()),
                "common_dir": str((root / ".git").resolve()),
                "branch": branch,
                "base_commit": base_commit,
                "origin_tip": base_commit,
                "bootstrap_tip": base_commit,
            }

        with (
            patch("orchestrator_harness.config.find_harness_root", return_value=self.harness.harness),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": self.harness.EPOCH}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(
                bootstrap, "_resolve_git_identity", side_effect=fake_git_identity
            ),
            patch.object(bootstrap, "_git_worktree_add", side_effect=fake_git_add),
            patch.object(
                bootstrap,
                "_capture_created_worktree_identity",
                return_value={
                    "admin_path": str(self.harness.root / "fake-admin"),
                    "admin_dev": 1,
                    "admin_ino": 1,
                    "gitfile_dev": 1,
                    "gitfile_ino": 1,
                },
            ),
            patch.object(bootstrap, "_verify_created_worktree"),
        ):
            store_argument = {} if search_stores is None else {"search_stores": search_stores}
            result = bootstrap.run_bootstrap(
                lane_id=lane_id,
                provider="codex",
                model="test-model",
                launch_config=dict(self.harness.launch_config),
                exclusive_resources=[],
                task_card_path=str(task_card_path),
                **store_argument,
            )
        return result, worktree

    def _resume(self, *, lane_id: str, search_stores=None) -> dict:
        resume_card = self.harness.root / f"{lane_id}-resume-card.json"
        self.harness.write_json(resume_card, self.card)
        with (
            patch.object(resume, "find_harness_root", return_value=self.harness.harness),
            patch.object(resume, "find_active_lane", side_effect=self.harness._read_active_lane),
        ):
            store_argument = {} if search_stores is None else {"search_stores": search_stores}
            return resume.run_resume(
                lane_id=lane_id, resume_task_card=str(resume_card), **store_argument,
            )

    def test_resume_with_live_stores_restores_both_trusted_sources(self) -> None:
        credential = "RECOVERY_STORE_CREDENTIAL_MUST_STAY_IN_PROCESS"
        live_query = self.everos_store.query
        everos_store = replace(
            self.everos_store,
            query=lambda request: live_query(request) if credential else (),
        )
        stores = (everos_store, self.atlas_store)
        prepared, worktree = self._bootstrap(
            lane_id="resume-stores", card=self.card, search_stores=stores,
        )
        self.assertTrue(prepared["ok"], prepared)
        previous = memory_handoff.load_envelope(worktree)
        self.harness.make_resumable("resume-stores")

        recovered = self._resume(lane_id="resume-stores", search_stores=stores)
        self.assertTrue(recovered["ok"], recovered)
        self.assertEqual("RESUME_OK", recovered["code"])
        envelope = memory_handoff.load_envelope(worktree)
        context = memory_handoff.load_final_context(worktree_path=worktree, envelope=envelope)
        prompt = (worktree / ".agent-workspace/worker-prompt.md").read_text(encoding="utf-8")
        self.assertNotEqual(previous["run_id"], envelope["run_id"])
        self.assertEqual(previous["decision_id"], envelope["decision_id"])
        self.assertEqual(
            {"everos-generated-skills", "atlas-shared-procedures"},
            {item["source_id"] for item in context["optional_content"]},
        )
        selected = {
            item["provenance"]["source_id"]: item["provenance"]
            for item in context["delivery_trace"]["selected"]
        }
        for source, procedure, marker in (
            ("everos-generated-skills", self.everos_procedure, EVEROS_MARKER),
            ("atlas-shared-procedures", self.atlas_procedure, ATLAS_MARKER),
        ):
            self.assertEqual(procedure["logical_id"], selected[source]["logical_id"])
            self.assertEqual(procedure["revision_id"], selected[source]["revision_id"])
            self.assertIn(marker, json.dumps(context))
            self.assertIn(marker, prompt)
            self.assertIn(source, prompt)
        self.assertEqual(4, len(self.everos.fake.search_calls))
        self.assertEqual(4, len(self.vector_store.calls))
        for path in (
            worktree / ".agent-workspace/task-card.json",
            self.harness.runtime / "epochs" / self.harness.EPOCH / "lanes" / "resume-stores" / "lane.json",
            worktree / ".agent-workspace/invocation.json",
            worktree / ".agent-workspace/result-template.json",
            memory_handoff.memory_paths(worktree)[1],
            worktree / ".agent-workspace/worker-prompt.md",
        ):
            data = path.read_text(encoding="utf-8")
            self.assertNotIn(credential, data)
            self.assertNotIn(RAW_APPROVAL_MATERIAL, data)
            self.assertNotIn("SearchStore(", data)
        self.assertNotIn(credential, json.dumps(recovered))

    def test_resume_without_live_stores_preserves_selected_prior_handoff(self) -> None:
        prepared, worktree = self._bootstrap(
            lane_id="resume-no-stores", card=self.card,
            search_stores=(self.everos_store, self.atlas_store),
        )
        self.assertTrue(prepared["ok"], prepared)
        self.harness.make_resumable("resume-no-stores")
        envelope = memory_handoff.load_envelope(worktree)
        context = memory_handoff.load_final_context(worktree_path=worktree, envelope=envelope)
        watched = (
            memory_handoff.memory_paths(worktree)[1],
            worktree / ".agent-workspace/worker-prompt.md",
            worktree / ".agent-workspace/invocation.json",
            self.harness.runtime / "epochs" / self.harness.EPOCH / "lanes" / "resume-no-stores" / "lane.json",
        )
        before = {path: path.read_bytes() for path in watched}

        for stores in (None, ()):
            with self.subTest(search_stores=stores):
                recovered = self._resume(lane_id="resume-no-stores", search_stores=stores)
                self.assertFalse(recovered["ok"], recovered)
                self.assertNotEqual("RESUME_OK", recovered["code"])
                self.assertIn("live", recovered["summary"])
                self.assertEqual(before, {path: path.read_bytes() for path in watched})
                self.assertEqual(
                    context, memory_handoff.load_final_context(
                        worktree_path=worktree, envelope=envelope,
                    ),
                )
        self.assertEqual(2, len(self.everos.fake.search_calls))
        self.assertEqual(2, len(self.vector_store.calls))

    def test_resume_refuses_lost_plan_required_source_before_replacing_handoff(self) -> None:
        for withdrawn in ("store", "publication"):
            with self.subTest(withdrawn=withdrawn):
                lane_id = f"resume-withdrawn-{withdrawn}"
                prepared, worktree = self._bootstrap(
                    lane_id=lane_id, card=self.card,
                    search_stores=(self.everos_store, self.atlas_store),
                )
                self.assertTrue(prepared["ok"], prepared)
                self.harness.make_resumable(lane_id)
                envelope = memory_handoff.load_envelope(worktree)
                context = memory_handoff.load_final_context(
                    worktree_path=worktree, envelope=envelope,
                )
                self.assertEqual(2, len(context["optional_content"]))
                store_path, envelope_path = memory_handoff.memory_paths(worktree)
                watched = (
                    envelope_path,
                    worktree / ".agent-workspace/worker-prompt.md",
                    worktree / ".agent-workspace/invocation.json",
                    self.harness.runtime / "epochs" / self.harness.EPOCH
                    / "lanes" / lane_id / "lane.json",
                )
                before = {path: path.read_bytes() for path in watched}
                with closing(sqlite3.connect(store_path)) as db:
                    count_before = db.execute("SELECT count(*) FROM final_contexts").fetchone()[0]

                if withdrawn == "store":
                    stores = (self.everos_store,)
                else:
                    self.vector_store.publication_ids.remove(self.publication["publication_id"])
                    stores = (self.everos_store, self.atlas_store)
                recovered = self._resume(lane_id=lane_id, search_stores=stores)
                self.assertFalse(recovered["ok"], recovered)
                self.assertNotEqual("RESUME_OK", recovered["code"])
                self.assertIn("atlas-shared-procedures", recovered["summary"])
                self.assertEqual(before, {path: path.read_bytes() for path in watched})
                with closing(sqlite3.connect(store_path)) as db:
                    self.assertEqual(
                        count_before,
                        db.execute("SELECT count(*) FROM final_contexts").fetchone()[0],
                    )
                self.assertEqual(
                    context,
                    memory_handoff.load_final_context(
                        worktree_path=worktree, envelope=envelope,
                    ),
                )
                if withdrawn == "publication":
                    self.vector_store.publication_ids.append(self.publication["publication_id"])

    def test_accepted_standard_bootstrap_delivers_both_trusted_sources(self) -> None:
        credential = "SEARCH_STORE_CREDENTIAL_MUST_STAY_IN_PROCESS"
        live_query = self.everos_store.query
        everos_store = replace(
            self.everos_store,
            query=lambda request: live_query(request) if credential else (),
        )
        result, worktree = self._bootstrap(
            lane_id="lane-1", card=self.card,
            search_stores=(everos_store, self.atlas_store),
        )
        self.assertTrue(result["ok"], result)
        envelope = memory_handoff.load_envelope(worktree)
        self.assertIsNotNone(envelope)
        context = memory_handoff.load_final_context(worktree_path=worktree, envelope=envelope)
        self.assertEqual(
            {"everos-generated-skills", "atlas-shared-procedures"},
            {item["source_id"] for item in context["optional_content"]},
        )
        selected = {
            item["provenance"]["source_id"]: item["provenance"]
            for item in context["delivery_trace"]["selected"]
        }
        for source, procedure in (
            ("everos-generated-skills", self.everos_procedure),
            ("atlas-shared-procedures", self.atlas_procedure),
        ):
            self.assertEqual(procedure["logical_id"], selected[source]["logical_id"])
            self.assertEqual(procedure["revision_id"], selected[source]["revision_id"])
            delivered = next(
                item for item in context["optional_content"]
                if item["source_id"] == source
            )
            self.assertEqual(self.receiver, delivered["content"]["recipient"])
        prompt = (worktree / ".agent-workspace/worker-prompt.md").read_text(encoding="utf-8")
        for marker in (EVEROS_MARKER, ATLAS_MARKER):
            self.assertIn(marker, prompt)
            self.assertIn(marker, json.dumps(context))
        for source, procedure in (
            ("everos-generated-skills", self.everos_procedure),
            ("atlas-shared-procedures", self.atlas_procedure),
        ):
            self.assertIn(source, prompt)
            self.assertIn(procedure["revision_id"], prompt)
        self.assertNotIn(RAW_APPROVAL_MATERIAL, prompt)
        self.assertNotIn("authority_evidence", prompt)
        self.assertNotIn(RAW_APPROVAL_MATERIAL, json.dumps(context))
        self.assertEqual(2, len(self.everos.fake.search_calls))
        self.assertEqual(2, len(self.vector_store.calls))
        for path in (
            worktree / ".agent-workspace/task-card.json",
            self.harness.runtime / "epochs" / self.harness.EPOCH / "lanes" / "lane-1" / "lane.json",
            worktree / ".agent-workspace/invocation.json",
            worktree / ".agent-workspace/result-template.json",
            memory_handoff.memory_paths(worktree)[1],
        ):
            data = path.read_text(encoding="utf-8")
            self.assertNotIn(RAW_APPROVAL_MATERIAL, data)
            self.assertNotIn(credential, data)
            self.assertNotIn("SearchStore(", data)
        self.assertNotIn(credential, prompt)
        self.assertNotIn(credential, json.dumps(context))
        self.assertNotIn(credential, json.dumps(result))
        self.assertNotIn(credential, json.dumps(envelope))

    def test_default_and_all_off_make_no_optional_store_calls(self) -> None:
        default, default_worktree = self._bootstrap(lane_id="lane-1", card=self.card)
        self.assertTrue(default["ok"], default)
        default_envelope = memory_handoff.load_envelope(default_worktree)
        self.assertEqual([], default_envelope["optional_content"])
        self.assertEqual(0, len(self.everos.fake.search_calls))
        self.assertEqual(0, len(self.vector_store.calls))

        all_off_card = contracts.make_task_card(
            task=self.card["task"], base_commit=self.card["base_commit"],
            memory_handoff=contracts.make_memory_handoff(
                objective_id=self.plan["objective_id"], route="ordinary",
                plan=self.plan, checkpoint="checkpoint-1",
                configuration={"all_features": False},
            ),
        )
        all_off, all_off_worktree = self._bootstrap(
            lane_id="all-off", card=all_off_card,
            search_stores=(self.everos_store, self.atlas_store),
        )
        self.assertTrue(all_off["ok"], all_off)
        self.assertIsNone(memory_handoff.load_envelope(all_off_worktree))
        self.assertEqual(0, len(self.everos.fake.search_calls))
        self.assertEqual(0, len(self.vector_store.calls))


if __name__ == "__main__":
    unittest.main()
