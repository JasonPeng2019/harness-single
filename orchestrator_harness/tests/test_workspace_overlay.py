"""Focused workspace-overlay lifecycle tests for REQ-O40..REQ-O42.

Covers contents-only ingest, re-ingest/direct-edit behavior, preflight-before-
mutation preparation, create/merge/exact-append, minimal no-hash receipts,
exact byte restoration, later-edit preservation, CLI surfaces, and the lane
controller prelaunch verification seam.  Only disposable local fake folders
and a fake provider child are used; no real provider, network, hardware, MCP,
USB, or display checkout is touched.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, cast
from unittest import mock

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.cli import build_parser, main as cli_main
from orchestrator_harness.lane_lifecycle import RetirementResult, retire_terminal_lane
from orchestrator_harness.models import ProcessSnapshot
from orchestrator_harness.mutation import MutationError, MutationReceipt, TargetState
from orchestrator_harness.tests.support import TemporaryGitRepository
from orchestrator_harness.workspace_overlay import (
    DECLARATION_NAME,
    OVERLAY_RECEIPT_SCHEMA,
    SUPER_CACHE_NAME,
    OverlayCollisionError,
    WorkspaceOverlayError,
    ingest_super_cache,
    prepare_worktree,
    restore_worktree,
    verify_overlay_receipt,
)


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


class OverlayModuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.harness.mkdir()
        self.cache = self.harness / SUPER_CACHE_NAME
        self.target = self.root / "target"
        self.target.mkdir()
        self.receipt = self.root / "receipts" / "overlay-receipt.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _source(self, name: str = "source") -> Path:
        source = self.root / name
        source.mkdir(exist_ok=True)
        return source

    def _write(self, root: Path, relative: str, data: bytes) -> None:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _ingest(self, source: Path) -> dict[str, object]:
        return ingest_super_cache(source_folder=source, harness_worktree=self.harness)

    # ---- REQ-O40: ingest ----

    def test_ingest_refreshes_exact_contents_not_the_container(self) -> None:
        source = self._source()
        self._write(source, "readme.md", b"# overlay\n")
        self._write(source, "config/app.ini", b"mode=fast\n")
        self._write(source, "notes.txt", b"alpha\n")
        result = self._ingest(source)
        self.assertTrue(result["complete"])
        self.assertEqual(4, result["entry_count"])
        self.assertFalse(result["replaced_prior_contents"])
        self.assertTrue((self.cache / "readme.md").is_file())
        self.assertTrue((self.cache / "config" / "app.ini").is_file())
        self.assertEqual(b"# overlay\n", (self.cache / "readme.md").read_bytes())
        self.assertEqual(
            b"mode=fast\n", (self.cache / "config" / "app.ini").read_bytes()
        )
        self.assertEqual(b"alpha\n", (self.cache / "notes.txt").read_bytes())
        # The container folder itself is not copied.
        self.assertFalse((self.cache / source.name).exists())
        self.assertEqual("orchestrator-workspace-overlay-ingest/v1", result["schema"])

    def test_reingest_replaces_prior_contents(self) -> None:
        source = self._source()
        self._write(source, "keep.txt", b"one\n")
        self._write(source, "drop.txt", b"old\n")
        self._ingest(source)
        (source / "drop.txt").unlink()
        (source / "keep.txt").write_bytes(b"two\n")
        self._write(source, "new.txt", b"added\n")
        result = self._ingest(source)
        self.assertTrue(result["complete"])
        self.assertTrue(result["replaced_prior_contents"])
        self.assertTrue(result["removed_previous"])
        self.assertEqual(b"two\n", (self.cache / "keep.txt").read_bytes())
        self.assertFalse((self.cache / "drop.txt").exists())
        self.assertEqual(b"added\n", (self.cache / "new.txt").read_bytes())
        # No stale previous-cache directory remains.
        leftovers = [
            item.name
            for item in self.harness.iterdir()
            if item.name.startswith(".super-cache")
        ]
        self.assertEqual([], leftovers)

    def test_direct_cache_edits_remain_allowed_between_ingestions(self) -> None:
        source = self._source()
        self._write(source, "tracked.txt", b"base\n")
        self._ingest(source)
        self._write(self.cache, "direct-edit.txt", b"edited directly\n")
        self.assertEqual(
            b"edited directly\n", (self.cache / "direct-edit.txt").read_bytes()
        )

    def test_ingest_rejects_unsafe_source_and_destination_relationships(self) -> None:
        self._ingest(self._source())
        with self.assertRaisesRegex(WorkspaceOverlayError, "different directories"):
            ingest_super_cache(source_folder=self.cache, harness_worktree=self.harness)
        nested_source = self.cache / "nested-source"
        nested_source.mkdir()
        with self.assertRaisesRegex(WorkspaceOverlayError, "must not be inside"):
            ingest_super_cache(
                source_folder=nested_source, harness_worktree=self.harness
            )
        with self.assertRaisesRegex(
            WorkspaceOverlayError, "existing regular directory"
        ):
            ingest_super_cache(
                source_folder=self.root / "missing", harness_worktree=self.harness
            )
        file_harness = self.root / "not-a-dir"
        file_harness.write_text("x", encoding="utf-8")
        with self.assertRaisesRegex(
            WorkspaceOverlayError, "existing regular directory"
        ):
            ingest_super_cache(
                source_folder=self._source(), harness_worktree=file_harness
            )

    def test_ingest_rejects_reparse_points_without_reporting_complete(self) -> None:
        source = self._source()
        self._write(source, "ok.txt", b"fine\n")
        junction = source / "junction"
        try:
            completed = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(junction), str(self.root)],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            completed = None
        if completed is None or completed.returncode != 0 or not junction.exists():
            self.skipTest("directory junctions are unavailable in this environment")
        with self.assertRaisesRegex(WorkspaceOverlayError, "reparse point"):
            self._ingest(source)
        self.assertFalse(self.cache.exists())

    # ---- REQ-O41: prepare ----

    def _declared_cache(self, *, append_text: list[str] | None = None) -> Path:
        source = self._source()
        self._write(source, "created.txt", b"created by overlay\n")
        self._write(source, "shared/merged.txt", b"from cache\n")
        self._write(source, "notes.txt", b"appended-payload\n")
        if append_text is not None:
            self._write(
                source,
                DECLARATION_NAME,
                json.dumps(
                    {
                        "schema": "orchestrator-super-cache-control/v1",
                        "append_text": append_text,
                    }
                ).encode("utf-8"),
            )
        self._ingest(source)
        return self.cache

    def test_prepare_creates_missing_paths_and_merges_directories(self) -> None:
        cache = self._declared_cache()
        self._write(self.target, "shared/existing.txt", b"existing target file\n")
        result = prepare_worktree(
            super_cache=cache,
            target_worktree=self.target,
            role="subagent",
            receipt_path=self.receipt,
        )
        self.assertTrue(result["complete"])
        self.assertEqual("subagent", result["role"])
        self.assertEqual(
            b"created by overlay\n", (self.target / "created.txt").read_bytes()
        )
        self.assertEqual(
            b"existing target file\n",
            (self.target / "shared" / "existing.txt").read_bytes(),
        )
        self.assertEqual(
            b"from cache\n", (self.target / "shared" / "merged.txt").read_bytes()
        )
        # The declaration is cache control data and is never copied.
        self.assertFalse((self.target / DECLARATION_NAME).exists())
        self.assertTrue(self.receipt.is_file())

    def test_prepare_rejects_collision_before_any_mutation(self) -> None:
        cache = self._declared_cache()
        self._write(self.target, "created.txt", b"preexisting\n")
        with self.assertRaises(OverlayCollisionError):
            prepare_worktree(
                super_cache=cache,
                target_worktree=self.target,
                role="subagent",
                receipt_path=self.receipt,
            )
        # No mutation happened: the merge/create entries were never applied and
        # no receipt was published.
        self.assertFalse((self.target / "shared" / "merged.txt").exists())
        self.assertFalse((self.target / "notes.txt").exists())
        self.assertFalse(self.receipt.exists())

    def test_prepare_appends_declared_exact_bytes_without_separators(self) -> None:
        cache = self._declared_cache(append_text=["notes.txt"])
        self._write(self.target, "notes.txt", b"prefix|")
        result = prepare_worktree(
            super_cache=cache,
            target_worktree=self.target,
            role="orchestrator",
            receipt_path=self.receipt,
        )
        self.assertEqual(["notes.txt"], result["appended_paths"])
        self.assertEqual(
            b"prefix|appended-payload\n", (self.target / "notes.txt").read_bytes()
        )
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(
            b"prefix|", base64.b64decode(receipt["pre_overlay_bytes"]["notes.txt"])
        )
        self.assertEqual(
            b"prefix|appended-payload\n",
            base64.b64decode(receipt["post_prepare_bytes"]["notes.txt"]),
        )

    def test_prepare_rejects_non_utf8_or_missing_append_target_before_mutation(
        self,
    ) -> None:
        cache = self._declared_cache(append_text=["notes.txt"])
        self._write(self.target, "notes.txt", b"\xff\xfe binary")
        with self.assertRaisesRegex(OverlayCollisionError, "UTF-8 text file"):
            prepare_worktree(
                super_cache=cache,
                target_worktree=self.target,
                role="subagent",
                receipt_path=self.receipt,
            )
        self.assertFalse((self.target / "created.txt").exists())
        # A declared append path that does not exist yet is created with the
        # exact payload (there is no existing file to append to).
        (self.target / "notes.txt").unlink()
        result = prepare_worktree(
            super_cache=cache,
            target_worktree=self.target,
            role="subagent",
            receipt_path=self.receipt,
        )
        self.assertEqual(
            b"appended-payload\n", (self.target / "notes.txt").read_bytes()
        )
        self.assertEqual("create_file", result["operations"]["notes.txt"])

    def test_prepare_writes_minimal_no_hash_receipt_for_both_roles(self) -> None:
        for role in ("orchestrator", "subagent"):
            with self.subTest(role=role):
                receipt = self.root / "receipts" / f"{role}.json"
                cache = self._declared_cache(append_text=["notes.txt"])
                self._write(self.target, "notes.txt", b"base\n")
                result = prepare_worktree(
                    super_cache=cache,
                    target_worktree=self.target,
                    role=role,
                    receipt_path=receipt,
                )
                self.assertEqual(role, result["role"])
                raw = json.loads(receipt.read_text(encoding="utf-8"))
                self.assertEqual(OVERLAY_RECEIPT_SCHEMA, raw["schema"])
                self.assertTrue(raw["completed"])
                self.assertEqual(role, raw["role"])
                self.assertIn("prepared_utc", raw)
                self.assertEqual(
                    sorted(raw["affected_paths"]), sorted(raw["operations"])
                )
                self.assertIn("notes.txt", raw["pre_overlay_bytes"])
                self.assertIn("notes.txt", raw["post_prepare_bytes"])
                self.assertIn("created.txt", raw["created_paths"])
                self.assertIn("created.txt", raw["post_prepare_bytes"])
                self.assertIn("shared", raw["created_paths"])
                self.assertIn("shared/merged.txt", raw["created_paths"])
                self.assertNotIn("created.txt", raw["pre_overlay_bytes"])
                serialized = json.dumps(raw)
                self.assertNotIn("sha256", serialized)
                self.assertNotIn("content_hash", serialized)
                (self.target / "created.txt").unlink()
                (self.target / "notes.txt").write_bytes(b"base\n")
                (self.target / "shared" / "merged.txt").unlink()
                (self.target / "shared").rmdir()

    def test_prepare_rejects_unsafe_roles_and_overlapping_directories(self) -> None:
        cache = self._declared_cache()
        with self.assertRaisesRegex(WorkspaceOverlayError, "role must be"):
            prepare_worktree(
                super_cache=cache,
                target_worktree=self.target,
                role="root",
                receipt_path=self.receipt,
            )
        with self.assertRaisesRegex(WorkspaceOverlayError, "separate directories"):
            prepare_worktree(
                super_cache=cache,
                target_worktree=cache,
                role="subagent",
                receipt_path=self.receipt,
            )
        nested = cache / "nested-target"
        nested.mkdir()
        with self.assertRaisesRegex(WorkspaceOverlayError, "separate directories"):
            prepare_worktree(
                super_cache=cache,
                target_worktree=nested,
                role="subagent",
                receipt_path=self.receipt,
            )

    def test_prepare_rolls_back_partially_applied_target_mutation(self) -> None:
        cache = self._declared_cache()
        from orchestrator_harness import workspace_overlay as overlay_module

        real_replace = overlay_module.mutation_replace
        calls: list[str] = []

        def failing_replace(
            parent: Path,
            relative: str,
            data: bytes,
            *,
            expected: TargetState | None = None,
        ) -> MutationReceipt:
            calls.append(relative)
            if relative == "notes.txt":
                raise MutationError("synthetic failure")
            return real_replace(parent, relative, data, expected=expected)

        with mock.patch(
            "orchestrator_harness.workspace_overlay.mutation_replace",
            side_effect=failing_replace,
        ):
            with self.assertRaisesRegex(WorkspaceOverlayError, "rolled back"):
                prepare_worktree(
                    super_cache=cache,
                    target_worktree=self.target,
                    role="subagent",
                    receipt_path=self.receipt,
                )
        self.assertIn("created.txt", calls)
        self.assertFalse((self.target / "created.txt").exists())
        self.assertFalse((self.target / "shared" / "merged.txt").exists())
        self.assertFalse(self.receipt.exists())

    def test_prepare_rolls_back_when_receipt_cannot_be_published(self) -> None:
        cache = self._declared_cache()
        from orchestrator_harness import workspace_overlay as overlay_module

        real_replace = overlay_module.mutation_replace

        def failing_receipt(
            parent: Path,
            relative: str,
            data: bytes,
            *,
            expected: TargetState | None = None,
        ) -> MutationReceipt:
            if relative == self.receipt.name:
                raise MutationError("receipt write failure")
            return real_replace(parent, relative, data, expected=expected)

        with mock.patch(
            "orchestrator_harness.workspace_overlay.mutation_replace",
            side_effect=failing_receipt,
        ):
            with self.assertRaisesRegex(
                WorkspaceOverlayError, "receipt could not be published"
            ):
                prepare_worktree(
                    super_cache=cache,
                    target_worktree=self.target,
                    role="subagent",
                    receipt_path=self.receipt,
                )
        self.assertFalse((self.target / "created.txt").exists())
        self.assertFalse((self.target / "shared" / "merged.txt").exists())
        self.assertFalse((self.target / "notes.txt").exists())

    # ---- REQ-O42: restore ----

    def _prepared_target(self) -> dict[str, object]:
        cache = self._declared_cache(append_text=["notes.txt"])
        self._write(self.target, "notes.txt", b"original-notes\n")
        self._write(self.target, "unrelated.txt", b"unrelated target state\n")
        return prepare_worktree(
            super_cache=cache,
            target_worktree=self.target,
            role="subagent",
            receipt_path=self.receipt,
        )

    def test_restore_exact_clean_returns_original_state(self) -> None:
        self._prepared_target()
        self.assertEqual(
            b"original-notes\nappended-payload\n",
            (self.target / "notes.txt").read_bytes(),
        )
        result = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("RESTORED", result["outcome"])
        self.assertEqual(b"original-notes\n", (self.target / "notes.txt").read_bytes())
        self.assertFalse((self.target / "created.txt").exists())
        self.assertFalse((self.target / "shared" / "merged.txt").exists())
        self.assertEqual(
            b"unrelated target state\n", (self.target / "unrelated.txt").read_bytes()
        )
        self.assertIn("notes.txt", result["restored_paths"])
        self.assertIn("created.txt", result["removed_paths"])

    def test_restore_preserves_later_edit_and_blocks(self) -> None:
        self._prepared_target()
        (self.target / "notes.txt").write_bytes(b"later edit by the agent\n")
        result = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("BLOCKED", result["outcome"])
        self.assertEqual(
            b"later edit by the agent\n", (self.target / "notes.txt").read_bytes()
        )
        self.assertIn("notes.txt", result["preserved_paths"])
        self.assertIn("later edit detected", result["reason"])

    def test_restore_keeps_created_directory_with_later_work_visible(self) -> None:
        self._prepared_target()
        self._write(self.target, "shared/agent-work.txt", b"agent later work\n")
        result = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("BLOCKED", result["outcome"])
        self.assertEqual(
            b"agent later work\n",
            (self.target / "shared" / "agent-work.txt").read_bytes(),
        )
        self.assertTrue((self.target / "shared").is_dir())
        self.assertIn("shared", result["left_directories"])

    def _fresh_target(self) -> Path:
        target = self.root / f"target-{len(list(self.root.glob('target-*')))}"
        target.mkdir()
        self.target = target
        return target

    def test_restore_blocks_on_missing_malformed_or_mismatched_receipt(self) -> None:
        self._prepared_target()
        missing = self.root / "missing-receipt.json"
        result = restore_worktree(receipt_path=missing)
        self.assertEqual("BLOCKED", result["outcome"])
        self.assertIn("receipt invalid", result["reason"])

        self._fresh_target()
        self._prepared_target()
        self.receipt.write_text("not json", encoding="utf-8")
        malformed = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("BLOCKED", malformed["outcome"])

        self._fresh_target()
        self._prepared_target()
        raw = json.loads(self.receipt.read_text(encoding="utf-8"))
        raw["schema"] = "orchestrator-workspace-overlay-receipt/v9"
        self.receipt.write_text(json.dumps(raw), encoding="utf-8")
        wrong_schema = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("BLOCKED", wrong_schema["outcome"])

        self._fresh_target()
        self._prepared_target()
        tampered = json.loads(self.receipt.read_text(encoding="utf-8"))
        tampered["post_prepare_bytes"]["created.txt"] = "AAAA"
        self.receipt.write_text(json.dumps(tampered), encoding="utf-8")
        mismatched = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("BLOCKED", mismatched["outcome"])
        self.assertIn("created.txt", mismatched["preserved_paths"])
        self.assertTrue((self.target / "created.txt").exists())

    def test_restore_blocks_when_target_worktree_is_missing(self) -> None:
        self._prepared_target()
        result = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("RESTORED", result["outcome"])
        # Reuse the same receipt against a removed target: target is missing.
        shutil.rmtree(self.target)
        blocked = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("BLOCKED", blocked["outcome"])
        self.assertIn("target worktree is missing", blocked["reason"])

    # ---- REQ-O41: prelaunch verification ----

    def test_verify_absent_receipt_is_allowed(self) -> None:
        result = verify_overlay_receipt(
            receipt_path=None,
            expected_target_worktree_id=self.target,
            role="subagent",
        )
        self.assertFalse(result["present"])
        self.assertTrue(result["verified"])

    def test_verify_present_receipt_requires_completed_matching_target_and_role(
        self,
    ) -> None:
        self._prepared_target()
        ok = verify_overlay_receipt(
            receipt_path=self.receipt,
            expected_target_worktree_id=self.target,
            role="subagent",
        )
        self.assertTrue(ok["present"])
        self.assertTrue(ok["verified"])
        role_mismatch = verify_overlay_receipt(
            receipt_path=self.receipt,
            expected_target_worktree_id=self.target,
            role="orchestrator",
        )
        self.assertFalse(role_mismatch["verified"])
        other_target = self.root / "other"
        other_target.mkdir()
        target_mismatch = verify_overlay_receipt(
            receipt_path=self.receipt,
            expected_target_worktree_id=other_target,
            role="subagent",
        )
        self.assertFalse(target_mismatch["verified"])
        self.receipt.write_text("{broken", encoding="utf-8")
        malformed = verify_overlay_receipt(
            receipt_path=self.receipt,
            expected_target_worktree_id=self.target,
            role="subagent",
        )
        self.assertTrue(malformed["present"])
        self.assertFalse(malformed["verified"])

    def test_empty_created_file_receipt_verifies_and_restores(self) -> None:
        # WO-R1-001: the encoder emits an empty base64 string for an empty
        # created file; the decoder and every receipt consumer must accept it.
        source = self._source()
        self._write(source, "empty.txt", b"")
        self._write(source, "shared/merged.txt", b"from cache\n")
        self._ingest(source)
        result = prepare_worktree(
            super_cache=self.cache,
            target_worktree=self.target,
            role="subagent",
            receipt_path=self.receipt,
        )
        self.assertTrue(result["complete"])
        self.assertEqual(b"", (self.target / "empty.txt").read_bytes())
        raw = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual("", raw["post_prepare_bytes"]["empty.txt"])
        self.assertEqual(b"", base64.b64decode(raw["post_prepare_bytes"]["empty.txt"]))
        verified = verify_overlay_receipt(
            receipt_path=self.receipt,
            expected_target_worktree_id=self.target,
            role="subagent",
        )
        self.assertTrue(verified["verified"])
        restored = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("RESTORED", restored["outcome"])
        self.assertFalse((self.target / "empty.txt").exists())
        self.assertIn("empty.txt", restored["removed_paths"])
        self.assertFalse((self.target / "shared" / "merged.txt").exists())
        # Non-string and malformed base64 values remain rejected.
        for index, bad in enumerate((123, "%%%", "a b")):
            bad_receipt = self.root / "receipts" / f"bad-empty-{index}.json"
            tampered = json.loads(self.receipt.read_text(encoding="utf-8"))
            tampered["post_prepare_bytes"]["empty.txt"] = bad
            bad_receipt.write_text(json.dumps(tampered), encoding="utf-8")
            blocked = restore_worktree(receipt_path=bad_receipt)
            self.assertEqual("BLOCKED", blocked["outcome"])
            self.assertIn("receipt invalid", blocked["reason"])

    def test_empty_append_preimage_receipt_verifies_and_restores(self) -> None:
        # WO-R1-001: appending to an initially empty UTF-8 target records an
        # empty preimage; verification and exact-byte restoration must work.
        cache = self._declared_cache(append_text=["notes.txt"])
        self._write(self.target, "notes.txt", b"")
        result = prepare_worktree(
            super_cache=cache,
            target_worktree=self.target,
            role="subagent",
            receipt_path=self.receipt,
        )
        self.assertEqual(["notes.txt"], result["appended_paths"])
        self.assertEqual(
            b"appended-payload\n", (self.target / "notes.txt").read_bytes()
        )
        raw = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual("", raw["pre_overlay_bytes"]["notes.txt"])
        self.assertEqual(b"", base64.b64decode(raw["pre_overlay_bytes"]["notes.txt"]))
        self.assertEqual(
            b"appended-payload\n",
            base64.b64decode(raw["post_prepare_bytes"]["notes.txt"]),
        )
        verified = verify_overlay_receipt(
            receipt_path=self.receipt,
            expected_target_worktree_id=self.target,
            role="subagent",
        )
        self.assertTrue(verified["verified"])
        restored = restore_worktree(receipt_path=self.receipt)
        self.assertEqual("RESTORED", restored["outcome"])
        self.assertEqual(b"", (self.target / "notes.txt").read_bytes())
        self.assertIn("notes.txt", restored["restored_paths"])
        self.assertFalse((self.target / "created.txt").exists())


class OverlayCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.harness.mkdir()
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "file.txt").write_bytes(b"cli payload\n")
        self.target = self.root / "target"
        self.target.mkdir()
        self.receipt = self.root / "receipt.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_cli_workspace_super_cache_ingest_and_prepare(self) -> None:
        parser = build_parser()
        parsed = parser.parse_args(
            [
                "workspace",
                "super-cache",
                "ingest",
                "--source",
                str(self.source),
                "--harness-worktree",
                str(self.harness),
            ]
        )
        self.assertEqual("workspace", parsed.command)
        self.assertEqual("super-cache", parsed.workspace_action)
        self.assertEqual("ingest", parsed.super_cache_action)
        from unittest import mock as _mock

        emitted: list[Any] = []

        def capture(value: object, **_: object) -> None:
            emitted.append(value)

        with _mock.patch("orchestrator_harness.cli._print_json", side_effect=capture):
            code = cli_main(
                [
                    "workspace",
                    "super-cache",
                    "ingest",
                    "--source",
                    str(self.source),
                    "--harness-worktree",
                    str(self.harness),
                ]
            )
        self.assertEqual(0, code)
        record = emitted[-1]
        self.assertTrue(record["complete"])
        with _mock.patch("orchestrator_harness.cli._print_json", side_effect=capture):
            code = cli_main(
                [
                    "workspace",
                    "prepare",
                    "--super-cache",
                    str(self.harness / SUPER_CACHE_NAME),
                    "--worktree",
                    str(self.target),
                    "--role",
                    "subagent",
                    "--receipt",
                    str(self.receipt),
                ]
            )
        self.assertEqual(0, code)
        record = emitted[-1]
        self.assertTrue(record["complete"])
        self.assertEqual(b"cli payload\n", (self.target / "file.txt").read_bytes())

    def test_cli_lane_retire_accepts_overlay_receipt(self) -> None:
        parsed = build_parser().parse_args(
            [
                "lane",
                "retire",
                "--lane-root",
                str(self.root / "lane"),
                "--archive-root",
                str(self.root / "archive"),
                "--lane-id",
                "WO.P",
                "--task-ref",
                str(self.root / "task.json"),
                "--result-ref",
                str(self.root / "result.json"),
                "--findings-ref",
                str(self.root / "findings.json"),
                "--acceptance-ref",
                str(self.root / "acceptance.json"),
                "--transcript-ref",
                str(self.root / "transcript.json"),
                "--dependency-ref",
                str(self.root / "dependency.json"),
                "--overlay-receipt",
                str(self.receipt),
            ]
        )
        self.assertEqual(str(self.receipt), str(parsed.overlay_receipt))


FAKE_CODEX = r"""
import json, os, sys
capture = os.environ.get("CODING_CONTROLLER_CAPTURE")
if capture:
    open(capture, "w", encoding="utf-8").write(json.dumps(sys.argv))
sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": "coding-thread"}), flush=True)
print(json.dumps({"type": "turn.completed"}), flush=True)
raise SystemExit(0)
"""


class OverlayLaneSeamTests(unittest.TestCase):
    """Prelaunch verification plus retirement restoration through the real seams."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.main_repo = self.root / "main"
        self.main_repo.mkdir()
        _git(self.main_repo, "init", "--initial-branch", "main")
        _git(self.main_repo, "config", "user.email", "tests@example.invalid")
        _git(self.main_repo, "config", "user.name", "Harness Tests")
        (self.main_repo / "tracked.txt").write_text("tracked\n", encoding="utf-8")
        _git(self.main_repo, "add", "tracked.txt")
        _git(self.main_repo, "commit", "-m", "initial")
        self.lane = self.root / "lane"
        _git(
            self.main_repo,
            "worktree",
            "add",
            "-b",
            "overlay-lane",
            str(self.lane),
            "HEAD",
        )
        self.revision = _git(self.lane, "rev-parse", "HEAD")
        self.workspace = self.lane / ".agent-workspace"
        self.workspace.mkdir()
        exclude = Path(_git(self.lane, "rev-parse", "--git-path", "info/exclude"))
        if not exclude.is_absolute():
            exclude = self.lane / exclude
        exclude.write_text(
            exclude.read_text(encoding="utf-8") + ".agent-workspace/\n",
            encoding="utf-8",
        )
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.cache_root = self.root / "cache"
        self.cache_root.mkdir()
        self.fake = self.root / "fake_codex.py"
        self.fake.write_text(FAKE_CODEX, encoding="utf-8")
        self.capture = self.root / "argv.json"

    def tearDown(self) -> None:
        os.environ.pop("CODING_CONTROLLER_CAPTURE", None)
        self.temporary.cleanup()

    def _prepare_overlay(self, *, role: str = "subagent") -> Path:
        source = self.root / "overlay-source"
        source.mkdir()
        (source / "instructions.md").write_bytes(b"# overlay instructions\n")
        (source / "config").mkdir()
        (source / "config" / "settings.txt").write_bytes(b"overlay setting\n")
        ingest_super_cache(source_folder=source, harness_worktree=self.cache_root)
        receipt = self.workspace / "overlay-receipt.json"
        prepare_worktree(
            super_cache=self.cache_root / SUPER_CACHE_NAME,
            target_worktree=self.lane,
            role=role,
            receipt_path=receipt,
        )
        return receipt

    def _prepare_overlay_with_empty_file(self) -> Path:
        source = self.root / "overlay-source-empty"
        source.mkdir()
        (source / "instructions.md").write_bytes(b"# overlay instructions\n")
        (source / "empty-marker.txt").write_bytes(b"")
        ingest_super_cache(source_folder=source, harness_worktree=self.cache_root)
        receipt = self.workspace / "overlay-receipt.json"
        prepare_worktree(
            super_cache=self.cache_root / SUPER_CACHE_NAME,
            target_worktree=self.lane,
            role="subagent",
            receipt_path=receipt,
        )
        return receipt

    def _invocation(self, receipt: Path | None) -> Path:
        common = Path(_git(self.lane, "rev-parse", "--git-common-dir"))
        if not common.is_absolute():
            common = (self.lane / common).resolve()
        branch = _git(self.lane, "symbolic-ref", "--short", "HEAD")
        prompt = self.workspace / "prompt.md"
        prompt.write_text("overlay lane prompt\n", encoding="utf-8")
        value: dict[str, object] = {
            "schema": controller.CODING_INVOCATION_SCHEMA,
            "action": "start",
            "run_root": str(self.lane),
            "runtime_root": str(self.runtime),
            "event_log_path": str(self.runtime / "events" / "controller.jsonl"),
            "worker_invocation_id": "worker-overlay",
            "lane_id": "overlay-lane",
            "task": "overlay lane task",
            "phase": "implementation",
            "prompt_path": str(prompt),
            "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
            "output_paths": {
                "status": str(self.workspace / "controller.status.json"),
                "jsonl": str(self.workspace / "codex.jsonl"),
                "stderr": str(self.workspace / "codex.stderr.log"),
                "last_message": str(self.workspace / "last-message.txt"),
            },
            "resources": ["workspace"],
            "repository": {
                "common_dir": str(common.resolve()),
                "worktree_root": str(self.lane.resolve()),
                "branch": branch,
                "base_commit": self.revision,
            },
            "codex": {
                "model": "synthetic",
                "reasoning_effort": "medium",
                "service_tier": "priority",
                "command": [sys.executable, str(self.fake)],
                "config_overrides": [],
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        if receipt is not None:
            value["overlay_receipt"] = str(receipt)
        path = self.workspace / "start.invocation.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def _status(self) -> dict[str, object]:
        return json.loads(
            (self.workspace / "controller.status.json").read_text(encoding="utf-8")
        )

    def _retire(self, receipt: Path) -> RetirementResult:
        evidence = self.root / "evidence"
        evidence.mkdir()
        refs: list[Path] = []
        for name in (
            "task",
            "result",
            "findings",
            "acceptance",
            "transcript",
            "dependency",
        ):
            ref = evidence / f"{name}.json"
            ref.write_text("{}\n", encoding="utf-8")
            refs.append(ref)
        with mock.patch(
            "orchestrator_harness.lane_lifecycle.process_snapshot",
            return_value=ProcessSnapshot(True, (), (), "synthetic-test"),
        ):
            return retire_terminal_lane(
                self.lane,
                self.root / "archive",
                lane_id="overlay-lane",
                task_ref=refs[0],
                result_ref=refs[1],
                findings_ref=refs[2],
                acceptance_ref=refs[3],
                transcript_ref=refs[4],
                dependency_ref=refs[5],
                overlay_receipt=receipt,
            )

    def test_prelaunch_verification_rejects_wrong_role_before_process_start(
        self,
    ) -> None:
        receipt = self._prepare_overlay(role="orchestrator")
        path = self._invocation(receipt)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(1, controller.main([str(path)]))
        self.assertFalse(self.capture.exists(), "provider process must never start")
        status = self._status()
        self.assertEqual("LAUNCH_FAILED", status["state"])
        self.assertIn("role", str(status["error"]))

    def test_prelaunch_verification_allows_completed_matching_receipt(self) -> None:
        receipt = self._prepare_overlay(role="subagent")
        path = self._invocation(receipt)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        self.assertTrue(self.capture.exists())
        status = self._status()
        self.assertTrue(status.get("overlay_receipt_verified"))
        self.assertEqual(str(receipt), status.get("overlay_receipt"))

    def test_retirement_restores_prepared_overlay_and_records_restoration(self) -> None:
        receipt = self._prepare_overlay(role="subagent")
        path = self._invocation(receipt)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        self.assertTrue((self.lane / "instructions.md").is_file())
        result = self._retire(receipt)
        self.assertEqual("CLOSED", result.outcome, result)
        self.assertFalse(self.lane.exists())
        archive = json.loads(
            cast(Path, result.archive_path).read_text(encoding="utf-8")
        )
        restoration = archive["overlay_restoration"]
        self.assertEqual("RESTORED", restoration["outcome"])
        self.assertTrue(restoration["removed_paths"])

    def test_retirement_blocks_and_stays_visible_when_restore_finds_later_edit(
        self,
    ) -> None:
        receipt = self._prepare_overlay(role="subagent")
        path = self._invocation(receipt)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        (self.lane / "instructions.md").write_text(
            "agent later edit\n", encoding="utf-8"
        )
        result = self._retire(receipt)
        self.assertEqual("VISIBLE", result.outcome)
        self.assertTrue(result.reason.startswith("OVERLAY_RESTORE_BLOCKED"))
        self.assertTrue(self.lane.exists())
        self.assertEqual(
            "agent later edit\n",
            (self.lane / "instructions.md").read_text(encoding="utf-8"),
        )

    def test_retirement_rejects_foreign_orchestrator_receipt_before_restoration(
        self,
    ) -> None:
        # WO-R1-002: a completed foreign orchestrator receipt must be rejected
        # before any restoration, leaving both worktrees unchanged.
        receipt = self._prepare_overlay(role="subagent")
        path = self._invocation(receipt)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        self.assertTrue((self.lane / "instructions.md").is_file())

        foreign = self.root / "foreign"
        _git(
            self.main_repo,
            "worktree",
            "add",
            "-b",
            "foreign-orchestrator",
            str(foreign),
            "HEAD",
        )
        (foreign / ".agent-workspace").mkdir()
        foreign_source = self.root / "foreign-overlay-source"
        foreign_source.mkdir()
        (foreign_source / "orchestrator-notes.md").write_bytes(
            b"orchestrator overlay\n"
        )
        ingest_super_cache(
            source_folder=foreign_source, harness_worktree=self.cache_root
        )
        foreign_receipt = foreign / ".agent-workspace" / "orchestrator-receipt.json"
        prepare_worktree(
            super_cache=self.cache_root / SUPER_CACHE_NAME,
            target_worktree=foreign,
            role="orchestrator",
            receipt_path=foreign_receipt,
        )
        self.assertTrue((foreign / "orchestrator-notes.md").is_file())

        result = self._retire(foreign_receipt)
        self.assertEqual("VISIBLE", result.outcome)
        self.assertTrue(result.reason.startswith("OVERLAY_RESTORE_BLOCKED"))
        self.assertTrue(self.lane.exists())
        self.assertTrue((self.lane / "instructions.md").is_file())
        self.assertTrue(foreign.exists())
        self.assertTrue((foreign / "orchestrator-notes.md").is_file())

    def test_retirement_restores_matching_subagent_receipt(self) -> None:
        # WO-R1-002: a matching completed subagent receipt still restores
        # through ordinary lane retirement; the empty created file also proves
        # WO-R1-001 end-to-end through the full retirement seam.
        receipt = self._prepare_overlay_with_empty_file()
        raw = json.loads(receipt.read_text(encoding="utf-8"))
        self.assertEqual("", raw["post_prepare_bytes"]["empty-marker.txt"])
        path = self._invocation(receipt)
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        self.assertTrue((self.lane / "empty-marker.txt").is_file())
        result = self._retire(receipt)
        self.assertEqual("CLOSED", result.outcome, result)
        self.assertFalse(self.lane.exists())
        archive = json.loads(
            cast(Path, result.archive_path).read_text(encoding="utf-8")
        )
        restoration = archive["overlay_restoration"]
        self.assertEqual("RESTORED", restoration["outcome"])
        self.assertIn("empty-marker.txt", restoration["removed_paths"])


if __name__ == "__main__":
    unittest.main()
