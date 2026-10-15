"""Actual filesystem interleavings for validated captured-tree removal."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import mutation, setup


class CapturedTreeMutationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="captured-tree-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).absolute()
        self.root = self.base / "root"
        self.root.mkdir()
        self.tree = self.root / "tree"
        (self.tree / "nested").mkdir(parents=True)
        (self.tree / "a-first").write_bytes(b"original first")
        (self.tree / "nested/z-last").write_bytes(b"original last")

    def capture(self):
        return mutation.capture_tree(self.root, "tree")

    def assert_original_retained(self):
        self.assertEqual(b"original first", (self.tree / "a-first").read_bytes())
        self.assertEqual(b"original last", (self.tree / "nested/z-last").read_bytes())

    def test_complete_capture_removes_only_selected_tree(self):
        sentinel = self.root / "sentinel"
        sentinel.write_bytes(b"unselected")
        captured = self.capture()
        self.assertEqual({Path("tree"), Path("tree/nested"), Path("tree/a-first"), Path("tree/nested/z-last")},
                         {path for path, state in captured.entries})
        state = dict(captured.entries)[Path("tree/a-first")]
        self.assertEqual(b"original first", state.content)
        self.assertEqual(hashlib.sha256(state.content).hexdigest(), state.content_sha256)
        mutation.remove_tree(self.root, "tree", captured=captured)
        self.assertFalse(self.tree.exists())
        self.assertEqual(b"unselected", sentinel.read_bytes())

    def test_absent_capture_does_not_adopt_later_tree(self):
        captured = mutation.capture_tree(self.root, "absent")
        target = self.root / "absent"
        target.mkdir()
        (target / "foreign").write_bytes(b"new owner")
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "absent", captured=captured)
        self.assertEqual(b"new owner", (target / "foreign").read_bytes())

    def test_absent_tree_removal_is_idempotent(self):
        captured = mutation.capture_tree(self.root, "absent")
        mutation.remove_tree(self.root, "absent", captured=captured)
        mutation.remove_tree(self.root, "absent")
        self.assertFalse((self.root / "absent").exists())

    def test_replaced_tree_after_capture_preserves_both_owners(self):
        captured = self.capture()
        retained = self.root / "retained-tree"
        self.tree.rename(retained)
        self.tree.mkdir()
        (self.tree / "foreign").write_bytes(b"new owner")
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "tree", captured=captured)
        self.assertEqual(b"new owner", (self.tree / "foreign").read_bytes())
        self.assertEqual(b"original first", (retained / "a-first").read_bytes())

    def test_replaced_leaf_with_identical_bytes_is_not_adopted(self):
        captured = self.capture()
        leaf = self.tree / "nested/z-last"
        retained = self.root / "retained-leaf"
        leaf.rename(retained)
        leaf.write_bytes(b"original last")
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "tree", captured=captured)
        self.assert_original_retained()
        self.assertEqual(b"original last", retained.read_bytes())

    def test_changed_bytes_on_same_leaf_fail_before_first_deletion(self):
        captured = self.capture()
        leaf = self.tree / "nested/z-last"
        identity = (leaf.stat().st_dev, leaf.stat().st_ino)
        leaf.write_bytes(b"changed last")
        self.assertEqual(identity, (leaf.stat().st_dev, leaf.stat().st_ino))
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "tree", captured=captured)
        self.assertEqual(b"original first", (self.tree / "a-first").read_bytes())
        self.assertEqual(b"changed last", leaf.read_bytes())

    def test_membership_addition_is_retained_before_any_deletion(self):
        captured = self.capture()
        foreign = self.tree / "nested/new-owner"
        foreign.write_bytes(b"foreign")
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "tree", captured=captured)
        self.assert_original_retained()
        self.assertEqual(b"foreign", foreign.read_bytes())

    def test_snapshot_of_another_root_or_path_is_refused(self):
        captured = self.capture()
        other = self.base / "other"
        other.mkdir()
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(other, "tree", captured=captured)
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "other", captured=captured)
        self.assert_original_retained()

    def test_incomplete_census_cannot_delete_uncaptured_entries(self):
        captured = self.capture()
        incomplete = replace(captured, entries=tuple((path, state) for path, state in captured.entries
                                                    if path != Path("tree/nested/z-last")))
        with self.assertRaises(mutation.MutationConflict):
            mutation.remove_tree(self.root, "tree", captured=incomplete)
        self.assert_original_retained()

    def test_late_addition_after_deletion_reports_partial_cleanup_and_preserves_foreign_file(self):
        captured = self.capture()
        foreign = self.tree / "new-owner"
        if os.name == "nt":
            real_delete = mutation._windows_delete_existing
            def delete(path, state):
                real_delete(path, state)
                foreign.write_bytes(b"foreign during cleanup")
            operation = patch.object(mutation, "_windows_delete_existing", side_effect=delete)
        else:
            real_delete = mutation.os.unlink
            def delete(path, *, dir_fd=None):
                real_delete(path, dir_fd=dir_fd)
                foreign.write_bytes(b"foreign during cleanup")
            operation = patch.object(mutation.os, "unlink", new=delete)
        with ExitStack() as interleaving:
            interleaving.enter_context(operation)
            if os.name != "nt":
                # The wrapper delegates the real dir_fd unlink; retain that
                # capability declaration for the primitive's native host gate.
                interleaving.enter_context(patch.object(
                    mutation.os, "supports_dir_fd", mutation.os.supports_dir_fd | {delete}))
            with self.assertRaises(OSError):
                mutation.remove_tree(self.root, "tree", captured=captured)
        self.assertTrue(self.tree.exists())
        self.assertFalse((self.tree / "nested/z-last").exists())
        self.assertEqual(b"foreign during cleanup", foreign.read_bytes())

    def test_setup_carries_prevalidation_census_into_removal(self):
        manifest = setup._manifest_for_tree(self.tree)
        real_check = setup._check_owned_tree
        retained = self.root / "retained-tree"
        def validate_then_substitute(*args, **kwargs):
            result = real_check(*args, **kwargs)
            self.tree.rename(retained)
            self.tree.mkdir()
            (self.tree / "foreign").write_bytes(b"foreign after validation")
            return result
        with patch.object(setup, "_check_owned_tree", side_effect=validate_then_substitute):
            with self.assertRaises(mutation.MutationConflict):
                setup._remove_owned_tree(self.tree, manifest, verify_bytes=True)
        self.assertEqual(b"original first", (retained / "a-first").read_bytes())
        self.assertEqual(b"foreign after validation", (self.tree / "foreign").read_bytes())

    def test_setup_partial_owned_tree_can_complete_verified_cleanup(self):
        manifest = setup._manifest_for_tree(self.tree)
        (self.tree / "nested/z-last").unlink()
        setup._remove_owned_tree(self.tree, manifest, verify_bytes=True)
        self.assertFalse(self.tree.exists())

    def test_promoted_backup_substitution_after_validation_is_preserved(self):
        staging = self.root / "staging"
        staging.mkdir()
        (staging / "new").write_bytes(b"promoted")
        old = setup._manifest_for_tree(self.tree)
        retained = self.root / "retained-backup"
        def validate_backup(path):
            setup._check_owned_tree(path, old, verify_bytes=True)
            path.rename(retained)
            path.mkdir()
            (path / "foreign").write_bytes(b"foreign backup")
        with self.assertRaises(mutation.MutationConflict):
            setup._replace_tree(staging, self.tree, validate_backup=validate_backup)
        self.assertEqual(b"promoted", (self.tree / "new").read_bytes())
        self.assertEqual(b"original first", (retained / "a-first").read_bytes())
        self.assertEqual(b"foreign backup", (self.root / "tree.old/foreign").read_bytes())


if __name__ == "__main__":
    unittest.main()
