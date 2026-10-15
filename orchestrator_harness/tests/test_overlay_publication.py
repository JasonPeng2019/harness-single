"""Real native interleavings for captured overlay publication."""
from __future__ import annotations

from contextlib import ExitStack
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import bootstrap, mutation, stable_io


class OverlayPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="overlay-native-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).absolute()
        self.source = self.base / "source"
        self.destination = self.base / "destination"
        (self.source / "nested").mkdir(parents=True)
        (self.destination / "nested").mkdir(parents=True)
        (self.source / "nested/a-first").write_bytes(b"new first")
        (self.source / "z-later").write_bytes(b"new later")

    def copy(self, **kwargs):
        bootstrap._copy_overlay(self.source, self.destination, **kwargs)

    def after_plan(self, callback):
        original = bootstrap._overlay_plan
        def plan(*args, **kwargs):
            captured = original(*args, **kwargs)
            callback()
            return captured
        return patch.object(bootstrap, "_overlay_plan", side_effect=plan)

    def assert_no_output(self):
        self.assertFalse((self.destination / "nested/a-first").exists())
        self.assertFalse((self.destination / "z-later").exists())

    def one_file(self):
        (self.source / "nested/a-first").unlink()
        (self.source / "nested").rmdir()

    def test_copies_captured_bytes_and_preserves_unselected_entries(self):
        unrelated = self.destination / "unrelated"
        unrelated.write_bytes(b"preserved")
        self.copy()
        self.assertEqual(b"new first", (self.destination / "nested/a-first").read_bytes())
        self.assertEqual(b"new later", (self.destination / "z-later").read_bytes())
        self.assertEqual(b"preserved", unrelated.read_bytes())
        self.assertEqual(b"new first", (self.source / "nested/a-first").read_bytes())

    def test_copies_permissions_and_modification_time(self):
        source = self.source / "nested/a-first"
        source.chmod(0o755)
        os.utime(source, ns=(1700000000123456000, 1700000000123456000))
        expected = source.stat()
        self.copy()
        actual = (self.destination / "nested/a-first").stat()
        self.assertEqual(stat.S_IMODE(expected.st_mode), stat.S_IMODE(actual.st_mode))
        self.assertEqual(expected.st_mtime_ns, actual.st_mtime_ns)

    @unittest.skipUnless(os.name == "nt", "native Windows readonly attribute check")
    def test_native_readonly_source_metadata_survives_publication(self):
        source = self.source / "nested/a-first"
        source.chmod(0o444)
        self.addCleanup(lambda: source.chmod(0o666))
        self.copy()
        target = self.destination / "nested/a-first"
        self.addCleanup(lambda: target.chmod(0o666))
        self.assertFalse(target.stat().st_mode & stat.S_IWRITE)
        self.assertEqual(b"new first", target.read_bytes())

    def test_identical_files_keep_identity_and_modification_time(self):
        first = self.destination / "nested/a-first"
        first.write_bytes(b"new first")
        expected = first.stat()
        self.copy()
        actual = first.stat()
        self.assertEqual((expected.st_dev, expected.st_ino, expected.st_mtime_ns),
                         (actual.st_dev, actual.st_ino, actual.st_mtime_ns))
        self.assertEqual(b"new later", (self.destination / "z-later").read_bytes())

    def test_predecessor_bytes_authorize_replacement(self):
        old = self.base / "predecessor"
        old.write_bytes(b"prior first")
        target = self.destination / "nested/a-first"
        target.write_bytes(b"prior first")
        self.copy(replace_if_matches={"nested/a-first": old})
        self.assertEqual(b"new first", target.read_bytes())
        self.assertEqual(b"prior first", old.read_bytes())

    def test_collision_rejects_all_writes(self):
        target = self.destination / "z-later"
        target.write_bytes(b"foreign")
        with self.assertRaises(bootstrap.BootstrapError) as raised:
            self.copy()
        self.assertEqual(bootstrap.BOOTSTRAP_CACHE_COLLISION, raised.exception.code)
        self.assertFalse((self.destination / "nested/a-first").exists())
        self.assertEqual(b"foreign", target.read_bytes())

    def test_missing_source_and_exclusions_do_not_create_outputs(self):
        bootstrap._copy_overlay(self.base / "missing", self.destination)
        self.copy(exclude=("nested/a-first", "z-later"))
        self.assert_no_output()

    def test_absent_destination_is_created_from_captured_parent(self):
        target = self.base / "absent/deeper"
        bootstrap._copy_overlay(self.source, target)
        self.assertEqual(b"new first", (target / "nested/a-first").read_bytes())
        self.assertEqual(b"new later", (target / "z-later").read_bytes())

    def test_source_tree_replacement_is_rejected_before_any_output(self):
        retained = self.base / "retained-source"
        def substitute():
            self.source.rename(retained)
            self.source.mkdir()
            (self.source / "foreign").write_bytes(b"foreign source")
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assert_no_output()
        self.assertEqual(b"new first", (retained / "nested/a-first").read_bytes())
        self.assertEqual(b"foreign source", (self.source / "foreign").read_bytes())

    def test_byte_identical_source_leaf_replacement_is_rejected(self):
        leaf = self.source / "nested/a-first"
        retained = self.base / "retained-source-leaf"
        def substitute():
            leaf.rename(retained)
            leaf.write_bytes(b"new first")
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assert_no_output()
        self.assertEqual(b"new first", leaf.read_bytes())
        self.assertEqual(b"new first", retained.read_bytes())

    def test_source_metadata_change_is_rejected_before_any_output(self):
        source = self.source / "nested/a-first"
        def substitute():
            os.utime(source, ns=(1600000000000000000, 1600000000000000000))
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assert_no_output()

    def test_destination_root_replacement_preserves_both_owners(self):
        retained = self.base / "retained-destination"
        def substitute():
            self.destination.rename(retained)
            self.destination.mkdir()
            (self.destination / "foreign").write_bytes(b"foreign destination")
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assertEqual(b"foreign destination", (self.destination / "foreign").read_bytes())
        self.assertFalse((retained / "nested/a-first").exists())

    def test_plain_parent_replacement_is_rejected_before_any_output(self):
        parent = self.destination / "nested"
        retained = self.base / "retained-parent"
        def substitute():
            parent.rename(retained)
            parent.mkdir()
            (parent / "foreign").write_bytes(b"foreign parent")
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assert_no_output()
        self.assertEqual(b"foreign parent", (parent / "foreign").read_bytes())
        self.assertTrue(retained.is_dir())

    @unittest.skipUnless(os.name == "nt", "native Windows junction check")
    def test_junction_after_planning_preserves_referent(self):
        parent = self.destination / "nested"
        retained = self.base / "retained-parent"
        referent = self.base / "referent"
        referent.mkdir()
        foreign = referent / "a-first"
        foreign.write_bytes(b"referent owner")
        def substitute():
            parent.rename(retained)
            subprocess.run(["cmd.exe", "/c", "mklink", "/J", str(parent), str(referent)],
                           capture_output=True, text=True, check=True)
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assertEqual(b"referent owner", foreign.read_bytes())
        self.assertFalse((self.destination / "z-later").exists())

    def test_identical_destination_leaf_substitution_is_rejected(self):
        target = self.destination / "nested/a-first"
        target.write_bytes(b"new first")
        retained = self.base / "retained-identical"
        def substitute():
            target.rename(retained)
            target.write_bytes(b"new first")
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assertFalse((self.destination / "z-later").exists())
        self.assertEqual(b"new first", retained.read_bytes())
        self.assertEqual(b"new first", target.read_bytes())

    def test_predecessor_leaf_substitution_with_same_bytes_is_rejected(self):
        old = self.base / "predecessor"
        old.write_bytes(b"prior first")
        (self.destination / "nested/a-first").write_bytes(b"prior first")
        retained = self.base / "retained-predecessor"
        def substitute():
            old.rename(retained)
            old.write_bytes(b"prior first")
        with self.after_plan(substitute):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy(replace_if_matches={"nested/a-first": old})
        self.assertEqual(b"prior first", (self.destination / "nested/a-first").read_bytes())
        self.assertFalse((self.destination / "z-later").exists())

    def test_parent_substitution_at_publication_entry_is_not_adopted(self):
        original = stable_io.mutation_replace
        parent = self.destination / "nested"
        retained = self.base / "retained-last-use-parent"
        def publish(*args, **kwargs):
            parent.rename(retained)
            parent.mkdir()
            (parent / "foreign").write_bytes(b"new last-use parent")
            return original(*args, **kwargs)
        with patch.object(stable_io, "mutation_replace", side_effect=publish):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assert_no_output()
        self.assertEqual(b"new last-use parent", (parent / "foreign").read_bytes())
        self.assertTrue(retained.is_dir())

    def test_late_file_at_native_no_replace_commit_is_preserved(self):
        self.one_file()
        target = self.destination / "z-later"
        if os.name == "nt":
            original = mutation._windows_rename_handle
            def rename(*args, **kwargs):
                target.write_bytes(b"late native owner")
                return original(*args, **kwargs)
            injection = patch.object(mutation, "_windows_rename_handle", side_effect=rename)
        else:
            original = mutation._posix_rename
            def rename(*args, **kwargs):
                target.write_bytes(b"late native owner")
                return original(*args, **kwargs)
            injection = patch.object(mutation, "_posix_rename", side_effect=rename)
        with injection:
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assertEqual(b"late native owner", target.read_bytes())
        self.assertFalse(any(path.name.startswith(".z-later.") for path in self.destination.iterdir()))

    def test_late_directory_at_native_creation_is_preserved(self):
        parent = self.destination / "nested"
        parent.rmdir()
        with ExitStack() as injection:
            if os.name == "nt":
                original = mutation._windows_open_directory
                def create(path, **kwargs):
                    if kwargs.get("create"):
                        parent.mkdir()
                        (parent / "foreign").write_bytes(b"late directory owner")
                    return original(path, **kwargs)
                injection.enter_context(patch.object(mutation, "_windows_open_directory", side_effect=create))
            else:
                original = mutation.os.mkdir
                def create(path, mode=0o777, *, dir_fd=None):
                    if dir_fd is not None and str(path) == "nested":
                        original(parent)
                        (parent / "foreign").write_bytes(b"late directory owner")
                    return original(path, mode, dir_fd=dir_fd)
                injection.enter_context(patch.object(mutation.os, "mkdir", new=create))
                injection.enter_context(patch.object(mutation.os, "supports_dir_fd",
                                                      mutation.os.supports_dir_fd | {create}))
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assertEqual(b"late directory owner", (parent / "foreign").read_bytes())
        self.assert_no_output()

    def test_late_foreign_second_file_reports_partial_publication(self):
        original = stable_io.PreparedOutputTransaction.write_bytes
        foreign = self.destination / "z-later"
        def publish(output, path, data, **kwargs):
            original(output, path, data, **kwargs)
            foreign.write_bytes(b"foreign during publication")
        with patch.object(stable_io.PreparedOutputTransaction, "write_bytes", new=publish):
            with self.assertRaises(bootstrap.BootstrapError):
                self.copy()
        self.assertEqual(b"new first", (self.destination / "nested/a-first").read_bytes())
        self.assertEqual(b"foreign during publication", foreign.read_bytes())
        self.assertEqual(b"new later", (self.source / "z-later").read_bytes())

    def test_overlapping_source_and_destination_are_rejected(self):
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap._copy_overlay(self.source, self.source / "nested")
        self.assertEqual(b"new first", (self.source / "nested/a-first").read_bytes())

    def test_same_source_noop_preserves_identity_and_modification_time(self):
        source = self.source / "nested/a-first"
        before = source.stat()
        bootstrap._copy_overlay(self.source, self.source)
        after = source.stat()
        self.assertEqual((before.st_dev, before.st_ino, before.st_mtime_ns),
                         (after.st_dev, after.st_ino, after.st_mtime_ns))
        self.assertEqual(b"new first", source.read_bytes())

    def test_ancestor_destination_can_publish_outside_observed_source(self):
        bootstrap._copy_overlay(self.source, self.base)
        self.assertEqual(b"new first", (self.base / "nested/a-first").read_bytes())
        self.assertEqual(b"new later", (self.base / "z-later").read_bytes())
        self.assertEqual(b"new first", (self.source / "nested/a-first").read_bytes())
        self.assertEqual(b"new later", (self.source / "z-later").read_bytes())

    @unittest.skipUnless(os.name == "nt", "native Windows timestamp range check")
    def test_invalid_native_metadata_rejects_before_existing_file_update(self):
        target = self.destination / "z-later"
        target.write_bytes(b"original owner")
        before = mutation.capture_target(self.destination, "z-later")
        invalid = mutation.FileMetadata(stat.S_IMODE(target.stat().st_mode),
                                        target.stat().st_atime_ns, -11644473600000000000)
        with self.assertRaises(mutation.MutationConflict):
            mutation.replace(self.destination, "z-later", b"must not be written",
                             expected=before, metadata=invalid)
        self.assertEqual(b"original owner", target.read_bytes())


if __name__ == "__main__":
    unittest.main()
