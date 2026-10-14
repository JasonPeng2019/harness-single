"""Owned native fixtures for every anchored filesystem operation."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from harness_common import platform_paths, process_identity
from orchestrator_harness import mutation, records
from orchestrator_harness.stable_io import PreparedOutputTransaction, read_stable, read_tail_stable


class PlatformMutationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="p")
        self.addCleanup(temporary.cleanup)
        self.fixture = Path(temporary.name).resolve()
        self.root = self.fixture / "owned"
        self.root.mkdir()
        self.protected = self.fixture / "protected"
        self.protected.mkdir()
        (self.protected / "sentinel").write_bytes(b"retain protected bytes")

    def directory_link(self, path, target):
        if os.name == "nt":
            subprocess.run(["cmd", "/c", "mklink", "/J", str(path), str(target)],
                           check=True, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            path.symlink_to(target, target_is_directory=True)

    def symbolic_link(self, path, value):
        try:
            path.symlink_to(value)
        except OSError as exc:
            if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
                self.skipTest("native Windows token lacks symbolic-link creation privilege")
            raise

    def test_all_primitives_preserve_complete_bytes_and_exact_identities(self):
        mutation.ensure_directory_path(self.root / "a/nested")
        mutation.ensure_directory_path(self.root / "b")
        mutation.publish_bytes(self.root, "a/nested/data", b"first")
        old = mutation.capture_target(self.root, "a/nested/data")
        receipt = mutation._publish(self.root, "a/nested/data", b"second", expected=None,
                                    expected_root_identity=None)
        self.assertEqual(old, receipt.expected)
        self.assertEqual(b"second", receipt.resulting.content)
        self.assertNotEqual(old.identity, receipt.resulting.identity)
        self.assertEqual(b"second", mutation.read_bytes(self.root, "a/nested/data"))
        appended = mutation.append_bytes(self.root, "a/nested/data", b" appended")
        self.assertEqual(b"second appended", appended.content)
        self.assertEqual(hashlib.sha256(appended.content).hexdigest(), appended.content_sha256)
        with mutation.open_append_file(self.root, "empty") as handle:
            self.assertEqual(0, os.fstat(handle.fileno()).st_size)
        moved = mutation.rename(self.root, "a/nested/data", "b/moved")
        self.assertEqual(appended.identity, moved.resulting.identity)
        self.assertFalse((self.root / "a/nested/data").exists())
        mutation.rename(self.root, "a/nested", "b/nested")
        temporary = mutation.make_temporary_directory(self.root, prefix=".stage.")
        self.assertEqual(self.root, temporary.parent)
        deletion = mutation.delete(self.root, "b/moved", expected=moved.resulting)
        self.assertFalse(deletion.resulting.present)
        self.assertFalse(mutation.delete(self.root, "absent").resulting.present)
        mutation.remove_tree(self.root, "b")
        mutation.remove_tree(self.root, temporary.name)
        self.assertFalse((self.root / "b").exists())
        self.assertFalse(temporary.exists())
        self.assertEqual(b"retain protected bytes", (self.protected / "sentinel").read_bytes())

    def test_rename_compares_identity_only_when_content_was_not_captured(self):
        (self.root / "source").write_bytes(b"native file")
        source = mutation._capture_path(self.root / "source", include_content=False)
        moved = mutation.rename(self.root, "source", "target", expected_source=source,
                                expected_target=mutation.TargetState.absent())
        self.assertEqual(source.identity, moved.resulting.identity)
        self.assertEqual(b"native file", mutation.read_bytes(self.root, "target"))

    def test_rename_can_replace_an_authorized_plain_destination(self):
        (self.root / "source").write_bytes(b"new complete bytes")
        (self.root / "target").write_bytes(b"old")
        receipt = mutation.rename(self.root, "source", "target",
            expected_source=mutation.capture_target(self.root, "source"),
            expected_target=mutation.capture_target(self.root, "target"))
        self.assertEqual(b"new complete bytes", receipt.resulting.content)
        self.assertFalse((self.root / "source").exists())

    def test_atomic_failure_preserves_prior_bytes_and_removes_owned_temporary(self):
        (self.root / "record").write_bytes(b"old complete bytes")
        function = "_replace_with_retry" if os.name == "nt" else "_posix_rename"
        module = records if os.name == "nt" else mutation
        with patch.object(module, function, side_effect=OSError("controlled publication failure")):
            with self.assertRaises(OSError):
                with records.RecordLock(self.root / "record", root=self.root):
                    records.atomic_write_bytes(self.root / "record", b"new bytes")
        (self.root / ".record.lock").unlink()
        self.assertEqual(b"old complete bytes", (self.root / "record").read_bytes())
        self.assertEqual(["record"], sorted(item.name for item in self.root.iterdir()))

    def test_conditional_update_verifies_bytes_and_platform_identity_semantics(self):
        target = self.root / "record"
        target.write_bytes(b"long authorized original")
        expected = mutation.capture_target(self.root, "record")
        result = mutation.replace(self.root, "record", b"short", expected=expected)
        self.assertEqual(expected, result.expected)
        self.assertEqual(b"short", result.resulting.content)
        self.assertEqual(b"short", mutation.read_bytes(self.root, "record"))
        if os.name == "nt":
            self.assertEqual(expected.identity, result.resulting.identity)
        else:
            self.assertNotEqual(expected.identity, result.resulting.identity)
        mutation.replace(self.root, "record", b"", expected=result.resulting)
        self.assertEqual(b"", target.read_bytes())

    def test_record_lock_publication_commits_complete_staged_bytes_atomically(self):
        target = self.root / "record"
        target.write_bytes(b"old complete bytes")
        expected = mutation.capture_target(self.root, "record")
        data = b"complete new record\n" * 4096
        original = records._replace_with_retry if os.name == "nt" else mutation._posix_rename
        def inspect_stage(*args, **kwargs):
            staged = Path(args[0]) if os.name == "nt" else target.parent / args[1]
            self.assertEqual(data, staged.read_bytes())
            self.assertEqual(b"old complete bytes", target.read_bytes())
            return original(*args, **kwargs)
        module, name = (records, "_replace_with_retry") if os.name == "nt" else (mutation, "_posix_rename")
        with records.RecordLock(target, root=self.root), patch.object(module, name, side_effect=inspect_stage) as commit:
            records.atomic_write_bytes(target, data)
        commit.assert_called_once()
        published = mutation.capture_target(self.root, "record")
        self.assertNotEqual(expected.identity, published.identity)
        self.assertEqual(data, published.content)
        self.assertEqual([".record.lock", "record"], sorted(p.name for p in self.root.iterdir()))

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_conditional_update_rejects_substitution_at_native_acquisition(self):
        target, foreign = self.root / "record", self.root / "foreign"
        target.write_bytes(b"authorized original")
        foreign.write_bytes(b"foreign publication")
        expected = mutation.capture_target(self.root, "record")
        original = mutation._windows_file_handle
        acquisitions = []
        def substitute(path, **kwargs):
            if Path(path) == target and kwargs["access"] & mutation._GENERIC_WRITE:
                acquisitions.append(path)
                os.replace(foreign, target)
            return original(path, **kwargs)
        with patch.object(mutation, "_windows_file_handle", side_effect=substitute):
            with self.assertRaises(mutation.MutationConflict):
                mutation.replace(self.root, "record", b"forbidden", expected=expected)
        self.assertEqual([target], acquisitions)
        self.assertEqual(b"foreign publication", target.read_bytes())
        self.assertEqual(["record"], sorted(p.name for p in self.root.iterdir()))
        target.unlink()  # Failed acquisition must not retain a no-delete handle.

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_conditional_update_holds_target_through_actual_write(self):
        target, foreign = self.root / "record", self.root / "foreign"
        target.write_bytes(b"authorized original")
        foreign.write_bytes(b"foreign publication")
        expected = mutation.capture_target(self.root, "record")
        original = mutation._windows_open_existing_for_update
        attempts = []
        fixture = self
        class Writer:
            def __init__(self, handle):
                self.handle = handle
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.handle.close()
            def __getattr__(self, name):
                return getattr(self.handle, name)
            def write(self, data):
                attempts.append("actual write")
                with fixture.assertRaises(OSError):
                    os.replace(foreign, target)
                with fixture.assertRaises(OSError):
                    target.write_bytes(b"foreign in-place write")
                return self.handle.write(data)
        with patch.object(mutation, "_windows_open_existing_for_update",
                          side_effect=lambda *args: Writer(original(*args))) as acquire:
            result = mutation.replace(self.root, "record", b"new bytes", expected=expected)
        acquire.assert_called_once_with(target, expected)
        self.assertEqual(["actual write"], attempts)
        self.assertEqual(expected.identity, result.resulting.identity)
        self.assertEqual(b"new bytes", target.read_bytes())
        self.assertEqual(b"foreign publication", foreign.read_bytes())
        target.unlink()

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_rename_validates_all_source_facts_at_final_acquisition(self):
        source, target = self.root / "source", self.root / "target"
        source.write_bytes(b"authorized source")
        target.write_bytes(b"authorized destination")
        source_state, target_state = mutation.capture_target(self.root, "source"), mutation.capture_target(self.root, "target")
        original = mutation._windows_file_handle
        acquisitions = []
        def change_source(path, **kwargs):
            if Path(path) == source and kwargs["access"] & mutation._DELETE:
                acquisitions.append(path)
                source.write_bytes(b"foreign source")
            return original(path, **kwargs)
        with patch.object(mutation, "_windows_file_handle", side_effect=change_source):
            with self.assertRaises(mutation.MutationConflict):
                mutation.rename(self.root, "source", "target", expected_source=source_state, expected_target=target_state)
        self.assertEqual([source], acquisitions)
        self.assertEqual(b"foreign source", source.read_bytes())
        self.assertEqual(b"authorized destination", target.read_bytes())
        self.assertEqual(source_state.identity, mutation.capture_target(self.root, "source").identity)
        self.assertEqual(target_state, mutation.capture_target(self.root, "target"))
        self.assertEqual(["source", "target"], sorted(p.name for p in self.root.iterdir()))

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_rename_rejects_destination_substitution_at_final_acquisition(self):
        source, target, foreign = self.root / "source", self.root / "target", self.root / "foreign"
        source.write_bytes(b"authorized source")
        target.write_bytes(b"authorized destination")
        foreign.write_bytes(b"foreign publication")
        source_state, target_state = mutation.capture_target(self.root, "source"), mutation.capture_target(self.root, "target")
        original = mutation._windows_file_handle
        acquisitions = []
        def substitute(path, **kwargs):
            if Path(path) == target and kwargs["access"] & mutation._DELETE:
                acquisitions.append(path)
                os.replace(foreign, target)
            return original(path, **kwargs)
        with patch.object(mutation, "_windows_file_handle", side_effect=substitute):
            with self.assertRaises(mutation.MutationConflict):
                mutation.rename(self.root, "source", "target", expected_source=source_state, expected_target=target_state)
        self.assertEqual([target], acquisitions)
        self.assertEqual(source_state, mutation.capture_target(self.root, "source"))
        self.assertEqual(b"foreign publication", target.read_bytes())
        self.assertEqual(["source", "target"], sorted(p.name for p in self.root.iterdir()))

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_rename_holds_both_original_entries_through_native_displacement(self):
        source, target, foreign = self.root / "source", self.root / "target", self.root / "foreign"
        source.write_bytes(b"authorized source")
        target.write_bytes(b"authorized destination")
        foreign.write_bytes(b"foreign publication")
        source_state, target_state = mutation.capture_target(self.root, "source"), mutation.capture_target(self.root, "target")
        original = mutation._windows_rename_handle
        attempts = []
        def inspect_binding(handle, destination, name, **kwargs):
            self.assertFalse(kwargs["replace_existing"])
            if mutation._windows_identity(handle) == target_state.identity:
                attempts.append("bound destination")
                with self.assertRaises(OSError):
                    os.replace(foreign, target)
                with self.assertRaises(OSError):
                    target.write_bytes(b"foreign in-place destination")
                with self.assertRaises(OSError):
                    os.replace(foreign, source)
                with self.assertRaises(OSError):
                    source.write_bytes(b"foreign in-place source")
            return original(handle, destination, name, **kwargs)
        with patch.object(mutation, "_windows_rename_handle", side_effect=inspect_binding):
            result = mutation.rename(self.root, "source", "target", expected_source=source_state, expected_target=target_state)
        self.assertEqual(["bound destination"], attempts)
        self.assertEqual(source_state, result.resulting)
        self.assertEqual(b"foreign publication", foreign.read_bytes())
        self.assertEqual(["foreign", "target"], sorted(p.name for p in self.root.iterdir()))

    @unittest.skipUnless(os.name == "nt", "native Windows handle rename")
    def test_windows_rename_final_publication_preserves_foreign_target_and_retained_original(self):
        source, target, foreign = self.root / "source", self.root / "target", self.root / "foreign"
        source.write_bytes(b"authorized source")
        target.write_bytes(b"authorized destination")
        foreign.write_bytes(b"foreign publication")
        source_state, target_state = mutation.capture_target(self.root, "source"), mutation.capture_target(self.root, "target")
        original = mutation._windows_rename_handle
        attempts = []
        def substitute(handle, destination, name, **kwargs):
            self.assertFalse(kwargs["replace_existing"])
            if name == "target" and mutation._windows_identity(handle) == source_state.identity:
                attempts.append("final source publication")
                os.replace(foreign, target)
            return original(handle, destination, name, **kwargs)
        with patch.object(mutation, "_windows_rename_handle", side_effect=substitute):
            with self.assertRaisesRegex(mutation.MutationConflict, "original destination .* retained at") as conflict:
                mutation.rename(self.root, "source", "target", expected_source=source_state, expected_target=target_state)
        self.assertEqual(["final source publication"], attempts)
        self.assertEqual(source_state, mutation.capture_target(self.root, "source"))
        self.assertEqual(b"foreign publication", target.read_bytes())
        retained = list(self.root.glob(".rename.*.retained"))
        self.assertEqual(1, len(retained))
        self.assertEqual(target_state, mutation.capture_target(self.root, retained[0].name))
        self.assertIn(str(retained[0]), str(conflict.exception))
        self.assertEqual(3, len(list(self.root.iterdir())))
        retained[0].unlink()  # Error exit released the target's native ownership.

    @unittest.skipUnless(os.name == "nt", "native Windows handle rename")
    def test_windows_rename_failure_restores_bound_destination_only_into_absent_name(self):
        (self.root / "source").write_bytes(b"authorized source")
        (self.root / "target").write_bytes(b"authorized destination")
        source_state, target_state = mutation.capture_target(self.root, "source"), mutation.capture_target(self.root, "target")
        original = mutation._windows_rename_handle
        attempts = []
        def fail_publication(handle, destination, name, **kwargs):
            self.assertFalse(kwargs["replace_existing"])
            if name == "target" and mutation._windows_identity(handle) == source_state.identity:
                attempts.append("final source publication")
                raise OSError("controlled native publication failure")
            return original(handle, destination, name, **kwargs)
        with patch.object(mutation, "_windows_rename_handle", side_effect=fail_publication):
            with self.assertRaisesRegex(OSError, "controlled native publication failure"):
                mutation.rename(self.root, "source", "target", expected_source=source_state, expected_target=target_state)
        self.assertEqual(["final source publication"], attempts)
        self.assertEqual(source_state, mutation.capture_target(self.root, "source"))
        self.assertEqual(target_state, mutation.capture_target(self.root, "target"))
        self.assertEqual(["source", "target"], sorted(p.name for p in self.root.iterdir()))

    @unittest.skipUnless(os.name == "nt", "native Windows handle rename")
    def test_windows_rename_absent_destination_remains_no_replace_at_native_commit(self):
        (self.root / "source").write_bytes(b"authorized source")
        foreign = self.root / "foreign"
        foreign.write_bytes(b"foreign publication")
        source_state = mutation.capture_target(self.root, "source")
        original = mutation._windows_rename_handle
        def substitute(handle, destination, name, **kwargs):
            self.assertFalse(kwargs["replace_existing"])
            os.replace(foreign, destination.parent / name)
            return original(handle, destination, name, **kwargs)
        with patch.object(mutation, "_windows_rename_handle", side_effect=substitute) as commit:
            with self.assertRaises(mutation.MutationConflict):
                mutation.rename(self.root, "source", "target", expected_source=source_state,
                                expected_target=mutation.TargetState.absent())
        commit.assert_called_once()
        self.assertEqual(source_state, mutation.capture_target(self.root, "source"))
        self.assertEqual(b"foreign publication", (self.root / "target").read_bytes())

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_read_append_and_delete_reject_substitution_at_native_acquisition(self):
        original = mutation._windows_file_handle
        for operation_name in ("read", "append", "delete"):
            with self.subTest(operation=operation_name):
                parent = self.root / operation_name
                parent.mkdir()
                target, foreign = parent / "record", parent / "foreign"
                target.write_bytes(b"authorized original")
                foreign.write_bytes(b"foreign publication")
                expected = mutation.capture_target(parent, "record")
                acquisitions = []
                access_bit = {"read": mutation._GENERIC_READ, "append": mutation._GENERIC_WRITE,
                              "delete": mutation._DELETE}[operation_name]
                def substitute(path, **kwargs):
                    if Path(path) == target and kwargs["access"] & access_bit:
                        acquisitions.append(path)
                        os.replace(foreign, target)
                    return original(path, **kwargs)
                with patch.object(mutation, "_windows_file_handle", side_effect=substitute):
                    with self.assertRaises(mutation.MutationConflict):
                        if operation_name == "read":
                            mutation.read_bytes(parent, "record")
                        elif operation_name == "append":
                            mutation.append_bytes(parent, "record", b"forbidden")
                        else:
                            mutation.delete(parent, "record", expected=expected)
                self.assertEqual([target], acquisitions)
                self.assertEqual(b"foreign publication", target.read_bytes())
                target.unlink()

    @unittest.skipUnless(os.name == "nt", "native Windows handle sharing")
    def test_windows_read_append_and_delete_keep_native_ownership_through_actual_io(self):
        target, foreign = self.root / "record", self.root / "foreign"
        target.write_bytes(b"authorized original")
        foreign.write_bytes(b"foreign publication")
        original_open = mutation.os.fdopen
        attempts = []
        fixture = self
        class Stream:
            def __init__(self, handle):
                self.handle = handle
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.handle.close()
            def __getattr__(self, name):
                return getattr(self.handle, name)
            def read(self, *args):
                attempts.append("read")
                with fixture.assertRaises(OSError):
                    os.replace(foreign, target)
                return self.handle.read(*args)
            def write(self, data):
                attempts.append("append")
                with fixture.assertRaises(OSError):
                    os.replace(foreign, target)
                return self.handle.write(data)
        with patch.object(mutation.os, "fdopen", side_effect=lambda *a, **kw: Stream(original_open(*a, **kw))):
            self.assertEqual(b"authorized original", mutation.read_bytes(self.root, "record"))
            self.assertEqual(b"authorized original suffix", mutation.append_bytes(self.root, "record", b" suffix").content)
        self.assertEqual(["read", "append", "read"], attempts)
        original_delete = mutation._windows_mark_handle_deleted
        def inspect_delete(handle, path):
            attempts.append("delete")
            with self.assertRaises(OSError):
                os.replace(foreign, target)
            return original_delete(handle, path)
        with patch.object(mutation, "_windows_mark_handle_deleted", side_effect=inspect_delete) as commit:
            mutation.delete(self.root, "record")
        commit.assert_called_once()
        self.assertEqual("delete", attempts[-1])
        self.assertFalse(target.exists())
        self.assertEqual(b"foreign publication", foreign.read_bytes())

    def test_descriptor_conversion_failure_releases_owned_native_resources(self):
        target = self.root / "record"
        target.write_bytes(b"retained bytes")
        for operation in (lambda: mutation.read_bytes(self.root, "record"),
                          lambda: mutation.publish_bytes(self.root, "new", b"new bytes"),
                          lambda: mutation.open_append_file(self.root, "record")):
            with self.subTest(operation=operation), patch.object(mutation.os, "fdopen",
                    side_effect=OSError("controlled descriptor conversion failure")):
                with self.assertRaises(OSError):
                    operation()
            self.assertEqual(b"retained bytes", target.read_bytes())
            self.assertEqual(["record"], sorted(item.name for item in self.root.iterdir()))
        target.unlink()  # An unclosed native reader/writer would block deletion on Windows.

    def test_ancestor_relocation_after_anchor_cannot_redirect_a_publication(self):
        container = self.root / "container"
        admitted = container / "admitted"
        admitted.mkdir(parents=True)
        target = admitted / "target"
        target.write_bytes(b"authorized")
        def substitute():
            container.rename(self.root / "retained-container")
            admitted.mkdir(parents=True)
            target.write_bytes(b"new owner")
        with patch.object(mutation, "_after_anchor", side_effect=substitute):
            with self.assertRaises(OSError):
                mutation.replace(admitted, "target", b"forbidden")
        retained = self.root / "retained-container/admitted/target"
        self.assertEqual(b"authorized", (retained if retained.exists() else target).read_bytes())
        if retained.exists():
            self.assertEqual(b"new owner", target.read_bytes())

    def test_target_replacement_after_authorization_is_not_overwritten(self):
        (self.root / "target").write_bytes(b"authorized bytes")
        captured = mutation.capture_target(self.root, "target")
        replacement = self.root / "replacement"
        replacement.write_bytes(b"foreign publication")
        def substitute():
            os.replace(replacement, self.root / "target")
        with patch.object(mutation, "_after_target_proof", side_effect=substitute):
            with self.assertRaises(OSError):
                mutation.replace(self.root, "target", b"forbidden", expected=captured)
        self.assertEqual(b"foreign publication", (self.root / "target").read_bytes())
        self.assertEqual(["target"], sorted(item.name for item in self.root.iterdir()))

    def test_rename_refuses_a_destination_that_appears_after_proof(self):
        (self.root / "source").write_bytes(b"source")
        with patch.object(mutation, "_after_target_proof",
                          side_effect=lambda: (self.root / "target").write_bytes(b"new owner")):
            with self.assertRaises(OSError):
                mutation.rename(self.root, "source", "target")
        self.assertEqual(b"new owner", (self.root / "target").read_bytes())
        self.assertEqual(b"source", (self.root / "source").read_bytes())

    def test_delete_rejects_changed_content_and_target_substitution(self):
        target = self.root / "target"
        target.write_bytes(b"authorized")
        state = mutation.capture_target(self.root, "target")
        target.write_bytes(b"changed in place")
        with self.assertRaises(OSError):
            mutation.delete(self.root, "target", expected=state)
        self.assertEqual(b"changed in place", target.read_bytes())
        replacement = self.root / "replacement"
        replacement.write_bytes(b"foreign bytes")
        with patch.object(mutation, "_after_target_proof", side_effect=lambda: os.replace(replacement, target)):
            with self.assertRaises(OSError):
                mutation.delete(self.root, "target")
        self.assertEqual(b"foreign bytes", target.read_bytes())

    def test_hardlinks_are_rejected_by_all_authority_operations(self):
        shared = self.protected / "sentinel"
        os.link(shared, self.root / "shared")
        operations = (
            lambda: mutation.read_bytes(self.root, "shared"),
            lambda: mutation.capture_target(self.root, "shared"),
            lambda: mutation.publish_bytes(self.root, "shared", b"forbidden"),
            lambda: mutation.replace(self.root, "shared", b"forbidden"),
            lambda: mutation.append_bytes(self.root, "shared", b"forbidden"),
            lambda: mutation.open_append_file(self.root, "shared"),
            lambda: mutation.delete(self.root, "shared"),
            lambda: mutation.rename(self.root, "shared", "other"),
            lambda: records.atomic_write_bytes(self.root / "shared", b"forbidden"),
        )
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaises(OSError):
                operation()
            self.assertEqual(b"retain protected bytes", shared.read_bytes())
        self.assertFalse((self.root / "other").exists())

    def test_traversal_alias_and_device_spellings_have_no_effects(self):
        invalid = ("../sentinel", "a/../sentinel", "a//sentinel", "a/./sentinel", "a/",
                   "a\\\\sentinel", "NUL", "target.", "target ", "target:stream",
                   "C:relative", "C:/outside", "/outside", "\\outside", "bad?name")
        for relative in invalid:
            with self.subTest(relative=relative), self.assertRaises(OSError):
                mutation.publish_bytes(self.root, relative, b"forbidden")
        for prefix in ("../", "escape/", "C:", "bad?"):
            with self.subTest(prefix=prefix), self.assertRaises(OSError):
                mutation.make_temporary_directory(self.root, prefix=prefix)
        self.assertEqual([], list(self.root.iterdir()))

    def test_case_aliases_and_unsafe_existing_directory_ancestry_are_rejected(self):
        (self.root / "Source").write_bytes(b"exact spelling")
        with self.assertRaises(OSError):
            mutation.read_bytes(self.root, "source")
        self.directory_link(self.fixture / "alias", self.root)
        with self.assertRaises(OSError):
            mutation.ensure_directory_path(self.fixture / "alias")
        with self.assertRaises(OSError):
            mutation.read_bytes(self.fixture / "alias", "Source")
        self.assertEqual(b"exact spelling", (self.root / "Source").read_bytes())

    def test_every_operation_refuses_linked_parent_and_leaves_referent_intact(self):
        self.directory_link(self.root / "link", self.protected)
        operations = (
            lambda: mutation.read_bytes(self.root, "link/sentinel"),
            lambda: mutation.publish_bytes(self.root, "link/sentinel", b"forbidden"),
            lambda: mutation.append_bytes(self.root, "link/sentinel", b"forbidden"),
            lambda: mutation.delete(self.root, "link/sentinel"),
            lambda: mutation.rename(self.root, "link/sentinel", "sentinel"),
            lambda: mutation.ensure_directory_path(self.root / "link/new"),
        )
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaises(OSError):
                operation()
        self.assertEqual(["sentinel"], [item.name for item in self.protected.iterdir()])
        self.assertEqual(b"retain protected bytes", (self.protected / "sentinel").read_bytes())

    def test_root_substitution_before_acquisition_is_rejected(self):
        (self.root / "target").write_bytes(b"original")
        def substitute():
            self.root.rename(self.fixture / "retained")
            self.root.mkdir()
            (self.root / "target").write_bytes(b"new owner")
        with patch.object(mutation, "_before_commit", side_effect=substitute):
            with self.assertRaises(OSError):
                mutation.replace(self.root, "target", b"forbidden")
        self.assertEqual(b"new owner", (self.root / "target").read_bytes())
        self.assertEqual(b"original", (self.fixture / "retained/target").read_bytes())

    def test_root_or_parent_substitution_after_anchor_cannot_redirect_a_write(self):
        for part in ("root", "parent"):
            with self.subTest(component=part):
                directory = self.root / part
                directory.mkdir()
                parent = directory / "parent"
                parent.mkdir()
                target = parent / "target"
                target.write_bytes(b"original")
                captured = mutation.capture_target(directory, "parent/target")
                displaced = directory if part == "root" else parent
                retained = self.fixture / (part + "-retained")
                def substitute():
                    displaced.rename(retained)
                    displaced.mkdir()
                    if part == "root":
                        (displaced / "parent").mkdir()
                    target.write_bytes(b"new owner")
                with patch.object(mutation, "_after_anchor", side_effect=substitute):
                    with self.assertRaises(OSError):
                        mutation.replace(directory, "parent/target", b"forbidden", expected=captured)
                original = retained / ("parent/target" if part == "root" else "target")
                self.assertEqual(b"original", (original if retained.exists() else target).read_bytes())
                if retained.exists():
                    self.assertEqual(b"new owner", target.read_bytes())

    def test_recursive_preflight_rejects_late_hardlink_before_deleting_earlier_files(self):
        tree = self.root / "tree"
        (tree / "nested").mkdir(parents=True)
        (tree / "a-first").write_bytes(b"retain early entry")
        os.link(self.protected / "sentinel", tree / "nested/z-shared")
        with self.assertRaises(OSError):
            mutation.remove_tree(self.root, "tree")
        self.assertEqual(b"retain early entry", (tree / "a-first").read_bytes())
        self.assertEqual(b"retain protected bytes", (self.protected / "sentinel").read_bytes())

    def test_recursive_preflight_rejects_link_and_tree_substitution(self):
        tree = self.root / "tree"
        tree.mkdir()
        (tree / "a-first").write_bytes(b"retain")
        self.directory_link(tree / "z-link", self.protected)
        with self.assertRaises(OSError):
            mutation.remove_tree(self.root, "tree")
        self.assertEqual(b"retain", (tree / "a-first").read_bytes())
        (tree / "z-link").rmdir() if os.name == "nt" else (tree / "z-link").unlink()
        def substitute():
            tree.rename(self.root / "retained-tree")
            tree.mkdir()
            (tree / "foreign").write_bytes(b"new owner")
        with patch.object(mutation, "_after_target_proof", side_effect=substitute):
            with self.assertRaises(OSError):
                mutation.remove_tree(self.root, "tree")
        self.assertEqual(b"retain", (self.root / "retained-tree/a-first").read_bytes())
        self.assertEqual(b"new owner", (tree / "foreign").read_bytes())

    def test_link_value_is_exact_dangling_and_never_an_authority_read(self):
        link = self.root / "link"
        self.symbolic_link(link, "../missing/opaque-target")
        identity = self.root.stat()
        self.assertEqual(b"../missing/opaque-target", mutation.read_link_value(self.root, "link",
                         expected_root_identity=(identity.st_dev, identity.st_ino)))
        with self.assertRaises(OSError):
            mutation.read_bytes(self.root, "link")
        with self.assertRaises(OSError):
            records.read_record(link, "example/v1")
        with self.assertRaises(OSError):
            mutation.read_link_value(self.root, "link", expected_root_identity=(0, 0))
        self.assertFalse((self.fixture / "missing").exists())

    def test_link_value_refuses_linked_ancestry_and_replacement(self):
        self.symbolic_link(self.root / "link", "../protected/sentinel")
        self.directory_link(self.fixture / "alias", self.root)
        with self.assertRaises(OSError):
            mutation.read_link_value(self.fixture / "alias", "link")
        replacement = self.root / "replacement"
        self.symbolic_link(replacement, "different-value")
        with patch.object(mutation, "_after_target_proof",
                          side_effect=lambda: os.replace(replacement, self.root / "link")):
            with self.assertRaises(OSError):
                mutation.read_link_value(self.root, "link")
        self.assertEqual(b"retain protected bytes", (self.protected / "sentinel").read_bytes())

    def test_plain_record_io_and_stable_tail_share_the_no_follow_boundary(self):
        path = self.root / "nested/record.json"
        records.write_record(path, "example/v1", {"value": 1})
        self.assertEqual(1, records.read_record(path, "example/v1")["value"])
        self.assertEqual(1, records.read_json(path)["value"])
        records.update_record(path, "example/v1", lambda old: {**old, "value": 2})
        self.assertEqual(2, records.read_record(path, "example/v1")["value"])
        events = self.root / "events"
        records.append_jsonl(events, {"n": 1}, header={"schema": "events/v1"})
        records.append_jsonl(events, {"n": 2}, header={"schema": "events/v1"})
        self.assertEqual([{"schema": "events/v1"}, {"n": 1}, {"n": 2}], records.read_jsonl(events))
        full = read_stable(events, max_bytes=200, retries=2, delay_seconds=0)
        tail = read_tail_stable(events, max_bytes=5, retries=2, delay_seconds=0)
        self.assertEqual(events.read_bytes(), full.data)
        self.assertEqual(full.data[-5:], tail.data)
        self.assertEqual(full.size, tail.size)
        self.assertEqual(b"", read_tail_stable(events, max_bytes=0, retries=1, delay_seconds=0).data)
        records.remove_record(path)
        records.remove_record(path)
        with self.assertRaises(FileNotFoundError):
            records.read_record(self.root / "absent/record", "example/v1")
        self.assertEqual([], records.read_jsonl(self.root / "absent/log"))
        self.directory_link(self.root / "alias", self.root / "nested")
        with self.assertRaises(OSError):
            records.read_json(self.root / "alias/record.json")
        for reader in (read_stable, read_tail_stable):
            with self.assertRaises(OSError):
                reader(self.root / "alias/record.json", max_bytes=100, retries=1, delay_seconds=0)

    def test_record_reader_preserves_utf8_object_and_schema_requirements(self):
        path = self.root / "record"
        for data in (b"[]", b"{}", b"\xef\xbb\xbf" + b'{"schema":"example/v1"}',
                     '{"schema":"example/v1"}'.encode("utf-16")):
            with self.subTest(data=data):
                path.write_bytes(data)
                with self.assertRaises(ValueError):
                    records.read_record(path, "example/v1")

    def test_prepared_output_rechecks_original_root_for_every_surface(self):
        output = PreparedOutputTransaction(self.root, allowed_roots=(self.fixture,))
        output.prepare()
        path = output.admit("events")
        output.append_jsonl(path, [{"n": 1}])
        output.atomic_json(output.admit("record"), {"schema": "example/v1"})
        self.root.rename(self.fixture / "retained")
        self.root.mkdir()
        for operation in (lambda: output.read_bytes(path), lambda: output.append_jsonl(path, [{"n": 2}]),
                          lambda: output.ensure_parent(self.root / "new/child"),
                          lambda: output.publish_json(path, {}), lambda: output.delete(path)):
            with self.subTest(operation=operation), self.assertRaises(OSError):
                operation()
        self.assertEqual([], list(self.root.iterdir()))

    def test_installed_module_wheel_executes_native_primitives(self):
        if importlib.util.find_spec("setuptools") is None or importlib.util.find_spec("wheel") is None:
            self.skipTest("owned environment needs the declared setuptools/wheel build inputs")
        repository = Path(__file__).resolve().parents[2]
        source, wheels, installed = (self.fixture / name for name in ("src", "whl", "installed"))
        ignored = shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info", "build", "dist")
        for directory in ("orchestrator_harness", "harness_common", "examples", "scripts", "tools", "docs"):
            shutil.copytree(repository / directory, source / directory, ignore=ignored)
        wheels.mkdir()
        command = [sys.executable, "-B", "-c", "from setuptools.build_meta import build_wheel; "
                   "import sys; build_wheel(sys.argv[1])", str(wheels)]
        build = subprocess.run(command, cwd=source / "orchestrator_harness", capture_output=True, text=True, timeout=60)
        self.assertEqual(0, build.returncode, build.stdout + build.stderr)
        archives = list(wheels.glob("*.whl"))
        self.assertEqual(1, len(archives))
        install = subprocess.run([sys.executable, "-B", "-m", "pip", "install", "--no-index", "--no-deps",
                                  "--target", str(installed), str(archives[0])],
                                 capture_output=True, text=True, timeout=60)
        self.assertEqual(0, install.returncode, install.stdout + install.stderr)
        program = '''import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from orchestrator_harness import mutation, records
import harness_common
installed, root = Path(sys.argv[1]), Path(sys.argv[2])
assert Path(mutation.__file__).is_relative_to(installed)
assert Path(records.__file__).is_relative_to(installed)
assert Path(harness_common.__file__).is_relative_to(installed)
mutation.ensure_directory_path(root / 'a')
mutation.ensure_directory_path(root / 'b')
mutation.publish_bytes(root, 'a/source', b'first')
authorized = mutation.capture_target(root, 'a/source')
updated = mutation.replace(root, 'a/source', b'second', expected=authorized)
assert updated.resulting.content == b'second'
if mutation.os.name == 'nt': assert updated.resulting.identity == authorized.identity
mutation.append_bytes(root, 'a/source', b' appended')
receipt = mutation.rename(root, 'a/source', 'b/target')
assert receipt.resulting.content == b'second appended'
mutation.publish_bytes(root, 'a/next', b'next complete source')
receipt = mutation.rename(root, 'a/next', 'b/target',
    expected_source=mutation.capture_target(root, 'a/next'),
    expected_target=mutation.capture_target(root, 'b/target'))
assert receipt.resulting.content == b'next complete source'
assert not list((root / 'b').glob('.rename.*.retained'))
records.write_record(root / 'record', 'example/v1', {'value': 1})
assert records.read_record(root / 'record', 'example/v1')['value'] == 1
with records.RecordLock(root / 'record', root=root): pass
records.append_jsonl(root / 'events', {'n': 1})
assert records.read_jsonl(root / 'events') == [{'n': 1}]
mutation.delete(root, 'b/target')
mutation.remove_tree(root, 'a')
mutation.remove_tree(root, 'b')
print(json.dumps({'installed_import': True, 'native_primitives': True}))
'''
        result = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(installed),
                                 str(self.fixture / "installed-fixture")], cwd=self.fixture,
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual({"installed_import": True, "native_primitives": True}, json.loads(result.stdout))


class PlatformIdentityTests(unittest.TestCase):
    def test_native_identity_and_invalid_pid_observations(self):
        identity = process_identity.exact_process_identity(os.getpid())
        self.assertIsNotNone(identity)
        self.assertEqual(os.getpid(), identity["pid"])
        self.assertTrue(identity["created_utc"])
        for invalid in (True, False, 0, -1, "1", 1.0, 1 << 64):
            self.assertIsNone(process_identity.exact_process_identity(invalid))

    def test_mount_timeout_and_malformed_output_are_unknown(self):
        platform_paths._mount_candidates.cache_clear()
        self.addCleanup(platform_paths._mount_candidates.cache_clear)
        with patch("shutil.which", return_value="findmnt"), patch.object(platform_paths.subprocess, "run",
            side_effect=subprocess.TimeoutExpired("findmnt", 5)):
            with self.assertRaises(RuntimeError):
                platform_paths._mount_candidates("proc", None)
            self.assertIsNone(process_identity._linux_process_identity(123))
        for output in ('{"filesystems":null}', '{"filesystems":[null]}', '[]', '{}'):
            with self.subTest(output=output), patch("shutil.which", return_value="findmnt"), patch.object(
                platform_paths.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, output, "")):
                with self.assertRaises(RuntimeError):
                    platform_paths._mount_candidates("proc", None)

    def test_linux_identity_rejects_mismatched_and_malformed_native_pid(self):
        with tempfile.TemporaryDirectory(prefix="i") as raw:
            root = Path(raw)
            (root / "123").mkdir()
            path = root / "123/stat"
            fields = ["S", *(["0"] * 18), "500"]
            for value in ("124 (other) " + " ".join(fields), "123 missing " + " ".join(fields)):
                path.write_text(value)
                with patch.object(process_identity, "procfs_root", return_value=root):
                    self.assertIsNone(process_identity._linux_process_identity(123))
            path.write_text("123 (name with ) and spaces) " + " ".join(fields))
            with patch.object(process_identity, "procfs_root", return_value=root):
                self.assertEqual({"pid": 123, "created_utc": "linux-start-ticks:500"},
                                 process_identity._linux_process_identity(123))

    def test_saturated_darwin_inventory_is_unknown(self):
        native = Mock()
        native.proc_listpids.side_effect = lambda kind, typeinfo, buffer, size: 4 if buffer is None else size
        with patch("ctypes.util.find_library", return_value="fixture-libproc"), patch("ctypes.CDLL", return_value=native):
            self.assertIsNone(process_identity.darwin_process_ids())
        self.assertEqual(4, native.proc_listpids.call_count)


if __name__ == "__main__":
    unittest.main()
