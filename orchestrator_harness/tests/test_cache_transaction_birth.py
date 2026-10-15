"""Native identity, journal birth and captured cache publication regressions."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
from contextlib import contextmanager
from copy import deepcopy
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import mutation, setup
from orchestrator_harness.tests.test_v2_materialization import MaterializationFixture


class DurableNativeIdentityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="durable-native-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).absolute()
        self.path = self.root / "file"
        self.path.write_bytes(b"original")
        self.state = mutation.capture_target(self.root, "file")

    def root_state(self, path=None):
        info = (path or self.root).stat()
        return mutation.TargetState(True, "directory", (info.st_dev, info.st_ino), None, None)

    def test_identity_identifies_original_file_and_authority(self):
        file_id = mutation.durable_identity(self.root, "file", expected=self.state)
        root_id = mutation.durable_identity(self.root, ".", expected=self.root_state())
        self.assertNotEqual(file_id, root_id)
        self.assertEqual(file_id, mutation.durable_identity(self.root, "file", expected=self.state))
        parent_id = mutation.durable_identity(self.root.parent, ".", expected=self.root_state(self.root.parent))
        self.assertNotEqual(root_id, parent_id)

    def test_identical_replacement_hardlink_and_changed_bytes_are_refused(self):
        self.path.rename(self.root / "retained")
        self.path.write_bytes(b"original")
        with self.assertRaises(mutation.MutationConflict):
            mutation.durable_identity(self.root, "file", expected=self.state)
        current = mutation.capture_target(self.root, "file")
        os.link(self.path, self.root / "alias")
        with self.assertRaises(mutation.MutationConflict):
            mutation.durable_identity(self.root, "file", expected=current)
        (self.root / "alias").unlink()
        self.path.write_bytes(b"changed")
        with self.assertRaises(mutation.MutationConflict):
            mutation.durable_identity(self.root, "file", expected=current)

    def test_explicit_authority_and_safe_relative_grammar(self):
        for relative in ("", "../file", "file:stream"):
            with self.subTest(relative=relative):
                with self.assertRaises(mutation.MutationError):
                    mutation.durable_identity(self.root, relative, expected=self.state)
        with self.assertRaises(mutation.MutationConflict):
            mutation.durable_identity(self.root, ".", expected=self.state)

    @unittest.skipUnless(os.name == "nt", "native Windows cross-Python FILE_ID_INFO")
    def test_python_311_and_available_other_runtime_agree_on_native_ids(self):
        other = shutil.which("python")
        self.assertIsNotNone(other, "configured native Python runtime must be available")
        source = Path(__file__).resolve().parents[2]
        code = (
            "import json,sys;from pathlib import Path;"
            "sys.path.insert(0,sys.argv[2]);from orchestrator_harness import mutation as m;"
            "p=Path(sys.argv[1]);i=p.stat();s=m.TargetState(True,'directory',(i.st_dev,i.st_ino),None,None);"
            "print(json.dumps({'version':list(sys.version_info[:3]),'root':m.durable_identity(p,'.',expected=s),"
            "'file':m.durable_identity(p,'file',expected=m.capture_target(p,'file'))}))"
        )
        result = subprocess.run([other, "-B", "-c", code, str(self.root), str(source)],
                                capture_output=True, text=True, timeout=30, check=True)
        observed = json.loads(result.stdout)
        if tuple(observed["version"][:2]) == tuple(sys.version_info[:2]):
            self.skipTest("no distinct supported native Python peer is available")
        self.assertEqual(mutation.durable_identity(self.root, ".", expected=self.root_state()), observed["root"])
        self.assertEqual(mutation.durable_identity(self.root, "file", expected=self.state), observed["file"])
        self.assertGreaterEqual(tuple(observed["version"][:2]), (3, 11))
        print(json.dumps({"native_identity_runtime_peer": observed["version"],
                          "current_runtime": list(sys.version_info[:3]), "canonical_ids_match": True}))

    def test_append_carries_original_leaf_and_complete_directory_proofs(self):
        chain = ((Path("."), self.root_state()),)
        first = mutation.append_bytes(self.root, "file", b" suffix", expected=self.state,
                                      expected_root_identity=chain[0][1].identity, expected_directories=chain)
        self.assertEqual(b"original suffix", first.content)
        self.path.rename(self.root / "retained")
        self.path.write_bytes(first.content)
        with self.assertRaises(mutation.MutationConflict):
            mutation.append_bytes(self.root, "file", b" foreign overwrite", expected=first,
                                  expected_root_identity=chain[0][1].identity, expected_directories=chain)
        self.assertEqual(b"original suffix", self.path.read_bytes())

    def test_append_rejects_same_inode_changed_bytes_before_output(self):
        self.path.write_bytes(b"changed")
        with self.assertRaises(mutation.MutationConflict):
            mutation.append_bytes(self.root, "file", b" suffix", expected=self.state)
        self.assertEqual(b"changed", self.path.read_bytes())


    def test_rename_uses_original_authority_proof_before_output(self):
        (self.root / "other").mkdir()
        other = mutation.capture_target(self.root, "other")
        with self.assertRaises(mutation.MutationConflict):
            mutation.rename(self.root, "file", "moved", expected_source=self.state,
                            expected_target=mutation.TargetState.absent(), expected_root_identity=other.identity)
        self.assertEqual(b"original", self.path.read_bytes())
        self.assertFalse((self.root / "moved").exists())
        moved = mutation.rename(self.root, "file", "moved", expected_source=self.state,
                                expected_target=mutation.TargetState.absent(),
                                expected_root_identity=self.root_state().identity)
        self.assertEqual(self.state.identity, moved.resulting.identity)
        self.assertEqual(b"original", (self.root / "moved").read_bytes())

    @unittest.skipUnless(os.name == "nt", "native Windows NO_REPLACE rename")
    def test_native_destination_collision_is_not_retried(self):
        real = mutation._NtSetInformationFile
        destination = self.root / "moved"
        def collision(*args):
            destination.write_bytes(b"foreign")
            return real(*args)
        with patch.object(mutation, "_NtSetInformationFile", side_effect=collision) as calls, \
                patch.object(setup.time, "sleep") as sleep:
            with self.assertRaises(mutation.MutationConflict):
                mutation.rename(self.root, "file", "moved", expected_source=self.state,
                                expected_target=mutation.TargetState.absent(),
                                expected_root_identity=self.root_state().identity)
        self.assertEqual(1, calls.call_count)
        sleep.assert_not_called()
        self.assertEqual(b"foreign", destination.read_bytes())
        self.assertEqual(b"original", self.path.read_bytes())


class CacheJournalDecoderTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="cache-journal-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        info = self.root.stat()
        root_state = mutation.TargetState(True, "directory", (info.st_dev, info.st_ino), None, None)
        stage = self.root / (".super-cache." + "b" * 32)
        stage.mkdir()
        (stage / "owned").write_bytes(b"owned")
        self.record = {
            "schema": setup.CACHE_TRANSACTION_SCHEMA, "runtime_root": str(self.root),
            "runtime_identity": mutation.durable_identity(self.root, ".", expected=root_state),
            "config_identity": hashlib.sha256(b"parser fixture configuration").hexdigest(),
            "transaction_id": "b" * 32, "stage_name": stage.name,
            "previous": None, "target": setup._manifest_for_tree(stage),
            "objects": [setup._cache_tree_proof(setup.capture_tree(self.root, stage.name), "target")],
            "state": "building", "error": None,
        }

    def test_torn_utf8_final_fragment_keeps_complete_native_snapshot(self):
        data = setup._journal_snapshot_bytes(self.record) + b'{"error":"\xf0\x9f'
        self.assertEqual(self.record, setup._decode_cache_transaction(data, self.root))

    def test_corrupt_complete_snapshot_is_never_ignored(self):
        for tail in (b'{"error":"\xff"}\n', b'{"schema":\n', b'[]\n',
                     b'broken\r' + setup._journal_snapshot_bytes(self.record)):
            with self.subTest(tail=tail):
                with self.assertRaises(setup.SetupError) as raised:
                    setup._decode_cache_transaction(setup._journal_snapshot_bytes(self.record) + tail, self.root)
                self.assertEqual(setup.SETUP_CACHE_INVALID, raised.exception.code)

    def test_append_history_cannot_drop_native_births_or_change_header(self):
        for mode in ("objects", "entry", "config", "native"):
            with self.subTest(mode=mode):
                changed = deepcopy(self.record)
                if mode == "objects":
                    changed["objects"] = []
                elif mode == "entry":
                    del changed["objects"][0]["entries"]["owned"]
                elif mode == "config":
                    changed["config_identity"] = hashlib.sha256(b"other configuration").hexdigest()
                else:
                    changed["objects"][0]["entries"]["owned"]["identity"] = self.record["runtime_identity"]
                with self.assertRaises(setup.SetupError):
                    setup._decode_cache_transaction(setup._journal_snapshot_bytes(self.record) +
                                                    setup._journal_snapshot_bytes(changed), self.root)

    def test_malformed_closed_native_fields_have_named_refusal(self):
        for field, value in (("objects", [{}]), ("state", []), ("runtime_identity", []),
                             ("config_identity", None), ("stage_name", [])):
            with self.subTest(field=field):
                record = deepcopy(self.record)
                record[field] = value
                with self.assertRaises(setup.SetupError) as raised:
                    setup._decode_cache_transaction(setup._journal_snapshot_bytes(record), self.root)
                self.assertEqual(setup.SETUP_CACHE_INVALID, raised.exception.code)


class CacheTransactionBirthTests(unittest.TestCase):
    def setUp(self):
        self.fixture = MaterializationFixture()
        self.addCleanup(self.fixture.close)
        self.harness = self.fixture.harness
        self.runtime = self.fixture.root_workspace / ".harness-runtime"
        self.cache = self.runtime / "super-cache"

    def install(self, **kwargs):
        catalog = kwargs.pop("catalog", None)
        plan = setup._plan_active_cache(self.harness, catalog=catalog)
        setup._install_active_cache(self.harness, self.runtime, plan=plan, overwrite=kwargs.pop("overwrite", False),
                                     catalog=catalog, **kwargs)

    def test_new_cache_uses_captured_bytes_and_clears_v2_journal(self):
        self.install()
        self.assertFalse(setup._transaction_path(self.runtime).exists())
        setup._validate_active_cache(setup._plan_active_cache(self.harness), self.cache)
        before = setup._manifest_for_tree(self.cache)
        self.install()
        self.assertEqual(before, setup._manifest_for_tree(self.cache))

    def test_source_replacement_after_catalog_capture_is_not_adopted(self):
        catalog = setup._capture_cache_catalog(self.harness)
        source = self.harness / "super-cache/custom/README.md"
        source.rename(source.with_name("retained"))
        source.write_bytes(b"custom marker\n")
        with self.assertRaises(setup.SetupError):
            self.install(catalog=catalog)
        self.assertFalse(self.cache.exists())
        self.assertEqual(b"custom marker\n", source.read_bytes())

    def test_catalog_validation_uses_original_bytes_and_rejects_changed_source(self):
        catalog = setup._capture_cache_catalog(self.harness)
        source = self.harness / "super-cache/workspace/.agent-workspace/lane-queue.py"
        original = source.read_bytes()
        source.write_bytes(b"")
        plan = setup._plan_active_cache(self.harness, catalog=catalog)
        self.assertTrue(plan)
        with self.assertRaises(setup.SetupError):
            setup._install_active_cache(self.harness, self.runtime, plan=plan, overwrite=False, catalog=catalog)
        self.assertFalse(self.cache.exists())
        self.assertEqual(b"", source.read_bytes())
        self.assertTrue(original)

    def journal_fixture(self):
        # Genuine fixture-created native trees are captured before any fault.
        self.fixture.active_cache()
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.cache, stage)
        root_state = mutation.TargetState(True, "directory",
                                         (self.runtime.stat().st_dev, self.runtime.stat().st_ino), None, None)
        record = {"schema": setup.CACHE_TRANSACTION_SCHEMA, "runtime_root": str(self.runtime.absolute()),
                  "runtime_identity": mutation.durable_identity(self.runtime, ".", expected=root_state),
                  "config_identity": setup._cache_config_identity(setup._capture_cache_catalog(self.harness)),
                  "transaction_id": "a" * 32, "stage_name": stage.name,
                  "previous": setup._manifest_for_tree(self.cache), "target": setup._manifest_for_tree(self.cache),
                  "objects": [setup._cache_tree_proof(setup.capture_tree(self.runtime, self.cache.name), "previous"),
                              setup._cache_tree_proof(setup.capture_tree(self.runtime, stage.name), "target")],
                  "state": "building", "error": None}
        state = setup._publish_cache_journal(self.runtime, record, mutation.TargetState.absent(), root_state)
        return stage, record, state, root_state

    def test_byte_identical_unknown_stage_root_is_preserved(self):
        stage, record, _, _ = self.journal_fixture()
        stage.rename(self.runtime / "retained-stage")
        shutil.copytree(self.runtime / "retained-stage", stage)
        before = setup._manifest_for_tree(stage)
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(before, setup._manifest_for_tree(stage))
        self.assertTrue(setup._transaction_path(self.runtime).exists())

    def test_byte_identical_unknown_stage_leaf_is_preserved(self):
        stage, _, _, _ = self.journal_fixture()
        leaf = stage / "custom/README.md"
        original = leaf.read_bytes()
        leaf.rename(self.runtime / "retained-leaf")
        leaf.write_bytes(original)
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(original, leaf.read_bytes())
        self.assertTrue(stage.is_dir())

    def test_truncated_last_append_keeps_complete_native_ownership_snapshot(self):
        stage, record, state, _ = self.journal_fixture()
        with setup._transaction_path(self.runtime).open("ab") as journal:
            journal.write(b'{"schema":"incomplete')
        self.assertEqual(record, setup._read_cache_transaction(self.runtime))
        self.install()
        self.assertFalse(stage.exists())
        self.assertFalse(setup._transaction_path(self.runtime).exists())

    def test_changed_journal_after_boundary_capture_is_not_deleted(self):
        stage, record, _, _ = self.journal_fixture()
        boundary = setup._capture_cache_boundary(self.runtime)
        path = setup._transaction_path(self.runtime)
        original = path.read_bytes()
        path.rename(self.runtime / "retained-journal")
        path.write_bytes(original)
        with self.assertRaises(mutation.MutationError):
            setup._recover_cache_transaction(self.runtime, record, captured=boundary)
        self.assertTrue(stage.exists())
        self.assertEqual(original, path.read_bytes())

    def test_legacy_byte_exact_stage_cannot_be_adopted_as_owned(self):
        self.install()
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.cache, stage)
        record = {"schema": setup.LEGACY_CACHE_TRANSACTION_SCHEMA, "runtime_root": str(self.runtime.absolute()),
                  "transaction_id": "a" * 32, "stage_name": stage.name,
                  "previous": setup._manifest_for_tree(self.cache), "target": setup._manifest_for_tree(self.cache)}
        setup._transaction_path(self.runtime).write_bytes(setup._journal_snapshot_bytes(record))
        before = setup._manifest_for_tree(stage)
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(before, setup._manifest_for_tree(stage))
        self.assertTrue(setup._transaction_path(self.runtime).exists())


    def test_relative_harness_catalog_uses_same_original_sources(self):
        relative = Path(os.path.relpath(self.harness, Path.cwd()))
        catalog = setup._capture_cache_catalog(relative)
        self.assertEqual(self.harness, catalog.harness)
        self.assertEqual(setup._plan_active_cache(self.harness, catalog=catalog),
                         setup._plan_active_cache(relative, catalog=catalog))
        self.assertEqual(setup._plan_root_payloads(self.harness, catalog=catalog),
                         setup._plan_root_payloads(relative, catalog=catalog))
        self.assertFalse(self.runtime.exists())

    def test_missing_or_misbound_root_source_map_refuses_before_output(self):
        catalog = setup._capture_cache_catalog(self.harness)
        plan = setup._plan_root_payloads(self.harness, catalog=catalog)
        sources = {source.path: source for source in catalog.sources}
        for mode in ("missing", "misbound"):
            with self.subTest(mode=mode):
                invalid = dict(sources)
                if mode == "missing":
                    invalid.pop(plan[0][0])
                else:
                    invalid[plan[0][0]] = sources[plan[1][0]]
                with self.assertRaises(setup.SetupError) as raised:
                    setup._capture_root_payloads(plan, self.fixture.root_workspace, sources=invalid)
                self.assertEqual(setup.SETUP_ADAPTER_COLLISION, raised.exception.code)
        self.assertEqual([], list(self.fixture.root_workspace.iterdir()))

    def promoted_fixture(self):
        stage, record, journal, root_state = self.journal_fixture()
        self.cache.rename(self.runtime / "super-cache.old")
        stage.rename(self.cache)
        return record, journal, root_state

    def test_identical_foreign_backup_root_is_preserved(self):
        self.promoted_fixture()
        backup = self.runtime / "super-cache.old"
        backup.rename(self.runtime / "retained-backup")
        shutil.copytree(self.runtime / "retained-backup", backup)
        before = setup._manifest_for_tree(backup)
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(before, setup._manifest_for_tree(backup))
        self.assertTrue(setup._transaction_path(self.runtime).exists())

    def test_identical_foreign_backup_leaf_is_preserved(self):
        self.promoted_fixture()
        backup = self.runtime / "super-cache.old"
        leaf = backup / "custom/README.md"
        original = leaf.read_bytes()
        leaf.rename(self.runtime / "retained-backup-leaf")
        leaf.write_bytes(original)
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(original, leaf.read_bytes())
        self.assertTrue(backup.exists())

    def test_prior_backup_changed_bytes_are_preserved(self):
        self.promoted_fixture()
        backup = self.runtime / "super-cache.old"
        leaf = backup / "custom/README.md"
        leaf.write_bytes(b"changed prior")
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(b"changed prior", leaf.read_bytes())
        self.assertTrue(setup._transaction_path(self.runtime).exists())

    def test_failed_rollback_is_not_repeated_until_explicit_setup(self):
        self.fixture.active_cache()
        previous = setup._manifest_for_tree(self.cache)
        (self.harness / "super-cache/custom/README.md").write_bytes(b"new catalog\n")
        real = setup.rename
        counts = {"promotion": 0, "rollback": 0}
        def fail_once(root, source, target, **kwargs):
            if Path(target) == Path("super-cache"):
                key = "rollback" if Path(source) == Path("super-cache.old") else "promotion"
                counts[key] += 1
                if counts[key] == 1:
                    raise OSError("first " + key + " failure")
            return real(root, source, target, **kwargs)
        with patch.object(setup, "rename", side_effect=fail_once):
            with self.assertRaises(setup.SetupError):
                self.install(overwrite=True)
        self.assertEqual({"promotion": 1, "rollback": 1}, counts)
        self.assertEqual(previous, setup._manifest_for_tree(self.runtime / "super-cache.old"))
        self.assertEqual("cleanup_unproven", setup._read_cache_transaction(self.runtime)["state"])
        self.install(overwrite=True)
        self.assertFalse((self.runtime / "super-cache.old").exists())
        self.assertFalse(setup._transaction_path(self.runtime).exists())
        self.assertEqual(b"new catalog\n", (self.cache / "custom/README.md").read_bytes())

    def test_failed_backup_cleanup_is_not_repeated_until_explicit_setup(self):
        self.fixture.active_cache()
        (self.harness / "super-cache/custom/README.md").write_bytes(b"new catalog\n")
        real = setup.remove_tree
        attempts = 0
        def fail_once(root, relative, **kwargs):
            nonlocal attempts
            if Path(relative) == Path("super-cache.old"):
                attempts += 1
                if attempts == 1:
                    raise OSError("first backup cleanup failure")
            return real(root, relative, **kwargs)
        with patch.object(setup, "remove_tree", side_effect=fail_once):
            with self.assertRaises(setup.SetupError):
                self.install(overwrite=True)
        self.assertEqual(1, attempts)
        self.assertTrue((self.runtime / "super-cache.old").exists())
        self.assertEqual("cleanup_unproven", setup._read_cache_transaction(self.runtime)["state"])
        self.install()
        self.assertFalse((self.runtime / "super-cache.old").exists())
        self.assertFalse(setup._transaction_path(self.runtime).exists())
        self.assertEqual(b"new catalog\n", (self.cache / "custom/README.md").read_bytes())

    def test_creator_receipts_clean_stage_after_torn_birth_append(self):
        real = setup._publish_cache_journal
        interrupted = False
        def torn(root, record, expected, root_state):
            nonlocal interrupted
            born_file = any(entry["kind"] == "file" for obj in record["objects"] if obj["purpose"] == "target"
                            for entry in obj["entries"].values())
            if born_file and not interrupted:
                interrupted = True
                mutation.append_bytes(root, setup.CACHE_TRANSACTION_NAME, b'{"error":"\xf0\x9f',
                                      expected=expected, expected_root_identity=root_state.identity,
                                      expected_directories=((Path("."), root_state),))
                raise OSError("interrupted append after native file receipt")
            return real(root, record, expected, root_state)
        with patch.object(setup, "_publish_cache_journal", side_effect=torn):
            with self.assertRaises(setup.SetupError):
                self.install()
        self.assertTrue(interrupted)
        self.assertEqual([], list(self.runtime.glob(".super-cache.*")))
        self.assertTrue(setup._transaction_path(self.runtime).exists())
        setup._read_cache_transaction(self.runtime)
        self.install()
        self.assertTrue(self.cache.is_dir())
        self.assertFalse(setup._transaction_path(self.runtime).exists())

    def test_foreign_destination_added_before_promotion_survives(self):
        self.fixture.active_cache()
        previous = setup._manifest_for_tree(self.cache)
        (self.harness / "super-cache/custom/README.md").write_bytes(b"new catalog\n")
        real = setup.rename
        def insert(root, source, target, **kwargs):
            if Path(target) == Path("super-cache") and Path(source).name.startswith(".super-cache."):
                self.cache.mkdir()
                (self.cache / "foreign").write_bytes(b"preserve")
            return real(root, source, target, **kwargs)
        with patch.object(setup, "rename", side_effect=insert):
            with self.assertRaises(setup.SetupError):
                self.install(overwrite=True)
        self.assertEqual(b"preserve", (self.cache / "foreign").read_bytes())
        self.assertEqual(previous, setup._manifest_for_tree(self.runtime / "super-cache.old"))
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(b"preserve", (self.cache / "foreign").read_bytes())

    def test_foreign_child_added_after_promoted_validation_survives(self):
        self.fixture.active_cache()
        previous = setup._manifest_for_tree(self.cache)
        (self.harness / "super-cache/custom/README.md").write_bytes(b"new catalog\n")
        real = setup._validate_active_cache
        def insert(plan, destination, **kwargs):
            real(plan, destination, **kwargs)
            if destination == self.cache:
                (destination / "foreign").write_bytes(b"preserve")
        with patch.object(setup, "_validate_active_cache", side_effect=insert):
            with self.assertRaises(setup.SetupError):
                self.install(overwrite=True)
        self.assertEqual(b"preserve", (self.cache / "foreign").read_bytes())
        self.assertEqual(previous, setup._manifest_for_tree(self.runtime / "super-cache.old"))
        self.assertTrue(setup._transaction_path(self.runtime).exists())

    @unittest.skipUnless(os.name == "posix", "POSIX original authority rename")
    def test_move_refuses_original_stage_moved_into_replacement_runtime(self):
        stage, _, _, _ = self.journal_fixture()
        original = setup.capture_tree(self.runtime, stage.name)
        retained = self.runtime.with_name("retained-runtime")
        real = setup.rename
        def swap(root, source, target, **kwargs):
            self.runtime.rename(retained)
            self.runtime.mkdir()
            (retained / stage.name).rename(self.runtime / stage.name)
            return real(root, source, target, **kwargs)
        with patch.object(setup, "rename", side_effect=swap):
            with self.assertRaises(mutation.MutationError):
                setup._move_cache_tree(original, "super-cache.moved")
        self.assertTrue((self.runtime / stage.name).exists())
        self.assertFalse((self.runtime / "super-cache.moved").exists())
        self.assertTrue((retained / "super-cache").exists())

    @unittest.skipUnless(os.name == "posix", "POSIX original authority rename")
    def test_journal_delete_refuses_original_leaf_moved_into_replacement_runtime(self):
        _, _, journal, root_state = self.journal_fixture()
        retained = self.runtime.with_name("retained-runtime")
        real = setup.RecordLock
        @contextmanager
        def swap(*args, **kwargs):
            with real(*args, **kwargs):
                self.runtime.rename(retained)
                self.runtime.mkdir()
                (retained / setup.CACHE_TRANSACTION_NAME).rename(self.runtime / setup.CACHE_TRANSACTION_NAME)
                yield
        with patch.object(setup, "RecordLock", side_effect=swap):
            with self.assertRaises((OSError, ValueError)):
                setup._delete_cache_journal(self.runtime, journal, root_state)
        self.assertTrue(setup._transaction_path(self.runtime).exists())

    def test_native_process_death_before_file_birth_record_preserves_unknown_file(self):
        source = Path(__file__).resolve().parents[2]
        observation = self.fixture.root / "native-interruption.json"
        code = """
import json,os,sys
from pathlib import Path
from orchestrator_harness import setup,processes
h=Path(sys.argv[1]); rt=Path(sys.argv[2]); out=Path(sys.argv[3])
real=setup._publish_cache_journal
def interrupt(root,record,expected,root_state):
    born=any(entry["kind"]=="file" for obj in record["objects"] if obj["purpose"]=="target" for entry in obj["entries"].values())
    if born:
        out.write_text(json.dumps({"owner":processes.process_identity(os.getpid()),"stage_name":record["stage_name"],"created_objects":record["objects"]}),encoding="utf-8")
        os._exit(73)
    return real(root,record,expected,root_state)
setup._publish_cache_journal=interrupt
setup._install_active_cache(h,rt,plan=setup._plan_active_cache(h),overwrite=False)
raise SystemExit(99)
"""
        result = subprocess.run([sys.executable, "-B", "-c", code, str(self.harness), str(self.runtime), str(observation)],
                                cwd=source, capture_output=True, text=True, timeout=90)
        self.assertEqual(73, result.returncode, result.stderr)
        observed = json.loads(observation.read_text(encoding="utf-8"))
        self.assertIsNotNone(observed["owner"])
        stage = self.runtime / observed["stage_name"]
        before = setup._manifest_for_tree(stage)
        journal_before = setup._transaction_path(self.runtime).read_bytes()
        record = setup._read_cache_transaction(self.runtime)
        expected_entries = {name for obj in record["objects"] if obj["purpose"] == "target"
                            for name, entry in obj["entries"].items() if entry["kind"] == "file"}
        self.assertTrue(set(before["files"]) - expected_entries)
        with self.assertRaises(setup.SetupError):
            self.install(overwrite=True)
        self.assertEqual(before, setup._manifest_for_tree(stage))
        self.assertEqual(journal_before, setup._transaction_path(self.runtime).read_bytes())
        print(json.dumps({"native_interrupted_owner": observed["owner"], "exit_code": result.returncode,
                          "unrecorded_birth_preserved": True}))

    @unittest.skipUnless(os.name == "nt", "native Windows cross-Python cache recovery")
    def test_python_312_peer_recovers_python_311_native_journal(self):
        other = shutil.which("python")
        self.assertIsNotNone(other)
        version = subprocess.run([other, "-B", "-c", "import json,sys;print(json.dumps(list(sys.version_info[:3])))"],
                                 capture_output=True, text=True, check=True, timeout=30)
        peer = json.loads(version.stdout)
        if tuple(peer[:2]) == tuple(sys.version_info[:2]):
            self.skipTest("distinct supported native Python peer unavailable")
        stage, _, _, _ = self.journal_fixture()
        source = Path(__file__).resolve().parents[2]
        code = (
            "import json,sys;from pathlib import Path;from orchestrator_harness import setup;"
            "h=Path(sys.argv[1]);r=Path(sys.argv[2]);"
            "setup._install_active_cache(h,r,plan=setup._plan_active_cache(h),overwrite=False);"
            "print(json.dumps({'version':list(sys.version_info[:3]),'journal_absent':not setup._transaction_path(r).exists()}))"
        )
        result = subprocess.run([other, "-B", "-c", code, str(self.harness), str(self.runtime)], cwd=source,
                                capture_output=True, text=True, timeout=90, check=True)
        observed = json.loads(result.stdout)
        self.assertEqual(peer, observed["version"])
        self.assertTrue(observed["journal_absent"])
        self.assertFalse(stage.exists())
        self.assertTrue(self.cache.is_dir())
        print(json.dumps({"native_cache_recovery_peer": observed["version"], "journal_cleared": True}))



if __name__ == "__main__":
    unittest.main()
