"""Native source/config/binding substitutions in actual ROOT payload installation."""
from __future__ import annotations

from contextlib import ExitStack
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import mutation, setup
from orchestrator_harness.tests.test_v2_materialization import MaterializationFixture


class RootPayloadPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="root-payload-native-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).absolute()
        self.harness = self.base / "harness"
        self.source = self.harness / "catalog"
        self.workspace = self.base / "workspace"
        self.source.mkdir(parents=True)
        self.workspace.mkdir()
        self.first = self.source / "first"
        self.second = self.source / "second"
        self.first.write_bytes(b"new first")
        self.second.write_bytes(b"new second")
        self.plan = [(self.first, Path("nested/first")), (self.second, Path("second"))]

    def preflight(self, **kwargs):
        return setup._preflight_root_payloads(self.plan, self.workspace, overwrite=kwargs.pop("overwrite", False),
                                              **kwargs)

    def install(self, captured, **kwargs):
        return setup._install_root_payloads(self.harness, self.workspace, plan=self.plan,
                                             overwrite=kwargs.pop("overwrite", False), captured=captured, **kwargs)

    def no_output(self):
        self.assertFalse((self.workspace / "nested/first").exists())
        self.assertFalse((self.workspace / "second").exists())

    def test_captured_bytes_metadata_and_unrelated_files_survive_installation(self):
        self.first.chmod(0o755)
        os.utime(self.first, ns=(1700000000123456000, 1700000000123456000))
        expected = self.first.stat()
        (self.workspace / "unrelated").write_bytes(b"keep")
        publications = []
        installed, overwritten, merged = self.install(self.preflight(), publications=publications)
        target = self.workspace / "nested/first"
        self.assertEqual([target, self.workspace / "second"], installed)
        self.assertEqual(([], []), (overwritten, merged))
        self.assertEqual(b"new first", target.read_bytes())
        self.assertEqual((stat.S_IMODE(expected.st_mode), expected.st_mtime_ns),
                         (stat.S_IMODE(target.stat().st_mode), target.stat().st_mtime_ns))
        self.assertEqual(b"keep", (self.workspace / "unrelated").read_bytes())
        self.assertEqual(1, len(publications))
        self.assertEqual(mutation.capture_target(self.workspace, "nested/first"),
                         dict(publications[0].files)[Path("nested/first")])

    def test_missing_workspace_is_created_from_original_existing_authority(self):
        self.workspace = self.base / "missing/deeper"
        self.install(self.preflight())
        self.assertEqual(b"new first", (self.workspace / "nested/first").read_bytes())

    def test_existing_identical_files_preserve_identity_and_mtime(self):
        target = self.workspace / "second"
        target.write_bytes(b"new second")
        before = target.stat()
        self.install(self.preflight())
        after = target.stat()
        self.assertEqual((before.st_dev, before.st_ino, before.st_mtime_ns),
                         (after.st_dev, after.st_ino, after.st_mtime_ns))

    def test_explicit_overwrite_replaces_only_original_planned_file(self):
        target = self.workspace / "second"
        target.write_bytes(b"original")
        unrelated = self.workspace / "unrelated"
        unrelated.write_bytes(b"keep")
        installed, overwritten, merged = self.install(self.preflight(overwrite=True), overwrite=True)
        self.assertEqual([target], overwritten)
        self.assertEqual([], merged)
        self.assertIn(target, installed)
        self.assertEqual(b"new second", target.read_bytes())
        self.assertEqual(b"keep", unrelated.read_bytes())

    def test_collision_in_later_file_rejects_all_output(self):
        (self.workspace / "second").write_bytes(b"foreign")
        with self.assertRaises(setup.SetupError):
            self.preflight()
        self.no_output_except_second()

    def no_output_except_second(self):
        self.assertFalse((self.workspace / "nested/first").exists())
        self.assertEqual(b"foreign", (self.workspace / "second").read_bytes())

    def test_duplicate_case_alias_and_unsafe_destinations_reject_before_output(self):
        for paths in (("same", "same"), ("Same", "same"), ("../escape", "second"),
                      ("nested", "nested/child"), ("NUL", "second")):
            with self.subTest(paths=paths):
                self.plan = [(self.first, Path(paths[0])), (self.second, Path(paths[1]))]
                with self.assertRaises(setup.SetupError):
                    self.preflight()
                self.assertEqual([], list(self.workspace.iterdir()))

    def test_byte_identical_source_replacement_after_preflight_is_rejected(self):
        captured = self.preflight()
        self.first.rename(self.source / "retained")
        self.first.write_bytes(b"new first")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output()
        self.assertEqual(b"new first", self.first.read_bytes())

    def test_source_parent_replacement_after_preflight_is_rejected(self):
        captured = self.preflight()
        self.source.rename(self.harness / "retained")
        self.source.mkdir()
        self.first.write_bytes(b"new first")
        self.second.write_bytes(b"new second")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output()

    def test_same_inode_source_byte_change_is_rejected(self):
        captured = self.preflight()
        self.first.write_bytes(b"foreign")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output()
        self.assertEqual(b"foreign", self.first.read_bytes())

    def test_source_metadata_change_is_rejected(self):
        captured = self.preflight()
        before = self.first.stat()
        os.utime(self.first, ns=(before.st_atime_ns, before.st_mtime_ns + 1000000000))
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output()

    def test_byte_identical_preserved_target_replacement_is_rejected(self):
        target = self.workspace / "second"
        target.write_bytes(b"new second")
        captured = self.preflight()
        target.rename(self.workspace / "retained")
        target.write_bytes(b"new second")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.assertFalse((self.workspace / "nested/first").exists())
        self.assertEqual(b"new second", target.read_bytes())

    def test_changed_existing_overwrite_target_is_rejected(self):
        target = self.workspace / "second"
        target.write_bytes(b"original")
        captured = self.preflight(overwrite=True)
        target.write_bytes(b"foreign")
        with self.assertRaises(setup.SetupError):
            self.install(captured, overwrite=True)
        self.no_output_except_second()

    def test_late_absent_target_is_preserved_before_any_output(self):
        captured = self.preflight()
        (self.workspace / "second").write_bytes(b"foreign")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output_except_second()

    def test_existing_plain_parent_substitution_is_rejected(self):
        parent = self.workspace / "nested"
        parent.mkdir()
        captured = self.preflight()
        parent.rename(self.workspace / "retained")
        parent.mkdir()
        (parent / "foreign").write_bytes(b"keep")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output()
        self.assertEqual(b"keep", (parent / "foreign").read_bytes())

    def test_authority_substitution_is_rejected(self):
        captured = self.preflight()
        self.workspace.rename(self.base / "retained")
        self.workspace.mkdir()
        (self.workspace / "foreign").write_bytes(b"keep")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.no_output()
        self.assertEqual(b"keep", (self.workspace / "foreign").read_bytes())

    def test_parent_substitution_at_last_use_is_preserved(self):
        parent = self.workspace / "nested"
        parent.mkdir()
        captured = self.preflight()
        original = setup.replace
        def publish(*args, **kwargs):
            parent.rename(self.workspace / "retained")
            parent.mkdir()
            (parent / "foreign").write_bytes(b"last-use owner")
            return original(*args, **kwargs)
        with patch.object(setup, "replace", side_effect=publish):
            with self.assertRaises(setup.SetupError):
                self.install(captured)
        self.no_output()
        self.assertEqual(b"last-use owner", (parent / "foreign").read_bytes())

    def test_capture_cannot_be_used_for_another_workspace_or_policy(self):
        captured = self.preflight()
        original_workspace = self.workspace
        self.workspace = self.base / "other"
        self.workspace.mkdir()
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.workspace = original_workspace
        with self.assertRaises(setup.SetupError):
            self.install(captured, overwrite=True)
        self.no_output()

    def test_source_hardlink_and_target_hardlink_are_refused(self):
        os.link(self.first, self.source / "alias")
        with self.assertRaises(setup.SetupError):
            self.preflight()
        (self.source / "alias").unlink()
        os.link(self.first, self.workspace / "second")
        with self.assertRaises(setup.SetupError):
            self.preflight(overwrite=True)
        self.assertFalse((self.workspace / "nested/first").exists())
        self.assertEqual(b"new first", self.first.read_bytes())

    @unittest.skipUnless(os.name == "nt", "native Windows sharing-denied authority read")
    def test_native_sharing_denial_rejects_capture_without_output(self):
        target = self.workspace / "second"
        target.write_bytes(b"new second")
        native, _ = mutation._windows_file_handle(target, access=mutation._GENERIC_READ, share=0)
        try:
            with self.assertRaises(setup.SetupError) as raised:
                self.preflight()
            self.assertEqual(setup.SETUP_ADAPTER_COLLISION, raised.exception.code)
            self.assertFalse((self.workspace / "nested/first").exists())
        finally:
            mutation._windows_close(native)
        self.assertEqual(b"new second", target.read_bytes())

    @unittest.skipUnless(os.name == "nt", "native Windows junction boundary")
    def test_junction_replaces_parent_after_preflight(self):
        parent = self.workspace / "nested"
        parent.mkdir()
        captured = self.preflight()
        parent.rename(self.workspace / "retained")
        outside = self.base / "outside"
        outside.mkdir()
        (outside / "foreign").write_bytes(b"keep")
        result = subprocess.run(["cmd", "/c", "mklink", "/J", str(parent), str(outside)],
                                capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stderr)
        try:
            with self.assertRaises(setup.SetupError):
                self.install(captured)
            self.assertEqual(["foreign"], [path.name for path in outside.iterdir()])
            self.assertFalse((self.workspace / "second").exists())
        finally:
            parent.rmdir()

    def test_late_directory_at_native_creation_is_preserved(self):
        captured = self.preflight()
        parent = self.workspace / "nested"
        with ExitStack() as injection:
            if os.name == "nt":
                original = mutation._windows_open_directory
                def create(path, **kwargs):
                    if kwargs.get("create"):
                        parent.mkdir()
                        (parent / "foreign").write_bytes(b"late owner")
                    return original(path, **kwargs)
                injection.enter_context(patch.object(mutation, "_windows_open_directory", side_effect=create))
            else:
                original = mutation.os.mkdir
                def create(path, mode=0o777, *, dir_fd=None):
                    if dir_fd is not None and str(path) == "nested":
                        original(parent)
                        (parent / "foreign").write_bytes(b"late owner")
                    return original(path, mode, dir_fd=dir_fd)
                injection.enter_context(patch.object(mutation.os, "mkdir", new=create))
                injection.enter_context(patch.object(mutation.os, "supports_dir_fd",
                                                      mutation.os.supports_dir_fd | {create}))
            with self.assertRaises(setup.SetupError):
                self.install(captured)
        self.no_output()
        self.assertEqual(b"late owner", (parent / "foreign").read_bytes())

    def test_late_file_at_native_no_replace_is_preserved(self):
        self.plan = [(self.second, Path("second"))]
        captured = self.preflight()
        target = self.workspace / "second"
        if os.name == "nt":
            original = mutation._windows_rename_handle
            def rename(native, anchor, name, **kwargs):
                target.write_bytes(b"foreign")
                return original(native, anchor, name, **kwargs)
            injection = patch.object(mutation, "_windows_rename_handle", side_effect=rename)
        else:
            original = mutation._posix_rename
            def rename(*args, **kwargs):
                target.write_bytes(b"foreign")
                return original(*args, **kwargs)
            injection = patch.object(mutation, "_posix_rename", side_effect=rename)
        with injection:
            with self.assertRaises(setup.SetupError):
                self.install(captured)
        self.assertEqual(b"foreign", target.read_bytes())
        self.assertFalse(any(path.name.startswith(".second.") for path in self.workspace.iterdir()))

    def test_partial_publication_preserves_late_foreign_second_file(self):
        captured = self.preflight()
        publications = []
        original = setup.replace
        def publish(*args, **kwargs):
            result = original(*args, **kwargs)
            (self.workspace / "second").write_bytes(b"foreign")
            return result
        with patch.object(setup, "replace", side_effect=publish):
            with self.assertRaises(setup.SetupError):
                self.install(captured, publications=publications)
        self.assertEqual([], publications)
        self.assertEqual(b"new first", (self.workspace / "nested/first").read_bytes())
        self.assertEqual(b"foreign", (self.workspace / "second").read_bytes())

    def config_plan(self, relative, shipped, existing):
        self.first.write_bytes(shipped)
        self.plan = [(self.first, relative)]
        target = self.workspace / relative
        target.parent.mkdir(parents=True)
        target.write_bytes(existing)
        return target

    def test_shared_codex_configuration_is_preserved_with_overwrite(self):
        target = self.config_plan(setup.CODEX_ROOT_CONFIG, b"[features]\nhooks = true\n",
                                 b'model = "custom"\n[features]\nhooks = true\n')
        before = target.stat()
        self.install(self.preflight(overwrite=True), overwrite=True)
        self.assertEqual(b'model = "custom"\n[features]\nhooks = true\n', target.read_bytes())
        self.assertEqual((before.st_ino, before.st_mtime_ns), (target.stat().st_ino, target.stat().st_mtime_ns))

    def test_shared_config_changed_after_preflight_is_preserved_as_foreign(self):
        target = self.config_plan(setup.CODEX_ROOT_CONFIG, b"[features]\nhooks = true\n",
                                 b"[features]\nhooks = true\n")
        captured = self.preflight()
        target.write_bytes(b"[features]\nhooks = false\n")
        with self.assertRaises(setup.SetupError):
            self.install(captured)
        self.assertEqual(b"[features]\nhooks = false\n", target.read_bytes())

    def test_shared_json_merge_keeps_fields_order_groups_and_is_idempotent(self):
        shipped = {"hooks": {"Stop": [{"hooks": [{"command": "harness"}]}]}}
        existing = {"custom": "kept", "permissions": {"mode": "ask"},
                    "hooks": {"SessionStart": [{"hooks": [{"command": "user-start"}]}],
                              "Stop": [{"hooks": [{"command": "user-stop"}]}]}}
        target = self.config_plan(Path(".qwen/settings.json"),
                                  json.dumps(shipped).encode(), json.dumps(existing).encode())
        _, overwritten, merged = self.install(self.preflight())
        self.assertEqual(([], [target]), (overwritten, merged))
        value = json.loads(target.read_bytes())
        self.assertEqual("kept", value["custom"])
        self.assertEqual({"mode": "ask"}, value["permissions"])
        self.assertEqual(["SessionStart", "Stop"], list(value["hooks"]))
        self.assertEqual(["user-stop", "harness"],
                         [group["hooks"][0]["command"] for group in value["hooks"]["Stop"]])
        before = target.stat()
        original_bytes = target.read_bytes()
        _, overwritten, merged = self.install(self.preflight(overwrite=True), overwrite=True)
        self.assertEqual(([], []), (overwritten, merged))
        self.assertEqual(original_bytes, target.read_bytes())
        self.assertEqual((before.st_ino, before.st_mtime_ns), (target.stat().st_ino, target.stat().st_mtime_ns))

    def test_changed_shipped_command_group_is_a_collision_even_with_overwrite(self):
        shipped = {"hooks": {"Stop": [{"hooks": [{"command": "harness"}]}]}}
        existing = {"hooks": {"Stop": [{"hooks": [{"command": "harness", "timeout": 1}]}]}}
        target = self.config_plan(Path(".claude/settings.json"),
                                  json.dumps(shipped).encode(), json.dumps(existing).encode())
        with self.assertRaises(setup.SetupError):
            self.preflight(overwrite=True)
        self.assertEqual(existing, json.loads(target.read_bytes()))


class RootBindingPublicationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = MaterializationFixture()
        self.addCleanup(self.fixture.close)
        self.harness = self.fixture.harness
        self.workspace = self.fixture.root_workspace
        self.runtime = self.workspace / ".harness-runtime"
        self.plan = setup._plan_root_payloads(self.harness)

    def installed(self):
        captured = setup._capture_root_payloads(self.plan, self.workspace)
        self.bindings = setup._plan_root_hook_bindings(
            self.harness, self.plan,
            captured_sources={file.source.path: file.source.state.content for file in captured.files})
        templates = {self.workspace / relative: value for _, relative, value in self.bindings}
        planned = setup._preflight_root_payloads(self.plan, self.workspace, overwrite=False,
                                                 harness_root=self.harness, runtime_root=self.runtime,
                                                 binding_templates=templates, captured=captured)
        publications = []
        setup._install_root_payloads(self.harness, self.workspace, plan=self.plan, overwrite=False,
                                      captured=planned, publications=publications)
        return publications[0]

    def bind(self, publication):
        return setup._materialize_root_hook_bindings(self.workspace, self.harness, self.runtime,
                                                      self.bindings, publication=publication)

    def test_installation_proof_binds_and_preserves_unchanged_materialized_binding(self):
        publication = self.installed()
        paths = self.bind(publication)
        for path in paths:
            self.assertEqual(str(self.runtime.resolve()), json.loads(path.read_bytes())["runtime_root"])
        before = {path: (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) for path in paths}
        publications = []
        templates = {self.workspace / relative: value for _, relative, value in self.bindings}
        planned = setup._preflight_root_payloads(self.plan, self.workspace, overwrite=False,
                                                 harness_root=self.harness, runtime_root=self.runtime,
                                                 binding_templates=templates)
        setup._install_root_payloads(self.harness, self.workspace, plan=self.plan, overwrite=False,
                                      captured=planned, publications=publications)
        self.bind(publications[0])
        self.assertEqual(before, {path: (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)
                                  for path in paths})

    def test_byte_identical_foreign_binding_after_install_is_refused(self):
        publication = self.installed()
        target = self.workspace / self.bindings[0][1]
        original = target.read_bytes()
        target.rename(target.with_name("retained-binding"))
        target.write_bytes(original)
        with self.assertRaises(setup.SetupError):
            self.bind(publication)
        self.assertEqual(original, target.read_bytes())
        self.assertIsNone(json.loads(target.read_bytes())["runtime_root"])

    def test_binding_publication_cannot_be_used_for_another_workspace(self):
        publication = self.installed()
        self.workspace = self.fixture.root / "other"
        self.workspace.mkdir()
        with self.assertRaises(setup.SetupError):
            self.bind(publication)
        self.assertEqual([], list(self.workspace.iterdir()))

    def test_partial_binding_preserves_late_foreign_second_binding(self):
        publication = self.installed()
        first = self.workspace / self.bindings[0][1]
        second = self.workspace / self.bindings[1][1]
        original = setup.replace
        def publish(*args, **kwargs):
            result = original(*args, **kwargs)
            second.write_bytes(b"foreign after first binding")
            return result
        with patch.object(setup, "replace", side_effect=publish):
            with self.assertRaises(setup.SetupError):
                self.bind(publication)
        self.assertEqual(str(self.runtime.resolve()), json.loads(first.read_bytes())["runtime_root"])
        self.assertEqual(b"foreign after first binding", second.read_bytes())

    def test_standalone_binding_carries_state_across_validation(self):
        self.installed()
        target = self.workspace / self.bindings[0][1]
        original = setup._materialized_binding
        saved_bytes = target.read_bytes()
        def expected(*args, **kwargs):
            result = original(*args, **kwargs)
            if not target.with_name("retained-binding").exists():
                target.rename(target.with_name("retained-binding"))
                target.write_bytes(saved_bytes)
            return result
        with patch.object(setup, "_materialized_binding", side_effect=expected):
            with self.assertRaises(setup.SetupError):
                setup._materialize_root_hook_bindings(self.workspace, self.harness, self.runtime, self.bindings)
        self.assertEqual(saved_bytes, target.read_bytes())

    def test_source_binding_validation_uses_original_bytes_then_install_refuses_change(self):
        captured = setup._capture_root_payloads(self.plan, self.workspace)
        source = next(file.source.path for file in captured.files
                      if file.relative.name == setup.ROOT_HOOK_BINDING_NAME)
        source.write_bytes(b"foreign invalid JSON")
        bindings = setup._plan_root_hook_bindings(
            self.harness, self.plan,
            captured_sources={file.source.path: file.source.state.content for file in captured.files})
        planned = setup._preflight_root_payloads(self.plan, self.workspace, overwrite=False, captured=captured)
        self.assertTrue(bindings)
        with self.assertRaises(setup.SetupError):
            setup._install_root_payloads(self.harness, self.workspace, plan=self.plan,
                                          overwrite=False, captured=planned)
        self.assertFalse((self.workspace / ".codex").exists())
        self.assertEqual(b"foreign invalid JSON", source.read_bytes())

    def test_public_setup_transports_the_original_preflight_plan(self):
        original = setup._install_active_cache
        target = self.workspace / ".codex/root.txt"
        def cache(*args, **kwargs):
            original(*args, **kwargs)
            target.parent.mkdir(parents=True)
            target.write_bytes(b"foreign after public preflight")
        with (self.fixture.native_setup() as observed,
              patch.object(setup, "_install_active_cache", side_effect=cache)):
            result = setup.run_setup()
        self.assertFalse(result["ok"], result)
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, result["code"])
        observed.assert_not_called()
        self.assertEqual(b"foreign after public preflight", target.read_bytes())
        self.assertFalse((self.workspace / ".custom").exists())

    def test_public_setup_transports_installed_identity_to_binding(self):
        original = setup._install_root_payloads
        target = self.workspace / ".codex/orchestrator-harness-binding.json"
        saved = []
        def install(*args, **kwargs):
            result = original(*args, **kwargs)
            saved.append(target.read_bytes())
            target.rename(target.with_name("retained-binding"))
            target.write_bytes(saved[0])
            return result
        with (self.fixture.native_setup() as observed,
              patch.object(setup, "_install_root_payloads", side_effect=install)):
            result = setup.run_setup()
        self.assertFalse(result["ok"], result)
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, result["code"])
        observed.assert_not_called()
        self.assertEqual(saved[0], target.read_bytes())
        self.assertIsNone(json.loads(target.read_bytes())["runtime_root"])

    def test_public_setup_reaches_actual_initialized_monitor(self):
        with self.fixture.native_setup() as observed:
            result = setup.run_setup()
        self.assertTrue(result["ok"], result)
        observed.assert_called_once()
        from orchestrator_harness.config import compute_config_identity, load_config, load_resource_manifest
        config = load_config(self.harness)
        setup.require_monitor_ready(self.runtime, compute_config_identity(config, load_resource_manifest(self.harness)))
        target = self.workspace / ".codex/orchestrator-harness-binding.json"
        self.assertEqual(str(self.harness.resolve()), json.loads(target.read_bytes())["harness_root"])


if __name__ == "__main__":
    unittest.main()
