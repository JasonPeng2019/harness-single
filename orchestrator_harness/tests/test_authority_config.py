"""Real filesystem regressions for configured authority and ROOT binding."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import mutation, root_hook_wrapper, setup
from orchestrator_harness.config import (
    ConfigError, HarnessConfig, ResourceManifest, compute_config_identity,
    load_config, path_identity,
)
from orchestrator_harness.core import read_json, sha256_hex


class AuthorityConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.manager = self.root / "manager"
        self.product = self.root / "product"
        for directory in (self.harness, self.manager, self.product):
            directory.mkdir()
        self.config_path = self.harness / "harness-config.json"
        self.write_config()
        (self.harness / "resource-manifest.json").write_text(
            json.dumps({"schema": "resource-manifest/v1", "resources": []}), encoding="utf-8")

    def write_config(self, **extra: object) -> None:
        self.config_path.write_text(json.dumps({"root_workspace": str(self.product), **extra}), encoding="utf-8")

    def link_directory(self, link: Path, target: Path) -> None:
        if os.name == "nt":
            completed = subprocess.run(["cmd", "/d", "/c", "mklink", "/J", str(link), str(target)],
                                       capture_output=True, text=True, check=False)
            if completed.returncode:
                self.skipTest(f"native directory junction unavailable: {completed.stderr}")
        else:
            link.symlink_to(target, target_is_directory=True)
        self.addCleanup(lambda: link.rmdir() if os.name == "nt" else link.unlink())

    def test_manager_default_preserves_positional_and_epoch_identity(self) -> None:
        loaded = load_config(self.harness)
        positional = HarnessConfig(self.harness, self.product, "enabled")
        self.assertEqual(self.product, loaded.manager_workspace)
        self.assertEqual(self.product, positional.manager_workspace)
        old_payload = {"root_workspace": str(self.product), "managed_coordination": "enabled",
                       "resource_manifest": [], "runtime_record_schema_version": 1}
        self.assertEqual(sha256_hex(old_payload), compute_config_identity(loaded, ResourceManifest()))
        self.write_config(manager_workspace=str(self.product))
        self.assertEqual(compute_config_identity(loaded, ResourceManifest()),
                         compute_config_identity(load_config(self.harness), ResourceManifest()))

    def test_split_manager_identity_and_runtime_remain_separate_in_plain_mode(self) -> None:
        self.write_config(manager_workspace=str(self.manager), managed_coordination="disabled")
        config = load_config(self.harness)
        self.assertEqual("plain", config.profile)
        self.assertEqual(self.manager, config.manager_workspace)
        self.assertEqual(self.product / ".harness-runtime", config.runtime_root)
        self.assertNotEqual(compute_config_identity(config, ResourceManifest()),
                            compute_config_identity(HarnessConfig(self.harness, self.product, "disabled"), ResourceManifest()))
        expected = {"root_workspace": str(self.product), "managed_coordination": "disabled",
                    "resource_manifest": [], "runtime_record_schema_version": 1,
                    "manager_workspace": path_identity(self.manager)}
        self.assertEqual(sha256_hex(expected), compute_config_identity(config, ResourceManifest()))

    def test_manager_values_are_closed_and_absolute(self) -> None:
        for value in (None, "", " ", "relative", 42, True):
            with self.subTest(value=value):
                self.write_config(manager_workspace=value)
                with self.assertRaises(ConfigError):
                    load_config(self.harness)

    def test_redirected_ancestor_is_rejected_for_both_destinations(self) -> None:
        redirect = self.root / "redirect"
        self.link_directory(redirect, self.root)
        for field, leaf in (("manager_workspace", "manager"), ("root_workspace", "product")):
            with self.subTest(field=field):
                self.write_config(**{field: str(redirect / leaf)})
                with self.assertRaises(ConfigError):
                    load_config(self.harness)

    def test_authority_reader_rejects_redirected_ancestry(self) -> None:
        redirect = self.root / "redirect"
        self.link_directory(redirect, self.harness)
        redirected = redirect / self.config_path.name
        with self.assertRaises(OSError):
            read_json(redirected)
        with self.assertRaises(setup.SetupError):
            setup._read_json_object(redirected, description="redirected authority")
        with self.assertRaises(ConfigError):
            load_config(redirect)

    def test_authority_reader_rejects_real_hardlink(self) -> None:
        alias = self.root / "authority-alias.json"
        os.link(self.config_path, alias)
        self.addCleanup(alias.unlink)
        with self.assertRaises(OSError):
            read_json(self.config_path)
        with self.assertRaises(setup.SetupError):
            setup._read_json_object(alias, description="shared authority")

    def test_authority_reader_rejects_actual_final_name_substitution(self) -> None:
        original = self.config_path.read_bytes()
        retained = self.config_path.with_suffix(".retained")
        def substitute() -> None:
            self.config_path.rename(retained)
            self.config_path.write_bytes(b'{"foreign":true}')
        with patch.object(mutation, "_after_target_proof", side_effect=substitute):
            with self.assertRaises(OSError):
                read_json(self.config_path)
        self.assertEqual(original, retained.read_bytes())
        self.assertEqual(b'{"foreign":true}', self.config_path.read_bytes())

    def test_authority_json_shape_and_bom_behavior_are_preserved(self) -> None:
        self.config_path.write_bytes(b'\xef\xbb\xbf{"value":1}')
        with self.assertRaises(ValueError):
            read_json(self.config_path)
        self.config_path.write_bytes(b'[]')
        with self.assertRaisesRegex(ValueError, "not a JSON object"):
            read_json(self.config_path)

    def make_binding(self) -> Path:
        self.write_config(manager_workspace=str(self.manager))
        dotdir = self.manager / ".codex"
        dotdir.mkdir()
        template = {"schema": "harness-hook-binding/v1", "role": "root", "provider_id": "codex",
                    "harness_root": None, "runtime_root": None,
                    "liveness_path": ".agent-workspace/root-hook-liveness.jsonl",
                    "delivery_receipt_path": ".agent-workspace/root-hook-delivery.jsonl"}
        target = dotdir / "orchestrator-harness-binding.json"
        target.write_text(json.dumps(template), encoding="utf-8")
        setup._materialize_root_hook_bindings(self.manager, self.harness,
                                             self.product / ".harness-runtime",
                                             [("codex", Path(".codex") / target.name, template)])
        return target

    def test_materialized_binding_records_both_destinations(self) -> None:
        target = self.make_binding()
        binding = root_hook_wrapper._read_binding(target, "codex")
        self.assertEqual(path_identity(self.manager), binding["manager_workspace"])
        self.assertEqual(path_identity(self.product), binding["root_workspace"])
        self.assertFalse((self.product / ".codex").exists())

    def test_stale_binding_rejects_before_dispatch_and_liveness(self) -> None:
        target = self.make_binding()
        self.write_config()
        with patch.object(root_hook_wrapper, "dispatch") as dispatch:
            result = root_hook_wrapper.run("codex", "stop", target)
        dispatch.assert_not_called()
        self.assertEqual("block", result["decision"])
        self.assertIn("ROOT_BINDING_STALE", result["reason"])
        self.assertFalse((self.manager / ".agent-workspace").exists())

    def test_binding_in_another_manager_cannot_dispatch(self) -> None:
        target = self.make_binding()
        foreign = self.root / "foreign" / ".codex" / target.name
        foreign.parent.mkdir(parents=True)
        foreign.write_bytes(target.read_bytes())
        with self.assertRaisesRegex(root_hook_wrapper.RootHookBindingError, "ROOT_BINDING_STALE"):
            root_hook_wrapper._read_binding(foreign, "codex")

    def test_full_split_setup_and_shutdown_use_actual_initialized_monitor(self) -> None:
        from orchestrator_harness import processes, shutdown
        from orchestrator_harness.tests.test_v2_materialization import MaterializationFixture
        for profile in ("enabled", "disabled"):
            with self.subTest(profile=profile):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                manager = fixture.root / "manager"
                manager.mkdir()
                fixture._write_json(fixture.harness / "harness-config.json", {
                    "root_workspace": str(fixture.root_workspace),
                    "manager_workspace": str(manager), "managed_coordination": profile,
                })
                with fixture.native_setup() as spawn:
                    result = setup.run_setup()
                self.assertTrue(result["ok"], result)
                spawn.assert_called_once()
                config = load_config(fixture.harness)
                ready = setup.require_monitor_ready(config.runtime_root,
                    compute_config_identity(config, ResourceManifest()))
                self.assertIs(ready["initialized"], True)
                self.assertTrue((manager / ".codex" / "root.txt").is_file())
                self.assertFalse((fixture.root_workspace / ".codex").exists())
                self.assertEqual(profile == "enabled", (config.runtime_root / "manager").exists())
                with patch.object(shutdown, "find_harness_root", return_value=fixture.harness):
                    closed = shutdown.run_shutdown()
                self.assertTrue(closed["ok"], closed)
                self.assertEqual("CLOSED", setup.read_runtime_state(config.runtime_root)["state"])
                self.assertTrue(processes.identity_exit_proven(ready["pid"], ready["creation_time"]))


if __name__ == "__main__":
    unittest.main()
