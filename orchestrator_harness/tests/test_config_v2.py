from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness.config import (
    ConfigError,
    CONFIG_FILE_NAME,
    LOCAL_CONFIG_DIR_NAME,
    MANIFEST_FILE_NAME,
    RUNTIME_DIR_NAME,
    find_harness_root,
    load_config,
    load_resource_manifest,
)
from orchestrator_harness.tests.support import write_json


class HarnessV2RootConfigTests(unittest.TestCase):
    """Focused product tests for the closed ROOT config and manifest shapes."""

    def _root(self) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def _write_config(self, root: Path, value: dict[str, object]) -> Path:
        path = root / "harness-config.json"
        write_json(path, value)
        return path

    def _write_manifest(self, root: Path, value: dict[str, object]) -> Path:
        path = root / "resource-manifest.json"
        write_json(path, value)
        return path

    def _write_product_markers(self, root: Path) -> None:
        package = root / "orchestrator_harness"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        (root / "adapters").mkdir()
        (root / "super-cache").mkdir()

    def test_local_config_pair_is_preferred_and_records_its_path(self) -> None:
        root = self._root()
        legacy_workspace = root / "legacy-workspace"
        local_workspace = root / "local-workspace"
        legacy_workspace.mkdir()
        local_workspace.mkdir()
        self._write_config(root, {"root_workspace": str(legacy_workspace)})
        self._write_manifest(
            root, {"schema": "resource-manifest/v1", "resources": []}
        )
        local = root / LOCAL_CONFIG_DIR_NAME
        write_json(local / CONFIG_FILE_NAME, {"root_workspace": str(local_workspace)})
        write_json(
            local / MANIFEST_FILE_NAME,
            {
                "schema": "resource-manifest/v1",
                "resources": [{"id": "local-only", "exclusive": True}],
            },
        )

        config = load_config(root)
        manifest = load_resource_manifest(root)

        self.assertEqual(local_workspace, config.root_workspace)
        self.assertEqual(local / CONFIG_FILE_NAME, config.config_path)
        self.assertEqual(("local-only",), manifest.resource_ids())

    def test_partial_local_pair_never_mixes_with_legacy_root_pair(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(root, {"root_workspace": str(workspace)})
        self._write_manifest(
            root, {"schema": "resource-manifest/v1", "resources": []}
        )
        local = root / LOCAL_CONFIG_DIR_NAME
        write_json(local / CONFIG_FILE_NAME, {"root_workspace": str(workspace)})

        with self.assertRaisesRegex(ConfigError, "local resource manifest missing"):
            load_resource_manifest(root)

    def test_empty_local_directory_disables_legacy_fallback(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(root, {"root_workspace": str(workspace)})
        self._write_manifest(
            root, {"schema": "resource-manifest/v1", "resources": []}
        )
        (root / LOCAL_CONFIG_DIR_NAME).mkdir()

        with self.assertRaisesRegex(ConfigError, "local harness config missing"):
            load_config(root)
        with self.assertRaisesRegex(ConfigError, "local resource manifest missing"):
            load_resource_manifest(root)

    def test_discovery_stops_at_unconfigured_product_root(self) -> None:
        outer = self._root()
        stale_workspace = outer / "stale-workspace"
        stale_workspace.mkdir()
        self._write_config(outer, {"root_workspace": str(stale_workspace)})
        product = outer / "harness-single"
        product.mkdir()
        self._write_product_markers(product)
        nested = product / "orchestrator_harness" / "tests"
        nested.mkdir()

        found = find_harness_root(nested)

        self.assertEqual(product, found)
        with self.assertRaisesRegex(
            ConfigError,
            r"local harness config missing: .*local-config.*copy examples/",
        ):
            load_config(found)

    def test_discovery_rejects_a_config_only_ancestor(self) -> None:
        outer = self._root()
        self._write_config(outer, {"root_workspace": str(outer / "workspace")})
        nested = outer / "not-a-product" / "child"
        nested.mkdir(parents=True)

        with self.assertRaisesRegex(ConfigError, "harness product root not found"):
            find_harness_root(nested)

    def test_tracked_local_config_examples_validate_after_copy(self) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        examples = repository_root / "examples"
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        local = root / LOCAL_CONFIG_DIR_NAME
        config_example = json.loads(
            (examples / "harness-config.example.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            "REPLACE_WITH_ABSOLUTE_PATH_TO_TARGET_REPOSITORY",
            config_example["root_workspace"],
        )
        write_json(local / CONFIG_FILE_NAME, config_example)
        with self.assertRaisesRegex(ConfigError, "root_workspace must be absolute"):
            load_config(root)
        config_example["root_workspace"] = str(workspace)
        write_json(local / CONFIG_FILE_NAME, config_example)
        manifest_example = json.loads(
            (examples / "resource-manifest.example.json").read_text(encoding="utf-8")
        )
        write_json(local / MANIFEST_FILE_NAME, manifest_example)

        self.assertEqual(workspace, load_config(root).root_workspace)
        self.assertEqual((), load_resource_manifest(root).resources)

    def test_config_exact_two_key_shape_loads(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(
            root,
            {
                "root_workspace": str(workspace),
                "managed_coordination": "enabled",
            },
        )
        config = load_config(root)
        self.assertEqual(workspace, config.root_workspace)
        self.assertEqual("enabled", config.managed_coordination)
        self.assertEqual("managed", config.profile)
        self.assertEqual(workspace / RUNTIME_DIR_NAME, config.runtime_root)

    def test_config_omitted_managed_coordination_defaults_to_enabled(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(root, {"root_workspace": str(workspace)})
        config = load_config(root)
        self.assertEqual("enabled", config.managed_coordination)
        self.assertEqual("managed", config.profile)

    def test_config_disabled_managed_coordination_is_plain(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(
            root,
            {
                "root_workspace": str(workspace),
                "managed_coordination": "disabled",
            },
        )
        config = load_config(root)
        self.assertEqual("disabled", config.managed_coordination)
        self.assertEqual("plain", config.profile)

    def test_config_rejects_literal_schema_field(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(
            root,
            {
                "root_workspace": str(workspace),
                "schema": "harness-config/v1",
            },
        )
        with self.assertRaises(ConfigError):
            load_config(root)

    def test_config_rejects_any_unknown_key(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        for extra in ("profile", "suite_root", "run_globs", "runtime_path"):
            self._write_config(
                root,
                {
                    "root_workspace": str(workspace),
                    extra: "anything",
                },
            )
            with self.assertRaises(ConfigError):
                load_config(root)

    def test_config_rejects_missing_root_workspace(self) -> None:
        root = self._root()
        self._write_config(root, {"managed_coordination": "enabled"})
        with self.assertRaises(ConfigError):
            load_config(root)

    def test_config_rejects_relative_root_workspace(self) -> None:
        root = self._root()
        self._write_config(root, {"root_workspace": "relative/workspace"})
        with self.assertRaises(ConfigError):
            load_config(root)

    def test_config_rejects_invalid_managed_coordination(self) -> None:
        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        for value in ("yes", "on", 1, None):
            self._write_config(
                root,
                {
                    "root_workspace": str(workspace),
                    "managed_coordination": value,
                },
            )
            with self.assertRaises(ConfigError):
                load_config(root)

    def test_config_rejects_symlink_root_workspace(self) -> None:
        root = self._root()
        target = root / "target"
        target.mkdir()
        link = root / "workspace-link"
        try:
            os.symlink(target, link, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("symlink creation is not permitted in this environment")
        self.assertTrue(link.is_symlink())
        self._write_config(root, {"root_workspace": str(link)})
        with self.assertRaises(ConfigError):
            load_config(root)

    def test_manifest_empty_resources_is_valid(self) -> None:
        root = self._root()
        self._write_manifest(
            root, {"schema": "resource-manifest/v1", "resources": []}
        )
        manifest = load_resource_manifest(root)
        self.assertEqual((), manifest.resources)
        self.assertEqual((), manifest.resource_ids())

    def test_manifest_exact_entry_shape_loads(self) -> None:
        root = self._root()
        self._write_manifest(
            root,
            {
                "schema": "resource-manifest/v1",
                "resources": [{"id": "gpu-0", "exclusive": True}],
            },
        )
        manifest = load_resource_manifest(root)
        self.assertEqual(({"id": "gpu-0", "exclusive": True},), manifest.resources)
        self.assertTrue(manifest.is_declared("gpu-0"))
        self.assertFalse(manifest.is_declared("missing"))

    def test_manifest_rejects_missing_schema(self) -> None:
        root = self._root()
        self._write_manifest(root, {"resources": []})
        with self.assertRaises(ConfigError):
            load_resource_manifest(root)

    def test_manifest_rejects_wrong_schema_value(self) -> None:
        root = self._root()
        for value in ("resource-manifest/v2", "harness-config/v1", 1, None):
            self._write_manifest(
                root,
                {"schema": value, "resources": []},
            )
            with self.assertRaises(ConfigError):
                load_resource_manifest(root)

    def test_manifest_rejects_unknown_root_key(self) -> None:
        root = self._root()
        self._write_manifest(
            root,
            {
                "schema": "resource-manifest/v1",
                "resources": [],
                "owner": "root",
            },
        )
        with self.assertRaises(ConfigError):
            load_resource_manifest(root)

    def test_manifest_rejects_non_list_resources(self) -> None:
        root = self._root()
        self._write_manifest(
            root,
            {"schema": "resource-manifest/v1", "resources": {"id": "gpu-0"}},
        )
        with self.assertRaises(ConfigError):
            load_resource_manifest(root)

    def test_manifest_rejects_non_object_entry(self) -> None:
        root = self._root()
        self._write_manifest(
            root,
            {"schema": "resource-manifest/v1", "resources": ["gpu-0"]},
        )
        with self.assertRaises(ConfigError):
            load_resource_manifest(root)

    def test_manifest_rejects_entry_unknown_key(self) -> None:
        root = self._root()
        self._write_manifest(
            root,
            {
                "schema": "resource-manifest/v1",
                "resources": [{"id": "gpu-0", "exclusive": True, "owner": "x"}],
            },
        )
        with self.assertRaises(ConfigError):
            load_resource_manifest(root)

    def test_manifest_rejects_missing_or_empty_id(self) -> None:
        root = self._root()
        for entry in ({"exclusive": True}, {"id": "", "exclusive": True}):
            self._write_manifest(
                root,
                {"schema": "resource-manifest/v1", "resources": [entry]},
            )
            with self.assertRaises(ConfigError):
                load_resource_manifest(root)

    def test_manifest_rejects_missing_or_non_true_exclusive(self) -> None:
        root = self._root()
        for entry in (
            {"id": "gpu-0"},
            {"id": "gpu-0", "exclusive": False},
            {"id": "gpu-0", "exclusive": "yes"},
            {"id": "gpu-0", "exclusive": 1},
        ):
            self._write_manifest(
                root,
                {"schema": "resource-manifest/v1", "resources": [entry]},
            )
            with self.assertRaises(ConfigError):
                load_resource_manifest(root)

    def test_manifest_rejects_duplicate_ids(self) -> None:
        root = self._root()
        self._write_manifest(
            root,
            {
                "schema": "resource-manifest/v1",
                "resources": [
                    {"id": "gpu-0", "exclusive": True},
                    {"id": "gpu-0", "exclusive": True},
                ]
            },
        )
        with self.assertRaises(ConfigError):
            load_resource_manifest(root)

    def test_setup_seam_returns_stable_config_error(self) -> None:
        from orchestrator_harness.setup import SETUP_CONFIG_INVALID, run_setup

        root = self._root()
        workspace = root / "workspace"
        workspace.mkdir()
        self._write_config(
            root,
            {
                "root_workspace": str(workspace),
                "schema": "harness-config/v1",
            },
        )
        self._write_manifest(
            root, {"schema": "resource-manifest/v1", "resources": []}
        )
        with patch("orchestrator_harness.setup.find_harness_root", return_value=root):
            result = run_setup()
        self.assertFalse(result["ok"])
        self.assertEqual(SETUP_CONFIG_INVALID, result["code"])
        self.assertIn("unknown key", result["summary"])


if __name__ == "__main__":
    unittest.main()
