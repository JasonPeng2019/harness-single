"""Configuration contracts for optional sparse checkout scope."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.config import (
    ConfigError,
    ResourceManifest,
    compute_config_identity,
    load_config,
)
from orchestrator_harness.core import sha256_hex


class CheckoutPathsConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.harness = self.root / "harness"
        self.product = self.root / "product"
        self.harness.mkdir()
        self.product.mkdir()
        self.path = self.harness / "harness-config.json"

    def write(self, **extra: object) -> None:
        self.path.write_text(
            json.dumps({"root_workspace": str(self.product), **extra}), encoding="utf-8"
        )

    def test_omission_retains_historical_full_checkout_identity(self) -> None:
        self.write()
        config = load_config(self.harness)
        self.assertIsNone(config.checkout_paths)
        self.assertEqual(
            sha256_hex(
                {
                    "root_workspace": str(self.product),
                    "managed_coordination": "enabled",
                    "resource_manifest": [],
                    "runtime_record_schema_version": 1,
                }
            ),
            compute_config_identity(config, ResourceManifest()),
        )

    def test_scope_is_preserved_in_config_and_epoch_identity(self) -> None:
        selected = ["src", "nested/has spaces/Δelta"]
        self.write(checkout_paths=selected)
        config = load_config(self.harness)
        self.assertEqual(tuple(selected), config.checkout_paths)
        scoped_identity = compute_config_identity(config, ResourceManifest())
        self.write()
        full_identity = compute_config_identity(
            load_config(self.harness), ResourceManifest()
        )
        self.assertNotEqual(
            scoped_identity,
            full_identity,
        )

    def test_rejects_empty_or_non_list_scope(self) -> None:
        for value in (None, [], "src", ["src", 3], [None]):
            with self.subTest(value=value):
                self.write(checkout_paths=value)
                with self.assertRaises(ConfigError):
                    load_config(self.harness)

    def test_rejects_paths_git_could_interpret_or_escape(self) -> None:
        invalid = (
            "/absolute",
            "C:/drive",
            "C:drive-relative",
            "\\\\server\\share",
            "src\\nested",
            "../outside",
            "src/../outside",
            "./src",
            "src//nested",
            "src/*",
            "src/?",
            "src/[ab]",
            ":(glob)src",
            "src:literal",
            "src\nother",
            "src\x00other",
        )
        for value in invalid:
            with self.subTest(value=value):
                self.write(checkout_paths=[value])
                with self.assertRaises(ConfigError):
                    load_config(self.harness)

    def test_rejects_duplicate_scope_entries_and_unknown_keys(self) -> None:
        self.write(checkout_paths=["src", "src"])
        with self.assertRaisesRegex(ConfigError, "duplicate"):
            load_config(self.harness)
        self.write(checkout_paths=["src"], unrelated=True)
        with self.assertRaisesRegex(ConfigError, "unknown key"):
            load_config(self.harness)


if __name__ == "__main__":
    unittest.main()
