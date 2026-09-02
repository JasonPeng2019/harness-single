"""Focused package metadata tests for the out-of-tree wheel surface.

Statically proves the pyproject ``packages``/``package-data`` declarations
cover every registered provider binding and every required package-owned
asset, and (when setuptools/wheel are importable) builds and inspects a wheel
in a temporary directory.
"""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import tempfile
import tomllib
import unittest
import zipfile
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPOSITORY_ROOT / "orchestrator_harness"
PYPROJECT_PATH = PACKAGE_ROOT / "pyproject.toml"

# Every package that must be declared so all runtime modules ship in the wheel.
REQUIRED_PACKAGES = (
    "orchestrator_harness",
    "orchestrator_harness.assets",
    "orchestrator_harness.assets.claude",
    "orchestrator_harness.assets.codex",
    "orchestrator_harness.assets.qwen",
    "orchestrator_harness.assets.rules",
    "orchestrator_harness.provider_adapters",
    "orchestrator_harness.provider_adapters.claude-code",
    "orchestrator_harness.provider_adapters.codex",
    "orchestrator_harness.provider_adapters.qwen-code",
    "harness_common",
)

# Package-owned assets that must be declared as package data.
REQUIRED_PACKAGE_DATA = (
    "assets/claude/manifest.json",
    "assets/claude/settings.fragment.json",
    "assets/claude/orchestrator_harness_post_tool_use.py",
    "assets/claude/orchestrator_harness_stop.py",
    "assets/codex/bounded-exclusions.gitignore",
    "config.example.json",
    "install_wsl_codex.ps1",
    "provider_adapters/claude-code/launcher_binding.py",
    "provider_adapters/codex/launcher_binding.py",
    "provider_adapters/qwen-code/launcher_binding.py",
)

# Harness-root single-source trees that must never be duplicated in the wheel.
FORBIDDEN_PACKAGE_DATA_PREFIXES = ("adapters/", "super-cache/")
FORBIDDEN_PACKAGE_DATA_NAMES = ("harness-config.json", "resource-manifest.json")


def _load_pyproject() -> dict:
    with PYPROJECT_PATH.open("rb") as handle:
        return tomllib.load(handle)


def _glob_matches(pattern: str, relative_path: str) -> bool:
    regex = re.escape(pattern).replace(r"\*", "[^/]*")
    return re.fullmatch(regex, relative_path) is not None


def _package_data_patterns(pyproject: dict) -> list[str]:
    return list(
        pyproject["tool"]["setuptools"]["package-data"]["orchestrator_harness"]
    )


def _on_disk_required_files() -> list[str]:
    """Every non-module package-owned file that must ship in the wheel."""
    required: list[str] = []
    for binding in sorted(
        (PACKAGE_ROOT / "provider_adapters").rglob("launcher_binding.py")
    ):
        required.append(binding.relative_to(PACKAGE_ROOT).as_posix())
    for relative in (
        "install_wsl_codex.ps1",
        "config.example.json",
        "assets/codex/bounded-exclusions.gitignore",
    ):
        required.append(relative)
    for sub in ("claude", "codex", "qwen"):
        for path in sorted((PACKAGE_ROOT / "assets" / sub).iterdir()):
            if path.is_file() and path.name != "__init__.py":
                required.append(path.relative_to(PACKAGE_ROOT).as_posix())
    for path in sorted((PACKAGE_ROOT / "assets" / "rules").iterdir()):
        if path.is_file() and path.name != "__init__.py":
            required.append(path.relative_to(PACKAGE_ROOT).as_posix())
    for path in sorted((PACKAGE_ROOT / "assets" / "release").rglob("*")):
        if path.is_file():
            required.append(path.relative_to(PACKAGE_ROOT).as_posix())
    return sorted(set(required))


class PackageMetadataStaticTests(unittest.TestCase):
    def test_packages_cover_every_runtime_module(self) -> None:
        pyproject = _load_pyproject()
        declared = set(pyproject["tool"]["setuptools"]["packages"])
        for package in REQUIRED_PACKAGES:
            self.assertIn(package, declared, package)
        for marker in sorted(PACKAGE_ROOT.rglob("__init__.py")):
            package_dir = marker.parent
            relative = package_dir.relative_to(PACKAGE_ROOT)
            if (
                "__pycache__" in relative.parts
                or "tests" in relative.parts
                or "egg-info" in relative.parts
            ):
                continue
            dotted = (
                "orchestrator_harness"
                if relative.parts == ()
                else "orchestrator_harness." + ".".join(relative.parts)
            )
            self.assertIn(dotted, declared, dotted)
        self.assertEqual(
            "../harness_common",
            pyproject["tool"]["setuptools"]["package-dir"]["harness_common"],
        )

    def test_package_data_covers_registered_bindings_and_required_assets(
        self,
    ) -> None:
        pyproject = _load_pyproject()
        patterns = _package_data_patterns(pyproject)
        self.assertTrue(patterns)
        for relative in _on_disk_required_files():
            self.assertTrue(
                any(_glob_matches(pattern, relative) for pattern in patterns),
                f"no package-data pattern covers {relative}",
            )
        for relative in REQUIRED_PACKAGE_DATA:
            self.assertTrue(
                any(_glob_matches(pattern, relative) for pattern in patterns),
                f"no package-data pattern covers {relative}",
            )
        for pattern in patterns:
            for prefix in FORBIDDEN_PACKAGE_DATA_PREFIXES:
                self.assertFalse(pattern.startswith(prefix), pattern)
            for name in FORBIDDEN_PACKAGE_DATA_NAMES:
                self.assertNotIn(name, pattern, pattern)

    def test_data_files_preserve_release_destinations(self) -> None:
        pyproject = _load_pyproject()
        data_files = pyproject["tool"]["setuptools"]["data-files"]
        self.assertIn("examples", data_files)
        self.assertIn("release_evidence_templates", data_files)
        self.assertIn(
            "assets/release/templates/*.md",
            data_files["release_evidence_templates"],
        )


class PackageWheelBuildTests(unittest.TestCase):
    def test_wheel_includes_registered_bindings_and_required_assets(self) -> None:
        if (
            importlib.util.find_spec("setuptools") is None
            or importlib.util.find_spec("wheel") is None
        ):
            self.skipTest(
                "setuptools/wheel are not importable in this environment"
            )
        with tempfile.TemporaryDirectory(prefix="orchestrator-package-wheel-") as raw:
            temporary = Path(raw)
            source = temporary / "source"
            wheel_dir = temporary / "wheel"
            wheel_dir.mkdir()
            ignored = shutil.ignore_patterns(
                "__pycache__", "*.pyc", "*.egg-info", "build", "dist"
            )
            shutil.copytree(
                PACKAGE_ROOT, source / "orchestrator_harness", ignore=ignored
            )
            shutil.copytree(
                REPOSITORY_ROOT / "harness_common",
                source / "harness_common",
                ignore=ignored,
            )
            shutil.copytree(
                REPOSITORY_ROOT / "examples", source / "examples", ignore=ignored
            )
            previous = os.getcwd()
            try:
                os.chdir(source / "orchestrator_harness")
                from setuptools.build_meta import build_wheel

                wheel_name = build_wheel(str(wheel_dir))
            finally:
                os.chdir(previous)
            wheel = wheel_dir / wheel_name
            self.assertTrue(wheel.is_file(), wheel)
            with zipfile.ZipFile(wheel) as archive:
                names = set(archive.namelist())
            required_entries = (
                "orchestrator_harness/__init__.py",
                "orchestrator_harness/release_checks.py",
                "orchestrator_harness/public_launch.py",
                "orchestrator_harness/cli.py",
                "orchestrator_harness/claude_installer.py",
                "orchestrator_harness/provider_adapters/__init__.py",
                "orchestrator_harness/provider_adapters/codex/__init__.py",
                "orchestrator_harness/provider_adapters/codex/launcher_binding.py",
                "orchestrator_harness/provider_adapters/claude-code/__init__.py",
                "orchestrator_harness/provider_adapters/claude-code/launcher_binding.py",
                "orchestrator_harness/provider_adapters/qwen-code/__init__.py",
                "orchestrator_harness/provider_adapters/qwen-code/launcher_binding.py",
                "orchestrator_harness/assets/claude/__init__.py",
                "orchestrator_harness/assets/claude/manifest.json",
                "orchestrator_harness/assets/claude/settings.fragment.json",
                "orchestrator_harness/assets/claude/orchestrator_harness_post_tool_use.py",
                "orchestrator_harness/assets/claude/orchestrator_harness_stop.py",
                "orchestrator_harness/assets/codex/bounded-exclusions.gitignore",
                "orchestrator_harness/config.example.json",
                "orchestrator_harness/install_wsl_codex.ps1",
                "orchestrator_harness/assets/release/manifest.json",
                "orchestrator_harness/assets/release/examples/public-coding-launch.example.md",
                "orchestrator_harness/assets/release/examples/release-selection.example.json",
                "orchestrator_harness/assets/release/templates/COMPLETION.md",
                "harness_common/__init__.py",
                "harness_common/process_identity.py",
            )
            for entry in required_entries:
                self.assertIn(entry, names, entry)
            self.assertTrue(any(".data/data/examples/" in name for name in names))
            self.assertTrue(
                any(".data/data/release_evidence_templates/" in name for name in names)
            )
            for name in names:
                lowered = name.lower()
                self.assertFalse(lowered.startswith("adapters/"), name)
                self.assertFalse(lowered.startswith("super-cache/"), name)
                self.assertNotIn("harness-config.json", lowered, name)
                self.assertNotIn("resource-manifest.json", lowered, name)


if __name__ == "__main__":
    unittest.main()
