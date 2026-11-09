"""Portable source identity and output-boundary checks for the native probe."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "probe_windows_worker_isolation.py"
spec = importlib.util.spec_from_file_location("windows_worker_probe", SCRIPT)
probe = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(probe)


class ProbePortabilityTests(unittest.TestCase):
    def test_installed_source_outside_git_is_supported(self):
        with tempfile.TemporaryDirectory() as temporary:
            installed = Path(temporary)
            self.assertIsNone(probe._source_head(installed))

    def test_git_identity_is_recorded_only_at_exact_repository_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "-c",
                    "user.name=Probe",
                    "-c",
                    "user.email=probe@invalid",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "seed",
                ],
                check=True,
            )
            self.assertTrue(probe._source_head(root))
            nested = root / "nested"
            nested.mkdir()
            self.assertIsNone(probe._source_head(nested))

    def test_output_boundary_requires_runtime_descendant(self):
        root = Path(os.path.abspath("portable-harness"))
        runtime = root / "runtime"
        self.assertTrue(probe._under(runtime / "probe-1", runtime))
        self.assertFalse(probe._under(root / "outside", runtime))


if __name__ == "__main__":
    unittest.main(verbosity=2)
