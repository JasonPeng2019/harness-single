from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness.config import HarnessConfig, ResourceManifest, compute_config_identity
from orchestrator_harness.setup import _start_monitor, read_monitor_record


class SetupMonitorIdentityTests(unittest.TestCase):
    def test_monitor_record_uses_immutable_config_identity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            workspace = root / "workspace"
            workspace.mkdir()
            config = HarnessConfig(root, workspace, "enabled")
            manifest = ResourceManifest(({"id": "gpu-0", "exclusive": True},))
            expected = compute_config_identity(config, manifest)
            with (
                patch("orchestrator_harness.setup.processes.identity_matches", return_value=False),
                patch("orchestrator_harness.setup.processes.spawn_detached", return_value=SimpleNamespace(pid=4321)),
                patch(
                    "orchestrator_harness.setup.processes.process_identity",
                    return_value={"pid": 4321, "creation_time": "2026-09-02T12:00:00Z"},
                ),
            ):
                _start_monitor(root, config.runtime_root, config, expected)
            record = read_monitor_record(config.runtime_root)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(expected, record["config_identity"])
            self.assertNotEqual(config.profile, record["config_identity"])


if __name__ == "__main__":
    unittest.main()
