from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import os
import unittest
from unittest.mock import patch

from orchestrator_harness.discovery import discover_run
from orchestrator_harness.tests.support import SuiteFixture, write_json


class DiscoveryTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def test_single_recursive_walk_classifies_helper_and_mcp_records_independently(self) -> None:
        workspace = self.fixture.workspace()
        helper = workspace / "nested" / "a" / "live-context.json"
        mcp = workspace / "nested" / "b" / "mcp_process.json"
        bad_helper = workspace / "nested" / "c" / "helper_process.json"
        bad_mcp = workspace / "nested" / "d" / "mcp-lifetime.json"
        helper_directory = workspace / "nested" / "e" / "helper_process.json"
        mcp_directory = workspace / "nested" / "f" / "mcp_processes.json"
        write_json(helper, {"kind": "helper"})
        write_json(mcp, {"kind": "mcp"})
        for path in (bad_helper, bad_mcp):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("[", encoding="utf-8")
        helper_directory.mkdir(parents=True)
        mcp_directory.mkdir(parents=True)

        with patch(
            "orchestrator_harness.discovery.os.walk", wraps=os.walk
        ) as recursive_walk:
            records = discover_run(workspace.parent, workspace, self.fixture.config)

        recursive_walk.assert_called_once_with(workspace)
        self.assertEqual([helper], [record.path for record in records.helper_records])
        self.assertEqual([mcp], [record.path for record in records.mcp_records])
        self.assertEqual(
            {
                (str(bad_helper), "HELPER_READ_ERROR"),
                (str(bad_mcp), "MCP_READ_ERROR"),
                (str(helper_directory), "HELPER_READ_ERROR"),
                (str(mcp_directory), "MCP_READ_ERROR"),
            },
            {(error.path, error.code) for error in records.errors},
        )


if __name__ == "__main__":
    unittest.main()
