from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import unittest
from unittest.mock import patch

from orchestrator_harness.discovery import (
    _clear_observation_caches,
    _json_record,
    discover_run,
)
from orchestrator_harness.git_safety import validate_coding_result
from orchestrator_harness.stable_io import read_stable
from orchestrator_harness.tests.support import (
    SuiteFixture,
    TemporaryGitRepository,
    write_json,
)


class DiscoveryTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def test_declared_manifest_bounds_helper_and_mcp_record_discovery(self) -> None:
        workspace = self.fixture.workspace()
        helper = workspace / "nested" / "a" / "live-context.json"
        mcp = workspace / "nested" / "b" / "mcp_process.json"
        write_json(helper, {"kind": "helper"})
        write_json(mcp, {"kind": "mcp"})
        unrelated = workspace / "nested" / "c" / "unrelated.json"
        write_json(unrelated, {"metadata": {"pid": 9001}})
        write_json(
            workspace / "record-manifest.json",
            {
                "schema": "orchestrator-record-manifest/v1",
                "helper_paths": ["nested/a/live-context.json"],
                "mcp_paths": ["nested/b/mcp_process.json"],
            },
        )

        records = discover_run(workspace.parent, workspace, self.fixture.config)

        self.assertEqual([helper], [record.path for record in records.helper_records])
        self.assertEqual([mcp], [record.path for record in records.mcp_records])
        self.assertFalse(any(error.path == str(unrelated) for error in records.errors))

    def test_declared_record_rejects_arbitrary_nested_pid_ownership(self) -> None:
        workspace = self.fixture.workspace()
        unrelated = workspace / "nested" / "metadata.json"
        write_json(unrelated, {"metadata": {"pid": 9001, "created_utc": "2026-07-30T12:00:00Z"}})
        write_json(
            workspace / "record-manifest.json",
            {
                "schema": "orchestrator-record-manifest/v1",
                "helper_paths": [],
                "mcp_paths": [],
            },
        )
        records = discover_run(workspace.parent, workspace, self.fixture.config)
        self.assertEqual((), records.helper_records)
        self.assertEqual((), records.mcp_records)

    def test_record_cache_hits_unchanged_identity_and_invalidates_on_file_change(self) -> None:
        path = self.fixture.workspace() / "helper_process.json"
        write_json(path, {"kind": "helper", "version": 1})
        _clear_observation_caches()
        with patch(
            "orchestrator_harness.discovery.read_stable", wraps=read_stable
        ) as read:
            first = _json_record(path, self.fixture.config)
            second = _json_record(path, self.fixture.config)
            self.assertIs(first, second)
            self.assertEqual(1, read.call_count)
            write_json(path, {"kind": "helper", "version": 2, "changed": True})
            changed = _json_record(path, self.fixture.config)
            self.assertEqual(2, read.call_count)
            self.assertNotEqual(first.stable.sha256, changed.stable.sha256)

    def test_unchanged_coding_result_uses_git_fingerprint_cache_and_can_revalidate(self) -> None:
        run_root = self.fixture.workspace().parent
        repository = TemporaryGitRepository.create(run_root)
        (run_root / ".gitignore").write_text(".agent-workspace/\n", encoding="utf-8")
        repository.git("add", ".gitignore")
        repository.git("commit", "-m", "ignore runtime")
        workspace = run_root / ".agent-workspace"
        status = workspace / "coding-status.json"
        write_json(
            status,
            {
                "schema": "orchestrator-lane-controller/v1",
                "invocation_schema": "orchestrator-coding-invocation/v1",
                "state": "CODEX_EXITED",
                "declared_lane_id": "coding:one",
                "worker_invocation_id": "worker-1",
                "repository": repository.declaration(),
            },
        )
        write_json(
            workspace / "RESULT.json",
            {
                "schema": "orchestrator-lane-result/v1",
                "lane_id": "coding:one",
                "worker_invocation_id": "worker-1",
                "branch": repository.branch,
                "commit": repository.head,
                "outcome": "PASS",
                "summary": "done",
                "checks": [],
            },
        )
        with patch(
            "orchestrator_harness.discovery.validate_coding_result",
            wraps=validate_coding_result,
        ) as validate:
            self.assertIsNotNone(discover_run(run_root, workspace, self.fixture.config).result)
            self.assertIsNotNone(discover_run(run_root, workspace, self.fixture.config).result)
            self.assertEqual(1, validate.call_count)
            self.assertIsNotNone(
                discover_run(
                    run_root,
                    workspace,
                    self.fixture.config,
                    revalidate_results=True,
                ).result
            )
            self.assertEqual(2, validate.call_count)
            (run_root / "tracked.txt").write_text("dirty\n", encoding="utf-8")
            dirty = discover_run(run_root, workspace, self.fixture.config)
            self.assertIsNone(dirty.result)
            self.assertEqual(3, validate.call_count)


if __name__ == "__main__":
    unittest.main()
