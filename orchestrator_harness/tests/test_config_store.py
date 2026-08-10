from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import json
import os
import unittest
from unittest.mock import patch

from orchestrator_harness.cli import _store
from orchestrator_harness.config import ConfigError, load_config, path_identity, same_path
from orchestrator_harness.stable_io import (
    PathSafetyError,
    SafeOutput,
    UnstableReadError,
    read_stable,
)
from orchestrator_harness.tests.support import SuiteFixture, write_json


class ConfigAndStoreTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def test_path_identity_uses_platform_case_and_separator_rules(self) -> None:
        left = self.fixture.suite_root / "Runs" / "lane"
        right = self.fixture.suite_root / "Runs" / "lane" / "."
        self.assertEqual(path_identity(left), path_identity(right))
        if os.name == "nt":
            self.assertTrue(same_path(str(left).upper(), str(left).lower()))
        else:
            self.assertNotEqual(path_identity(str(left).upper()), path_identity(str(left).lower()))

    def test_config_accepts_bounded_record_declarations(self) -> None:
        write_json(
            self.fixture.config_path,
            {
                "suite_root": str(self.fixture.suite_root),
                "run_globs": ["runs/*"],
                "record_paths": ["nested/helper.json"],
                "record_manifests": ["record-manifest.json"],
            },
        )
        config = load_config(self.fixture.config_path, harness_root=self.fixture.harness_root)
        self.assertEqual(("nested/helper.json",), config.record_paths)
        self.assertEqual(("record-manifest.json",), config.record_manifests)

    def test_config_rejects_parent_glob(self) -> None:
        write_json(
            self.fixture.config_path,
            {
                "suite_root": str(self.fixture.suite_root),
                "run_globs": ["../*"],
                "output_dir": ".state",
            },
        )
        with self.assertRaises(ConfigError):
            load_config(
                self.fixture.config_path, harness_root=self.fixture.harness_root
            )

    def test_config_rejects_wide_pid_creation_tolerance(self) -> None:
        write_json(
            self.fixture.config_path,
            {
                "suite_root": str(self.fixture.suite_root),
                "run_globs": ["fresh-experiments/*"],
                "output_dir": ".state",
                "process_start_tolerance_seconds": 3,
            },
        )
        with self.assertRaises(ConfigError):
            load_config(
                self.fixture.config_path, harness_root=self.fixture.harness_root
            )

    def test_cli_permits_only_multi_agent_logs_outside_harness_root(self) -> None:
        allowed = self.fixture.suite_root / "multi-agent-logs" / "epoch"
        write_json(
            self.fixture.config_path,
            {"suite_root": str(self.fixture.suite_root), "run_globs": ["runs/*"], "output_dir": str(allowed)},
        )
        config = load_config(self.fixture.config_path, harness_root=self.fixture.harness_root)
        self.assertEqual(allowed, _store(config).output_root)

        for forbidden in (
            self.fixture.suite_root / "arbitrary-sibling",
            self.fixture.suite_root / "fresh-experiments" / "epoch",
        ):
            write_json(
                self.fixture.config_path,
                {"suite_root": str(self.fixture.suite_root), "run_globs": ["runs/*"], "output_dir": str(forbidden)},
            )
            config = load_config(self.fixture.config_path, harness_root=self.fixture.harness_root)
            with self.assertRaises(PathSafetyError):
                _store(config)

    def test_output_rejects_observed_root(self) -> None:
        store = SafeOutput(
            harness_root=self.fixture.root,
            output_root=self.fixture.suite_root / "fresh-experiments" / "state",
            forbidden_roots=(self.fixture.suite_root / "fresh-experiments",),
        )
        with self.assertRaises(PathSafetyError):
            store.prepare()

    def test_output_allows_sibling_of_observed_root(self) -> None:
        parent = self.fixture.harness_root / "run"
        observed = parent / "synthetic-suite" / ".agent-workspace"
        observed.mkdir(parents=True)
        store = SafeOutput(
            harness_root=self.fixture.harness_root,
            output_root=parent / "watcher-state",
            forbidden_roots=(observed,),
        )
        store.prepare()
        self.assertTrue(store.output_root.is_dir())

    def test_output_rejects_reparse_component(self) -> None:
        target = self.fixture.harness_root / "link" / "state"
        with patch("orchestrator_harness.stable_io._is_reparse") as mocked:
            mocked.side_effect = lambda path: path.name == "link"
            (self.fixture.harness_root / "link").mkdir()
            store = SafeOutput(
                harness_root=self.fixture.harness_root,
                output_root=target,
                forbidden_roots=(),
            )
            with self.assertRaises(PathSafetyError):
                store.prepare()

    @unittest.skipUnless(os.name == "nt", "Windows ADS rule")
    def test_output_rejects_ads(self) -> None:
        store = SafeOutput(
            harness_root=self.fixture.harness_root,
            output_root=self.fixture.harness_root / "state:stream",
            forbidden_roots=(),
        )
        with self.assertRaises(PathSafetyError):
            store.prepare()

    def test_output_root_replacement_is_detected(self) -> None:
        store = SafeOutput(
            harness_root=self.fixture.harness_root,
            output_root=self.fixture.harness_root / "state",
            forbidden_roots=(),
        )
        store.prepare()
        original = self.fixture.harness_root / "state"
        moved = self.fixture.harness_root / "state-old"
        original.rename(moved)
        original.mkdir()
        with self.assertRaises(PathSafetyError):
            store.atomic_json(store.snapshot_path, {"x": 1})

    def test_event_append_precedes_cursor_and_survives_injected_crash(self) -> None:
        store = SafeOutput(
            harness_root=self.fixture.harness_root,
            output_root=self.fixture.harness_root / "state",
            forbidden_roots=(),
            fail_after_event_append=True,
        )
        store.prepare()
        event = {"event_id": "same", "identity": "x", "type": "X"}
        with self.assertRaises(RuntimeError):
            store.commit(snapshot={"n": 1}, events=[event], conditions={"x": event})
        self.assertTrue(store.events_path.exists())
        self.assertFalse(store.snapshot_path.exists())
        logged = json.loads(store.events_path.read_text(encoding="utf-8").strip())
        self.assertEqual("same", logged["event_id"])

    def test_unstable_stat_read_stat_fails_closed(self) -> None:
        path = self.fixture.harness_root / "changing.json"
        path.write_text('{"x": 1}', encoding="utf-8")
        with patch(
            "orchestrator_harness.stable_io._signature",
            side_effect=[(1, 1, 1, 1), (2, 2, 2, 2)] * 2,
        ):
            with self.assertRaises(UnstableReadError):
                read_stable(
                    path,
                    max_bytes=100,
                    retries=2,
                    delay_seconds=0,
                )


if __name__ == "__main__":
    unittest.main()
