"""Claude whole-tree usage, session deltas, and protected worker discovery."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HARNESS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(HARNESS))
sys.path.insert(0, str(HARNESS / "scripts"))
import token_ledger as ledger


class ClaudeUsageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    @staticmethod
    def write_json(path: Path, value) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def transcript(
        self,
        path: Path,
        *,
        session="session-1",
        inp=100,
        out=25,
        cache=None,
        model="claude-sonnet-4",
    ) -> None:
        child = {
            "type": "result",
            "subtype": "success",
            "session_id": "child-session",
            "parent_tool_use_id": "tool-1",
            "modelUsage": {"child-model": {"inputTokens": 500, "outputTokens": 500}},
        }
        counters = {"inputTokens": inp, "outputTokens": out}
        if cache is not None:
            counters["cacheReadInputTokens"] = cache
        events = [
            {"type": "system", "subtype": "init", "session_id": session},
            {"type": "assistant", "message": {"usage": {"output_tokens": 99999}}},
            child,
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "session_id": session,
                "usage": {"output_tokens": 99999},
                "modelUsage": {model: counters},
            },
        ]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
        )

    def root_task(
        self, *, first_out=25, second_out=None, second_session="session-1"
    ) -> Path:
        run = self.base / "run"
        workspace = self.base / "root-workspace"
        command = [
            "claude",
            "--print",
            "--model",
            "claude-sonnet-4",
            "--effort",
            "high",
        ]
        launch = {
            "schema": ledger.RUN_SCHEMA,
            "run_id": "fixture",
            "task": "fixture",
            "arm": "raw",
            "provider_id": "claude-code",
            "model": "claude-sonnet-4",
            "reasoning_effort": "high",
            "service_tier": None,
            "workspace": str(workspace),
            "started_at_utc": "2026-11-01T12:00:00Z",
            "command": command,
            "budget_tokens": None,
        }
        self.write_json(run / "launch.json", launch)
        self.write_json(
            run / "manifest.json",
            {
                **launch,
                "exit_code": 0,
                "elapsed_seconds": 8,
                "ended_at_utc": "2026-11-01T12:00:08Z",
            },
        )
        self.transcript(run / "claude-events.jsonl", out=first_out)
        if second_out is not None:
            second = run / "segments/0002"
            command2 = [*command, "--resume", second_session]
            launch2 = {
                **launch,
                "started_at_utc": "2026-11-01T12:01:00Z",
                "command": command2,
                "segment_number": 2,
                "resume_session_id": second_session,
            }
            self.write_json(second / "launch.json", launch2)
            self.write_json(
                second / "manifest.json",
                {
                    **launch2,
                    "exit_code": 0,
                    "elapsed_seconds": 5,
                    "ended_at_utc": "2026-11-01T12:01:05Z",
                },
            )
            self.transcript(
                second / "claude-events.jsonl",
                session=second_session,
                inp=200,
                out=second_out,
                cache=5,
            )
        task = {
            "schema": ledger.TASK_SCHEMA,
            "run_id": "fixture",
            "task": "fixture",
            "arm": "raw",
            "root_run_dir": str(run),
            "budget_tokens": None,
            "interruption_policy": "planned_resume" if second_out is not None else None,
        }
        self.write_json(run / "task.json", task)
        return run / "task.json"

    def test_inclusive_terminal_rollup_ignores_nested_and_placeholder_usage(self):
        path = self.base / "claude-events.jsonl"
        self.transcript(path, cache=7)
        result = ledger.extract_usage(path, provider_id="claude-code")
        self.assertEqual("ok", result["status"])
        self.assertEqual(
            (100, 25, 125),
            (
                result["input_tokens"],
                result["output_tokens"],
                result["comparison_total_tokens"],
            ),
        )
        self.assertEqual(7, result["cached_input_tokens"])
        self.assertEqual(
            "terminal_whole_tree_including_builtin_subagents", result["usage_scope"]
        )
        self.assertIn("usage_sha256", result["model_usage"]["claude-sonnet-4"])

    def test_result_usage_is_not_fallback_for_missing_whole_tree_rollup(self):
        path = self.base / "events.jsonl"
        self.transcript(path)
        raw = json.loads(path.read_text().splitlines()[-1])
        raw["modelUsage"] = {}
        path.write_text(
            path.read_text().splitlines()[0]
            + "\n"
            + path.read_text().splitlines()[1]
            + "\n"
            + path.read_text().splitlines()[2]
            + "\n"
            + json.dumps(raw)
            + "\n",
            encoding="utf-8",
        )
        self.assertEqual(
            "missing_usage",
            ledger.extract_usage(path, provider_id="claude-code")["status"],
        )

    def test_completed_bad_counters_and_failed_terminal_are_not_exact(self):
        path = self.base / "events.jsonl"
        self.transcript(path)
        events = path.read_text().splitlines()
        terminal = json.loads(events[-1])
        terminal["modelUsage"]["claude-sonnet-4"]["outputTokens"] = True
        path.write_text(
            "\n".join(events[:-1] + [json.dumps(terminal)]) + "\n", encoding="utf-8"
        )
        with self.assertRaises(ledger.LedgerError):
            ledger.extract_usage(path, provider_id="claude-code")
        terminal["modelUsage"]["claude-sonnet-4"]["outputTokens"] = 0
        terminal["is_error"] = True
        terminal["subtype"] = "error_during_execution"
        path.write_text(
            "\n".join(events[:-1] + [json.dumps(terminal)]) + "\n", encoding="utf-8"
        )
        self.assertEqual(
            "terminal_error_usage_unverified",
            ledger.extract_usage(path, provider_id="claude-code")["status"],
        )

    def test_partial_active_tail_is_in_flight_but_completed_tail_is_malformed(self):
        path = self.base / "events.jsonl"
        path.write_text(
            '{"type":"system","subtype":"init","session_id":"s"}\n{', encoding="utf-8"
        )
        self.assertEqual(
            "in_flight",
            ledger.extract_usage(path, provider_id="claude-code", completed=False)[
                "status"
            ],
        )
        with self.assertRaises(ledger.LedgerError):
            ledger.extract_usage(path, provider_id="claude-code")

    def test_completed_result_requires_top_level_init_session_binding(self):
        path = self.base / "events.jsonl"
        self.transcript(path)
        terminal = path.read_text(encoding="utf-8").splitlines()[-1]
        path.write_text(terminal + "\n", encoding="utf-8")
        with self.assertRaisesRegex(
            ledger.LedgerError, "no top-level init session binding"
        ):
            ledger.extract_usage(path, provider_id="claude-code")

    def indexed_terminal_stream(self, path: Path, *, final_out=25) -> None:
        """Portable regression shape captured from Claude CLI 2.1.319."""
        sid = "portable-session"
        base = {"inputTokens": 100, "outputTokens": 25}
        final = {"inputTokens": 100, "outputTokens": final_out}
        events = [
            {"type": "system", "subtype": "init", "session_id": sid},
            {"type": "system", "subtype": "task_notification", "session_id": sid},
            {"type": "system", "subtype": "init", "session_id": sid},
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "session_id": sid,
                "result_index": 0,
                "modelUsage": {"claude-sonnet-5-5": base},
            },
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "session_id": sid,
                "result_index": 1,
                "origin": {"kind": "task-notification"},
                "model_usage_since_last_result": {},
                "modelUsage": {"claude-sonnet-5-5": final},
            },
        ]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
        )

    def test_real_cli_indexed_rollups_and_repeated_live_init_are_one_segment(self):
        path = self.base / "claude-events.jsonl"
        self.indexed_terminal_stream(path)
        result = ledger.extract_usage(path, provider_id="claude-code")
        self.assertEqual("ok", result["status"])
        self.assertEqual("portable-session", result["thread_id"])
        self.assertEqual(
            (2, 5, 125),
            (
                result["terminal_records"],
                result["json_records"],
                result["comparison_total_tokens"],
            ),
        )
        self.assertEqual(
            25, result["model_usage"]["claude-sonnet-5-5"]["output_tokens"]
        )

    def test_provider_aware_summarize_reports_indexed_cli_rollup(self):
        path = self.base / "claude-events.jsonl"
        self.indexed_terminal_stream(path)
        report = subprocess.run(
            [
                sys.executable,
                str(HARNESS / "scripts" / "collect_usage.py"),
                "summarize",
                "--provider",
                "claude-code",
                "--events",
                str(path),
            ],
            cwd=self.base,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, report.returncode, report.stderr)
        self.assertEqual(125, json.loads(report.stdout)["comparison_total_tokens"])

    def test_indexed_terminal_replay_and_decreasing_counters_are_rejected(self):
        path = self.base / "claude-events.jsonl"
        self.indexed_terminal_stream(path)
        events = [json.loads(line) for line in path.read_text().splitlines()]
        events[-1]["result_index"] = 0
        path.write_text("".join(json.dumps(event) + "\n" for event in events))
        with self.assertRaisesRegex(ledger.LedgerError, "ambiguous multiple"):
            ledger.extract_usage(path, provider_id="claude-code")
        events[-1]["result_index"] = 1
        events[-1].pop("origin")
        path.write_text("".join(json.dumps(event) + "\n" for event in events))
        with self.assertRaisesRegex(ledger.LedgerError, "progression evidence"):
            ledger.extract_usage(path, provider_id="claude-code")
        events[-1]["origin"] = {"kind": "task-notification"}
        events[-1]["modelUsage"]["claude-sonnet-5-5"]["outputTokens"] = 24
        path.write_text("".join(json.dumps(event) + "\n" for event in events))
        with self.assertRaisesRegex(ledger.LedgerError, "counter decreased"):
            ledger.extract_usage(path, provider_id="claude-code")

    def test_appended_worker_resume_segments_select_their_own_terminal_rollups(self):
        first, second = self.base / "first.jsonl", self.base / "second.jsonl"
        combined = self.base / "combined.jsonl"
        self.transcript(first, session="same-session", inp=20, out=5)
        self.transcript(second, session="same-session", inp=40, out=9)
        combined.write_bytes(first.read_bytes() + second.read_bytes())
        one = ledger.extract_usage(combined, provider_id="claude-code", segment_index=1)
        two = ledger.extract_usage(combined, provider_id="claude-code", segment_index=2)
        self.assertEqual(
            (25, 49), (one["comparison_total_tokens"], two["comparison_total_tokens"])
        )
        self.assertEqual("ok", one["status"])
        self.assertEqual("ok", two["status"])

    def test_resume_charges_cumulative_delta_and_preserves_unknown_cache(self):
        task = self.root_task(first_out=25, second_out=40)
        snapshot = ledger.build_snapshot(task)
        self.assertEqual([], snapshot["errors"])
        self.assertEqual(240, snapshot["known_total_tokens"])
        self.assertEqual(
            [125, 115], [row["charged_tokens"] for row in snapshot["rows"]]
        )
        self.assertIsNone(snapshot["rows"][0]["cached_input_tokens"])
        self.assertEqual(5, snapshot["rows"][1]["cached_input_tokens"])
        self.assertEqual(15, snapshot["rows"][1]["charged_output_tokens"])
        self.assertEqual(
            100,
            snapshot["rows"][1]["charged_model_usage"]["claude-sonnet-4"][
                "input_tokens"
            ],
        )

    def test_resume_rejects_dropped_prior_model_breakdown(self):
        task = self.root_task(first_out=25, second_out=25)
        transcript = task.parent / "segments/0002/claude-events.jsonl"
        events = [
            json.loads(line)
            for line in transcript.read_text(encoding="utf-8").splitlines()
        ]
        events[-1]["modelUsage"] = {
            "claude-opus-4": {
                "inputTokens": 200,
                "outputTokens": 25,
                "cacheReadInputTokens": 5,
            }
        }
        transcript.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
        )

        snapshot = ledger.build_snapshot(task)
        self.assertTrue(
            any(
                "dropped prior model claude-sonnet-4" in error
                for error in snapshot["errors"]
            )
        )
        self.assertEqual(100, snapshot["rows"][1]["charged_tokens"])
        self.assertIsNone(
            snapshot["rows"][1]["charged_model_usage"]["claude-opus-4"]["input_tokens"]
        )
        self.assertFalse(snapshot["token_total_is_exact"])

    def test_resume_reconciles_a_legitimate_new_model_with_prior_model(self):
        task = self.root_task(first_out=25, second_out=40)
        transcript = task.parent / "segments/0002/claude-events.jsonl"
        events = [
            json.loads(line)
            for line in transcript.read_text(encoding="utf-8").splitlines()
        ]
        events[-1]["modelUsage"] = {
            "claude-sonnet-4": {"inputTokens": 150, "outputTokens": 25},
            "claude-opus-4": {"inputTokens": 50, "outputTokens": 15},
        }
        transcript.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
        )

        snapshot = ledger.build_snapshot(task)
        self.assertEqual([], snapshot["errors"])
        charged = snapshot["rows"][1]["charged_model_usage"]
        self.assertEqual(
            (50, 0),
            (
                charged["claude-sonnet-4"]["input_tokens"],
                charged["claude-sonnet-4"]["output_tokens"],
            ),
        )
        self.assertEqual(
            (50, 15),
            (
                charged["claude-opus-4"]["input_tokens"],
                charged["claude-opus-4"]["output_tokens"],
            ),
        )

    def test_resume_session_mismatch_is_rejected_and_decrease_is_not_zeroed(self):
        task = self.root_task(second_out=20, second_session="different-session")
        with self.assertRaisesRegex(ledger.LedgerError, "session differs"):
            ledger.build_snapshot(task)
        task = self.root_task(second_out=20)
        report = ledger.build_snapshot(task)
        self.assertTrue(
            any(
                "cumulative output_tokens decreased" in error
                for error in report["errors"]
            )
        )
        self.assertFalse(report["token_total_is_exact"])

    def test_final_archive_uses_provider_filename_and_metric_scoped_summary(self):
        task = self.root_task()
        archive = self.base / "archive"
        summary = ledger.finalize(task, archive)
        self.assertEqual(
            "claude_uncached_input_plus_output",
            summary["provider_metric_totals"]["claude-code"]["comparison_metric"],
        )
        self.assertTrue((archive / "invocations/0001/claude-events.jsonl").is_file())
        self.assertEqual("single_provider_metric", summary["aggregate_metric_scope"])

    def test_native_claude_worker_discovery_reads_protected_invocation_attempt_and_events(
        self,
    ):
        runtime = self.base / "workspace/.harness-runtime"
        worktree = runtime / "worktrees/epoch-1/worker-a"
        native = runtime / "epochs/epoch-1/lanes/worker-a"
        control = native / "runs/worker-run"
        transcript = control / "provider-transcript.jsonl"
        self.write_json(
            native / "lane.json",
            {
                "schema": "lane/v1",
                "lane_id": "worker-a",
                "worktree_path": str(worktree),
                "control_dir": str(control),
                "run_id": "worker-run",
            },
        )
        provider = {
            "id": "claude-code",
            "model": "claude-sonnet-4",
            "launch_config": {"effort": "high"},
        }
        self.write_json(
            control / "invocation.json",
            {
                "schema": "controller-invocation/v1",
                "lane_id": "worker-a",
                "run_id": "worker-run",
                "cwd": str(worktree),
                "provider": provider,
                "paths": {
                    "attempts": str(control / "controller.attempts.jsonl"),
                    "transcript": str(transcript),
                },
            },
        )
        attempts = [
            {"schema": "controller-attempts/v1"},
            {
                "attempt": 1,
                "run_id": "worker-run",
                "lane_id": "worker-a",
                "argv": [
                    "claude",
                    "--print",
                    "--model",
                    "claude-sonnet-4",
                    "--effort",
                    "high",
                ],
                "provider": provider,
                "session_id": "worker-session",
                "at": "2026-11-01T12:00:04Z",
                "exit_code": 0,
                "transcript_path": str(transcript),
            },
        ]
        (control / "controller.attempts.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in attempts), encoding="utf-8"
        )
        events = [
            {"event_type": "controller_started", "run_id": "worker-run"},
            {
                "event_type": "provider_started",
                "run_id": "worker-run",
                "ts": "2026-11-01T12:00:01Z",
            },
        ]
        (control / "controller.events.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in events), encoding="utf-8"
        )
        self.transcript(transcript, session="worker-session")
        transition_path = native / "resume-history/old-run/transition.json"
        self.write_json(
            transition_path,
            {
                "schema": "profile-transition/v1",
                "lane_id": "worker-a",
                "from_run_id": "old-run",
                "to_run_id": "worker-run",
                "old_provider": provider,
                "effective_new_provider": provider,
                "previous_session_id": "old-session",
            },
        )
        task = {
            "harness_runtime": str(runtime),
            "root_run_dir": str(self.base / "unused"),
            "run_id": "fixture",
            "epoch_id": "epoch-1",
        }
        root_row = {
            "invocation_id": "fixture.root",
            "started_at_utc": "2026-11-01T11:59:00Z",
            "ended_at_utc": "2026-11-01T12:05:00Z",
        }
        with patch.object(ledger, "_root_invocations", return_value=[root_row]):
            rows = ledger._worker_invocations(task)
        self.assertEqual(1, len(rows))
        self.assertEqual("claude-code", rows[0]["provider_id"])
        self.assertEqual("worker-session", rows[0]["session_id"])
        self.assertEqual("high", rows[0]["reasoning_effort"])
        self.assertIn(str(transition_path), rows[0]["source_manifest_paths"])


if __name__ == "__main__":
    unittest.main()
