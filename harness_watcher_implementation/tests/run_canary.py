"""Board-free Harness Watcher canary; writes all retained evidence under test_results."""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from harness_watcher_implementation import settings
from harness_watcher_implementation.config import ObservedSource, WatcherConfig
from harness_watcher_implementation.evaluator import TerraHighEvaluator
from harness_watcher_implementation.poller import poll
from orchestrator_harness.cli import scan_command, watch_once
from orchestrator_harness.config import HarnessConfig, load_config
from orchestrator_harness.tests.support import launch_synthetic_controller, write_json

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "harness_watcher_implementation" / "test_results"
HARNESS_ROOT = ROOT / "orchestrator_harness"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(mode: str, active: bool) -> dict[str, object]:
    destination = RESULTS / f"canary_{mode}"
    shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir(parents=True)
    schema = destination / "strict-verdict.schema.json"
    schema.write_text(json.dumps({
        "type": "object", "additionalProperties": False,
        "required": ["defect", "kind", "severity", "summary", "implicated", "evidence"],
        "properties": {
            "defect": {"type": "boolean"}, "kind": {"type": ["string", "null"]},
            "severity": {"enum": ["warning", "error", "critical"]}, "summary": {"type": "string"},
            "implicated": {"type": "array", "items": {"type": "string"}},
            "evidence": {"type": "array", "items": {"type": "object", "additionalProperties": False,
                "required": ["path", "sha256", "offset"], "properties": {"path": {"type": "string"}, "sha256": {"type": "string"}, "offset": {"type": "integer"}}}},
        },
    }), encoding="utf-8")
    real_terra_high = ("codex", "exec", "--ephemeral", "-m", "gpt-5.6-terra", "-c", 'model_reasoning_effort="high"', "--output-schema", str(schema), "-")
    accepted_state = ROOT / ".agent-workspace" / "CURRENT_SUITE_STATE.json"
    before = sha(accepted_state)
    started = time.monotonic()
    integration_runtime = ROOT / "harness_watcher"
    integration_runtime_preexisted = integration_runtime.exists()

    # First run the real current suite record configuration without writes.
    actual_config = load_config(HARNESS_ROOT / "suite-restart-watch.json")
    actual_stream = (destination / "actual-suite-scan.json").open("w", encoding="utf-8")
    try:
        actual_exit = scan_command(actual_config, stream=actual_stream)
    finally:
        actual_stream.close()

    # The worker is isolated beneath test_results and makes no MCP/hardware call.
    suite = destination / "board_free_suite"
    workspace = suite / "runs" / "T00_watcher_canary" / ".agent-workspace"
    workspace.mkdir(parents=True)
    harness_config = HarnessConfig(
        config_path=destination / "synthetic-config.json",
        harness_root=HARNESS_ROOT,
        suite_root=suite,
        run_globs=("runs/*",), workspace_relpath=".agent-workspace",
        output_dir=HARNESS_ROOT / f".watcher-canary-{mode}-state",
        poll_interval_seconds=0.05, watch_timeout_seconds=5,
        request_warning_seconds=120, request_critical_seconds=30,
        process_start_tolerance_seconds=2, max_json_bytes=4_000_000,
        max_jsonl_tail_bytes=512_000, stable_read_retries=4,
        stable_read_delay_seconds=0.01, manager_review_interval_seconds=300,
        lane_no_progress_seconds=600, manager_heartbeat_timeout_seconds=420,
    )
    shutil.rmtree(harness_config.output_dir, ignore_errors=True)
    controller = launch_synthetic_controller(workspace)
    try:
        request = workspace / "permission-requests" / "synthetic-request.json"
        deadline = time.monotonic() + 10
        while not request.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        if not request.exists():
            raise RuntimeError("synthetic board-free controller did not publish its request")
        if active:
            manager_log = workspace / "manager-events.jsonl"
            harness_log = workspace / "synthetic-harness-events.jsonl"
            manager_log.write_text('{"event":"heartbeat","actor":"manager"}\n', encoding="utf-8")
            harness_log.write_text('{"event":"observation","actor":"harness"}\n', encoding="utf-8")
            watcher_config = WatcherConfig(
                repository_root=ROOT, runtime_root=destination / "harness_watcher",
                observed_log_roots=(manager_log, harness_log, workspace / "synthetic_codex.jsonl"),
                observed_sources=(
                    ObservedSource(manager_log, "orchestrator", "synthetic-manager"),
                    ObservedSource(harness_log, "harness", "synthetic-harness"),
                    ObservedSource(workspace / "synthetic_codex.jsonl", "subagent", "synthetic-agent"),
                ),
            )
            watcher_result = poll(watcher_config, TerraHighEvaluator(real_terra_high))
        else:
            watcher_result = {"disabled": True}
        event_out = (destination / "harness-events.jsonl").open("w", encoding="utf-8")
        try:
            settings.harness_watcher_active = active
            event_exit, events = watch_once(harness_config, no_write=False, stream=event_out)
        finally:
            event_out.close()
        request_hash = sha(request)
        write_json(workspace / "permission-requests" / "synthetic-request.relay.json", {
            "decision": "approved", "request_sha256": request_hash,
            "run_id": "synthetic-run", "session_id": "synthetic-session",
        })
        controller.wait(timeout=20)
        stdout, stderr = controller.communicate(timeout=1)
        after_out = (destination / "harness-after-events.jsonl").open("w", encoding="utf-8")
        try:
            after_exit, after_events = watch_once(harness_config, no_write=False, stream=after_out)
        finally:
            after_out.close()
        if controller.returncode != 0:
            raise RuntimeError(f"synthetic controller failed: {controller.returncode} {stdout} {stderr}")
        result = {
            "mode": mode, "active": active, "actual_suite_scan_exit": actual_exit,
            "synthetic_watch_exit": event_exit, "synthetic_after_exit": after_exit,
            "initial_events": [item.get("type") for item in events],
            "after_events": [item.get("type") for item in after_events],
            "checkpoint": (workspace / "PARALLEL_CHECKPOINT.md").is_file(),
            "controller_exit_code": controller.returncode,
            "terminal_status": json.loads((workspace / "synthetic_controller.status.json").read_text(encoding="utf-8")).get("state"),
            "watcher_poll_alert": getattr(watcher_result.get("alert"), "alert_id", None) if active else None,
            "watcher_runtime_created": (destination / "harness_watcher").exists(),
            "four_way_log_paths": {
                category: (destination / "harness_watcher" / category / "events.jsonl").is_file()
                for category in ("orchestrator", "harness", "watcher")
            } | {"subagent": any((destination / "harness_watcher" / "subagents").glob("*/events.jsonl"))},
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "accepted_state_sha256_before": before,
            "accepted_state_sha256_after": sha(accepted_state),
        }
        if active and integration_runtime.exists():
            shutil.copytree(integration_runtime, destination / "harness-integration-runtime")
        (destination / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        settings.harness_watcher_active = True
        if controller.poll() is None:
            controller.terminate()
            controller.wait(timeout=5)
        # Harness's ephemeral output is not experiment evidence; result copies stay below test_results.
        shutil.rmtree(harness_config.output_dir, ignore_errors=True)
        if active and not integration_runtime_preexisted:
            shutil.rmtree(integration_runtime, ignore_errors=True)


if __name__ == "__main__":
    all_results = [run("active", True), run("disabled", False)]
    (RESULTS / "canary_summary.json").write_text(json.dumps(all_results, indent=2) + "\n", encoding="utf-8")
    (RESULTS / "canary_cleanup.json").write_text(json.dumps({"root_harness_watcher_residue": (ROOT / "harness_watcher").exists()}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(all_results, indent=2))
