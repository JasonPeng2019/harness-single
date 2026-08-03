"""Exercise a synthetic meaningful loop through alert delivery and safe recovery."""
from __future__ import annotations

import json
import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path

from harness_watcher_implementation.config import WatcherConfig
from harness_watcher_implementation.evaluator import FakeEvaluator
from harness_watcher_implementation.poller import poll
from harness_watcher_implementation.state import transition
from orchestrator_harness.notifications import select_actionable
from orchestrator_harness.watcher_integration import merge_watcher_conditions

ROOT = Path(__file__).resolve().parents[2]
DESTINATION = ROOT / "harness_watcher_implementation" / "test_results" / "defect_canary"
RUNTIME = ROOT / "harness_watcher"


def main() -> None:
    if RUNTIME.exists():
        raise RuntimeError("refuse to overwrite a pre-existing watcher runtime")
    shutil.rmtree(DESTINATION, ignore_errors=True)
    DESTINATION.mkdir(parents=True)
    log = DESTINATION / "synthetic-agent.jsonl"
    log.write_text('{"stage":"build"}\n{"stage":"test"}\n{"stage":"build","meaning":"regression loop"}\n', encoding="utf-8")
    observed_sha = hashlib.sha256(log.read_bytes()).hexdigest()
    verdict = {
        "defect": True, "kind": "loop", "severity": "error",
        "summary": "synthetic agent regressed build -> test -> build",
        "implicated": ["synthetic-board-free-agent"],
        "evidence": [{"path": str(log), "sha256": observed_sha, "offset": 0}],
    }
    try:
        outcome = poll(WatcherConfig(ROOT, RUNTIME, (log,)), FakeEvaluator(verdict))
        alert = outcome["alert"]
        conditions = merge_watcher_conditions({"lanes": [], "requests": []})
        selected = select_actionable(
            conditions, {"lanes": [], "requests": []},
            observed_at=datetime.now(timezone.utc), acknowledged_event_ids=set(),
        )
        history = []
        for state in ("STOP_ASSIGNING", "CHECKPOINT_REQUESTED", "PAUSED", "REPAIRED", "RESUMED", "RESOLVED"):
            transition(RUNTIME, alert["alert_id"], state)
            history.append(state)
        final_conditions = merge_watcher_conditions({"lanes": [], "requests": []})
        result = {
            "alert_id": alert["alert_id"], "event_id": alert["event_id"],
            "selected_type": selected and selected["type"],
            "selected_priority": "highest (HARNESS_WATCHER_ALERT)",
            "recovery_history": history,
            "resolved_alert_absent_from_harness_conditions": alert["event_id"] not in {
                item.get("event_id") for item in final_conditions.values()
            },
            "hardware_actions": 0, "subagent_process_actions": 0,
        }
        shutil.copytree(RUNTIME, DESTINATION / "runtime")
        (DESTINATION / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        shutil.rmtree(RUNTIME, ignore_errors=True)


if __name__ == "__main__":
    main()
