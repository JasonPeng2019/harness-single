from __future__ import annotations
import hashlib, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ORDER = (
    "STOP_ASSIGNING",
    "CHECKPOINT_REQUESTED",
    "PAUSED",
    "REPAIRED",
    "RESUMED",
    "RESOLVED",
)


def _path(root: Path) -> Path:
    return root / "watcher" / "alerts.json"


def _load(root: Path) -> dict[str, Any]:
    p = _path(root)
    if not p.exists():
        return {"schema": "harness-watcher-alerts/v1", "alerts": []}
    raw = json.loads(p.read_text(encoding="utf-8"))
    if raw.get("schema") != "harness-watcher-alerts/v1" or not isinstance(
        raw.get("alerts"), list
    ):
        raise ValueError("invalid alert state")
    return raw


def _save(root: Path, value: dict[str, Any]) -> None:
    p = _path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    temp = p.with_suffix(".tmp")
    temp.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temp.replace(p)


def save_alert(
    root: Path,
    verdict: Any,
    packet: dict[str, Any],
    evaluator_identity: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    if not verdict.defect:
        return None
    signature = hashlib.sha256(
        json.dumps(
            {
                "kind": verdict.kind,
                "summary": verdict.summary,
                "evidence": verdict.evidence,
            },
            sort_keys=True,
            default=list,
        ).encode()
    ).hexdigest()
    state = _load(root)
    for alert in state["alerts"]:
        if alert["signature"] == signature and alert["recovery_state"] != "RESOLVED":
            return alert
    alert = {
        "alert_id": "hwa-" + signature[:24],
        "event_id": "hwa-" + signature,
        "signature": signature,
        "type": "HARNESS_WATCHER_ALERT",
        "severity": verdict.severity,
        "summary": verdict.summary,
        "kind": verdict.kind,
        "implicated": list(verdict.implicated),
        "evidence": list(verdict.evidence),
        "packet_id": packet["packet_id"],
        "evaluator_identity": dict(evaluator_identity or {}),
        "acknowledged": False,
        "recovery_state": None,
        "recovery_history": [],
    }
    state["alerts"].append(alert)
    _save(root, state)
    return alert


def unresolved_alerts(repository_root: Path) -> list[dict[str, Any]]:
    try:
        state = _load(repository_root / "harness_watcher")
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    return [a for a in state["alerts"] if a.get("recovery_state") != "RESOLVED"]


def acknowledge(root: Path, event_id: str) -> bool:
    state = _load(root)
    for a in state["alerts"]:
        if a.get("event_id") == event_id:
            a["acknowledged"] = True
            _save(root, state)
            return True
    return False


def transition(root: Path, alert_id: str, state_name: str) -> dict[str, Any]:
    if state_name not in ORDER:
        raise ValueError("unknown recovery state")
    state = _load(root)
    for a in state["alerts"]:
        if a["alert_id"] == alert_id:
            expected = (
                ORDER[0]
                if a["recovery_state"] is None
                else ORDER[ORDER.index(a["recovery_state"]) + 1]
                if a["recovery_state"] != ORDER[-1]
                else None
            )
            if expected != state_name:
                raise ValueError("recovery transition out of order")
            a["recovery_state"] = state_name
            a["recovery_history"].append(
                {"state": state_name, "at_utc": datetime.now(timezone.utc).isoformat()}
            )
            _save(root, state)
            return a
    raise ValueError("unknown alert")
