from __future__ import annotations
from pathlib import Path
from typing import Any
from .events import conditions_from_snapshot, stable_condition

def merge_watcher_conditions(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    conditions = conditions_from_snapshot(snapshot)
    try:
        from harness_watcher_implementation import settings
        from harness_watcher_implementation.state import unresolved_alerts
        from harness_watcher_implementation.logging import log
    except ImportError:
        return conditions
    if not settings.harness_watcher_active: return conditions
    root = Path(__file__).resolve().parents[1]
    for alert in unresolved_alerts(root):
        event=stable_condition("harness-watcher:"+alert["alert_id"],"HARNESS_WATCHER_ALERT",alert.get("severity","warning"), {"alert_id":alert["alert_id"],"event_id":alert["event_id"],"summary":alert.get("summary"),"implicated":alert.get("implicated",[]),"evidence":alert.get("evidence",[]),"required_response":["STOP_ASSIGNING","CHECKPOINT_REQUESTED","PAUSED","REPAIRED","RESUMED","RESOLVED"],"model":"gpt-5.6-terra","reasoning":"high"},event_id_data={"watcher_event_id":alert["event_id"]})
        event["event_id"]=alert["event_id"]; conditions[event["identity"]]=event
    log(root/"harness_watcher","harness","OBSERVED_WATCHER_ALERTS",{"count":len(conditions)})
    return conditions

def acknowledge_watcher_event(event_id: str) -> bool:
    try:
        from harness_watcher_implementation import settings
        from harness_watcher_implementation.state import acknowledge
    except ImportError:
        return False
    if not settings.harness_watcher_active:return False
    return acknowledge(Path(__file__).resolve().parents[1]/"harness_watcher",event_id)
