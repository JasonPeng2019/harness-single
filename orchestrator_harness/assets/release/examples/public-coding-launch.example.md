# Public harness v2 launch

The public boundary is the native operator launcher. Bootstrap prepares a
lane, launch starts its controller, and the controller loads the registered
provider binding directly.

```powershell
python -m orchestrator_harness.operator_launch harness setup
python -m orchestrator_harness.operator_launch lane bootstrap `
  --lane-id coding-lane `
  --provider codex `
  --model <configured-model> `
  --provider-option reasoning_effort=<configured-effort> `
  --provider-option service_tier=<configured-tier> `
  --task-card <task-card.json>
python -m orchestrator_harness.operator_launch lane launch --lane-id coding-lane
```

Observe with `watch --until-actionable`. In managed mode it returns only a
durable queue wake and its top-level `event_id`; repeated waits in the same ROOT
session suppress that unresolved event. A manager acknowledges the envelope's
top-level `event_id`; a worker request's `data.signal_id` is not an
acknowledgement ID. Plain mode has no queue. No detached receipt or candidate
launcher route is part of the v2 public interface.
