# Public harness v2 launch

The public boundary is the native operator launcher. Bootstrap prepares a
lane, launch starts its controller, and the controller loads the registered
provider binding directly.

```powershell
python -m orchestrator_harness.operator_launch harness setup
python -m orchestrator_harness.operator_launch lane bootstrap `
  --lane-id coding-lane `
  --provider codex `
  --model gpt-5.6-terra `
  --task-card <task-card.json>
python -m orchestrator_harness.operator_launch lane launch --lane-id coding-lane
```

Observe with `watch --until-actionable`. A manager acknowledges the envelope's
top-level `event_id`; a worker request's `data.signal_id` is not an
acknowledgement ID. No detached receipt or candidate launcher route is part of
the v2 public interface.
