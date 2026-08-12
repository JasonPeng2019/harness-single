# Dual-path manager example

This is an operator recipe, not a second runner. The native harness remains the only discovery and
event-delivery surface. It shows an ordinary coding V1 lane and a retained schema-less firmware lane
sharing one fresh observer epoch without changing either contract.

```powershell
$epoch = "dual-path-$(Get-Date -Format yyyyMMdd-HHmmss)"
New-Item -ItemType Directory -Force "local-config" | Out-Null
Copy-Item examples/harness.example.json "local-config/$epoch.json"
# Edit suite_root, run_globs, and output_dir in local-config/$epoch.json before proceeding.
python -m orchestrator_harness --config "local-config/$epoch.json" scan --no-write

# Start each explicitly prepared invocation in separate manager-launched terminals. The manager chooses
# whether either invocation is eligible; this recipe is not a wrapper or scheduler.
python -m orchestrator_harness.lane_controller C:/absolute/path/coding.invocation.json
# In the other terminal:
python -m orchestrator_harness.lane_controller C:/absolute/path/legacy-firmware.invocation.json

# Native blocking discovery is the sole manager wake mechanism.
python -m orchestrator_harness --config "local-config/$epoch.json" watch --until-actionable --timeout 60
# After handling and verifying the returned envelope, the bound S3 manager router acknowledges it:
router.acknowledge("<top-level-event-id>", binding=router.registration)
```

Use `examples/coding.invocation.example.json` unchanged for the coding lane. Use
`examples/legacy-firmware.invocation.example.json` as the retained schema-less policy-bound shape;
replace its paths and policy facts, but do not add coding V1 fields to it. Legacy firmware remains
subject to its existing board, relay, lease, MCP, authorization, and physical-cleanup rules.
