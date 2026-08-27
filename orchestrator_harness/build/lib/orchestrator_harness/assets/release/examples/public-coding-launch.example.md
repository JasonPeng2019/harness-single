# Public coding launch

The operator boundary records the exact detached controller identity. The
controller then validates the coding invocation, launches one provider child,
publishes lifecycle/result events, and owns claims and terminal cleanup.

```powershell
python -m orchestrator_harness.operator_launch `
  --receipt <run>/.agent-workspace/operator-launch.json `
  --label coding-lane `
  --role coding-lane-controller `
  --cwd <harness-root> `
  -- python -m orchestrator_harness.lane_controller <run>/.agent-workspace/invocation.json
```

Observe the run with the native diagnostic path. A manager waits on
`watch --until-actionable`, handles the returned envelope, and acknowledges
the envelope's top-level `event_id`; a worker `data.signal_id` is not an
acknowledgement ID. The receipt's PID plus creation identity and the
controller's lifecycle registry are the cleanup evidence. Stop cooperatively
and verify that exact identity before closing a run.

The disposable fake-agent journey in
`orchestrator_harness.tests.test_s6_public_release` uses this same public
operator/controller path. It never launches a test-only controller, WSL,
hardware, MCP, provider, credential, or endpoint.
