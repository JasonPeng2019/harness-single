# Firmware V2 acceptance kit

This is the S2 static test medium. It does not launch MCP, use a serial port, invoke pyOCD, or touch hardware. The candidate-owned `python -m firmware_acceptance.c3_harness` facade accepts only signed immutable attempt-local requests, materializes targets, dispatches target workers through the existing lane controller, and retains controller-owned sessions. It exposes no process, endpoint, credential, or physical capability to `F.C3.O`.

`ACCEPTANCE_MANIFEST.json` pins the candidate, clean server revision, resource-mirror manifest, fixtures, datasheets, and toolchain locks. `LANE_TEMPLATES.json` requires separate process, `.firm`, artifact, log, probe/profile/target, and rediscovered-route values for each board. Its empty worker environment is intentional: target workers and `F.C3.O` receive no endpoint, command, credential, secret, or physical-launch capability.

The charter adapts only H00/H01/H02/H05, S10-S13, A21/A23/A24, and D30-D36. A20-A26 except those apps, B01-B39/Q40, Q41 soaks, destructive/try-last actions, UI/database work, extra boards, and endurance expansion are non-gating. P.05 remains DIO2-independent; no GPIO mapping is inferred.

`seed/` is deliberately prospective and contains exactly its manifest plus the four manifest-bound files. It contains no application source, tests, build output, or mutable state. The included synthetic validator is used only to test controller admission and deadline/policy failure handling.
