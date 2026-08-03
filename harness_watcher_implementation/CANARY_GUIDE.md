# Board-free canary guide — final

Run:

```powershell
python -m harness_watcher_implementation.tests.run_canary
python -m harness_watcher_implementation.tests.run_defect_canary
python -m harness_watcher_implementation.tests.run_lifecycle_canary
```

The healthy canary scans actual suite records read-only and runs an isolated board-free controller. Enabled mode makes a real Terra-high strict evaluator call and records separated manager/harness/watcher/subagent trees; disabled mode follows the original harness path and leaves no watcher runtime. The defect canary proves durable alert delivery and ordered manager recovery. The lifecycle canary proves duplicate/identity/stop/owner-loss behavior. Inspect `test_results/final_cleanup.json` and `final_process_cleanup.json` afterward; all residue/identity values must be false.
