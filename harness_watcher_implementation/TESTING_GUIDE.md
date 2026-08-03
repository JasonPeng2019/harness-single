# Harness Watcher testing guide - legacy firmware compatibility

The MCP-Trial-3, counted-sprint, canary, and production-source-routing instructions in this guide
are retained for the supported legacy firmware path. They are not required for current ordinary
coding operation or its disposable fixture; use `QUICK_START.md` for the general coding path.

Run from `MCP-Trial-3`:

```powershell
$env:PYTHONUTF8='1'
python -m compileall -q harness_watcher_implementation orchestrator_harness
python -m unittest discover -s harness_watcher_implementation/tests -t . -v
python -m unittest discover -s orchestrator_harness/tests -t . -q
python -m harness_watcher_implementation.tests.run_lifecycle_canary
python -m harness_watcher_implementation.tests.run_canary
python -m harness_watcher_implementation.tests.run_defect_canary
```

Expected baseline: 14 focused passed; harness 108 passed/1 skipped. For production-source routing, use a fresh epoch configuration that declares the current manager log, managed-harness event JSONL, and lane event JSONL. The example documents the required role/source-ID form. Do not use hardware or alter accepted experiment evidence for these checks.
