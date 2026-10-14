# Harness Watcher testing guide

These checks cover the optional watcher around the canonical coding workflow. They are diagnostic
only and do not launch providers, manage lanes, or modify accepted evidence; use `QUICK_START.md`
for the general coding path.

Run from the repository root:

```powershell
$env:PYTHONUTF8='1'
python -m compileall -q harness_watcher_implementation orchestrator_harness
python -m unittest discover -s harness_watcher_implementation/tests -t . -v
python -m unittest discover -s orchestrator_harness/tests -t . -q
python -m harness_watcher_implementation.tests.run_lifecycle_canary
```

Use a fresh epoch configuration that declares the current manager log, managed-harness event JSONL,
and lane event JSONL. The example documents the required role/source-ID form. Do not alter accepted
evidence for these checks.
