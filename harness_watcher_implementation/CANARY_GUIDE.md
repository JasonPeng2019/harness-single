# Board-free canary guide — final

Run:

```powershell
python -m harness_watcher_implementation.tests.run_lifecycle_canary
```

The lifecycle canary is board-free and exercises duplicate-start rejection, exact PID/creation identity reporting, cooperative stop, and owner-identity loss for the watcher subprocess.
