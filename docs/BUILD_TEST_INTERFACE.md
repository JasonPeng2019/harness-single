# Worker build/test interface

The optional deterministic service lets a worker build and test an exact
committed revision and receive stdout, stderr, exit status and cleanup evidence
in its own lane. It does not launch agents, choose repairs or approve work.
Without local policy, bootstrap, resume and shutdown retain their normal behavior.

## Project configuration

Use `examples/public-check-policy.example.json` as a template for
`local-config/public-check-policy.json` in a configured harness copy. The example
is intentionally incomplete: the operator must provide a real adapter file and
its SHA-256. There is no bundled task backend or required language, build tool,
container image, dataset or project layout.

The `public-check-policy/v2` fields are:

- `adapter`: an absolute path to an operator-owned Python source file and its
  exact lowercase `sha256`. Keep it outside the worker-editable ROOT workspace,
  with no symlink or linked ancestor. Configure it before preparation; changing
  its contents or policy invalidates the service and ROOT prelaunch proof.
- `input_paths`: required committed repository-relative files or directories.
- `optional_input_paths`: committed files/directories included when present.
- `fixed_paths`: an optional immutable subset of the selected inputs, checked
  against the prepared ROOT revision. Ordinary source, build definitions and
  tests remain editable unless the project explicitly marks them fixed.
- `commands`: fixed argv arrays for `build` and `public`. `public` is the existing
  interface name for the project's test command; no special test visibility or
  scoring system is implied. Commands execute inside the adapter's environment.
- `allow_focused`: whether workers may supply focused command argv. Use `false`
  unless the backend safely contains arbitrary worker commands.
- `timeout_seconds`: 1..600 per check; `max_parallel_checks`: 1..4 concurrent checks.

Input selections cannot overlap, escape the repository, use Git pathspec patterns,
or include provider credentials or native harness metadata. Only those committed
inputs enter a temporary snapshot; links and special files are rejected. Untracked
and uncommitted project files do not silently enter a check.
Each path component must have one portable spelling: device names, streams,
control characters, trailing dots/spaces, case aliases and file/directory
conflicts are rejected before snapshot writes. A snapshot starts in an empty
owned directory and materializes literal committed blobs without export filters.

## Adapter contract

Provide these two functions in the trusted adapter:

```python
def preflight() -> dict:
    # Verify an already-prepared execution environment or raise on failure.
    return {"backend": "project-checks"}

def run_public_command(snapshot, command, *, timeout, cancel) -> dict:
    # Run command argv against snapshot in the project's restricted backend.
    # Enforce timeout, observe cancel, and clean up owned processes/resources.
    return {"stdout": "", "stderr": "", "exit_code": 0, "cleanup_proven": True}
```

`snapshot` is a temporary directory containing only the selected revision's
inputs. The adapter owns language/tool/image selection and execution isolation.
It must not expose host credentials, unrestricted Docker flags or host mounts to
worker commands. An adapter is trusted operator code, not worker-supplied code;
the general service is routing and evidence infrastructure, not a host-command
sandbox. Backend setup belongs to the operator, outside this interface. Preflight
must not start workers, initialize sandboxes, pull dependencies or retry setup.

Return a JSON object with string stdout/stderr, integer exit status, and an honest
boolean cleanup proof, plus optional backend diagnostics. Cancellation/timeouts
must stop only resources created by that invocation, using exact identities. A
failed cleanup prevents successful shutdown; a zero test exit is not cleanup proof.
The service verifies the adapter hash before import/execution. Use a self-contained
adapter or make it validate any extra runtime dependencies itself.
Adapters use normal registered Python modules and support standard-library types
such as dataclasses. A module is reused for the same path and source hash; every
check still verifies its pinned source. Parallel checks may call it concurrently.
Preflight and checks run in the service process, with only tool/OS environment
essentials forwarded. Arbitrary variables, API tokens and credential-bearing proxy
settings are omitted; its home/cache directories are owned runtime directories.

## Worker and lifecycle behavior

Operator preparation starts the service once before ROOT and records its policy
and exact process identity. Bootstrap/resume stage a lane-local client and route.
Workers commit the configured inputs, then request and consume feedback:

```powershell
python .agent-workspace/public-check.py request --check build
python .agent-workspace/public-check.py request --check public
python .agent-workspace/public-check.py wait --request-id <id> --timeout 30
```

Focused checks, when enabled, use `request --check focused -- <command argv>`.
Exit 75 means pending: wait on that same request within the active session.
Completed backend statuses remain intact in the feedback; the client maps a
completed exit 75 (or a native status outside 0..255) to CLI exit 2.
Build/test failures carry diagnostics; repair, commit and request a new check for
the new revision. Repeated checks are separate from process-launch retries.

The service binds request/lane/run/revision identities, deduplicates delivery,
rejects stale/resumed or changed-tip requests, and publishes feedback directly
to the worker. The staged client reads the lane's committed `HEAD` and refuses to
request a check while configured inputs are uncommitted; checks therefore run
against an exact committed revision, and a new commit needs a new request.
Historical registrations cannot overwrite another invocation's feedback. Native
bootstrap and resume stage a lane-local client and route bound to the current
run, so a resumed session receives a fresh route; a request naming a stale run or
route is refused and the lane must be refreshed through native preparation.
Shutdown cancels checks, proves adapter/service cleanup, then stops the native
monitor. ROOT resume revalidates the original service/prelaunch proof without
setup or an automatic service restart.
Routes retain the prepared worktree's filesystem identity. Requests are limited
to 64 KiB and read through one file handle with held, nonlinked ancestors; growth
after an initial size check cannot evade that bound. An old route without its
prepared identity must be replaced through fresh native lane preparation.

Publication takes runtime admission and the native lane operation lock and rechecks the current route,
run and original request bytes. An old in-flight job retains operator-only evidence
without writing into a resumed worker's response directory. Missing or malformed
expected service evidence leaves cleanup unproven and prevents CLOSED.
The service uses the native venv-aware launcher, so its launch PID identifies
the executing Python interpreter on Windows as well. Each service startup has
a nonce bound to its configuration and exact process identity. Delayed startup,
job feedback and STOPPED publication refuse a replacement owner.
Shutdown's CLOSED receipt pins the monitor and service owners whose exits were
proved. ROOT Stop, CLOSED resume and setup re-opening verify that receipt against
current records, retained lane cleanup and outstanding leases. A legacy CLOSED
record without a receipt must be reconciled by native shutdown from its retained
ownership evidence; the state label alone does not authorize ROOT exit or resume.
Feedback and refreshed routes use atomic replacement while holding the complete
plain directory chain. This relies on the existing native mutation backend;
hosts without its required directory guarantees report an unsupported operation
instead of using an unsafe pathname fallback.
