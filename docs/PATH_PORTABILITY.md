# Portable paths

The distribution contains no selected operator workspace. Set the empty
`root_workspace` in the harness-root `harness-config.json` to the absolute path
of your chosen Git product workspace before running setup; the optional
`manager_workspace` selects where ROOT hooks, skills and `.agent-workspace` are
installed and defaults to `root_workspace`. An unconfigured distribution fails
with a configuration error and cannot silently write into another project.
Configured paths remain operator data; the harness never guesses a developer's
workspace. Legacy observation examples also require an explicit `suite_root`.

Source and packaged assets use relative paths inside the harness. External tools
come from PATH, explicit arguments, or OS discovery. Windows CIM uses discovered
PowerShell; macOS process identity uses the OS library lookup for libproc. Linux
process and cgroup discovery uses util-linux `findmnt` with explicit JSON columns.
If discovery is unavailable, process observations remain unknown rather than
proving absence. `HARNESS_PROCFS_ROOT` and `HARNESS_CGROUP_ROOT` can select an
absolute mounted filesystem when there are multiple eligible mounts; discovery
validates the selected filesystem type and current process visibility.

The optional WSL test tooling requires an explicit distribution and installation
directory. For example:

```powershell
./orchestrator_harness/install_wsl_codex.ps1 -Distro '<chosen-distro>' -InstallRoot '<absolute-linux-install-directory>' -Release '<chosen-release>'
```

The preparer accepts any explicitly selected installation with the expected pinned
release layout. It discovers system bubblewrap on PATH if the pinned release does
not supply it. Preparation still requires exact provider device/inode identity,
one-use claim, cgroup limits, network allowlist, Git/preflight proof, and cleanup.

Pass `--system-mounts <absolute-manifest-path>` to the WSL preparer. Its JSON has
exactly `readonly_paths` and `forbidden_probe_paths`, each a nonempty list of
operator-selected absolute Linux paths. Select only required OS runtime/library
directories, CA certificates and account files for read-only exposure; include
the discovered Python and Git dependencies. List host workspace/auth/drive and
device locations that must remain absent as forbidden probes. Filesystem root,
operator home, assigned workspace and the manifest itself cannot be included in
the system mounts. An incomplete allowlist fails the preflight before publication
of READY state. The private namespace destinations are derived internal harness
paths; only the assigned worktree and disposable authentication home receive
writable host binds. No host filesystem-root bind is permitted.

Fill the placeholders in example invocation and build/test policy files with
deliberate operator values. The adapter path and SHA-256 must identify your actual
adapter; example placeholders are intentionally unusable. Tests use temporary
fixtures instead of fixed host or installation locations. Live WSL isolation and
native sandbox enforcement require their configured environments; unit manifest
checks alone do not prove those OS enforcement properties.

## Native worker Git storage

For isolated Codex workers, keep linked-worker Git administration readable under
the worker isolation policy. A normal product-owned `.git` directory works.
If using a separate common Git
directory, keep it inside the selected product workspace, outside
`.harness-runtime/operator-only` and `.secrets`. A common store under a protected
sibling directory is denied even when a worker has narrower write grants beneath
it; bootstrap reports that conflict before publishing its isolation profile.
Keep persistent Git administration outside disposable runtime directories.
The explicit `permission_mode=yolo` launch does not select the worker isolation
profile and is exempt from this profile's Git read-access guard.

On Windows, keep the checkout, runtime and lane names reasonably short. Git can
reject a long linked-worktree Git directory before loading `core.longpaths`, so
that option alone does not make every deep temporary fixture usable. Choose a
short operator-owned workspace or temporary root when Git reports this limit.
