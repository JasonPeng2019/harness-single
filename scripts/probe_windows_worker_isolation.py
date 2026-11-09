"""Run one explicit native Codex probe and record only its observed result."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def _sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _git(cwd: Path, *arguments: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(cwd),
            "-c",
            "user.name=Harness Probe",
            "-c",
            "user.email=probe@invalid",
            *arguments,
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
        env=dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull),
    )
    if completed.returncode:
        raise RuntimeError(
            f"owned Git fixture failed ({completed.returncode}): {completed.stderr.strip()}"
        )
    return completed.stdout.strip()


def _source_head(harness: Path) -> str | None:
    """Record Git identity only when the installed source is itself a Git root."""
    completed = subprocess.run(
        ["git", "-C", str(harness), "rev-parse", "--show-toplevel"],
        capture_output=True,
        check=False,
        text=True,
        timeout=15,
        env=dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull),
    )
    if completed.returncode or Path(completed.stdout.strip()).resolve() != harness:
        return None
    return _git(harness, "rev-parse", "HEAD")


def _under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _native_helper() -> str:
    return r"""import hashlib, json, os, pathlib, subprocess, sys, time, datetime
request = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
started = datetime.datetime.now(datetime.timezone.utc).timestamp()
print(json.dumps({"native_helper_started": started, "worker_id": request["worker_id"]}), flush=True)
time.sleep(1.0)
protected = request["protected"]
result = {"denied": [], "unexpectedly_writable": [], "probe_errors": []}
if request.get("require_read_denial"):
    try:
        pathlib.Path(request["read_sentinel"]).read_bytes()
    except PermissionError:
        result["read_denied"] = True
    except OSError as exc:
        result["read_error"] = str(exc)
    else:
        result["read_sentinel_accessible"] = True
pathlib.Path("actual-change.txt").write_text("Native worker profile write probe\\n", encoding="utf-8")
for raw in protected:
    try:
        with pathlib.Path(raw).open("ab") as stream:
            stream.write(b"R8_FORBIDDEN_PROBE\\n")
        result["unexpectedly_writable"].append(raw)
    except PermissionError:
        result["denied"].append(raw)
    except OSError as exc:
        result["probe_errors"].append({"path": raw, "error": str(exc)})
result["worker_id"] = request["worker_id"]
result["native_helper_started"] = started
print(json.dumps(result), flush=True)
if result["unexpectedly_writable"] or result["probe_errors"] or (request.get("require_read_denial") and not result.get("read_denied", False)):
    raise SystemExit(3)
git_env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
for args in (["add", "actual-change.txt"],
             ["-c", "user.name=Harness Probe", "-c", "user.email=probe@invalid",
              "commit", "-m", "native worker profile write probe"],
             ["log", "-1", "--format=%H"]):
    command = ["git", "-c", "safe.directory=" + str(pathlib.Path.cwd()), *args]
    completed = subprocess.run(command, capture_output=True, text=True, env=git_env)
    print(json.dumps({"argv": args, "exit_code": completed.returncode,
                      "stdout": completed.stdout, "stderr": completed.stderr}), flush=True)
    if completed.returncode:
        raise SystemExit(completed.returncode)
"""


def _prepare_fixture(repo: Path) -> tuple[Path, Path, list[Path]]:
    repo.mkdir(parents=True)
    _git(repo, "init", "-q")
    (repo / "README.md").write_text(
        "Owned Codex worker profile probe fixture\n", encoding="utf-8"
    )
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-qm", "seed owned probe fixture")
    worker, peer = repo / "worker", repo / "peer"
    _git(repo, "worktree", "add", "-qb", "lane/probe-worker/work", str(worker))
    _git(repo, "worktree", "add", "-qb", "lane/probe-peer/work", str(peer))
    (repo / "unrelated").mkdir()
    sentinel = repo / "unrelated" / "sentinel.txt"
    sentinel.write_bytes(b"owned protected sentinel\n")
    hook = repo / ".git" / "hooks" / "owned-sentinel"
    hook.write_bytes(b"owned protected hook\n")
    protected = [
        repo / ".git" / "config",
        repo / ".git" / "index",
        hook,
        sentinel,
    ]
    return worker, peer, protected


def _probe(args: argparse.Namespace) -> dict:
    if os.name != "nt":
        raise ValueError(
            "native Windows worker qualification is available only on Windows"
        )
    harness = args.harness_dir.resolve(strict=True)
    if not (harness / "orchestrator_harness" / "bootstrap.py").is_file():
        raise ValueError(
            "harness directory must contain the installed orchestrator_harness package"
        )
    output = (
        args.output_dir if args.output_dir.is_absolute() else harness / args.output_dir
    ).resolve()
    runtime = (harness / "runtime").resolve()
    if not _under(output, runtime):
        raise ValueError(
            "probe output must be a new owned directory under harness runtime/"
        )
    if output.exists() or output.is_symlink():
        raise ValueError(
            f"probe output already exists; preserve it and select a fresh path: {output}"
        )
    cache = (
        args.local_appdata
        if args.local_appdata.is_absolute()
        else harness / args.local_appdata
    ).resolve()
    if not _under(cache, runtime) or _under(output, cache) or _under(cache, output):
        raise ValueError(
            "process-local LOCALAPPDATA must be a separate owned path under harness runtime/"
        )

    sys.path.insert(0, str(harness))
    from orchestrator_harness import processes
    from orchestrator_harness.bootstrap import _install_codex_worker_isolation
    from orchestrator_harness.provider_adapters.codex.launcher_binding import (
        _direct_codex_executable,
        _isolation_overrides,
    )
    from orchestrator_harness.records import atomic_write_json
    from orchestrator_harness.windows_sandbox_preflight import (
        health_path,
        profile_contract_digest,
        startup_failure,
        validate_worker_profile,
    )

    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").resolve()
    marker = codex_home / ".sandbox" / "setup_marker.json"
    if not marker.is_file():
        raise ValueError(f"existing native Codex sandbox marker is required: {marker}")
    json.loads(marker.read_text(encoding="utf-8-sig"))
    cache.mkdir(parents=True, exist_ok=True)
    if cache.is_symlink() or cache.resolve() != cache.absolute():
        raise ValueError("process-local LOCALAPPDATA must be a plain directory path")
    cache.mkdir(parents=True, exist_ok=True)
    native = Path(_direct_codex_executable()).resolve(strict=True)
    native_hash = _sha256(native)
    marker_hash = _sha256(marker)
    permission_mode = args.permission_mode
    contract_hash = profile_contract_digest(harness, permission_mode)

    output.mkdir(parents=True, exist_ok=False)
    repo = output / "repo"
    worker, peer, protected = _prepare_fixture(repo)
    before = {str(path): _sha256(path) for path in protected}
    protected_path = output / "protected-paths.json"
    read_sentinel = (
        codex_home / f".harness-worker-probe-{output.name}.txt"
        if permission_mode == "worker-isolated"
        else None
    )
    if read_sentinel is not None and (
        read_sentinel.exists() or read_sentinel.is_symlink()
    ):
        raise ValueError(
            "unique read sentinel unexpectedly exists in the active Codex home"
        )
    protected_path.write_text(
        json.dumps(
            {
                "protected": [str(path) for path in protected],
                "read_sentinel": str(read_sentinel) if read_sentinel else None,
                "require_read_denial": permission_mode == "worker-isolated",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    helper = output / "native_probe.py"
    helper.write_text(_native_helper(), encoding="utf-8")
    temporary = output / "private-temp"
    temporary.mkdir()
    peer_temporary = output / "peer-private-temp"
    if permission_mode == "worker-isolated":
        peer_temporary.mkdir()
    peer_cache = (
        cache.parent / f"{cache.name}-peer"
        if permission_mode == "worker-isolated"
        else None
    )
    if peer_cache is not None and peer_cache.exists():
        raise ValueError("concurrent worker LOCALAPPDATA must be a fresh owned path")
    if peer_cache is not None:
        peer_cache.mkdir(parents=True)
    profile_path = worker / ".codex" / "config.toml"
    _install_codex_worker_isolation(
        worker,
        harness,
        repo,
        permission_mode=permission_mode,
        manager_workspace=repo,
    )
    profile_hash = validate_worker_profile(profile_path, permission_mode)
    peer_profile_path = peer / ".codex" / "config.toml"
    peer_profile_hash = None
    peer_request_path = None
    peer_argv = None
    if permission_mode == "worker-isolated":
        _install_codex_worker_isolation(
            peer, harness, repo, permission_mode=permission_mode, manager_workspace=repo
        )
        peer_profile_hash = validate_worker_profile(peer_profile_path, permission_mode)
        peer_request_path = output / "peer-protected-paths.json"
        peer_request_path.write_text(
            json.dumps(
                {
                    "protected": [],
                    "read_sentinel": str(read_sentinel),
                    "require_read_denial": True,
                    "worker_id": "peer",
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    worker_protected = list(protected)
    if permission_mode == "worker-isolated":
        worker_protected.extend(
            (
                Path(_git(worker, "rev-parse", "--absolute-git-dir"))
                / ".."
                / "peer"
                / "index",
                repo / ".git" / "refs/heads/lane/probe-peer/work",
            )
        )
    protected_request = {
        "protected": [str(path.resolve()) for path in worker_protected],
        "read_sentinel": str(read_sentinel) if read_sentinel else None,
        "require_read_denial": permission_mode == "worker-isolated",
        "worker_id": "worker",
    }
    protected_path.write_text(
        json.dumps(protected_request, indent=2) + "\n", encoding="utf-8"
    )
    profile = __import__("tomllib").loads(profile_path.read_text(encoding="utf-8"))
    external_writes = {
        name
        for name, mode in profile["permissions"][permission_mode]["filesystem"].items()
        if name != ":workspace_roots" and mode == "write"
    }
    if len(external_writes) != 4:
        raise ValueError(
            "generated worker-workspace profile lacks the four narrow linked-Git write grants"
        )

    argv = [str(native), "-c", 'windows.sandbox="elevated"']
    for override in _isolation_overrides(str(worker), permission_mode):
        argv.extend(("-c", override))
    argv.extend(
        (
            "sandbox",
            "--include-managed-config",
            "--permission-profile",
            permission_mode,
            "--cd",
            str(worker),
            "--",
            sys.executable,
            str(helper),
            str(protected_path),
        )
    )
    if peer_profile_hash is not None:
        peer_argv = [str(native), "-c", 'windows.sandbox="elevated"']
        for override in _isolation_overrides(str(peer), permission_mode):
            peer_argv.extend(("-c", override))
        peer_argv.extend(
            (
                "sandbox",
                "--include-managed-config",
                "--permission-profile",
                permission_mode,
                "--cd",
                str(peer),
                "--",
                sys.executable,
                str(helper),
                str(peer_request_path),
            )
        )
    environment = os.environ.copy()
    environment.update(TEMP=str(temporary), TMP=str(temporary), LOCALAPPDATA=str(cache))
    peer_environment = os.environ.copy()
    if peer_cache is not None:
        peer_environment.update(
            TEMP=str(peer_temporary),
            TMP=str(peer_temporary),
            LOCALAPPDATA=str(peer_cache),
        )
    result: dict = {
        "protected_write_targets": json.loads(
            protected_path.read_text(encoding="utf-8")
        )["protected"],
        "schema": "windows-worker-profile-probe/v1",
        "source": str(harness),
        "source_head": _source_head(harness),
        "profile_contract_sha256": contract_hash,
        "argv": argv,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "local_appdata": str(cache),
        "permission_mode": permission_mode,
        "codex_home": str(codex_home),
        "read_sentinel": str(read_sentinel) if read_sentinel else None,
        "profile_path": str(profile_path),
        "profile_sha256": profile_hash,
        "worker_base_tip": _git(repo, "rev-parse", "HEAD"),
        "protected_before": before,
        "protected_after": None,
        "protected_unchanged": False,
        "exit_code": None,
        "cleanup_proven": False,
    }
    child = boundary = peer_child = peer_boundary = None
    stdout_path, stderr_path = output / "stdout.log", output / "stderr.log"
    peer_stdout_path, peer_stderr_path = (
        output / "peer-stdout.log",
        output / "peer-stderr.log",
    )
    if read_sentinel is not None:
        read_sentinel.write_bytes(b"owned non-secret read-denial sentinel\n")
    try:
        with (
            stdout_path.open("wb") as stdout,
            stderr_path.open("wb") as stderr,
            peer_stdout_path.open("wb") as peer_stdout,
            peer_stderr_path.open("wb") as peer_stderr,
            open(os.devnull, "rb") as stdin,
        ):
            child = processes.spawn_provider(
                argv,
                cwd=worker,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                env=environment,
            )
            take = getattr(child, "take_job_handle", None)
            boundary = processes.ProcessBoundary.for_process(
                child.pid, windows_job_handle=take() if callable(take) else None
            )
            if not boundary.observe():
                raise RuntimeError("native Codex process boundary identity is unproven")
            if peer_argv is not None:
                peer_child = processes.spawn_provider(
                    peer_argv,
                    cwd=peer,
                    stdin=stdin,
                    stdout=peer_stdout,
                    stderr=peer_stderr,
                    env=peer_environment,
                )
                peer_take = getattr(peer_child, "take_job_handle", None)
                peer_boundary = processes.ProcessBoundary.for_process(
                    peer_child.pid,
                    windows_job_handle=peer_take() if callable(peer_take) else None,
                )
                if not peer_boundary.observe():
                    raise RuntimeError(
                        "concurrent native Codex process boundary identity is unproven"
                    )
            resume = getattr(child, "resume", None)
            if callable(resume):
                resume()
            if peer_child is not None:
                peer_resume = getattr(peer_child, "resume", None)
                if callable(peer_resume):
                    peer_resume()
            try:
                result["exit_code"] = child.wait(timeout=args.timeout_seconds)
            except subprocess.TimeoutExpired:
                result["timeout"] = True
            if peer_child is not None:
                try:
                    peer_exit = peer_child.wait(timeout=args.timeout_seconds)
                except subprocess.TimeoutExpired:
                    peer_exit = None
                    result["concurrent_timeout"] = True
            else:
                peer_exit = None
            result["cleanup_proven"] = boundary.cleanup(
                force=True, timeout_seconds=15.0
            )
            result["cleanup_proven"] = (
                processes.reap_created_process(child, timeout_seconds=15.0)
                and result["cleanup_proven"]
            )
            if peer_child is not None and peer_boundary is not None:
                peer_cleanup = peer_boundary.cleanup(force=True, timeout_seconds=15.0)
                peer_cleanup = (
                    processes.reap_created_process(peer_child, timeout_seconds=15.0)
                    and peer_cleanup
                )
            else:
                peer_cleanup = None
        health_view = {"marker_path": str(marker), "marker_sha256": marker_hash}
        result["sandbox_error"] = startup_failure(health_view, stderr_path, 0)
        if peer_child is not None:
            result["concurrent_sandbox_error"] = startup_failure(
                health_view, peer_stderr_path, 0
            )
            result["concurrent_workers"] = [
                {
                    "worker_id": "peer",
                    "profile_path": str(peer_profile_path),
                    "profile_sha256": peer_profile_hash,
                    "argv": peer_argv,
                    "local_appdata": str(peer_cache),
                    "exit_code": peer_exit,
                    "cleanup_proven": peer_cleanup,
                    "sandbox_error": result.get("concurrent_sandbox_error"),
                    "boundary": peer_boundary.record()
                    if peer_boundary is not None
                    else None,
                }
            ]
    except Exception as exc:  # noqa: BLE001 - persist errors after exact owned-process cleanup
        result["error"] = f"{type(exc).__name__}: {exc}"
        for owned_boundary in (boundary, peer_boundary):
            if owned_boundary is not None:
                owned_boundary.cleanup(force=True, timeout_seconds=15.0)
        for owned_child in (child, peer_child):
            if owned_child is not None:
                processes.reap_created_process(owned_child, timeout_seconds=15.0)
    finally:
        if read_sentinel is not None:
            read_sentinel.unlink(missing_ok=True)
        result["ended_at"] = dt.datetime.now(dt.UTC).isoformat()
        if boundary is not None:
            result["boundary"] = boundary.record()
        if peer_boundary is not None and result.get("concurrent_workers"):
            result["concurrent_workers"][0]["boundary"] = peer_boundary.record()
    after = {str(path): _sha256(path) for path in protected}
    result["protected_after"] = after
    result["protected_unchanged"] = before == after
    observations = []
    try:
        for line in stdout_path.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if isinstance(item, dict) and {
                "denied",
                "unexpectedly_writable",
                "probe_errors",
            }.issubset(item):
                observations.append(item)
    except OSError as exc:
        result["observation_error"] = str(exc)
    if len(observations) == 1:
        result["protected_write_denials"] = observations[0]["denied"]
        result["unexpectedly_writable"] = observations[0]["unexpectedly_writable"]
        result["probe_errors"] = observations[0]["probe_errors"]
        result["codex_home_read_denied"] = observations[0].get("read_denied") is True
        result["codex_home_read_error"] = observations[0].get("read_error")
        result["worker_helper_started"] = observations[0].get("native_helper_started")
    else:
        result["observation_error"] = (
            "native helper emitted no unique write-boundary observation"
        )
    if peer_argv is not None and result.get("concurrent_workers"):
        peer_observations = []
        for line in peer_stdout_path.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if isinstance(item, dict) and "read_denied" in item:
                peer_observations.append(item)
        peer_record = result["concurrent_workers"][0]
        peer_record["codex_home_read_denied"] = (
            len(peer_observations) == 1
            and peer_observations[0].get("read_denied") is True
        )
        peer_record["helper_started"] = (
            peer_observations[0].get("native_helper_started")
            if len(peer_observations) == 1
            else None
        )
        peer_record["worker_base_tip"] = result["worker_base_tip"]
        try:
            peer_record["worker_tip"] = _git(peer, "rev-parse", "HEAD")
        except (OSError, RuntimeError) as exc:
            peer_record["worker_tip_error"] = str(exc)
        helper_times = [
            item.get("native_helper_started")
            for item in observations[:1] + peer_observations[:1]
        ]
        result["concurrent_overlap_proven"] = (
            len(helper_times) == 2
            and all(isinstance(item, (int, float)) for item in helper_times)
            and abs(helper_times[0] - helper_times[1]) < 1.0
            and peer_record.get("exit_code") == 0
            and peer_record.get("cleanup_proven") is True
            and peer_record.get("codex_home_read_denied") is True
            and peer_record.get("worker_tip") != peer_record.get("worker_base_tip")
        )
        peer_record["concurrent_overlap_proven"] = result["concurrent_overlap_proven"]
    try:
        result["worker_tip"] = _git(worker, "rev-parse", "HEAD")
    except (OSError, RuntimeError) as exc:
        result["worker_tip_error"] = str(exc)
    result["profile_contract_sha256_after"] = profile_contract_digest(
        harness, permission_mode
    )
    result["native_logs"] = {
        str(path.name): {"path": str(path.resolve()), "sha256": _sha256(path)}
        for path in (stdout_path, stderr_path, peer_stdout_path, peer_stderr_path)
    }
    result_path = output / "result.json"
    passed = (
        result.get("exit_code") == 0
        and result.get("cleanup_proven") is True
        and result.get("protected_unchanged") is True
        and result.get("sandbox_error") is None
        and set(result.get("protected_write_denials", []))
        == set(json.loads(protected_path.read_text(encoding="utf-8"))["protected"])
        and not result.get("unexpectedly_writable")
        and not result.get("probe_errors")
        and (
            permission_mode != "worker-isolated"
            or (
                result.get("codex_home_read_denied") is True
                and result.get("concurrent_overlap_proven") is True
                and len(result.get("concurrent_workers", [])) == 1
            )
        )
        and result.get("worker_tip") != result.get("worker_base_tip")
        and result.get("observation_error") is None
        and result.get("profile_contract_sha256_after") == contract_hash
        and result.get("worker_tip") is not None
        and result.get("worker_tip") != _git(repo, "rev-parse", "HEAD")
    )
    target = health_path(harness, permission_mode)
    if passed:
        if target.exists() and not args.replace_existing_health:
            result["health_receipt_written"] = False
            result["health_receipt_error"] = (
                f"health receipt already exists; rerun with --replace-existing-health to preserve the prior copy: {target}"
            )
            atomic_write_json(result_path, result)
            print(json.dumps(result, indent=2))
            return result
        if target.is_symlink():
            raise ValueError(f"health receipt path must not be a symlink: {target}")
        if target.exists():
            prior = output / "prior-health-receipt.json"
            prior.write_bytes(target.read_bytes())
            result["prior_health_receipt_backup"] = str(prior)
        result["health_receipt_written"] = True
        atomic_write_json(result_path, result)
        receipt = {
            "schema": "windows-sandbox-health/v2",
            "status": "PASS",
            "probe_exit_code": 0,
            "permission_mode": permission_mode,
            "profile_contract_sha256": contract_hash,
            "backend_id": "codex-windows-elevated",
            "codex_home": str(codex_home),
            "local_appdata": str(cache),
            "native_codex_executable": str(native),
            "native_codex_sha256": native_hash,
            "setup_marker_sha256": marker_hash,
            "probe_evidence_path": str(result_path.resolve()),
            "probe_evidence_sha256": _sha256(result_path),
            "probe_profile_sha256": profile_hash,
        }
        atomic_write_json(target, receipt)
    else:
        result["health_receipt_written"] = False
        atomic_write_json(result_path, result)
    if read_sentinel is not None:
        read_sentinel.unlink(missing_ok=True)
    print(json.dumps(result, indent=2))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="fresh output path below harness runtime/",
    )
    parser.add_argument(
        "--local-appdata",
        required=True,
        type=Path,
        help="operator-selected process-local cache directory",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--permission-mode",
        choices=("worker-workspace", "worker-isolated"),
        default="worker-workspace",
    )
    parser.add_argument(
        "--replace-existing-health",
        action="store_true",
        help="back up the existing mode receipt into this new evidence directory",
    )
    args = parser.parse_args()
    if not 10 <= args.timeout_seconds <= 600:
        parser.error("--timeout-seconds must be 10..600")
    try:
        result = _probe(args)
    except Exception as exc:  # noqa: BLE001 - provide a clear CLI failure for any validation error
        print(f"WINDOWS_WORKER_PROBE_FAILED: {exc}", file=sys.stderr)
        return 2
    return 0 if result.get("health_receipt_written") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
