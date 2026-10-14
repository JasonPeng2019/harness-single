"""Restricted worker test service; executes task tools, never model workers."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from . import processes
from .core import iso_utc, new_id
from .records import RecordLock, atomic_write_json
from .records import runtime_admission_lock
from .lanes import lane_operation_lock
from .portable_paths import admit_entry, repository_parts
from .mutation import read_bytes as anchored_read
from .stable_io import PreparedOutputTransaction

POLICY = "local-config/public-check-policy.json"
RESERVED_PARTS = {".git", ".codex", ".claude", ".qwen", ".agent-workspace", ".harness-runtime"}
_ADAPTER_IMPORT_LOCK = threading.Lock()


def read(path: Path) -> dict:
    value = json.loads(anchored_read(path.parent, path.name).decode("utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def home(root: Path) -> Path:
    return root / ".harness-runtime" / "operator-only" / "public-check-service"


def policy(harness: Path) -> dict | None:
    path = harness / POLICY
    if not path.exists():
        return None
    value = read(path)
    if value.get("schema") != "public-check-policy/v2":
        raise ValueError("unsupported public check policy")
    if type(value.get("timeout_seconds")) is not int or not 1 <= value["timeout_seconds"] <= 600:
        raise ValueError("public check timeout must be 1..600 seconds")
    if type(value.get("max_parallel_checks")) is not int or not 1 <= value["max_parallel_checks"] <= 4:
        raise ValueError("public check parallelism must be 1..4")
    for field in ("input_paths", "optional_input_paths", "fixed_paths"):
        paths = value.get(field)
        if not isinstance(paths, list) or (field == "input_paths" and not paths):
            raise ValueError(f"{field} must be a list of repository-relative paths")
        for name in paths:
            try:
                parts = repository_parts(name)
            except ValueError as exc:
                raise ValueError(f"unsafe {field} path: {exc}") from exc
            if (not isinstance(name, str) or not name or name.startswith("-")
                    or any(char in name for char in "\\:*?[]\0")
                    or PurePosixPath(name).is_absolute() or PurePosixPath(name).as_posix() != name
                    or any(part.casefold() in RESERVED_PARTS for part in parts)):
                raise ValueError(f"unsafe {field} path")
        if len({name.casefold() for name in paths}) != len(paths):
            raise ValueError(f"duplicate {field} path")
    selected = value["input_paths"] + value["optional_input_paths"]
    if any(a.casefold() == b.casefold() or a.casefold().startswith(b.casefold() + "/") or b.casefold().startswith(a.casefold() + "/")
           for index, a in enumerate(selected) for b in selected[index + 1:]):
        raise ValueError("input paths must not overlap")
    if any(not any(name == parent or name.startswith(parent + "/") for parent in selected)
           for name in value["fixed_paths"]):
        raise ValueError("fixed paths must be included in the snapshot")
    commands = value.get("commands")
    if not isinstance(commands, dict) or set(commands) != {"build", "public"}:
        raise ValueError("commands must define build and public argv")
    for command in commands.values():
        _validate_argv(command)
    if type(value.get("allow_focused")) is not bool:
        raise ValueError("allow_focused must be a boolean")
    _adapter_path(value)
    return value


def _validate_argv(command: object) -> list[str]:
    if (not isinstance(command, list) or not 0 < len(command) <= 128
            or not all(isinstance(arg, str) and arg and "\0" not in arg and len(arg) <= 16000
                       for arg in command)):
        raise ValueError("invalid public check command argv")
    return command


def _adapter_path(selected: dict, root: Path | None = None) -> Path:
    adapter = selected.get("adapter")
    if not isinstance(adapter, dict) or set(adapter) != {"path", "sha256"}:
        raise ValueError("an operator-supplied adapter path and SHA-256 are required")
    name, expected = adapter["path"], adapter["sha256"]
    if (not isinstance(name, str) or not name or "\0" in name or not Path(name).is_absolute()
            or not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}", expected)):
        raise ValueError("adapter must have an absolute path and exact SHA-256")
    path = Path(name)
    if path.is_symlink() or path.resolve(strict=True) != path.absolute() or not path.is_file():
        raise ValueError("adapter must be a regular file without linked ancestors")
    if root is not None and path.is_relative_to(root.resolve()):
        raise ValueError("adapter must be outside the worker-editable ROOT workspace")
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError("public check adapter changed or its SHA-256 does not match")
    return path


def git(worktree: Path, *arguments: str) -> bytes:
    from .git_source import root_git
    completed = root_git(worktree, *arguments)
    if completed.returncode:
        raise ValueError(completed.stderr.decode("utf-8", "replace").strip())
    return completed.stdout


def _input_git(worktree: Path, source: dict | None, *arguments: str) -> bytes:
    if source is None:
        return git(worktree, *arguments)
    from .git_source import git as assigned_git
    if Path(source["worktree"]) != worktree.absolute():
        raise ValueError("public input source differs from the assigned worktree")
    if len(arguments) == 3 and arguments[:2] == ("cat-file", "blob"):
        from .git_source import verified_object
        return verified_object(source, "blob", arguments[2])
    return assigned_git(source, *arguments)


def bridge(selected: dict, root: Path):
    path = _adapter_path(selected, root)
    source = path.read_bytes()
    if hashlib.sha256(source).hexdigest() != selected["adapter"]["sha256"]:
        raise ValueError("public check adapter changed before import")
    identity = hashlib.sha256(str(path).encode() + b"\0" + source).hexdigest()
    name = f"operator_public_check_adapter_{identity}"
    with _ADAPTER_IMPORT_LOCK:
        if name in sys.modules:
            return sys.modules[name]
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ValueError("adapter must be an importable Python source file")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            # Execute the hashed source, never an unbound cached bytecode file.
            exec(compile(source, str(path), "exec"), module.__dict__)
            if not callable(getattr(module, "preflight", None)) or not callable(getattr(module, "run_public_command", None)):
                raise ValueError("adapter must provide preflight and run_public_command")
        except BaseException:
            if sys.modules.get(name) is module:
                del sys.modules[name]
            raise
        return module


def _file_digests(worktree: Path, revision: str, paths: list[str], *, source: dict | None = None) -> dict:
    if not paths:
        return {}
    result = {}
    entries = _input_git(worktree, source, "ls-tree", "-r", "-z", revision, "--", *paths).split(b"\0")
    for entry in entries:
        if not entry:
            continue
        metadata, filename = entry.split(b"\t", 1)
        mode, kind, digest = metadata.decode().split()
        if mode not in {"100644", "100755"} or kind != "blob":
            raise ValueError("public task inputs must be regular committed files")
        result[filename.decode("utf-8")] = digest
    return result


def snapshot(lane: Path, revision: str, destination: Path, selected: dict, *, source: dict | None = None) -> None:
    names = []
    for name in selected["input_paths"] + selected["optional_input_paths"]:
        if _input_git(lane, source, "ls-tree", "-z", revision, "--", name):
            names.append(name)
        elif name in selected["input_paths"]:
            raise ValueError("committed public check inputs are missing")
    if not names:
        return
    # Archive export attributes and checkout filters can omit or transform blobs.
    # Materialize the exact committed bytes whose identities were checked.
    entries = _input_git(lane, source, "ls-tree", "-r", "-z", revision, "--", *names).split(b"\0")
    nodes = {}
    files = []
    for entry in entries:
        if not entry:
            continue
        metadata, filename = entry.split(b"\t", 1)
        mode, kind, digest = metadata.decode("ascii").split()
        name = filename.decode("utf-8")
        parts = repository_parts(name)
        relative = PurePosixPath(*parts)
        if (any(part.casefold() in RESERVED_PARTS for part in parts)
                or not any(name == parent or name.startswith(parent + "/") for parent in names)):
            raise ValueError("unsafe public input path")
        if mode not in {"100644", "100755"} or kind != "blob":
            raise ValueError("links and special files are forbidden in public inputs")
        admit_entry(nodes, parts)
        files.append((relative, mode, digest))
    # Validate the complete tree before creating any output. A snapshot must
    # never inherit unrelated files or silently overwrite a Windows alias.
    if destination.is_symlink() or destination.resolve() != destination.absolute():
        raise ValueError("public snapshot destination has linked ancestors")
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise ValueError("public snapshot destination must be an empty owned directory")
    for relative, mode, digest in files:
        target = destination.joinpath(*relative.parts)
        if not target.resolve().is_relative_to(destination.resolve()):
            raise ValueError("unsafe public input destination")
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(_input_git(lane, source, "cat-file", "blob", digest))
        target.chmod(0o755 if mode == "100755" else 0o644)


def validate_service(root: Path, harness: Path, *, runtime_closed: bool = False) -> dict:
    record = read(home(root) / "service.json")
    _service_identity(record, root, harness)
    if (record.get("schema") != "worker-public-check-service/v1"
            or Path(record.get("root", "")).resolve() != root.resolve()
            or Path(record.get("harness", "")).resolve() != harness.resolve()
            or record.get("status") != ("STOPPED" if runtime_closed else "READY")):
        raise ValueError("public check service is not healthy for this ROOT")
    if runtime_closed:
        if (record.get("cleanup_proven") is not True
                or not processes.identity_exit_proven(record["pid"], record["creation_time"])):
            raise ValueError("public check service cleanup is not proven for ROOT resume")
    elif not processes.identity_matches(record["pid"], record["creation_time"]):
        raise ValueError("public check service is not healthy for this ROOT")
    config = read(home(root) / "config.json")
    if (not isinstance(record.get("startup_id"), str)
            or not re.fullmatch(r"[a-f0-9]{32}", record["startup_id"])
            or config.get("startup_id") != record["startup_id"]):
        raise ValueError("public check service startup identity mismatch")
    if config["policy"] != policy(harness):
        raise ValueError("public check policy changed after service startup")
    _adapter_path(config["policy"], root)
    policy_hash = hashlib.sha256(json.dumps(config["policy"], sort_keys=True).encode()).hexdigest()
    if record.get("policy_sha256") != policy_hash:
        raise ValueError("public check service policy identity mismatch")
    return {key: record[key] for key in ("schema", "pid", "creation_time", "root", "harness", "status", "policy_sha256", "startup_id")}


def _service_identity(record: dict, root: Path, harness: Path | None = None) -> tuple[int, str]:
    if (record.get("schema") != "worker-public-check-service/v1"
            or type(record.get("pid")) is not int or record["pid"] <= 0
            or not processes.valid_creation_identity(record.get("creation_time"))
            or not isinstance(record.get("root"), str) or not Path(record["root"]).is_absolute()
            or Path(record["root"]).resolve() != root.resolve()
            or (harness is not None and (not isinstance(record.get("harness"), str)
                or not Path(record["harness"]).is_absolute()
                or Path(record["harness"]).resolve() != harness.resolve()))):
        raise ValueError("public check service identity is invalid or foreign")
    return record["pid"], record["creation_time"]


def _service_environment(harness: Path) -> dict[str, str]:
    # Forward tool/runtime essentials only; arbitrary names and proxy URLs can
    # contain credentials even when they lack a familiar provider prefix.
    allowed = {"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "TMPDIR",
               "LANG", "LC_ALL", "TZ", "PROCESSOR_ARCHITECTURE", "NUMBER_OF_PROCESSORS"}
    environment = {key: value for key, value in os.environ.items() if key.upper() in allowed}
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(harness), PYTHONUTF8="1")
    return environment


def _require_service_owner(config: dict, owner: tuple[int, str, str]) -> dict:
    """An executing service can operate only its original startup incarnation."""
    root, harness = Path(config["root"]), Path(config["harness"])
    current_config = read(home(root) / "config.json")
    record = read(home(root) / "service.json")
    identity = _service_identity(record, root, harness)
    if (identity + (record.get("startup_id"),) != owner
            or current_config.get("startup_id") != owner[2]
            or current_config != config
            or record.get("policy_sha256") != hashlib.sha256(
                json.dumps(config["policy"], sort_keys=True).encode()).hexdigest()):
        raise ValueError("public check service ownership changed")
    return record


def start_service(harness: Path, root: Path) -> dict | None:
    with runtime_admission_lock(root / '.harness-runtime'):
        selected = policy(harness)
        if selected is None:
            return None
        base = home(root)
        if (base / "service.json").exists() or (base / "service.json").is_symlink():
            return validate_service(root, harness)
        if read(root / ".harness-runtime/RUNTIME_STATE.json").get("state") != "OPEN":
            raise ValueError("public checks require an OPEN native runtime")
        revision = git(root, "rev-parse", "HEAD").decode().strip()
        base.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="preflight-", dir=base) as temporary:
            snapshot(root, revision, Path(temporary), selected)
        if read(root / ".harness-runtime/RUNTIME_STATE.json").get("state") != "OPEN":
            raise ValueError("runtime changed before public check service creation")
        startup_id = new_id()
        with RecordLock(base / "config.json"):
            atomic_write_json(base / "config.json", {
                "root": str(root.resolve()), "harness": str(harness.resolve()), "policy": selected,
                "baseline_revision": revision, "fixed_inputs": _file_digests(root, revision, selected["fixed_paths"]),
                "startup_id": startup_id,
            })
        environment = _service_environment(harness)
        service_home = base / "service-home"
        service_home.mkdir(exist_ok=True)
        environment.update(HOME=str(service_home), USERPROFILE=str(service_home),
                           APPDATA=str(service_home), LOCALAPPDATA=str(service_home))
        with (base / "service.stdout.log").open("ab") as stdout, (base / "service.stderr.log").open("ab") as stderr:
            process = processes.spawn_detached(
                [sys.executable, "-B", "-m", "orchestrator_harness.public_checks", "--root", str(root)],
                cwd=harness, env=environment, stdout=stdout, stderr=stderr,
            )
        try:
            created_identity = processes.process_identity(process.pid)
            if not created_identity or not processes.valid_creation_identity(created_identity.get("creation_time")):
                raise ValueError("new public check service identity is unproven")
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise ValueError(f"public check service exited during startup; inspect {base}")
                if (base / "service.json").is_file():
                    record = read(base / "service.json")
                    if (record.get("startup_id") != startup_id or record.get("pid") != process.pid
                            or record.get("creation_time") != created_identity["creation_time"]):
                        raise ValueError("public check readiness belongs to a different service incarnation")
                    return validate_service(root, harness)
                time.sleep(0.1)
            raise ValueError("public check service did not become ready")
        except BaseException as exc:
            if not processes.reap_created_process(process):
                raise ValueError(f"public check startup failed and exact child cleanup is unproven: {exc}") from exc
            raise


def install_route(harness: Path, root: Path, lane: Path, lane_id: str, run_id: str, *,
                  output: PreparedOutputTransaction | None = None) -> None:
    if policy(harness) is None:
        if (home(root) / "service.json").exists() or (home(root) / "config.json").exists():
            raise ValueError("configured public check service policy is missing")
        return
    validate_service(root, harness)
    lane = lane.absolute()
    if (lane.parent.parent != root.resolve() / ".harness-runtime/worktrees"
            or lane.name != lane_id or not re.fullmatch(r"[a-f0-9]{32}", run_id)):
        raise ValueError("public check route must select one exact native lane")
    helper = harness / "tools/worker_public_check.py"
    agent = lane / ".agent-workspace"
    if output is None:
        output = PreparedOutputTransaction(lane, allowed_roots=(lane.parent,))
        output.prepare()
    elif output.root != lane:
        raise ValueError("public route output differs from the assigned worktree")
    helper_path = output.admit(agent / "public-check.py")
    output.publish_bytes(helper_path, helper.read_bytes())
    selected = policy(harness)
    record = {"schema": "worker-public-check-route/v1", "lane_id": lane_id,
              "run_id": run_id, "worktree": str(lane),
              "worktree_identity": list(output.root_identity),
              "input_paths": selected["input_paths"] + selected["optional_input_paths"],
              "allow_focused": selected["allow_focused"]}
    atomic_write_json(home(root) / "routes" / f"{run_id}.json", record)
    output.publish_json(output.admit(agent / "public-check-route.json"), record)


def _command(request: dict, selected: dict) -> list[str]:
    check, command = request.get("check"), request.get("command")
    if check in {"build", "public"} and command == []:
        return selected["commands"][check]
    if check == "focused" and selected["allow_focused"]:
        return _validate_argv(command)
    raise ValueError("invalid public check operation")


def _registered_request(route: dict, path: Path) -> tuple[dict, str]:
    """Read only requests addressed to this route; never answer for another run."""
    lane = Path(route["worktree"])
    identifier = path.parent.name
    if not re.fullmatch(r"[a-f0-9]{32}", identifier):
        raise ValueError("invalid public check request ID")
    expected = lane / ".agent-workspace/public-checks" / identifier / "request.json"
    if path.absolute() != expected.absolute():
        raise ValueError("public request escaped its registered lane")
    identity = route.get("worktree_identity")
    if (not isinstance(identity, list) or len(identity) != 2
            or any(type(item) is not int for item in identity)):
        raise ValueError("public route lacks its prepared worktree identity; prepare a fresh route")
    raw = anchored_read(lane, expected.relative_to(lane), max_bytes=65536,
                        expected_root_identity=tuple(identity))
    request = json.loads(raw)
    if not isinstance(request, dict):
        raise ValueError("public request must be a JSON object")
    expected_identity = {"request_id": identifier, "lane_id": route["lane_id"], "run_id": route["run_id"]}
    if any(request.get(field) != value for field, value in expected_identity.items()):
        raise ValueError("public request identity mismatch")
    return request, hashlib.sha256(raw).hexdigest()


def serve_request(config: dict, route: dict, path: Path, cancel: threading.Event, runner=None, *,
                  service_owner: tuple[int, str, str] | None = None) -> dict:
    root, harness = Path(config["root"]), Path(config["harness"])
    lane = Path(route["worktree"])
    identifier = path.parent.name
    request, digest = _registered_request(route, path)
    expected = lane / ".agent-workspace/public-checks" / identifier / "request.json"
    authoritative = home(root) / "jobs" / route["run_id"] / identifier / "result.json"
    with RecordLock(authoritative):
        if authoritative.is_file():
            result = read(authoritative)
            if result["request_sha256"] != digest:
                raise ValueError("completed public request was mutated")
        else:
            result = {"schema": "worker-public-check-result/v1", "request_id": identifier,
                      "lane_id": route["lane_id"], "run_id": route["run_id"],
                      "revision": request.get("revision"), "request_sha256": digest,
                      "status": "ERROR", "stdout": "", "stderr": "", "exit_code": 2,
                      "cleanup_proven": True, "started_at": iso_utc()}
            try:
                if service_owner is not None:
                    _require_service_owner(config, service_owner)
                if not isinstance(request, dict) or request.get("schema") != "worker-public-check/v1":
                    raise ValueError("public check request schema mismatch")
                for field in ("request_id", "lane_id", "run_id"):
                    if request.get(field) != result[field]:
                        raise ValueError("public request identity mismatch")
                if read(root / ".harness-runtime/RUNTIME_STATE.json").get("state") != "OPEN":
                    raise ValueError("public check runtime is no longer OPEN")
                epoch = lane.parent.name
                native = read(root / ".harness-runtime/epochs" / epoch / "lanes" / route["lane_id"] / "lane.json")
                if (native.get("run_id") != route["run_id"] or native.get("lifecycle") in {"retired", "abandoned"}
                        or Path(native.get("worktree_path", "")).resolve() != lane.resolve()):
                    raise ValueError("public check invocation is stale or foreign")
                revision = request.get("revision")
                if not isinstance(revision, str) or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", revision):
                    raise ValueError("an exact full Git revision is required")
                from .git_source import current_tip
                if current_tip(native) != revision:
                    raise ValueError("requested revision is no longer the lane tip")
                selected_policy = config["policy"]
                _adapter_path(selected_policy, root)
                if _file_digests(lane, revision, selected_policy["fixed_paths"], source=native["git_source"]) != config["fixed_inputs"]:
                    raise ValueError("fixed public check inputs changed")
                command = _command(request, selected_policy)
                result["command"] = command
                with tempfile.TemporaryDirectory(prefix="snapshot-", dir=home(root)) as temporary:
                    selected = Path(temporary)
                    snapshot(lane, revision, selected, selected_policy, source=native["git_source"])
                    result["cleanup_proven"] = False
                    checked = (runner or bridge(selected_policy, root).run_public_command)(
                        selected, command, timeout=config["policy"]["timeout_seconds"], cancel=cancel,
                    )
                    if (not isinstance(checked, dict)
                            or not isinstance(checked.get("stdout"), str)
                            or not isinstance(checked.get("stderr"), str)
                            or type(checked.get("exit_code")) is not int
                            or type(checked.get("cleanup_proven")) is not bool):
                        raise ValueError("adapter returned invalid feedback or cleanup proof")
                    json.dumps(checked)  # Reject unserializable diagnostics before publication.
                    result.update({key: checked[key] for key in ("stdout", "stderr", "exit_code", "cleanup_proven")})
                    result["backend"] = {key: value for key, value in checked.items()
                                         if key not in {"stdout", "stderr", "exit_code", "cleanup_proven"}}
                    if not result["cleanup_proven"]:
                        result["error"] = "adapter cleanup is not proven"
                        result["backend_exit_code"] = result["exit_code"]
                        result["exit_code"] = 2
                    else:
                        result["status"] = "PASS" if result["exit_code"] == 0 else "FAIL"
            except Exception as exc:
                # An adapter error is feedback, never a scheduler or launch retry.
                # Cleanup remains unproven once adapter execution has begun.
                result["error"] = str(exc)
            result["completed_at"] = iso_utc()
            atomic_write_json(authoritative, result)
    # Release the job record lock before admission: all lifecycle transactions
    # take runtime, lane, then record locks. Slow adapter work holds neither.
    epoch = lane.parent.name
    with runtime_admission_lock(root / ".harness-runtime"), lane_operation_lock(root / ".harness-runtime", epoch, route["lane_id"]):
        try:
            if service_owner is not None:
                _require_service_owner(config, service_owner)
            native = read(root / ".harness-runtime/epochs" / epoch / "lanes" / route["lane_id"] / "lane.json")
            identity = tuple(route["worktree_identity"])
            current_route = json.loads(anchored_read(
                lane, ".agent-workspace/public-check-route.json", max_bytes=65536,
                expected_root_identity=identity))
            _, current_digest = _registered_request(route, path)
            publish = (native.get("run_id") == route["run_id"]
                       and native.get("lifecycle") not in {"resuming", "retired", "abandoned"}
                       and Path(native.get("worktree_path", "")).absolute() == lane.absolute()
                       and current_route == route and current_digest == digest)
        except (OSError, ValueError, KeyError, TypeError):
            publish = False
        if publish:
            output = PreparedOutputTransaction(lane, allowed_roots=(lane.parent,))
            output.prepare()
            if output.root_identity != identity:
                raise ValueError("public response worktree changed after preparation")
            output.publish_json(output.admit(expected.parent / "result.json"), result)
    return result


def run_service(root: Path) -> None:
    base = home(root)
    config = read(base / "config.json")
    initial_config = dict(config)
    identity = processes.process_identity(os.getpid())
    if identity is None or not processes.valid_creation_identity(identity.get("creation_time")):
        raise ValueError("cannot bind public test service process identity")
    startup_id = config.get("startup_id")
    if not isinstance(startup_id, str) or not re.fullmatch(r"[a-f0-9]{32}", startup_id):
        raise ValueError("public test service has no valid startup identity")
    owner = (identity["pid"], identity["creation_time"], startup_id)
    # Adapter code executes only in this credential-filtered service process.
    backend = bridge(config["policy"], root).preflight()
    if not isinstance(backend, dict):
        raise ValueError("adapter preflight must return a JSON object or raise on failure")
    config["backend"] = json.loads(json.dumps(backend, allow_nan=False))
    record = {"schema": "worker-public-check-service/v1", "root": config["root"],
              "harness": config["harness"], "pid": os.getpid(),
              "creation_time": identity["creation_time"], "status": "READY",
              "policy_sha256": hashlib.sha256(json.dumps(config["policy"], sort_keys=True).encode()).hexdigest(),
              "startup_id": config["startup_id"],
              "started_at": iso_utc(), "stop_requested": False, "cleanup_proven": False}
    # Startup cannot acquire runtime admission here: its parent holds admission
    # until READY. Serialize config/record publication and compare the original
    # nonce instead, so delayed preflight cannot overwrite a later startup.
    with RecordLock(base / "config.json"), RecordLock(base / "service.json"):
        if read(base / "config.json") != initial_config:
            raise ValueError("public check configuration changed during startup")
        if (base / "service.json").exists() or (base / "service.json").is_symlink():
            raise ValueError("public check service already has an ownership record")
        atomic_write_json(base / "config.json", config)
        atomic_write_json(base / "service.json", record)
    cancel = threading.Event()
    pending, seen = {}, set()
    clean = True
    with ThreadPoolExecutor(max_workers=config["policy"]["max_parallel_checks"]) as executor:
        try:
            while True:
                with runtime_admission_lock(root / ".harness-runtime"):
                    current = _require_service_owner(config, owner)
                    if (read(root / ".harness-runtime/RUNTIME_STATE.json").get("state") != "OPEN"
                            or current.get("stop_requested") or current.get("status") != "READY"):
                        break
                for future, key in list(pending.items()):
                    if future.done():
                        try:
                            clean = future.result().get("cleanup_proven") is True and clean
                        except Exception as exc:
                            clean = False
                            with (base / "errors.jsonl").open("a", encoding="utf-8") as log:
                                log.write(json.dumps({"request": key, "error": str(exc)}) + "\n")
                        del pending[future]
                for registration in (base / "routes").glob("*.json"):
                    route = read(registration)
                    lane = Path(route["worktree"])
                    for request in (lane / ".agent-workspace/public-checks").glob("*/request.json"):
                        key = (route["run_id"], str(request))
                        if key in seen or len(pending) >= config["policy"]["max_parallel_checks"]:
                            continue
                        try:
                            _registered_request(route, request)
                        except (OSError, ValueError, KeyError, TypeError):
                            # Old run registrations remain as evidence, but cannot
                            # publish into a resumed invocation's request directory.
                            continue
                        seen.add(key)
                        pending[executor.submit(serve_request, config, route, request, cancel,
                                                service_owner=owner)] = key
                time.sleep(0.2)
        finally:
            cancel.set()
            for future in pending:
                try:
                    clean = future.result().get("cleanup_proven") is True and clean
                except Exception:
                    clean = False
    record.update(status="STOPPED", cleanup_proven=clean, stopped_at=iso_utc())
    with runtime_admission_lock(root / ".harness-runtime"), RecordLock(base / "service.json"):
        current = _require_service_owner(config, owner)
        record["stop_requested"] = current.get("stop_requested", False)
        atomic_write_json(base / "service.json", record)


def stop_service(root: Path, harness: Path | None = None) -> dict | None:
    path = home(root) / "service.json"
    if not path.is_file():
        if path.exists() or (home(root) / "config.json").exists() or (harness is not None and policy(harness) is not None):
            raise ValueError("expected public check service record is missing or invalid")
        return
    with runtime_admission_lock(root / ".harness-runtime"), RecordLock(path):
        record = read(path)
        identity = _service_identity(record, root, harness)
        startup_id = record.get("startup_id")
        record["stop_requested"] = True
        atomic_write_json(path, record)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        current = read(path)
        if (_service_identity(current, root, harness) != identity
                or current.get("startup_id") != startup_id):
            raise ValueError("public check service identity changed during shutdown")
        if processes.identity_exit_proven(*identity):
            if current.get("status") != "STOPPED" or current.get("cleanup_proven") is not True:
                raise ValueError("public check service cleanup is not proven")
            return current
        time.sleep(0.1)
    raise ValueError("public check service failed to stop within its cleanup window")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    run_service(parser.parse_args().root.resolve())
