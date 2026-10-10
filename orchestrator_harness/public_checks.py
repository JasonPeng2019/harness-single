"""Restricted worker test service; executes task tools, never model workers."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time

from . import processes
from .core import iso_utc
from .records import RecordLock, atomic_write_json

POLICY = "local-config/public-check-policy.json"
TASK_INPUTS = ("src", "test_artifacts", "rfc8878.txt", "test.sh", "timer.sh")
FIXED_INPUTS = ("test_artifacts", "rfc8878.txt", "test.sh", "timer.sh", "src/Makefile", "src/harness.c")
BUILD = ["make", "-C", "/app/src"]
PUBLIC = ["bash", "-lc", "set -o pipefail; make -C /app/src && tr -d '\\r' < /app/test.sh | bash"]


def read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
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
    if value.get("schema") != "public-check-policy/v1" or value.get("task") != "zstd-decoder":
        raise ValueError("unsupported public check policy")
    if type(value.get("timeout_seconds")) is not int or not 1 <= value["timeout_seconds"] <= 600:
        raise ValueError("public check timeout must be 1..600 seconds")
    if type(value.get("max_parallel_checks")) is not int or not 1 <= value["max_parallel_checks"] <= 4:
        raise ValueError("public check parallelism must be 1..4")
    return value


def git(worktree: Path, *arguments: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(worktree), *arguments], timeout=20)


def bridge(harness: Path):
    spec = importlib.util.spec_from_file_location("run_public_bridge", harness / "tools/public_task_check.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _file_digests(worktree: Path, revision: str, paths=FIXED_INPUTS) -> dict:
    result = {}
    entries = git(worktree, "ls-tree", "-r", "-z", revision, "--", *paths).split(b"\0")
    for entry in entries:
        if not entry:
            continue
        metadata, filename = entry.split(b"\t", 1)
        mode, kind, digest = metadata.decode().split()
        if mode not in {"100644", "100755"} or kind != "blob":
            raise ValueError("public task inputs must be regular committed files")
        result[filename.decode("utf-8")] = digest
    return result


def snapshot(lane: Path, revision: str, destination: Path) -> None:
    top = git(lane, "ls-tree", "--name-only", revision).decode().splitlines()
    if not all(name in top for name in TASK_INPUTS):
        raise ValueError("committed public task inputs are missing")
    names = [*TASK_INPUTS, *(["scratch"] if "scratch" in top else [])]
    archived = git(lane, "archive", "--format=tar", revision, "--", *names)
    with tarfile.open(fileobj=io.BytesIO(archived)) as archive:
        for member in archive:
            relative = PurePosixPath(member.name)
            if (relative.is_absolute() or ".." in relative.parts or "\\" in member.name
                    or ":" in member.name or relative.parts[0] not in names):
                raise ValueError("unsafe public input archive path")
            target = destination.joinpath(*relative.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
            else:
                raise ValueError("links and special files are forbidden in public inputs")


def validate_service(root: Path, harness: Path, *, runtime_closed: bool = False) -> dict:
    record = read(home(root) / "service.json")
    if (record.get("schema") != "worker-public-check-service/v1"
            or Path(record.get("root", "")).resolve() != root.resolve()
            or Path(record.get("harness", "")).resolve() != harness.resolve()
            or record.get("status") != ("STOPPED" if runtime_closed else "READY")):
        raise ValueError("public check service is not healthy for this ROOT")
    identity_matches = processes.identity_matches(record.get("pid"), record.get("creation_time"))
    if runtime_closed:
        if (record.get("cleanup_proven") is not True or identity_matches
                or (processes.process_alive(record.get("pid"))
                    and processes.process_identity(record.get("pid")) is None)):
            raise ValueError("public check service cleanup is not proven for ROOT resume")
    elif not identity_matches:
        raise ValueError("public check service is not healthy for this ROOT")
    config = read(home(root) / "config.json")
    if config["policy"] != policy(harness):
        raise ValueError("public check policy changed after service startup")
    return {key: record[key] for key in ("schema", "pid", "creation_time", "root", "harness", "status")}


def start_service(harness: Path, root: Path) -> dict | None:
    selected = policy(harness)
    if selected is None:
        return None
    base = home(root)
    if (base / "service.json").exists():
        return validate_service(root, harness)
    if read(root / ".harness-runtime/RUNTIME_STATE.json").get("state") != "OPEN":
        raise ValueError("public checks require an OPEN native runtime")
    revision = git(root, "rev-parse", "HEAD").decode().strip()
    if not all((root / name).exists() for name in TASK_INPUTS):
        raise ValueError("ZSTD public inputs must be supplied before preparing this run")
    base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="preflight-", dir=base) as temporary:
        snapshot(root, revision, Path(temporary))
    selected_bridge = bridge(harness)
    actual_image = subprocess.check_output(
        ["docker", "image", "inspect", selected_bridge.IMAGE, "--format", "{{.Id}}"],
        text=True, timeout=20,
    ).strip()
    if actual_image != selected_bridge.IMAGE_ID:
        raise ValueError("pinned public task image is missing or changed")
    atomic_write_json(base / "config.json", {
        "root": str(root.resolve()), "harness": str(harness.resolve()), "policy": selected,
        "baseline_revision": revision, "fixed_inputs": _file_digests(root, revision),
    })
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = str(harness)
    # Test code enters Docker with no host environment variables or mounts.
    for key in list(environment):
        if key.startswith(("MEMORY_HARNESS_", "EVEROS_", "DEEPINFRA_")):
            environment.pop(key)
    with (base / "service.stdout.log").open("ab") as stdout, (base / "service.stderr.log").open("ab") as stderr:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", "orchestrator_harness.public_checks", "--root", str(root)],
            cwd=harness, env=environment, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
            creationflags=(subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP) if os.name == "nt" else 0,
        )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise ValueError(f"public check service exited during startup; inspect {base}")
        if (base / "service.json").is_file():
            return validate_service(root, harness)
        time.sleep(0.1)
    # The handle belongs to this exact new child; never use broad-name killing.
    process.terminate()
    process.wait(timeout=10)
    raise ValueError("public check service did not become ready")


def install_route(harness: Path, root: Path, lane: Path, lane_id: str, run_id: str) -> None:
    if policy(harness) is None:
        return
    validate_service(root, harness)
    lane = lane.resolve()
    if lane.parent.parent != root.resolve() / ".harness-runtime/worktrees":
        raise ValueError("public check route must select one exact native lane")
    helper = harness / "tools/worker_public_check.py"
    agent = lane / ".agent-workspace"
    shutil.copyfile(helper, agent / "public-check.py")
    record = {"schema": "worker-public-check-route/v1", "lane_id": lane_id,
              "run_id": run_id, "worktree": str(lane)}
    atomic_write_json(home(root) / "routes" / f"{run_id}.json", record)
    atomic_write_json(agent / "public-check-route.json", record)


def _command(request: dict) -> list[str]:
    check, command = request.get("check"), request.get("command")
    if check in {"build", "public"} and command == []:
        return BUILD if check == "build" else PUBLIC
    if (check == "focused" and isinstance(command, list) and 0 < len(command) <= 128
            and all(isinstance(arg, str) and arg and "\0" not in arg and len(arg) <= 16000 for arg in command)):
        return command
    raise ValueError("invalid public check operation")


def _registered_request(route: dict, path: Path) -> tuple[dict, str]:
    """Read only requests addressed to this route; never answer for another run."""
    lane = Path(route["worktree"])
    identifier = path.parent.name
    if not re.fullmatch(r"[a-f0-9]{32}", identifier):
        raise ValueError("invalid public check request ID")
    expected = lane / ".agent-workspace/public-checks" / identifier / "request.json"
    if path.is_symlink() or path.resolve() != expected.absolute() or path.stat().st_size > 65536:
        raise ValueError("public request escaped its registered lane or exceeded its size limit")
    raw = path.read_bytes()
    request = json.loads(raw)
    if not isinstance(request, dict):
        raise ValueError("public request must be a JSON object")
    expected_identity = {"request_id": identifier, "lane_id": route["lane_id"], "run_id": route["run_id"]}
    if any(request.get(field) != value for field, value in expected_identity.items()):
        raise ValueError("public request identity mismatch")
    return request, hashlib.sha256(raw).hexdigest()


def serve_request(config: dict, route: dict, path: Path, cancel: threading.Event, runner=None) -> dict:
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
                if not isinstance(revision, str) or not re.fullmatch(r"[a-f0-9]{40}", revision):
                    raise ValueError("an exact full Git revision is required")
                if git(lane, "rev-parse", "HEAD").decode().strip() != revision:
                    raise ValueError("requested revision is no longer the lane tip")
                if _file_digests(lane, revision) != config["fixed_inputs"]:
                    raise ValueError("fixed public task inputs or Makefile changed")
                command = _command(request)
                result["command"] = command
                with tempfile.TemporaryDirectory(prefix="snapshot-", dir=home(root)) as temporary:
                    selected = Path(temporary)
                    snapshot(lane, revision, selected)
                    result["cleanup_proven"] = False
                    checked = (runner or bridge(harness).run_public_command)(
                        selected, command, timeout=config["policy"]["timeout_seconds"], cancel=cancel,
                    )
                    result.update(checked)
                    result["status"] = "PASS" if result["exit_code"] == 0 else "FAIL"
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                result["error"] = str(exc)
            result["completed_at"] = iso_utc()
            atomic_write_json(authoritative, result)
        if expected.parent.resolve() != expected.parent.absolute():
            raise ValueError("public response directory escaped its lane")
        atomic_write_json(expected.parent / "result.json", result)
    return result


def run_service(root: Path) -> None:
    base = home(root)
    config = read(base / "config.json")
    identity = processes.process_identity(os.getpid())
    if identity is None:
        raise ValueError("cannot bind public test service process identity")
    record = {"schema": "worker-public-check-service/v1", "root": config["root"],
              "harness": config["harness"], "pid": os.getpid(),
              "creation_time": identity["creation_time"], "status": "READY",
              "started_at": iso_utc(), "stop_requested": False, "cleanup_proven": False}
    atomic_write_json(base / "service.json", record)
    cancel = threading.Event()
    pending, seen = {}, set()
    clean = True
    with ThreadPoolExecutor(max_workers=config["policy"]["max_parallel_checks"]) as executor:
        try:
            while True:
                if (read(root / ".harness-runtime/RUNTIME_STATE.json").get("state") != "OPEN"
                        or read(base / "service.json").get("stop_requested")):
                    break
                for future, key in list(pending.items()):
                    if future.done():
                        try:
                            clean = future.result().get("cleanup_proven") is True and clean
                        except Exception as exc:
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
                        pending[executor.submit(serve_request, config, route, request, cancel)] = key
                time.sleep(0.2)
        finally:
            cancel.set()
            for future in pending:
                try:
                    clean = future.result().get("cleanup_proven") is True and clean
                except Exception:
                    clean = False
    record.update(status="STOPPED", cleanup_proven=clean, stopped_at=iso_utc())
    atomic_write_json(base / "service.json", record)


def stop_service(root: Path) -> None:
    path = home(root) / "service.json"
    if not path.is_file():
        return
    with RecordLock(path):
        record = read(path)
        record["stop_requested"] = True
        atomic_write_json(path, record)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        current = read(path)
        if not processes.identity_matches(current.get("pid"), current.get("creation_time")):
            if processes.process_alive(current.get("pid")) and processes.process_identity(current["pid"]) is None:
                raise ValueError("public check service process identity is unprovable")
            if current.get("status") != "STOPPED" or current.get("cleanup_proven") is not True:
                raise ValueError("public check service cleanup is not proven")
            return
        time.sleep(0.1)
    raise ValueError("public check service failed to stop within its cleanup window")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    run_service(parser.parse_args().root.resolve())
