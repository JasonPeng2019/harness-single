"""Protected linked-worktree assignment and operator Git reads.

Workers need a writable private Git directory for atomic lock creation. That
directory also contains redirects: ROOT pins them before launch and selects the
protected common repository explicitly, rather than trusting worker Git config.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import stat
import subprocess
from typing import Any

from .core import content_hash
from .mutation import read_bytes as anchored_read
from .portable_paths import repository_parts

SCHEMA = "linked-git-source/v1"


def environment(*, index: Path | None = None) -> dict[str, str]:
    """Discard inherited repository/config/command redirects for operator reads."""
    result = {key: value for key, value in os.environ.items()
              if not key.upper().startswith("GIT_")}
    result.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                  GIT_OPTIONAL_LOCKS="0", GIT_NO_LAZY_FETCH="1")
    if index is not None:
        result["GIT_INDEX_FILE"] = str(index)
    return result


def root_git(root: Path, *arguments: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "--no-replace-objects", "--no-lazy-fetch", "-C", str(root),
         "-c", f"core.hooksPath={os.devnull}", "-c", "core.fsmonitor=false", *arguments],
        env=environment(), capture_output=True, timeout=20,
    )


def _checked(completed: subprocess.CompletedProcess[bytes]) -> bytes:
    if completed.returncode:
        raise ValueError(f"Git source observation failed: {completed.stderr.decode('utf-8', 'replace').strip()}")
    return completed.stdout


def _plain(path: Path) -> None:
    for component in (path, *path.parents):
        try:
            metadata = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode) or getattr(metadata, "st_file_attributes", 0) & 0x400:
            raise ValueError(f"linked Git metadata path is not plain: {component}")


def _identity(path: Path) -> list[int]:
    _plain(path)
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"Git source directory is not a directory: {path}")
    return [metadata.st_dev, metadata.st_ino]


def _read(directory: Path, name: str) -> bytes:
    return anchored_read(directory, name)


def _alternates(common: Path) -> str | None:
    try:
        value = anchored_read(common, "objects/info/alternates")
    except FileNotFoundError:
        return None
    return hashlib.sha256(value).hexdigest()


def _attributes(common: Path) -> str | None:
    try:
        value = anchored_read(common, "info/attributes")
    except FileNotFoundError:
        return None
    return hashlib.sha256(value).hexdigest()


def _path_value(value: bytes, parent: Path) -> Path:
    text = value.decode("utf-8").strip()
    if not text or "\n" in text or "\r" in text:
        raise ValueError("linked Git metadata path is empty or multiline")
    selected = Path(text)
    selected = selected if selected.is_absolute() else parent / selected
    selected = selected.absolute()
    _plain(selected)
    return selected.resolve()


def common_directory(root: Path) -> Path:
    selected = _path_value(_checked(root_git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")), root)
    _identity(selected)
    return selected


def namespace(branch_ref: str) -> str:
    if not branch_ref.startswith("refs/heads/"):
        raise ValueError("worker must remain on its assigned branch")
    parts = repository_parts(branch_ref.removeprefix("refs/heads/"))
    if len(parts) < 2:
        raise ValueError("isolated worker branch needs a dedicated parent namespace, for example lane/<lane>/work")
    return "refs/heads/" + "/".join(parts[:-1])


def require_exclusive_namespace(root: Path, branch_ref: str, *, rt: Path | None = None) -> str:
    """Never give a worker directory-write capability over ROOT or a peer ref."""
    selected = namespace(branch_ref)
    if root_git(root, "check-ref-format", branch_ref).returncode:
        raise ValueError("worker branch name is invalid")
    refs = _checked(root_git(root, "for-each-ref", "--format=%(refname)", "refs/heads/")).decode().splitlines()
    _exclusive_refs(refs, branch_ref, selected)
    common = common_directory(root)
    for relative in (selected, "logs/" + selected):
        _plain(common / relative)
    if rt is not None:
        from .lanes import retained_lanes
        epochs = rt / "epochs"
        if epochs.exists():
            _identity(epochs)
            for epoch in epochs.iterdir():
                _identity(epoch)
                for lane in retained_lanes(rt, epoch.name).values():
                    source = lane.get("git_source")
                    if not source or not lane.get("git_write_namespace"):
                        continue
                    if Path(source["common_dir"]) != common:
                        continue
                    prior = lane["git_write_namespace"].casefold()
                    proposed = selected.casefold()
                    if prior == proposed or prior.startswith(proposed + "/") or proposed.startswith(prior + "/"):
                        raise ValueError("worker branch namespace is reserved by a retained lane; choose a fresh dedicated parent")
    return selected


def _exclusive_refs(refs: list[str], branch_ref: str, selected: str) -> None:
    selected_folded = selected.casefold() + "/"
    for ref in refs:
        if ref != branch_ref and (ref.casefold() == selected.casefold() or ref.casefold().startswith(selected_folded)):
            raise ValueError(f"worker branch namespace contains another branch: {ref}; choose a dedicated parent")


def capture(root: Path, worktree: Path, *, branch: str | None = None) -> dict[str, Any]:
    root, worktree = root.absolute(), worktree.absolute()
    root_identity, worktree_identity = _identity(root), _identity(worktree)
    common = common_directory(root)
    pointer = _read(worktree, ".git")
    if not pointer.startswith(b"gitdir: "):
        raise ValueError("worker Git metadata is not a linked worktree")
    private = _path_value(pointer[len(b"gitdir: "):], worktree)
    if private.parent != common / "worktrees":
        raise ValueError("worker Git metadata is not a linked worktree of ROOT")
    private_identity = _identity(private)
    redirects = {name: _read(private, name) for name in ("HEAD", "commondir", "gitdir")}
    if (_path_value(redirects["commondir"], private) != common
            or _path_value(redirects["gitdir"], private) != worktree / ".git"):
        raise ValueError("linked worktree common directory or backlink differs from its assignment")
    head = redirects["HEAD"].decode("utf-8").strip()
    if not head.startswith("ref: refs/heads/"):
        raise ValueError("worker must remain on its assigned branch")
    branch_ref = head.removeprefix("ref: ")
    repository_parts(branch_ref.removeprefix("refs/heads/"))
    if root_git(root, "check-ref-format", branch_ref).returncode:
        raise ValueError("worker branch name is invalid")
    if branch is not None and branch_ref != "refs/heads/" + branch:
        raise ValueError("worker branch differs from the operator assignment")
    source = {
        "schema": SCHEMA, "root_workspace": str(root), "root_identity": root_identity,
        "worktree": str(worktree), "worktree_identity": worktree_identity,
        "common_dir": str(common), "common_identity": _identity(common),
        "objects_identity": _identity(common / "objects"), "alternates_sha256": _alternates(common),
        "attributes_sha256": _attributes(common),
        "private_dir": str(private), "private_identity": private_identity,
        "branch_ref": branch_ref, "pointer_sha256": hashlib.sha256(pointer).hexdigest(),
        "redirect_sha256": {name: hashlib.sha256(value).hexdigest() for name, value in redirects.items()},
        "config_sha256": hashlib.sha256(_read(common, "config")).hexdigest(),
    }
    source["base_commit"] = _checked(_run(source, "rev-parse", "--verify", branch_ref + "^{commit}")).decode().strip()
    source["content_hash"] = content_hash(source)
    validate(source)
    return source


def validate(source: dict[str, Any]) -> None:
    if (not isinstance(source, dict) or source.get("schema") != SCHEMA
            or source.get("content_hash") != content_hash(source)):
        raise ValueError("protected Git assignment is missing or corrupt; reconcile or create a fresh lane")
    for name in ("root_workspace", "worktree", "common_dir", "private_dir"):
        selected = Path(source[name])
        if not selected.is_absolute():
            raise ValueError("protected Git assignment path must be absolute")
        identity_name = {"root_workspace": "root_identity", "worktree": "worktree_identity",
                         "common_dir": "common_identity", "private_dir": "private_identity"}[name]
        if _identity(selected) != source[identity_name]:
            raise ValueError(f"assigned Git source directory changed: {name}")
    private, common, worktree = (Path(source[name]) for name in ("private_dir", "common_dir", "worktree"))
    if private.parent != common / "worktrees":
        raise ValueError("assigned private Git directory is outside ROOT metadata")
    if (_identity(common / "objects") != source["objects_identity"]
            or _alternates(common) != source["alternates_sha256"]):
        raise ValueError("assigned Git object store or alternates changed")
    if _attributes(common) != source["attributes_sha256"]:
        raise ValueError("ROOT Git attributes changed after preparation")
    if hashlib.sha256(_read(worktree, ".git")).hexdigest() != source["pointer_sha256"]:
        raise ValueError("assigned worktree Git pointer changed")
    for name, digest in source["redirect_sha256"].items():
        if hashlib.sha256(_read(private, name)).hexdigest() != digest:
            raise ValueError(f"assigned private Git redirect changed: {name}")
    if set(source["redirect_sha256"]) != {"HEAD", "commondir", "gitdir"}:
        raise ValueError("protected Git redirects are incomplete")
    if hashlib.sha256(_read(common, "config")).hexdigest() != source["config_sha256"]:
        raise ValueError("ROOT Git configuration changed after preparation")
    if not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", source["base_commit"]):
        raise ValueError("protected Git origin is not an exact full commit")


def _run(source: dict[str, Any], *arguments: str, input_bytes: bytes | None = None,
         config_overrides: dict[str, str] | None = None) -> subprocess.CompletedProcess[bytes]:
    env = environment(index=Path(source["private_dir"]) / "index")
    if config_overrides:
        # Keep key/value separate: a valid Git subsection can contain '=' and
        # cannot safely be interpolated into a -c key=value argument.
        env["GIT_CONFIG_COUNT"] = str(len(config_overrides))
        for index, (key, value) in enumerate(config_overrides.items()):
            env[f"GIT_CONFIG_KEY_{index}"] = key
            env[f"GIT_CONFIG_VALUE_{index}"] = value
    return subprocess.run(
        ["git", "--no-replace-objects", "--no-lazy-fetch", "--git-dir", source["common_dir"],
         "--work-tree", source["worktree"], "-c", f"core.hooksPath={os.devnull}",
         "-c", "core.fsmonitor=false", "-c", "core.commitGraph=false", *arguments],
        env=env,
        input=input_bytes, capture_output=True, timeout=20,
    )


def git(source: dict[str, Any], *arguments: str, input_bytes: bytes | None = None,
        config_overrides: dict[str, str] | None = None) -> bytes:
    validate(source)
    result = _checked(_run(source, *arguments, input_bytes=input_bytes, config_overrides=config_overrides))
    validate(source)
    return result


def object_digest(kind: str, value: bytes, oid_length: int) -> str:
    digest = hashlib.sha1(usedforsecurity=False) if oid_length == 40 else hashlib.sha256()
    digest.update(f"{kind} {len(value)}\0".encode("ascii"))
    digest.update(value)
    return digest.hexdigest()


def verified_object(source: dict[str, Any], kind: str, oid: str) -> bytes:
    if kind not in {"commit", "tree", "blob"} or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", oid):
        raise ValueError("Git object identity is not exact")
    value = git(source, "cat-file", kind, oid)
    if object_digest(kind, value, len(oid)) != oid:
        raise ValueError(f"Git {kind} bytes do not match their object identity")
    return value


def verify_objects(source: dict[str, Any], kind: str, oids: list[str]) -> None:
    """Verify object contents in one native Git read for normal source trees."""
    unique = list(dict.fromkeys(oids))
    if not unique:
        return
    if kind not in {"commit", "tree", "blob"} or any(not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", oid) for oid in unique):
        raise ValueError("Git batch object identities are invalid")
    output = git(source, "cat-file", "--batch", input_bytes=("\n".join(unique) + "\n").encode("ascii"))
    offset = 0
    for oid in unique:
        end = output.index(b"\n", offset)
        fields = output[offset:end].decode("ascii").split()
        if len(fields) != 3 or fields[:2] != [oid, kind]:
            raise ValueError("Git batch returned a missing, foreign or wrong-kind object")
        size = int(fields[2])
        start = end + 1
        value = memoryview(output)[start:start + size]
        if size < 0 or len(value) != size or output[start + size:start + size + 1] != b"\n":
            raise ValueError("Git batch object is truncated")
        if object_digest(kind, value, len(oid)) != oid:
            raise ValueError(f"Git {kind} bytes do not match their object identity")
        offset = start + size + 1
    if offset != len(output):
        raise ValueError("Git batch returned extra objects")


def tree_entries(source: dict[str, Any], tip: str) -> dict[str, tuple[str, str]]:
    """Read verified tree objects, rather than trusting object-store filenames."""
    commit = verified_object(source, "commit", tip)
    first = commit.split(b"\n", 1)[0]
    if not first.startswith(b"tree "):
        raise ValueError("Git commit has no tree")
    root_tree = first[5:].decode("ascii")
    width = len(tip) // 2
    result: dict[str, tuple[str, str]] = {}
    pending = [("", root_tree)]
    from .portable_paths import admit_entry
    nodes: dict[str, tuple[str, str]] = {}
    while pending:
        prefix, oid = pending.pop()
        tree = verified_object(source, "tree", oid)
        offset = 0
        while offset < len(tree):
            end = tree.index(b"\0", offset)
            mode, name = tree[offset:end].split(b" ", 1)
            digest = tree[end + 1:end + 1 + width]
            if len(digest) != width:
                raise ValueError("Git tree object is truncated")
            offset = end + 1 + width
            relative = prefix + name.decode("utf-8")
            parts = repository_parts(relative)
            if mode == b"40000":
                key = relative.casefold()
                if key in nodes:
                    raise ValueError("Git tree directories alias or conflict")
                nodes[key] = relative, "directory"
                pending.append((relative + "/", digest.hex()))
            elif mode in {b"100644", b"100755", b"120000", b"160000"}:
                admit_entry(nodes, parts)
                result[relative] = (mode.decode("ascii"), digest.hex())
            else:
                raise ValueError("Git tree contains an unsupported entry mode")
    return result


def current_tip(lane: dict[str, Any]) -> str:
    source = lane.get("git_source")
    validate(source)
    if Path(lane["worktree_path"]).absolute() != Path(source["worktree"]):
        raise ValueError("protected Git assignment differs from the lane worktree")
    if lane.get("git_write_namespace") is not None:
        selected = namespace(source["branch_ref"])
        if selected != lane["git_write_namespace"]:
            raise ValueError("protected Git namespace differs from the assigned branch")
        refs = git(source, "for-each-ref", "--format=%(refname)", "refs/heads/").decode().splitlines()
        _exclusive_refs(refs, source["branch_ref"], selected)
    tip = git(source, "rev-parse", "--verify", source["branch_ref"] + "^{commit}").decode().strip()
    if not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", tip):
        raise ValueError("assigned branch has no exact full commit")
    ancestry = _run(source, "merge-base", "--is-ancestor", source["base_commit"], tip)
    if ancestry.returncode:
        raise ValueError("assigned branch no longer descends from its protected origin")
    verified_object(source, "commit", source["base_commit"])
    if tip != source["base_commit"]:
        verify_objects(source, "commit", git(source, "rev-list", source["base_commit"] + ".." + tip).decode().splitlines())
    validate(source)
    return tip
