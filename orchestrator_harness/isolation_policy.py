"""Shared native read denials for coding workers and public checks.

Windows materializes these restrictions on a persistent sandbox group. Profiles
in one workspace therefore use the same deny set; a check must not deny source,
Git administration or executable snapshots required by another profile.
"""
from __future__ import annotations

import os
from pathlib import Path


def shared_read_denials(
    harness_root: Path,
    root_workspace: Path,
    manager_workspace: Path,
    *,
    home: Path | None = None,
    codex_home: Path | None = None,
) -> set[Path]:
    """Return workspace/operator and credential restrictions for both profiles.

    Callers retain their own authority/plain-path validation. Optional home
    arguments let the trusted operator adapter bind the same native environment
    explicitly instead of inheriting its service process's private HOME.
    """
    home = (home if home is not None else Path.home()).resolve()
    codex_home = (codex_home if codex_home is not None else
                  Path(os.environ.get("CODEX_HOME", home / ".codex"))).resolve()
    root_workspace = root_workspace.resolve()
    manager_workspace = Path(os.path.abspath(manager_workspace))
    denied = {
        codex_home, (home / ".codex").resolve(), harness_root.resolve(),
        root_workspace / ".harness-runtime" / "operator-only",
        root_workspace / ".secrets",
        *(manager_workspace / leaf for leaf in (".codex", ".claude", ".qwen", ".agent-workspace")),
        *((home / leaf).resolve() for leaf in (".ssh", ".aws", ".azure", ".config/gcloud", ".docker",
                                   ".kube", ".npmrc", ".netrc", ".git-credentials", ".claude", ".qwen")),
    }
    if manager_workspace.resolve() != root_workspace:
        denied.add((manager_workspace / ".git").resolve())
    if root_workspace.parent.is_dir():
        denied.update(sibling.resolve() for sibling in root_workspace.parent.iterdir()
                      if sibling.is_dir() and sibling.resolve() != root_workspace)
    # A run-local harness may share an ancestor with the product. Specific
    # sibling restrictions remain, but no profile can deny a product ancestor.
    return {path for path in denied
            if path != root_workspace and path not in root_workspace.parents}


def require_git_read_access(denied: set[Path], common_dir: Path, private_dir: Path) -> None:
    """Refuse Git storage whose required reads a shared denial would prevent."""
    for target in (common_dir.resolve(), private_dir.resolve()):
        for path in sorted(denied, key=lambda value: str(value).casefold()):
            if path == target or path in target.parents:
                raise ValueError(f"Git metadata {target} is inside denied path {path}; "
                                 "place the common Git directory inside the product workspace, "
                                 "outside operator-only and .secrets")
