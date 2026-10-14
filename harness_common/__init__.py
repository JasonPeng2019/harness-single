"""Shared infrastructure used by the harness and deterministic watcher."""

from .platform_paths import cgroup_root, procfs_root
from .process_identity import exact_process_identity

__all__ = ["cgroup_root", "procfs_root", "exact_process_identity"]
