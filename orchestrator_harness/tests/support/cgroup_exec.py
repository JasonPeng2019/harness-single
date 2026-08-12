from __future__ import annotations

import argparse
import os
import re
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Join one pre-created cgroup-v2 subtree, then exec an exact command."
    )
    parser.add_argument("--cgroup", required=True)
    parser.add_argument("--network-namespace")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError("cgroup launcher must run as root")
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise RuntimeError("no command supplied")
    if args.network_namespace is not None:
        if not re.fullmatch(r"oh-[0-9a-f]{8}", args.network_namespace):
            raise RuntimeError("network namespace identity is invalid")
        command = ["ip", "netns", "exec", args.network_namespace, *command]
    cgroup = Path(args.cgroup).resolve()
    if cgroup.parent != Path("/sys/fs/cgroup"):
        raise RuntimeError(f"cgroup must be a direct /sys/fs/cgroup child: {cgroup}")
    (cgroup / "cgroup.procs").write_text(f"{os.getpid()}\n", encoding="ascii")
    os.execvp(command[0], command)
    raise AssertionError("exec returned")


if __name__ == "__main__":
    raise SystemExit(main())
