"""Explicitly prepare exact generated Windows Codex worker profiles."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", type=Path, required=True)
    parser.add_argument(
        "--profile",
        type=Path,
        action="append",
        help="exact profile path for an owned probe; repeat with matching --worktree/--temp-dir",
    )
    parser.add_argument(
        "--worktree",
        type=Path,
        action="append",
        help="explicit worktree paired with each raw --profile; never inferred from profile location",
    )
    parser.add_argument(
        "--lane-id",
        action="append",
        help="active Codex lane ID; repeat to prepare its operator-validated protected profile",
    )
    parser.add_argument(
        "--permission-mode",
        choices=("worker-isolated", "worker-workspace"),
        required=True,
    )
    parser.add_argument(
        "--local-appdata",
        type=Path,
        required=True,
        help="one explicit LOCALAPPDATA cache shared by this harness installation",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        action="append",
        help="exact private per-invocation TEMP path matching each profile, in profile order",
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        required=True,
        help="fresh output path below harness runtime/",
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    if not 10 <= args.timeout_seconds <= 600:
        parser.error("--timeout-seconds must be 10..600")
    harness = args.harness_dir.resolve()
    sys.path.insert(0, str(harness))
    from orchestrator_harness.windows_worker_preparation import (
        WorkerPreparationError,
        WorkerProfileTarget,
        prepare_profiles,
        resolve_active_profile_targets,
    )

    try:
        if args.lane_id:
            if args.profile or args.worktree or args.temp_dir:
                parser.error(
                    "--lane-id cannot be combined with raw --profile/--worktree/--temp-dir"
                )
            targets = resolve_active_profile_targets(harness, args.lane_id)
        else:
            if not args.profile or not args.worktree or not args.temp_dir:
                parser.error(
                    "raw probe mode requires equal repeated --profile, --worktree, and --temp-dir inputs"
                )
            if not (len(args.profile) == len(args.worktree) == len(args.temp_dir)):
                parser.error(
                    "raw probe mode requires one --worktree and --temp-dir per --profile"
                )
            targets = [
                WorkerProfileTarget(
                    config_path=profile,
                    worktree=worktree,
                    temporary=temporary,
                    permission_mode=args.permission_mode,
                )
                for profile, worktree, temporary in zip(
                    args.profile, args.worktree, args.temp_dir, strict=True
                )
            ]
        proofs = prepare_profiles(
            harness,
            targets,
            permission_mode=args.permission_mode,
            local_appdata=args.local_appdata,
            evidence_dir=args.evidence_dir,
            timeout_seconds=args.timeout_seconds,
        )
    except (OSError, ValueError, WorkerPreparationError) as exc:
        print(f"WINDOWS_WORKER_PREPARATION_FAILED: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "schema": "windows-worker-preparation-result/v1",
                "status": "PREPARED",
                "proofs": [
                    {
                        "profile_path": proof["profile_path"],
                        "proof_path": str(
                            Path(proof["profile_path"]).with_name(
                                Path(proof["profile_path"]).name + ".preparation.json"
                            )
                        ),
                        "profile_sha256": proof["profile_sha256"],
                        "permission_mode": proof["permission_mode"],
                    }
                    for proof in proofs
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
