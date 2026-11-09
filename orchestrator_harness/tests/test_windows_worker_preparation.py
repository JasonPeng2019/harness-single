"""Model-free tests for explicit Windows worker capability preparation."""

from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import windows_worker_preparation as preparation


class WindowsWorkerPreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.harness = self.base / "harness"
        self.runtime = self.harness / "runtime"
        self.runtime.mkdir(parents=True)
        self.home = self.base / "codex-home"
        self.home.mkdir()
        self.env = patch.dict(os.environ, {"CODEX_HOME": str(self.home)})
        self.env.start()
        self.addCleanup(self.env.stop)

    def profile(self, name: str, *, extra_deny: str | None = None) -> Path:
        worktree = self.base / name
        git = worktree / ".git"
        worktree.mkdir()
        git.mkdir()
        write_roots = [
            git / "objects",
            git / "worktrees" / name,
            git / "refs" / "heads" / "lane" / name,
            git / "logs" / "refs" / "heads" / "lane" / name,
        ]
        for path in write_roots:
            path.mkdir(parents=True)
        private_temp = worktree / ".agent-workspace" / "tmp" / "run"
        private_temp.mkdir(parents=True)
        deny = [self.home]
        if extra_deny is not None:
            deny.append(self.base / extra_deny)
        lines = [
            'default_permissions = "worker-isolated"',
            "[permissions.worker-isolated]",
            'extends = ":workspace"',
            "[permissions.worker-isolated.filesystem]",
            *[f'{json.dumps(str(path))} = "deny"' for path in deny],
            *[f'{json.dumps(str(path))} = "write"' for path in write_roots],
            '[permissions.worker-isolated.filesystem.":workspace_roots"]',
            '"." = "write"',
        ]
        config = worktree / ".codex" / "config.toml"
        config.parent.mkdir()
        config.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return config

    def target(self, config: Path) -> preparation.WorkerProfileTarget:
        worktree = config.parent.parent
        return preparation.WorkerProfileTarget(
            config_path=config,
            worktree=worktree,
            temporary=worktree / ".agent-workspace" / "tmp" / "run",
            permission_mode="worker-isolated",
        )

    def test_mutex_key_is_stable_and_codex_home_scoped(self) -> None:
        one = preparation.codex_home_mutex_name(self.home)
        self.assertEqual(one, preparation.codex_home_mutex_name(self.home))
        self.assertNotEqual(one, preparation.codex_home_mutex_name(self.base))
        self.assertTrue(one.startswith("Local\\CodexSandboxPrepare-"))

    def test_strict_batch_refuses_incompatible_global_deny_sets(self) -> None:
        first = self.profile("worker-a")
        second = self.profile("worker-b", extra_deny="other-secret")
        with self.assertRaisesRegex(
            preparation.WorkerPreparationError, "incompatible global read-denial sets"
        ):
            preparation.validate_batch_profiles(
                [self.target(first), self.target(second)], "worker-isolated"
            )

    def test_duplicate_profiles_are_rejected_before_native_setup(self) -> None:
        profile = self.profile("worker-a")
        with self.assertRaisesRegex(
            preparation.WorkerPreparationError, "more than once"
        ):
            preparation.validate_batch_profiles(
                [self.target(profile), self.target(profile)], "worker-isolated"
            )

    def test_capability_mapping_requires_current_sid_allow_ace(self) -> None:
        profile = self.profile("worker-a")
        record = preparation.validate_batch_profiles(
            [self.target(profile)], "worker-isolated"
        )[0]
        roots = [record["worktree"], *record["write_roots"]]
        sid = "S-1-5-21-1-2-3-1001"
        cap = {
            "workspace_by_cwd": {preparation._canonical_path(roots[0]): sid},
            "writable_root_by_path": {
                preparation._canonical_path(path): sid for path in roots[1:]
            },
        }
        cap_file = self.home / "cap_sid"
        cap_file.write_text(json.dumps(cap), encoding="utf-8")
        good_acl = {
            "path": str(roots[0]),
            "aces": [
                {
                    "sid": sid,
                    "type": "Allow",
                    "rights": "Modify, Synchronize",
                    "inherited": False,
                }
            ],
        }
        with (
            patch.object(preparation.os, "name", "nt"),
            patch.object(preparation, "_acl_snapshot", return_value=good_acl),
        ):
            evidence = preparation.inspect_profile_capabilities(record, self.home)
        self.assertEqual(5, len(evidence["observations"]))
        self.assertTrue(
            all(item["matching_write_ace"] for item in evidence["observations"])
        )
        denied_acl = {**good_acl, "aces": [{**good_acl["aces"][0], "type": "Deny"}]}
        with (
            patch.object(preparation.os, "name", "nt"),
            patch.object(preparation, "_acl_snapshot", return_value=denied_acl),
            self.assertRaisesRegex(
                preparation.WorkerPreparationError, "no matching write ACE"
            ),
        ):
            preparation.inspect_profile_capabilities(record, self.home)

    def test_batch_publishes_proofs_only_after_each_serial_noop_passes(self) -> None:
        first = self.profile("worker-a")
        second = self.profile("worker-b")
        cache = self.runtime / "local-appdata"
        cache.mkdir()
        native = self.base / "codex.exe"
        native.write_bytes(b"owned CLI fixture")
        marker = self.home / ".sandbox" / "setup_marker.json"
        marker.parent.mkdir()
        marker.write_text('{"fixture": true}', encoding="utf-8")
        output = self.runtime / "preparation"
        sequence: list[str] = []

        @contextlib.contextmanager
        def mutex(_codex_home: Path):
            sequence.append("mutex")
            yield

        def noop(record, *, invocation, persist, **kwargs):
            sequence.append(record["config_path"].parent.parent.name)
            invocation.update(
                {
                    "argv": [
                        str(native),
                        "sandbox",
                        "--",
                        os.fspath(Path(sys.executable)),
                        "-c",
                        "pass",
                    ],
                    "exit_code": 0,
                    "cleanup_proven": True,
                    "ended_at": "2026-01-01T00:00:00+00:00",
                    "pid": 1234,
                    "process_identity": {
                        "pid": 1234,
                        "creation_time": "windows-filetime:1",
                    },
                    "boundary": {
                        "kind": "windows-job",
                        "root": {"pid": 1234, "creation_time": "windows-filetime:1"},
                    },
                }
            )
            persist()

        capability = {
            "cap_sid_sha256": "a" * 64,
            "observations": [{"matching_write_ace": True}],
        }
        with (
            patch.object(preparation.os, "name", "nt"),
            patch.object(preparation, "_mutex", mutex),
            patch.object(preparation, "_run_noop", side_effect=noop),
            patch.object(
                preparation, "inspect_profile_capabilities", return_value=capability
            ),
            patch.object(preparation, "profile_contract_digest", return_value="b" * 64),
            patch.object(preparation, "_direct_codex_executable", return_value=native),
        ):
            proofs = preparation.prepare_profiles(
                self.harness,
                [self.target(first), self.target(second)],
                permission_mode="worker-isolated",
                local_appdata=cache,
                evidence_dir=output,
            )
        self.assertEqual(2, len(proofs))
        self.assertEqual(1, sequence.count("mutex"))
        self.assertEqual(
            "PASS", json.loads((output / "attempt.json").read_text())["status"]
        )
        for config in (first, second):
            proof_path = preparation.preparation_path(config)
            self.assertTrue(proof_path.is_file())
            self.assertEqual("PREPARED", json.loads(proof_path.read_text())["status"])

    def test_live_owned_strict_lane_with_different_denies_refuses_preparation(
        self,
    ) -> None:
        requested = self.profile("worker-requested")
        active = self.profile("worker-active", extra_deny="other-secret")
        records = preparation.validate_batch_profiles(
            [self.target(requested)], "worker-isolated"
        )
        records[0]["lane_id"] = "lane-requested"
        lane = {
            "lane_id": "lane-active",
            "lifecycle": "running",
            "provider": {"id": "codex"},
            "isolation_config_path": str(active),
            "worktree_path": str(active.parent.parent),
        }
        with (
            patch(
                "orchestrator_harness.config.load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime),
            ),
            patch(
                "orchestrator_harness.epochs.read_current_epoch",
                return_value={"epoch_id": "epoch"},
            ),
            patch(
                "orchestrator_harness.lanes.retained_lanes",
                return_value={"lane-active": lane},
            ),
            patch(
                "orchestrator_harness.lane_authority.read_invocation",
                return_value={
                    "provider": {
                        "launch_config": {"permission_mode": "worker-isolated"}
                    }
                },
            ),
            self.assertRaisesRegex(
                preparation.WorkerPreparationError, "beneath owned active Codex lane"
            ),
        ):
            preparation.validate_known_live_lane_compatibility(
                self.harness, records, "worker-isolated"
            )

    def test_authoritative_lane_profile_prepares_and_passes_launch_gate(self) -> None:
        from orchestrator_harness import config as harness_config
        from orchestrator_harness.tests.support import prepare_lane_fixture

        harness = self.base / "installed-harness"
        product = self.base / "product"
        worktree = product / "lane-one"
        harness.mkdir()
        product.mkdir()
        (harness / "harness-config.json").write_text(
            json.dumps({"root_workspace": str(product)}), encoding="utf-8"
        )
        runtime = harness_config.load_config(harness).runtime_root
        config_path = worktree / ".codex" / "config.toml"
        config_path.parent.mkdir(parents=True)
        branch_parent = (
            product / ".git" / "refs" / "heads" / "lane" / "epoch-1" / "lane-1"
        )
        log_parent = (
            product / ".git" / "logs" / "refs" / "heads" / "lane" / "epoch-1" / "lane-1"
        )
        metadata_roots = [
            product / ".git" / "objects",
            product / ".git" / "worktrees" / worktree.name,
            branch_parent,
            log_parent,
        ]
        config_path.write_text(
            'default_permissions = "worker-isolated"\n'
            "[permissions.worker-isolated]\n"
            'extends = ":workspace"\n'
            "[permissions.worker-isolated.filesystem]\n"
            f'{json.dumps(str(self.home))} = "deny"\n'
            + "".join(f'{json.dumps(str(path))} = "write"\n' for path in metadata_roots)
            + '[permissions.worker-isolated.filesystem.":workspace_roots"]\n"." = "write"\n',
            encoding="utf-8",
        )
        lane = prepare_lane_fixture(
            runtime,
            "epoch-1",
            {
                "lane_id": "lane-1",
                "run_id": "run-1",
                "branch": "lane/epoch-1/lane-1/work",
                "worktree_path": str(worktree),
                "provider": {
                    "id": "codex",
                    "launch_config": {"permission_mode": "worker-isolated"},
                },
            },
        )
        targets = preparation.resolve_active_profile_targets(harness, ["lane-1"])
        self.assertEqual(lane["isolation_config_path"], str(targets[0].config_path))
        self.assertEqual(worktree.resolve(), targets[0].worktree)
        self.assertEqual(
            worktree / ".agent-workspace" / "tmp" / "run-1", targets[0].temporary
        )
        cache = harness / "runtime" / "cache"
        native = self.base / "codex.exe"
        native.write_bytes(b"owned CLI fixture")
        marker = self.home / ".sandbox" / "setup_marker.json"
        marker.parent.mkdir()
        marker.write_text('{"fixture": true}', encoding="utf-8")

        @contextlib.contextmanager
        def mutex(_codex_home: Path):
            yield

        def noop(_record, *, invocation, persist, **_kwargs):
            invocation.update(
                {
                    "argv": [
                        str(native),
                        "sandbox",
                        "--",
                        os.fspath(Path(sys.executable)),
                        "-c",
                        "pass",
                    ],
                    "exit_code": 0,
                    "cleanup_proven": True,
                    "ended_at": "2026-01-01T00:00:00+00:00",
                    "pid": 1235,
                    "process_identity": {
                        "pid": 1235,
                        "creation_time": "windows-filetime:2",
                    },
                    "boundary": {
                        "kind": "windows-job",
                        "root": {"pid": 1235, "creation_time": "windows-filetime:2"},
                    },
                }
            )
            persist()

        batch_record = preparation.validate_batch_profiles(targets, "worker-isolated")[
            0
        ]
        capability_paths = [
            ("workspace_by_cwd", batch_record["worktree"]),
            *[("writable_root_by_path", path) for path in batch_record["write_roots"]],
        ]
        capability = {
            "cap_sid_sha256": "c" * 64,
            "observations": [
                {
                    "mapping": mapping,
                    "path": str(path),
                    "cap_sid": f"S-1-5-21-{index + 10}",
                    "matching_write_ace": True,
                }
                for index, (mapping, path) in enumerate(capability_paths)
            ],
        }
        with (
            patch.object(preparation, "_mutex", mutex),
            patch.object(preparation, "_run_noop", side_effect=noop),
            patch.object(
                preparation, "inspect_profile_capabilities", return_value=capability
            ),
            patch.object(preparation, "profile_contract_digest", return_value="d" * 64),
            patch.object(preparation, "_direct_codex_executable", return_value=native),
        ):
            proof = preparation.prepare_profiles(
                harness,
                targets,
                permission_mode="worker-isolated",
                local_appdata=cache,
                evidence_dir=harness / "runtime" / "preparation",
            )[0]
            gate = preparation.validate_prepared_profile(
                harness,
                product,
                targets[0].config_path,
                "worker-isolated",
                executable=native,
                codex_home=self.home,
                local_appdata=cache,
                temporary=targets[0].temporary,
            )
            unrelated_map = {
                **capability,
                "cap_sid_sha256": "e" * 64,
                "observations": [*capability["observations"]],
            }
            with patch.object(
                preparation, "inspect_profile_capabilities", return_value=unrelated_map
            ):
                preparation.validate_prepared_profile(
                    harness,
                    product,
                    targets[0].config_path,
                    "worker-isolated",
                    executable=native,
                    codex_home=self.home,
                    local_appdata=cache,
                    temporary=targets[0].temporary,
                )
            changed_sid = {
                **capability,
                "cap_sid_sha256": "f" * 64,
                "observations": [
                    {**capability["observations"][0], "cap_sid": "S-1-5-21-999"},
                    *capability["observations"][1:],
                ],
            }
            with (
                patch.object(
                    preparation,
                    "inspect_profile_capabilities",
                    return_value=changed_sid,
                ),
                self.assertRaisesRegex(
                    preparation.WorkerPreparationError,
                    "SID bindings changed for this profile",
                ),
            ):
                preparation.validate_prepared_profile(
                    harness,
                    product,
                    targets[0].config_path,
                    "worker-isolated",
                    executable=native,
                    codex_home=self.home,
                    local_appdata=cache,
                    temporary=targets[0].temporary,
                )
        self.assertEqual(str(targets[0].worktree), proof["worktree_path"])
        self.assertEqual("lane-1", proof["lane_id"])
        self.assertEqual(
            str(preparation.preparation_path(targets[0].config_path)),
            gate["proof_path"],
        )
        self.assertTrue(preparation.preparation_path(targets[0].config_path).is_file())


if __name__ == "__main__":
    unittest.main(verbosity=2)
