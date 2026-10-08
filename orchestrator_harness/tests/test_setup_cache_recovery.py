"""Recovery and overwrite contracts for the installed super-cache boundary."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import bootstrap, setup
from orchestrator_harness.tests.test_v2_materialization import MaterializationFixture


class SetupCacheRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = MaterializationFixture()
        self.addCleanup(self.fixture.close)
        self.fixture.add_shared_root_configs()
        self.runtime = self.fixture.root_workspace / ".harness-runtime"
        self.destination = self.runtime / "super-cache"
        self.backup = self.runtime / "super-cache.old"
        self.journal = self.runtime / "super-cache.transaction.json"
        self.source = self.fixture.harness / "super-cache" / "custom" / "README.md"
        self.unrelated = self.fixture.root_workspace / ".super-cache.user"
        self.unrelated.write_bytes(b"preserve this file\n")

    def _setup(self, *, overwrite: bool = False) -> dict:
        with (
            patch.object(setup, "find_harness_root", return_value=self.fixture.harness),
            patch.object(setup, "_start_monitor"),
        ):
            return setup.run_setup(overwrite=overwrite)

    @staticmethod
    def _bytes(root: Path) -> dict[str, bytes]:
        return {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        }

    def _assert_clean_boundary(self) -> None:
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.journal.exists())
        self.assertEqual([], list(self.runtime.glob(".super-cache.*")))
        setup._validate_active_cache(
            setup._plan_active_cache(self.fixture.harness), self.destination
        )

    @staticmethod
    def _manifest(root: Path) -> dict:
        entries = list(root.rglob("*"))
        return {
            "directories": sorted(path.relative_to(root).as_posix() for path in entries if path.is_dir()),
            "files": {
                path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in entries if path.is_file()
            },
        }

    def _record_transaction(self, stage: Path, *, previous: dict | None = None, target: dict | None = None) -> None:
        self.journal.write_text(json.dumps({
            "schema": "super-cache-transaction/v1",
            "runtime_root": str(self.runtime.absolute()),
            "transaction_id": "a" * 32,
            "stage_name": stage.name,
            "previous": previous,
            "target": target or self._manifest(self.destination),
        }), encoding="utf-8")

    def _blocked_payload(self) -> None:
        with self.assertRaises(setup.SetupError):
            setup._validated_worker_payload(self.fixture.harness, self.runtime, "codex")
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap._managed_payload(self.fixture.harness, self.runtime, "codex")
        result = self._bootstrap_preflight()
        self.assertFalse(result["ok"], result)
        self.assertIn(result["code"], (bootstrap.BOOTSTRAP_CACHE_MISSING, bootstrap.BOOTSTRAP_CACHE_COLLISION))

    def _bootstrap_preflight(self) -> dict:
        with (
            patch("orchestrator_harness.config.find_harness_root", return_value=self.fixture.harness),
            patch.object(bootstrap, "open_epoch") as open_epoch,
        ):
            result = bootstrap.run_bootstrap(
                lane_id="recovery-check",
                provider="codex",
                model="test-model",
                launch_config={"reasoning_effort": "high", "service_tier": "priority"},
                exclusive_resources=[],
                task_card_path=str(self.runtime / "unused-task-card.json"),
            )
        open_epoch.assert_not_called()
        return result

    def test_failed_stage_or_first_rename_keeps_prior_cache_and_next_setup_repairs(self) -> None:
        for failure in ("copy", "destination-to-backup", "destination-to-backup-after"):
            with self.subTest(failure=failure):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                runtime = fixture.root_workspace / ".harness-runtime"
                destination = fixture.active_cache()
                before = self._bytes(destination)
                source = fixture.harness / "super-cache" / "custom" / "README.md"
                source.write_bytes(b"updated catalog\n")
                real_copy = shutil.copy2
                real_replace = setup._replace_with_retry

                def copy(source_path: Path, target: Path) -> Path:
                    if source_path == source:
                        raise OSError("injected staging failure")
                    return real_copy(source_path, target)

                def replace(source_path: Path, target: Path) -> None:
                    if source_path == destination:
                        if failure.endswith("-after"):
                            real_replace(source_path, target)
                        raise OSError("injected first rename failure")
                    real_replace(source_path, target)

                with patch.object(
                    setup.shutil if failure == "copy" else setup,
                    "copy2" if failure == "copy" else "_replace_with_retry",
                    side_effect=copy if failure == "copy" else replace,
                ):
                    with self.assertRaises(setup.SetupError) as raised:
                        setup._install_active_cache(
                            fixture.harness,
                            runtime,
                            plan=setup._plan_active_cache(fixture.harness),
                            overwrite=True,
                        )
                self.assertEqual(setup.SETUP_OVERWRITE_FAILED, raised.exception.code)
                self.assertEqual(before, self._bytes(destination))
                self.assertFalse((runtime / "super-cache.old").exists())
                self.assertEqual([], list(runtime.glob(".super-cache.*")))
                with (
                    patch.object(setup, "find_harness_root", return_value=fixture.harness),
                    patch.object(setup, "_start_monitor"),
                ):
                    recovered = setup.run_setup(overwrite=True)
                self.assertTrue(recovered["ok"], recovered)
                setup._validate_active_cache(setup._plan_active_cache(fixture.harness), destination)

    def test_failed_promotion_or_post_validation_rolls_back_prior_bytes(self) -> None:
        for failure in (
            "staged-to-destination",
            "staged-to-destination-after",
            "post-validation",
        ):
            with self.subTest(failure=failure):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                runtime = fixture.root_workspace / ".harness-runtime"
                destination = fixture.active_cache()
                before = self._bytes(destination)
                source = fixture.harness / "super-cache" / "custom" / "README.md"
                source.write_bytes(b"updated catalog\n")
                real_replace = setup._replace_with_retry
                real_validate = setup._validate_active_cache

                def replace(source_path: Path, target: Path) -> None:
                    if target == destination and source_path.name.startswith(".super-cache."):
                        if failure.endswith("-after"):
                            real_replace(source_path, target)
                        raise OSError("injected promotion failure")
                    real_replace(source_path, target)

                def validate(plan: list[tuple[Path, Path]], target: Path) -> None:
                    if target == destination and (target / "custom" / "README.md").read_bytes() == b"updated catalog\n":
                        raise setup.SetupError(setup.SETUP_CACHE_INVALID, "injected post-promotion validation failure")
                    real_validate(plan, target)

                with patch.object(
                    setup,
                    "_replace_with_retry" if failure.startswith("staged-to-destination") else "_validate_active_cache",
                    side_effect=replace if failure.startswith("staged-to-destination") else validate,
                ):
                    with self.assertRaises(setup.SetupError):
                        setup._install_active_cache(
                            fixture.harness,
                            runtime,
                            plan=setup._plan_active_cache(fixture.harness),
                            overwrite=True,
                        )
                self.assertEqual(before, self._bytes(destination))
                self.assertFalse((runtime / "super-cache.old").exists())
                self.assertEqual([], list(runtime.glob(".super-cache.*")))
                with (
                    patch.object(setup, "find_harness_root", return_value=fixture.harness),
                    patch.object(setup, "_start_monitor"),
                ):
                    recovered = setup.run_setup(overwrite=True)
                self.assertTrue(recovered["ok"], recovered)
                setup._validate_active_cache(setup._plan_active_cache(fixture.harness), destination)

    def test_interrupted_current_backup_is_restored_and_owned_stage_removed(self) -> None:
        first = self._setup()
        self.assertTrue(first["ok"], first)
        before = self._bytes(self.destination)
        stage = self.runtime / (".super-cache." + "a" * 32)
        previous = self._manifest(self.destination)
        shutil.copytree(self.destination, stage)
        self._record_transaction(stage, previous=previous)
        self.destination.rename(self.backup)
        (stage / "custom" / "README.md").unlink()
        self._blocked_payload()
        with patch.object(setup, "_replace_tree", side_effect=AssertionError("must restore backup")):
            recovered = self._setup()
        self.assertTrue(recovered["ok"], recovered)
        self.assertEqual(before, self._bytes(self.destination))
        self._assert_clean_boundary()
        self.assertEqual(b"preserve this file\n", self.unrelated.read_bytes())

    def test_ambiguous_backup_or_stage_fails_closed_without_changing_bytes(self) -> None:
        for name in ("super-cache.old", ".super-cache.ambiguous"):
            with self.subTest(name=name):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                destination = fixture.active_cache()
                runtime = destination.parent
                suspect = runtime / name
                suspect.mkdir()
                (suspect / "unknown.txt").write_bytes(b"foreign bytes\n")
                before = self._bytes(destination)
                with (
                    patch.object(setup, "find_harness_root", return_value=fixture.harness),
                    patch.object(setup, "_start_monitor") as monitor,
                ):
                    result = setup.run_setup(overwrite=True)
                self.assertFalse(result["ok"], result)
                self.assertEqual(setup.SETUP_CACHE_INVALID, result["code"])
                self.assertEqual(before, self._bytes(destination))
                self.assertEqual({"unknown.txt": b"foreign bytes\n"}, self._bytes(suspect))
                with self.assertRaises(setup.SetupError):
                    setup._validated_worker_payload(fixture.harness, runtime, "codex")
                monitor.assert_not_called()

    def test_noncurrent_backup_or_stage_is_preserved_even_with_overwrite(self) -> None:
        for name in ("super-cache.old", ".super-cache.previous-plan"):
            with self.subTest(name=name):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                destination = fixture.active_cache()
                suspect = destination.parent / name
                if name == "super-cache.old":
                    destination.rename(suspect)
                else:
                    shutil.copytree(destination, suspect)
                before = self._bytes(suspect)
                (fixture.harness / "super-cache" / "custom" / "README.md").write_bytes(
                    b"new current plan\n"
                )
                with (
                    patch.object(setup, "find_harness_root", return_value=fixture.harness),
                    patch.object(setup, "_start_monitor") as monitor,
                ):
                    result = setup.run_setup(overwrite=True)
                self.assertFalse(result["ok"], result)
                self.assertEqual(setup.SETUP_CACHE_INVALID, result["code"])
                self.assertEqual(before, self._bytes(suspect))
                self.assertEqual(name != "super-cache.old", destination.exists())
                monitor.assert_not_called()

    def test_later_changed_or_missing_cache_file_needs_explicit_repair(self) -> None:
        first = self._setup()
        self.assertTrue(first["ok"], first)
        hooks = self.fixture.root_workspace / ".codex" / "hooks.json"
        hooks_before = hooks.read_bytes()
        config = self.fixture.root_workspace / ".codex" / "config.toml"
        config_before = config.read_bytes()
        owned = self.destination / "composed-payloads" / "codex" / ".agent-workspace" / "lane-queue.py"
        original = owned.read_bytes()
        for fault in ("changed", "missing"):
            with self.subTest(fault=fault):
                if fault == "changed":
                    owned.write_bytes(b"overwritten\n")
                else:
                    owned.unlink()
                rejected = self._setup()
                self.assertFalse(rejected["ok"], rejected)
                self.assertEqual(setup.SETUP_CACHE_INVALID, rejected["code"])
                with self.assertRaises(setup.SetupError):
                    setup._validated_worker_payload(self.fixture.harness, self.runtime, "codex")
                repaired = self._setup(overwrite=True)
                self.assertTrue(repaired["ok"], repaired)
                self._assert_clean_boundary()
                self.assertEqual(original, owned.read_bytes())
                self.assertEqual(hooks_before, hooks.read_bytes())
                self.assertEqual(config_before, config.read_bytes())
                self.assertEqual(b"preserve this file\n", self.unrelated.read_bytes())
                self.assertEqual(
                    self.destination / "composed-payloads" / "codex",
                    setup._validated_worker_payload(self.fixture.harness, self.runtime, "codex"),
                )

    def test_unknown_live_cache_paths_block_overwrite_without_mutation(self) -> None:
        self.assertTrue(self._setup()["ok"])
        for relative in (
            Path("operator-notes.txt"),
            Path("operator-notes") / "note.txt",
            Path("empty-operator-dir"),
        ):
            with self.subTest(relative=relative):
                unknown = self.destination / relative
                if relative == Path("empty-operator-dir"):
                    unknown.mkdir()
                else:
                    unknown.parent.mkdir(parents=True, exist_ok=True)
                    unknown.write_bytes(b"UNRELATED OPERATOR DATA\n")
                before = self._bytes(self.runtime)
                plain = self._setup()
                self.assertFalse(plain["ok"], plain)
                self.assertEqual(setup.SETUP_CACHE_INVALID, plain["code"])
                self.assertEqual(before, self._bytes(self.runtime))
                rejected = self._setup(overwrite=True)
                self.assertFalse(rejected["ok"], rejected)
                self.assertEqual(setup.SETUP_CACHE_INVALID, rejected["code"])
                self.assertIn(str(unknown), rejected["summary"])
                self.assertTrue(unknown.exists())
                self.assertEqual(before, self._bytes(self.runtime))
                if unknown.is_dir():
                    unknown.rmdir()
                else:
                    unknown.unlink()
                if unknown.parent != self.destination:
                    unknown.parent.rmdir()

    def test_unjournaled_artifacts_name_every_path_and_safe_resolution(self) -> None:
        self.assertTrue(self._setup()["ok"])
        stages = [self.runtime / (".super-cache." + letter * 32) for letter in ("a", "b")]
        artifacts = [self.backup, *stages]
        for artifact in artifacts:
            artifact.mkdir()
            (artifact / "sentinel.txt").write_bytes(b"preserve\n")
        before = {str(path): self._bytes(path) for path in artifacts}
        rejected = self._setup(overwrite=True)
        self.assertFalse(rejected["ok"], rejected)
        self.assertEqual(setup.SETUP_CACHE_INVALID, rejected["code"])
        for artifact in artifacts:
            self.assertIn(str(artifact), rejected["summary"])
            self.assertIn(str(artifact), rejected["next_action"])
            self.assertEqual(before[str(artifact)], self._bytes(artifact))
        self.assertIn("inspect", rejected["next_action"].lower())
        self.assertIn("move each artifact outside the runtime root", rejected["next_action"])

    @unittest.skipUnless(os.name == "nt", "Windows junction contract")
    def test_root_payload_junction_blocks_preflight_and_install_recheck(self) -> None:
        self.assertTrue(self._setup()["ok"])
        plan = setup._plan_root_payloads(self.fixture.harness)
        link = self.fixture.root_workspace / ".codex"
        outside = self.fixture.root / "outside-root-payload"
        changed = outside / "hooks" / "post-tool-use.py"
        setup._preflight_root_payloads(plan, self.fixture.root_workspace, overwrite=True)
        link.rename(outside)
        try:
            changed.write_bytes(b"UNRELATED EXTERNAL BYTES\n")
            subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(outside)], check=True, capture_output=True)
            before = self._bytes(outside)
            runtime_before = self._bytes(self.runtime)
            rejected = self._setup(overwrite=True)
            self.assertFalse(rejected["ok"], rejected)
            self.assertEqual(setup.SETUP_ADAPTER_COLLISION, rejected["code"])
            self.assertIn(str(link), rejected["summary"])
            self.assertEqual(before, self._bytes(outside))
            self.assertEqual(runtime_before, self._bytes(self.runtime))
            with self.assertRaises(setup.SetupError) as raised:
                setup._install_root_payloads(
                    self.fixture.harness,
                    self.fixture.root_workspace,
                    plan=plan,
                    overwrite=True,
                )
            self.assertEqual(setup.SETUP_ADAPTER_COLLISION, raised.exception.code)
            self.assertIn(str(link), str(raised.exception))
            self.assertEqual(before, self._bytes(outside))
        finally:
            if link.exists():
                link.rmdir()
            outside.rename(link)

    @unittest.skipUnless(os.name == "nt", "Windows junction contract")
    def test_root_payload_target_junction_blocks_before_setup_writes(self) -> None:
        self.assertTrue(self._setup()["ok"])
        target = self.fixture.root_workspace / ".codex" / "root.txt"
        saved = target.with_name("root.saved")
        outside = self.fixture.root / "outside-target"
        outside.mkdir()
        (outside / "sentinel.txt").write_bytes(b"EXTERNAL DATA\n")
        target.rename(saved)
        try:
            subprocess.run(["cmd", "/c", "mklink", "/J", str(target), str(outside)], check=True, capture_output=True)
            before = self._bytes(outside)
            runtime_before = self._bytes(self.runtime)
            rejected = self._setup(overwrite=True)
            self.assertFalse(rejected["ok"], rejected)
            self.assertEqual(setup.SETUP_ADAPTER_COLLISION, rejected["code"])
            self.assertIn(str(target), rejected["summary"])
            self.assertEqual(before, self._bytes(outside))
            self.assertEqual(runtime_before, self._bytes(self.runtime))
        finally:
            if target.exists():
                target.rmdir()
            saved.rename(target)

    def test_valid_repeat_preserves_cache_bytes_and_skips_replacement(self) -> None:
        first = self._setup()
        self.assertTrue(first["ok"], first)
        before = self._bytes(self.destination)
        with patch.object(setup, "_replace_tree", side_effect=AssertionError("unexpected replacement")):
            repeated = self._setup(overwrite=True)
        self.assertTrue(repeated["ok"], repeated)
        self.assertEqual(before, self._bytes(self.destination))
        self._assert_clean_boundary()

    def test_valid_live_cache_repairs_incomplete_owned_stage_without_replacement(self) -> None:
        first = self._setup()
        self.assertTrue(first["ok"], first)
        before = self._bytes(self.destination)
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        self._record_transaction(stage, previous=self._manifest(self.destination))
        (stage / "custom" / "README.md").unlink()
        self._blocked_payload()
        with patch.object(setup, "_replace_tree", side_effect=AssertionError("unexpected replacement")):
            repeated = self._setup()
        self.assertTrue(repeated["ok"], repeated)
        self.assertEqual(before, self._bytes(self.destination))
        self._assert_clean_boundary()
        self.assertEqual(b"preserve this file\n", self.unrelated.read_bytes())

    def test_owned_old_backup_survives_source_catalog_change_after_interruption(self) -> None:
        self.assertTrue(self._setup()["ok"])
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        self._record_transaction(stage, previous=self._manifest(self.destination))
        self.destination.rename(self.backup)
        (stage / "custom" / "README.md").unlink()
        self.source.write_bytes(b"later source catalog\n")
        self._blocked_payload()
        repaired = self._setup(overwrite=True)
        self.assertTrue(repaired["ok"], repaired)
        self._assert_clean_boundary()
        self.assertEqual(b"later source catalog\n", (self.destination / "custom" / "README.md").read_bytes())

    def test_byte_exact_unowned_stage_and_backup_are_preserved_and_block_dispatch(self) -> None:
        self.assertTrue(self._setup()["ok"])
        for name in (".super-cache." + "a" * 32, "super-cache.old"):
            with self.subTest(name=name):
                suspect = self.runtime / name
                shutil.copytree(self.destination, suspect)
                before = self._bytes(suspect)
                self._blocked_payload()
                rejected = self._setup(overwrite=True)
                self.assertFalse(rejected["ok"], rejected)
                self.assertEqual(setup.SETUP_CACHE_INVALID, rejected["code"])
                self.assertEqual(before, self._bytes(suspect))
                shutil.rmtree(suspect)

    def test_owned_backup_cleanup_failure_and_partial_cleanup_converge(self) -> None:
        self.assertTrue(self._setup()["ok"])
        real_remove = shutil.rmtree

        def fail_backup(path: Path, *args: object, **kwargs: object) -> None:
            if Path(path) == self.backup:
                raise OSError("injected backup cleanup failure")
            real_remove(path, *args, **kwargs)

        for partial in (False, True):
            with self.subTest(partial=partial):
                expected = f"updated catalog {partial}\n".encode()
                self.source.write_bytes(expected)
                with patch.object(setup.shutil, "rmtree", side_effect=fail_backup):
                    failed = self._setup(overwrite=True)
                self.assertFalse(failed["ok"], failed)
                self.assertTrue(self.journal.is_file())
                self.assertTrue(self.backup.is_dir())
                self.assertEqual(expected, (self.destination / "custom" / "README.md").read_bytes())
                self._blocked_payload()
                if partial:
                    (self.backup / "custom" / "README.md").unlink()
                repaired = self._setup()
                self.assertTrue(repaired["ok"], repaired)
                self._assert_clean_boundary()
        self.assertEqual(
            self.destination / "composed-payloads" / "codex",
            setup._validated_worker_payload(self.fixture.harness, self.runtime, "codex"),
        )
        self.assertEqual(
            self.destination / "composed-payloads" / "codex",
            bootstrap._managed_payload(self.fixture.harness, self.runtime, "codex"),
        )

    def test_transaction_record_is_closed_and_bound_to_this_runtime(self) -> None:
        self.assertTrue(self._setup()["ok"])
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        self._record_transaction(stage, previous=self._manifest(self.destination))
        valid = json.loads(self.journal.read_text(encoding="utf-8"))
        for changed in ({**valid, "credential": "should not be accepted"}, {**valid, "runtime_root": str(self.fixture.harness)}):
            with self.subTest(changed=changed):
                self.journal.write_text(json.dumps(changed), encoding="utf-8")
                rejected = self._setup(overwrite=True)
                self.assertFalse(rejected["ok"], rejected)
                self.assertTrue(stage.is_dir())
                self._blocked_payload()

    def test_hardlinked_transaction_journal_blocks_recovery_without_mutation(self) -> None:
        self.assertTrue(self._setup()["ok"])
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        shutil.copytree(self.destination, self.backup)
        self._record_transaction(stage, previous=self._manifest(self.destination))
        setup.atomic_write_json(self.journal, json.loads(self.journal.read_text(encoding="utf-8")))
        outside_link = self.fixture.root / "linked-transaction.json"
        os.link(self.journal, outside_link)
        before = (self.journal.read_bytes(), self._bytes(stage), self._bytes(self.backup), self._bytes(self.destination))

        rejected = self._setup(overwrite=True)
        self.assertFalse(rejected["ok"], rejected)
        self.assertEqual(setup.SETUP_CACHE_INVALID, rejected["code"])
        self.assertEqual(before, (self.journal.read_bytes(), self._bytes(stage), self._bytes(self.backup), self._bytes(self.destination)))
        self._blocked_payload()

        outside_link.unlink()
        self.assertEqual(1, self.journal.stat().st_nlink)
        repaired = self._setup()
        self.assertTrue(repaired["ok"], repaired)
        self._assert_clean_boundary()

    def test_journal_owned_partial_copy_bytes_at_planned_path_are_repairable(self) -> None:
        self.assertTrue(self._setup()["ok"])
        prior = self._bytes(self.destination)
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        self._record_transaction(stage, previous=self._manifest(self.destination))
        (stage / "custom" / "README.md").write_bytes(b"interrupted partial copy\n")
        self._blocked_payload()

        repaired = self._setup()
        self.assertTrue(repaired["ok"], repaired)
        self.assertEqual(prior, self._bytes(self.destination))
        self._assert_clean_boundary()

    def test_plain_bootstrap_blocks_unresolved_cache_artifacts(self) -> None:
        self.assertTrue(self._setup()["ok"])
        self.fixture._write_json(self.fixture.harness / "harness-config.json", {
            "root_workspace": str(self.fixture.root_workspace),
            "managed_coordination": "disabled",
        })
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        result = self._bootstrap_preflight()
        self.assertFalse(result["ok"], result)
        self.assertEqual(bootstrap.BOOTSTRAP_CACHE_COLLISION, result["code"])
        self.assertTrue(stage.is_dir())

    def test_failed_promotion_and_rollback_rename_recovers_prior_backup(self) -> None:
        self.assertTrue(self._setup()["ok"])
        previous = self._bytes(self.destination)
        self.source.write_bytes(b"updated catalog\n")
        real_replace = setup._replace_with_retry

        def fail_renames(source: Path, target: Path) -> None:
            if target == self.destination and source != self.destination:
                raise OSError("injected promotion and rollback rename failure")
            real_replace(source, target)

        with patch.object(setup, "_replace_with_retry", side_effect=fail_renames):
            failed = self._setup(overwrite=True)
        self.assertFalse(failed["ok"], failed)
        self.assertTrue(self.backup.is_dir())
        self.assertEqual(previous, self._bytes(self.backup))
        self._blocked_payload()
        repaired = self._setup(overwrite=True)
        self.assertTrue(repaired["ok"], repaired)
        self._assert_clean_boundary()
        self.assertEqual(b"updated catalog\n", (self.destination / "custom" / "README.md").read_bytes())

    def test_invalid_live_with_exact_owned_prior_backup_is_restored(self) -> None:
        self.assertTrue(self._setup()["ok"])
        prior_bytes = (self.destination / "custom" / "README.md").read_bytes()
        previous = self._manifest(self.destination)
        stage = self.runtime / (".super-cache." + "a" * 32)
        self._record_transaction(stage, previous=previous)
        shutil.copytree(self.destination, self.backup)
        (self.destination / "custom" / "README.md").write_bytes(b"corrupt promoted bytes\n")
        self._blocked_payload()
        repaired = self._setup()
        self.assertTrue(repaired["ok"], repaired)
        self._assert_clean_boundary()
        self.assertEqual(prior_bytes, (self.destination / "custom" / "README.md").read_bytes())

    def test_foreign_bytes_inside_claimed_transaction_stage_fail_closed(self) -> None:
        self.assertTrue(self._setup()["ok"])
        stage = self.runtime / (".super-cache." + "a" * 32)
        shutil.copytree(self.destination, stage)
        self._record_transaction(stage, previous=self._manifest(self.destination))
        (stage / "foreign.txt").write_bytes(b"preserve\n")
        rejected = self._setup(overwrite=True)
        self.assertFalse(rejected["ok"], rejected)
        self.assertEqual(b"preserve\n", (stage / "foreign.txt").read_bytes())
        self._blocked_payload()

    @unittest.skipUnless(os.name == "nt", "Windows junction contract")
    def test_junction_at_root_or_beneath_cache_blocks_validation_and_dispatch(self) -> None:
        self.assertTrue(self._setup()["ok"])
        for relative in (Path("."), Path("workspace")):
            with self.subTest(relative=relative):
                link = self.destination if relative == Path(".") else self.destination / relative
                target = self.runtime / ("junction-target-" + ("root" if relative == Path(".") else "child"))
                link.rename(target)
                try:
                    subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True)
                    self._blocked_payload()
                    rejected = self._setup(overwrite=True)
                    self.assertFalse(rejected["ok"], rejected)
                    self.assertEqual(setup.SETUP_CACHE_INVALID, rejected["code"])
                finally:
                    if link.exists():
                        link.rmdir()
                    target.rename(link)

    @unittest.skipUnless(os.name == "nt", "Windows junction contract")
    def test_workspace_and_runtime_junctions_block_first_setup_without_writes(self) -> None:
        for boundary in ("workspace", "runtime"):
            with self.subTest(boundary=boundary):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                fixture.add_shared_root_configs()
                runtime = fixture.root_workspace / ".harness-runtime"
                link = fixture.root_workspace if boundary == "workspace" else runtime
                target = fixture.root / "outside-first-setup"
                if boundary == "workspace":
                    link.rename(target)
                else:
                    target.mkdir()
                try:
                    subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True)
                    before = self._bytes(target)
                    with (
                        patch.object(setup, "find_harness_root", return_value=fixture.harness),
                        patch.object(setup, "_start_monitor") as monitor,
                    ):
                        rejected = setup.run_setup(overwrite=True)
                    self.assertFalse(rejected["ok"], rejected)
                    self.assertEqual(setup.SETUP_CONFIG_INVALID, rejected["code"])
                    self.assertEqual(before, self._bytes(target))
                    monitor.assert_not_called()
                finally:
                    if link.exists():
                        link.rmdir()
                    if boundary == "workspace":
                        target.rename(link)

    @unittest.skipUnless(os.name == "nt", "Windows junction contract")
    def test_workspace_and_runtime_junctions_block_setup_and_both_bootstrap_profiles(self) -> None:
        self.assertTrue(self._setup()["ok"])
        for boundary in ("workspace", "runtime"):
            with self.subTest(boundary=boundary):
                link = self.fixture.root_workspace if boundary == "workspace" else self.runtime
                target = self.fixture.root / f"outside-{boundary}"
                link.rename(target)
                try:
                    subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True)
                    before = self._bytes(target)
                    rejected = self._setup(overwrite=True)
                    self.assertFalse(rejected["ok"], rejected)
                    self.assertIn(rejected["code"], (setup.SETUP_CONFIG_INVALID, setup.SETUP_CACHE_INVALID))
                    self.assertEqual(before, self._bytes(target))
                    with self.assertRaises(setup.SetupError):
                        setup._validated_cache_for_dispatch(self.fixture.harness, self.runtime)
                    for profile in ("enabled", "disabled"):
                        with self.subTest(profile=profile):
                            self.fixture._write_json(self.fixture.harness / "harness-config.json", {
                                "root_workspace": str(self.fixture.root_workspace),
                                "managed_coordination": profile,
                            })
                            result = self._bootstrap_preflight()
                            self.assertFalse(result["ok"], result)
                            self.assertEqual(bootstrap.BOOTSTRAP_CACHE_COLLISION, result["code"])
                            self.assertEqual(before, self._bytes(target))
                finally:
                    if link.exists():
                        link.rmdir()
                    target.rename(link)


if __name__ == "__main__":
    unittest.main()
