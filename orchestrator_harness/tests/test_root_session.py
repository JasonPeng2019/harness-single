"""Prepared logical ROOT identity, using actual native monitor readiness."""
from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from orchestrator_harness import root_session, setup
from orchestrator_harness.config import ConfigError, load_config
from orchestrator_harness.core import content_hash, iso_utc
from orchestrator_harness.records import atomic_write_json, runtime_admission_lock
from orchestrator_harness.tests.support import PreparedRootFixture, write_json


class RootSessionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = PreparedRootFixture(self)
        self.config = self.fixture.config
        self.path = root_session.root_session_path(self.config)
        self.record = self.fixture.session
        self.environment = patch.dict(os.environ, self.fixture.environment, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_split_session_published_in_manager_and_stable_across_repeated_preparation(self):
        self.assertEqual(self.fixture.manager / ".agent-workspace/ROOT_SESSION.json", self.path)
        self.assertFalse((self.fixture.product / ".agent-workspace/ROOT_SESSION.json").exists())
        before = self.path.read_bytes()
        again = root_session.publish_root_session(self.config, run_id=self.record["run_id"], provider_id="codex")
        self.assertEqual(self.record, again)
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual((self.record["session_id"], "codex"), root_session.resolve_root_session(self.config))

    def test_same_workspace_profile_uses_the_same_prepared_contract(self):
        fixture = PreparedRootFixture(self, same_workspace=True)
        self.assertEqual(fixture.product / ".agent-workspace/ROOT_SESSION.json", root_session.root_session_path(fixture.config))
        self.assertEqual(fixture.session, root_session.read_root_session(fixture.config))

    def test_missing_session_requires_preparation_without_a_process_fallback(self):
        self.path.unlink()
        with self.assertRaisesRegex(ConfigError, "ROOT_SESSION_REQUIRED"):
            root_session.resolve_root_session(self.config)

    def test_changed_run_or_provider_preserves_the_existing_binding(self):
        before = self.path.read_bytes()
        for values in ({"run_id": "another-run", "provider_id": "codex"},
                       {"run_id": self.record["run_id"], "provider_id": "claude-code"}):
            with self.subTest(values=values), self.assertRaisesRegex(ConfigError, "ROOT_SESSION_STALE"):
                root_session.publish_root_session(self.config, **values)
        self.assertEqual(before, self.path.read_bytes())

    def test_existing_session_cannot_bypass_actual_readiness(self):
        before = self.path.read_bytes()
        with runtime_admission_lock(self.fixture.rt):
            setup.set_runtime_state(self.fixture.rt, "SHUTTING_DOWN")
            with self.assertRaises(ConfigError):
                root_session.publish_root_session(self.config, run_id=self.record["run_id"], provider_id="codex")
        self.assertEqual(before, self.path.read_bytes())

    def test_session_lock_is_acquired_after_readiness_record_has_been_released(self):
        held = False
        real_lock = root_session.RecordLock
        real_ready = setup.require_monitor_ready

        class SessionLock:
            def __init__(inner, path):
                inner.real = real_lock(path)
            def __enter__(inner):
                nonlocal held
                inner.real.__enter__()
                held = True
                return inner
            def __exit__(inner, *args):
                nonlocal held
                held = False
                return inner.real.__exit__(*args)

        def ready(*args):
            self.assertFalse(held)
            return real_ready(*args)

        with patch.object(root_session, "RecordLock", SessionLock), patch.object(setup, "require_monitor_ready", side_effect=ready):
            self.assertEqual(self.record, root_session.publish_root_session(self.config, run_id=self.record["run_id"], provider_id="codex"))

    def test_explicit_and_environment_scope_mismatches_are_rejected(self):
        for values in ({"session_id": "another-session"}, {"session_id": ""},
                       {"binding_id": "root"}, {"provider_id": "qwen-code"}):
            with self.subTest(values=values), self.assertRaisesRegex(ConfigError, "ROOT_SESSION_MISMATCH"):
                root_session.resolve_root_session(self.config, **values)
        with patch.dict(os.environ, {"HARNESS_ROOT_SESSION_ID": "another-session"}):
            with self.assertRaisesRegex(ConfigError, "ROOT_SESSION_MISMATCH"):
                root_session.resolve_root_session(self.config)

    def test_native_variable_never_becomes_the_logical_scope_and_bind_is_once(self):
        with patch.dict(os.environ, {"CODEX_THREAD_ID": "unit-native-session"}):
            self.assertEqual((self.record["session_id"], "codex"), root_session.resolve_root_session(self.config))
            bound = root_session.bind_native_root_session(self.config, run_id=self.record["run_id"],
                        session_id=self.record["session_id"], native_session_id="unit-native-session")
            self.assertEqual(self.record["session_id"], bound["session_id"])
            self.assertEqual("unit-native-session", bound["native_session_id"])
            self.assertEqual(bound, root_session.read_root_session(self.config))
            self.assertEqual((self.record["session_id"], "codex"), root_session.resolve_root_session(self.config))
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ConfigError, "ROOT_SESSION_MISMATCH"):
            root_session.bind_native_root_session(self.config, run_id=self.record["run_id"],
                        session_id=self.record["session_id"], native_session_id="another-native-session")
        with patch.dict(os.environ, {"CODEX_THREAD_ID": "another-native-session"}):
            with self.assertRaisesRegex(ConfigError, "ROOT_SESSION_MISMATCH"):
                root_session.resolve_root_session(self.config)
        self.assertEqual(before, self.path.read_bytes())

    def test_damaged_shape_digest_and_timestamp_fail_closed(self):
        bad = [{"provider_id": []}, {"extra": "unknown"}, {"content_hash": "wrong"},
               {"prepared_at": datetime.now().isoformat()},
               {"prepared_at": iso_utc(datetime.now(timezone.utc) + timedelta(days=1))}]
        for changed in bad:
            record = {**self.record, **changed}
            if "content_hash" not in changed:
                record["content_hash"] = content_hash(record)
            atomic_write_json(self.path, record)
            with self.subTest(changed=changed), self.assertRaisesRegex(ConfigError, "ROOT_SESSION_INVALID"):
                root_session.read_root_session(self.config)

    def test_changed_configuration_invalidates_binding_without_rewriting_it(self):
        before = self.path.read_bytes()
        config_path = self.fixture.harness / "harness-config.json"
        values = json.loads(config_path.read_text(encoding="utf-8"))
        values["managed_coordination"] = "disabled"
        write_json(config_path, values)
        with self.assertRaisesRegex(ConfigError, "ROOT_SESSION_STALE"):
            root_session.read_root_session(load_config(self.fixture.harness))
        self.assertEqual(before, self.path.read_bytes())


if __name__ == "__main__":
    unittest.main()
