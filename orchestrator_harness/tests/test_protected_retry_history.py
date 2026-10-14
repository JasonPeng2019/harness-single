"""A worker's diagnostic ledger cannot replenish provider launch authority."""
from pathlib import Path
import hashlib
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch
import tempfile
import unittest

from orchestrator_harness import controller, processes, retry_policy as policy
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.support import prepare_lane_fixture, seed_provider_history


class ProtectedRetryHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.worktree = self.root / 'worktree'
        self.worktree.mkdir()
        self.local = self.worktree / 'attempts.jsonl'
        self.lane = {'lane_id': 'lane', 'run_id': 'run-1', 'worktree_path': str(self.worktree),
                     'attempts_path': str(self.local), 'provider': {'id': 'codex'},
                     'session': {'session_id': 'native'}}

    def test_valid_local_truncation_cannot_refund_invocations(self):
        # Independent fixture: two completed controller facts in protected state.
        key = hashlib.sha256(('lane\0' + os.path.normcase(str(self.worktree.resolve()))).encode()).hexdigest()
        protected = self.root / 'operator-only/provider-invocations' / (key + '.jsonl')
        protected.parent.mkdir(parents=True)
        self.lane['provider_history_path'] = str(protected)
        rows = [{'schema': 'worker-provider-history/v1', 'lane_id': 'lane',
                 'worktree': os.path.normcase(str(self.worktree.resolve())), 'provider': {'id': 'codex'}}]
        for n in (1, 2):
            identity = {'invocation_id': str(n) * 32, 'run_id': 'run-' + str(n), 'attempt': 1}
            rows.append({**identity, 'kind': 'RESERVED'})
            rows.append({**identity, 'kind': 'COMPLETED', 'provider_started': True,
                         'cleanup_proven': True, 'session_id': 'native'})
        protected.write_text('\n'.join(json.dumps(row) for row in rows) + '\n', encoding='utf-8')
        localrows = [{'run_id': 'run-' + str(n), 'attempt': 1, 'provider_started': True} for n in (1, 2)]
        self.local.write_text('\n'.join(json.dumps(row) for row in localrows), encoding='utf-8')
        self.assertEqual(2, policy.completed_provider_invocations(self.lane))
        self.local.write_text(json.dumps(localrows[0]), encoding='utf-8')
        self.assertEqual(2, policy.completed_provider_invocations(self.lane))

    def executable_fixture(self):
        self.lane["session"] = {}
        workspace = self.worktree / '.agent-workspace'
        workspace.mkdir()
        for field, filename in (("controller_status_path", "status.json"), ("controller_events_path", "events.jsonl"),
                                ("last_message_path", "last-message.txt")):
            self.lane[field] = str(workspace / filename)
        self.runtime = self.root / 'runtime'
        self.lane = prepare_lane_fixture(self.runtime, 'epoch', self.lane)
        seed_provider_history(self.runtime, self.lane, epoch_id='epoch')
        prompt = workspace / 'prompt.md'
        prompt.write_text('fixture', encoding='utf-8')
        invocation = {'provider': {'model': 'fixture', 'launch_config': {}}}
        binding = SimpleNamespace(
            build_argv=lambda **_: [sys.executable, '-B', '-c', 'import json; print(json.dumps({"session_id":"native-test"}))'],
            parse_line=lambda line: json.loads(line))
        return prompt, invocation, binding

    def test_real_provider_has_durable_reservation_before_creation_and_only_one_manual_retry(self):
        prompt, invocation, binding = self.executable_fixture()
        actual_spawn = processes.spawn_provider
        started = []
        def checked_spawn(*args, **kwargs):
            # Observe disk at the actual process-creation call, independently of
            # the policy's returned count or any mocked completion record.
            rows = [json.loads(line) for line in Path(self.lane['provider_history_path']).read_text().splitlines()]
            self.assertEqual('RESERVED', rows[-1]['kind'])
            self.assertEqual(self.lane['run_id'], rows[-1]['run_id'])
            started.append(self.lane['run_id'])
            return actual_spawn(*args, **kwargs)
        with patch.object(processes, 'spawn_provider', side_effect=checked_spawn):
            for run in ('initial', 'manual'):
                self.lane['run_id'] = run
                self.lane = prepare_lane_fixture(self.runtime, 'epoch', self.lane)
                execution = controller._run_provider(self.runtime, 'epoch', self.lane, invocation, binding, prompt)
                self.assertTrue(execution.boundary.cleanup(force=True))
                controller._append_attempt(self.lane, attempt_number=1, argv=list(execution.argv),
                                           session_id=execution.session_id, prompt_path=prompt,
                                           paths=controller._attempt_paths(self.lane, 1), exit_code=execution.exit_code,
                                           result_state='invalid', cleanup_proven=True, binding=binding, provider_started=True,
                                           transcript_start_byte=execution.transcript_start_byte)
                self.lane['session'] = {'session_id': execution.session_id}
            self.lane['run_id'] = 'third'
            self.lane = prepare_lane_fixture(self.runtime, 'epoch', self.lane)
            with self.assertRaisesRegex(controller.ControllerError, 'exhausted'):
                controller._run_provider(self.runtime, 'epoch', self.lane, invocation, binding, prompt)
        self.assertEqual(['initial', 'manual'], started)

    def test_stale_epoch_and_shutdown_refuse_before_provider_creation(self):
        prompt, invocation, binding = self.executable_fixture()
        for state, epoch in (('SHUTTING_DOWN', 'epoch'), ('OPEN', 'new-epoch')):
            atomic_write_json(self.runtime / 'CURRENT_EPOCH.json', {'schema': 'current-epoch/v1', 'epoch_id': epoch})
            atomic_write_json(self.runtime / 'RUNTIME_STATE.json', {'schema': 'runtime-state/v1', 'state': state})
            with patch.object(processes, 'spawn_provider') as spawn:
                with self.assertRaisesRegex(controller.ControllerError, 'owner changed'):
                    controller._run_provider(self.runtime, 'epoch', self.lane, invocation, binding, prompt)
            spawn.assert_not_called()
        self.assertEqual(0, policy.completed_provider_invocations(self.lane))

    def test_native_missing_executable_refunds_only_proven_no_provider(self):
        prompt, invocation, binding = self.executable_fixture()
        binding.build_argv = lambda **_: [str(self.root / 'absent-provider.exe')]
        with self.assertRaises(controller.ControllerError) as raised:
            controller._run_provider(self.runtime, 'epoch', self.lane, invocation, binding, prompt)
        self.assertTrue(raised.exception.no_provider_started)
        self.assertEqual(0, policy.completed_provider_invocations(self.lane))


if __name__ == '__main__':
    unittest.main()
