from __future__ import annotations

import json
import os
import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.config import load_config
from orchestrator_harness.discovery import discover_suite


FAKE = r'''
import json, os, sys
argv=sys.argv[1:]
if any('AUTHORITATIVE' in item for item in argv): raise SystemExit(91)
prompt=sys.stdin.read()
if not prompt.startswith('## AUTHORITATIVE ZERO-OPERATOR OVERRIDE\n'): raise SystemExit(92)
capture=os.environ.get('LANE_FAKE_CAPTURE')
if capture: open(capture,'w',encoding='utf-8').write(json.dumps(argv))
if os.environ.get('LANE_FAKE_NO_THREAD') != '1':
 print(json.dumps({'type':'thread.started','thread_id':'fake-thread'}), flush=True)
print(json.dumps({'type':'turn.completed'}), flush=True)
'''


class LaneControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / 'project'
        self.run = self.root / 'runs' / 'T00_fake'
        self.workspace = self.run / '.agent-workspace'
        self.workspace.mkdir(parents=True)
        self.harness = self.root / 'orchestrator_harness'
        self.harness.mkdir()
        self.fake_module = self.harness / 'lane_controller.py'
        self.fake_module.write_text('', encoding='utf-8')
        self.events = self.root / 'multi-agent-logs' / 'orchestrator-harness' / 'test-epoch' / 'LANE_EVENTS.jsonl'
        self.events.parent.mkdir(parents=True)
        self.policy = self.root / '.agent-workspace' / 'AUTONOMOUS_EXECUTION_POLICY.md'
        self.policy.parent.mkdir(exist_ok=True)
        self.policy.write_text('Canonical autonomous policy.\n', encoding='utf-8')
        self.policy_sha = hashlib.sha256(self.policy.read_bytes()).hexdigest()
        (self.policy.parent / 'AUTONOMOUS_EXECUTION_POLICY.sha256').write_text(self.policy_sha + '  AUTONOMOUS_EXECUTION_POLICY.md\n', encoding='utf-8')
        self.prompt = self.run / 'bound-prompt.md'
        self.prompt.write_text(
            '## AUTHORITATIVE ZERO-OPERATOR OVERRIDE\n\n'
            f'Policy SHA-256: `{self.policy_sha}`\n\nCanonical autonomous policy.\n\n'
            '## END AUTHORITATIVE ZERO-OPERATOR OVERRIDE\n\nTask with spaces.\n\n'
            '## FINAL PRECEDENCE REMINDER\n\n'
            f'Policy `{self.policy_sha}` and the latest signed run amendment control.\n',
            encoding='utf-8',
        )
        self.fake = self.root / 'fake_codex.py'
        self.fake.write_text(FAKE, encoding='utf-8')
        self.capture = self.root / 'argv.json'
        self.old_file = controller.__file__
        controller.__file__ = str(self.fake_module)

    def tearDown(self) -> None:
        controller.__file__ = self.old_file
        os.environ.pop('LANE_FAKE_CAPTURE', None)
        os.environ.pop('LANE_FAKE_NO_THREAD', None)
        self.temporary.cleanup()

    def invocation(self, *, action: str = 'start', label: str = 'fake', thread: str | None = None) -> Path:
        paths = {
            'status': str(self.workspace / f'{label}_controller.status.json'),
            'jsonl': str(self.workspace / f'{label}_codex.jsonl'),
            'stderr': str(self.workspace / f'{label}_codex.stderr.log'),
            'last_message': str(self.workspace / f'{label}_last_message.txt'),
        }
        raw = {
            'action': action, 'run_root': str(self.run), 'prompt_path': str(self.prompt), 'label': label,
            'doer': 'Fake', 'task': 'T00', 'phase': 'host-only', 'declared_lane_id': 'T00:Fake:one',
            'leases': [], 'server_snapshot': {'head': 'fake'},
            'board_tokens': ['board:fake'], 'mcp_servers': ['fake-mcp'],
            'policy_sha256': self.policy_sha, 'prompt_sha256': hashlib.sha256(self.prompt.read_bytes()).hexdigest(),
            'model_settings': {'model': 'gpt-5.6-luna', 'reasoning_effort': 'high', 'service_tier': 'default'},
            'codex_command': [sys.executable, str(self.fake)],
            'config_overrides': ['mcp_servers.fake.command="nothing"'], 'resume_thread_id': thread,
            'output_paths': paths, 'lane_event_log': str(self.events),
        }
        path = self.workspace / f'{label}-{action}.invocation.json'
        path.write_text(json.dumps(raw), encoding='utf-8')
        return path

    def status(self, label: str = 'fake') -> dict:
        return json.loads((self.workspace / f'{label}_controller.status.json').read_text(encoding='utf-8'))

    def test_start_streams_stdin_records_flags_and_is_discoverable(self) -> None:
        os.environ['LANE_FAKE_CAPTURE'] = str(self.capture)
        self.assertEqual(0, controller.main([str(self.invocation())]))
        status = self.status()
        self.assertEqual('CODEX_EXITED', status['state'])
        self.assertEqual('fake-thread', status['thread_id'])
        self.assertEqual(['board:fake'], status['board_tokens'])
        self.assertEqual(['fake-mcp'], status['mcp_servers'])
        self.assertEqual(self.policy_sha, status['policy_sha256'])
        self.assertEqual(hashlib.sha256(self.prompt.read_bytes()).hexdigest(), status['prompt_sha256'])
        self.assertIsInstance(status['controller_pid'], int)
        self.assertIsInstance(status['codex_pid'], int)
        argv = json.loads(self.capture.read_text(encoding='utf-8'))
        self.assertIn('--dangerously-bypass-approvals-and-sandbox', argv)
        self.assertIn('--ignore-user-config', argv)
        self.assertIn('--skip-git-repo-check', argv)
        self.assertIn('approval_policy="never"', argv)
        self.assertIn('approvals_reviewer="user"', argv)
        self.assertEqual('-', argv[-1])
        self.assertNotIn('AUTHORITATIVE', ' '.join(argv))
        config_path = self.root / 'harness.json'
        config_path.write_text(json.dumps({'suite_root': str(self.root), 'run_globs': ['runs/*']}), encoding='utf-8')
        records = discover_suite(load_config(config_path, harness_root=self.harness))
        self.assertEqual('fake', records[0].controllers[0].label)

    def test_resume_reuses_persisted_thread(self) -> None:
        self.assertEqual(0, controller.main([str(self.invocation())]))
        self.assertEqual(0, controller.main([str(self.invocation(action='resume'))]))
        self.assertEqual('fake-thread', self.status()['thread_id'])

    def test_missing_thread_is_launch_failure_even_when_child_exits_zero(self) -> None:
        os.environ['LANE_FAKE_NO_THREAD'] = '1'
        self.assertEqual(1, controller.main([str(self.invocation(label='none'))]))
        self.assertEqual('LAUNCH_FAILED', self.status('none')['state'])
        self.assertEqual(0, self.status('none')['exit_code'])

    def test_resume_without_thread_and_bad_paths_are_rejected(self) -> None:
        self.assertEqual(2, controller.main([str(self.invocation(action='resume', label='new'))]))
        path = self.invocation(label='escape')
        raw = json.loads(path.read_text(encoding='utf-8'))
        raw['output_paths']['status'] = str(self.root / 'escaped.json')
        path.write_text(json.dumps(raw), encoding='utf-8')
        self.assertEqual(2, controller.main([str(path)]))

    def test_malformed_invocation_is_rejected(self) -> None:
        bad = self.workspace / 'bad.json'
        bad.write_text('{not json', encoding='utf-8')
        self.assertEqual(2, controller.main([str(bad)]))

    def test_unbound_changed_and_policy_mismatch_prompts_are_rejected(self) -> None:
        raw_prompt = self.run / 'raw.md'
        raw_prompt.write_text('raw task', encoding='utf-8')
        raw = self.invocation(label='raw')
        value = json.loads(raw.read_text(encoding='utf-8'))
        value['prompt_path'] = str(raw_prompt)
        value['prompt_sha256'] = hashlib.sha256(raw_prompt.read_bytes()).hexdigest()
        raw.write_text(json.dumps(value), encoding='utf-8')
        self.assertEqual(2, controller.main([str(raw)]))

        changed = self.invocation(label='changed')
        self.prompt.write_text(self.prompt.read_text(encoding='utf-8') + 'changed', encoding='utf-8')
        self.assertEqual(2, controller.main([str(changed)]))
        # Restore the prompt for the mismatched policy invocation.
        self.prompt.write_text(
            '## AUTHORITATIVE ZERO-OPERATOR OVERRIDE\n\n'
            f'Policy SHA-256: `{self.policy_sha}`\n\nCanonical autonomous policy.\n\n'
            '## END AUTHORITATIVE ZERO-OPERATOR OVERRIDE\n\nTask with spaces.\n\n'
            '## FINAL PRECEDENCE REMINDER\n\n'
            f'Policy `{self.policy_sha}` and the latest signed run amendment control.\n', encoding='utf-8')
        mismatch = self.invocation(label='mismatch')
        value = json.loads(mismatch.read_text(encoding='utf-8'))
        value['policy_sha256'] = '0' * 64
        mismatch.write_text(json.dumps(value), encoding='utf-8')
        self.assertEqual(2, controller.main([str(mismatch)]))


if __name__ == '__main__':
    unittest.main()
