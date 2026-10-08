from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import devctl as d
import worker_runtime as w


class WorkerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.repo = self.root / 'repo'; self.repo.mkdir()
        for args in (['init', '-b', 'feature'], ['config', 'user.name', 'Test'], ['config', 'user.email', 'test@example.invalid']):
            subprocess.check_call(['git', '-C', str(self.repo), *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (self.repo / 'file').write_text('baseline')
        subprocess.check_call(['git', '-C', str(self.repo), 'add', '.'])
        subprocess.check_call(['git', '-C', str(self.repo), 'commit', '-m', 'baseline'], stdout=subprocess.DEVNULL)
        self.data = self.root / 'state/tasks'
        registry = {'test': {'machine': 'work', 'workspace': str(self.repo)}}
        for p in (patch.object(d, 'DATA_DIR', self.data), patch.object(d, 'load_projects', return_value=registry)):
            p.start(); self.addCleanup(p.stop)

    def wait(self, task_id):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            meta = d.load_meta(task_id)
            state = d.task_state(meta, d.task_dir(task_id))[0]
            if state == 'DONE': return meta
            time.sleep(0.01)
        self.fail('Dummy worker failed to complete')

    def test_real_local_dummy_launch_and_rpc_replay_and_resume(self):
        source = 'import json; print(json.dumps({"type":"thread.started","thread_id":"session-test"})); print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"done"}}))'
        command = shlex.join([sys.executable, '-c', source])
        with redirect_stdout(io.StringIO()), patch.object(d, 'build_agent_command', return_value=command):
            d.run_project('test', 'prompt', request_id='a' * 64)
            task_id = 'a' * 24
            first = self.wait(task_id)
            d.run_project('test', 'prompt', request_id='a' * 64)
            self.assertEqual(d.load_meta(task_id)['pid'], first['pid'])
            self.assertTrue(first['git_baseline']['clean'])
            with self.assertRaises(ValueError): d.run_project('test', 'different prompt', request_id='a' * 64)
            original = d.machine_command
            def safe_command(machine, text):
                if 'codex exec' in text: return [sys.executable, '-c', source]
                return original(machine, text)
            with patch.object(d, 'machine_command', side_effect=safe_command):
                d.continue_task(task_id, 'followup', 'b' * 64)
                second = self.wait(task_id)
                d.continue_task(task_id, 'followup', 'b' * 64)
                self.assertEqual(d.load_meta(task_id)['turns'], 2)
                self.assertEqual(d.load_meta(task_id)['pid'], second['pid'])
                audit = json.loads((d.task_dir(task_id) / 'runtime_audit.json').read_text())
                self.assertEqual(audit[first['runtime_generation']]['liveness']['exit_code'], 0)
                self.assertEqual(audit[first['runtime_generation']]['session_id'], 'session-test')

    def test_git_baseline_fingerprint_detects_dirty_content_change(self):
        dirty = self.repo / 'dirty.txt'
        dirty.write_text('one\\n')

        first = d.capture_git_baseline('work', str(self.repo))
        self.assertFalse(first['clean'])
        self.assertTrue(first['snapshot_fingerprint'])

        # Same porcelain path/status, different bytes: fingerprint must change.
        dirty.write_text('two\\n')
        second = d.capture_git_baseline('work', str(self.repo))
        self.assertEqual(first['status_porcelain'], second['status_porcelain'])
        self.assertNotEqual(
            first['snapshot_fingerprint'],
            second['snapshot_fingerprint'],
        )

        dirty.write_text('one\\n')
        restored = d.capture_git_baseline('work', str(self.repo))
        self.assertEqual(
            first['snapshot_fingerprint'],
            restored['snapshot_fingerprint'],
        )

    def test_lease_capacity_and_traversal(self):
        task = self.data / 'active'; task.mkdir(parents=True)
        from durable import write_json
        import os
        write_json(task / 'meta.json', {'id': 'active', 'pid': os.getpid(), 'machine': 'work', 'role': 'coding'})
        with self.assertRaises(RuntimeError): w.check_capacity(self.data, 'another', 'work', 'coding', d.task_state)
        with self.assertRaises(ValueError): d.task_dir('../outside')
        with self.assertRaises(ValueError): d.task_dir('/outside')

    def test_cli_routes_request_and_role(self):
        argv = ['devctl', 'run-project', 'test', 'prompt', '--request-id', 'a' * 64, '--role', 'review']
        with patch.object(sys, 'argv', argv), patch.object(d, 'run_project', return_value=0) as run, self.assertRaises(SystemExit):
            d.main()
        self.assertEqual(run.call_args.args[-2:], ('a' * 64, 'review'))

    def test_new_turn_cannot_reuse_previous_terminal_result_or_error(self):
        from durable import write_json
        task = self.data / 'task'; task.mkdir(parents=True)
        meta = dict(id='task', machine='work', workspace=str(self.repo), agent='codex', pid=0,
                    session_id='session', turns=2)
        old = json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'old PASS'}}) + '\n'
        old += json.dumps({'type': 'error', 'message': 'old failure'}) + '\n'
        for boundary in ('offset', 'legacy-marker'):
            with self.subTest(boundary=boundary):
                log = old
                if boundary == 'offset': meta['turn_log_offset'] = len(old.encode())
                else:
                    meta.pop('turn_log_offset', None)
                    log += json.dumps({'type': 'devorchestrator.continue'}) + '\n'
                log += json.dumps({'type': 'thread.started', 'thread_id': 'session'}) + '\n'
                (task / 'events.jsonl').write_text(log)
                (task / 'exit_code').write_text('0')
                write_json(task / 'meta.json', meta)
                stream = io.StringIO()
                with redirect_stdout(stream): d.task_report('task', as_json=True)
                report = json.loads(stream.getvalue())
                self.assertIsNone(report['result'])
                self.assertIsNone(report['error'])
                self.assertEqual(report['session_id'], 'session')

    def test_corrupt_task_metadata_is_unknown_and_other_tasks_are_listed(self):
        from durable import write_json
        corrupt = self.data / 'corrupt'; corrupt.mkdir(parents=True)
        (corrupt / 'meta.json').write_text('{broken')
        healthy = self.data / 'healthy'; healthy.mkdir()
        write_json(healthy / 'meta.json', dict(id='healthy', machine='work', workspace=str(self.repo), pid=0))
        (healthy / 'exit_code').write_text('0')
        stream = io.StringIO()
        with redirect_stdout(stream): d.list_tasks(as_json=True)
        tasks = {t['id']: t for t in json.loads(stream.getvalue())}
        self.assertEqual(tasks['corrupt']['status'], 'UNKNOWN')
        self.assertEqual(tasks['healthy']['status'], 'DONE')
        stream = io.StringIO()
        with redirect_stdout(stream): d.list_tasks()
        self.assertIn('UNKNOWN', stream.getvalue())
        for malformed in ([], {'id': ['bad']}, {'id': 'healthy', 'machine': 'work', 'workspace': str(self.repo)}):
            (corrupt / 'meta.json').write_text(json.dumps(malformed))
            stream = io.StringIO()
            with redirect_stdout(stream): d.list_tasks(as_json=True, cached=True)
            tasks = {t['id']: t for t in json.loads(stream.getvalue())}
            self.assertEqual(tasks['corrupt']['status'], 'UNKNOWN')
            self.assertEqual(tasks['healthy']['status'], 'DONE')

    def test_cached_listing_avoids_ssh_and_keeps_uncertain_capacity(self):
        from durable import write_json
        task = self.data / 'remote'; task.mkdir(parents=True)
        meta = dict(id='remote', machine='firefly', workspace='/registered', pid=0, remote_key='remote-turn-1')
        write_json(task / 'meta.json', meta)
        with patch.object(d.remote_worker, 'refresh', side_effect=AssertionError('must not probe SSH')):
            for expected in ('UNKNOWN', 'RUNNING', 'DONE'):
                if expected == 'RUNNING':
                    meta['remote_status'] = 'RUNNING'; write_json(task / 'meta.json', meta)
                elif expected == 'DONE': (task / 'exit_code').write_text('0')
                stream = io.StringIO()
                with redirect_stdout(stream): d.list_tasks(as_json=True, cached=True)
                self.assertEqual(json.loads(stream.getvalue())[0]['status'], expected)

    def test_cli_routes_cached_listing(self):
        with patch.object(sys, 'argv', ['devctl', 'tasks', '--json', '--cached']), \
             patch.object(d, 'list_tasks', return_value=0) as listing, self.assertRaises(SystemExit):
            d.main()
        listing.assert_called_once_with(True, False, True)
