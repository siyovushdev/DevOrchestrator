from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import bridge_effects as effects
import devctl
from durable import read_json, write_json
from scheduler import Limits, Scheduler
import worker_runtime as worker
from tests_helpers import proc_fixture


class RecoveryTests(unittest.TestCase):
    def test_corrupt_state_fails_closed_and_atomic_save(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'state.json'
            write_json(path, {'issues': {'1': {'task_id': 'a'}}})
            self.assertEqual(read_json(path)['issues']['1']['task_id'], 'a')
            path.write_text('{corrupt')
            with self.assertRaises(json.JSONDecodeError): read_json(path)

    def test_receipt_replays_never_launch_twice_even_after_error(self):
        with tempfile.TemporaryDirectory() as root, redirect_stdout(io.StringIO()):
            data = Path(root) / 'tasks'
            launch = Mock(side_effect=RuntimeError('network interruption'))
            key = 'a' * 64
            with self.assertRaises(RuntimeError): worker.launch(data, key, 'task', launch)
            self.assertEqual(worker.launch(data, key, 'task', launch), 0)
            self.assertEqual(launch.call_count, 1)

    def test_worker_identity_pid_zero_reuse_and_reboot(self):
        self.assertFalse(worker.alive({'pid': 0}))
        self.assertTrue(worker.alive({'pid': os.getpid(), 'process_identity': worker.process_identity(os.getpid())}))
        self.assertFalse(worker.alive({'pid': os.getpid(), 'process_identity': {'boot': 'old', 'start': '1'}}))

    def test_reused_session_leader_start_mismatch_releases_local_lease(self):
        leader = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
        try:
            self.assertEqual(os.getsid(leader.pid), leader.pid)
            self.assertEqual(os.getpgid(leader.pid), leader.pid)
            identity = worker.process_identity(leader.pid)
            self.assertTrue(worker.alive({'pid': leader.pid, 'process_identity': identity}))
            stale = dict(identity, start=str(int(identity['start']) + 1))
            meta = {'pid': leader.pid, 'process_identity': stale, 'machine': 'work'}
            self.assertFalse(worker.alive(meta))
            with tempfile.TemporaryDirectory() as root:
                data = Path(root) / 'tasks'
                directory = data / 'old'; directory.mkdir(parents=True)
                write_json(directory / 'meta.json', meta)
                worker.admit(data, 'work', '/registered', 'old', devctl.task_state)
                worker.admit(data, 'work', '/registered', 'new', devctl.task_state)
                self.assertEqual(next(iter(read_json(data.parent / 'worker_leases.json').values()))['task_id'], 'new')
            self.assertIsNone(leader.poll())
        finally:
            leader.terminate(); leader.wait()

    def test_dead_local_and_unknown_remote(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            self.assertEqual(devctl.task_state({'pid': 99999999, 'machine': 'work'}, directory)[0], 'FAILED')
            self.assertEqual(devctl.task_state({'pid': 99999999, 'machine': 'firefly'}, directory)[0], 'UNKNOWN')

    def test_child_scan_only_when_leader_pid_is_gone(self):
        with tempfile.TemporaryDirectory() as root:
            proc = proc_fixture(Path(root))

            def proc_path(value):
                value = str(value)
                return proc / value.removeprefix('/proc/').lstrip('/') if value.startswith('/proc/') else proc

            meta = {'pid': 123, 'process_identity': {'boot': 'test-boot', 'start': '10'}}
            with patch.object(worker, 'Path', side_effect=proc_path):
                self.assertFalse(worker.alive(meta))
                (proc / '123/stat').unlink(); (proc / '123').rmdir()
                self.assertTrue(worker.alive(meta))

    def test_coding_and_review_restart_keep_capacity_and_locks(self):
        for kind in ('coding', 'review'):
            with self.subTest(kind=kind):
                state = {'issues': {'1': {'task_id': 'coding', 'review_task_id': 'review' if kind == 'review' else None}}}
                s = Scheduler(state, Limits())
                s.enqueue('key', '1', kind, 'work', 'repo'); s.reserve('key'); s.started('key', kind)
                restored = json.loads(json.dumps(state))
                s = Scheduler(restored, Limits())
                s.recover([{'id': kind, 'project': 'test', 'status': 'RUNNING'}])
                self.assertFalse(s.release('1'))
                self.assertEqual(s.jobs['key']['status'], 'RUNNING')

    def test_done_before_review_and_pass_before_owner_and_commit_before_push(self):
        for owner_state in (None, 'READY_FOR_OWNER', 'COMMITTED'):
            with self.subTest(owner_state=owner_state):
                state = {'issues': {'1': {'task_id': 'coding', 'owner_state': owner_state,
                                         'commit_sha': 'abc' if owner_state == 'COMMITTED' else None}}}
                s = Scheduler(state, Limits())
                s.enqueue('key', '1', 'coding', 'work', 'repo'); s.reserve('key'); s.started('key', 'coding')
                restored = json.loads(json.dumps(state))
                s = Scheduler(restored, Limits())
                s.recover([{'id': 'coding', 'project': 'test', 'status': 'DONE'}])
                self.assertEqual(s.jobs['key']['status'], 'DONE')
                self.assertEqual(restored['issues']['1']['owner_state'], owner_state)

    def test_github_comment_accepted_before_crash_is_not_repeated(self):
        state = {}
        key = effects.enqueue(state, 1, 'comment', 'review passed')
        comments = Mock(return_value=[{'body': '<!-- DEVORCHESTRATOR_EFFECT_V1 ' + key + ' -->'}])
        post = Mock()
        effects.flush(state, Mock(), comments, post, Mock(), Mock(), now=0)
        post.assert_not_called()
        self.assertEqual(state['outbox'][key]['status'], 'DONE')

    def test_transient_github_telegram_and_backoff(self):
        state = {}
        effects.enqueue(state, 1, 'comment', 'report')
        effects.enqueue(state, 1, 'telegram', 'needs owner')
        post = Mock(side_effect=[OSError(), None]); telegram = Mock(side_effect=[False, True])
        effects.flush(state, Mock(), lambda n: [], post, Mock(), telegram, now=0)
        effects.flush(state, Mock(), lambda n: [], post, Mock(), telegram, now=1)
        self.assertEqual(post.call_count, 1)
        effects.flush(state, Mock(), lambda n: [], post, Mock(), telegram, now=3)
        self.assertTrue(all(v['status'] == 'DONE' for v in state['outbox'].values()))

    def test_alive_lease_cannot_be_stolen_failure_can_release(self):
        with tempfile.TemporaryDirectory() as root:
            data = Path(root) / 'tasks'; task = data / 'old'; task.mkdir(parents=True)
            write_json(task / 'meta.json', {'pid': os.getpid(), 'machine': 'work'})
            worker.admit(data, 'work', '/repo', 'old', devctl.task_state)
            with self.assertRaises(RuntimeError): worker.admit(data, 'work', '/repo', 'new', devctl.task_state)
            (task / 'exit_code').write_text('1')
            worker.admit(data, 'work', '/repo', 'new', devctl.task_state)

    def test_telegram_crash_after_acceptance_restart_does_not_resend(self):
        state = {}; effects.enqueue(state, 1, 'telegram', 'final')
        durable = []
        def persist(): durable.append(json.loads(json.dumps(state)))
        telegram = Mock(side_effect=KeyboardInterrupt('accepted before crash'))
        with self.assertRaises(KeyboardInterrupt):
            effects.flush(state, persist, Mock(), Mock(), Mock(), telegram, now=0)
        restored = durable[-1]
        effects.flush(restored, Mock(), Mock(), Mock(), Mock(), telegram, now=0)
        self.assertEqual(telegram.call_count, 1)
        self.assertEqual(next(iter(restored['outbox'].values()))['status'], 'UNCERTAIN')

    def test_telegram_exception_and_unknown_outcome_cannot_crash_or_repeat(self):
        for outcome in (OSError('unknown acceptance'), None):
            state = {}; key = effects.enqueue(state, 1, 'telegram', 'final')
            telegram = Mock(side_effect=outcome) if isinstance(outcome, Exception) else Mock(return_value=None)
            effects.flush(state, Mock(), Mock(), Mock(), Mock(), telegram, now=0)
            effects.flush(state, Mock(), Mock(), Mock(), Mock(), telegram, now=1000)
            self.assertEqual(state['outbox'][key]['status'], 'UNCERTAIN')
            self.assertEqual(telegram.call_count, 1)

    def test_telegram_known_failure_has_bounded_outbox_attempts(self):
        state = {}; key = effects.enqueue(state, 1, 'telegram', 'final')
        telegram = Mock(return_value=False)
        for now in range(0, 1000, 100):
            effects.flush(state, Mock(), Mock(), Mock(), Mock(), telegram, now=now)
        self.assertEqual(telegram.call_count, 5)
        self.assertEqual(state['outbox'][key]['status'], 'FAILED')

    def test_github_label_failure_recovers_without_duplicate_comment(self):
        state = {}; effects.enqueue(state, 1, 'comment', 'result')
        effects.enqueue(state, 1, 'transition', {'issue': {'number': 1}, 'target': 'dev:ready-for-review'})
        post = Mock(); transition = Mock(side_effect=[OSError(), None])
        effects.flush(state, Mock(), lambda n: [], post, transition, Mock(), now=0)
        restored = json.loads(json.dumps(state))
        effects.flush(restored, Mock(), lambda n: [], post, transition, Mock(), now=3)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(transition.call_count, 2)
        self.assertTrue(all(e['status'] == 'DONE' for e in restored['outbox'].values()))

    def test_outbox_batch_cannot_starve_reconciliation(self):
        state = {}
        for number in range(20): effects.enqueue(state, number, 'telegram', str(number))
        telegram = Mock(return_value=True)
        effects.flush(state, Mock(), Mock(), Mock(), Mock(), telegram, now=0)
        self.assertEqual(telegram.call_count, 5)
