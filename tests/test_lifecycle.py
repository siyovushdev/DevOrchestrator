from datetime import datetime, timezone
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from durable import read_json, write_json
import lifecycle as l


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / 'tasks'; self.task = self.data / 'task'; self.task.mkdir(parents=True)
        write_json(self.task / 'meta.json', {'id': 'task', 'machine': 'work', 'workspace': '/registered',
            'updated_at': '2020-01-01T00:00:00+00:00', 'pid': 0})
        (self.task / 'exit_code').write_text('0')
        (self.task / 'events.jsonl').write_text('history')
        self.registry = {'test': {'machine': 'work', 'workspace': '/registered'}}
        self.state = {'issues': {'1': {'owner_state': 'MERGED', 'task_id': 'task', 'completed_at': '2020-01-01T00:00:00+00:00'}}}

    def test_dry_run_and_idempotent_archive_preserve_history(self):
        actions = l.plan(self.state, self.data, self.registry)
        self.assertTrue(self.task.exists())
        self.assertEqual(len(actions), 2)
        close = Mock()
        l.apply(self.state, self.data, actions, Mock(), close)
        l.apply(self.state, self.data, actions, Mock(), close)
        self.assertEqual(close.call_count, 1)
        self.assertEqual((self.data / 'archives/task/events.jsonl').read_text(), 'history')
        self.assertTrue(read_json(self.data / 'archives/task/meta.json')['archived'])
        self.assertEqual(l.plan(self.state, self.data, self.registry), [])

    def test_unknown_files_and_unfinished_owner_work_protected(self):
        (self.task / 'user-file').write_text('never touch')
        self.assertFalse(any(a['action'] == 'archive_task' for a in l.plan(self.state, self.data, self.registry)))
        (self.task / 'user-file').unlink()
        self.state['issues']['1']['owner_state'] = 'COMMITTED'
        self.assertEqual(l.plan(self.state, self.data, self.registry), [])
        self.state['issues']['1']['owner_state'] = 'MERGED'
        (self.task / 'exit_code').unlink()
        self.assertFalse(any(a['action'] == 'archive_task' for a in l.plan(self.state, self.data, self.registry)))

    def test_unregistered_and_symlinks_are_ignored(self):
        self.assertFalse(any(a['action'] == 'archive_task' for a in l.plan(self.state, self.data, {})))
        (self.task / 'prompt.txt').symlink_to('/etc/passwd')
        self.assertFalse(any(a['action'] == 'archive_task' for a in l.plan(self.state, self.data, self.registry)))

    @patch.dict(os.environ, {}, clear=True)
    def test_home_disabled_explicit_work_host_required(self):
        with self.assertRaises(RuntimeError): l.worker_guard()
        os.environ['DEVORCHESTRATOR_WORKER'] = 'work'
        os.environ['DEVORCHESTRATOR_WORK_HOST'] = 'WORK'
        os.environ['DEVORCHESTRATOR_QUEUE_LABEL'] = 'work:queued'
        with patch('socket.gethostname', return_value='HOME'), self.assertRaises(RuntimeError): l.worker_guard()
        with patch('socket.gethostname', return_value='WORK'): l.worker_guard()

    def test_apply_rechecks_unknown_files_added_after_plan(self):
        actions = l.plan(self.state, self.data, self.registry)
        (self.task / 'user-file').write_text('preserve')
        l.apply(self.state, self.data, actions, Mock(), Mock())
        self.assertEqual((self.task / 'user-file').read_text(), 'preserve')
        self.assertFalse((self.data / 'archives/task').exists())


class WorkerGuardTests(unittest.TestCase):
    def setUp(self):
        # Exact production configuration reproduced by controlled rollout retry #16.
        env = patch.dict(os.environ, {
            'DEVORCHESTRATOR_WORKER': 'work',
            'DEVORCHESTRATOR_WORK_HOST': 'siovush-Thin-15-B12UC',
            'DEVORCHESTRATOR_QUEUE_LABEL': 'work:queued',
        }, clear=True)
        env.start(); self.addCleanup(env.stop)
        host = patch.object(l.socket, 'gethostname', return_value='siovush-Thin-15-B12UC')
        host.start(); self.addCleanup(host.stop)

    def test_exact_production_work_configuration_passes(self):
        l.worker_guard()

    def test_missing_enrollment_fails_even_with_work_queue(self):
        del os.environ['DEVORCHESTRATOR_WORKER']
        with self.assertRaisesRegex(RuntimeError, 'DEVORCHESTRATOR_WORKER=work'):
            l.worker_guard()

    def test_home_or_unknown_role_cannot_enroll_with_a_queue_label(self):
        for role in ('home', 'firefly', '', 'WORK'):
            with self.subTest(role=role):
                os.environ['DEVORCHESTRATOR_WORKER'] = role
                with self.assertRaisesRegex(RuntimeError, 'DEVORCHESTRATOR_WORKER=work'):
                    l.worker_guard()

    def test_home_hostname_mismatch_fails_even_with_work_enrollment(self):
        with patch.object(l.socket, 'gethostname', return_value='HOME'):
            with self.assertRaisesRegex(RuntimeError, 'matching this WORK host'):
                l.worker_guard()

    def test_missing_or_inexact_host_binding_fails(self):
        for host in (None, '', 'siovush-thin-15-b12uc', 'siovush-Thin-15-B12UC '):
            with self.subTest(host=host):
                if host is None:
                    os.environ.pop('DEVORCHESTRATOR_WORK_HOST', None)
                else:
                    os.environ['DEVORCHESTRATOR_WORK_HOST'] = host
                with self.assertRaisesRegex(RuntimeError, 'matching this WORK host'):
                    l.worker_guard()

    def test_legacy_dev_queue_is_rejected_on_enrolled_work_host(self):
        os.environ['DEVORCHESTRATOR_QUEUE_LABEL'] = 'dev:queued'
        with self.assertRaisesRegex(RuntimeError, 'DEVORCHESTRATOR_QUEUE_LABEL=work:queued'):
            l.worker_guard()

    def test_arbitrary_or_inexact_queue_labels_fail(self):
        for label in ('home:queued', 'unrelated:queued', 'work:running', 'WORK:queued', 'work:queued '):
            with self.subTest(label=label):
                os.environ['DEVORCHESTRATOR_QUEUE_LABEL'] = label
                with self.assertRaisesRegex(RuntimeError, 'DEVORCHESTRATOR_QUEUE_LABEL=work:queued'):
                    l.worker_guard()

    def test_missing_or_empty_queue_label_fails(self):
        for label in (None, ''):
            with self.subTest(label=label):
                if label is None:
                    os.environ.pop('DEVORCHESTRATOR_QUEUE_LABEL', None)
                else:
                    os.environ['DEVORCHESTRATOR_QUEUE_LABEL'] = label
                with self.assertRaisesRegex(RuntimeError, 'DEVORCHESTRATOR_QUEUE_LABEL=work:queued'):
                    l.worker_guard()
