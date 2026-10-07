import unittest
from unittest.mock import patch
from scheduler import Limits, QueueWait, Scheduler


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.state = {'issues': {}}
        self.s = Scheduler(self.state, Limits(coding=2, review=1, total=2, machines={'work': 1, 'firefly': 1}))

    def job(self, key, repo='a', machine='work', kind='coding'):
        return self.s.enqueue(key, key, kind, machine, repo)

    def test_fifo_and_capacity(self):
        self.job('old'); self.job('new', 'b')
        with self.assertRaises(QueueWait): self.s.reserve('new')
        self.s.reserve('old'); self.s.started('old', 'task-old')
        with self.assertRaises(QueueWait): self.s.reserve('new')
        self.s.observe('task-old', 'DONE'); self.s.release('old')
        self.s.reserve('new')

    def test_same_repo_waits_different_repo_runs(self):
        self.job('a'); self.s.reserve('a'); self.s.started('a', 'task-a')
        self.job('b', 'a', 'firefly')
        with self.assertRaises(QueueWait): self.s.reserve('b')
        self.job('c', 'c', 'firefly'); self.s.reserve('c')

    def test_failure_release_and_unknown_retention(self):
        self.job('a'); self.s.reserve('a'); self.s.started('a', 'task-a')
        self.s.observe('task-a', 'UNKNOWN')
        self.assertFalse(self.s.release('a'))
        self.s.observe('task-a', 'FAILED')
        self.assertTrue(self.s.release('a'))
        self.job('b'); self.s.reserve('b')

    def test_review_capacity_and_restart(self):
        self.job('coding'); self.s.reserve('coding'); self.s.started('coding', 'task')
        self.job('review', 'b', 'firefly', 'review'); self.s.reserve('review')
        recovered = Scheduler(self.state, self.s.limits)
        self.assertEqual(recovered.locks['b']['owner'], 'review')
        self.assertFalse(recovered.release('coding'))
        self.s.observe('task', 'DONE'); self.s.release('coding')
        self.job('review-2', 'c', 'work', 'review')
        with self.assertRaises(QueueWait): self.s.reserve('review-2')

    def test_recovery_resolution_failure_holds_task_and_continues(self):
        tasks = [{'id': 'bad', 'project': 'bad-project', 'status': 'RUNNING'},
                 {'id': 'good', 'project': 'good-project', 'status': 'RUNNING', 'role': 'review'}]
        with patch('scheduler.devctl.load_projects', return_value={'bad-project': {'machine': 'firefly'}}), \
             patch('scheduler.project_identity', side_effect=[RuntimeError('offline'), ('work', 'good-repo')]):
            self.s.recover(tasks)
        bad = self.s.jobs['external:bad']
        self.assertEqual(bad['status'], 'UNKNOWN')
        self.assertTrue(bad['identity_pending'])
        self.assertFalse(self.s.release('external:bad'))
        self.assertEqual(self.s.jobs['external:good']['status'], 'RUNNING')
        self.assertEqual(self.s.locks['good-repo']['owner'], 'external:good')

    def test_unresolved_identity_retries_after_restart_and_blocks_machine(self):
        self.s.limits = Limits(coding=3, review=1, total=4, machines={'work': 2, 'firefly': 2})
        task = {'id': 'bad', 'project': 'bad-project', 'status': 'STARTING'}
        with patch('scheduler.devctl.load_projects', return_value={'bad-project': {'machine': 'firefly'}}), \
             patch('scheduler.project_identity', side_effect=OSError()):
            self.s.recover([task])
        self.job('blocked', 'other-repo', 'firefly')
        with self.assertRaises(QueueWait): self.s.reserve('blocked')
        self.job('unaffected', 'work-repo', 'work'); self.s.reserve('unaffected')
        self.s = Scheduler(self.state, self.s.limits)
        with patch('scheduler.project_identity', return_value=('firefly', 'recovered-repo')) as resolve:
            self.s.recover([dict(task, status='RUNNING')])
            resolve.assert_called_once_with('bad-project')
        self.assertFalse(self.s.jobs['external:bad']['identity_pending'])
        self.assertEqual(self.s.jobs['external:bad']['repo'], 'recovered-repo')
        self.assertEqual(self.s.locks['recovered-repo']['owner'], 'external:bad')
        self.s.reserve('blocked')

    def test_unresolved_terminal_task_releases_without_lookup(self):
        task = {'id': 'bad', 'project': 'bad-project', 'status': 'UNKNOWN'}
        with patch('scheduler.devctl.load_projects', side_effect=ValueError()), \
             patch('scheduler.project_identity', side_effect=RuntimeError()):
            self.s.recover([task])
        self.job('blocked')
        with self.assertRaises(QueueWait): self.s.reserve('blocked')
        with patch('scheduler.project_identity') as resolve:
            self.s.recover([dict(task, status='FAILED')])
            resolve.assert_not_called()
        self.assertEqual(self.s.jobs['external:bad']['status'], 'FAILED')
        self.s.reserve('blocked')

    def test_resolved_hold_does_not_steal_or_lose_existing_repo_lock(self):
        self.s.limits = Limits(coding=4, review=1, total=4, machines={'work': 4})
        self.job('original'); self.s.reserve('original'); self.s.started('original', 'original-task')
        task = {'id': 'external-task', 'project': 'alias', 'status': 'RUNNING'}
        with patch('scheduler.devctl.load_projects', return_value={'alias': {'machine': 'work'}}), \
             patch('scheduler.project_identity', side_effect=OSError()):
            self.s.recover([task])
        with patch('scheduler.project_identity', return_value=('work', 'a')):
            self.s.recover([task])
        self.assertEqual(self.s.locks['a']['owner'], 'original')
        self.s.observe('original-task', 'FAILED'); self.s.release('original')
        self.job('next')
        with self.assertRaises(QueueWait): self.s.reserve('next')
        self.assertTrue(self.s.repository_busy('next', 'work', 'a'))
        self.s.recover([dict(task, status='FAILED')])
        self.s.reserve('next')

    def test_active_task_without_project_keeps_machine_hold_then_releases(self):
        task = dict(id='legacy', project='-', machine='firefly', workspace='/unregistered', status='RUNNING')
        with patch('scheduler.devctl.load_projects', return_value={}): self.s.recover([task])
        self.job('blocked', 'repo', 'firefly')
        self.assertFalse(self.s.available(self.s.jobs['blocked']))
        self.job('healthy', 'other', 'work'); self.s.reserve('healthy')
        self.s.recover([dict(task, status='DONE')])
        self.assertEqual(self.s.jobs['external:legacy']['status'], 'DONE')
        self.s.reserve('blocked')
