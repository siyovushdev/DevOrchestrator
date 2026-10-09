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

    def test_recovery_keeps_repo_for_next_phase_before_intent_is_admitted(self):
        for status in ('DONE', 'FAILED'):
            with self.subTest(status=status):
                state = {'issues': {'5': {'task_id': 'task-5', 'coding_status': 'RUNNING'},
                                    '3': {'queue_status': 'QUEUED'}}}
                s = Scheduler(state, self.s.limits)
                s.enqueue('coding-5', '5', 'coding', 'work', 'repo')
                s.reserve('coding-5'); s.started('coding-5', 'task-5')
                s.enqueue('coding-3', '3', 'coding', 'work', 'repo')
                s = Scheduler(state, self.s.limits)  # Restart before phase processing.
                for _ in range(2):
                    s.recover([{'id': 'task-5', 'status': status, 'cleanup_state': 'CLEANED'}])
                    self.assertEqual(s.jobs['coding-5']['status'], status)
                    with self.assertRaises(QueueWait): s.reserve('coding-3')
                    self.assertEqual(s.locks['repo']['owner'], '5')
                # Although queued later, the owner's next phase remains eligible.
                s.enqueue('next-5', '5', 'review', 'work', 'repo')
                s.reserve('next-5')
                self.assertEqual(s.locks['repo']['key'], 'next-5')

    def test_recovery_releases_repo_after_durable_issue_outcome(self):
        outcomes = ([{'review_status': status} for status in
                     ('NEEDS_OWNER', 'FAILED', 'FRESH_REVIEW_REQUIRED')] +
                    [{'owner_state': status} for status in
                     ('READY_FOR_OWNER', 'COMMITTED', 'PUSHED', 'PR_CREATED', 'MERGED')] +
                    [{'last_report_signature': 'DONE:1:0'}, {'compatibility_status': 'QUARANTINED'},
                     {'issue_closed': True}, {'completed': True}, {'superseded': True}])
        # Already published records must not acquire a retroactive Chief hold.
        outcomes += [dict(owner_state=status, review_status='PASS', chief_required=True,
                          chief_status='STALE') for status in ('COMMITTED', 'PUSHED', 'PR_CREATED', 'MERGED')]
        outcomes += [{'review_status': 'PASS', 'chief_required': True, 'issue_closed': True},
                     {'review_status': 'PASS', 'publication_journal': {'commit': {'status': 'DONE'}}}]
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                state = {'issues': {'5': dict(task_id='task-5', coding_status='DONE', **outcome)}}
                s = Scheduler(state, self.s.limits)
                s.enqueue('coding-5', '5', 'coding', 'work', 'repo')
                s.reserve('coding-5'); s.started('coding-5', 'task-5')
                s.enqueue('coding-3', '3', 'coding', 'work', 'repo')
                for _ in range(2):
                    s.recover([{'id': 'task-5', 'status': 'DONE', 'cleanup_state': 'CLEANED'}])
                    self.assertNotIn('repo', s.locks)
                s.reserve('coding-3')

    def test_independent_pass_and_incomplete_chief_keep_repo_owned(self):
        outcomes = [{'review_status': 'PASS'},
                    {'review_status': 'PASS', 'owner_state': 'READY_FOR_OWNER',
                     'owner_approved_at': '2026-10-07T00:00:00+00:00',
                     'reviewed_tree': {'fingerprint': 'a' * 64}},
                    {'review_status': 'PASS', 'chief_required': True, 'chief_status': 'AWAITING_CHIEF_REVIEW'},
                    {'review_status': 'PASS', 'chief_required': True, 'chief_status': 'PASS'},
                    {'chief_required': True, 'chief_status': 'CHANGES_REQUIRED', 'last_report_signature': 'DONE:1:0'}]
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                s = Scheduler({'issues': {'5': dict(task_id='task-5', coding_status='DONE', **outcome)}}, self.s.limits)
                s.enqueue('coding-5', '5', 'coding', 'work', 'repo')
                s.reserve('coding-5'); s.started('coding-5', 'task-5')
                s.enqueue('coding-3', '3', 'coding', 'work', 'repo')
                for _ in range(2):
                    s.recover([{'id': 'task-5', 'status': 'DONE', 'cleanup_state': 'CLEANED'}])
                    with self.assertRaises(QueueWait): s.reserve('coding-3')
                    self.assertFalse(s.release('5'))
                    self.assertEqual(s.locks['repo']['owner'], '5')

    def test_post_merge_hold_survives_recovery_archival_and_missing_lock(self):
        for status in ('PENDING', 'BLOCKED'):
            for archived in (False, True):
                with self.subTest(status=status, archived=archived):
                    state = {'issues': {'5': {'owner_state': 'MERGED', 'merged': True,
                             'issue_closed': archived, 'post_merge_convergence': {'status': status}}}}
                    s = Scheduler(state, self.s.limits)
                    s.hold_publication_repository('5', 'work', 'repo')
                    s.enqueue('next', '3', 'coding', 'work', 'repo')
                    s.locks.clear()  # Repair records produced by the previous release bug.
                    self.assertTrue(s.repository_busy('3', 'work', 'repo'))
                    with self.assertRaises(QueueWait): s.reserve('next')
                    s = Scheduler(state, self.s.limits)
                    s.recover([])
                    self.assertFalse(s.release('5'))
                    self.assertEqual(s.locks['repo']['owner'], '5')
                    self.assertIn('post-merge', state['scheduler_diagnostics']['repo'])
                    state['issues']['5']['post_merge_convergence']['status'] = 'DONE'
                    s.recover([])
                    self.assertNotIn('repo', s.locks)
                    self.assertNotIn('repo', state['scheduler_diagnostics'])
                    s.reserve('next')

    def test_publication_hold_cannot_steal_another_owner_repository(self):
        self.job('other', 'repo'); self.s.reserve('other')
        with self.assertRaises(QueueWait):
            self.s.hold_publication_repository('5', 'work', 'repo')
        self.assertEqual(self.s.locks['repo']['owner'], 'other')

    def test_not_required_must_explicitly_exclude_convergence_before_release(self):
        import operational_state
        for required, released in ((True, False), (None, False), (False, True)):
            with self.subTest(required=required):
                item = {'owner_state': 'MERGED', 'merged': True,
                        'post_merge_convergence': {'status': 'NOT_REQUIRED', 'required': required}}
                s = Scheduler({'issues': {'5': item}}, self.s.limits)
                s.hold_publication_repository('5', 'work', 'repo')
                self.assertEqual(operational_state.merge_convergence_complete(item), released)
                self.assertEqual(s.release('5'), released)

    def test_missing_queue_diagnostic_clears_when_ownership_is_proven(self):
        for item in ({'review_status': 'RUNNING'},
                     {'review_status': 'PASS', 'chief_required': True, 'chief_status': 'AWAITING_CHIEF_REVIEW'}):
            for status in ('QUEUED', 'RUNNING', 'DONE'):
                with self.subTest(item=item, status=status):
                    s = Scheduler({'issues': {'5': item},
                                   'repo_locks': {'repo': {'owner': '5', 'key': 'job'}}}, self.s.limits)
                    s.recover([])
                    self.assertIn('repo', s.state['scheduler_diagnostics'])
                    job = s.enqueue('job', '5', 'review', 'work', 'repo')
                    job['status'] = status
                    for _ in range(2):
                        s.recover([])
                        self.assertEqual(s.state['scheduler_diagnostics'], {})
                        self.assertEqual(s.locks['repo']['owner'], '5')
                    s.jobs.clear()
                    s.recover([])
                    self.assertIn('repo', s.state['scheduler_diagnostics'])

    def test_pending_next_phase_overrides_old_terminal_markers(self):
        for intent in ({'pending_rpc': {'operation': 'review:2'}}, {'review_status': 'RUNNING'}, {}):
            with self.subTest(intent=intent):
                # Even an empty tracked record cannot prove lifecycle completion.
                item = dict(last_report_signature='DONE:1:0', owner_state='READY_FOR_OWNER', **intent) if intent else {}
                s = Scheduler({'issues': {'5': item}}, self.s.limits)
                s.enqueue('coding-5', '5', 'coding', 'work', 'repo')
                s.reserve('coding-5'); s.started('coding-5', 'task-5')
                s.recover([{'id': 'task-5', 'status': 'DONE', 'cleanup_state': 'CLEANED'}])
                self.assertEqual(s.locks['repo']['owner'], '5')

    def test_tracked_orphan_lock_is_retained_with_diagnostic(self):
        state = {'issues': {'5': {'task_id': 'task-5', 'coding_status': 'RUNNING'}},
                 'repo_locks': {'repo': {'owner': '5', 'key': 'missing'}}}
        s = Scheduler(state, self.s.limits)
        for _ in range(2):
            s.recover([])
            self.assertEqual(s.locks['repo']['owner'], '5')
            self.assertEqual(state['scheduler_diagnostics'],
                             {'repo': 'Lock retained: missing queue ownership proof'})

    def test_archived_handoff_releases_only_proven_unlaunched_cancellation(self):
        cases = [({}, True), ({'cancelled': None}, False), ({'task_id': 'uncertain-task'}, False),
                 ({'launch_pending': True}, False), ({'runtime_cleanup_pending': True}, False)]
        for extra, released in cases:
            with self.subTest(extra=extra):
                s = Scheduler({'issues': {'5': {'issue_closed': True, 'pending_rpc': {'operation': 'review:1'}}}},
                              self.s.limits)
                s.enqueue('coding-5', '5', 'coding', 'work', 'repo')
                s.reserve('coding-5'); s.started('coding-5', 'task-5')
                job = s.enqueue('review-5', '5', 'review', 'work', 'repo')
                job.update(status='CANCELLED', cancelled='archived_issue')
                job.update(extra)
                for _ in range(2):
                    s.recover([{'id': 'task-5', 'status': 'DONE', 'cleanup_state': 'CLEANED'}])
                    self.assertEqual('repo' not in s.locks, released)

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
