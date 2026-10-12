import json
import shlex
import subprocess
import unittest
from unittest.mock import patch
import operational_state
import publication as p
import task_bridge as b
from scheduler import QueueWait, Scheduler


class FakeRepo:
    def __init__(self):
        self.cfg = {'github_repo': 'owner/repo'}
        self.current = dict(head='abc', branch='feature', origin='git@github.com:owner/repo.git', status='', tree='tree')
        self.remote = None
        self.pr = None
        self.calls = []
        self.fail_after = None

    def snapshot(self): return dict(self.current)

    def git(self, *args):
        self.calls.append(args)
        if args[:2] == ('remote', 'get-url'): return self.current['origin']
        if args[0] == 'ls-remote': return (self.remote + '\trefs/heads/feature') if self.remote else ''
        if args[0] == 'push':
            self.assert_safe_push(args)
            self.remote = 'abc'
            if self.fail_after == 'PUSH': self.fail_after = None; raise p.PublicationError('interrupted')
            return ''
        raise AssertionError(args)

    def assert_safe_push(self, args):
        if args != ('push', '--no-verify', '--no-follow-tags', 'origin', 'abc:refs/heads/feature'): raise AssertionError(args)

    def run(self, *args):
        self.calls.append(args)
        if args[:3] == ('gh', 'repo', 'view'): return json.dumps({'defaultBranchRef': {'name': 'main'}, 'nameWithOwner': 'owner/repo', 'url': 'https://github.com/owner/repo'})
        if args[:3] == ('gh', 'pr', 'list'): return json.dumps([self.pr] if self.pr else [])
        if args[:3] == ('gh', 'pr', 'create'):
            self.pr = dict(number=12, url='https://github.com/owner/repo/pull/12', state='OPEN', headRefOid='abc', isCrossRepository=False)
            if self.fail_after == 'CREATE_PR': self.fail_after = None; raise p.PublicationError('interrupted')
            return self.pr['url']
        if args[:2] == ('gh', 'api'):
            if 'sha=abc' not in args or args[2:6] != ('--hostname', 'github.com', '--method', 'PUT'): raise AssertionError(args)
            self.pr['state'] = 'MERGED'
            if self.fail_after == 'MERGE': self.fail_after = None; raise p.PublicationError('interrupted')
            return json.dumps({'merged': True})
        if args[:3] == ('gh', 'pr', 'view'): return json.dumps(self.pr)
        raise AssertionError(args)


class GithubPublicationTests(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepo()
        self.item = dict(task_id='test', owner_state='COMMITTED', commit_sha='abc', publication_branch='feature',
                         publication_origin='git@github.com:owner/repo.git')

    def action(self, action):
        plan = b.owner_action_plan(self.item['owner_state'], action)
        if plan['status'] == 'ALLOWED':
            p.execute(self.repo, self.item, {}, {'action': action}, lambda: None)
            self.item['owner_state'] = plan['next_state']
        return plan['status']

    def test_all_gates_and_duplicates_no_implicit_merge(self):
        self.assertEqual(self.action('MERGE'), 'REJECTED')
        for action in ('PUSH', 'CREATE_PR', 'MERGE'):
            self.assertEqual(self.action(action), 'ALLOWED')
            calls = len(self.repo.calls)
            self.assertEqual(self.action(action), 'ALREADY_DONE')
            self.assertEqual(len(self.repo.calls), calls)
            if action != 'MERGE': self.assertFalse(self.item.get('merged'))
        self.assertTrue(self.item['merged'])
        self.assertEqual(self.item['post_merge_convergence']['status'], 'NOT_REQUIRED')
        self.assertFalse(self.item['post_merge_convergence']['required'])
        self.assertTrue(b.merge_complete(self.item))

    def test_interrupted_publications_are_reconciled_without_repeat(self):
        for action in ('PUSH', 'CREATE_PR', 'MERGE'):
            self.repo.fail_after = action
            state = self.item['owner_state']
            with self.assertRaises(p.PublicationError): self.action(action)
            self.assertEqual(self.item['owner_state'], state)
            self.action(action)
            command = {'PUSH': 'push', 'CREATE_PR': 'create', 'MERGE': 'merge'}[action]
            self.assertEqual(sum(c[0] == 'push' if action == 'PUSH' else c[:2] == ('gh', 'api') if action == 'MERGE' else c[:3] == ('gh', 'pr', command) for c in self.repo.calls), 1)
        self.assertEqual(self.item['post_merge_convergence']['status'], 'NOT_REQUIRED')

    def test_already_merged_historical_pr_does_not_gain_terminal_proof(self):
        self.action('PUSH'); self.action('CREATE_PR')
        self.repo.pr['state'] = 'MERGED'
        self.action('MERGE')
        self.assertTrue(self.item['merged'])
        self.assertNotIn('post_merge_convergence', self.item)
        self.assertFalse(b.merge_complete(self.item))
        self.assertFalse(any(c[:2] == ('gh', 'api') for c in self.repo.calls))

    def test_new_publication_with_null_assessment_records_explicit_result(self):
        self.action('PUSH'); self.action('CREATE_PR')
        self.item['post_merge_convergence'] = None
        self.action('MERGE')
        self.assertEqual(self.item['post_merge_convergence']['status'], 'NOT_REQUIRED')
        self.assertTrue(b.merge_complete(self.item))

    def test_new_merge_assessment_persisted_before_uncertain_external_result(self):
        self.action('PUSH'); self.action('CREATE_PR')
        saved = []
        self.repo.fail_after = 'MERGE'
        with self.assertRaises(p.PublicationError):
            p.execute(self.repo, self.item, {}, {'action': 'MERGE'},
                      lambda: saved.append(json.loads(json.dumps(self.item))))
        self.assertEqual(saved[-1]['post_merge_convergence']['status'], 'PENDING')
        self.assertNotIn('merged', saved[-1])
        self.assertFalse(b.merge_complete(self.item))

    def test_legacy_required_assessment_cannot_be_downgraded_by_registry_change(self):
        self.action('PUSH'); self.action('CREATE_PR')
        self.item['post_merge_convergence'] = {'status': 'BLOCKED', 'expected_head': 'abc'}
        with self.assertRaisesRegex(p.PublicationError, 'assessment changed'):
            self.action('MERGE')
        self.assertFalse(any(c[:2] == ('gh', 'api') for c in self.repo.calls))
        self.assertEqual(self.item['post_merge_convergence']['status'], 'BLOCKED')

    def test_head_origin_branch_dirty_default_branch_rejected(self):
        for change in ({'head': 'other'}, {'branch': 'other'}, {'origin': 'https://evil.invalid/o/r'}, {'status': ' M file'}):
            before = dict(self.repo.current); self.repo.current.update(change)
            with self.assertRaises(p.PublicationError): self.action('PUSH')
            self.repo.current = before
        self.item['publication_branch'] = 'main'; self.repo.current['branch'] = 'main'
        with self.assertRaises(p.PublicationError): self.action('PUSH')

    def test_wrong_open_pr_and_remote_movement_rejected(self):
        self.action('PUSH')
        self.repo.pr = dict(number=12, url='url', state='OPEN', headRefOid='wrong', isCrossRepository=False)
        with self.assertRaises(p.PublicationError): self.action('CREATE_PR')
        self.assertFalse(any(c[:3] == ('gh', 'pr', 'create') for c in self.repo.calls))
        self.repo.pr = None; self.action('CREATE_PR'); self.repo.remote = 'different'
        with self.assertRaises(p.PublicationError): self.action('MERGE')

    def test_interrupted_action_command_cannot_change(self):
        self.repo.fail_after = 'PUSH'
        with self.assertRaises(p.PublicationError): self.action('PUSH')
        with self.assertRaises(p.PublicationError):
            p.execute(self.repo, self.item, {}, {'action': 'PUSH', 'message': 'different'}, lambda: None)

    def pending_merge(self, command=None):
        self.action('PUSH'); self.action('CREATE_PR')
        state = {'issues': {'1': self.item}}
        s = Scheduler(state)
        s.hold_publication_repository('1', 'work', 'repo')
        s.enqueue('next', '2', 'coding', 'work', 'repo')
        original = self.repo.run
        def run(*args):
            if args[:2] == ('gh', 'api'):
                self.repo.calls.append(args)
                raise p.PublicationError('Merge result unavailable')
            return original(*args)
        with patch.object(self.repo, 'run', side_effect=run), self.assertRaises(p.PublicationError):
            p.execute(self.repo, self.item, {}, command or {'action': 'MERGE'}, lambda: None)
        self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
        self.assertNotIn('merged', self.item)
        # A restart cannot infer that the timed-out external request did nothing.
        state = json.loads(json.dumps(state))
        self.item = state['issues']['1']
        s = Scheduler(state)
        s.recover([])
        self.assertFalse(s.release('1'))
        with self.assertRaises(QueueWait): s.reserve('next')
        return state

    def assert_non_merge_releases(self, state):
        self.assertEqual(self.item['post_merge_convergence']['status'], 'NOT_MERGED')
        self.assertEqual(self.item['publication_journal']['MERGE']['outcome'], 'NOT_MERGED')
        self.assertFalse(b.merge_complete(self.item))
        self.assertFalse(operational_state.post_merge_repository_hold(self.item))
        self.assertEqual(operational_state.lifecycle_state(self.item), 'MERGE_NOT_COMPLETED')
        self.assertEqual(operational_state.next_action(self.item), 'INSPECT_MERGE_RESULT')
        view = operational_state.status_view(state, 1)
        self.assertIn('repository merge hold released', view['convergence']['diagnostic'])
        b.project_merge({'number': 1}, self.item, state)
        self.assertNotIn('completed_at', self.item)
        self.assertNotIn('outbox', state)
        s = Scheduler(state)
        self.assertTrue(s.release('1'))
        s.recover([])
        self.assertNotIn('repo', s.locks)
        s.reserve('next')
        self.assertEqual(s.locks['repo']['owner'], '2')

    def test_closed_after_pending_persists_non_merge_and_releases_on_restart(self):
        for required in (False, True):
            with self.subTest(required=required):
                self.setUp()
                command = {'action': 'MERGE'}
                if required:
                    self.repo.cfg['long_lived_branch'] = 'feature'
                    command['method'] = 'merge'
                state = self.pending_merge(command)
                self.assertEqual(self.item['post_merge_convergence']['required'], required)
                self.repo.pr['state'] = 'CLOSED'
                saved = []
                with self.assertRaisesRegex(p.PublicationError, 'closed without merge'):
                    p.execute(self.repo, self.item, {}, command,
                              lambda: saved.append(json.loads(json.dumps(state))))
                self.assertTrue(saved)
                state = saved[-1]
                self.item = state['issues']['1']
                self.assert_non_merge_releases(state)
                self.assertEqual(self.item['publication_journal']['MERGE']['command'], command)
                self.assertEqual(sum(c[:2] == ('gh', 'api') for c in self.repo.calls), 1)

    def test_early_permanent_error_resolves_closed_pending_without_publication(self):
        for change in ({'status': ' M file'}, {'head': 'moved'}, {'origin': 'https://other.invalid/repo'}):
            with self.subTest(change=change):
                self.setUp()
                state = self.pending_merge()
                self.repo.pr['state'] = 'CLOSED'
                self.repo.current.update(change)
                with self.assertRaises(p.PublicationError): self.action('MERGE')
                self.assert_non_merge_releases(state)
                self.assertEqual(sum(c[:2] == ('gh', 'api') for c in self.repo.calls), 1)

    def test_unavailable_or_unbound_closed_observation_retains_uncertainty(self):
        cases = [None, {'headRefOid': 'wrong'}, {'url': 'https://other.invalid'},
                 {'number': 99}, {'isCrossRepository': True}, {'mergedAt': '2026-10-08T09:00:00Z'},
                 {'mergeCommit': {'oid': 'merged-sha'}}]
        for changed in cases:
            with self.subTest(changed=changed):
                self.setUp()
                state = self.pending_merge()
                self.repo.pr['state'] = 'CLOSED'
                self.repo.current['status'] = ' M file'
                original = self.repo.run
                def run(*args):
                    if args[:3] == ('gh', 'pr', 'view'):
                        if changed is None: raise OSError('https://token:secret.invalid')
                        return json.dumps(dict(self.repo.pr, **changed))
                    return original(*args)
                with patch.object(self.repo, 'run', side_effect=run), self.assertRaises(p.PublicationError):
                    self.action('MERGE')
                self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
                self.assertEqual(operational_state.lifecycle_state(self.item), 'MERGE_UNCERTAIN')
                self.assertEqual(operational_state.next_action(self.item), 'INSPECT_MERGE_RESULT')
                self.assertNotIn('secret.invalid', json.dumps(state))
                s = Scheduler(state); s.recover([])
                self.assertFalse(s.release('1'))
                with self.assertRaises(QueueWait): s.reserve('next')

    def test_explicit_rejection_resolves_fresh_attempt_but_not_prior_uncertainty(self):
        for uncertain in (False, True):
            with self.subTest(uncertain=uncertain):
                self.setUp()
                if uncertain:
                    state = self.pending_merge()
                else:
                    self.action('PUSH'); self.action('CREATE_PR')
                    state = {'issues': {'1': self.item}}
                    s = Scheduler(state); s.hold_publication_repository('1', 'work', 'repo')
                    s.enqueue('next', '2', 'coding', 'work', 'repo')
                original = self.repo.run
                def run(*args):
                    if args[:2] == ('gh', 'api'): return json.dumps({'merged': False})
                    return original(*args)
                with patch.object(self.repo, 'run', side_effect=run), self.assertRaises(p.PublicationError):
                    self.action('MERGE')
                if uncertain:
                    self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
                    self.assertTrue(operational_state.post_merge_repository_hold(self.item))
                else:
                    self.assert_non_merge_releases(state)

    def test_retry_after_definitive_non_merge_reinstates_hold_before_external_call(self):
        state = self.pending_merge()
        self.repo.pr['state'] = 'CLOSED'
        with self.assertRaises(p.PublicationError): self.action('MERGE')
        self.assertTrue(Scheduler(state).release('1'))
        self.repo.pr['state'] = 'OPEN'
        original = self.repo.run
        saved = []
        def run(*args):
            if args[:2] == ('gh', 'api'):
                self.assertEqual(saved[-1]['issues']['1']['post_merge_convergence']['status'], 'PENDING')
                self.assertTrue(operational_state.post_merge_repository_hold(self.item))
                raise p.PublicationError('Merge result unavailable')
            return original(*args)
        with patch.object(self.repo, 'run', side_effect=run), self.assertRaises(p.PublicationError):
            p.execute(self.repo, self.item, {}, {'action': 'MERGE'},
                      lambda: saved.append(json.loads(json.dumps(state))))
        self.assertIsNone(self.item.get('owner_blocker'))
        s = Scheduler(saved[-1]); s.recover([])
        self.assertFalse(s.release('1'))
        with self.assertRaises(QueueWait): s.reserve('next')

    def test_closed_observation_cannot_erase_known_merged_convergence_blocker(self):
        state = self.pending_merge()
        self.item.update(merged=True, owner_state='MERGED')
        self.item['post_merge_convergence'].update(status='BLOCKED', master_sha='merged-sha')
        self.repo.pr['state'] = 'CLOSED'
        with self.assertRaises(p.PublicationError):
            p.execute(self.repo, self.item, {}, {'action': 'MERGE'}, lambda: None)
        self.assertEqual(self.item['post_merge_convergence']['status'], 'BLOCKED')
        self.assertFalse(Scheduler(state).release('1'))

    def test_positive_merge_evidence_is_latched_before_later_failure(self):
        for positive_source in ('api', 'pr_list'):
            with self.subTest(positive_source=positive_source):
                self.setUp()
                state = self.pending_merge()
                if positive_source == 'pr_list':
                    self.repo.pr['state'] = 'MERGED'
                    # A permanent policy error after a bound MERGED observation
                    # must retain that observation, even without convergence yet.
                    self.item['post_merge_convergence']['required'] = True
                original = self.repo.run
                def run(*args):
                    if args[:3] == ('gh', 'pr', 'view'):
                        return json.dumps(dict(self.repo.pr, state='CLOSED'))
                    return original(*args)
                saved = []
                with patch.object(self.repo, 'run', side_effect=run), self.assertRaises(p.PublicationError):
                    p.execute(self.repo, self.item, {}, {'action': 'MERGE'},
                              lambda: saved.append(json.loads(json.dumps(state))))
                self.assertTrue(saved[-1]['issues']['1']['merged'])
                self.assertEqual(saved[-1]['issues']['1']['publication_journal']['MERGE']['outcome'], 'MERGED')
                self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
                self.assertEqual(operational_state.lifecycle_state(self.item), 'POST_MERGE_BLOCKED')
                self.assertFalse(b.merge_complete(self.item))
                s = Scheduler(state); s.recover([])
                self.assertFalse(s.release('1'))
                with self.assertRaises(QueueWait): s.reserve('next')

    def converged_merge_retry(self):
        self.repo.cfg['long_lived_branch'] = 'feature'
        state = self.pending_merge({'action': 'MERGE', 'method': 'merge'})
        target = 'd' * 40
        self.item.update(merged=True, owner_state='MERGED')
        self.item['post_merge_convergence']['master_sha'] = target
        self.repo.current['head'] = self.repo.remote = target
        self.repo.pr.update(state='MERGED', headRefOid=target, headRefName='feature', baseRefName='main')
        return state

    def test_converged_retry_rejects_other_list_head_or_pr_binding(self):
        for changes in ({'headRefOid': 'other'}, {'number': 13}, {'url': 'https://github.com/owner/repo/pull/13'},
                        {'headRefName': 'other'}, {'headRefName': None}, {'baseRefName': 'other'},
                        {'state': 'OPEN'}, {'isCrossRepository': True}):
            with self.subTest(changes=changes):
                self.setUp()
                state = self.converged_merge_retry()
                self.repo.pr.update(changes)
                calls = len(self.repo.calls)
                with patch.object(p, 'converge_merge') as converge, self.assertRaises(p.PublicationError):
                    p.execute(self.repo, self.item, {}, {'action': 'MERGE', 'method': 'merge'}, lambda: None)
                converge.assert_not_called()
                self.assertFalse(any(args[:2] == ('gh', 'api') for args in self.repo.calls[calls:]))
                self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
                self.assertFalse(b.merge_complete(self.item))
                self.assertFalse(Scheduler(state).release('1'))

    def test_converged_retry_rejects_changed_view_head_or_pr_binding(self):
        for changes in ({'headRefOid': 'other'}, {'number': 13}, {'url': 'https://github.com/owner/repo/pull/13'},
                        {'headRefName': 'other'}, {'baseRefName': 'other'}, {'mergeCommit': {'oid': 'e' * 40}},
                        {'isCrossRepository': True}):
            with self.subTest(changes=changes):
                self.setUp()
                state = self.converged_merge_retry()
                original = self.repo.run
                def run(*args):
                    if args[:3] == ('gh', 'pr', 'view'):
                        return json.dumps({**self.repo.pr, 'mergeCommit': {'oid': 'd' * 40}, **changes})
                    return original(*args)
                with patch.object(self.repo, 'run', side_effect=run), patch.object(p, 'converge_merge') as converge:
                    p.execute(self.repo, self.item, {}, {'action': 'MERGE', 'method': 'merge'}, lambda: None)
                converge.assert_not_called()
                self.assertEqual(self.item['post_merge_convergence']['status'], 'BLOCKED')
                self.assertFalse(b.merge_complete(self.item))
                self.assertFalse(Scheduler(state).release('1'))


class NonzeroMergeRefusalTests(unittest.TestCase):
    """Exercise Repository.run's real non-zero capture path, with no external I/O."""
    def setUp(self):
        self.fake = FakeRepo()
        self.fake.remote = 'abc'
        self.fake.pr = dict(number=12, url='https://github.com/owner/repo/pull/12', state='OPEN',
                            headRefOid='abc', isCrossRepository=False)
        registry = patch.object(p.devctl, 'load_projects', return_value={'test': dict(machine='work',
            workspace='/registered/test', github_repo='owner/repo', long_lived_branch='feature')})
        registry.start(); self.addCleanup(registry.stop)
        self.repo = p.Repository('test')
        self.command = {'action': 'MERGE', 'method': 'merge'}
        self.item = dict(task_id='task', owner_state='PR_CREATED', commit_sha='abc', publication_branch='feature',
            publication_origin=self.fake.current['origin'], pr_number=12, pr_url=self.fake.pr['url'])
        self.state = {'issues': {'1': self.item}}
        s = Scheduler(self.state); s.hold_publication_repository('1', 'work', 'repo')
        s.enqueue('next', '2', 'coding', 'work', 'repo')
        self.api_calls = 0
        self.api_result = (1, self.envelope(405), 'gh: refusal (HTTP 405) token=private-error')
        self.view_error = False
        self.proof_changes = {}

    def envelope(self, status, body=None):
        # Match gh's status line plus headers/blank line; no provider text may leak.
        return (f'HTTP/2.0 {status} response\nContent-Type: application/json\r\n'
                'X-Sensitive: private-header\r\n\r\n' +
                json.dumps(body or {'message': 'private-body', 'status': str(status)}))

    def capture(self, machine, command, separate_stderr=False):
        self.assertEqual(machine, 'work'); self.assertTrue(separate_stderr)
        args = shlex.split(command)
        if args[:4] == ['/usr/bin/python3', '-I', '-B', '-c']:
            self.assertEqual(len(args),6);self.assertEqual(args[5],'/registered/test')
            actual=args[4].splitlines();expected=p.SNAPSHOT_SCRIPT.splitlines()
            self.assertRegex(actual[2],r'^_git_end=min\(time.clock_gettime\(time.CLOCK_BOOTTIME\)\+30,[0-9.]+\)$')
            actual[2]=expected[2]
            self.assertEqual(actual,expected)
            return subprocess.CompletedProcess(args, 0, json.dumps(self.fake.snapshot()), '')
        self.assertEqual(args[:3], ['cd', '/registered/test', '&&'])
        args = tuple(args[3:])
        if args[:2] == ('gh', 'api'):
            self.assertIn('--include', args)
            self.assertIn('sha=abc', args); self.assertIn('merge_method=merge', args)
            self.api_calls += 1
            return subprocess.CompletedProcess(args, *self.api_result)
        if self.view_error and args[:3] == ('gh', 'pr', 'view'):
            return subprocess.CompletedProcess(args, 1, '', 'private-view-error')
        result = self.fake.git(*args[1:]) if args[0] == 'git' else self.fake.run(*args)
        if self.proof_changes and args[:3] == ('gh', 'pr', 'view'):
            result = json.dumps({**json.loads(result), **self.proof_changes})
        return subprocess.CompletedProcess(args, 0, result, '')

    def attempt(self):
        saved = []
        with patch.object(p.devctl, 'capture', side_effect=self.capture), self.assertRaises(p.PublicationError) as exc:
            p.execute(self.repo, self.item, {}, self.command,
                      lambda: saved.append(json.loads(json.dumps(self.state))))
        self.assertTrue(saved)
        for text in ('private-header', 'private-body', 'private-error', 'private-view-error'):
            self.assertNotIn(text, json.dumps(self.state)); self.assertNotIn(text, str(exc.exception))
        self.assertEqual(self.api_calls, 1)
        return exc.exception, saved

    def test_nonzero_http_405_and_409_prove_non_merge_and_release(self):
        for code in (405, 409):
            with self.subTest(code=code):
                self.api_result = (1, self.envelope(code), f'gh: private-error (HTTP {code})')
                exc, saved = self.attempt()
                self.assertIsInstance(exc, p.MergeRefused)
                self.assertEqual(exc.status, code)
                result = self.item['post_merge_convergence']
                self.assertEqual(result['status'], 'NOT_MERGED')
                self.assertEqual(result['refusal_http_status'], code)
                self.assertFalse(b.merge_complete(self.item))
                s = Scheduler(saved[-1]); s.recover([])
                self.assertNotIn('repo', s.locks)
                s.reserve('next')
                self.assertEqual(s.locks['repo']['owner'], '2')
                self.api_calls = 0

    def test_transport_server_and_unstructured_errors_keep_pending_hold(self):
        cases = [(1, '', 'network private-error (HTTP 405)'),
                 (1, self.envelope(500), 'private-error'),
                 (255, self.envelope(405), 'SSH private-error'),
                 (1, '{"status":"409","message":"private-body"}', 'private-error'),
                 (1, 'HTTP/2.0 409 response\n', 'private-error')]
        for result in cases:
            with self.subTest(result=result[:2]):
                self.item.pop('post_merge_convergence', None)
                self.item.get('publication_journal', {}).pop('MERGE', None)
                self.api_result = result
                exc, saved = self.attempt()
                self.assertNotIsInstance(exc, p.MergeRefused)
                self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
                s = Scheduler(saved[-1]); s.recover([])
                self.assertFalse(s.release('1'))
                with self.assertRaises(QueueWait): s.reserve('next')
                self.api_calls = 0

    def test_nonzero_refusal_cannot_clear_prior_uncertain_attempt(self):
        self.item['post_merge_convergence'] = dict(status='PENDING', expected_head='abc', required=True,
            method='merge', branch='feature', base='main')
        exc, saved = self.attempt()
        self.assertIsInstance(exc, p.MergeRefused)
        self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
        s = Scheduler(saved[-1]); s.recover([])
        self.assertFalse(s.release('1'))
        with self.assertRaises(QueueWait): s.reserve('next')

    def test_nonzero_refusal_without_bound_pr_proof_keeps_hold(self):
        self.view_error = True
        exc, saved = self.attempt()
        self.assertIsInstance(exc, p.MergeRefused)
        self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
        self.assertFalse(Scheduler(saved[-1]).release('1'))

    def test_nonzero_refusal_with_changed_or_merged_pr_proof_keeps_hold(self):
        for changes in ({'headRefOid': 'other'}, {'isCrossRepository': True},
                        {'url': 'https://github.com/other/repo/pull/12'},
                        {'state': 'MERGED'}, {'mergeCommit': {'oid': 'merged'}}):
            with self.subTest(changes=changes):
                self.item.pop('post_merge_convergence', None)
                self.item.get('publication_journal', {}).pop('MERGE', None)
                self.proof_changes = changes
                exc, saved = self.attempt()
                self.assertIsInstance(exc, p.MergeRefused)
                self.assertEqual(self.item['post_merge_convergence']['status'], 'PENDING')
                s = Scheduler(saved[-1]); s.recover([])
                self.assertFalse(s.release('1'))
                with self.assertRaises(QueueWait): s.reserve('next')
                self.api_calls = 0

    def test_response_classification_is_scoped_to_merge_put_and_strips_success_headers(self):
        args = ('gh', 'api', '--hostname', 'github.com', '--method', 'PUT', 'repos/owner/repo/pulls/12/merge')
        result = subprocess.CompletedProcess([], 0, self.envelope(200, {'merged': True}), '')
        with patch.object(p.devctl, 'capture', return_value=result):
            self.assertEqual(json.loads(self.repo.run(*args)), {'merged': True})
        result = subprocess.CompletedProcess([], 1, self.envelope(409), 'private-error')
        with patch.object(p.devctl, 'capture', return_value=result), self.assertRaises(p.PublicationError) as exc:
            self.repo.run('gh', 'api', '--hostname', 'github.com', '--method', 'GET', 'repos/owner/repo/pulls/12')
        self.assertNotIsInstance(exc.exception, p.MergeRefused)
