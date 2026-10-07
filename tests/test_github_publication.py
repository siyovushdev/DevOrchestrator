import json
import unittest
import publication as p
import task_bridge as b


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

    def test_interrupted_publications_are_reconciled_without_repeat(self):
        for action in ('PUSH', 'CREATE_PR', 'MERGE'):
            self.repo.fail_after = action
            state = self.item['owner_state']
            with self.assertRaises(p.PublicationError): self.action(action)
            self.assertEqual(self.item['owner_state'], state)
            self.action(action)
            command = {'PUSH': 'push', 'CREATE_PR': 'create', 'MERGE': 'merge'}[action]
            self.assertEqual(sum(c[0] == 'push' if action == 'PUSH' else c[:2] == ('gh', 'api') if action == 'MERGE' else c[:3] == ('gh', 'pr', command) for c in self.repo.calls), 1)

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
