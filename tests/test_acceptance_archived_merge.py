"""Historical closed MERGE journals must not gain a new GitHub merge send.

Production does not have archived reconciliation. The integrated bridge does.
A repository lock owned by another issue skips that recovery. If recovery does
reach the publication core, reconcile_only still refuses a new merge PUT and
does not invent an outcome or convergence record for a send that never started.
"""
import json
import unittest
from unittest.mock import patch

import devctl
import publication
import task_bridge as bridge
import test_bridge_integration as integration
from publication import PublicationError
from scheduler import Scheduler
from transition_executor import execute
from transition_io import LATEST_ISSUE, WriterContinuation


SHA = 'a' * 40
REPO = 'b' * 64


def historical_merge_item():
    return {
        'task_id': 'task',
        'project': 'test',
        'queue_status': 'STARTED',
        'coding_status': 'DONE',
        'review_status': 'PASS',
        'owner_state': 'PR_CREATED',
        'merged': False,
        'issue_closed': True,
        'runtime_exclusion': 'ISSUE_CLOSED',
        'commit_sha': SHA,
        'publication_branch': 'feature',
        'publication_origin': 'https://github.com/example/repo.git',
        'pr_number': 10,
        'pr_url': 'https://github.com/example/repo/pull/10',
        'chief_required': True,
        'chief_status': 'PASS',
        'publication_journal': {
            'COMMIT': {'done': True, 'command': {'action': 'COMMIT'}},
            'PUSH': {'done': True, 'command': {'action': 'PUSH'}},
            'CREATE_PR': {'done': True, 'command': {'action': 'CREATE_PR'}},
            'MERGE': {'command': {'action': 'MERGE', 'method': 'merge'},
                      'base': 'master', 'repo': 'example/repo'},
        },
    }


class ArchivedMergeTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    def prepare(self, *, lock=False):
        self.h.tasks['task'] = dict(id='task', status='DONE', role='coding', machine='work',
                                     workspace=str(self.h.repo), agent='codex', turns=1, exit_code=0,
                                     runtime_generation='generation', cleanup_state='CLEANED')
        self.h.state['issues'] = {'1': historical_merge_item()}
        if lock:
            self.h.state['issues']['47'] = {
                'project': 'test', 'review_status': 'PASS', 'chief_required': True,
                'chief_status': 'AWAITING_CHIEF_REVIEW', 'coding_status': 'DONE',
            }
            self.h.state['repo_locks'] = {REPO: {'owner': '47', 'key': 'held'}}
        self.h.issues[1]['state'] = 'CLOSED'
        self.calls = []
        bridge.WORK_CONTEXT = (self.h.state, '1', self.h.state['issues']['1'])

    async def drive(self, coroutine):
        writer = WriterContinuation(coroutine)
        writer.context.run(LATEST_ISSUE.set, dict(self.h.issues[1]))
        command = writer.advance()
        while command is not None:
            self.calls.append((command.name, command.arguments()))
            try:
                bridge.authorize_dispatch('1', command, self.h.state, self.h.issues[1])
            except (PublicationError, bridge.QueueWait) as exc:
                if writer.done:
                    break
                command = writer.advance(None, exc)
                continue
            try:
                value = await execute(command)
            except Exception as exc:
                value, error = None, exc
            else:
                error = None
            if writer.done:
                break
            command = writer.advance(value, error)
        return writer

    async def test_foreign_repository_lock_skips_archived_merge_without_mutation(self):
        self.prepare(lock=True)
        before = json.dumps(self.h.state['issues']['1']['publication_journal'], sort_keys=True)
        with patch.object(bridge, 'project_identity', return_value=('work', REPO)):
            writer = await self.drive(bridge.reconcile_archived(self.h.issues[1], self.h.state['issues']['1'], self.h.state))
        self.assertTrue(writer.done)
        self.assertEqual(json.dumps(self.h.state['issues']['1']['publication_journal'], sort_keys=True), before)
        self.assertNotIn('post_merge_convergence', self.h.state['issues']['1'])
        self.assertFalse(any(call[0] == 'repository' for call in self.calls))
        self.assertEqual(self.h.state['repo_locks'][REPO]['owner'], '47')
        self.assertFalse(Scheduler(self.h.state).release('47'))

    def publication_io(self, *, state='OPEN'):
        origin = 'https://github.com/example/repo.git'

        def snapshot(repo):
            self.calls.append(('repo-snapshot',))
            return dict(head=SHA, branch='feature', origin=origin, root=str(self.h.repo),
                        push_urls=[origin], tree='c' * 40, status='', index='d' * 64, fingerprint='e' * 64)

        def run(repo, *args):
            self.calls.append(('repo-run', args))
            if args[:3] == ('gh', 'repo', 'view'):
                return json.dumps({'nameWithOwner': 'example/repo', 'url': 'https://github.com/example/repo',
                                   'defaultBranchRef': {'name': 'master'}})
            if args[:2] == ('git', 'remote'):
                return origin
            if args[:2] == ('git', 'ls-remote'):
                return SHA + '\trefs/heads/feature'
            if args[:3] == ('gh', 'pr', 'list'):
                return json.dumps([{'number': 10, 'url': 'https://github.com/example/repo/pull/10',
                                    'state': state, 'headRefOid': SHA, 'headRefName': 'feature',
                                    'baseRefName': 'master', 'isCrossRepository': False}])
            if args[:3] == ('gh', 'pr', 'view'):
                return json.dumps({'number': 10, 'url': 'https://github.com/example/repo/pull/10',
                                   'state': state, 'headRefOid': SHA, 'isCrossRepository': False})
            if args[:6] == ('gh', 'api', '--hostname', 'github.com', '--method', 'PUT'):
                raise AssertionError('historical closed merge issued a merge PUT')
            raise AssertionError(args)

        projects = {'test': {'machine': 'work', 'workspace': str(self.h.repo),
                             'long_lived_branch': 'feature', 'github_repo': 'example/repo'}}
        return projects, snapshot, run

    async def test_unsent_archived_merge_does_not_invent_pending_or_put(self):
        self.prepare()
        before = json.dumps(self.h.state['issues']['1']['publication_journal'], sort_keys=True)
        projects, snapshot, run = self.publication_io()
        with (patch.object(devctl, 'load_projects', return_value=projects),
              patch.object(publication.Repository, 'snapshot', snapshot),
              patch.object(publication.Repository, 'run', run),
              patch.object(bridge, 'project_identity', return_value=('work', REPO))):
            writer = await self.drive(bridge.reconcile_archived(
                self.h.issues[1], self.h.state['issues']['1'], self.h.state))
        self.assertTrue(writer.done)
        puts = [call for call in self.calls if call[0] == 'repo-run' and call[1][:6] == (
            'gh', 'api', '--hostname', 'github.com', '--method', 'PUT')]
        self.assertEqual(puts, [])
        item = self.h.state['issues']['1']
        self.assertEqual(json.dumps(item['publication_journal'], sort_keys=True), before)
        self.assertNotIn('outcome', item['publication_journal']['MERGE'])
        self.assertNotIn('post_merge_convergence', item)
        self.assertIn('archived_publication', item.get('consistency_diagnostics', {}))
        self.assertFalse(any(call[0] == 'repo-run' and call[1][:3] == ('gh', 'pr', 'view') for call in self.calls))

    async def test_latched_pending_merge_is_kept_without_another_put(self):
        self.prepare()
        item = self.h.state['issues']['1']
        item['publication_journal']['MERGE']['outcome'] = 'PENDING'
        item['post_merge_convergence'] = {
            'status': 'PENDING', 'expected_head': SHA, 'required': True,
            'branch': 'feature', 'base': 'master', 'method': 'merge',
        }
        before = json.dumps(item['publication_journal'], sort_keys=True)
        convergence = json.dumps(item['post_merge_convergence'], sort_keys=True)
        projects, snapshot, run = self.publication_io()
        with (patch.object(devctl, 'load_projects', return_value=projects),
              patch.object(publication.Repository, 'snapshot', snapshot),
              patch.object(publication.Repository, 'run', run),
              patch.object(bridge, 'project_identity', return_value=('work', REPO))):
            writer = await self.drive(bridge.reconcile_archived(
                self.h.issues[1], item, self.h.state))
        self.assertTrue(writer.done)
        puts = [call for call in self.calls if call[0] == 'repo-run' and call[1][:6] == (
            'gh', 'api', '--hostname', 'github.com', '--method', 'PUT')]
        self.assertEqual(puts, [])
        self.assertEqual(json.dumps(item['publication_journal'], sort_keys=True), before)
        self.assertEqual(json.dumps(item['post_merge_convergence'], sort_keys=True), convergence)

    async def test_observed_merged_pr_is_latched_without_put(self):
        self.prepare()
        projects, snapshot, run = self.publication_io(state='MERGED')
        with (patch.object(devctl, 'load_projects', return_value=projects),
              patch.object(publication.Repository, 'snapshot', snapshot),
              patch.object(publication.Repository, 'run', run),
              patch.object(bridge, 'project_identity', return_value=('work', REPO))):
            writer = await self.drive(bridge.reconcile_archived(
                self.h.issues[1], self.h.state['issues']['1'], self.h.state))
        self.assertTrue(writer.done)
        puts = [call for call in self.calls if call[0] == 'repo-run' and call[1][:6] == (
            'gh', 'api', '--hostname', 'github.com', '--method', 'PUT')]
        self.assertEqual(puts, [])
        item = self.h.state['issues']['1']
        self.assertTrue(item.get('merged'))
        self.assertEqual(item['publication_journal']['MERGE'].get('outcome'), 'MERGED')
        self.assertNotIn('post_merge_convergence', item)
