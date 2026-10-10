"""Observed issue closure revokes new sends, never already accepted effects.

All providers are isolated fixtures; Git runs only inside temporary repositories.
"""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

import durable
import publication
import task_bridge as bridge
from control_loop import ControlLoop
from transition_io import Command
from transition_executor import execute
import test_resumable_control as helpers


class RemoteRepository:
    """Fake GitHub/remote Git state, without any bridge item or save callback."""
    def __init__(self, repository, snapshot, sha):
        self.project, self.report = repository.project, repository.report
        self.cfg, self.machine, self.workspace = repository.cfg, repository.machine, repository.workspace
        self.sha = sha
        self.view = dict(snapshot, head=sha, status='', origin='https://github.com/owner/repo.git')
        self.view['push_urls'] = [self.view['origin']]
        self.pushed = True
        self.pr = dict(number=7, url='https://github.com/owner/repo/pull/7', state='OPEN',
                       headRefOid=sha, headRefName='feature', baseRefName='main', isCrossRepository=False)
        self.prs = [copy.deepcopy(self.pr)]
        self.effects = []

    def snapshot(self):
        return copy.deepcopy(self.view)

    def git(self, *args):
        if args[:2] == ('remote', 'get-url'):
            return self.view['origin']
        if args[0] == 'ls-remote':
            return self.sha + '\trefs/heads/feature' if self.pushed else ''
        if args[0] == 'push':
            self.effects.append('PUSH')
            self.pushed = True
            return ''
        raise AssertionError(args)

    def run(self, *args):
        if args[:3] == ('gh', 'repo', 'view'):
            return json.dumps(dict(defaultBranchRef={'name': 'main'}, nameWithOwner='owner/repo',
                                   url='https://github.com/owner/repo'))
        if args[:3] == ('gh', 'pr', 'list'):
            return json.dumps(self.prs)
        if args[:3] == ('gh', 'pr', 'create'):
            self.effects.append('CREATE_PR')
            self.prs = [copy.deepcopy(self.pr)]
            return self.pr['url']
        if args[:2] == ('gh', 'api'):
            self.effects.append('MERGE')
            self.prs[0]['state'] = 'MERGED'
            return json.dumps({'merged': True})
        if args[:3] == ('gh', 'pr', 'view'):
            return json.dumps(self.prs[0])
        raise AssertionError(args)


class PublicationClosureTests(unittest.IsolatedAsyncioTestCase):
    setUp = helpers.ResumableControlTests.setUp
    pump = helpers.ResumableControlTests.pump
    coding = helpers.ResumableControlTests.coding
    owner_ready = helpers.ResumableControlTests.owner_ready

    def unrelated(self):
        self.h.issues[2] = dict(self.h.issue(2), labels=[])
        self.h.comments[2] = []
        self.h.state['issues']['2'] = {'queue_status': 'QUEUED', 'runtime_exclusion': 'TEST_HOLD'}

    async def closed(self, loop, item):
        self.h.issues[1]['state'] = 'CLOSED'
        await self.pump(loop, lambda: item.get('issue_closed'))
        self.assertEqual(loop.issues['1']['state'], 'CLOSED')

    async def blocked_commit(self, boundary):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        sends, polls = [], []
        async def detached(command):
            args = command.arguments()
            if command.name == 'control_read':
                polls.append(args['number'])
            if boundary(command):
                entered.set()
                await release.wait()
            if command.name == 'repository' and publication.new_publication_mutation(
                    args['method'], tuple(args.get('args', [])), 'COMMIT'):
                sends.append(args)
            return await execute(command)
        try:
            item = await self.owner_ready(loop)
            self.unrelated()
            original_head = self.h.git('rev-parse', 'HEAD')
            loop.dispatcher.execute = detached
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, entered.is_set)
            await self.closed(loop, item)
            before = polls.count('2')
            await self.pump(loop, lambda: polls.count('2') >= before + 3)
            # Duplicate CLOSED observations must not produce durable churn.
            with patch.object(durable, 'write_json', wraps=durable.write_json) as write:
                for _ in range(10):
                    loop._observe_control('1', dict(issue=copy.deepcopy(self.h.issues[1]), comments=[]))
                write.assert_not_called()
            release.set()
            await self.pump(loop, lambda: '1' not in loop.continuations)
            self.assertEqual(sends, [])
            self.assertNotEqual(item.get('owner_state'), 'COMMITTED')
            self.assertFalse(item.get('commit_sha'))
            self.assertEqual(self.h.git('rev-parse', 'HEAD'), original_head)
        finally:
            release.set()
            await loop.close()

    async def test_close_during_delayed_owner_preflight_blocks_commit(self):
        await self.blocked_commit(lambda c: c.name == 'rpc' and
            c.arguments()['name'] == 'list_tasks' and c.arguments()['arguments'].get('control'))

    async def test_close_during_delayed_repository_snapshot_blocks_commit(self):
        await self.blocked_commit(lambda c: c.name == 'repository' and c.arguments()['method'] == 'snapshot')

    async def test_close_after_commit_object_read_blocks_ref_mutation(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        refs = []
        async def detached(command):
            args = command.arguments()
            if command.name == 'repository' and (args.get('args') or [None])[0] == 'update-ref':
                refs.append(args)
            result = await execute(command)
            if command.name == 'repository' and (args.get('args') or [None])[0] == 'commit-tree':
                entered.set()
                await release.wait()
            return result
        try:
            item = await self.owner_ready(loop)
            loop.dispatcher.execute = detached
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, entered.is_set)
            await self.closed(loop, item)
            release.set()
            await self.pump(loop, lambda: bool(item.get('processed_owner_actions')))
            self.assertEqual(refs, [])
            self.assertFalse(item.get('commit_sha'))
            self.assertTrue(item['publication_journal']['COMMIT']['candidate'])
            self.assertFalse(item['publication_journal']['COMMIT'].get('done'))
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '1')
        finally:
            release.set()
            await loop.close()

    async def test_close_while_mutation_waits_for_capacity_blocks_dispatch(self):
        loop = ControlLoop(self.h.state)
        waiting, release = asyncio.Event(), asyncio.Event()
        refs = []
        original = loop.dispatcher.submit
        def submit(key, command, **kwargs):
            if command.name == 'repository' and (command.arguments().get('args') or [None])[0] == 'update-ref':
                waiting.set()
                if not release.is_set():
                    return False
                refs.append(command)
            return original(key, command, **kwargs)
        try:
            item = await self.owner_ready(loop)
            with patch.object(loop.dispatcher, 'submit', side_effect=submit):
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
                await self.pump(loop, waiting.is_set)
                await self.closed(loop, item)
                release.set()
                await self.pump(loop, lambda: bool(item.get('processed_owner_actions')))
            self.assertEqual(refs, [])
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '1')
            self.assertFalse(bridge.Scheduler(self.h.state).release('1'))
        finally:
            release.set()
            await loop.close()

    async def test_accepted_unacknowledged_commit_survives_closure_and_two_restarts(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        refs = []
        async def detached(command):
            result = await execute(command)
            if command.name == 'repository' and (command.arguments().get('args') or [None])[0] == 'update-ref':
                refs.append(command)
                entered.set()
                await release.wait()
            return result
        try:
            item = await self.owner_ready(loop)
            loop.dispatcher.execute = detached
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, entered.is_set)
            candidate = item['publication_journal']['COMMIT']['candidate']
            self.assertEqual(self.h.git('rev-parse', 'HEAD'), candidate)
            await self.closed(loop, item)
            self.assertEqual(item['publication_journal']['COMMIT']['dispatch_state'], 'SENDING')
            self.assertFalse(bridge.Scheduler(self.h.state).release('1'))
            release.set()
            await loop.close()  # Lose the response; retain the durable receipt.
            for _ in range(2):
                self.h.restart()
                item = self.h.state['issues']['1']
                loop = ControlLoop(self.h.state)
                loop.dispatcher.execute = detached
                await self.pump(loop, lambda: item.get('owner_state') == 'COMMITTED')
                self.assertTrue(item['issue_closed'])
                self.assertTrue(item['publication_journal']['COMMIT']['done'])
                self.assertEqual(item['commit_sha'], candidate)
                await loop.close()
            self.assertEqual(len(refs), 1)
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '2')
        finally:
            release.set()
            await loop.close()

    async def test_closed_reviewer_preparation_cannot_authorize_launch(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        async def detached(command):
            result = await execute(command)
            if command.name == 'prepare_review':
                entered.set()
                await release.wait()
            return result
        try:
            loop.dispatcher.execute = detached
            item = await self.coding(loop)
            await self.pump(loop, entered.is_set)
            key = item['review_transition']['request_id']
            await self.closed(loop, item)
            release.set()
            await self.pump(loop, lambda: item.get('review_transition') is None)
            self.assertEqual(len(self.h.starts), 1)
            self.assertNotIn(key, self.h.requests)
            self.assertFalse(item.get('pending_rpc'))
        finally:
            release.set()
            await loop.close()

    async def remote_closure(self, action, accepted=False):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        try:
            item = await self.owner_ready(loop)
            real = bridge.Repository('test', self.h.tasks[item['task_id']])
            remote = RemoteRepository(real, real.snapshot(), 'e' * 40)
            item.update(owner_state=bridge.OWNER_ACTION_RULES[action][0], commit_sha=remote.sha,
                        publication_branch='feature', publication_origin=remote.view['origin'])
            if action == 'PUSH':
                remote.pushed = False
            if action == 'CREATE_PR':
                remote.prs = []
            if action == 'MERGE':
                item.update(pr_number=remote.pr['number'], pr_url=remote.pr['url'])
            async def detached(command):
                args = command.arguments()
                result = await execute(command)
                if command.name == 'repository':
                    call = tuple(args.get('args') or ())
                    mutation = publication.new_publication_mutation(args['method'], call, action)
                    last_read = (args['method'] == 'git' and call[:1] == ('ls-remote',) if action == 'PUSH'
                                 else args['method'] == 'run' and call[:3] == ('gh', 'pr', 'list'))
                    if (mutation if accepted else last_read) and not entered.is_set():
                        entered.set()
                        await release.wait()
                return result
            loop.dispatcher.execute = detached
            with patch.object(bridge, 'Repository', return_value=remote):
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 ' + json.dumps({'action': action}) + ' -->')
                await self.pump(loop, entered.is_set)
                await self.closed(loop, item)
                release.set()
                if accepted:
                    self.assertEqual(remote.effects, [action])
                    await loop.close()  # Response lost, external state remains.
                    self.h.restart()
                    item = self.h.state['issues']['1']
                    loop = ControlLoop(self.h.state)
                    loop.dispatcher.execute = detached
                    await self.pump(loop, lambda: item.get('owner_state') == bridge.OWNER_ACTION_RULES[action][1])
                    self.assertTrue(item['publication_journal'][action]['done'])
                    self.assertEqual(remote.effects, [action])
                else:
                    await self.pump(loop, lambda: bool(item.get('processed_owner_actions')))
                    self.assertEqual(remote.effects, [])
                    self.assertNotEqual(item['owner_state'], bridge.OWNER_ACTION_RULES[action][1])
        finally:
            release.set()
            await loop.close()

    async def test_closed_after_remote_read_cannot_push(self):
        await self.remote_closure('PUSH')

    async def test_closed_after_pr_read_cannot_create_pr(self):
        await self.remote_closure('CREATE_PR')

    async def test_closed_after_pr_read_cannot_merge(self):
        await self.remote_closure('MERGE')

    async def test_accepted_push_is_reconciled_after_closed_restart(self):
        await self.remote_closure('PUSH', accepted=True)

    async def test_accepted_pr_creation_is_reconciled_after_closed_restart(self):
        await self.remote_closure('CREATE_PR', accepted=True)

    async def test_accepted_merge_is_reconciled_after_closed_restart(self):
        await self.remote_closure('MERGE', accepted=True)

    async def test_fresh_issue_read_blocks_mutation_before_next_control_poll(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        async def detached(command):
            if command.name == 'issue' and command.arguments().get('purpose') == 'publication_boundary':
                entered.set()
                await release.wait()
            return await execute(command)
        try:
            item = await self.owner_ready(loop)
            loop.dispatcher.execute = detached
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, entered.is_set)
            # Freeze ordinary polling; only the publication boundary fetch can
            # observe this close. Advance just this actor to its next boundary.
            self.h.issues[1]['state'] = 'CLOSED'
            release.set()
            for _ in range(500):
                loop.next_heavy = float('inf')
                loop.next_control['1'] = float('inf')
                await loop.step()
                await asyncio.sleep(.002)
                if item.get('processed_owner_actions'):
                    break
            self.assertTrue(item['issue_closed'])
            self.assertFalse(item.get('commit_sha'))
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '1')
        finally:
            release.set()
            await loop.close()

    async def test_revoked_chief_gate_during_read_cannot_update_ref(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        refs = []
        async def detached(command):
            args = command.arguments()
            result = await execute(command)
            if command.name == 'repository' and (args.get('args') or [None])[0] == 'commit-tree':
                entered.set()
                await release.wait()
            if command.name == 'repository' and (args.get('args') or [None])[0] == 'update-ref':
                refs.append(command)
            return result
        try:
            item = await self.owner_ready(loop)
            loop.dispatcher.execute = detached
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, entered.is_set)
            item['chief_status'] = 'CONFLICT'  # Test injects revocation on the writer thread.
            bridge.save_state(self.h.state)
            release.set()
            await self.pump(loop, lambda: bool(item.get('processed_owner_actions')))
            self.assertEqual(refs, [])
            self.assertFalse(item.get('commit_sha'))
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '1')
        finally:
            release.set()
            await loop.close()

    async def test_closed_coding_launch_waiting_for_capacity_is_fenced_without_send(self):
        loop = ControlLoop(self.h.state)
        waiting, release = asyncio.Event(), asyncio.Event()
        original = loop.dispatcher.submit
        sends = []
        def submit(key, command, **kwargs):
            if command.name == 'rpc' and command.arguments()['name'] == 'start_task':
                waiting.set()
                if not release.is_set():
                    return False
                sends.append(command)
            return original(key, command, **kwargs)
        try:
            with patch.object(loop.dispatcher, 'submit', side_effect=submit):
                await self.pump(loop, waiting.is_set)
                item = self.h.state['issues']['1']
                key = item['pending_rpc']['arguments']['request_id']
                await self.closed(loop, item)
                release.set()
                await self.pump(loop, lambda: not item.get('pending_rpc'))
            self.assertEqual(sends, [])
            self.assertEqual(self.h.starts, [])
            self.assertIn(key, self.h.fenced)
            self.assertEqual(self.h.state['work_queue'][key]['no_launch_proof'], 'FENCED')
        finally:
            release.set()
            await loop.close()

    async def test_closed_accepted_coding_launch_is_adopted_after_restart_without_replay(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.h.transport
        async def transport(name, arguments):
            result = await original(name, arguments)
            if name == 'start_task':
                entered.set()
                await release.wait()
            return result
        try:
            with patch.object(bridge, 'transport_call', transport):
                await self.pump(loop, entered.is_set)
                item = self.h.state['issues']['1']
                request = copy.deepcopy(item['pending_rpc'])
                await self.closed(loop, item)
                self.assertEqual(item['pending_rpc'], request)
                release.set()
                await loop.close()
            self.h.restart()
            item = self.h.state['issues']['1']
            loop = ControlLoop(self.h.state)
            await self.pump(loop, lambda: not item.get('pending_rpc'))
            self.assertEqual(len(self.h.starts), 1)
            self.assertEqual(item['task_id'], request['arguments']['request_id'][:24])
            self.assertTrue(item['issue_closed'])
            self.assertFalse(bridge.Scheduler(self.h.state).release('1'))
        finally:
            release.set()
            await loop.close()

    def test_later_publication_hold_blocks_reserved_unrelated_launch_without_state_churn(self):
        key = 'a' * 64
        state = dict(issues={
            '1': {'publication_journal': {'COMMIT': {'candidate': 'b' * 40, 'dispatch_state': 'SENDING'}}},
            '2': {'queue_status': 'QUEUED'}},
            work_queue={
                'old': dict(owner='1', machine='work', repo='repo', kind='coding', status='DONE'),
                key: dict(owner='2', machine='work', repo='repo', kind='coding', status='RESERVED')},
            repo_locks={'repo': {'owner': '2', 'key': key}})
        before = copy.deepcopy(state)
        command = Command.create('rpc', dict(name='start_task', arguments={'request_id': key}))
        with self.assertRaises(bridge.QueueWait):
            bridge.authorize_dispatch('2', command, state, {'number': 2, 'state': 'OPEN'})
        self.assertEqual(state, before)
        self.assertFalse(bridge.Scheduler(state).release('1'))


if __name__ == '__main__':
    unittest.main()
