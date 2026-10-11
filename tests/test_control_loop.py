"""M2I deterministic control, notification, convergence and telemetry checks."""
import asyncio
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import time
import io
import shlex
import sys
from contextlib import redirect_stdout
import unittest
from unittest.mock import AsyncMock, Mock, patch

import bridge_timing
import chief_review
import devctl
import operational_state
import publication
import task_bridge as b
import test_bridge_integration as integration
import test_chief_runtime as chief
import test_worker_integration as workers
import worker_runtime
from durable import write_json


class ControlTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp
    tick = chief.ChiefRuntimeTests.tick
    pending = chief.ChiefRuntimeTests.pending
    verdict = chief.ChiefRuntimeTests.verdict

    async def test_slow_initial_rpc_keeps_control_polling_and_single_launch_intent(self):
        from transition_executor import execute
        loop = b.ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        polls = []
        async def detached(command):
            if command.name == 'control_read':
                polls.append(command.arguments()['number'])
            if command.name == 'rpc' and command.arguments()['name'] == 'start_task':
                entered.set()
                await release.wait()
            return await execute(command)
        loop.dispatcher.execute = detached
        try:
            await self.pump_control(loop, entered.is_set)
            item = self.h.state['issues']['1']
            pending = copy.deepcopy(item['pending_rpc'])
            before = len(polls)
            await self.pump_control(loop, lambda: len(polls) >= before + 5)
            self.assertEqual(item['pending_rpc'], pending)
            self.assertEqual(self.h.starts, [])
            self.assertIsNone(b.WORK_CONTEXT)
            release.set()
            await self.pump_control(loop, lambda: bool(self.h.starts))
            self.assertEqual(len(self.h.starts), 1)
        finally:
            release.set()
            await loop.close()

    async def test_control_polling_continues_during_rpc_cancellation_cleanup(self):
        from transition_executor import execute
        loop = b.ControlLoop(self.h.state)
        entered, cancelling, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        polls = []
        async def detached(command):
            if command.name == 'control_read':
                polls.append(command.arguments()['number'])
            if command.name == 'rpc' and command.arguments()['name'] == 'start_task':
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelling.set()
                    await release.wait()
            return await execute(command)
        loop.dispatcher.execute = detached
        try:
            await self.pump_control(loop, entered.is_set)
            pending = copy.deepcopy(self.h.state['issues']['1']['pending_rpc'])
            loop.dispatcher.cancel('writer:1')
            await cancelling.wait()
            before = len(polls)
            await self.pump_control(loop, lambda: len(polls) >= before + 5)
            self.assertIn('writer:1', loop.dispatcher.flights)
            self.assertEqual(self.h.state['issues']['1']['pending_rpc'], pending)
            self.assertFalse(self.h.starts)
            release.set()
            await self.pump_control(loop, lambda: bool(self.h.state['issues']['1'].get('reconciliation_retry')))
            self.assertEqual(self.h.state['issues']['1']['pending_rpc'], pending)
        finally:
            release.set()
            await loop.close()

    async def pump_control(self, loop, predicate):
        # Detached Git and durable fsync run in a real isolated filesystem.
        # A tick-count is not a completion deadline; keep a finite wall bound.
        until = time.monotonic() + 10
        while time.monotonic() < until:
            loop.next_heavy = 0
            loop.next_control.clear()
            await loop.step()
            await asyncio.sleep(.002)
            if predicate():
                return
        self.fail('Resumable control loop did not reach expected state')

    def wakeups(self):
        return [call.args[0] for call in self.h.telegram.call_args_list
                if call.args[0].startswith('CHIEF_REVIEW_REQUIRED')]

    async def test_complete_bundle_one_wakeup_across_ticks_and_restart(self):
        item = await self.pending()
        for _ in range(3):
            await b.control_pass(self.h.state)
        self.h.restart()
        await b.control_pass(self.h.state)
        self.assertEqual(len(self.wakeups()), 1)
        text = self.wakeups()[0]
        for value in ('ChatGPT', 'OPEN_CHATGPT_FOR_CHIEF_REVIEW', 'Reviewer:', 'Review round:',
                      item['chief_review']['identity']['bundle_id'][:16], self.h.issues[1]['url']):
            self.assertIn(value, text)
        self.assertNotIn('COMMIT', text)
        self.verdict(self.h.state['issues']['1'])
        await b.control_pass(self.h.state)
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')
        self.assertTrue(any(c.args[0].startswith('REVIEW_PASSED / READY_FOR_OWNER')
                            for c in self.h.telegram.call_args_list))

    async def unsent(self):
        self.h.telegram.return_value = False
        item = await self.pending()
        self.h.telegram.reset_mock()
        self.h.telegram.return_value = True
        entry = next(e for e in self.h.state['outbox'].values()
                     if (e.get('notification') or {}).get('status') == 'CHIEF_REVIEW_REQUIRED')
        entry.update(status='PENDING', next_attempt=0)
        return item, entry

    async def test_stale_bundle_pending_suppressed(self):
        item, entry = await self.unsent()
        item['review_round'] += 1
        item['chief_review']['identity'] = chief_review.build_identity(item).to_dict()
        b.flush_effects(self.h.state)
        self.assertEqual(entry['status'], 'DONE')
        self.assertFalse(self.wakeups())

    async def test_pass_arriving_at_send_boundary_suppresses_pending(self):
        item, entry = await self.unsent()
        self.verdict(item)
        b.flush_effects(self.h.state)
        self.assertEqual(entry['status'], 'DONE')
        self.assertFalse(self.wakeups())
        await b.control_pass(self.h.state)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.assertTrue(any(c.args[0].startswith('REVIEW_PASSED / READY_FOR_OWNER')
                            for c in self.h.telegram.call_args_list))

    async def test_closed_archived_superseded_suppress_pending(self):
        item, entry = await self.unsent()
        for field in ('issue_closed', 'superseded', 'runtime_exclusion'):
            entry.update(status='PENDING', next_attempt=0)
            item[field] = True
            b.flush_effects(self.h.state)
            self.assertEqual(entry['status'], 'DONE')
            item.pop(field)
        self.assertFalse(self.wakeups())

    async def test_heavy_discovery_pending_and_unavailable_does_not_gate_chief_or_owner(self):
        from transition_executor import execute
        item = await self.pending()
        self.verdict(item)
        gate = asyncio.Event()
        loop = b.ControlLoop(self.h.state)
        async def detached(command):
            if command.name == 'discovery':
                await gate.wait()
                raise OSError('token=https://private.invalid')
            return await execute(command)
        loop.dispatcher.execute = detached
        try:
            await self.pump_control(loop, lambda: item.get('owner_state') == 'READY_FOR_OWNER')
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump_control(loop, lambda: item.get('owner_state') == 'COMMITTED')
            sha = item['commit_sha']
            gate.set()
            await self.pump_control(loop, lambda: bool(self.h.state.get('recovery_diagnostic')))
            await loop.close()
            self.h.restart()
            await b.control_pass(self.h.state)
            self.assertEqual(self.h.state['issues']['1']['commit_sha'], sha)
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '2')
            self.assertNotIn('private.invalid', json.dumps(self.h.state))
        finally:
            gate.set()
            await loop.close()

    async def test_fast_rejects_changed_tree_turn_and_uncertain_runtime(self):
        item = await self.pending()
        self.verdict(item)
        await b.control_pass(self.h.state)
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        with patch.object(b, 'publish', wraps=b.publish) as publish:
            self.h.tasks[item['task_id']]['cleanup_state'] = 'AMBIGUOUS'
            await b.control_pass(self.h.state)
            publish.assert_not_called()
        self.h.tasks[item['task_id']].pop('cleanup_state')
        (self.h.repo / 'file').write_text('changed after review')
        await b.control_pass(self.h.state)
        self.assertIsNone(item.get('commit_sha'))
        self.assertEqual(item['review_status'], 'FRESH_REVIEW_REQUIRED')
        self.assertFalse(chief_review.approval_complete(item))

    async def test_stale_chief_outbox_effect_does_not_spend_a_provider_read(self):
        await self.pending()
        key = b.bridge_effects.enqueue(self.h.state, 1, 'chief_comment', 'replaced bundle', ['old-bundle'])
        with patch.object(b, 'fetch_issue') as read, patch.object(b, 'chief_tree_current') as tree:
            b.flush_effects(self.h.state)
        read.assert_not_called()
        tree.assert_not_called()
        self.assertEqual(self.h.state['outbox'][key]['status'], 'DONE')


    async def test_production_loop_full_review_chief_and_commit_flow(self):
        from tests_helpers import coding_result, review_result
        loop = b.ControlLoop(self.h.state)
        async def pump(predicate):
            for _ in range(600):
                loop.next_control.clear()
                loop.next_heavy = 0
                await loop.step()
                await asyncio.sleep(.01)
                if predicate():
                    return
            self.fail('Production loop did not reach the expected durable phase')
        try:
            await pump(lambda: bool(self.h.state['issues'].get('1', {}).get('task_id')))
            item = self.h.state['issues']['1']
            self.h.finish(item['task_id'], coding_result())
            await pump(lambda: bool(item.get('review_task_id')))
            self.h.finish(item['review_task_id'], review_result())
            await pump(lambda: bool((item.get('chief_review') or {}).get('complete')))
            self.assertEqual(item['chief_status'], 'AWAITING_CHIEF_REVIEW')
            self.verdict(item)
            await pump(lambda: item.get('owner_state') == 'READY_FOR_OWNER')
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await pump(lambda: item.get('owner_state') == 'COMMITTED')
            self.assertEqual(self.h.git('rev-parse', 'HEAD'), item['commit_sha'])
            self.assertEqual(len(self.h.starts), 2)
            self.assertFalse(self.h.continues)
        finally:
            await loop.close()

    async def test_production_loop_backfills_legacy_ready_owner_once_and_releases_hold(self):
        from scheduler import Scheduler
        from transition_executor import execute
        item = await self.pending()
        for field in ('chief_review', 'chief_required', 'chief_status', 'independent_review_summary'):
            item.pop(field, None)
        item.update(owner_state='READY_FOR_OWNER', owner_approved_at='2026-01-01T00:00:00+00:00')
        self.h.state['outbox'] = {}
        self.h.comments[1] = []
        self.h.telegram.reset_mock()
        starts, continues = len(self.h.starts), len(self.h.continues)
        self.assertTrue(operational_state.chief_repository_hold(item))
        loop = b.ControlLoop(self.h.state)
        evidence_gate, entered = asyncio.Event(), asyncio.Event()
        async def detached(command):
            if command.name == 'repository' and command.arguments()['method'] == 'review_evidence':
                entered.set()
                await evidence_gate.wait()
            return await execute(command)
        loop.dispatcher.execute = detached
        try:
            with patch.object(chief_review, 'evidence_comments', wraps=chief_review.evidence_comments) as bundle, \
                 patch.object(publication, 'execute_async', wraps=publication.execute_async) as publish:
                await self.pump_control(loop, entered.is_set)
                self.assertFalse(chief_review.approval_complete(item))
                self.assertIsNone(item.get('chief_review'))
                self.assertFalse(Scheduler(self.h.state).release('1'))
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
                early_id = self.h.comments[1][-1]['id']
                for _ in range(10):
                    loop.next_control.clear()
                    await loop.step()
                    await asyncio.sleep(.002)
                publish.assert_not_called()
                evidence_gate.set()
                await self.pump_control(loop, lambda: bool((item.get('chief_review') or {}).get('complete')) and len(self.wakeups()) == 1
                    and early_id in item.get('processed_owner_actions', []))
                bundle.assert_called_once()
                self.assertIsNone(item['owner_state'])
                self.assertFalse(Scheduler(self.h.state).release('1'))
                self.verdict(item)
                await self.pump_control(loop, lambda: item.get('owner_state') == 'READY_FOR_OWNER')
                self.assertTrue(chief_review.approval_complete(item))
                self.assertTrue(Scheduler(self.h.state).release('1'))
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
                command_id = self.h.comments[1][-1]['id']
                await self.pump_control(loop, lambda: item.get('owner_state') == 'COMMITTED')
                await loop.close()
                self.h.restart()
                item = self.h.state['issues']['1']
                loop = b.ControlLoop(self.h.state)
                await self.pump_control(loop, lambda: len(loop.issues) == 1)
                bundle.assert_called_once()
                publish.assert_called_once()
                self.assertEqual(item['processed_owner_actions'].count(command_id), 1)
                self.assertEqual(sum('DEVORCHESTRATOR_CHIEF_BUNDLE_V1' in c['body'] for c in self.h.comments[1]), 1)
                self.assertEqual(sum('COMMIT: REJECTED' in c['body'] for c in self.h.comments[1]), 1)
                self.assertEqual(len(self.wakeups()), 1)
                self.assertEqual(len(self.h.starts), starts)
                self.assertEqual(len(self.h.continues), continues)
                self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '2')
        finally:
            evidence_gate.set()
            await loop.close()


    async def test_heavy_snapshot_mismatch_revokes_pass_without_a_bundle(self):
        item = await self.pending()
        item.pop('chief_review')
        item['chief_status'] = 'AWAITING_CHIEF_REVIEW'
        (self.h.repo / 'file').write_text('changed after review')
        loop = b.ControlLoop(self.h.state)
        try:
            await self.pump_control(loop, lambda: item.get('review_status') == 'FRESH_REVIEW_REQUIRED')
            self.assertIsNone(item.get('reviewed_tree'))
            self.assertFalse(chief_review.approval_complete(item))
        finally:
            await loop.close()


    async def test_legacy_identity_resolution_is_read_only_and_applied_serially(self):
        from control_loop import discover
        task = dict(id='legacy', project='-', machine='work', workspace=str(self.h.repo),
                    agent='codex', role='coding', status='RUNNING')
        transport = self.h.transport
        async def legacy(name, args):
            return [task] if name == 'list_tasks' else await transport(name, args)
        loop = b.ControlLoop(self.h.state)
        with patch.object(b, 'transport_call', side_effect=legacy):
            result = await discover()
            self.assertEqual(self.h.state['issues'], {})
            self.assertIn('test', result['identities'])
            loop._apply_discovery(result)
        self.assertEqual(self.h.state['work_queue']['external:legacy']['status'], 'RUNNING')
        self.assertFalse(self.h.starts)
        await loop.close()


    async def test_same_session_followup_and_stale_turn_stay_closed_on_fast_path(self):
        item = await self.pending()
        self.verdict(item)
        await b.control_pass(self.h.state)
        self.h.comment(b.FOLLOWUP_MARKER + '\nFix an edge case')
        await b.control_pass(self.h.state)
        self.assertEqual(self.h.continues[0][0], item['task_id'])
        self.assertEqual(item['coding_turn'], 2)
        self.assertFalse(chief_review.approval_complete(item))
        self.assertIsNone(item.get('owner_state'))
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        await b.control_pass(self.h.state)
        self.assertIsNone(item.get('commit_sha'))
        self.assertTrue(any('COMMIT: REJECTED' in c['body'] for c in self.h.comments[1]))

    async def test_slow_telegram_callback_cannot_gate_commands_and_crash_is_uncertain(self):
        import threading
        item = await self.pending()
        self.verdict(item)
        release = threading.Event()
        entered = asyncio.Event()
        event_loop = asyncio.get_running_loop()
        def slow(text):
            event_loop.call_soon_threadsafe(entered.set)
            release.wait(10)
            return True
        loop = b.ControlLoop(self.h.state)
        try:
            with patch.object(b, 'send_telegram', side_effect=slow):
                await self.pump_control(loop, entered.is_set)
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
                await self.pump_control(loop, lambda: item.get('owner_state') == 'COMMITTED')
                sending = list(loop.telegram_tasks)
                self.assertTrue(sending)
                self.h.restart()
                b.flush_effects(self.h.state)
                self.assertTrue(all(self.h.state['outbox'][key]['status'] == 'UNCERTAIN' for key in sending))
        finally:
            release.set()
            await loop.close()

    async def test_wakeup_acceptance_uncertainty_never_resends(self):
        self.h.telegram.return_value = None
        item = await self.pending()
        calls = len(self.wakeups())
        self.assertEqual(calls, 1)
        self.h.restart()
        for _ in range(2):
            await b.control_pass(self.h.state)
        self.assertEqual(len(self.wakeups()), calls)
        entry = next(e for e in self.h.state['outbox'].values()
                     if (e.get('notification') or {}).get('status') == 'CHIEF_REVIEW_REQUIRED')
        self.assertEqual(entry['status'], 'UNCERTAIN')


    async def test_completion_projection_requires_convergence_and_is_idempotent(self):
        await self.pending()
        item = self.h.state['issues']['1']
        item.update(merged=True, owner_state='MERGED', post_merge_convergence={'status': 'BLOCKED'})
        b.project_merge(self.h.issues[1], item, self.h.state)
        self.assertFalse(any(e['kind'] == 'completion' for e in self.h.state['outbox'].values()))
        self.assertEqual(operational_state.next_action(item), 'INSPECT_POST_MERGE_CONVERGENCE')
        item['post_merge_convergence']['status'] = 'DONE'
        item.update(owner_state='PR_CREATED', publication_journal={'MERGE': {'command': {'action': 'MERGE', 'method': 'merge'}}})
        b.restore_merged_state(item, self.h.state)
        self.assertEqual(item['owner_state'], 'MERGED')
        b.project_merge(self.h.issues[1], item, self.h.state)
        original = self.h.gh
        def gh(*args):
            if args[:2] == ('issue', 'close'):
                self.h.issues[int(args[2])]['state'] = 'CLOSED'
                return ''
            return original(*args)
        with patch.object(b, 'gh', side_effect=gh):
            for _ in range(3):
                b.flush_effects(self.h.state)
        self.assertEqual(self.h.issues[1]['state'], 'CLOSED')
        self.assertEqual(b.label_names(self.h.issues[1]) & b.STATE_LABELS, {'dev:done'})
        self.assertEqual(sum('DEVORCHESTRATOR_MERGE_COMPLETE' in c['body'] for c in self.h.comments[1]), 1)

    async def test_historical_merge_missing_record_stays_archival_on_both_passes(self):
        item = await self.pending()
        item.update(merged=True, owner_state='MERGED',
                    publication_journal={'MERGE': {'command': {'action': 'MERGE', 'method': 'merge'}}})
        self.assertFalse(b.merge_complete(item))
        # Even a pending completion from an older version cannot close the issue.
        key = b.bridge_effects.enqueue(self.h.state, 1, 'completion', {'number': 1}, ['legacy'])
        for run in (b.control_pass, b.reconcile_tick):
            await run(self.h.state)
            b.flush_effects(self.h.state)
            self.assertEqual(self.h.issues[1].get('state', 'OPEN'), 'OPEN')
            self.assertNotIn('dev:done', b.label_names(self.h.issues[1]))
            self.assertNotIn('completed_at', item)
            self.assertNotIn('post_merge_convergence', item)
        self.assertEqual(self.h.state['outbox'][key]['status'], 'DONE')
        self.assertFalse(any('DEVORCHESTRATOR_MERGE_COMPLETE' in c['body'] for c in self.h.comments[1]))
        self.assertEqual(operational_state.lifecycle_state(item), 'ARCHIVED')

    async def test_merge_boundary_reacquires_released_repository_and_blocks_next_task(self):
        from scheduler import Scheduler, QueueWait
        item = await self.pending()
        self.verdict(item)
        await b.control_pass(self.h.state)
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        await b.control_pass(self.h.state)
        item['owner_state'] = 'PR_CREATED'
        machine, repo = b.project_identity(item['project'])
        scheduler = Scheduler(self.h.state)
        self.assertTrue(scheduler.release('1'))
        scheduler.enqueue('next', '2', 'coding', machine, repo)
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"MERGE","method":"merge"} -->')
        def blocked(repository, target, report, command, persist):
            self.assertEqual(self.h.state['repo_locks'][repo]['owner'], '1')
            target.update(merged=True, post_merge_convergence={'status': 'BLOCKED', 'diagnostic': 'PR merged; test failure'})
            persist()
        with patch.object(b, 'publish', side_effect=blocked):
            await b.control_pass(self.h.state)
        self.assertEqual(item['owner_state'], 'MERGED')
        self.h.restart()
        scheduler = Scheduler(self.h.state)
        scheduler.recover([])
        self.assertFalse(scheduler.release('1'))
        with self.assertRaises(QueueWait):
            scheduler.reserve('next')
        self.assertEqual(self.h.issues[1].get('state', 'OPEN'), 'OPEN')

    async def test_uncertain_merge_blocks_same_session_followup_before_gate_invalidation(self):
        item = await self.pending()
        self.verdict(item)
        await b.control_pass(self.h.state)
        item.update(owner_state='PR_CREATED', post_merge_convergence={'status': 'PENDING'})
        before = copy.deepcopy(item)
        self.h.comment(b.FOLLOWUP_MARKER + '\nFix another edge case')
        await b.control_pass(self.h.state)
        self.assertFalse(self.h.continues)
        for key in ('coding_turn', 'owner_state', 'chief_review', 'post_merge_convergence'):
            self.assertEqual(item.get(key), before.get(key))
        self.assertNotIn('pending_rpc', item)
        self.assertFalse(item.get('processed_followups'))
        self.assertNotIn('control', item.get('consistency_diagnostics', {}))

    async def check_pending_merge_with_deferred_followup(self, path='control', followup=None, refused=False):
        from scheduler import Scheduler, QueueWait
        from test_github_publication import FakeRepo
        self.h.git('remote', 'add', 'origin', 'git@github.com:owner/repo.git')
        item = await self.pending()
        self.verdict(item)
        await b.control_pass(self.h.state)
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        await b.control_pass(self.h.state)
        command = {'action': 'MERGE', 'method': 'merge'}
        repo = FakeRepo()
        repo.current.update(head=item['commit_sha'], branch=item['publication_branch'])
        repo.remote = item['commit_sha']
        repo.pr = dict(number=12, url='https://github.com/owner/repo/pull/12', state='OPEN' if refused else 'CLOSED',
                       headRefOid=item['commit_sha'], isCrossRepository=False)
        item.update(owner_state='PR_CREATED', publication_origin=repo.current['origin'],
                    pr_number=12, pr_url=repo.pr['url'],
                    post_merge_convergence=dict(status='PENDING', expected_head=item['commit_sha'],
                        required=False, method='merge', branch=item['publication_branch'], base='main'),
                    publication_journal={'MERGE': {'command': command, 'repo': 'owner/repo', 'base': 'main'}})
        machine, repo_key = b.project_identity(item['project'])
        s = Scheduler(self.h.state)
        s.hold_publication_repository('1', machine, repo_key)
        s.enqueue('next', '2', 'coding', machine, repo_key)
        if followup:
            self.h.comment(followup)
            followup_id = self.h.comments[1][-1]['id']
        before = copy.deepcopy(item)
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 ' + json.dumps(command) + ' -->')
        merge_id = self.h.comments[1][-1]['id']
        original = repo.run
        def run(*args):
            if args[:2] == ('gh', 'api'):
                repo.calls.append(args)
                raise publication.MergeRefused(405)
            return original(*args)
        async def process():
            if path == 'control':
                await b.control_pass(self.h.state)
            else:
                await b.process_tracked(self.h.issues[1], item, self.h.state)
                b.WORK_CONTEXT = None
                b.flush_effects(self.h.state)
        with patch.object(b, 'Repository', return_value=repo), patch.object(repo, 'run', side_effect=run):
            await process()
            self.assertIn(merge_id, item['processed_owner_actions'])
            if followup:
                self.assertNotIn(followup_id, item.get('processed_followups', []))
                self.assertNotIn(followup_id, item['processed_owner_actions'])
            self.assertFalse(self.h.continues)
            self.assertNotIn('pending_rpc', item)
            for key in ('coding_turn', 'owner_state', 'chief_review', 'reviewed_tree'):
                self.assertEqual(item.get(key), before.get(key))
            self.assertNotIn('control', item.get('consistency_diagnostics', {}))
            self.assertNotIn('reconciliation_retry', item)
            if refused:
                self.assertEqual(item['post_merge_convergence']['status'], 'PENDING')
                self.assertTrue(any('HTTP 405' in c['body'] for c in self.h.comments[1]))
                await process()
                self.assertEqual(sum(args[:2] == ('gh', 'api') for args in repo.calls), 1)
                self.h.restart()
                s = Scheduler(self.h.state); s.recover([])
                self.assertFalse(s.release('1'))
                with self.assertRaises(QueueWait): s.reserve('next')
                self.assertNotIn(followup_id, self.h.state['issues']['1'].get('processed_followups', []))
                return
        self.assertEqual(item['post_merge_convergence']['status'], 'NOT_MERGED')
        self.assertTrue(any('closed without merge' in c['body'] for c in self.h.comments[1]))
        self.assertTrue(any('FAILED' in call.args[0] and 'INSPECT_MERGE_RESULT' in call.args[0]
                            and 'closed without merging' in call.args[0]
                            for call in self.h.telegram.call_args_list))
        self.assertEqual(self.h.issues[1]['state'], 'OPEN')
        self.assertNotIn('dev:done', self.h.issues[1]['labels'])
        self.h.restart()
        s = Scheduler(self.h.state); s.recover([])
        self.assertNotIn(repo_key, s.locks)
        s.reserve('next')
        self.assertEqual(s.locks[repo_key]['owner'], '2')

    async def test_closed_pending_merge_is_acknowledged_and_releases_queue_after_restart(self):
        await self.check_pending_merge_with_deferred_followup()

    async def test_control_pending_followup_does_not_starve_merge_proof(self):
        await self.check_pending_merge_with_deferred_followup(followup=b.FOLLOWUP_MARKER + '\nFix an edge case')

    async def test_tracked_pending_followup_does_not_starve_merge_proof(self):
        await self.check_pending_merge_with_deferred_followup(path='tracked', followup=b.FOLLOWUP_MARKER + '\nFix an edge case')

    async def test_control_pending_answer_does_not_starve_merge_proof(self):
        await self.check_pending_merge_with_deferred_followup(followup=
            '<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Use SQLite"} -->')

    async def test_tracked_pending_answer_does_not_starve_merge_proof(self):
        await self.check_pending_merge_with_deferred_followup(path='tracked', followup=
            '<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Use SQLite"} -->')

    async def test_control_pending_followup_allows_refused_merge_retry_without_clearing_uncertainty(self):
        await self.check_pending_merge_with_deferred_followup(refused=True, followup=b.FOLLOWUP_MARKER + '\nFix an edge case')

    async def test_tracked_pending_followup_allows_refused_merge_retry_without_clearing_uncertainty(self):
        await self.check_pending_merge_with_deferred_followup(path='tracked', refused=True,
            followup=b.FOLLOWUP_MARKER + '\nFix an edge case')

    async def test_non_merge_correction_reviews_new_commit_and_republishes_same_session(self):
        from test_github_publication import FakeRepo
        from tests_helpers import coding_result, review_result
        self.h.git('remote', 'add', 'origin', 'git@github.com:owner/repo.git')
        item = await self.pending()
        coding_id = item['task_id']
        self.verdict(item)
        await b.control_pass(self.h.state)
        async def command(action):
            payload = {'action': action}
            if action == 'MERGE': payload['method'] = 'merge'
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 ' + json.dumps(payload) + ' -->')
            await b.control_pass(self.h.state)
        await command('COMMIT')
        old = item['commit_sha']
        fake = FakeRepo()
        fake.current = publication.Repository('test').snapshot()
        heads = []
        def git(*args):
            if args[0] == 'push':
                self.assertEqual(args, ('push', '--no-verify', '--no-follow-tags', 'origin',
                                       fake.current['head'] + ':refs/heads/feature'))
                fake.remote = fake.current['head']
                if fake.pr: fake.pr['headRefOid'] = fake.remote
                return ''
            return FakeRepo.git(fake, *args)
        def run(*args):
            if args[:3] == ('gh', 'pr', 'create'):
                fake.pr = dict(number=12, url='https://github.com/owner/repo/pull/12', state='OPEN',
                               headRefOid=fake.current['head'], isCrossRepository=False)
                return fake.pr['url']
            if args[:2] == ('gh', 'api'):
                self.assertIn('sha=' + fake.current['head'], args)
                self.assertIn('merge_method=merge', args)
                heads.append(item['post_merge_convergence']['expected_head'])
                if len(heads) == 1: return json.dumps({'merged': False})
                fake.pr['state'] = 'MERGED'
                return json.dumps({'merged': True})
            return FakeRepo.run(fake, *args)
        with patch.object(fake, 'git', side_effect=git), patch.object(fake, 'run', side_effect=run), \
             patch.object(b, 'Repository', return_value=fake):
            for action in ('PUSH', 'CREATE_PR', 'MERGE'): await command(action)
        self.assertEqual(item['post_merge_convergence']['status'], 'NOT_MERGED')
        self.h.comment(b.FOLLOWUP_MARKER + '\nCorrect the rejected publication')
        await b.control_pass(self.h.state)
        self.assertEqual(self.h.continues[-1][0], coding_id)
        self.assertNotIn('post_merge_convergence', item)
        self.assertEqual(item['publication_journal'], {})
        self.assertFalse(chief_review.approval_complete(item))
        (self.h.repo / 'file').write_text('corrected implementation')
        self.h.finish(coding_id, coding_result())
        await self.tick()
        self.h.finish(item['review_task_id'], review_result())
        for _ in range(35):
            await self.tick()
            record = item.get('chief_review') or {}
            if record.get('complete') and all(self.h.state['outbox'][key]['status'] == 'DONE'
                                               for key in record['effects']): break
        self.verdict(item)
        await b.control_pass(self.h.state)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        await command('COMMIT')
        self.assertEqual(item['owner_state'], 'COMMITTED')
        new = item['commit_sha']
        self.assertNotEqual(old, new)
        fake.current = publication.Repository('test').snapshot()
        original = self.h.gh
        def gh(*args):
            if args[:2] == ('issue', 'close'):
                self.h.issues[int(args[2])]['state'] = 'CLOSED'
                return ''
            return original(*args)
        with patch.object(fake, 'git', side_effect=git), patch.object(fake, 'run', side_effect=run), \
             patch.object(b, 'Repository', return_value=fake), patch.object(b, 'gh', side_effect=gh):
            for action in ('PUSH', 'CREATE_PR', 'MERGE'): await command(action)
        self.assertEqual(heads, [old, new])
        self.assertEqual(self.h.git('rev-parse', 'HEAD'), new)
        self.assertEqual(item['task_id'], coding_id)
        self.assertEqual(item['post_merge_convergence']['expected_head'], new)
        self.assertTrue(b.merge_complete(item))
        self.assertEqual(self.h.issues[1]['state'], 'CLOSED')


class ConvergenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.remote = self.root / 'remote.git'; self.work = self.root / 'work'
        self.shell('git', 'init', '--bare', str(self.remote))
        self.shell('git', 'init', '-b', 'master', str(self.work))
        self.git('config', 'user.name', 'Test'); self.git('config', 'user.email', 'test@example.invalid')
        (self.work / 'file').write_text('baseline')
        self.git('add', '.'); self.git('commit', '-m', 'baseline')
        self.baseline = self.git('rev-parse', 'HEAD')
        self.git('checkout', '-b', 'dev/self-development')
        (self.work / 'file').write_text('coding')
        self.git('commit', '-am', 'coding'); self.old = self.git('rev-parse', 'HEAD')
        self.git('checkout', 'master')
        (self.work / 'base-file').write_text('master advancement')
        self.git('add', '.'); self.git('commit', '-m', 'master advancement')
        self.base_before = self.git('rev-parse', 'HEAD')
        self.git('merge', '--no-ff', 'dev/self-development', '-m', 'normal merge')
        self.target = self.git('rev-parse', 'HEAD')
        self.git('remote', 'add', 'origin', str(self.remote))
        self.git('push', 'origin', 'master', 'dev/self-development')
        self.git('checkout', 'dev/self-development')
        registry = patch.object(devctl, 'load_projects', return_value={'test': {'machine': 'work', 'workspace': str(self.work)}})
        registry.start(); self.addCleanup(registry.stop)
        self.repo = publication.Repository('test')
        self.item = {'commit_sha': self.old, 'publication_branch': 'dev/self-development',
                     'publication_origin': str(self.remote), 'merged': True,
                     'post_merge_convergence': {'status': 'PENDING', 'master_sha': self.target}}
        self.entry = {'base': 'master'}

    def shell(self, *args):
        return subprocess.check_output(args, stderr=subprocess.DEVNULL).decode().strip()

    def git(self, *args):
        return self.shell('git', '-C', str(self.work), *args)

    def remote_ref(self, name):
        return self.shell('git', '--git-dir=' + str(self.remote), 'rev-parse', name)

    def converge(self):
        publication.converge_merge(self.repo, self.item, self.entry, lambda: None)
        return self.item['post_merge_convergence']['status']

    def execute_merge(self, missing_identity=False):
        self.repo.cfg.update(github_repo='owner/repo', long_lived_branch='dev/self-development')
        self.item.update(task_id='task', pr_number=7, pr_url='https://github.com/owner/repo/pull/7',
                         publication_origin='git@github.com:owner/repo.git')
        self.item.pop('post_merge_convergence'); self.item.pop('merged')
        state = {'pr': 'OPEN'}
        original_run, original_git, original_snapshot = self.repo.run, self.repo.git, self.repo.snapshot
        def snapshot():
            return dict(original_snapshot(), origin=self.item['publication_origin'])
        def git(*args):
            if args[:2] == ('remote', 'get-url'):
                return self.item['publication_origin']
            return original_git(*args)
        def run(*args):
            if args[:3] == ('gh', 'repo', 'view'):
                return json.dumps(dict(defaultBranchRef={'name': 'master'}, nameWithOwner='owner/repo',
                                       url='https://github.com/owner/repo'))
            pr = dict(number=7, url=self.item['pr_url'], state=state['pr'], headRefOid=self.old,
                      headRefName='dev/self-development', baseRefName='master', isCrossRepository=False,
                      mergeCommit={'oid': self.target})
            if args[:3] == ('gh', 'pr', 'list'):
                return json.dumps([pr])
            if args[:3] == ('gh', 'pr', 'view'):
                if missing_identity:
                    pr['mergeCommit'] = {}
                return json.dumps(pr)
            if args[:2] == ('gh', 'api'):
                self.assertIn('merge_method=merge', args)
                self.assertIn('sha=' + self.old, args)
                state['pr'] = 'MERGED'
                return json.dumps({'merged': True})
            return original_run(*args)
        saved = []
        with patch.object(self.repo, 'snapshot', side_effect=snapshot), patch.object(self.repo, 'git', side_effect=git), \
             patch.object(self.repo, 'run', side_effect=run):
            publication.execute(self.repo, self.item, {}, {'action': 'MERGE', 'method': 'merge'},
                                lambda: saved.append(copy.deepcopy(self.item)))
        self.assertTrue(self.item['merged'])
        self.assertEqual(self.item['post_merge_convergence']['status'], 'BLOCKED' if missing_identity else 'DONE')
        self.assertTrue(self.item['publication_journal']['MERGE']['done'])
        self.assertTrue(any(i.get('post_merge_convergence', {}).get('status') == 'PENDING' for i in saved))


    def test_execute_explicit_merge_persists_verified_convergence(self):
        self.execute_merge()

    def test_execute_merged_but_identity_unavailable_records_blocker(self):
        self.execute_merge(missing_identity=True)
        self.item['owner_state'] = 'MERGED'
        self.assertFalse(b.merge_complete(self.item))
        self.assertNotIn('completed_at', self.item)
        self.assertIn('PR merged', self.item['post_merge_convergence']['diagnostic'])
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.old)
        self.assertEqual(self.remote_ref('dev/self-development'), self.old)

    def test_normal_merge_ff_local_remote_and_restart(self):
        self.assertEqual(self.converge(), 'DONE')
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.target)
        self.assertEqual(self.remote_ref('dev/self-development'), self.target)
        self.assertEqual(self.remote_ref('master'), self.target)
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual((self.work / 'base-file').read_text(), 'master advancement')
        self.assertEqual(self.converge(), 'DONE')

    def check_execute_retry_after_remote_fast_forward(self, blocked=False):
        from scheduler import Scheduler, QueueWait
        command = {'action': 'MERGE', 'method': 'merge'}
        self.repo.cfg.update(github_repo='owner/repo', long_lived_branch='dev/self-development')
        self.item.update(task_id='task', owner_state='PR_CREATED', pr_number=7,
            pr_url='https://github.com/owner/repo/pull/7', publication_origin='git@github.com:owner/repo.git',
            publication_journal={'MERGE': dict(command=command, repo='owner/repo', base='master', outcome='MERGED')})
        self.item['post_merge_convergence'].update(expected_head=self.old, required=True, method='merge',
            branch='dev/self-development', base='master')
        state = {'issues': {'1': self.item}}
        scheduler = Scheduler(state)
        scheduler.hold_publication_repository('1', 'work', 'repo')
        scheduler.enqueue('next', '2', 'coding', 'work', 'repo')
        saved, pushes = [], []
        original_run, original_git, original_snapshot = self.repo.run, self.repo.git, self.repo.snapshot
        class CrashAfterPush(BaseException):
            pass
        def snapshot():
            return dict(original_snapshot(), origin=self.item['publication_origin'])
        def git(*args):
            if args[:2] == ('remote', 'get-url'):
                return self.item['publication_origin']
            return original_git(*args)
        def run(*args):
            if args[:3] == ('gh', 'repo', 'view'):
                return json.dumps(dict(defaultBranchRef={'name': 'master'}, nameWithOwner='owner/repo',
                    url='https://github.com/owner/repo'))
            tip = self.remote_ref('dev/self-development')
            pr = dict(number=7, url=self.item['pr_url'], state='MERGED', headRefOid=tip,
                headRefName='dev/self-development', baseRefName='master', isCrossRepository=False,
                mergeCommit={'oid': self.target})
            if args[:3] == ('gh', 'pr', 'list'):
                # Historical PRs on this same branch may also report its new tip.
                history = [dict(pr, number=6, url='https://github.com/owner/repo/pull/6'),
                           dict(pr, number=5, url='https://github.com/owner/repo/pull/5', headRefOid=self.old)] if tip == self.target else []
                return json.dumps([*history, pr])
            if args[:3] == ('gh', 'pr', 'view'):
                return json.dumps(pr)
            if args[:2] == ('gh', 'api'):
                self.fail('Recovery must not issue another GitHub merge')
            output = original_run(*args)
            if args[:3] == ('python3', '-c', publication.CAS_PUSH_SCRIPT):
                pushes.append(args)
                if blocked:
                    raise publication.PublicationError('Interrupted after remote fast-forward')
                raise CrashAfterPush()
            return output
        def persist():
            saved.append(copy.deepcopy(state))
        with patch.object(self.repo, 'snapshot', side_effect=snapshot), patch.object(self.repo, 'git', side_effect=git), \
             patch.object(self.repo, 'run', side_effect=run):
            if blocked:
                publication.execute(self.repo, self.item, {}, command, persist)
            else:
                with self.assertRaises(CrashAfterPush):
                    publication.execute(self.repo, self.item, {}, command, persist)
            self.assertEqual(self.git('rev-parse', 'HEAD'), self.target)
            self.assertEqual(self.remote_ref('dev/self-development'), self.target)
            state = json.loads(json.dumps(saved[-1]))
            self.item = state['issues']['1']
            self.assertEqual(self.item['post_merge_convergence']['status'], 'BLOCKED' if blocked else 'PENDING')
            scheduler = Scheduler(state); scheduler.recover([])
            self.assertFalse(scheduler.release('1'))
            with self.assertRaises(QueueWait): scheduler.reserve('next')
            with patch.object(b, 'save_state'):
                b.restore_merged_state(self.item, state)
                b.project_merge({'number': 1}, self.item, state)
            self.assertFalse(state.get('outbox'))
            publication.execute(self.repo, self.item, {}, command, persist)
        result = self.item['post_merge_convergence']
        self.assertEqual(result['status'], 'DONE')
        self.assertEqual(saved[-1]['issues']['1']['post_merge_convergence']['status'], 'DONE')
        self.assertEqual(result['expected_head'], self.old)
        self.assertEqual(result['method'], 'merge')
        self.assertEqual(len(pushes), 1)
        self.assertTrue(b.merge_complete(self.item))
        for sha in (self.git('rev-parse', 'HEAD'), self.remote_ref('dev/self-development'), self.remote_ref('master')):
            self.assertEqual(sha, self.target)
        self.assertEqual(self.git('status', '--porcelain'), '')
        scheduler.recover([])
        self.assertNotIn('repo', scheduler.locks)
        scheduler.reserve('next')

        issue, comments = {'number': 1, 'state': 'OPEN', 'labels': ['dev:ready-for-review']}, []
        def close(*args):
            self.assertEqual(args[:3], ('issue', 'close', '1'))
            issue['state'] = 'CLOSED'
        with patch.object(b, 'save_state'), patch.object(b, 'WORK_CONTEXT', None), \
             patch.object(b, 'fetch_issue', side_effect=lambda number: copy.deepcopy(issue)), \
             patch.object(b, 'issue_comments', return_value=comments), \
             patch.object(b, 'raw_add_comment', side_effect=lambda number, body: comments.append({'body': body})), \
             patch.object(b, 'raw_transition_issue', side_effect=lambda current, target: issue.update(labels=[target])), \
             patch.object(b, 'gh', side_effect=close) as github:
            b.project_merge(issue, self.item, state)
            b.flush_effects(state)
            b.flush_effects(state)
            github.assert_called_once()
        self.assertEqual(issue['state'], 'CLOSED')
        self.assertEqual(issue['labels'], ['dev:done'])
        self.assertEqual(len(comments), 1)

    def test_execute_retry_after_remote_fast_forward_crash_proves_convergence_and_closes(self):
        self.check_execute_retry_after_remote_fast_forward()

    def test_execute_retry_after_remote_fast_forward_error_clears_blocker_and_closes(self):
        self.check_execute_retry_after_remote_fast_forward(blocked=True)

    def test_remote_moves_between_preflight_and_push_even_to_target_ancestor(self):
        original = self.repo.run
        def run(*args):
            if args[0] == 'python3':
                self.shell('git', '--git-dir=' + str(self.remote), 'update-ref',
                         'refs/heads/dev/self-development', self.base_before, self.old)
            return original(*args)
        with patch.object(self.repo, 'run', side_effect=run):
            self.assertEqual(self.converge(), 'BLOCKED')
        self.assertEqual(self.remote_ref('dev/self-development'), self.base_before)
        self.assertIn('PR merged', self.item['post_merge_convergence']['diagnostic'])

    def test_non_fast_forward_fails_before_mutation(self):
        self.shell('git', '--git-dir=' + str(self.remote), 'update-ref', 'refs/heads/master', self.baseline)
        self.item['post_merge_convergence']['master_sha'] = self.baseline
        self.assertEqual(self.converge(), 'BLOCKED')
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.old)
        self.assertEqual(self.remote_ref('dev/self-development'), self.old)

    def test_local_expected_head_race_fails_closed(self):
        original = self.repo.git
        def git(*args):
            if args[0] == 'update-ref':
                self.git('update-ref', 'refs/heads/dev/self-development', self.baseline, self.old)
            return original(*args)
        with patch.object(self.repo, 'git', side_effect=git):
            self.assertEqual(self.converge(), 'BLOCKED')
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.baseline)
        self.assertEqual(self.remote_ref('dev/self-development'), self.old)

    def test_read_tree_failure_after_local_ref_update_retains_repository_on_restart(self):
        from scheduler import Scheduler, QueueWait
        self.item['owner_state'] = 'MERGED'
        state = {'issues': {'1': self.item}}
        scheduler = Scheduler(state)
        scheduler.hold_publication_repository('1', 'work', 'repo')
        scheduler.enqueue('next', '2', 'coding', 'work', 'repo')
        original = self.repo.git
        def git(*args):
            if args[0] == 'read-tree':
                raise publication.PublicationError('test interruption after update-ref')
            return original(*args)
        with patch.object(self.repo, 'git', side_effect=git):
            self.assertEqual(self.converge(), 'BLOCKED')
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.target)
        self.assertTrue(self.git('status', '--porcelain'))
        self.assertEqual(self.remote_ref('dev/self-development'), self.old)
        state = json.loads(json.dumps(state))
        scheduler = Scheduler(state)
        for _ in range(2):
            scheduler.recover([])
            self.assertFalse(scheduler.release('1'))
            with self.assertRaises(QueueWait):
                scheduler.reserve('next')
        self.assertFalse(b.merge_complete(state['issues']['1']))
        self.assertEqual(self.converge(), 'BLOCKED')  # Dirty recovery cannot repair silently.


class ControlWorkerTests(unittest.TestCase):
    setUp = workers.WorkerIntegrationTests.setUp
    wait = workers.WorkerIntegrationTests.wait

    def test_targeted_worker_read_skips_sweep_but_proves_real_terminal_absence(self):
        source = ('import json; print(json.dumps({"type":"thread.started","thread_id":"session"})); '
                  'print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"done"}}))')
        command = shlex.join([sys.executable, '-c', source])
        with redirect_stdout(io.StringIO()), patch.object(devctl, 'build_agent_command', return_value=command), \
                patch.object(devctl.execution_profiles, 'discover', return_value={'version': 'dummy-codex', 'help': '--model --config'}):
            devctl.run_project('test', 'dummy', request_id='a' * 64)
        meta = self.wait('a' * 24)
        # The real cleanup/recovery tests cover survivors; this proves that the
        # read-only fast path also accepts a genuinely absent terminal runtime.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not worker_runtime.publication_safe(self.data / meta['id'], meta):
            time.sleep(.01)
        self.assertTrue(worker_runtime.publication_safe(self.data / meta['id'], meta))
        stream = io.StringIO()
        with patch.object(devctl, 'reconcile_workers') as recovery, redirect_stdout(stream):
            devctl.list_tasks(as_json=True, cached=True, control=True, control_project='test')
        recovery.assert_not_called()
        self.assertEqual(json.loads(stream.getvalue())[0]['status'], 'DONE')
        meta['turns'] = meta.get('turns', 1) + 1
        self.assertFalse(worker_runtime.publication_safe(self.data / meta['id'], meta))

    def test_missing_lease_and_corrupt_metadata_remain_uncertain(self):
        self.data.mkdir(parents=True)
        write_json(self.data.parent / 'worker_leases.json', {'lease': {'task_id': 'missing', 'machine': 'work'}})
        bad = self.data / 'bad'; bad.mkdir(); write_json(bad / 'meta.json', {'id': 'wrong'})
        stream = io.StringIO()
        with redirect_stdout(stream):
            devctl.list_tasks(as_json=True, cached=True, control=True, control_project='test')
        rows = json.loads(stream.getvalue())
        self.assertEqual({row['status'] for row in rows}, {'UNKNOWN'})
        self.assertEqual(len(rows), 2)


class TimingTests(unittest.TestCase):
    def test_bounded_numeric_fixed_schema_and_read_only_status(self):
        state = {'issues': {'1': {'task_id': 'task'}}}
        for _ in range(100):
            with bridge_timing.measure(state, 'control_total'):
                pass
        bridge_timing.record(state, 'https://token:secret', 1)
        bridge_timing.record(state, 'heavy_total', float('nan'))
        self.assertEqual(len(state['timings']['samples']), bridge_timing.LIMIT)
        self.assertTrue(all(set(row) == {'stage', 'seconds'} and row['seconds'] >= 0
                            for row in state['timings']['samples']))
        before = copy.deepcopy(state)
        view = operational_state.status_view(state, 1)
        view['timings']['samples'].clear()
        self.assertEqual(state, before)
        self.assertNotIn('secret', json.dumps(state))
