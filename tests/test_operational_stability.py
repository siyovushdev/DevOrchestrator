"""Production regressions: historical tasks, projections, alerts and owned cleanup."""
import copy
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import bridge_effects
import devctl as d
from durable import read_json, write_json
from operational_state import lifecycle_state, next_action, status_view
from scheduler import Scheduler, Limits
import task_bridge as b
import worker_runtime as w
import test_bridge_integration as integration
import test_worker_integration as workers
from tests_helpers import coding_result, review_result


class OperationalBridgeTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    async def approved(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        await b.tick(self.h.state)
        self.h.finish(item['review_task_id'], review_result())
        await b.tick(self.h.state)
        return item

    async def coding_completion_with_older_waiter(self, result):
        self.h.issues = {5: self.h.issue(5)}
        self.h.comments = {5: []}
        await b.tick(self.h.state)
        task_id = self.h.state['issues']['5']['task_id']
        self.h.issues[3] = self.h.issue(3)
        self.h.comments[3] = []
        await b.tick(self.h.state)  # Older issue is queued behind the running owner.
        self.assertNotIn('task_id', self.h.state['issues']['3'])
        self.h.finish(task_id, result)
        self.h.tasks[task_id]['cleanup_state'] = 'CLEANED'
        self.h.restart()
        await b.tick(self.h.state)  # Recovery precedes issue 3, then issue 5.
        self.assertNotIn('task_id', self.h.state['issues']['3'])
        self.assertEqual([lock['owner'] for lock in self.h.state['repo_locks'].values()], ['5'])
        return self.h.state['issues']['5']

    async def test_coding_completion_retains_lock_for_review_before_older_issue(self):
        item = await self.coding_completion_with_older_waiter(coding_result())
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertEqual([role for role, _ in self.h.starts], ['coding', 'review'])
        self.assertEqual(self.h.continues, [])
        self.h.restart()
        await b.tick(self.h.state)
        self.assertNotIn('task_id', self.h.state['issues']['3'])

    async def test_independent_pass_keeps_lock_until_valid_chief_approval(self):
        item = await self.coding_completion_with_older_waiter(coding_result())
        self.h.finish(item['review_task_id'], review_result())
        await b.tick(self.h.state)
        for _ in range(3):
            self.assertEqual([lock['owner'] for lock in self.h.state['repo_locks'].values()], ['5'])
            self.assertEqual(item['chief_status'], 'AWAITING_CHIEF_REVIEW')
            self.assertNotIn('task_id', self.h.state['issues']['3'])
            self.assertNotIn('review_hold', item)
            self.h.restart()
            await b.tick(self.h.state)
            item = self.h.state['issues']['5']
        self.assertEqual(sum(c.args[0].startswith('CHIEF_REVIEW_REQUIRED') for c in self.h.telegram.call_args_list), 1)
        self.h.comments[5].append({'id': '123', 'author': {'login': 'siyovushdev'},
            'createdAt': '2050-01-01T00:00:00Z',
            'body': '<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 ' + json.dumps(dict(
                bundle_id=item['chief_review']['identity']['bundle_id'], verdict='PASS',
                summary='Chief reviewed', findings=[])) + ' -->'})
        await b.tick(self.h.state)
        self.assertTrue(b.notifications.review_approval_complete(item))
        self.assertEqual(self.h.state['repo_locks'], {})
        await b.tick(self.h.state)
        self.assertTrue(self.h.state['issues']['3']['task_id'])

    async def test_restart_after_saved_pass_before_chief_backfill_keeps_repository(self):
        item = await self.coding_completion_with_older_waiter(coding_result())
        self.h.finish(item['review_task_id'], review_result())
        # Model process loss after PASS persistence, before Chief backfill or
        # the finally-block release. The next tick uses the real release path.
        with patch.object(b, 'process_chief_review', return_value=False), \
             patch.object(Scheduler, 'release', return_value=False):
            await b.tick(self.h.state)
        self.assertEqual(item['review_status'], 'PASS')
        self.assertNotIn('chief_review', item)
        self.h.restart()
        for _ in range(2):
            await b.tick(self.h.state)
            item = self.h.state['issues']['5']
            self.assertNotIn('task_id', self.h.state['issues']['3'])
            self.assertEqual(item['chief_status'], 'AWAITING_CHIEF_REVIEW')
            self.assertEqual(item['review_status'], 'PASS')
            self.assertNotIn('review_hold', item)
            self.assertEqual([lock['owner'] for lock in self.h.state['repo_locks'].values()], ['5'])
        self.assertTrue(item['chief_review'])
        self.assertEqual(sum(c.args[0].startswith('CHIEF_REVIEW_REQUIRED') for c in self.h.telegram.call_args_list), 1)

    async def test_operator_status_clears_recovered_ownership_diagnostic(self):
        await b.tick(self.h.state)
        s = Scheduler(self.h.state)
        jobs = copy.deepcopy(s.jobs)
        s.jobs.clear()
        s.recover([])
        self.assertTrue(status_view(self.h.state, 1)['scheduler_diagnostics'])
        s.jobs.update(jobs)
        s.recover(list(self.h.tasks.values()))
        b.save_state(self.h.state)
        stream = io.StringIO()
        with redirect_stdout(stream): d.status('1', as_json=True, issue=True)
        view = json.loads(stream.getvalue())
        self.assertEqual(view['scheduler_diagnostics'], {})
        self.assertEqual(view['lifecycle'], 'RUNNING')
        self.assertTrue(view['locks'])

    async def test_coding_completion_retains_lock_for_correction_before_older_issue(self):
        item = await self.coding_completion_with_older_waiter(coding_result('CONTINUE'))
        self.assertEqual(self.h.tasks[item['task_id']]['status'], 'RUNNING')
        self.assertEqual(item['coding_turn'], 2)
        self.assertEqual([role for role, _ in self.h.starts], ['coding'])
        self.assertEqual([task_id for task_id, _ in self.h.continues], [item['task_id']])
        self.h.restart()
        await b.tick(self.h.state)
        self.assertEqual(len(self.h.continues), 1)
        self.assertEqual(self.h.state['issues']['5']['coding_status'], 'RUNNING')
        self.assertNotIn('task_id', self.h.state['issues']['3'])

    async def test_review_correction_and_second_review_keep_repository_ownership(self):
        item = await self.coding_completion_with_older_waiter(coding_result())
        self.h.finish(item['review_task_id'], review_result('CHANGES_REQUIRED', findings=['Fix the defect']))
        self.h.restart()
        await b.tick(self.h.state)
        item = self.h.state['issues']['5']
        self.assertEqual([task_id for task_id, _ in self.h.continues], [item['task_id']])
        self.assertNotIn('task_id', self.h.state['issues']['3'])
        self.h.finish(item['task_id'], coding_result())
        self.h.restart()
        await b.tick(self.h.state)
        item = self.h.state['issues']['5']
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertEqual(item['review_round'], 2)
        self.assertEqual([role for role, _ in self.h.starts], ['coding', 'review', 'review'])
        self.assertNotIn('task_id', self.h.state['issues']['3'])
        self.assertEqual([lock['owner'] for lock in self.h.state['repo_locks'].values()], ['5'])

    async def test_archived_deferred_handoff_releases_terminal_repository_lock(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        s = Scheduler(self.h.state)
        repo = next(iter(s.locks))
        s.enqueue('deferred-review', '1', 'review', 'work', repo)
        item['pending_rpc'] = {'name': 'start_task', 'operation': 'review:1',
                               'arguments': {'project': 'test', 'role': 'review', 'prompt': 'Review'}}
        self.h.issues[1]['state'] = 'CLOSED'
        await b.tick(self.h.state)
        self.assertEqual(s.jobs['deferred-review']['status'], 'CANCELLED')
        self.h.issues[2] = self.h.issue(2)
        self.h.comments[2] = []
        self.h.restart()
        await b.tick(self.h.state)
        self.assertTrue(self.h.state['issues']['2']['task_id'])
        self.assertEqual(self.h.state['repo_locks'][repo]['owner'], '2')
        self.assertEqual([role for role, _ in self.h.starts], ['coding', 'coding'])
        self.h.telegram.assert_not_called()

    async def test_closed_old_pass_with_drift_never_backfills_chief_or_alerts(self):
        item = await self.approved()
        self.h.issues[1]['state'] = 'CLOSED'
        (self.h.repo / 'file').write_text('later unrelated work')
        before = copy.deepcopy(item)
        self.h.telegram.reset_mock()
        with patch.object(b, 'process_chief_review', side_effect=AssertionError('no Chief backfill')):
            for _ in range(3):
                await b.tick(self.h.state)
        self.h.telegram.assert_not_called()
        for key in ('review_status', 'owner_state', 'chief_status', 'reviewed_tree', 'coding_status'):
            self.assertEqual(item.get(key), before.get(key))
        self.assertNotIn('review_hold', item)
        self.assertEqual(item['runtime_exclusion'], 'ISSUE_CLOSED')

    async def test_closed_superseded_stays_inert_and_reopen_does_not_resume(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        item['superseded'] = True
        self.h.issues[1]['state'] = 'CLOSED'
        before = copy.deepcopy(item)
        starts = len(self.h.starts)
        comments = len(self.h.comments[1])
        for state in ('CLOSED', 'CLOSED', 'OPEN', 'OPEN'):
            self.h.issues[1]['state'] = state
            self.h.issues[1]['labels'] = [b.QUEUE_LABEL]
            await b.tick(self.h.state)
        self.assertEqual(len(self.h.starts), starts)
        self.assertEqual(len(self.h.comments[1]), comments)
        self.assertEqual(item['coding_status'], before['coding_status'])
        self.assertEqual(lifecycle_state(item), 'ARCHIVED')
        self.assertIn('reopen', item['consistency_diagnostics'])
        self.h.telegram.assert_not_called()

    async def test_running_projection_repairs_receipt_after_crash_once(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.issues[1]['labels'] = [b.QUEUE_LABEL]
        # Simulate STARTED durable before the first label/comment was enqueued.
        self.h.state['outbox'] = {}
        self.h.comments[1] = []
        for _ in range(3):
            await b.tick(self.h.state)
        self.assertEqual(b.label_names(self.h.issues[1]) & b.STATE_LABELS, {'dev:running'})
        self.assertEqual(sum('Task started:' in c['body'] for c in self.h.comments[1]), 1)
        self.assertEqual(item['projection']['health'], 'SYNCED')
        self.h.telegram.assert_not_called()

    async def test_start_projection_precedes_backlog_and_other_recovery(self):
        for n in range(20):
            bridge_effects.enqueue(self.h.state, 1, 'comment', f'old comment {n}')
        # Once the durable launch returns, its label/receipt must be visible
        # before this tick processes any subsequent tracked task.
        self.h.issues[2] = self.h.issue(2)
        self.h.comments[2] = []
        original = self.h.transport
        async def transport(name, args):
            if name == 'start_task' and self.h.starts:
                self.assertIn('dev:running', b.label_names(self.h.issues[1]))
                self.assertTrue(any('Task started:' in c['body'] for c in self.h.comments[1]))
            return await original(name, args)
        with patch.object(b, 'transport_call', side_effect=transport):
            await b.tick(self.h.state)

    async def test_superseded_notification_dropped_current_action_recomputed(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        item.update(coding_status='DONE', desired_label='dev:needs-owner', owner_blocker='Choose A or B')
        b.WORK_CONTEXT = (self.h.state, '1', item)
        b.notify('NEEDS_OWNER', self.h.issues[1], item, blocker='stale question')
        item.update(coding_status='RUNNING', desired_label='dev:running', coding_turn=2)
        b.WORK_CONTEXT = None
        b.flush_effects(self.h.state)
        self.h.telegram.assert_not_called()
        self.assertEqual(item['consistency_diagnostics']['notification'], 'Superseded notification dropped')
        item.update(coding_status='DONE', desired_label='dev:needs-owner', worker_uncertain=True)
        b.WORK_CONTEXT = (self.h.state, '1', item)
        b.notify('NEEDS_OWNER', self.h.issues[1], item, blocker='Inspect runtime')
        b.WORK_CONTEXT = None
        b.flush_effects(self.h.state)
        self.assertIn('Next owner action: INSPECT_WORKER', self.h.telegram.call_args.args[0])
        self.assertIn('Choose A or B', self.h.telegram.call_args.args[0])
        b.flush_effects(self.h.state)
        self.assertEqual(self.h.telegram.call_count, 1)

    async def test_close_at_delivery_suppresses_alert(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        item.update(coding_status='DONE', desired_label='dev:needs-owner')
        b.WORK_CONTEXT = (self.h.state, '1', item)
        b.notify('NEEDS_OWNER', self.h.issues[1], item, blocker='Choose')
        b.WORK_CONTEXT = None
        self.h.issues[1]['state'] = 'CLOSED'
        b.flush_effects(self.h.state)
        self.h.telegram.assert_not_called()
        self.assertTrue(item['issue_closed'])

    async def test_closed_pending_comment_and_chief_effects_are_suppressed(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        before = len(self.h.comments[1])
        bridge_effects.enqueue(self.h.state, 1, 'comment', 'stale lifecycle report')
        bridge_effects.enqueue(self.h.state, 1, 'chief_comment', 'stale Chief evidence')
        self.h.issues[1]['state'] = 'CLOSED'
        b.flush_effects(self.h.state)
        self.assertEqual(len(self.h.comments[1]), before)
        self.assertTrue(item['issue_closed'])

    async def test_close_reopen_without_supersession_remains_archived(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.issues[1]['state'] = 'CLOSED'
        await b.tick(self.h.state)
        self.h.issues[1]['state'] = 'OPEN'
        self.h.issues[1]['labels'] = [b.QUEUE_LABEL]
        self.h.comment('<!-- DEVORCHESTRATOR_FOLLOWUP_V1 -->\nResume stale work')
        for _ in range(2): await b.tick(self.h.state)
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(self.h.continues, [])
        self.assertEqual(next_action(item), 'NONE')

    async def test_closed_unlaunched_fifo_intent_does_not_block_new_task(self):
        self.h.state['issues']['1'] = {'queue_status': 'QUEUED'}
        from scheduler import project_identity
        machine, repo = project_identity('test')
        Scheduler(self.h.state).enqueue('archived', '1', 'coding', machine, repo)
        self.h.issues[1]['state'] = 'CLOSED'
        self.h.issues[2] = self.h.issue(2)
        self.h.comments[2] = []
        await b.tick(self.h.state)
        self.assertEqual(self.h.state['work_queue']['archived']['status'], 'CANCELLED')
        self.assertNotIn('task_id', self.h.state['issues']['1'])
        self.assertTrue(self.h.state['issues']['2']['task_id'])
        self.h.telegram.assert_not_called()

    async def test_projection_repairs_during_worker_discovery_outage(self):
        await b.tick(self.h.state)
        self.h.issues[1]['labels'] = [b.QUEUE_LABEL]
        with patch.object(b, 'transport_call', side_effect=OSError('worker unavailable')):
            with self.assertRaises(OSError): await b.tick(self.h.state)
        self.assertIn('dev:running', b.label_names(self.h.issues[1]))
        self.assertEqual(len(self.h.starts), 1)

    async def test_historical_start_comment_survives_missing_machine_metadata(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        item.pop('coding_machine')
        self.h.state['outbox'] = {}
        await b.tick(self.h.state)
        self.assertEqual(sum('Task started:' in c['body'] for c in self.h.comments[1]), 1)

    async def test_stale_projection_payload_does_not_regress_current_chief_state(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        item.update(coding_status='DONE', chief_status='AWAITING_CHIEF_REVIEW', desired_label='dev:awaiting-chief-review')
        self.h.issues[1]['labels'] = ['dev:awaiting-chief-review']
        bridge_effects.enqueue(self.h.state, 1, 'transition',
            {'issue': {'number': 1, 'labels': [b.QUEUE_LABEL]}, 'target': 'dev:running'}, 'stale-running')
        b.flush_effects(self.h.state)
        self.assertEqual(b.label_names(self.h.issues[1]) & b.STATE_LABELS, {'dev:awaiting-chief-review'})
        b.reconcile_projection(self.h.issues[1], item, self.h.state)
        b.flush_effects(self.h.state)
        self.assertEqual(item['projection']['health'], 'SYNCED')

    async def test_repeated_projection_failure_has_bounded_diagnostic(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.issues[1]['labels'] = [b.QUEUE_LABEL]
        with patch.object(b, 'raw_transition_issue', side_effect=OSError('secret must never be persisted')):
            for _ in range(5):
                b.reconcile_projection(self.h.issues[1], item, self.h.state)
                for e in self.h.state['outbox'].values():
                    e['next_attempt'] = 0
                b.flush_effects(self.h.state)
        self.assertEqual(item['projection'], {'health': 'ERROR', 'target': 'dev:running', 'failures': 3})
        self.assertNotIn('secret', json.dumps(item))
        self.assertIn('projection', item['consistency_diagnostics'])
        b.reconcile_projection(self.h.issues[1], item, self.h.state)
        for e in self.h.state['outbox'].values(): e['next_attempt'] = 0
        b.flush_effects(self.h.state)
        self.assertEqual(item['projection']['health'], 'SYNCED')

    async def test_comment_preflight_failures_surface_and_recover_without_duplicate(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        key = bridge_effects.enqueue(self.h.state, 1, 'comment', 'Required transition receipt')
        with patch.object(b, 'fetch_issue', side_effect=OSError('secret transport details')):
            for _ in range(3):
                self.h.state['outbox'][key]['next_attempt'] = 0
                b.flush_effects(self.h.state)
        self.assertEqual(item['projection']['health'], 'ERROR')
        self.assertNotIn('secret', json.dumps(self.h.state['outbox'][key]))
        self.h.state['outbox'][key]['next_attempt'] = 0
        b.flush_effects(self.h.state); b.flush_effects(self.h.state)
        self.assertEqual(item['projection']['health'], 'SYNCED')
        self.assertEqual(sum('Required transition receipt' in c['body'] for c in self.h.comments[1]), 1)

    async def test_owned_terminal_cleanup_uncertainty_blocks_publication(self):
        await integration.BridgeIntegrationTests.tick(self)
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        await integration.BridgeIntegrationTests.tick(self)
        self.h.finish(item['review_task_id'], review_result())
        await integration.BridgeIntegrationTests.tick(self)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.h.tasks[item['task_id']]['cleanup_state'] = 'AMBIGUOUS'
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        with patch.object(b, 'publish') as publish:
            await integration.BridgeIntegrationTests.tick(self)
        publish.assert_not_called()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')

    async def test_status_surface_has_current_action_runtime_and_projection(self):
        await b.tick(self.h.state)
        item = self.h.state['issues']['1']
        item['coding_session_id'] = 'session-1'
        view = status_view(self.h.state, 1, {'state': 'RUNNING_HEALTHY'})
        self.assertEqual((view['lifecycle'], view['runtime']['state'], view['projection']['health']),
                         ('RUNNING', 'RUNNING_HEALTHY', 'SYNCED'))
        self.assertEqual(view['next_action'], 'NONE')
        self.assertTrue(view['locks'])
        self.assertEqual(view['session'], 'session-1')
        item.update(coding_status='DONE', chief_status='AWAITING_CHIEF_REVIEW', desired_label='dev:awaiting-chief-review')
        self.assertEqual(status_view(self.h.state, 1)['next_action'], 'CHIEF_VERDICT')
        item.update(owner_state='COMMITTED')
        self.assertEqual(status_view(self.h.state, 1)['next_action'], 'PUSH')
        with tempfile.TemporaryDirectory() as root, patch.object(d, 'DATA_DIR', Path(root) / 'tasks'):
            write_json(Path(root) / 'task_bridge_state.json', self.h.state)
            before = {p.name: p.read_bytes() for p in Path(root).iterdir() if p.is_file()}
            stream = io.StringIO()
            with redirect_stdout(stream): d.status('1', as_json=True, issue=True)
            self.assertEqual(json.loads(stream.getvalue())['next_action'], 'PUSH')
            self.assertEqual(before, {p.name: p.read_bytes() for p in Path(root).iterdir() if p.is_file()})


class OwnedCleanupTests(unittest.TestCase):
    setUp = workers.WorkerIntegrationTests.setUp
    wait = workers.WorkerIntegrationTests.wait

    def launch_orphan(self, subgroup=False):
        source = ('import subprocess,sys,json; '
            'subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]' + (',process_group=0' if subgroup else '') + '); '
            'print(json.dumps({"type":"thread.started","thread_id":"owned-session"})); '
            'print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"done"}}))')
        command = shlex.join([sys.executable, '-c', source])
        with redirect_stdout(io.StringIO()), patch.object(d, 'build_agent_command', return_value=command), \
                patch.object(d.execution_profiles, 'discover', return_value={'version': 'dummy-codex', 'help': '--model --config'}):
            d.run_project('test', 'dummy', request_id='c' * 64)
        task_id = 'c' * 24
        meta = self.wait(task_id)
        deadline = time.monotonic() + 3
        while not (d.task_dir(task_id) / 'exit_code').exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue((d.task_dir(task_id) / 'exit_code').exists())
        receipt = read_json(d.task_dir(task_id) / 'process_group.json')
        # A test-only finalizer kills ONLY this newly created isolated group.
        def finish():
            for group in [None, *receipt.get('groups', [])]:
                try:
                    if w.group_ownership(receipt, group) == 'OWNED':
                        os.killpg((group or receipt)['pgid'], signal.SIGKILL)
                except ProcessLookupError: pass
            d.reap_processes()
        self.addCleanup(finish)
        return task_id, meta, receipt

    def test_terminal_owned_child_in_separate_group_is_cleaned(self):
        task_id, meta, receipt = self.launch_orphan(subgroup=True)
        self.assertTrue(receipt['groups'])
        self.assertTrue(w.session_members(receipt['sid']))
        d.reconcile_workers()
        self.assertEqual(d.load_meta(task_id)['status'], 'DONE')
        self.assertFalse(w.session_members(receipt['sid']))
        self.assertEqual(read_json(self.data.parent / 'worker_leases.json'), {})

    def test_terminal_owned_children_metadata_and_lease_converge_idempotently(self):
        task_id, meta, receipt = self.launch_orphan()
        directory = d.task_dir(task_id)
        self.assertTrue(w.group_members(receipt['pgid']))
        self.assertEqual(meta['status'], 'RUNNING')
        original_created = meta['created_at']
        d.reconcile_workers()
        terminal = d.load_meta(task_id)
        self.assertEqual(terminal['status'], 'DONE')
        self.assertEqual(terminal['cleanup_state'], 'CLEANED')
        self.assertEqual(terminal['cleanup_generation'], terminal['runtime_generation'])
        self.assertEqual(terminal['effective_exit_code'], 0)
        self.assertEqual(terminal['session_id'] if 'session_id' in terminal else d.task_output(terminal, directory)['session_id'], 'owned-session')
        self.assertEqual(terminal['created_at'], original_created)
        self.assertFalse(w.group_members(receipt['pgid']))
        self.assertEqual(read_json(self.data.parent / 'worker_leases.json'), {})
        before = (directory / 'meta.json').read_bytes()
        with patch.object(w.os, 'killpg') as kill:
            d.reconcile_workers()
        kill.assert_not_called()
        self.assertEqual((directory / 'meta.json').read_bytes(), before)

    def test_active_and_identity_or_turn_mismatch_are_never_signalled(self):
        task_id, meta, receipt = self.launch_orphan()
        directory = d.task_dir(task_id)
        with patch.object(w.os, 'killpg') as kill:
            self.assertEqual(w.cleanup_group(directory, meta, 'RUNNING'), 'ACTIVE')
            for field, value in (('wrapper_identity', dict(receipt['wrapper_identity'], start='1')),
                                 ('pgid', receipt['pgid'] + 1), ('sid', receipt['sid'] + 1),
                                 ('generation', 'another-turn'), ('turn', 100), ('task_id', 'other')):
                write_json(directory / 'process_group.json', dict(receipt, **{field: value}))
                if field == 'wrapper_identity':
                    # Identity disagreement is observable while the leader is
                    # present. After exit, an exact surviving child anchor still
                    # proves group ownership. Pin this case's leader premise.
                    actual_details = w.process_details
                    leader = dict(pid=receipt['wrapper_pid'], state='S',
                                  pgid=receipt['pgid'], sid=receipt['sid'],
                                  identity=receipt['wrapper_identity'])
                    def observed(pid):
                        return leader if pid == receipt['wrapper_pid'] else actual_details(pid)
                    with patch.object(w, 'process_details', side_effect=observed):
                        self.assertEqual(w.cleanup_group(directory, meta, 'DONE'), 'AMBIGUOUS')
                else:
                    self.assertEqual(w.cleanup_group(directory, meta, 'DONE'), 'AMBIGUOUS')
            kill.assert_not_called()
        d.reconcile_workers()
        self.assertTrue(read_json(self.data.parent / 'worker_leases.json'))
        self.assertIn('ambiguous', d.load_meta(task_id)['consistency_diagnostic'])

    def test_escalation_rechecks_ownership_and_is_bounded(self):
        task_id, meta, receipt = self.launch_orphan()
        with patch.object(w, 'group_ownership', side_effect=['OWNED', 'OWNED', 'OWNED', 'ABSENT']), patch.object(w.os, 'killpg') as kill:
            self.assertEqual(w.cleanup_group(d.task_dir(task_id), meta, 'DONE', grace_seconds=0), 'CLEANED')
            self.assertEqual([c.args for c in kill.call_args_list], [(receipt['pgid'], signal.SIGTERM), (receipt['pgid'], signal.SIGKILL)])
        with patch.object(w, 'group_ownership', side_effect=['OWNED', 'OWNED', 'AMBIGUOUS']), patch.object(w.os, 'killpg') as kill:
            self.assertEqual(w.cleanup_group(d.task_dir(task_id), meta, 'DONE', grace_seconds=0), 'AMBIGUOUS')
            kill.assert_called_once_with(receipt['pgid'], signal.SIGTERM)

    def test_operator_status_with_runtime_is_strictly_read_only(self):
        task_id, meta, _ = self.launch_orphan()
        directory = d.task_dir(task_id)
        write_json(self.data.parent / 'task_bridge_state.json', {'issues': {'1': dict(task_id=task_id,
            coding_status='DONE', desired_label='dev:needs-owner', project='test')}})
        before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
        with redirect_stdout(io.StringIO()), patch.object(w.os, 'killpg') as kill:
            d.status('1', issue=True, as_json=True)
        kill.assert_not_called()
        self.assertEqual(before, {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()})

    def test_unrecorded_survivors_fail_closed(self):
        task_id, meta, receipt = self.launch_orphan()
        # The exit marker precedes supervisor exit. This case needs genuinely
        # unrecorded survivors, without a live wrapper acting as a valid anchor.
        wrapper = next((p for p in d._CHILDREN if p.pid == receipt['wrapper_pid']), None)
        if wrapper is not None:  # task_state may already have reaped it.
            wrapper.wait(timeout=3)
        d.reap_processes()
        self.assertIsNone(w.process_details(receipt['wrapper_pid']))
        receipt['members'] = []
        write_json(d.task_dir(task_id) / 'process_group.json', receipt)
        with patch.object(w.os, 'killpg') as kill:
            self.assertEqual(w.cleanup_group(d.task_dir(task_id), meta, 'DONE'), 'AMBIGUOUS')
        kill.assert_not_called()

    def test_terminal_failure_metadata_and_ambiguous_orphan_lease(self):
        directory = self.data / 'failed'; directory.mkdir(parents=True)
        meta = dict(id='failed', machine='work', agent='codex', workspace=str(self.repo),
                    status='RUNNING', pid=0, session_id='original', updated_at='original-time')
        write_json(directory / 'meta.json', meta)
        (directory / 'exit_code').write_text('7')
        write_json(self.data.parent / 'worker_leases.json', {'proven': {'task_id': 'failed', 'machine': 'work'},
            'ambiguous': {'task_id': 'missing', 'machine': 'work'}})
        d.reconcile_workers()
        result = d.load_meta('failed')
        self.assertEqual((result['status'], result['effective_exit_code'], result['session_id'], result['updated_at']),
                         ('FAILED', 7, 'original', 'original-time'))
        self.assertEqual(set(read_json(self.data.parent / 'worker_leases.json')), {'ambiguous'})
        self.assertIn('ambiguous', read_json(self.data.parent / 'worker_consistency.json')['leases'])

    def test_scheduler_releases_only_proven_terminal_locks(self):
        state = {'issues': {}}
        s = Scheduler(state, Limits())
        s.enqueue('known', '1', 'coding', 'work', 'repo'); s.reserve('known'); s.started('known', 'task')
        state['repo_locks']['orphan'] = {'owner': 'missing', 'key': 'missing'}
        s.recover([{'id': 'task', 'status': 'DONE'}])
        self.assertNotIn('repo', state['repo_locks'])
        self.assertIn('orphan', state['repo_locks'])
        self.assertIn('orphan', state['scheduler_diagnostics'])
        before = copy.deepcopy(state)
        s.recover([{'id': 'task', 'status': 'DONE'}])
        self.assertEqual(state, before)

    def test_terminal_ambiguous_group_retains_scheduler_capacity(self):
        state = {'issues': {}}
        s = Scheduler(state, Limits())
        s.enqueue('owned', '1', 'coding', 'work', 'repo'); s.reserve('owned'); s.started('owned', 'task')
        s.recover([{'id': 'task', 'status': 'DONE', 'cleanup_state': 'AMBIGUOUS'}])
        self.assertFalse(s.release('1'))
        self.assertIn('repo', state['repo_locks'])
        s.observe('task', 'DONE')  # An older report cannot erase cleanup uncertainty.
        self.assertFalse(s.release('1'))
        s.recover([{'id': 'task', 'status': 'DONE', 'cleanup_state': 'CLEANED'}])
        self.assertNotIn('repo', state['repo_locks'])

    def test_external_terminal_ambiguous_group_is_imported_as_occupied(self):
        state = {'issues': {}}
        s = Scheduler(state, Limits())
        with patch('scheduler.project_identity', return_value=('work', 'repo')):
            s.recover([{'id': 'external', 'project': 'test', 'status': 'DONE', 'cleanup_state': 'AMBIGUOUS'}])
        self.assertEqual(s.jobs['external:external']['status'], 'UNKNOWN')
        self.assertFalse(s.release('external:external'))


if __name__ == '__main__': unittest.main()
