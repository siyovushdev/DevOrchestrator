"""Isolated incident-shaped review admission; never contacts live providers."""
import asyncio
import copy
import io
import json
import time
from contextlib import redirect_stdout
import unittest
from unittest.mock import AsyncMock, patch

import task_bridge as b
import durable
import devctl
import execution_profiles
import subprocess
import test_bridge_integration as integration
from tests_helpers import coding_result


class ReviewLaunchRetryTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp
    tick = integration.BridgeIntegrationTests.tick

    async def coding_done(self):
        await self.tick()
        item = self.h.state['issues']['1']
        self.h.tasks[item['task_id']]['turns'] = 2
        self.h.finish(item['task_id'], coding_result())
        item.update(coding_turn=2, coding_status='DONE', review_round=1,
                    review_status=None, review_task_id=None, stale_review_task_ids=[])
        return item

    async def application(self, item):
        loop = b.ControlLoop(self.h.state)
        attempts = item.get('reconciliation_retry', {}).get('attempts', 0)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                loop.next_control.clear()
                await loop.step()
                await asyncio.sleep(.01)
                if item.get('review_task_id') or item.get('reconciliation_retry', {}).get('attempts', 0) > attempts:
                    return
            self.fail('Resumable reviewer did not settle before test deadline')
        finally:
            await loop.close()

    async def test_slow_successful_preflight_can_launch_round_two(self):
        item = await self.coding_done()
        async def slow(name, args):
            # Fresh coding checks + near-deadline Cursor discovery exceed
            # the old five-second aggregate budget despite each RPC succeeding.
            if name == 'get_task_report':
                await asyncio.sleep(1.3)
            elif name == 'preflight_task':
                await asyncio.sleep(2.6)
            return await self.h.transport(name, args)
        with patch.object(b, 'transport_call', side_effect=slow):
            await self.application(item)
        self.assertEqual(item.get('review_status'), 'RUNNING')
        self.assertEqual(item['review_round'], 2)
        self.assertEqual(len(self.h.starts), 2)
        self.assertFalse(item.get('pending_rpc'))
        self.assertFalse(item.get('reconciliation_retry'))

    async def test_cancellation_preserves_retry_history_and_queued_ownership(self):
        item = await self.coding_done()
        for attempt in range(1, 4):
            item.setdefault('reconciliation_retry', {})['next_attempt'] = 0
            async def cancelled(name, args):
                if name == 'preflight_task':
                    raise asyncio.CancelledError()
                return await self.h.transport(name, args)
            with patch.object(b, 'transport_call', side_effect=cancelled):
                with self.assertRaises(asyncio.CancelledError):
                    await b.process_tracked(self.h.issues[1], item, self.h.state)
            b.defer_issue(item, self.h.state, '1')
            self.assertEqual(item['reconciliation_retry']['attempts'], attempt)
            queued = [j for j in self.h.state['work_queue'].values() if j['status'] == 'QUEUED']
            self.assertEqual(len(queued), 1)
            self.assertIsNone(queued[0]['task_id'])
            self.assertFalse(item.get('pending_rpc'))
            self.assertEqual(len(self.h.starts), 1)
            self.assertTrue(self.h.state['repo_locks'])
            self.h.restart()
            item = self.h.state['issues']['1']

    async def test_backoff_skip_does_not_erase_failure(self):
        item = await self.coding_done()
        item['reconciliation_retry'] = {'attempts': 3, 'next_attempt': time.time() + 300}
        before = copy.deepcopy(item['reconciliation_retry'])
        await b.process_tracked(self.h.issues[1], item, self.h.state)
        self.assertEqual(item.get('reconciliation_retry'), before)

    async def test_report_stage_observations_never_rewrite_authoritative_state(self):
        # Isolate stage tracking from scheduler/telemetry changes covered by #47.
        # Use the real durable writer, including the save inside get_task_report.
        item = {'project': 'test', 'task_id': 'fixture'}
        state = {'issues': {'1': item}}
        b.save_state(state)
        before = copy.deepcopy(state)
        before_stat = b.STATE_FILE.stat()
        before_bytes = b.STATE_FILE.read_bytes()
        progress = {}
        token = b.RECONCILIATION_PROGRESS.set(progress)
        b.WORK_CONTEXT = (state, '1', item)
        try:
            with (patch.object(b, 'transport_call', new_callable=AsyncMock,
                               return_value={'status': 'RUNNING'}),
                  patch.object(b, 'Scheduler'),
                  patch.object(durable, 'write_json', wraps=durable.write_json) as write):
                for _ in range(100):
                    b.observe_reconciliation_stage('review_snapshot')
                    b.save_state(state)
                    await b.mcp_call('get_task_report', {'task_id': 'fixture'})
                    self.assertEqual(progress['stage'], 'get_task_report')
                    self.assertEqual(state, before)
                write.assert_not_called()
        finally:
            b.RECONCILIATION_PROGRESS.reset(token)
        self.assertEqual(b.STATE_FILE.read_bytes(), before_bytes)
        self.assertEqual(b.STATE_FILE.stat().st_ino, before_stat.st_ino)
        self.assertEqual(b.STATE_FILE.stat().st_mtime_ns, before_stat.st_mtime_ns)

    async def test_successful_attempt_does_not_create_diagnostics_or_cleanup_save(self):
        item = {'project': 'test', 'task_id': 'fixture'}
        state = {'issues': {'1': item}}
        b.save_state(state)
        before = copy.deepcopy(state)
        with (patch.object(b, 'transport_call', new_callable=AsyncMock,
                           return_value={'status': 'RUNNING'}),
              patch.object(b, 'Scheduler'), patch.object(b, 'reconcile_project', return_value=False),
              patch.object(b, 'save_state', wraps=b.save_state) as save,
              patch.object(durable, 'write_json', wraps=durable.write_json) as write):
            for _ in range(3):
                await b.process_tracked(self.h.issues[1], item, state)
                self.assertIsNone(b.RECONCILIATION_PROGRESS.get())
                self.assertEqual(state, before)
            self.assertEqual(save.call_count, 3)  # Only the report's save, no cleanup save.
            write.assert_not_called()

    async def test_transient_preflight_retries_same_queue_and_exposes_bounded_diagnostic(self):
        item = await self.coding_done()
        queue_key = None
        logs = io.StringIO()
        async def unavailable(name, args):
            if name == 'preflight_task':
                return {'status': 'UNAVAILABLE', 'launch_started': False}
            return await self.h.transport(name, args)
        with patch.object(b, 'transport_call', side_effect=unavailable), redirect_stdout(logs):
            for attempt in range(1, 4):
                item.setdefault('reconciliation_retry', {})['next_attempt'] = 0
                await b.process_tracked(self.h.issues[1], item, self.h.state)
                self.assertEqual(item['reconciliation_retry']['attempts'], attempt)
                self.assertIsNone(item.get('review_status'))
                self.assertFalse(item.get('pending_rpc'))
                queued = [(k, j) for k, j in self.h.state['work_queue'].items() if j['status'] == 'QUEUED']
                self.assertEqual(len(queued), 1)
                if queue_key is None:
                    queue_key = queued[0][0]
                self.assertEqual(queued[0][0], queue_key)
                self.h.restart()
                item = self.h.state['issues']['1']
        self.assertIn('attempt 3', logs.getvalue())
        diagnostic = item['consistency_diagnostics']['reconciliation']
        self.assertIn('preflight_task: RuntimeUnavailable', diagnostic)
        self.assertLessEqual(len(diagnostic), 512)
        self.assertTrue(self.h.state['repo_locks'])
        self.assertEqual(len(self.h.starts), 1)
        item['reconciliation_retry']['next_attempt'] = 0
        await self.application(item)
        self.assertEqual(item['review_task_id'], queue_key[:24])
        self.assertEqual(len(self.h.starts), 2)
        self.assertNotIn('reconciliation', item['consistency_diagnostics'])

    async def test_nonzero_cursor_models_exit_retries_real_preflight_with_same_request(self):
        item = await self.coding_done()
        requests = []
        offline = True
        def capture(machine, command, **kwargs):
            model = execution_profiles.resolve(devctl.load_projects()['test'], agent='cursor',
                                               role='review')['runtime_model']
            outputs = {'agent --version': 'fixture', 'agent --help': '--model <model>',
                       'agent models': model + ' - Fixture'}
            return subprocess.CompletedProcess([], int(offline and command == 'agent models'),
                                               stdout=outputs[command])
        async def transport(name, args):
            if name == 'preflight_task':
                requests.append(args['arguments']['request_id'])
                return devctl.preflight_task(args['operation'], args['arguments'])
            return await self.h.transport(name, args)
        with (patch.object(b, 'transport_call', side_effect=transport),
              patch.object(devctl, 'capture_execution', side_effect=capture)):
            await b.process_tracked(self.h.issues[1], item, self.h.state)
            self.assertIsNone(item.get('review_status'))
            self.assertFalse(item.get('pending_rpc'))
            self.assertEqual(len(self.h.starts), 1)
            key = requests[0]
            self.assertEqual(self.h.state['work_queue'][key]['status'], 'QUEUED')
            self.assertTrue(self.h.state['repo_locks'])
            self.h.restart()
            item = self.h.state['issues']['1']
            item['reconciliation_retry']['next_attempt'] = 0
            offline = False
            await b.process_tracked(self.h.issues[1], item, self.h.state)
        self.assertEqual(requests, [key, key])
        self.assertEqual(item['review_task_id'], key[:24])
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertEqual(len(self.h.starts), 2)
        self.assertIsNone(item.get('owner_state'))

    async def test_deadline_after_worker_acceptance_replays_receipt_without_duplicate(self):
        item = await self.coding_done()
        async def accepted(name, args):
            result = await self.h.transport(name, args)
            if name == 'start_task':
                raise asyncio.TimeoutError()  # Acceptance happened; response lost.
            return result
        with patch.object(b, 'transport_call', side_effect=accepted), \
                patch.object(b, 'ISSUE_RECONCILIATION_TIMEOUT', .8):
            await self.application(item)
        pending = copy.deepcopy(item['pending_rpc'])
        key = pending['arguments']['request_id']
        self.assertTrue(self.h.state['work_queue'][key]['launch_pending'])
        self.assertEqual(self.h.state['work_queue'][key]['status'], 'RESERVED')
        self.assertTrue(self.h.state['repo_locks'])
        self.assertEqual(len(self.h.starts), 2)
        self.assertIn('TimeoutError', item['reconciliation_retry']['diagnostic'])
        self.assertIn('start_task: TimeoutError', item['reconciliation_retry']['diagnostic'])
        self.assertNotIn('reconciliation_stage', item)
        self.assertIsNone(b.RECONCILIATION_PROGRESS.get())
        self.h.restart()
        item = self.h.state['issues']['1']
        item['reconciliation_retry']['next_attempt'] = 0
        await self.application(item)
        self.assertEqual(item['review_task_id'], key[:24])
        self.assertFalse(item.get('pending_rpc'))
        self.assertEqual(len(self.h.starts), 2)

    async def test_exception_payload_is_never_saved_or_logged(self):
        item = await self.coding_done()
        logs = io.StringIO()
        async def unavailable(name, args):
            if name == 'preflight_task':
                raise RuntimeError('private prompt ghp_' + 'A' * 40 + '\n' + 'sensitive' * 1000)
            return await self.h.transport(name, args)
        with patch.object(b, 'transport_call', side_effect=unavailable), redirect_stdout(logs):
            await b.process_tracked(self.h.issues[1], item, self.h.state)
        evidence = json.dumps(self.h.state) + logs.getvalue()
        for secret in ('private prompt', 'ghp_', 'sensitive'):
            self.assertNotIn(secret, evidence)
        self.assertIn('preflight_task: RuntimeError', evidence)


if __name__ == '__main__':
    unittest.main()
