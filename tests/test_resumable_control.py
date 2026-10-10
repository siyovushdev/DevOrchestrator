"""Production driver with isolated providers and deterministic suspended RPCs."""
import asyncio
import copy
import unittest
from unittest.mock import patch

import task_bridge as bridge
import bridge_telemetry
from control_loop import ControlLoop
from transition_executor import execute
import test_bridge_integration as integration
from tests_helpers import coding_result, review_result
import durable


class ResumableControlTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    async def pump(self, loop, predicate, limit=600):
        for _ in range(limit):
            loop.next_heavy = 0
            loop.next_control.clear()
            await loop.step()
            await asyncio.sleep(.002)
            if predicate():
                return
        self.fail('Resumable loop did not converge: ' + str(self.h.state))

    async def coding(self, loop):
        await self.pump(loop, lambda: bool(self.h.state['issues'].get('1', {}).get('task_id')))
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        return item

    async def owner_ready(self, loop):
        item = await self.coding(loop)
        await self.pump(loop, lambda: bool(item.get('review_task_id')))
        self.h.finish(item['review_task_id'], review_result())
        await self.pump(loop, lambda: bool((item.get('chief_review') or {}).get('complete')))
        import json
        self.h.comment('<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 ' + json.dumps(dict(
            bundle_id=item['chief_review']['identity']['bundle_id'], verdict='PASS', summary='Verified', findings=[])) + ' -->')
        await self.pump(loop, lambda: item.get('owner_state') == 'READY_FOR_OWNER')
        return item

    async def test_full_review_chief_owner_commit_uses_one_writer(self):
        loop = ControlLoop(self.h.state)
        try:
            item = await self.coding(loop)
            self.assertIn('repository_binding', item['task_spec']['arguments'])
            await self.pump(loop, lambda: bool(item.get('review_task_id')))
            self.assertFalse(item.get('pending_rpc'))
            self.h.finish(item['review_task_id'], review_result())
            await self.pump(loop, lambda: bool((item.get('chief_review') or {}).get('complete')))
            identity = item['chief_review']['identity']
            import json
            self.h.comment('<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 ' + json.dumps(dict(
                bundle_id=identity['bundle_id'], verdict='PASS', summary='Verified', findings=[])) + ' -->')
            await self.pump(loop, lambda: item.get('owner_state') == 'READY_FOR_OWNER')
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, lambda: item.get('owner_state') == 'COMMITTED')
            self.assertEqual(self.h.git('rev-parse', 'HEAD'), item['commit_sha'])
            self.assertEqual(len(self.h.starts), 2)
            self.assertFalse(self.h.continues)
        finally:
            await loop.close()

    async def test_slow_reviewer_rpc_does_not_block_owner_polling_or_duplicate_launch(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        polls = []
        original = self.h.transport
        async def transport(name, arguments):
            if name == 'start_task' and arguments.get('role') == 'review':
                entered.set()
                await release.wait()
            return await original(name, arguments)
        def comments(number):
            polls.append(number)
            return copy.deepcopy(self.h.comments[int(number)])
        try:
            with patch.object(bridge, 'transport_call', transport), patch.object(bridge, 'issue_comments', comments):
                item = await self.coding(loop)
                await self.pump(loop, entered.is_set)
                saved = copy.deepcopy(item['pending_rpc'])
                before = len(polls)
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
                await self.pump(loop, lambda: len(item.get('processed_owner_actions', [])) == 1)
                self.assertGreater(len(polls), before)
                self.assertEqual(item['pending_rpc'], saved)
                self.assertFalse(item.get('commit_sha'))
                self.assertEqual(len(self.h.starts), 1)
                release.set()
                await self.pump(loop, lambda: bool(item.get('review_task_id')))
                self.assertEqual(len(self.h.starts), 2)
        finally:
            release.set()
            await loop.close()

    async def test_unchanged_running_polls_and_timings_never_rewrite_authority(self):
        loop = ControlLoop(self.h.state)
        try:
            await self.pump(loop, lambda: bool(self.h.state['issues'].get('1', {}).get('task_id')))
            item = self.h.state['issues']['1']
            task = self.h.tasks[item['task_id']]
            task['liveness'] = dict(state='RUNNING_HEALTHY', reason='worker_and_agent_alive',
                generation=task['runtime_generation'], transition_sequence=1, observed_at=0,
                heartbeat_sequence=1, heartbeat_age_seconds=0, last_heartbeat_at=0, lease_expires_at=30)
            # Settle initial receipts, projection and additive semantic fields.
            for _ in range(160):
                loop.next_control.clear()
                await loop.step()
                await asyncio.sleep(.002)
            before = bridge.STATE_FILE.stat()
            contents = bridge.STATE_FILE.read_bytes()
            with patch.object(durable, 'write_json', wraps=durable.write_json) as write:
                for count in range(120):
                    task['liveness'].update(observed_at=count, heartbeat_sequence=count + 1,
                        heartbeat_age_seconds=count % 2, last_heartbeat_at=count, lease_expires_at=count + 30)
                    loop.next_control.clear()
                    await loop.step()
                    await asyncio.sleep(.002)
                self.assertFalse(any(c.args[0] == bridge.STATE_FILE for c in write.call_args_list))
                self.assertTrue(any(c.args[0] == bridge_telemetry.telemetry_path(bridge.STATE_FILE)
                                    for c in write.call_args_list))
            self.assertEqual(bridge.STATE_FILE.read_bytes(), contents)
            self.assertEqual(bridge.STATE_FILE.stat().st_ino, before.st_ino)
            self.assertEqual(bridge.STATE_FILE.stat().st_mtime_ns, before.st_mtime_ns)
        finally:
            await loop.close()

    async def test_only_actionable_followup_invalidates_pending_review(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.h.transport
        async def transport(name, arguments):
            if name == 'start_task' and arguments.get('role') == 'review':
                entered.set()
                await release.wait()
            return await original(name, arguments)
        try:
            with patch.object(bridge, 'transport_call', transport):
                item = await self.coding(loop)
                await self.pump(loop, entered.is_set)
                saved = copy.deepcopy(item['pending_rpc'])
                self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Old answer"} -->')
                self.h.comment(bridge.FOLLOWUP_MARKER)
                observation = dict(issue=self.h.issues[1], comments=self.h.comments[1])
                loop._observe_control('1', copy.deepcopy(observation))
                self.assertFalse(item['review_transition']['invalidated'])
                self.assertNotIn('pending_followup', item)
                self.h.comment(bridge.FOLLOWUP_MARKER + '\nPlease fix the edge case.')
                observation['comments'] = self.h.comments[1]
                loop._observe_control('1', copy.deepcopy(observation))
                self.assertTrue(item['review_transition']['invalidated'])
                self.assertEqual(item['pending_rpc'], saved)
                self.assertTrue(item['pending_followup'])
        finally:
            release.set()
            await loop.close()

    async def test_control_reducer_save_failure_stops_before_further_dispatch(self):
        from review_coordinator import DurabilityFailure
        from transition_io import Command, DRIVING
        self.h.state['issues']['1'] = {'queue_status': 'QUEUED'}
        loop = ControlLoop(self.h.state)
        try:
            issue = dict(self.h.issues[1], state='CLOSED')
            result = ('control:1', Command.create('control_read', {'number': '1'}, lane='control'),
                      dict(issue=issue, comments=[]), None)
            with (patch.object(loop.dispatcher, 'collect', return_value=[result]),
                  patch.object(loop.dispatcher, 'submit') as submit,
                  patch.object(durable, 'write_json', side_effect=OSError('disk unavailable'))):
                with self.assertRaises(DurabilityFailure):
                    await loop.step()
                submit.assert_not_called()
            self.assertFalse(DRIVING.get())
            self.assertIsNone(bridge.WORK_CONTEXT)
        finally:
            await loop.close()

    async def test_slow_publication_keeps_polling_and_restart_reconciles_existing_commit(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        polls = []
        async def detached(command):
            if command.name == 'control_read':
                polls.append(command.arguments()['number'])
            result = await execute(command)
            if (command.name == 'repository' and command.arguments()['method'] == 'git' and
                    command.arguments().get('args', [None])[0] == 'update-ref'):
                entered.set()
                await release.wait()  # Git mutation happened; writer has not received its result.
            return result
        try:
            item = await self.owner_ready(loop)
            loop.dispatcher.execute = detached
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
            await self.pump(loop, entered.is_set)
            checkpoint = bridge.load_state()
            candidate = checkpoint['issues']['1']['publication_journal']['COMMIT']['candidate']
            self.assertEqual(self.h.git('rev-parse', 'HEAD'), candidate)
            self.assertNotEqual(item.get('owner_state'), 'COMMITTED')
            before = len(polls)
            await self.pump(loop, lambda: len(polls) >= before + 5)
            self.assertNotEqual(item.get('owner_state'), 'COMMITTED')
            release.set()
            await loop.close()  # Discard the response, as after a bridge interruption.
            self.h.restart()
            item = self.h.state['issues']['1']
            loop = ControlLoop(self.h.state)
            await self.pump(loop, lambda: item.get('owner_state') == 'COMMITTED')
            self.assertEqual(item['commit_sha'], candidate)
            self.assertEqual(self.h.git('rev-list', '--count', 'HEAD'), '2')
            self.assertEqual(len(self.h.starts), 2)
        finally:
            release.set()
            await loop.close()

    async def test_close_observed_during_preflight_prevents_new_launch(self):
        loop = ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        async def detached(command):
            if command.name == 'rpc' and command.arguments()['name'] == 'preflight_task':
                entered.set()
                await release.wait()
            return await execute(command)
        loop.dispatcher.execute = detached
        try:
            await self.pump(loop, entered.is_set)
            self.h.issues[1]['state'] = 'CLOSED'
            await self.pump(loop, lambda: loop.issues.get('1', {}).get('state') == 'CLOSED')
            release.set()
            await self.pump(loop, lambda: bool(self.h.state['issues']['1'].get('issue_closed')))
            self.assertFalse(self.h.starts)
            self.assertFalse(self.h.state['issues']['1'].get('pending_rpc'))
        finally:
            release.set()
            await loop.close()


if __name__ == '__main__':
    unittest.main()
