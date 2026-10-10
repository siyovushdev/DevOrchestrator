"""Chief no-send proof, real flock contention, crash receipts and semantic saves."""
import asyncio
import copy
import time
import unittest
from unittest.mock import Mock, patch

import bridge_effects as effects
import bridge_timing
import chief_review
import devctl
import durable
import task_bridge as bridge
from control_loop import ControlLoop
from review_coordinator import DurabilityFailure
from transition_io import DRIVING
from transition_executor import execute
import test_bridge_integration as integration
import test_semantic_durability as semantic
from test_semantic_durability import health
from tests_helpers import coding_result, review_result


class NoSendCheckpointTests(unittest.TestCase):
    setUp = semantic.SemanticDurabilityTests.setUp

    def flush(self, post, now, comments=()):
        effects.flush(self.state, lambda: bridge.save_state(self.state),
                      lambda number: comments, post, Mock(), Mock(), now=now)

    def test_explicit_no_send_has_bounded_backoff_and_survives_restart(self):
        key = effects.enqueue(self.state, 1, 'chief_comment', 'evidence')
        post = Mock(side_effect=effects.CommentNotDispatched())
        now = 0
        for attempt in range(1, 13):
            self.flush(post, now)
            self.state = bridge.load_state()
            entry = self.state['outbox'][key]
            self.assertEqual(entry['status'], 'PENDING')
            self.assertEqual(entry['attempts'], attempt)
            self.assertGreater(entry['next_attempt'], now)
            self.assertLessEqual(entry['next_attempt'] - now, 300)
            self.assertEqual(entry['payload'], 'evidence')
            before = self.path.read_bytes(), self.path.stat()
            with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                self.flush(post, entry['next_attempt'] - .1)
            self.assertFalse(any(call.args[0] == self.path for call in writes.call_args_list))
            self.assertEqual(self.path.read_bytes(), before[0])
            self.assertEqual(self.path.stat().st_mtime_ns, before[1].st_mtime_ns)
            now = entry['next_attempt']
        self.assertEqual(post.call_count, 12)
        post.side_effect = None
        self.flush(post, now)
        self.assertEqual(bridge.load_state()['outbox'][key]['status'], 'DONE')
        self.flush(post, now + 1000)
        self.assertEqual(post.call_count, 13)

    def test_crash_before_no_send_checkpoint_keeps_sending_and_never_blind_retries(self):
        key = effects.enqueue(self.state, 1, 'chief_comment', 'evidence')
        def persist():
            if self.state['outbox'][key]['status'] == 'PENDING':
                raise KeyboardInterrupt('crash before no-send checkpoint')
            bridge.save_state(self.state)
        post = Mock(side_effect=effects.CommentNotDispatched())
        with self.assertRaises(KeyboardInterrupt):
            effects.flush(self.state, persist, lambda number: [], post, Mock(), Mock(), now=0)
        self.state = bridge.load_state()
        self.assertEqual(self.state['outbox'][key]['status'], 'SENDING')
        post.reset_mock()
        self.flush(post, 10)
        self.assertEqual(bridge.load_state()['outbox'][key]['status'], 'UNCERTAIN')
        post.assert_not_called()
        receipt = dict(body='evidence\n\n<!-- DEVORCHESTRATOR_EFFECT_V1 ' + key + ' -->')
        self.flush(post, 20, [receipt])
        self.assertEqual(bridge.load_state()['outbox'][key]['status'], 'DONE')
        post.assert_not_called()

    def test_crash_after_post_reconciles_exact_receipt_without_duplicate(self):
        key = effects.enqueue(self.state, 1, 'chief_comment', 'evidence')
        receipts = []
        def post(number, body):
            receipts.append(dict(body=body))
            raise KeyboardInterrupt('accepted before crash')
        with self.assertRaises(KeyboardInterrupt):
            self.flush(post, 0, receipts)
        self.state = bridge.load_state()
        self.assertEqual(self.state['outbox'][key]['status'], 'SENDING')
        no_post = Mock()
        self.flush(no_post, 10, [])  # Absence is not proof of no-send.
        self.assertEqual(self.state['outbox'][key]['status'], 'UNCERTAIN')
        self.flush(no_post, 20, receipts + receipts)  # Duplicate receipts stay held.
        self.assertEqual(self.state['outbox'][key]['status'], 'UNCERTAIN')
        self.flush(no_post, 30, receipts)
        self.flush(no_post, 40, receipts)
        self.assertEqual(self.state['outbox'][key]['status'], 'DONE')
        self.assertEqual(len(receipts), 1)
        no_post.assert_not_called()

    def test_failed_no_send_checkpoint_is_fatal_and_retains_sending_receipt_boundary(self):
        key = effects.enqueue(self.state, 1, 'chief_comment', 'evidence')
        original = bridge.write_json_if_changed
        def checkpoint(path, state):
            if state['outbox'][key]['status'] == 'PENDING':
                raise OSError('checkpoint unavailable')
            return original(path, state)
        token = DRIVING.set(True)
        try:
            with patch.object(bridge, 'write_json_if_changed', side_effect=checkpoint):
                with self.assertRaises(DurabilityFailure):
                    self.flush(Mock(side_effect=effects.CommentNotDispatched()), 0)
        finally:
            DRIVING.reset(token)
        self.state = bridge.load_state()
        self.assertEqual(self.state['outbox'][key]['status'], 'SENDING')
        no_post = Mock()
        self.flush(no_post, 10)
        self.assertEqual(self.state['outbox'][key]['status'], 'UNCERTAIN')
        no_post.assert_not_called()


class ChiefDeliveryRetryTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    async def bundle(self):
        await bridge.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        await bridge.tick(self.h.state)
        self.h.finish(item['review_task_id'], review_result())
        with patch.object(bridge, 'flush_effects'):
            await bridge.tick(self.h.state)
        keys = item['chief_review']['effects']
        self.assertTrue(keys)
        self.assertTrue(all(self.h.state['outbox'][key]['status'] == 'PENDING' for key in keys))
        self.assertFalse(item['chief_review']['complete'])
        # Restrict this isolated outbox to the validated Chief bundle.
        self.h.state['outbox'] = {key: self.h.state['outbox'][key] for key in keys}
        bridge.save_state(self.h.state)
        return item, keys

    def flush(self):
        # Exercises the real resumable writer callback, repository snapshots,
        # locking and exact receipt logic, with isolated fake GitHub I/O.
        bridge.flush_effects(self.h.state, control_fresh={'1'})

    def gates_closed(self, item):
        self.assertFalse(chief_review.approval_complete(item))
        self.assertFalse(bridge.notifications.review_approval_complete(item))
        self.assertIsNone(item.get('owner_state'))

    async def test_real_flock_retry_restart_recovery_once_and_no_heartbeat_writes(self):
        item, keys = await self.bundle()
        self.h.state['large_payload'] = 'x' * 2_280_000
        item['runtime_health'] = {'coding': health()}
        clock = time.time()
        original = self.h.post_comment
        with patch.object(self.h, 'post_comment', wraps=original) as post:
            with durable.exclusive(devctl.DATA_DIR.parent / 'worker.lock'):
                with patch.object(effects.time, 'time', return_value=clock):
                    started = time.monotonic()
                    self.flush()
                    self.assertLess(time.monotonic() - started, 2)
                    self.assertTrue(all(self.h.state['outbox'][key]['status'] == 'PENDING' for key in keys))
                    post.assert_not_called()
                    deadlines = {key: self.h.state['outbox'][key]['next_attempt'] for key in keys}
                    self.h.restart()
                    item = self.h.state['issues']['1']
                    self.gates_closed(item)
                    item['runtime_health'] = {'coding': health()}
                    bridge.save_state(self.h.state)
                    before, contents = bridge.STATE_FILE.stat(), bridge.STATE_FILE.read_bytes()
                    with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                        for tick in range(40):
                            item['runtime_health']['coding'] = health(1000 + tick, tick + 1)
                            bridge_timing.record(self.h.state, 'control_total', tick / 10)
                            self.flush()
                        self.assertFalse(any(call.args[0] == bridge.STATE_FILE for call in writes.call_args_list))
                        self.assertTrue(any(call.args[0] == bridge.bridge_telemetry.telemetry_path(bridge.STATE_FILE)
                                            for call in writes.call_args_list))
                    self.assertEqual(bridge.STATE_FILE.read_bytes(), contents)
                    self.assertEqual(bridge.STATE_FILE.stat().st_mtime_ns, before.st_mtime_ns)
                    self.assertEqual(bridge.STATE_FILE.stat().st_ino, before.st_ino)
                    self.assertEqual({key: self.h.state['outbox'][key]['next_attempt'] for key in keys}, deadlines)
                    post.assert_not_called()
            with patch.object(effects.time, 'time', return_value=max(deadlines.values()) + .1):
                self.flush()
            self.assertEqual(post.call_count, len(keys))
            self.assertTrue(all(self.h.state['outbox'][key]['status'] == 'DONE' for key in keys))
            self.h.restart()
            for _ in range(5):
                self.flush()
            self.assertEqual(post.call_count, len(keys))
        item = self.h.state['issues']['1']
        await bridge.process_chief_review(self.h.issues[1], item, self.h.state, self.h.tasks[item['task_id']])
        self.assertTrue(item['chief_review']['complete'])
        self.gates_closed(item)  # Delivered evidence does not grant Chief/owner approval.
        for key in keys:
            self.assertEqual(sum(row['body'].endswith('DEVORCHESTRATOR_EFFECT_V1 ' + key + ' -->')
                                 for row in self.h.comments[1]), 1)

    async def ambiguous_post(self, accepted):
        item, keys = await self.bundle()
        original = self.h.post_comment
        def post(number, body):
            if accepted:
                original(number, body)
            raise BlockingIOError('transport error after potential dispatch')
        with patch.object(self.h, 'post_comment', side_effect=post) as calls:
            self.flush()
            self.assertTrue(all(self.h.state['outbox'][key]['status'] == 'UNCERTAIN' for key in keys))
            self.assertEqual(calls.call_count, len(keys))
            self.h.restart()
            for _ in range(5):
                self.flush()
            self.assertEqual(calls.call_count, len(keys))
            status = 'DONE' if accepted else 'UNCERTAIN'
            self.assertTrue(all(self.h.state['outbox'][key]['status'] == status for key in keys))
            self.gates_closed(self.h.state['issues']['1'])
        self.assertEqual(sum(chief_review.is_evidence_comment(row['body']) for row in self.h.comments[1]),
                         len(keys) if accepted else 0)

    async def test_blocking_io_error_inside_post_is_uncertain_and_never_retried(self):
        await self.ambiguous_post(False)

    async def test_accepted_blocking_io_error_reconciles_on_restart_without_duplicate(self):
        await self.ambiguous_post(True)

    async def test_control_loop_keeps_polling_while_real_worker_lock_is_busy(self):
        item, keys = await self.bundle()
        loop = ControlLoop(self.h.state)
        loop.issues['1'] = copy.deepcopy(self.h.issues[1])
        loop.after_work.add('1')
        loop.next_heavy = float('inf')
        polls = []
        async def detached(command):
            if command.name == 'control_read':
                polls.append(command.arguments()['number'])
            return await execute(command)
        loop.dispatcher.execute = detached
        try:
            with durable.exclusive(devctl.DATA_DIR.parent / 'worker.lock'):
                for _ in range(600):
                    loop.next_control.clear()
                    await loop.step()
                    await asyncio.sleep(.002)
                    if len(polls) >= 4 and any(self.h.state['outbox'][key]['attempts'] for key in keys):
                        break
                else:
                    self.fail('ControlLoop stopped polling during lock contention')
                self.assertGreaterEqual(len(polls), 4)
                self.assertTrue(all(self.h.state['outbox'][key]['status'] == 'PENDING' for key in keys))
                self.assertFalse(any(chief_review.is_evidence_comment(row['body']) for row in self.h.comments[1]))
                self.gates_closed(item)
        finally:
            await loop.close()
