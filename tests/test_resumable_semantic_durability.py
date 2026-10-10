"""F3 checkpoints through #47's real semantic writer; isolated local fixtures."""
import asyncio
import copy
import unittest
from unittest.mock import patch

import bridge_telemetry as telemetry
import bridge_timing
import chief_review
import durable
import task_bridge as bridge
from review_coordinator import DurabilityFailure, ReviewCoordinator
from review_migration import LegacyReviewRecovery
from scheduler import Scheduler
from transition_io import DRIVING
import test_review_coordinator as checkpoints
import test_resumable_control as control
from test_semantic_durability import health


class CheckpointDurabilityTests(unittest.TestCase):
    def setUp(self):
        checkpoints.ReviewCoordinatorTests.setUp(self)
        p = patch.object(bridge, 'STATE_FILE', self.path)
        p.start()
        self.addCleanup(p.stop)
        self.coordinator = ReviewCoordinator(self.state, bridge.save_state, 'org/repo')
        self.item = self.state['issues']['1']
        self.item['runtime_health'] = {'coding': health()}
        bridge.save_state(self.state)

    inspection = checkpoints.ReviewCoordinatorTests.inspection
    worker_report = checkpoints.ReviewCoordinatorTests.worker_report
    persist = checkpoints.ReviewCoordinatorTests.persist

    def test_all_f3_phases_roundtrip_and_heartbeat_never_changes_authority(self):
        for phase in ('PREPARE', 'LAUNCH', 'VERIFY', 'DRAIN'):
            with self.subTest(phase=phase):
                if phase == 'LAUNCH':
                    self.coordinator.authorize('1', self.candidate)
                elif phase == 'VERIFY':
                    self.coordinator.apply_inspection('1', self.inspection())
                elif phase == 'DRAIN':
                    self.coordinator.invalidate('1')
                expected = copy.deepcopy(telemetry.durable_state(self.state))
                contents, before = self.path.read_bytes(), self.path.stat()
                with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                    for n in range(12):
                        self.item['runtime_health']['coding'] = dict(health(1000 + n, n + 1), lease_state='EXPIRED')
                        bridge_timing.record(self.state, 'control_total', n)
                        self.assertFalse(bridge.save_state(self.state))
                self.assertFalse(any(c.args[0] == self.path for c in writes.call_args_list))
                self.assertEqual(self.path.read_bytes(), contents)
                self.assertEqual(self.path.stat().st_ino, before.st_ino)
                self.assertEqual(self.path.stat().st_mtime_ns, before.st_mtime_ns)
                telemetry.telemetry_path(self.path).write_text('{broken')
                self.state = bridge.load_state()
                self.assertEqual(self.state, expected)
                self.item = self.state['issues']['1']
                self.assertEqual(self.item['review_transition']['phase'], phase)
                self.coordinator = ReviewCoordinator(self.state, bridge.save_state, 'org/repo')
                self.coordinator.record('1')
                self.assertFalse(chief_review.approval_complete(self.item))
                self.assertFalse(Scheduler(self.state).release('1'))

    def test_optional_telemetry_failure_does_not_replay_authorization_or_adoption(self):
        with patch.object(telemetry, 'write_json_if_changed', side_effect=OSError('sidecar unavailable')):
            command = self.coordinator.authorize('1', self.candidate)
            saved = bridge.load_state()
            self.assertEqual(saved['issues']['1']['pending_rpc']['arguments'], command['arguments'])
            self.assertTrue(saved['work_queue'][self.key]['launch_pending'])
            self.coordinator.apply_inspection('1', self.inspection())
            effects = [dict(kind='comment', payload='Review started')]
            self.assertTrue(self.coordinator.adopt('1', self.worker_report(), self.report,
                self.inputs, self.binding['snapshot'], effects))
        restored = bridge.load_state()
        self.assertNotIn('pending_rpc', restored['issues']['1'])
        self.assertEqual(restored['issues']['1']['review_task_id'], self.key[:24])
        self.assertEqual(len(restored['outbox']), 1)
        self.assertFalse(bridge.notifications.review_approval_complete(restored['issues']['1']))
        self.assertIsNone(restored['issues']['1']['owner_state'])

    def test_failed_authoritative_save_stops_writer_before_telemetry_or_command(self):
        before = self.path.read_bytes()
        token = DRIVING.set(True)
        try:
            with patch.object(bridge, 'write_json_if_changed', side_effect=OSError('disk failure')), \
                    patch.object(telemetry, 'save') as sidecar:
                with self.assertRaises(DurabilityFailure):
                    self.coordinator.authorize('1', self.candidate)
                sidecar.assert_not_called()
        finally:
            DRIVING.reset(token)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertNotIn('pending_rpc', bridge.load_state()['issues']['1'])

    def test_legacy_review_migrates_once_keeps_envelope_and_never_launches(self):
        self.coordinator.authorize('1', self.candidate)
        self.item.pop('review_transition')
        self.item['pending_rpc']['arguments'].pop('review_binding')
        original = copy.deepcopy(self.item['pending_rpc'])
        bridge_timing.record(self.state, 'heavy_total', .01)
        durable.write_json(self.path, self.state)  # Old file with volatile fields.
        state = bridge.load_state()
        with patch('transition_io.Dispatcher.submit') as submit:
            recovery = LegacyReviewRecovery(state, bridge.save_state, 'org/repo', None)
            recovery.enroll()
            submit.assert_not_called()
        contents = self.path.read_bytes()
        for _ in range(3):
            telemetry.telemetry_path(self.path).write_text('{broken')
            state = bridge.load_state()
            recovery = LegacyReviewRecovery(state, bridge.save_state, 'org/repo', None)
            recovery.enroll()
            self.assertFalse(bridge.save_state(state))
            self.assertEqual(state['issues']['1']['pending_rpc'], original)
            self.assertEqual(state['issues']['1']['review_migration']['phase'], 'INSPECT')
            self.assertFalse(Scheduler(state).release('1'))
            self.assertFalse(chief_review.approval_complete(state['issues']['1']))
            self.assertEqual(self.path.read_bytes(), contents)


class ResumableHeartbeatTests(unittest.IsolatedAsyncioTestCase):
    setUp = control.ResumableControlTests.setUp
    pump = control.ResumableControlTests.pump
    coding = control.ResumableControlTests.coding

    async def test_accepted_review_response_lost_heartbeat_polling_and_restart_no_relaunch(self):
        loop = bridge.ControlLoop(self.h.state)
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.h.transport

        async def transport(name, args):
            result = await original(name, args)
            if name == 'start_task' and args.get('role') == 'review':
                entered.set()
                await release.wait()  # Accepted, response remains detached.
            return result

        try:
            with patch.object(bridge, 'transport_call', transport):
                item = await self.coding(loop)
                await self.pump(loop, entered.is_set)
                pending = copy.deepcopy(item['pending_rpc'])
                # Discovery may now observe the accepted reviewer before the
                # held launch response arrives. Settle that semantic queue
                # receipt before counting heartbeat-only writes.
                for _ in range(50):
                    loop.next_heavy = 0
                    loop.next_control.clear()
                    await loop.step()
                    await asyncio.sleep(.002)
                self.assertEqual(item['review_transition']['phase'], 'LAUNCH')
                self.assertEqual(item['pending_rpc'], pending)
                item['runtime_health'] = {'coding': health()}
                bridge.save_state(self.h.state)
                before, contents = bridge.STATE_FILE.stat(), bridge.STATE_FILE.read_bytes()
                with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                    for tick in range(30):
                        sample = health(1000 + tick, tick + 1)
                        item['runtime_health']['coding'] = sample
                        self.h.tasks[item['task_id']]['liveness'] = sample
                        bridge_timing.record(self.h.state, 'control_total', tick / 100)
                        bridge.save_state(self.h.state)
                        loop.next_control.clear()
                        await loop.step()
                        await asyncio.sleep(.002)
                self.assertFalse(any(c.args[0] == bridge.STATE_FILE for c in writes.call_args_list))
                self.assertEqual(item['pending_rpc'], pending)
                self.assertFalse(Scheduler(self.h.state).release('1'))
                self.assertFalse(chief_review.approval_complete(item))
                self.assertEqual(bridge.STATE_FILE.read_bytes(), contents)
                self.assertEqual(bridge.STATE_FILE.stat().st_ino, before.st_ino)
                release.set()
                await loop.close()
            telemetry.telemetry_path(bridge.STATE_FILE).write_text('{broken')
            self.h.restart()
            item = self.h.state['issues']['1']
            loop = bridge.ControlLoop(self.h.state)
            await self.pump(loop, lambda: bool(item.get('review_task_id')))
            self.assertEqual(item['review_task_id'], pending['arguments']['request_id'][:24])
            self.assertEqual(len(self.h.starts), 2)
            self.assertFalse(self.h.continues)
            self.assertNotIn('pending_rpc', item)
            self.assertFalse(bridge.notifications.review_approval_complete(item))
            self.assertIsNone(item['owner_state'])
        finally:
            release.set()
            await loop.close()
