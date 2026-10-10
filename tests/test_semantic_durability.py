"""Semantic persistence, optional bounded telemetry and real loop regressions."""
import asyncio
import copy
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import bridge_telemetry as telemetry
import bridge_timing
import devctl
import durable
from operational_state import status_view
from scheduler import Scheduler, QueueWait
import task_bridge as bridge
import test_bridge_integration as integration


def health(now=1000, sequence=1):
    return dict(task_id='task', generation='generation', state='RUNNING_HEALTHY',
        reason='lease_fresh_and_processes_owned', worker_alive=True, process_alive=True,
        worker_pid=123, agent_pid=456, phase='CODING', started_at=900,
        transition_sequence=1, last_transition_at=950, observed_at=now,
        last_heartbeat_at=now - 2, heartbeat_sequence=sequence,
        heartbeat_age_seconds=2, lease_expires_at=now + 28, lease_state='FRESH')


class SemanticDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'task_bridge_state.json'
        p = patch.object(bridge, 'STATE_FILE', self.path)
        p.start(); self.addCleanup(p.stop)
        self.item = dict(task_id='task', project='test', coding_status='RUNNING', coding_turn=1,
                         runtime_health={'coding': health(), 'review': health()})
        self.state = {'issues': {'1': self.item}, 'large_payload': 'x' * 2_280_000}

    def test_all_volatile_fields_and_timing_change_without_authoritative_write(self):
        self.assertTrue(bridge.save_state(self.state))
        before = self.path.stat()
        contents = self.path.read_bytes()
        with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
            for tick in range(40):
                for slot in ('coding', 'review'):
                    self.item['runtime_health'][slot] = health(1001 + tick, tick + 2)
                    self.item['runtime_health'][slot].update(heartbeat_age_seconds=tick, lease_state='EXPIRED')
                bridge_timing.record(self.state, 'control_total', tick / 10)
                self.assertFalse(bridge.save_state(self.state))
        self.assertFalse(any(call.args[0] == self.path for call in writes.call_args_list))
        self.assertEqual(self.path.read_bytes(), contents)
        self.assertEqual((self.path.stat().st_mtime_ns, self.path.stat().st_ino),
                         (before.st_mtime_ns, before.st_ino))
        stored = durable.read_json(self.path)
        self.assertNotIn('timings', stored)
        for slot in ('coding', 'review'):
            self.assertFalse(telemetry.VOLATILE_HEALTH & stored['issues']['1']['runtime_health'][slot].keys())
        self.assertTrue(self.state['timings']['samples'])  # Live observability retained.

    def test_semantic_transition_and_critical_receipts_persist_immediately(self):
        bridge.save_state(self.state)
        for key, value in [('worker_uncertain', True), ('pending_rpc', {'name': 'continue_task',
                'operation': 'followup:1', 'arguments': {'task_id': 'task'}}),
                ('review_status', 'RUNNING'), ('chief_review', {'identity': {'bundle_id': 'bundle'}}),
                ('publication_journal', {'COMMIT': {'status': 'PENDING'}}), ('owner_state', 'COMMITTED')]:
            self.item[key] = value
            self.assertTrue(bridge.save_state(self.state))
            self.assertEqual(durable.read_json(self.path)['issues']['1'][key], value)
        for key, value in [('state', 'SUSPECTED_STALL'), ('reason', 'lease_stale_but_process_owned'),
                           ('transition_sequence', 2), ('last_transition_at', 1100),
                           ('generation', 'new-generation'), ('worker_alive', None),
                           ('future_safety_field', 'held')]:
            self.item['runtime_health']['coding'][key] = value
            self.assertTrue(bridge.save_state(self.state))
            self.assertEqual(durable.read_json(self.path)['issues']['1']['runtime_health']['coding'][key], value)

    def test_restart_does_not_depend_on_telemetry_and_retains_reservations(self):
        scheduler = Scheduler(self.state)
        scheduler.enqueue('launch', '1', 'coding', 'work', 'repo')
        scheduler.reserve('launch')
        scheduler.jobs['launch'].update(task_id='task', launch_pending=True)
        self.item.update(worker_uncertain=True, pending_rpc={'name': 'start_task',
            'operation': 'coding:1', 'arguments': {'project': 'test'}},
            review_status='RUNNING', chief_required=True, chief_status='AWAITING_CHIEF_REVIEW',
            publication_journal={'COMMIT': {'status': 'PENDING'}})
        bridge.save_state(self.state)
        expected = telemetry.durable_state(self.state)
        telemetry.telemetry_path(self.path).write_text('{broken')
        restored = bridge.load_state()
        self.assertEqual(restored, expected)
        recovered = Scheduler(restored)
        recovered.recover([])
        self.assertFalse(recovered.release('1'))
        recovered.enqueue('next', '2', 'coding', 'work', 'repo')
        with self.assertRaises(QueueWait):
            recovered.reserve('next')
        telemetry.telemetry_path(self.path).unlink()
        self.assertEqual(bridge.load_state(), expected)

    def test_legacy_state_migrates_once_without_losing_workflow(self):
        bridge_timing.record(self.state, 'control_total', 1)
        durable.write_json(self.path, self.state)
        restored = bridge.load_state()
        self.assertEqual(restored, telemetry.durable_state(self.state))
        self.assertTrue(bridge.save_state(restored))
        self.assertFalse(bridge.save_state(restored))
        self.assertEqual(bridge.load_state(), restored)

    def test_status_reads_bounded_telemetry_and_derives_age_without_writes(self):
        bridge_timing.record(self.state, 'control_total', .25)
        bridge.save_state(self.state)
        stored = bridge.load_state()
        samples = telemetry.load(self.path)
        original = copy.deepcopy((stored, samples))
        with patch.object(telemetry.time, 'time', return_value=1010):
            view = status_view(stored, '1', telemetry=samples)
        self.assertEqual(view['runtime']['heartbeat_age_seconds'], 12)
        self.assertEqual(view['runtime']['observed_at'], 1000)
        self.assertEqual(view['review_runtime']['lease_state'], 'FRESH')
        self.assertEqual(view['timings']['samples'], [{'stage': 'control_total', 'seconds': .25}])
        self.assertEqual((stored, samples), original)
        text = json.dumps(samples)
        self.assertNotIn('heartbeat_age_seconds', text)
        self.assertNotIn('lease_state', text)
        before = self.path.stat()
        with patch.object(devctl, 'DATA_DIR', self.path.parent / 'tasks'), \
                patch.object(devctl, 'readonly_runtime', return_value=None), \
                patch.object(durable, 'write_json') as writes, redirect_stdout(io.StringIO()) as out:
            devctl.status('1', as_json=True, issue=True)
        writes.assert_not_called()
        output = json.loads(out.getvalue())
        self.assertEqual(output['timings'], view['timings'])
        self.assertEqual(output['runtime']['state'], 'RUNNING_HEALTHY')
        self.assertEqual(self.path.stat().st_mtime_ns, before.st_mtime_ns)

    def test_stale_or_malformed_telemetry_cannot_override_semantic_state(self):
        samples = telemetry.snapshot(self.state)
        for mutate in (lambda: self.item.update(task_id='new-task'),
                       lambda: self.item['runtime_health']['coding'].update(state='SUSPECTED_STALL')):
            mutate()
            view = status_view(telemetry.durable_state(self.state), '1', telemetry=samples)
            self.assertIsNone(view['runtime']['heartbeat_age_seconds'])
            self.assertEqual(view['runtime']['state'], self.item['runtime_health']['coding']['state'])
        for value in ('{broken', '[]', '{"schema_version":2}', 'x' * (telemetry.MAX_BYTES + 1)):
            telemetry.telemetry_path(self.path).write_text(value)
            self.assertEqual(telemetry.load(self.path), {})
        for values in ({'last_heartbeat_at': 'bad'}, {'last_heartbeat_at': float('inf')}, None):
            valid_state = {'issues': {'1': {'task_id': 'task', 'runtime_health': {'coding': health()}}}}
            samples = telemetry.snapshot(valid_state)
            samples['runtime'][0]['values'] = values
            view = status_view(telemetry.durable_state(valid_state), '1', telemetry=samples)
            self.assertIsNone(view['runtime']['heartbeat_age_seconds'])

    def test_telemetry_count_and_file_size_are_bounded(self):
        for n in range(500):
            self.state['issues'][str(n)] = dict(task_id='task', runtime_health={
                'coding': health(n), 'review': health(n)})
            bridge_timing.record(self.state, 'control_total', n)
        bridge.save_state(self.state)
        samples = telemetry.load(self.path)
        self.assertEqual(len(samples['runtime']), telemetry.RUNTIME_LIMIT)
        self.assertEqual(len(samples['timings']['samples']), bridge_timing.LIMIT)
        self.assertLess(telemetry.telemetry_path(self.path).stat().st_size, telemetry.MAX_BYTES)
        self.state['issues'] = {}
        bridge.save_state(self.state)
        self.assertEqual(telemetry.load(self.path)['runtime'], [])

    def test_telemetry_write_failure_does_not_fail_or_replay_workflow_persistence(self):
        with patch.object(telemetry, 'write_json_if_changed', side_effect=OSError('unavailable')):
            self.assertTrue(bridge.save_state(self.state))
            self.assertEqual(bridge.load_state(), telemetry.durable_state(self.state))
            self.assertFalse(bridge.save_state(self.state))
        with patch.object(bridge, 'write_json_if_changed', side_effect=OSError('durability failed')), \
                patch.object(telemetry, 'save') as save:
            with self.assertRaises(OSError):
                bridge.save_state(self.state)
            save.assert_not_called()

    def test_telegram_duration_is_rendered_only_for_send_and_payload_stays_stable(self):
        created = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
        issue = {'number': 1, 'title': 'Old title', 'state': 'OPEN', 'labels': ['dev:needs-owner']}
        self.item.update(coding_status='DONE', desired_label='dev:needs-owner', owner_blocker='Choose A or B')
        report = {'created_at': created.isoformat()}
        with patch.object(bridge, 'WORK_CONTEXT', (self.state, '1', self.item)), \
                patch.object(bridge.notifications, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = created + timedelta(minutes=5)
            bridge.notify('NEEDS_OWNER', issue, self.item, report, 'Choose A or B')
        key, entry = next(iter(self.state['outbox'].items()))
        payload = entry['payload']
        self.assertNotIn('Duration:', payload)
        issue['title'] = 'Fresh title'
        expected = 'Duration: 5m 30s'

        def assert_send_boundary(text):
            stored = durable.read_json(self.path)['outbox'][key]
            self.assertEqual(stored['status'], 'SENDING')
            self.assertEqual(stored['payload'], payload)
            self.assertIn(expected, text)
            self.assertIn('Fresh title', text)
            self.assertIn('Next owner action: ANSWER', text)
            return True

        with patch.object(bridge.notifications, 'datetime', wraps=datetime) as clock, \
                patch.object(bridge, 'fetch_issue', return_value=issue), \
                patch.object(bridge, 'send_telegram', side_effect=assert_send_boundary) as send:
            clock.now.return_value = created + timedelta(minutes=5, seconds=30)
            bridge.flush_effects(self.state)
        send.assert_called_once()
        self.assertEqual(entry['status'], 'DONE')
        self.assertEqual(entry['payload'], payload)

        # The production loop uses asynchronous dispatch; it receives the same
        # transient buffer, with SENDING durable before dispatch starts.
        entry['status'] = 'PENDING'
        bridge.save_state(self.state)
        expected = 'Duration: 6m 0s'
        dispatch = Mock(side_effect=lambda effect_key, text: (
            self.assertEqual(effect_key, key), assert_send_boundary(text)))
        with patch.object(bridge.notifications, 'datetime', wraps=datetime) as clock, \
                patch.object(bridge, 'fetch_issue', return_value=issue):
            clock.now.return_value = created + timedelta(minutes=6)
            bridge.flush_effects(self.state, telegram_dispatch=dispatch)
        dispatch.assert_called_once()
        self.assertEqual(entry['status'], 'SENDING')
        self.assertEqual(entry['payload'], payload)
        # A restart after dispatch retains uncertain acceptance rather than
        # resending or trusting a transient text buffer as a delivery receipt.
        restored = bridge.load_state()
        with patch.object(bridge, 'send_telegram') as send:
            bridge.flush_effects(restored)
        send.assert_not_called()
        self.assertEqual(restored['outbox'][key]['status'], 'UNCERTAIN')

    def test_legacy_payload_cannot_bypass_freshness_gate_for_regenerated_commit_invitation(self):
        self.item.update(coding_status='DONE', review_status='PASS', review_coding_turn=1,
            owner_state='READY_FOR_OWNER', owner_approved_at='2026-10-09T12:00:00+00:00',
            reviewed_tree={'fingerprint': 'reviewed'})
        issue = {'number': 1, 'state': 'OPEN', 'title': 'Current task'}
        key = bridge.bridge_effects.enqueue(self.state, 1, 'telegram', 'COMMITTED\nLegacy snapshot')
        entry = self.state['outbox'][key]
        self.assertTrue(bridge.ready_owner_notification(entry, self.item))
        with patch.object(bridge, 'fetch_issue', return_value=issue), \
                patch.object(bridge.notifications, 'terminal_text', wraps=bridge.notifications.terminal_text) as render, \
                patch.object(bridge, 'send_telegram') as send:
            bridge.flush_effects(self.state, reconciled=False)
            bridge.flush_effects(self.state, control_fresh=set())
            render.assert_not_called()
            send.assert_not_called()
            self.assertEqual(entry['status'], 'PENDING')
            send.return_value = True
            bridge.flush_effects(self.state, control_fresh={'1'})
        send.assert_called_once()
        self.assertIn('Next owner action: COMMIT', send.call_args.args[0])
        self.assertTrue(send.call_args.args[0].startswith('REVIEW_PASSED / READY_FOR_OWNER'))
        self.assertEqual(entry['payload'], 'COMMITTED\nLegacy snapshot')


class IdleLoopTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    async def drive(self, loop, count=80, observe=None):
        # F3 advances one explicit I/O boundary per writer step. Exercise real
        # discovery/control/report commands rather than the previous loop API.
        for tick in range(count):
            if observe is not None:
                observe(tick)
            loop.next_heavy = 0
            loop.next_control.clear()
            await loop.step()
            await asyncio.sleep(.002)

    async def test_deferred_telegram_with_moving_clock_does_not_rewrite_large_state(self):
        created = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
        now = created + timedelta(minutes=5)
        for with_metadata in (True, False):
            with self.subTest(with_metadata=with_metadata):
                issue = self.h.issues[1]
                issue['labels'] = ['dev:needs-owner']
                item = dict(task_id='task', project='test', coding_status='DONE',
                    desired_label='dev:needs-owner', owner_blocker='Choose A or B',
                    compatibility_status='QUARANTINED',
                    notification_report={'created_at': created.isoformat()})
                state = {'issues': {'1': item}, 'large_payload': 'x' * 2_280_000}
                with patch.object(bridge, 'WORK_CONTEXT', (state, '1', item)):
                    bridge.notify('NEEDS_OWNER', issue, item, item['notification_report'], 'Choose A or B')
                entry = next(iter(state['outbox'].values()))
                # Also model existing pre-fix snapshots containing a frozen
                # duration. Eligibility must leave those payloads untouched.
                with patch.object(bridge.notifications, 'datetime', wraps=datetime) as clock:
                    clock.now.return_value = now
                    entry['payload'] = bridge.notifications.terminal_text('NEEDS_OWNER', issue, item,
                        item['notification_report'], 'Choose A or B')
                self.assertIn('Duration: 5m 0s', entry['payload'])
                if not with_metadata:
                    entry.pop('notification')
                entry['next_attempt'] = (now + timedelta(hours=1)).timestamp()
                loop = bridge.ControlLoop(state)
                loop.next_heavy = float('inf')
                try:
                    with patch.object(bridge.notifications, 'datetime', wraps=datetime) as clock, \
                            patch.object(bridge.time, 'time', return_value=now.timestamp()) as wall_clock:
                        clock.now.return_value = now
                        await self.drive(loop)  # Settle projection/diagnostic containers.
                        before = bridge.STATE_FILE.stat()
                        contents = bridge.STATE_FILE.read_bytes()
                        snapshot = copy.deepcopy(entry)
                        with patch.object(durable, 'write_json', wraps=durable.write_json) as writes, \
                                patch.object(bridge.notifications, 'terminal_text', wraps=bridge.notifications.terminal_text) as render:
                            for tick in range(1, 5):
                                clock.now.return_value = now + timedelta(seconds=30 * tick)
                                wall_clock.return_value = clock.now.return_value.timestamp()
                                await self.drive(loop, count=20)
                            await loop.close()
                            state = bridge.load_state()
                            loop = bridge.ControlLoop(state)
                            loop.next_heavy = float('inf')
                            await self.drive(loop)
                        render.assert_not_called()
                        self.assertFalse(any(call.args[0] == bridge.STATE_FILE for call in writes.call_args_list))
                    self.h.telegram.assert_not_called()
                    self.assertEqual(state['outbox'], {next(iter(state['outbox'])): snapshot})
                    self.assertEqual(bridge.STATE_FILE.read_bytes(), contents)
                    self.assertEqual(bridge.STATE_FILE.stat().st_mtime_ns, before.st_mtime_ns)
                    self.assertEqual(bridge.STATE_FILE.stat().st_ino, before.st_ino)
                finally:
                    await loop.close()

    async def test_runtime_uncertainty_transition_persists_and_restart_deduplicates_it(self):
        await bridge.tick(self.h.state)
        item = self.h.state['issues']['1']
        task = self.h.tasks[item['task_id']]
        task['liveness'] = health()
        await bridge.tick(self.h.state)
        task.update(status='UNKNOWN', liveness=dict(health(1100, 2), state='SUSPECTED_STALL',
            reason='lease_stale_but_process_owned', transition_sequence=2, last_transition_at=1100))
        await bridge.tick(self.h.state)
        persisted = durable.read_json(bridge.STATE_FILE)
        self.assertTrue(persisted['issues']['1']['worker_uncertain'])
        self.assertEqual(persisted['issues']['1']['desired_label'], 'dev:needs-owner')
        self.assertEqual(persisted['issues']['1']['runtime_health']['coding']['state'], 'SUSPECTED_STALL')
        self.assertTrue(persisted['repo_locks'])
        notifications = len(self.h.telegram.call_args_list)
        self.h.restart()
        task['liveness'].update(observed_at=1110, heartbeat_age_seconds=12)
        with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
            await bridge.tick(self.h.state)
        self.assertFalse(any(call.args[0] == bridge.STATE_FILE for call in writes.call_args_list))
        self.assertEqual(len(self.h.telegram.call_args_list), notifications)
        self.assertFalse(Scheduler(self.h.state).release('1'))
        self.assertEqual(len(self.h.starts), 1)

    async def test_running_idle_control_loop_heavy_observations_and_restart_do_not_write_large_state(self):
        await bridge.tick(self.h.state)
        item = self.h.state['issues']['1']
        task = self.h.tasks[item['task_id']]
        self.h.state['large_payload'] = 'x' * 2_280_000
        loop = bridge.ControlLoop(self.h.state)

        def observe(tick):
            task['liveness'] = health(1000 + tick, tick + 1)

        try:
            await self.drive(loop, count=120, observe=observe)
            self.assertIn('coding', item['runtime_health'])
            bridge.save_state(self.h.state)
            before = bridge.STATE_FILE.stat()
            contents = bridge.STATE_FILE.read_bytes()
            with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                await self.drive(loop, count=120, observe=lambda n: observe(n + 120))
            self.assertFalse(any(call.args[0] == bridge.STATE_FILE for call in writes.call_args_list))
            self.assertEqual(bridge.STATE_FILE.read_bytes(), contents)
            self.assertEqual(bridge.STATE_FILE.stat().st_mtime_ns, before.st_mtime_ns)
            self.assertTrue(telemetry.load(bridge.STATE_FILE)['timings']['samples'])
            self.assertGreater(item['runtime_health']['coding']['observed_at'], 1119)
            await loop.close()
            self.h.restart()
            loop = bridge.ControlLoop(self.h.state)
            with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                await self.drive(loop, observe=lambda n: observe(n + 240))
            self.assertFalse(any(call.args[0] == bridge.STATE_FILE for call in writes.call_args_list))
            self.assertEqual(bridge.STATE_FILE.read_bytes(), contents)
            self.assertEqual(len(self.h.starts), 1)
            self.assertFalse(self.h.continues)
            self.assertTrue(self.h.state['repo_locks'])
        finally:
            await loop.close()

    async def test_empty_idle_control_loop_keeps_authoritative_file_unchanged(self):
        self.h.issues.clear()
        loop = bridge.ControlLoop(self.h.state)
        try:
            for _ in range(5):
                loop.next_heavy = 0
                await loop.step()
                await asyncio.sleep(.01)
            bridge.save_state(self.h.state)
            before = bridge.STATE_FILE.stat()
            with patch.object(durable, 'write_json', wraps=durable.write_json) as writes:
                for _ in range(20):
                    loop.next_heavy = 0
                    await loop.step()
                    await asyncio.sleep(.01)
            self.assertFalse(any(call.args[0] == bridge.STATE_FILE for call in writes.call_args_list))
            self.assertEqual(bridge.STATE_FILE.stat().st_mtime_ns, before.st_mtime_ns)
        finally:
            await loop.close()
