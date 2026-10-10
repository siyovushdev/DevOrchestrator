"""Final latch invariants across registered MCP, writer, restart and replay.

All workers are disposable sleepers. Transports, signals and remote cleanup
are mocked; only the MCP adapter's local devctl subprocess is real where noted.
"""
import copy
import json
import os
import subprocess
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import bridge_effects
import devctl
import external_effects
import mcp_server
import remote_worker
import runtime_liveness
import task_bridge as bridge
import worker_runtime
from control_loop import ControlLoop
from durable import read_json, write_json
from external_effects import ENV, ExternalEffectBlocked
from scheduler import Scheduler
import test_acceptance_report_latch as report_fixtures
from transition_executor import execute
from transition_io import Command


class FinalReportTests(unittest.IsolatedAsyncioTestCase):
    # Reuse the isolated HOME/disposable process fixtures, not their test cases.
    setUp = report_fixtures.ReportLatchTests.setUp
    restore_env = report_fixtures.ReportLatchTests.restore_env
    latch = report_fixtures.ReportLatchTests.latch
    share = report_fixtures.ReportLatchTests.share
    terminal_task = report_fixtures.ReportLatchTests.terminal_task
    exec_devctl = report_fixtures.ReportLatchTests.exec_devctl
    launcher = report_fixtures.ReportLatchTests.launcher
    dispatched = report_fixtures.ReportLatchTests.dispatched

    async def test_completion_semantics_through_registered_dispatch(self):
        process, task_id, root = self.terminal_task()
        directory = root / 'tasks' / task_id
        for output, expected in (('', 'FAILED'), ('invalid json\n', 'FAILED'),
                                 (json.dumps({'type': 'result', 'result': 'verified final'}) + '\n', 'DONE')):
            with self.subTest(output=output):
                (directory / 'events.jsonl').write_text(output)
                self.latch()
                before = report_fixtures.snapshot(root)
                report = await self.dispatched('get_task_report', {'task_id': task_id})
                self.assertEqual(report['status'], expected)
                self.assertEqual(report['observation_confidence'], 'READ_ONLY')
                self.assertEqual(report['liveness']['state'], 'UNKNOWN')
                self.assertEqual(report_fixtures.snapshot(root), before)
                self.latch(False)
                meta = read_json(directory / 'meta.json')
                with patch.object(worker_runtime.os, 'killpg'), patch.object(devctl, 'DATA_DIR', root / 'tasks'):
                    self.assertEqual(devctl.task_state(meta, directory)[0], expected)
                # Restore the source receipt between cases; normal failure
                # diagnosis deliberately records missing final evidence.
                record = read_json(directory / 'liveness.json')
                record['terminal_reason'] = 'process_exited'
                write_json(directory / 'liveness.json', record)
        self.assertIsNone(process.poll())

    async def test_stale_terminal_and_remote_cached_metadata_are_unverified(self):
        _process, task_id, root = self.terminal_task(remote=True)
        directory = root / 'tasks' / task_id
        self.latch()
        meta = read_json(directory / 'meta.json')
        meta.update(runtime_generation='new-generation', remote_status='DONE')
        write_json(directory / 'meta.json', meta)
        before = report_fixtures.snapshot(root)
        report = await self.dispatched('get_task_report', {'task_id': task_id})
        self.assertEqual(report['status'], 'UNKNOWN')
        self.assertEqual(remote_worker.cached_refresh(meta, directory), ('UNKNOWN', None))
        self.assertEqual(report_fixtures.snapshot(root), before)
        (directory / 'exit_code').unlink()
        (directory / 'liveness.json').unlink()
        meta.pop('runtime_generation')
        write_json(directory / 'meta.json', meta)
        self.assertEqual(remote_worker.cached_refresh(meta, directory), ('UNKNOWN', None))
        report = await self.dispatched('get_task_report', {'task_id': task_id})
        self.assertEqual(report['status'], 'UNKNOWN')

    async def test_missing_health_receipt_through_registered_running_report(self):
        process, task_id, root = self.terminal_task('running-no-health')
        directory = root / 'tasks' / task_id
        (directory / 'exit_code').unlink()
        record = read_json(directory / 'liveness.json')
        record.update(phase='CODING', exit_code=None, terminal_reason=None, heartbeat_sequence=1)
        write_json(directory / 'liveness.json', record)
        self.latch()
        before = report_fixtures.snapshot(root)
        with patch.object(worker_runtime.os, 'killpg') as groups, patch.object(devctl.os, 'kill') as signals, \
                patch('subprocess.Popen', side_effect=AssertionError('spawn')), \
                patch.object(remote_worker, 'rpc', side_effect=AssertionError('SSH')):
            report = await self.dispatched('get_task_report', {'task_id': task_id})
        self.assertEqual(report['status'], 'UNKNOWN')
        self.assertEqual(report['liveness']['state'], 'UNKNOWN')
        self.assertEqual(report_fixtures.snapshot(root), before)
        groups.assert_not_called()
        signals.assert_not_called()
        self.assertIsNone(process.poll())

    async def test_explicit_read_only_request_latches_child_of_unlatched_server(self):
        process, task_id, root = self.terminal_task()
        self.latch(False)
        before = report_fixtures.snapshot(root)
        result = await mcp_server.mcp.call_tool('get_task_observation', {'task_id': task_id})
        report = bridge.decode_mcp_result('get_task_report', result)
        self.assertEqual(report['status'], 'DONE')
        self.assertEqual(report['observation_confidence'], 'READ_ONLY')
        # This checkout intentionally has no projects.json. Supply a disposable
        # registry in the real task-list child, retaining the adapter's env.
        real_run = subprocess.run
        def fixture_run(argv, **kwargs):
            argv = [sys.executable, '-c',
                    "import devctl; devctl.load_projects=lambda: {'test': {'machine': 'work', 'workspace': '/registered'}}; devctl.main()",
                    *argv[1:]]
            return real_run(argv, **kwargs)
        with patch.object(mcp_server.subprocess, 'run', fixture_run):
            listed = await mcp_server.mcp.call_tool('list_task_observations', {})
        self.assertEqual(bridge.decode_mcp_result('list_tasks', listed)[0]['observation_confidence'], 'READ_ONLY')
        self.assertEqual(report_fixtures.snapshot(root), before)
        self.assertIsNone(process.poll())

    async def test_poll_removal_restart_flush_and_reconciliation_do_not_replay(self):
        terminal, task_id, root = self.terminal_task()
        running, running_id, _ = self.terminal_task('running-task')
        directory = root / 'tasks' / running_id
        (directory / 'exit_code').unlink()
        record = read_json(directory / 'liveness.json')
        record.update(phase='CODING', exit_code=None, terminal_reason=None, heartbeat_sequence=1)
        write_json(directory / 'liveness.json', record)
        # Both missing health and stale LOST are exercised; neither is authority.
        write_json(directory / 'runtime_health.json', dict(state='LOST_ORPHANED',
                   reason='lease_expired_without_owned_process', worker_alive=False, process_alive=False))
        state = {'issues': {'1': {'task_id': task_id, 'queue_status': 'STARTED', 'project': 'test'},
                            '2': {'task_id': running_id, 'queue_status': 'STARTED', 'project': 'test'}}}
        # Pre-upgrade smoke pollution, with no trustworthy origin evidence.
        legacy = []
        for kind, payload in [('comment', 'old smoke completion'), ('telegram', 'old smoke alert'),
                              ('transition', {'issue': {'number': 1}, 'target': 'dev:ready-for-review'})]:
            key = bridge_effects.enqueue(state, '1', kind, payload)
            state['outbox'][key].pop('effect_origin')
            legacy.append(key)
        unrelated = bridge_effects.enqueue(state, '99', 'comment', 'authorized before smoke')
        preserved = copy.deepcopy(state['outbox'][unrelated])
        state['work_queue'] = {'job': dict(owner='1', task_id=task_id, status='RUNNING',
            sequence=1, machine='work', repo='owned-repo', kind='coding')}
        state['repo_locks'] = {'owned-repo': {'owner': '1', 'key': 'job'}}
        file = self.home / 'bridge.json'
        before = report_fixtures.snapshot(root)
        reports = {}

        async def transport(name, args):
            self.assertIn(name, ('get_task_report', 'list_tasks'))
            value = await self.dispatched(name, args)
            if name == 'get_task_report':
                reports[args['task_id']] = value
            return value

        issues = {n: dict(number=n, title='Task', body='', labels=[], state='OPEN') for n in (1, 2, 99)}
        with patch.object(bridge, 'STATE_FILE', file), patch.object(bridge, 'transport_call', transport), \
                patch.object(worker_runtime.os, 'killpg') as signals, \
                patch.object(devctl.os, 'kill') as pid_signals, \
                patch.object(remote_worker, 'rpc', side_effect=AssertionError('SSH')), \
                patch('subprocess.Popen', side_effect=AssertionError('worker spawn')), \
                patch.object(devctl, 'capture', side_effect=AssertionError('mutation')):
            self.latch()
            await bridge.tick(state)
            await bridge.control_pass(state)
            loop = ControlLoop(state)
            try:
                await loop.step()
            finally:
                await loop.close()
            signals.assert_not_called()
            pid_signals.assert_not_called()
            self.assertEqual(report_fixtures.snapshot(root), before)
            self.assertIsNone(terminal.poll())
            self.assertIsNone(running.poll())
            self.assertEqual(reports[running_id]['status'], 'UNKNOWN')
            self.assertEqual(reports[running_id]['liveness']['state'], 'UNKNOWN')
            for item in (state['issues']['1'], state['issues']['2']):
                self.assertIn('safety_latch_hold', item)
                self.assertNotIn('desired_label', item)
                self.assertNotIn('coding_status', item)
                self.assertNotIn('last_report_signature', item)
            self.assertEqual(state['outbox'][unrelated], preserved)
            self.assertTrue(all(state['outbox'][key]['status'] == 'QUARANTINED' for key in legacy))
            self.assertEqual(state['work_queue']['job']['status'], 'RUNNING')
            from operational_state import lifecycle_state, next_action
            self.assertEqual(lifecycle_state(state['issues']['1']), 'OBSERVATION_HELD')
            self.assertEqual(next_action(state['issues']['1']), 'INSPECT_SAFETY_LATCH_AUTHORIZATION')
            self.latch(False)
            state = bridge.load_state()  # Real durable reload, not just same-process flags.
            published = []
            async def normal_transport(name, args):
                self.assertEqual(name, 'list_tasks')  # Held tasks must not enter reconciliation.
                return [dict(value, observation_confidence='RUNTIME') for value in reports.values()]
            with patch.object(bridge, 'transport_call', normal_transport), \
                    patch.object(bridge, 'open_issues', return_value=[]), \
                    patch.object(bridge, 'fetch_issue', side_effect=lambda n: issues[int(n)]), \
                    patch.object(bridge, 'issue_comments', return_value=[]), \
                    patch.object(bridge, 'raw_add_comment', side_effect=lambda n, body: published.append((n, body))), \
                    patch.object(bridge, 'raw_transition_issue', side_effect=AssertionError('smoke labels')), \
                    patch.object(bridge, 'send_telegram', side_effect=AssertionError('smoke telegram')):
                bridge.flush_effects(state)
                await bridge.tick(state)
                await bridge.control_pass(state)
                bridge.flush_effects(state)
            self.assertEqual([n for n, _ in published], ['99'])
            self.assertEqual(state['outbox'][unrelated]['status'], 'DONE')
            self.assertTrue(all(state['outbox'][key]['status'] == 'QUARANTINED' for key in legacy))
            self.assertEqual(set(state['outbox']), set(legacy) | {unrelated})
            self.assertEqual(state['work_queue']['job']['status'], 'RUNNING')
            self.assertIn('owned-repo', state['repo_locks'])

    def test_cli_exec_and_direct_runtime_launch_are_blocked(self):
        self.latch()
        before = report_fixtures.snapshot(self.home)
        with patch('subprocess.run', side_effect=AssertionError('exec')), \
                patch('subprocess.Popen', side_effect=AssertionError('spawn')):
            for machine in ('work', 'firefly'):
                with self.assertRaises(ExternalEffectBlocked):
                    devctl.exec_command(machine, 'true')
            with self.assertRaises(RuntimeError):
                runtime_liveness.run(self.home / 'absent', 'generation', 'true')
            with self.assertRaises(ExternalEffectBlocked):
                devctl.spawn_runtime(self.home / 'absent', {}, 'true', StringIO())
            with self.assertRaises(ExternalEffectBlocked):
                devctl.certify_local_absent('task', 'generation', control_group_reset_verified=True)
        self.assertEqual(report_fixtures.snapshot(self.home), before)

    def test_direct_remote_script_is_read_only_and_start_is_blocked(self):
        self.latch()
        before = report_fixtures.snapshot(self.home)
        report = subprocess.run([sys.executable, '-c', remote_worker.SCRIPT, 'report', 'absent-key', '0'],
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(report.returncode, 0, report.stderr)
        self.assertEqual(json.loads(report.stdout)['status'], 'UNKNOWN')
        start = subprocess.run([sys.executable, '-c', remote_worker.SCRIPT, 'start', 'absent-key', 'true'],
                               text=True, capture_output=True, timeout=10)
        self.assertNotEqual(start.returncode, 0)
        self.assertEqual(report_fixtures.snapshot(self.home), before)


class DurableQuarantineTests(unittest.IsolatedAsyncioTestCase):
    async def test_inline_launch_cannot_adopt_a_read_only_final_report(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp, patch.object(bridge, 'STATE_FILE', Path(tmp) / 'bridge.json'), \
                patch.dict(os.environ, {}, clear=False):
            os.environ.pop(ENV, None)
            item = {'queue_status': 'QUEUED'}
            state = {'issues': {'1': item}}
            bridge.WORK_CONTEXT = (state, '1', item)
            self.addCleanup(lambda: setattr(bridge, 'WORK_CONTEXT', None))
            async def io(name, args, **kwargs):
                if name == 'project_identity':
                    return 'work', 'owned-repo'
                if name == 'rpc' and args['name'] == 'preflight_task':
                    return {'status': 'READY'}
                if name == 'rpc' and args['name'] == 'start_task':
                    task_id = args['arguments']['request_id'][:24]
                    return {'id': task_id, 'task_id': task_id, 'status': 'DONE', 'turns': 1,
                            'exit_code': 0, 'observation_confidence': 'READ_ONLY'}
                raise AssertionError(name)
            with patch.object(bridge, 'io_call', io), self.assertRaises(ExternalEffectBlocked):
                await bridge.mcp_call('start_task', {'project': 'test', 'prompt': 'authorized', 'role': 'coding'})
            self.assertIn('pending_rpc', item)
            self.assertTrue(item['safety_latch_hold'])
            self.assertEqual(next(iter(state['work_queue'].values()))['status'], 'RESERVED')
            self.assertNotIn('coding_status', item)
            self.assertFalse(state.get('outbox'))

    async def test_old_server_without_observation_tool_fails_before_mutating_report(self):
        import mcp
        client = AsyncMock()
        client.__aenter__.return_value = client
        mutation = Mock()
        async def call(name, args):
            if name != 'get_task_report':
                raise RuntimeError('Unknown tool')
            mutation()
        client.call_tool.side_effect = call
        with patch.dict(os.environ, {ENV: '1'}), patch.object(mcp, 'Client', return_value=client):
            with self.assertRaisesRegex(RuntimeError, 'Unknown tool'):
                await bridge.transport_call('get_task_report', {'task_id': 'task'})
        mutation.assert_not_called()

    async def test_network_report_requests_propagate_read_only_without_changing_arguments(self):
        import mcp
        client = AsyncMock()
        client.__aenter__.return_value = client
        result = Mock(content=[Mock(type='text', text=json.dumps({'id': 'task', 'observation_confidence': 'READ_ONLY'}))],
                      is_error=False, isError=False)
        client.call_tool.return_value = result
        args = {'task_id': 'task'}
        with patch.dict(os.environ, {ENV: '1'}), patch.object(mcp, 'Client', return_value=client):
            report = await bridge._transport_call('get_task_report', args)
        client.call_tool.assert_awaited_once_with('get_task_observation', {'task_id': 'task'})
        self.assertEqual(args, {'task_id': 'task'})
        self.assertEqual(report['observation_confidence'], 'READ_ONLY')
    async def test_generic_flush_quarantines_legacy_pending_without_deleting_unrelated(self):
        state = {'issues': {'1': {'task_id': 'task'}}, 'outbox': {}}
        keys = [bridge_effects.enqueue(state, number, 'comment', payload)
                for number, payload in (('1', 'legacy smoke'), ('99', 'unrelated'))]
        for key in keys:
            state['outbox'][key].pop('effect_origin')
        unrelated = copy.deepcopy(state['outbox'][keys[1]])
        sent, persisted = [], []
        with patch.dict(os.environ, {ENV: '1'}):
            bridge_effects.flush(state, lambda: persisted.append(json.loads(json.dumps(state))),
                                 lambda n: [], lambda n, body: sent.append(n), Mock(), Mock())
        self.assertTrue(persisted)
        self.assertEqual(state['outbox'][keys[0]]['status'], 'QUARANTINED')
        self.assertEqual(state['outbox'][keys[1]], unrelated)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(ENV, None)
            bridge_effects.flush(state, lambda: None, lambda n: [], lambda n, body: sent.append(n), Mock(), Mock())
        self.assertEqual(sent, ['99'])

    async def test_discovery_cache_race_does_not_revoke_sticky_closure_or_latch_hold(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp, patch.object(bridge, 'STATE_FILE', Path(tmp) / 'bridge.json'), \
                patch.dict(os.environ, {}, clear=False):
            os.environ.pop(ENV, None)
            state = {'issues': {'1': {'task_id': 'task', 'queue_status': 'STARTED'}}}
            loop = ControlLoop(state)
            closed = dict(number=1, state='CLOSED', labels=[], title='Task', body='')
            opened = dict(closed, state='OPEN')
            loop._observe_control('1', {'issue': closed, 'comments': []})
            loop._apply_discovery({'issues': [opened], 'tasks': [], 'identities': {}})
            self.assertEqual(loop.issues['1']['state'], 'OPEN')  # F4 reproduced, explicitly deferred.
            self.assertTrue(state['issues']['1']['issue_closed'])
            self.assertTrue(bridge.archive_issue(opened, state['issues']['1'], state))
            with patch.dict(os.environ, {ENV: '1'}):
                bridge.observation_barrier(state, '1')
            loop.issues['1'] = closed
            loop._apply_discovery({'issues': [opened], 'tasks': [], 'identities': {}})
            self.assertEqual(loop.issues['1']['state'], 'CLOSED')
            await loop.close()

    async def test_enqueue_tombstones_survive_recreation_and_status_reset(self):
        state = {'issues': {'1': {}}, 'outbox': {}}
        with patch.dict(os.environ, {ENV: '1'}):
            keys = [bridge_effects.enqueue(state, '1', kind, payload) for kind, payload in (
                ('comment', 'report'), ('telegram', 'notice'),
                ('transition', {'issue': {'number': 1}, 'target': 'dev:running'}))]
        state = json.loads(json.dumps(state))
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(ENV, None)
            for key in keys:
                entry = state['outbox'][key]
                self.assertEqual(bridge_effects.enqueue(state, '1', entry['kind'], entry['payload']), key)
                entry['status'] = 'PENDING'  # Even an old receipt-reset path cannot authorize replay.
            changed = bridge_effects.enqueue(state, '1', 'comment', 'regenerated with changed event', event='new-key')
            self.assertEqual(state['outbox'][changed]['status'], 'QUARANTINED')
            callback = Mock(side_effect=AssertionError('smoke dispatched'))
            bridge_effects.flush(state, callback, callback, callback, callback, callback)
            callback.assert_not_called()

    async def test_inflight_report_keeps_read_only_provenance_after_removal(self):
        async def transport(name, args):
            os.environ.pop(ENV, None)
            return {'id': 'task', 'status': 'DONE', 'result': 'valid', 'exit_code': 0}
        with patch.dict(os.environ, {ENV: '1'}), patch.object(bridge, 'transport_call', transport):
            report = await execute(Command.create('rpc', dict(name='get_task_report', arguments={'task_id': 'task'})))
        self.assertEqual(report['observation_confidence'], 'READ_ONLY')

    async def test_registered_dispatch_retains_provenance_if_latch_changes_inflight(self):
        def adapter(*args):
            os.environ.pop(ENV, None)
            return json.dumps({'id': 'task', 'status': 'DONE', 'observation_confidence': 'RUNTIME'})
        with patch.dict(os.environ, {ENV: '1'}), patch.object(mcp_server, 'devctl', adapter):
            result = await mcp_server.mcp.call_tool('get_task_report', {'task_id': 'task'})
        self.assertEqual(bridge.decode_mcp_result('get_task_report', result)['observation_confidence'], 'READ_ONLY')

    async def test_late_nested_result_cannot_adopt_after_latch_removal(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp, patch.object(bridge, 'STATE_FILE', Path(tmp) / 'bridge.json'):
            state = {'issues': {'1': {'task_id': 'task', 'queue_status': 'STARTED'}}}
            loop = ControlLoop(state)
            loop.reviews.operations['review:request'] = ('1', 'verify')
            value = {'source_report': {'id': 'task', 'status': 'DONE', 'observation_confidence': 'READ_ONLY'}}
            with patch.dict(os.environ, {}, clear=False), patch.object(loop.reviews, 'complete') as complete:
                os.environ.pop(ENV, None)
                loop._completion('review:request', Command.create('verify_review', {}), value, None)
                complete.assert_not_called()
            self.assertTrue(state['issues']['1']['safety_latch_hold'])
            await loop.close()

    def test_telegram_setup_stops_before_input_files_or_transport(self):
        import telegram_setup
        with patch.dict(os.environ, {ENV: '1'}), \
                patch('urllib.request.urlopen', side_effect=AssertionError('network')) as network, \
                patch.object(telegram_setup.getpass, 'getpass', side_effect=AssertionError('input')):
            with self.assertRaises(ExternalEffectBlocked):
                telegram_setup.main()
            with self.assertRaises(ExternalEffectBlocked):
                telegram_setup.api('fake', 'sendMessage', {'text': 'test'})
        network.assert_not_called()

    async def test_operational_smoke_client_cannot_reach_an_unlatched_server(self):
        import mcp_smoke
        with patch.dict(os.environ, {ENV: '1'}), patch.object(mcp_smoke, 'Client') as client:
            with self.assertRaises(ExternalEffectBlocked):
                await mcp_smoke.main()
        client.assert_not_called()

    def test_rpc_guard_allows_only_observational_tools(self):
        with patch.dict(os.environ, {ENV: '1'}):
            for name, args in (
                ('orchestrator_health', {}), ('get_execution_catalog', {'discover': True}),
                ('preflight_task', {'operation': 'start_task', 'arguments': {}}),
                ('unknown_tool', {}), ('inspect_start_request', {'fence': True})):
                with self.subTest(name=name), self.assertRaises(ExternalEffectBlocked):
                    external_effects.guard_worker_rpc(name, args)
            for name in ('get_task_report', 'list_tasks', 'list_projects', 'get_transition_capabilities',
                         'get_execution_catalog', 'inspect_start_request'):
                external_effects.guard_worker_rpc(name, {})

    async def test_missing_health_does_not_probe_or_classify_healthy(self):
        with patch.dict(os.environ, {ENV: '1'}), patch.object(runtime_liveness, 'owned', side_effect=AssertionError('probe')):
            health = runtime_liveness.watchdog(Path('/absent'), {'id': 'task'})
            self.assertEqual(health['state'], 'UNKNOWN')
            self.assertEqual(runtime_liveness.task_status(health), ('UNKNOWN', None))
