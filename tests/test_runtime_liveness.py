from contextlib import redirect_stdout
import asyncio
import base64
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import devctl
from durable import read_json, write_json
import remote_worker
import runtime_liveness as runtime
import task_bridge as bridge
import worker_runtime
from worker_runtime import process_identity


class RuntimeLivenessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name) / 'tasks/task'
        self.directory.mkdir(parents=True)
        self.meta = dict(id='task', machine='work', project='test', agent='codex', workspace='/registered', pid=0)
        self.record = runtime.initialize(self.directory, self.meta, clock=1000)
        self.meta['runtime_generation'] = self.record['generation']
        write_json(self.directory / 'meta.json', self.meta)

    def record_alive(self):
        self.record.update(worker_pid=os.getpid(), worker_identity=process_identity(os.getpid()),
                           agent_pid=os.getpid(), agent_identity=process_identity(os.getpid()),
                           heartbeat_sequence=1, phase='CODING')
        write_json(self.directory / 'liveness.json', self.record)

    def status(self, clock, **kwargs):
        return runtime.watchdog(self.directory, self.meta, clock=clock, **kwargs)

    def test_fresh_heartbeat_and_owned_process_are_healthy(self):
        self.record_alive()
        health = self.status(1005)
        self.assertEqual(health['state'], 'RUNNING_HEALTHY')
        self.assertEqual(health['heartbeat_age_seconds'], 5)
        self.assertEqual(runtime.task_status(health), ('RUNNING', None))
        self.assertTrue(health['worker_alive'])

    def test_stale_heartbeat_with_live_process_is_suspected_never_lost(self):
        self.record_alive()
        health = self.status(1100)
        self.assertEqual(health['state'], 'SUSPECTED_STALL')
        self.assertEqual(health['reason'], 'lease_stale_but_process_owned')
        self.assertEqual(runtime.task_status(health), ('UNKNOWN', None))

    def test_expired_lease_and_absent_processes_are_lost(self):
        self.record_alive()
        with patch.object(runtime, 'owned', return_value=False):
            health = self.status(1030)
        self.assertEqual(health['state'], 'LOST_ORPHANED')
        self.assertEqual(health['lease_state'], 'EXPIRED')
        self.assertEqual(runtime.task_status(health), ('FAILED', None))
        self.assertEqual(read_json(self.directory / 'runtime_health.json')['state'], 'LOST_ORPHANED')

    def test_expired_launch_without_identity_fails_and_releases_capacity(self):
        self.meta['pid'] = 99999999
        write_json(self.directory / 'meta.json', self.meta)
        with patch.object(runtime.time, 'time', return_value=1029):
            self.assertEqual(devctl.task_state(self.meta, self.directory), ('UNKNOWN', None))
            with self.assertRaises(RuntimeError):
                worker_runtime.check_capacity(self.directory.parent, 'next', 'work', 'coding', devctl.task_state)
        with patch.object(runtime.time, 'time', return_value=1030):
            self.assertEqual(devctl.task_state(self.meta, self.directory), ('FAILED', None))
            health = devctl.runtime_status(self.meta, self.directory)
            self.assertEqual(health['state'], 'LOST_ORPHANED')
            self.assertEqual(health['reason'], 'lease_expired_before_worker_registered')
            worker_runtime.check_capacity(self.directory.parent, 'next', 'work', 'coding', devctl.task_state)
            worker_runtime.admit(self.directory.parent, 'work', '/registered', 'task', devctl.task_state)
            worker_runtime.admit(self.directory.parent, 'work', '/registered', 'next', devctl.task_state)
            with patch.object(runtime, 'write_json') as write:
                self.assertEqual(devctl.task_state(self.meta, self.directory), ('FAILED', None))
                write.assert_not_called()

    def test_expired_unregistered_worker_with_live_launch_receipt_is_suspected(self):
        self.meta.update(pid=os.getpid(), process_identity=process_identity(os.getpid()))
        with patch.object(runtime.time, 'time', return_value=1040):
            self.assertEqual(devctl.task_state(self.meta, self.directory), ('UNKNOWN', None))
            health = devctl.runtime_status(self.meta, self.directory)
        self.assertEqual(health['state'], 'SUSPECTED_STALL')
        self.assertTrue(health['worker_alive'])

    def test_expired_unregistered_remote_mirror_keeps_uncertain_capacity(self):
        self.meta.update(remote_key='task-turn-1', machine='firefly')
        self.record['machine'] = 'firefly'
        write_json(self.directory / 'liveness.json', self.record)
        with patch.object(runtime.time, 'time', return_value=1040):
            self.assertEqual(devctl.task_state(self.meta, self.directory, refresh_remote=False), ('UNKNOWN', None))

    def test_unregistered_worker_is_not_failed_before_expiry_after_clock_rollback(self):
        health = self.status(999)
        self.assertEqual(health['state'], 'SUSPECTED_STALL')
        self.assertEqual(health['lease_state'], 'UNKNOWN')
        self.assertEqual(runtime.task_status(health), ('UNKNOWN', None))

    def test_restart_reprobes_and_does_not_trust_persisted_healthy_state(self):
        self.record_alive()
        self.assertEqual(self.status(1005)['state'], 'RUNNING_HEALTHY')
        restored = read_json(self.directory / 'meta.json')
        with patch.object(runtime, 'owned', return_value=False):
            health = runtime.watchdog(self.directory, restored, clock=1006)
        self.assertEqual(health['state'], 'SUSPECTED_STALL')
        self.assertEqual(health['reason'], 'process_ownership_unproven')

    def test_restart_unbound_pid_is_not_proof(self):
        self.record_alive()
        self.record.update(worker_identity=None, agent_identity=None)
        write_json(self.directory / 'liveness.json', self.record)
        self.assertEqual(self.status(1005)['state'], 'SUSPECTED_STALL')

    def test_reboot_and_pid_reuse_fail_ownership(self):
        self.record_alive()
        self.record['worker_identity']['start'] = 'different'
        self.record['agent_identity']['boot'] = 'old-boot'
        write_json(self.directory / 'liveness.json', self.record)
        health = self.status(1040)
        self.assertFalse(health['worker_alive'])
        self.assertFalse(health['process_alive'])
        self.assertEqual(health['state'], 'LOST_ORPHANED')

    def test_duplicate_watchdog_ticks_do_not_write_or_advance_transition(self):
        self.record_alive()
        first = self.status(1005)
        with patch.object(runtime, 'write_json') as write:
            second = self.status(1006)
        write.assert_not_called()
        self.assertEqual(first['transition_sequence'], second['transition_sequence'])
        self.assertEqual(first['last_transition_at'], second['last_transition_at'])
        self.assertEqual(second['heartbeat_age_seconds'], 6)

    def test_corrupt_runtime_fails_closed_and_is_not_overwritten(self):
        self.record_alive()
        malformed = ['{broken', '[]', 'null', json.dumps(self.record)[:-1] + ', "heartbeat_sequence": 1}']
        for changes in ({'schema_version': True}, {'heartbeat_sequence': -1}, {'heartbeat_sequence': True},
                        {'last_heartbeat_at': float('nan')}, {'lease_expires_at': 999},
                        {'worker_identity': {'start': 'x'}}, {'task_id': 'other'},
                        {'generation': 'old-turn'}, {'terminal_reason': 'unknown'},
                        {'exit_code': 0}, {'phase': 'SECRET'}, {'phase': 'EXITED'}):
            malformed.append(json.dumps(dict(self.record, **changes)))
        for payload in malformed:
            with self.subTest(payload=payload):
                path = self.directory / 'liveness.json'; path.write_text(payload)
                (self.directory / 'exit_code').write_text('0')
                health = self.status(1005)
                self.assertEqual(health['state'], 'SUSPECTED_STALL')
                self.assertEqual(health['reason'], 'invalid_runtime_record')
                self.assertEqual(path.read_text(), payload)
                self.assertEqual(runtime.task_status(health), ('UNKNOWN', None))

    def test_corrupt_observation_is_rebuilt_from_evidence_without_leaking_extra_fields(self):
        self.record_alive()
        write_json(self.directory / 'runtime_health.json', {'transition_sequence': 'bad', 'environment': 'SECRET'})
        self.assertEqual(self.status(1005)['state'], 'RUNNING_HEALTHY')
        health = read_json(self.directory / 'runtime_health.json')
        health['environment'] = 'SECRET'
        write_json(self.directory / 'runtime_health.json', health)
        self.assertNotIn('environment', self.status(1006))

    def test_deleted_runtime_record_is_uncertain_even_with_exit_marker(self):
        (self.directory / 'liveness.json').unlink()
        (self.directory / 'exit_code').write_text('0')
        self.assertEqual(self.status(1005, terminal=True)['reason'], 'invalid_runtime_record')
        self.assertEqual(devctl.task_state(self.meta, self.directory)[0], 'UNKNOWN')

    def test_session_discovery_does_not_refresh_heartbeat_or_lease(self):
        runtime.annotate_session(self.directory, self.meta, 'discovered-session')
        record = read_json(self.directory / 'liveness.json')
        self.assertEqual(record['session_id'], 'discovered-session')
        for key in ('last_heartbeat_at', 'lease_expires_at', 'heartbeat_sequence'):
            self.assertEqual(record[key], self.record[key])

    def test_external_labels_and_meta_status_cannot_override_local_liveness(self):
        self.record_alive()
        self.meta.update(status='RUNNING', remote_status='RUNNING', labels=['dev:running'], clickup_status='in progress')
        with patch.object(runtime, 'owned', return_value=False), patch.object(runtime.time, 'time', return_value=1040):
            self.assertEqual(devctl.task_state(self.meta, self.directory), ('FAILED', None))
        self.assertEqual(self.status(1040, probe=False)['state'], 'LOST_ORPHANED')

    def test_remote_cached_health_cannot_prove_ownership_or_free_capacity(self):
        self.record_alive()
        self.meta['remote_key'] = 'task-turn-1'
        self.meta['remote_status'] = 'RUNNING'
        fresh = self.status(1005, probe=False)
        self.assertEqual(fresh['state'], 'SUSPECTED_STALL')
        expired = self.status(1040, probe=False)
        self.assertEqual(expired['state'], 'LOST_ORPHANED')
        self.assertEqual(runtime.task_status(expired), ('UNKNOWN', None))

    def test_process_probe_permission_errors_are_uncertain_not_absence(self):
        with patch.object(runtime.Path, 'read_text', side_effect=PermissionError):
            self.assertIsNone(runtime.owned(os.getpid(), process_identity(os.getpid())))

    def test_wall_clock_rollback_cannot_mark_fresh_heartbeat_healthy(self):
        self.record_alive()
        self.assertEqual(self.status(999)['state'], 'SUSPECTED_STALL')

    def test_heartbeat_generation_fence_rejects_old_runner(self):
        with self.assertRaises(RuntimeError):
            runtime.heartbeat(self.directory, 'old', worker_pid=os.getpid(), agent_pid=os.getpid(), phase='CODING')
        self.assertEqual(read_json(self.directory / 'liveness.json'), self.record)

    def launch(self, command, role='coding'):
        self.meta['role'] = role
        self.meta.pop('runtime_generation', None)
        self.record = runtime.initialize(self.directory, self.meta)
        self.meta['runtime_generation'] = self.record['generation']
        write_json(self.directory / 'meta.json', self.meta)
        source = 'import runtime_liveness as r,sys; r.HEARTBEAT_SECONDS=0.025; sys.exit(r.run(*sys.argv[1:4]))'
        process = subprocess.Popen([sys.executable, '-c', source, str(self.directory), self.record['generation'], command],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def cleanup():
            if process.poll() is None:
                record = read_json(self.directory / 'liveness.json', {})
                if record.get('agent_pid'):
                    try: os.kill(record['agent_pid'], signal.SIGTERM)
                    except ProcessLookupError: pass
                process.terminate()
            process.wait(timeout=3)
        self.addCleanup(cleanup)
        return process

    def await_heartbeats(self):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            record = read_json(self.directory / 'liveness.json')
            if record['heartbeat_sequence'] >= 3:
                return record
            time.sleep(0.01)
        self.fail('Runner did not heartbeat')

    def test_real_silent_runner_heartbeats_and_is_healthy_in_review_phase(self):
        process = self.launch("exec python3 -c 'import time; time.sleep(2)'", 'review')
        record = self.await_heartbeats()
        health = runtime.watchdog(self.directory, self.meta)
        self.assertIsNone(process.poll())
        self.assertEqual(health['state'], 'RUNNING_HEALTHY')
        self.assertEqual(health['phase'], 'REVIEW')
        self.assertGreaterEqual(record['heartbeat_sequence'], 3)
        self.assertFalse((self.directory / 'events.jsonl').exists())

    def test_surviving_child_after_worker_death_remains_suspected(self):
        process = self.launch("exec python3 -c 'import time; time.sleep(2)'")
        record = self.await_heartbeats()
        process.terminate(); process.wait(timeout=3)
        try:
            health = runtime.watchdog(self.directory, self.meta, clock=record['lease_expires_at'] + 1)
            self.assertFalse(health['worker_alive'])
            self.assertTrue(health['process_alive'])
            self.assertEqual(health['state'], 'SUSPECTED_STALL')
            self.assertEqual(runtime.task_status(health), ('UNKNOWN', None))
        finally:
            os.kill(record['agent_pid'], signal.SIGTERM)

    def test_exit_without_final_report_is_durable_diagnosable_failure(self):
        process = self.launch('true'); process.wait(timeout=3)
        with patch.object(devctl, 'DATA_DIR', self.directory.parent):
            for _ in range(2):
                stream = io.StringIO()
                with redirect_stdout(stream): devctl.task_report('task', as_json=True)
                report = json.loads(stream.getvalue())
                self.assertEqual(report['status'], 'FAILED')
                self.assertEqual(report['error'], 'process_exited_without_final_report')
                self.assertEqual(report['liveness']['state'], 'TERMINAL')
        self.assertEqual(read_json(self.directory / 'liveness.json')['terminal_reason'], 'process_exited_without_final_report')

    def test_zero_exit_is_failed_on_every_status_path_before_report_is_read(self):
        for action in ('state', 'status', 'list'):
            with self.subTest(action=action):
                process = self.launch('true'); process.wait(timeout=3)
                with patch.object(devctl, 'DATA_DIR', self.directory.parent):
                    output = io.StringIO()
                    if action == 'state':
                        self.assertEqual(devctl.task_state(self.meta, self.directory), ('FAILED', 0))
                    elif action == 'status':
                        with redirect_stdout(output): devctl.status('task')
                        self.assertIn('status:    FAILED', output.getvalue())
                        self.assertIn('process_exited_without_final_report', output.getvalue())
                    else:
                        with redirect_stdout(output): devctl.list_tasks(as_json=True, cached=True)
                        task = json.loads(output.getvalue())[0]
                        self.assertEqual(task['status'], 'FAILED')
                        self.assertEqual(task['liveness']['reason'], 'process_exited_without_final_report')
                self.assertEqual(read_json(self.directory / 'liveness.json')['terminal_reason'],
                                 'process_exited_without_final_report')

    def test_shared_status_rejects_old_turn_results_and_malformed_agent_output(self):
        old = json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'old result'}}) + '\n'
        malformed = '\n'.join(json.dumps(event) for event in (
            [], {'type': 'item.completed', 'item': []},
            {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': {'invalid': True}}},
            {'type': 'assistant', 'message': {'content': ['invalid', {'type': 'text', 'text': 10}]}}))
        (self.directory / 'events.jsonl').write_text(old + malformed + '\n')
        self.meta['turn_log_offset'] = len(old.encode())
        process = self.launch('true'); process.wait(timeout=3)
        self.assertEqual(devctl.task_state(self.meta, self.directory), ('FAILED', 0))
        self.assertEqual(read_json(self.directory / 'liveness.json')['terminal_reason'], 'process_exited_without_final_report')

    def test_nonzero_exit_is_terminal_and_exposes_exit_status(self):
        process = self.launch('exit 7'); process.wait(timeout=3)
        health = runtime.watchdog(self.directory, self.meta)
        self.assertEqual(health['state'], 'TERMINAL')
        self.assertEqual(runtime.task_status(health), ('FAILED', 7))

    def test_normal_completion_with_final_report_preserves_existing_semantics(self):
        event = json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'complete'}})
        (self.directory / 'events.jsonl').write_text(event + '\n')
        process = self.launch('true'); process.wait(timeout=3)
        with patch.object(devctl, 'DATA_DIR', self.directory.parent), redirect_stdout(io.StringIO()) as stream:
            devctl.task_report('task', as_json=True)
        report = json.loads(stream.getvalue())
        self.assertEqual(report['status'], 'DONE')
        self.assertEqual(report['result'], 'complete')

    def test_duplicate_bridge_projections_are_idempotent_and_override_running_label(self):
        self.record_alive()
        health = self.status(1040)
        report = dict(id='task', status='UNKNOWN', turns=1, liveness=health)
        item = dict(task_id='task', project='test')
        state = dict(issues={'1': item})
        issue = dict(number=1, title='Task', labels=[{'name': 'dev:running'}])
        with patch.object(bridge, 'WORK_CONTEXT', (state, '1', item)), patch.object(bridge, 'save_state'):
            self.assertTrue(bridge.project_runtime_health(issue, item, state, report))
            first = json.dumps(state, sort_keys=True)
            self.assertTrue(bridge.project_runtime_health(issue, item, state, report))
            self.assertEqual(json.dumps(state, sort_keys=True), first)
        self.assertEqual(item['desired_label'], 'dev:needs-owner')
        self.assertEqual(len(state['outbox']), 3)
        self.assertIn('lease_stale_but_process_owned', next(v['payload'] for v in state['outbox'].values() if v['kind'] == 'comment'))

    def test_bridge_rpc_failure_projects_cached_loss_instead_of_running(self):
        self.record_alive()
        health = self.status(1040, probe=False)
        report = dict(id='task', status='UNKNOWN', turns=1, liveness=health)
        item = dict(task_id='task', project='test')
        state = dict(issues={'1': item})
        issue = dict(number=1, title='Task', labels=[{'name': 'dev:running'}])
        with patch.object(bridge, 'mcp_call', side_effect=RuntimeError('RPC unavailable')), patch.object(bridge, 'save_state'), \
             patch.object(bridge, 'WORK_CONTEXT', None):
            asyncio.run(bridge.process_tracked(issue, item, state, {'task': report}))
        self.assertEqual(item['desired_label'], 'dev:failed')
        self.assertIn('reconciliation_retry', item)
        self.assertTrue(item['worker_uncertain'])
        self.assertEqual(len(state['outbox']), 3)

    def test_reviewer_health_uses_same_projection_and_keeps_reservation(self):
        self.record_alive()
        health = self.status(1040, probe=False)
        report = dict(id='review-task', status='UNKNOWN', turns=1, liveness=health)
        item = dict(task_id='task', review_task_id='review-task', project='test')
        state = dict(issues={'1': item})
        issue = dict(number=1, title='Task', labels=[{'name': 'dev:running'}])
        with patch.object(bridge, 'WORK_CONTEXT', (state, '1', item)), patch.object(bridge, 'save_state'):
            self.assertTrue(bridge.project_runtime_health(issue, item, state, report, 'review'))
        self.assertTrue(item['review_uncertain'])
        self.assertEqual(item['desired_label'], 'dev:failed')
        self.assertNotIn('review_status', item)  # Runtime detection does not advance review.


class RemoteRuntimeLivenessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = dict(os.environ, HOME=str(self.root))
        self.directory = self.root / 'local/task'; self.directory.mkdir(parents=True)
        self.meta = dict(id='task', project='test', machine='firefly', agent='codex',
                         remote_key='task-turn-1', workspace='/registered', runtime_generation='generation')
        write_json(self.directory / 'meta.json', self.meta)

    def rpc(self, machine, mode, key, value, context=None):
        args = [sys.executable, '-c', remote_worker.SCRIPT, mode, key, str(value)]
        if context is not None: args.append(json.dumps(context))
        return json.loads(subprocess.check_output(args, env=self.env))

    def remote_lease(self, diagnostic=None):
        record = runtime.initialize(self.directory, self.meta)
        self.meta['runtime_generation'] = record['generation']
        write_json(self.directory / 'meta.json', self.meta)
        record = runtime.heartbeat(self.directory, record['generation'], worker_pid=os.getpid(),
                                   agent_pid=os.getpid(), phase='CODING')
        health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(True, True))
        parent = self.directory.parent.parent
        leases = {'remote': {'task_id': 'task', 'machine': 'firefly'}}
        write_json(parent / 'worker_leases.json', leases)
        write_json(parent / 'worker_consistency.json', {'leases': {'remote': diagnostic} if diagnostic else {}})
        return leases, dict(liveness=record, runtime_health=health, status='RUNNING',
                           chunk='', more=False, cleanup_state='ACTIVE')

    def test_skipped_remote_lease_probe_preserves_diagnostics_until_fresh_probe(self):
        for diagnostic in (None, 'Previously classified ownership ambiguity'):
            with self.subTest(diagnostic=diagnostic):
                leases, response = self.remote_lease(diagnostic)
                parent = self.directory.parent.parent
                before = (parent / 'worker_consistency.json').read_bytes()
                with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
                     patch.object(remote_worker, 'rpc', return_value=response) as rpc, \
                     patch.object(worker_runtime.os, 'killpg') as kill:
                    self.assertEqual(devctl.task_state(self.meta, self.directory, refresh_remote=False)[0], 'UNKNOWN')
                    for _ in range(2): devctl.reconcile_workers()
                    rpc.assert_not_called()
                    self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                    self.assertEqual((parent / 'worker_consistency.json').read_bytes(), before)
                    devctl.reconcile_workers(refresh_remote=True)
                    rpc.assert_called_once()
                    self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                    self.assertEqual(read_json(parent / 'worker_consistency.json')['leases'], {})
                    devctl.reconcile_workers()
                    self.assertEqual(read_json(parent / 'worker_consistency.json')['leases'], {})
                    kill.assert_not_called()

    def test_failed_remote_probe_is_unclassified_and_preserves_diagnostic(self):
        for diagnostic in (None, 'Previously classified ownership ambiguity'):
            with self.subTest(diagnostic=diagnostic):
                leases, _ = self.remote_lease(diagnostic)
                parent = self.directory.parent.parent
                before = (parent / 'worker_consistency.json').read_bytes()
                with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
                     patch.object(remote_worker, 'rpc', side_effect=OSError('offline')):
                    devctl.reconcile_workers(refresh_remote=True)
                self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                self.assertEqual((parent / 'worker_consistency.json').read_bytes(), before)

    def test_fresh_report_and_discovery_clear_remote_lease_diagnostic(self):
        for command in (devctl.task_report, devctl.list_tasks):
            with self.subTest(command=command.__name__):
                leases, response = self.remote_lease('Previously classified ownership ambiguity')
                parent = self.directory.parent.parent
                with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
                     patch.object(devctl, 'load_projects', return_value={}), \
                     patch.object(remote_worker, 'rpc', return_value=response) as rpc, \
                     redirect_stdout(io.StringIO()) as output:
                    if command is devctl.task_report:
                        command('task', as_json=True)
                    else:
                        command(as_json=True)
                rpc.assert_called_once()
                report = json.loads(output.getvalue())
                self.assertEqual((report[0] if isinstance(report, list) else report)['status'], 'RUNNING')
                self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                self.assertEqual(read_json(parent / 'worker_consistency.json')['leases'], {})

    def test_superseded_fresh_observation_preserves_current_lease_diagnostic(self):
        leases, response = self.remote_lease('Keep newer diagnostic')
        parent = self.directory.parent.parent
        with patch.object(remote_worker, 'rpc', return_value=response):
            state, code = devctl.task_state(self.meta, self.directory)
        newer = read_json(self.directory / 'meta.json')
        newer['runtime_generation'] = 'newer-turn'
        write_json(self.directory / 'meta.json', newer)
        before = (parent / 'worker_consistency.json').read_bytes()
        with patch.object(devctl, 'DATA_DIR', self.directory.parent), patch.object(worker_runtime.os, 'killpg') as kill:
            devctl.reconcile_remote_observation(self.directory, self.meta, state, code)
        kill.assert_not_called()
        self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
        self.assertEqual((parent / 'worker_consistency.json').read_bytes(), before)
        self.assertEqual(read_json(self.directory / 'meta.json'), newer)

    def test_prepared_remote_turn_does_not_reuse_an_earlier_probe(self):
        leases, response = self.remote_lease('Keep previous diagnostic')
        parent = self.directory.parent.parent
        with patch.object(remote_worker, 'rpc', return_value=response):
            self.assertEqual(devctl.task_state(self.meta, self.directory)[0], 'RUNNING')
        for flag in ('launch_prepared', 'resume_prepared'):
            with self.subTest(flag=flag):
                self.meta[flag] = True
                devctl.save_meta(self.directory, self.meta)
                before = (parent / 'worker_consistency.json').read_bytes()
                with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
                     patch.object(remote_worker, 'rpc') as rpc:
                    state, code = devctl.task_state(self.meta, self.directory)
                    self.assertEqual(state, 'UNKNOWN')
                    devctl.reconcile_remote_observation(self.directory, self.meta, state, code)
                rpc.assert_not_called()
                self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                self.assertEqual((parent / 'worker_consistency.json').read_bytes(), before)
                self.meta.pop(flag)

    def test_terminal_ambiguity_is_not_cleared_while_final_logs_are_pending(self):
        leases, _ = self.remote_lease('Previously classified ownership ambiguity')
        parent = self.directory.parent.parent
        record = runtime.heartbeat(self.directory, self.meta['runtime_generation'], worker_pid=os.getpid(),
                                   agent_pid=os.getpid(), phase='EXITED', exit_code=0)
        health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(False, False))
        response = dict(liveness=record, runtime_health=health, status='DONE', exit_code=0,
                        cleanup_state='AMBIGUOUS', chunk='', more=True)
        with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
             patch.object(remote_worker, 'rpc', return_value=response), redirect_stdout(io.StringIO()) as output:
            devctl.task_report('task', as_json=True)
        self.assertEqual(json.loads(output.getvalue())['status'], 'RUNNING')
        self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
        self.assertIn('ambiguous', read_json(parent / 'worker_consistency.json')['leases']['remote'])

    def test_unvisited_remote_directory_is_not_an_orphan_but_missing_directory_is(self):
        leases, _ = self.remote_lease()
        parent = self.directory.parent.parent
        leases['missing'] = {'task_id': 'missing', 'machine': 'work'}
        write_json(parent / 'worker_leases.json', leases)
        with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
             patch.object(devctl.time, 'monotonic', side_effect=[0, 6]), \
             patch.object(remote_worker, 'rpc') as rpc:
            devctl.reconcile_workers()
        rpc.assert_not_called()
        self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
        self.assertEqual(set(read_json(parent / 'worker_consistency.json')['leases']), {'missing'})

    def test_remote_terminal_lease_requires_classified_cleanup_evidence(self):
        for cleanup in ('CLEANED', 'AMBIGUOUS'):
            with self.subTest(cleanup=cleanup):
                (self.directory / 'exit_code').unlink(missing_ok=True)
                leases, _ = self.remote_lease()
                parent = self.directory.parent.parent
                record = runtime.heartbeat(self.directory, self.meta['runtime_generation'], worker_pid=os.getpid(),
                                           agent_pid=os.getpid(), phase='EXITED', exit_code=0)
                health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(False, False))
                chunk = json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'complete'}}) + '\n'
                response = dict(liveness=record, runtime_health=health, status='DONE', exit_code=0,
                                cleanup_state=cleanup, chunk=base64.b64encode(chunk.encode()).decode(), more=False)
                with patch.object(devctl, 'DATA_DIR', self.directory.parent), \
                     patch.object(remote_worker, 'rpc', return_value=response), \
                     patch.object(worker_runtime.os, 'killpg') as kill:
                    devctl.reconcile_workers()
                    self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                    self.assertEqual(read_json(parent / 'worker_consistency.json')['leases'], {})
                    devctl.reconcile_workers(refresh_remote=True)
                    if cleanup == 'CLEANED':
                        self.assertEqual(read_json(parent / 'worker_leases.json'), {})
                        self.assertEqual(read_json(parent / 'worker_consistency.json')['leases'], {})
                    else:
                        self.assertEqual(read_json(parent / 'worker_leases.json'), leases)
                        self.assertIn('remote', read_json(parent / 'worker_consistency.json')['leases'])
                    before = (parent / 'worker_consistency.json').read_bytes()
                    devctl.reconcile_workers()
                    self.assertEqual((parent / 'worker_consistency.json').read_bytes(), before)
                    kill.assert_not_called()

    def test_real_remote_probe_and_restart_cached_report_are_distinct(self):
        with patch.object(remote_worker, 'rpc', side_effect=self.rpc):
            remote_worker.start('firefly', 'task-turn-1', "exec python3 -c 'import time; time.sleep(1)'", self.meta)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                state = devctl.task_state(self.meta, self.directory)[0]
                if state == 'RUNNING': break
                time.sleep(0.01)
            self.assertEqual(state, 'RUNNING')
            self.assertEqual(self.meta['_runtime_health']['state'], 'RUNNING_HEALTHY')
            restored = read_json(self.directory / 'meta.json')
            self.assertNotIn('_runtime_health', restored)
            self.assertEqual(devctl.task_state(restored, self.directory, refresh_remote=False)[0], 'UNKNOWN')
            self.assertEqual(devctl.runtime_status(restored, self.directory)['state'], 'SUSPECTED_STALL')
            # Wait for the detached test worker before deleting its temporary HOME.
            time.sleep(1.1)
            self.assertEqual(devctl.task_state(restored, self.directory)[0], 'FAILED')
            with patch.object(devctl, 'DATA_DIR', self.directory.parent), redirect_stdout(io.StringIO()) as stream:
                devctl.task_report('task', as_json=True)
            report = json.loads(stream.getvalue())
            self.assertEqual(report['status'], 'FAILED')
            self.assertEqual(report['error'], 'process_exited_without_final_report')
            self.assertEqual(devctl.task_state(restored, self.directory)[0], 'FAILED')

    def test_remote_final_report_diagnosis_waits_for_complete_log_download(self):
        record = runtime.initialize(self.directory, self.meta)
        record = runtime.heartbeat(self.directory, record['generation'], worker_pid=os.getpid(),
                                   agent_pid=os.getpid(), phase='EXITED', exit_code=0)
        health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(True, False))
        start = json.dumps({'type': 'thread.started', 'thread_id': 'remote-session'}) + '\n'
        final = json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'complete'}}) + '\n'
        responses = [dict(liveness=dict(record), runtime_health=dict(health), status='DONE', exit_code=0,
                          chunk=base64.b64encode(chunk.encode()).decode(), more=more)
                     for chunk, more in ((start, True), (final, False))]
        with patch.object(remote_worker, 'rpc', side_effect=responses):
            self.assertEqual(devctl.task_state(self.meta, self.directory), ('RUNNING', None))
            restored = read_json(self.directory / 'meta.json')
            self.assertEqual(devctl.task_state(restored, self.directory, refresh_remote=False), ('RUNNING', None))
            self.assertEqual(read_json(self.directory / 'liveness.json')['terminal_reason'], 'process_exited')
            self.assertEqual(devctl.task_state(restored, self.directory), ('DONE', 0))
        with patch.object(devctl, 'DATA_DIR', self.directory.parent), redirect_stdout(io.StringIO()) as output:
            devctl.list_tasks(as_json=True, cached=True)
        self.assertEqual(json.loads(output.getvalue())[0]['status'], 'DONE')

    def test_cached_remote_exit_without_confirmed_log_download_is_uncertain(self):
        record = runtime.initialize(self.directory, self.meta)
        runtime.heartbeat(self.directory, record['generation'], worker_pid=os.getpid(),
                          agent_pid=os.getpid(), phase='EXITED', exit_code=0)
        self.assertEqual(devctl.task_state(self.meta, self.directory, refresh_remote=False), ('UNKNOWN', None))
        self.assertEqual(read_json(self.directory / 'liveness.json')['terminal_reason'], 'process_exited')

    def test_remote_malformed_evidence_fails_closed(self):
        record = runtime.initialize(self.directory, self.meta)
        health = runtime.watchdog(self.directory, self.meta, probe=False)
        for changes in ({'worker_alive': 'yes'}, {'heartbeat_age_seconds': float('nan')},
                        {'observed_at': float('nan')}, {'generation': 'other'}):
            response = dict(liveness=record, runtime_health=dict(health, **changes), status='RUNNING')
            with self.subTest(changes=changes), patch.object(remote_worker, 'rpc', return_value=response):
                self.assertEqual(remote_worker.refresh(self.meta, self.directory), ('UNKNOWN', None))
                self.assertNotEqual(self.meta['_runtime_health']['state'], 'RUNNING_HEALTHY')

    def test_remote_clock_rollback_cannot_become_healthy_when_age_is_clamped(self):
        record = runtime.initialize(self.directory, self.meta)
        record = runtime.heartbeat(self.directory, record['generation'], worker_pid=os.getpid(),
                                   agent_pid=os.getpid(), phase='CODING')
        health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(True, True),
                                  clock=record['last_heartbeat_at'] - 1)
        response = dict(liveness=record, runtime_health=health, status='RUNNING', chunk='')
        with patch.object(remote_worker, 'rpc', return_value=response):
            self.assertEqual(devctl.task_state(self.meta, self.directory), ('UNKNOWN', None))
        self.assertEqual(self.meta['_runtime_health']['reason'], 'heartbeat_clock_uncertain')

    def test_missing_remote_liveness_cannot_be_replaced_by_status_running(self):
        runtime.initialize(self.directory, self.meta)
        with patch.object(remote_worker, 'rpc', return_value={'status': 'RUNNING', 'chunk': ''}):
            self.assertEqual(remote_worker.refresh(self.meta, self.directory), ('UNKNOWN', None))
        self.assertEqual(self.meta['_runtime_health']['state'], 'SUSPECTED_STALL')

    def test_remote_snapshot_cannot_regress_heartbeat_sequence(self):
        old = runtime.initialize(self.directory, self.meta)
        runtime.heartbeat(self.directory, old['generation'], worker_pid=os.getpid(), agent_pid=os.getpid(), phase='CODING')
        record = read_json(self.directory / 'liveness.json')
        health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(True, True))
        with patch.object(remote_worker, 'rpc', return_value=dict(liveness=old, runtime_health=health, status='RUNNING')):
            self.assertEqual(remote_worker.refresh(self.meta, self.directory), ('UNKNOWN', None))
        self.assertEqual(read_json(self.directory / 'liveness.json'), record)

    def test_remote_corrupt_mirror_is_preserved_and_fails_closed(self):
        record = runtime.initialize(self.directory, self.meta)
        runtime.heartbeat(self.directory, record['generation'], worker_pid=os.getpid(), agent_pid=os.getpid(), phase='CODING')
        record = read_json(self.directory / 'liveness.json')
        health = runtime.watchdog(self.directory, self.meta, probe=False, evidence=(True, True))
        path = self.directory / 'liveness.json'; path.write_text('[]')
        with patch.object(remote_worker, 'rpc', return_value=dict(liveness=record, runtime_health=health, status='RUNNING')):
            self.assertEqual(remote_worker.refresh(self.meta, self.directory), ('UNKNOWN', None))
        self.assertEqual(path.read_text(), '[]')
        self.assertEqual(self.meta['_runtime_health']['reason'], 'invalid_runtime_record')


if __name__ == '__main__':
    unittest.main()
