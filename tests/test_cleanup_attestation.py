"""Generation-bound local cleanup proof and explicit offline migration."""
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import devctl
from durable import read_json, write_json
import runtime_liveness as runtime
from scheduler import Limits
import worker_runtime as worker


class CleanupAttestationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / 'tasks'
        self.directory = self.data / 'historical'
        self.directory.mkdir(parents=True)
        self.meta = dict(id='historical', machine='work', agent='codex', project='test',
                         workspace='/registered', pid=99999999, status='RUNNING',
                         created_at='original-time', session_id='original-session')
        self.record = runtime.initialize(self.directory, self.meta)
        self.meta['runtime_generation'] = self.record['generation']
        self.record.update(phase='EXITED', exit_code=0, terminal_reason='process_exited',
                           worker_pid=self.meta['pid'], agent_pid=99999998,
                           worker_identity=dict(boot=worker.boot_id(), start='1'), agent_identity=None)
        write_json(self.directory / 'liveness.json', self.record)
        write_json(self.directory / 'meta.json', self.meta)
        (self.directory / 'exit_code').write_text('0')
        (self.directory / 'events.jsonl').write_text(json.dumps(dict(type='item.completed',
            item=dict(type='agent_message', text='original final result'))) + '\n')
        for fixture in (patch.object(devctl, 'DATA_DIR', self.data),
                        patch.object(Limits, 'from_env', return_value=Limits(total=1))):
            fixture.start()
            self.addCleanup(fixture.stop)

    def certify(self):
        return devctl.certify_local_absent(self.meta['id'], self.meta['runtime_generation'],
                                          control_group_reset_verified=True)

    def test_terminal_current_generation_certification_does_not_probe_or_signal(self):
        for state in ('DONE', 'FAILED'):
            for cleanup in ('ABSENT', 'CLEANED'):
                with self.subTest(state=state, cleanup=cleanup):
                    meta = dict(self.meta, cleanup_generation=self.meta['runtime_generation'],
                                cleanup_state=cleanup)
                    with patch.object(worker, 'read_json') as read, \
                         patch.object(worker, 'process_details') as details, \
                         patch.object(worker, 'session_members') as sessions, \
                         patch.object(worker, 'group_members') as groups, \
                         patch.object(worker, 'alive') as alive, \
                         patch.object(worker.os, 'killpg') as kill:
                        self.assertEqual(worker.cleanup_group(self.directory, meta, state), cleanup)
                    for probe in (read, details, sessions, groups, alive, kill):
                        probe.assert_not_called()

    def test_generation_mismatch_is_not_trusted(self):
        for generation in ('older-generation', None):
            with self.subTest(generation=generation):
                meta = dict(self.meta, cleanup_generation=generation, cleanup_state='ABSENT')
                with patch.object(worker.os, 'killpg') as kill:
                    self.assertEqual(worker.cleanup_group(self.directory, meta, 'DONE'), 'AMBIGUOUS')
                kill.assert_not_called()
        meta = dict(self.meta, cleanup_state='ABSENT')
        meta.pop('runtime_generation')
        with patch.object(worker, 'read_json', side_effect=ValueError('No ownership proof')):
            self.assertEqual(worker.cleanup_group(self.directory, meta, 'DONE'), 'AMBIGUOUS')

    def test_active_and_prepared_tasks_cannot_use_matching_certification(self):
        meta = dict(self.meta, cleanup_generation=self.meta['runtime_generation'], cleanup_state='ABSENT')
        for state, flag in (('RUNNING', None), ('UNKNOWN', None), ('STARTING', None),
                            ('DONE', 'launch_prepared'), ('FAILED', 'resume_prepared')):
            with self.subTest(state=state, flag=flag), patch.object(worker.os, 'killpg') as kill:
                candidate = dict(meta, **({flag: True} if flag else {}))
                self.assertEqual(worker.cleanup_group(self.directory, candidate, state), 'ACTIVE')
                kill.assert_not_called()

    def test_normal_absent_group_cleanup_persists_current_generation(self):
        pid = self.meta['pid']
        receipt = dict(task_id=self.meta['id'], generation=self.meta['runtime_generation'],
                       turn=1, role='coding', agent='codex', machine='work', wrapper_pid=pid,
                       wrapper_identity=self.record['worker_identity'], pgid=pid, sid=pid,
                       members=[], groups=[])
        write_json(self.directory / 'process_group.json', receipt)
        with patch.object(worker, 'process_details', return_value=None), \
             patch.object(worker, 'session_members', return_value=[]), \
             patch.object(worker, 'group_members', return_value=[]), patch.object(worker.os, 'killpg') as kill:
            self.assertEqual(worker.reconcile_task(self.directory, self.meta, 'DONE', 0), 'ABSENT')
        kill.assert_not_called()
        saved = read_json(self.directory / 'meta.json')
        self.assertEqual(saved['cleanup_generation'], saved['runtime_generation'])
        self.assertEqual(saved['cleanup_state'], 'ABSENT')
        self.assertNotIn('cleanup_proof', saved)
        # Durable success remains usable even after the ownership receipt is unavailable.
        (self.directory / 'process_group.json').unlink()
        with patch.object(worker, 'read_json') as read:
            self.assertEqual(worker.cleanup_group(self.directory, saved, 'DONE'), 'ABSENT')
        read.assert_not_called()

    def test_ambiguous_cleanup_does_not_create_or_refresh_cleanup_generation(self):
        for generation in (None, 'older-generation'):
            with self.subTest(generation=generation):
                meta = dict(self.meta)
                if generation is not None:
                    meta.update(cleanup_generation=generation, cleanup_state='CLEANED')
                self.assertEqual(worker.reconcile_task(self.directory, meta, 'DONE', 0), 'AMBIGUOUS')
                saved = read_json(self.directory / 'meta.json')
                self.assertEqual(saved['cleanup_state'], 'AMBIGUOUS')
                if generation is None:
                    self.assertNotIn('cleanup_generation', saved)
                else:
                    self.assertEqual(saved['cleanup_generation'], generation)

    def test_historical_terminal_runtime_without_group_receipt_remains_ambiguous(self):
        self.assertEqual(devctl.task_state(self.meta, self.directory), ('DONE', 0))
        # Neither missing identities, bound but gone direct PIDs, nor zero PIDs
        # can establish absence of descendants from a completed runtime.
        for mode in ('null-agent-identity', 'bound-identities', 'zero-pids'):
            with self.subTest(mode=mode):
                record = dict(self.record)
                if mode == 'bound-identities':
                    record['agent_identity'] = record['worker_identity']
                elif mode == 'zero-pids':
                    record.update(worker_pid=0, agent_pid=0, worker_identity=None, agent_identity=None)
                write_json(self.directory / 'liveness.json', record)
                with patch.object(worker.os, 'killpg') as kill:
                    self.assertEqual(worker.cleanup_group(self.directory, self.meta, 'DONE'), 'AMBIGUOUS')
                    devctl.reconcile_workers()
                kill.assert_not_called()
                saved = read_json(self.directory / 'meta.json')
                self.assertEqual(saved['cleanup_state'], 'AMBIGUOUS')
                self.assertNotIn('cleanup_generation', saved)

    def test_explicit_certification_releases_capacity_and_preserves_original_evidence(self):
        with self.assertRaisesRegex(RuntimeError, 'Worker capacity'):
            worker.check_capacity(self.data, 'next', 'work', 'coding', devctl.task_state)
        original = {name: (self.directory / name).read_bytes()
                    for name in ('liveness.json', 'exit_code', 'events.jsonl')}
        with patch.object(worker.os, 'killpg') as kill:
            self.certify()
            worker.check_capacity(self.data, 'next', 'work', 'coding', devctl.task_state)
        kill.assert_not_called()
        saved = read_json(self.directory / 'meta.json')
        self.assertEqual(saved['cleanup_generation'], self.meta['runtime_generation'])
        self.assertEqual(saved['cleanup_proof'], 'systemd_control_group_reset')
        self.assertEqual(saved['created_at'], 'original-time')
        self.assertEqual(saved['session_id'], 'original-session')
        self.assertEqual(original, {name: (self.directory / name).read_bytes() for name in original})

    def test_explicit_certification_releases_only_the_bound_worker_lease(self):
        leases = {'bound': dict(task_id='historical', machine='work'),
                  'unbound': dict(task_id='historical', machine='firefly')}
        path = self.data.parent / 'worker_leases.json'
        write_json(path, leases)
        devctl.reconcile_workers()
        self.assertEqual(read_json(path), leases)
        self.certify()
        devctl.reconcile_workers()
        self.assertEqual(read_json(path), {'unbound': leases['unbound']})
        diagnostics = read_json(self.data.parent / 'worker_consistency.json')['leases']
        self.assertEqual(set(diagnostics), {'unbound'})

    def test_repeated_certified_reconciliation_is_idempotent(self):
        write_json(self.data.parent / 'worker_leases.json', {'lease': dict(task_id='historical', machine='work')})
        devctl.reconcile_workers()
        self.certify()
        devctl.reconcile_workers()
        paths = [self.directory / 'meta.json', self.data.parent / 'worker_leases.json',
                 self.data.parent / 'worker_consistency.json']
        before = {path: path.read_bytes() for path in paths}
        with patch.object(worker.os, 'killpg') as kill, patch.object(worker, 'write_json') as write:
            for _ in range(3):
                devctl.reconcile_workers()
                worker.check_capacity(self.data, 'next', 'work', 'coding', devctl.task_state)
        kill.assert_not_called()
        write.assert_not_called()
        self.assertEqual(before, {path: path.read_bytes() for path in paths})

    def test_new_runtime_generation_cannot_reuse_local_certification(self):
        self.meta = self.certify()
        previous = self.meta['cleanup_generation']
        with patch.object(devctl, 'spawn_detached'), patch.object(devctl, 'project_for_task', return_value='test'):
            devctl.spawn_runtime(self.directory, self.meta, 'true', io.BytesIO())
        self.assertNotEqual(self.meta['runtime_generation'], previous)
        self.assertEqual(self.meta['cleanup_generation'], previous)
        record = runtime.read_record(self.directory)
        record.update(phase='EXITED', exit_code=0, terminal_reason='process_exited')
        write_json(self.directory / 'liveness.json', record)
        with patch.object(worker.os, 'killpg') as kill:
            self.assertEqual(worker.cleanup_group(self.directory, self.meta, 'DONE'), 'AMBIGUOUS')
        kill.assert_not_called()

    def test_operator_helper_requires_explicit_reset_assertion_and_exact_generation(self):
        original = (self.directory / 'meta.json').read_bytes()
        with self.assertRaisesRegex(ValueError, 'verified complete MCP control-group reset'):
            devctl.certify_local_absent('historical', self.meta['runtime_generation'])
        for generation in ('older-generation', '', None):
            with self.subTest(generation=generation), self.assertRaisesRegex(ValueError, 'generation'):
                devctl.certify_local_absent('historical', generation, control_group_reset_verified=True)
        self.assertEqual((self.directory / 'meta.json').read_bytes(), original)

    def test_operator_helper_refuses_nonterminal_tasks(self):
        self.record.update(phase='CODING', exit_code=None, terminal_reason=None,
                           worker_pid=os.getpid(), agent_pid=os.getpid(),
                           worker_identity=worker.process_identity(os.getpid()),
                           agent_identity=worker.process_identity(os.getpid()), heartbeat_sequence=1)
        write_json(self.directory / 'liveness.json', self.record)
        self.assertEqual(devctl.task_state(self.meta, self.directory), ('RUNNING', None))
        original = (self.directory / 'meta.json').read_bytes()
        with self.assertRaisesRegex(ValueError, 'terminal task'):
            self.certify()
        self.assertEqual((self.directory / 'meta.json').read_bytes(), original)

    def test_local_certification_cannot_replace_remote_cleanup_contract(self):
        meta = dict(self.meta, remote_key='remote-turn', machine='firefly',
                    cleanup_generation=self.meta['runtime_generation'], cleanup_state='ABSENT')
        with patch.object(worker.os, 'killpg') as kill:
            self.assertEqual(worker.cleanup_group(self.directory, meta, 'DONE'), 'AMBIGUOUS')
            legacy_remote = dict(meta)
            legacy_remote.pop('remote_key')
            self.assertEqual(worker.cleanup_group(self.directory, legacy_remote, 'DONE'), 'AMBIGUOUS')
            meta.pop('cleanup_generation')
            for cleanup in ('ABSENT', 'CLEANED'):
                meta.update(remote_cleanup_generation=meta['runtime_generation'], remote_cleanup_state=cleanup)
                self.assertEqual(worker.reconcile_task(self.directory, meta, 'DONE', 0), cleanup)
                self.assertNotIn('cleanup_generation', read_json(self.directory / 'meta.json'))
        kill.assert_not_called()
        write_json(self.directory / 'meta.json', meta)
        with self.assertRaisesRegex(ValueError, 'local task'):
            self.certify()


if __name__ == '__main__':
    unittest.main()
