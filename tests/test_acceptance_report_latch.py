"""Latched MCP reports must not signal, spawn, SSH, or write worker state.

The registered tool handler is the dispatch under test. One case also lets
that handler start the real devctl subprocess, with the latch in the inherited
environment. killpg is mocked anywhere a signal would otherwise be required to
prove the branch.
"""
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

import devctl
import mcp_server
import remote_worker
import runtime_liveness
import task_bridge as bridge
import worker_runtime
from durable import write_json
from external_effects import ENV, ExternalEffectBlocked


def snapshot(root):
    found = {}
    if root.exists():
        for path in root.rglob('*'):
            if path.is_file():
                found[str(path.relative_to(root))] = path.read_bytes()
    return found


def sleeper():
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(120)'],
        start_new_session=True, stdin=subprocess.DEVNULL)
    return process


def stop(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def identity(pid):
    return worker_runtime.process_identity(pid)


def receipt_for(process, task_id, generation, machine='work'):
    found = identity(process.pid)
    return dict(
        task_id=task_id, generation=generation, turn=1, role='coding', agent='codex',
        machine=machine, wrapper_pid=process.pid, wrapper_identity=found,
        pgid=process.pid, sid=process.pid,
        members=[{'pid': process.pid, 'identity': found}], groups=[])


class ReportLatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.previous_home = os.environ.get('HOME')
        self.previous_latch = os.environ.get(ENV)
        os.environ['HOME'] = str(self.home)
        self.addCleanup(self.restore_env)

    def restore_env(self):
        if self.previous_home is None:
            os.environ.pop('HOME', None)
        else:
            os.environ['HOME'] = self.previous_home
        if self.previous_latch is None:
            os.environ.pop(ENV, None)
        else:
            os.environ[ENV] = self.previous_latch

    def latch(self, enabled=True):
        if enabled:
            os.environ[ENV] = '1'
        else:
            os.environ.pop(ENV, None)

    def share(self):
        root = self.home / '.local/share/devorchestrator'
        root.mkdir(parents=True, exist_ok=True)
        return root

    def terminal_task(self, task_id='terminal-task', *, remote=False):
        process = sleeper()
        self.addCleanup(stop, process)
        root = self.share()
        directory = root / 'tasks' / task_id
        directory.mkdir(parents=True)
        meta = dict(
            id=task_id, project='test', machine='firefly' if remote else 'work',
            agent='codex', workspace='/registered', role='coding', turns=1,
            status='RUNNING', pid=process.pid, created_at='original', updated_at='original',
            session_id='original-session')
        if remote:
            meta.update(remote_key='remote-key', remote_status='RUNNING')
        record = runtime_liveness.initialize(directory, meta)
        meta['runtime_generation'] = record['generation']
        found = identity(process.pid)
        record.update(
            phase='EXITED', exit_code=0, terminal_reason='process_exited',
            worker_pid=process.pid, agent_pid=0, worker_identity=found, agent_identity=None)
        write_json(directory / 'liveness.json', record)
        write_json(directory / 'meta.json', meta)
        write_json(directory / 'process_group.json', receipt_for(
            process, task_id, record['generation'], meta['machine']))
        (directory / 'exit_code').write_text('0')
        (directory / 'events.jsonl').write_text(json.dumps({
            'type': 'item.completed',
            'item': {'type': 'agent_message', 'text': 'original final result'}}) + '\n')
        write_json(root / 'worker_leases.json', {'slot': {'task_id': task_id, 'machine': meta['machine']}})
        write_json(root / 'worker_consistency.json', {'leases': {}})
        return process, task_id, root

    def exec_devctl(self, argv):
        source = Path(argv[0]).read_text()
        namespace = {'__name__': '__main__', '__file__': str(argv[0])}
        previous = sys.argv
        sys.argv = [str(part) for part in argv]
        stdout = StringIO()
        code = 0
        try:
            with redirect_stdout(stdout):
                try:
                    exec(compile(source, str(argv[0]), 'exec'), namespace)
                except SystemExit as exc:
                    code = exc.code if isinstance(exc.code, int) else 0
        finally:
            sys.argv = previous
        if code != 0:
            raise AssertionError('devctl exited %s' % code)
        return subprocess.CompletedProcess(argv, code, stdout.getvalue(), '')

    def launcher(self, argv, **kwargs):
        try:
            if tuple(argv[1:4]) == ('tasks', '--json', '--cached'):
                projects = {'test': {'machine': 'work', 'workspace': '/registered'}}
                stdout = StringIO()
                with patch.object(devctl, 'DATA_DIR', self.share() / 'tasks'), \
                        patch.object(devctl, 'load_projects', return_value=projects), \
                        redirect_stdout(stdout):
                    devctl.list_tasks(True, False, True)
                return subprocess.CompletedProcess(argv, 0, stdout.getvalue(), '')
            return self.exec_devctl(argv)
        except ExternalEffectBlocked as exc:
            return subprocess.CompletedProcess(argv, 1, exc.kind, '')

    async def dispatched(self, name, arguments):
        with patch.object(mcp_server.subprocess, 'run', self.launcher):
            result = await mcp_server.mcp.call_tool(name, arguments)
        self.assertFalse(getattr(result, 'is_error', False) or getattr(result, 'isError', False))
        return bridge.decode_mcp_result(name, result)

    async def test_registered_report_with_mocked_killpg_does_not_signal_or_write(self):
        self.latch()
        process, task_id, root = self.terminal_task()
        before = snapshot(root)
        with patch.object(worker_runtime.os, 'killpg') as kill, \
                patch('subprocess.Popen', side_effect=AssertionError('popen')), \
                patch('subprocess.run', side_effect=AssertionError('run')), \
                patch.object(remote_worker, 'rpc', side_effect=AssertionError('ssh')):
            report = await self.dispatched('get_task_report', {'task_id': task_id})
            listed = await self.dispatched('list_tasks', {})
        kill.assert_not_called()
        self.assertEqual(report['status'], 'DONE')
        self.assertIsNone(report.get('cleanup_state'))
        self.assertEqual(report['result'], 'original final result')
        self.assertEqual([row['id'] for row in listed], [task_id])
        self.assertEqual(snapshot(root), before)
        self.assertIsNone(process.poll())

    async def test_real_mcp_subprocess_inherits_latch_and_does_not_signal(self):
        self.latch()
        process, task_id, root = self.terminal_task()
        before = snapshot(root)
        result = await mcp_server.mcp.call_tool('get_task_report', {'task_id': task_id})
        report = bridge.decode_mcp_result('get_task_report', result)
        self.assertEqual(report['status'], 'DONE')
        self.assertIsNone(report.get('cleanup_state'))
        self.assertEqual(snapshot(root), before)
        self.assertIsNone(process.poll())

    async def test_unlatched_same_receipt_still_reaches_killpg(self):
        self.latch(False)
        process, task_id, root = self.terminal_task()
        with patch.object(devctl, 'DATA_DIR', root / 'tasks'), \
                patch.object(worker_runtime.os, 'killpg') as kill, \
                patch('subprocess.Popen', side_effect=AssertionError('popen')):
            with redirect_stdout(StringIO()):
                devctl.task_report(task_id, as_json=True)
        self.assertTrue(any(call.args[1] == signal.SIGTERM for call in kill.call_args_list))
        self.assertIsNone(process.poll())

    async def test_remote_report_does_not_ssh_or_signal(self):
        self.latch()
        process, task_id, root = self.terminal_task(remote=True)
        before = snapshot(root)
        with patch.object(worker_runtime.os, 'killpg') as kill, \
                patch('subprocess.Popen', side_effect=AssertionError('popen')), \
                patch('subprocess.run', side_effect=AssertionError('run')), \
                patch.object(remote_worker, 'rpc', side_effect=AssertionError('ssh')):
            report = await self.dispatched('get_task_report', {'task_id': task_id})
        kill.assert_not_called()
        self.assertEqual(report['status'], 'DONE')
        self.assertEqual(snapshot(root), before)
        self.assertIsNone(process.poll())

    def test_remote_script_killpg_is_mocked_and_skipped_when_latched(self):
        process = sleeper()
        self.addCleanup(stop, process)
        key = 'remote-key'
        root = self.share() / 'remote-tasks' / key
        root.mkdir(parents=True)
        found = identity(process.pid)
        meta = dict(id=key, project='test', machine='firefly', agent='codex', role='coding', turns=1)
        record = runtime_liveness.initialize(root, meta)
        record.update(phase='EXITED', exit_code=0, terminal_reason='process_exited',
                      worker_pid=process.pid, agent_pid=0, worker_identity=found)
        write_json(root / 'liveness.json', record)
        write_json(root / 'meta.json', dict(pid=process.pid, boot=found['boot'], start=found['start'], state='RUNNING'))
        write_json(root / 'process_group.json', receipt_for(process, key, record['generation'], 'firefly'))
        (root / 'exit_code').write_text('0')
        log = self.home / 'killpg.log'
        wrapper = textwrap.dedent('''
            import os
            def killpg(pgid, sig):
                with open(os.environ["KILL_LOG"], "a") as stream:
                    stream.write("%s %s\\n" % (pgid, int(sig)))
            os.killpg = killpg
        ''')
        def run(latched):
            log.write_text('')
            env = dict(os.environ, HOME=str(self.home), KILL_LOG=str(log))
            if latched:
                env[ENV] = '1'
            else:
                env.pop(ENV, None)
            completed = subprocess.run(
                [sys.executable, '-c', wrapper + remote_worker.SCRIPT, 'report', key, '0'],
                env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            return json.loads(completed.stdout), log.read_text()
        quiet, quiet_log = run(True)
        self.assertEqual(quiet.get('cleanup_state'), 'ACTIVE')
        self.assertEqual(quiet_log, '')
        self.assertIsNone(process.poll())
        noisy, noisy_log = run(False)
        self.assertIn(str(process.pid), noisy_log)
        self.assertIn(str(int(signal.SIGTERM)), noisy_log)
        self.assertNotEqual(noisy.get('cleanup_state'), 'CLEANED')
        self.assertIsNone(process.poll())

    async def test_other_read_tools_do_not_probe(self):
        self.latch()

        calls = []

        def guarded(argv=None, **kwargs):
            args = argv if argv is not None else kwargs.get('args')
            calls.append(args)
            if isinstance(args, (list, tuple)) and args and str(args[0]).endswith('devctl.py'):
                return self.launcher(args, **kwargs)
            raise AssertionError('run')

        with patch('subprocess.Popen', side_effect=AssertionError('popen')) as popen, \
                patch('subprocess.run', guarded):
            capabilities = await mcp_server.mcp.call_tool('get_transition_capabilities', {})
            decoded = bridge.decode_mcp_result('get_transition_capabilities', capabilities)
            self.assertEqual(decoded['schema_version'], 1)
            with self.assertRaises(Exception) as health:
                await mcp_server.mcp.call_tool('orchestrator_health', {})
            self.assertIsInstance(health.exception.__cause__, ExternalEffectBlocked)
            for name, arguments in (
                    ('get_execution_catalog', {'discover': True, 'project': 'test'}),
                    ('preflight_task', {'operation': 'start_task', 'arguments': {
                        'project': 'test', 'role': 'coding'}})):
                with self.assertRaises(Exception) as blocked:
                    await mcp_server.mcp.call_tool(name, arguments)
                self.assertIn('worker', str(blocked.exception.__cause__))
        popen.assert_not_called()
        self.assertTrue(calls)
        self.assertTrue(all(isinstance(args, (list, tuple)) and str(args[0]).endswith('devctl.py') for args in calls))

    async def test_bridge_keeps_ownership_without_retry_approval_or_failed(self):
        self.latch()
        process, task_id, root = self.terminal_task()
        before = snapshot(root)
        item = {'task_id': task_id, 'project': 'test'}
        state = {'issues': {'7': item}, 'work_queue': {}, 'outbox': {}}
        issue = {'number': 7, 'title': 'Task', 'labels': ['dev:running'], 'state': 'OPEN'}
        projects = {'test': {'machine': 'work', 'workspace': '/registered'}}

        async def io_call(kind, payload, lane=None):
            if kind == 'comments':
                return []
            if kind == 'rpc' and payload.get('name') == 'get_task_report':
                return await self.dispatched('get_task_report', payload['arguments'])
            raise AssertionError(kind)

        with patch.object(worker_runtime.os, 'killpg') as kill, \
                patch('subprocess.Popen', side_effect=AssertionError('popen')), \
                patch('subprocess.run', side_effect=AssertionError('run')), \
                patch.object(devctl, 'load_projects', return_value=projects), \
                patch.object(bridge, 'save_state'), \
                patch.object(bridge, 'io_call', io_call), \
                patch.object(bridge, 'gh', side_effect=AssertionError('github')), \
                patch.object(bridge, 'send_telegram', side_effect=AssertionError('telegram')):
            await bridge.process_tracked(issue, item, state)
        kill.assert_not_called()
        self.assertNotIn('reconciliation_retry', item)
        self.assertNotEqual(item.get('desired_label'), 'dev:failed')
        self.assertNotEqual(item.get('queue_status'), 'FAILED')
        self.assertIsNone(item.get('owner_state'))
        self.assertFalse(item.get('approval_complete', False))
        self.assertIsNone(item.get('publication_journal'))
        self.assertEqual(snapshot(root), before)
        self.assertIsNone(process.poll())
        for entry in state.get('outbox', {}).values():
            self.assertEqual(entry.get('status'), 'PENDING')
            self.assertNotEqual(entry.get('payload', {}).get('target') if isinstance(entry.get('payload'), dict) else None,
                                'dev:failed')


if __name__ == '__main__':
    unittest.main()
