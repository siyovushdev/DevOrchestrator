import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import devctl
from durable import write_json
import remote_worker as r
from worker_runtime import process_identity
from tests_helpers import proc_fixture


class RemoteWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = dict(os.environ, HOME=str(self.root))

    def rpc(self, mode, key='task-turn-1', value='0'):
        return json.loads(subprocess.check_output(['python3', '-c', r.SCRIPT, mode, key, value], env=self.env))

    def test_detached_launch_transport_loss_and_idempotent_replay(self):
        sentinel = self.root / 'count'
        command = f"python3 -c 'import pathlib,time; p=pathlib.Path(\"{sentinel}\"); p.write_text(p.read_text()+\"x\" if p.exists() else \"x\"); print(\"final result\"); time.sleep(0.1)'"
        first = self.rpc('start', value=command)
        second = self.rpc('start', value=command)
        self.assertEqual(first['pid'], second['pid'])
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            report = self.rpc('report')
            if report['status'] == 'DONE': break
            time.sleep(0.02)
        self.assertEqual(report['status'], 'DONE')
        self.assertEqual(sentinel.read_text(), 'x')
        self.assertIn('final result', __import__('base64').b64decode(report['chunk']).decode())

    def test_remote_reboot_and_ambiguous_start_are_safe(self):
        directory = self.root / '.local/share/devorchestrator/remote-tasks/task-turn-1'; directory.mkdir(parents=True)
        write_json(directory / 'meta.json', {'pid': 123, 'boot': 'old-boot', 'start': '1'})
        self.assertEqual(self.rpc('report')['status'], 'FAILED')
        write_json(directory / 'meta.json', {'pid': 0, 'boot': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'start': None})
        self.assertEqual(self.rpc('report')['status'], 'UNKNOWN')
        self.rpc('start', value='touch ' + str(self.root / 'should-not-run'))
        self.assertFalse((self.root / 'should-not-run').exists())

    def test_network_error_is_unknown_not_failed(self):
        directory = self.root / 'task'; directory.mkdir()
        meta = {'remote_key': 'task-turn-1', 'machine': 'firefly'}; write_json(directory / 'meta.json', meta)
        with patch.object(r, 'rpc', side_effect=OSError()):
            self.assertEqual(r.refresh(meta, directory), ('UNKNOWN', None))

    def test_remote_reused_session_leader_start_mismatch_is_failed(self):
        leader = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
        try:
            self.assertEqual(os.getsid(leader.pid), leader.pid)
            self.assertEqual(os.getpgid(leader.pid), leader.pid)
            identity = process_identity(leader.pid)
            directory = self.root / '.local/share/devorchestrator/remote-tasks/task-turn-1'
            directory.mkdir(parents=True)
            meta = dict(pid=leader.pid, **identity)
            write_json(directory / 'meta.json', meta)
            self.assertEqual(self.rpc('report')['status'], 'RUNNING')
            meta['start'] = str(int(meta['start']) + 1)
            write_json(directory / 'meta.json', meta)
            self.assertEqual(self.rpc('report')['status'], 'FAILED')
            self.assertIsNone(leader.poll())
        finally:
            leader.terminate(); leader.wait()

    def test_remote_child_scan_only_when_leader_pid_is_gone(self):
        proc = proc_fixture(self.root)
        directory = self.root / '.local/share/devorchestrator/remote-tasks/task-turn-1'
        directory.mkdir(parents=True)
        write_json(directory / 'meta.json', {'pid': 123, 'boot': 'test-boot', 'start': '10'})
        # Substitute only proc filesystem roots; execute the actual report script.
        script = r.SCRIPT.replace("'/proc", "'" + str(proc))
        with patch.object(r, 'SCRIPT', script):
            self.assertEqual(self.rpc('report')['status'], 'FAILED')
            (proc / '123/stat').unlink(); (proc / '123').rmdir()
            self.assertEqual(self.rpc('report')['status'], 'RUNNING')

    def test_log_offset_restart_and_incomplete_final_download(self):
        import base64
        directory = self.root / 'task'; directory.mkdir()
        meta = {'remote_key': 'task-turn-1', 'machine': 'firefly'}; write_json(directory / 'meta.json', meta)
        responses = [{'status': 'DONE', 'chunk': base64.b64encode(b'first\n').decode(), 'exit_code': 0, 'more': True},
                     {'status': 'DONE', 'chunk': base64.b64encode(b'final\n').decode(), 'exit_code': 0, 'more': False}]
        with patch.object(r, 'rpc', side_effect=responses):
            self.assertEqual(r.refresh(meta, directory), ('RUNNING', None))
            self.assertEqual(r.refresh(meta, directory), ('DONE', 0))
        self.assertEqual((directory / 'events.jsonl').read_text(), 'first\nfinal\n')
        self.assertEqual(meta['remote_offset'], 12)
        self.assertEqual(meta['remote_status'], 'DONE')

    def test_remote_launch_receipts_fsync_directory_before_returning(self):
        sentinel = self.root / 'durable-receipts'
        audit = f'''
import stat
original_fsync = os.fsync
def audited_fsync(fd):
    original_fsync(fd)
    if stat.S_ISDIR(os.fstat(fd).st_mode):
        receipt = pathlib.Path({str(sentinel)!r})
        receipt.write_text((receipt.read_text() if receipt.exists() else '') + 'x')
os.fsync = audited_fsync
'''
        script = r.SCRIPT.replace('mode, key = sys.argv[1:3]', audit + '\nmode, key = sys.argv[1:3]')
        with patch.object(r, 'SCRIPT', script): self.rpc('start', value='true')
        self.assertEqual(sentinel.read_text(), 'xx')  # Both pre-launch intent and PID receipt.
