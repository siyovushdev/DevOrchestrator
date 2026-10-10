"""Request reconciliation never converts absence or cancellation into success."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from durable import exclusive, read_json, write_json
import worker_runtime as w


class RequestProtocolTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / 'tasks'
        self.key = 'a' * 64
        self.task = self.key[:24]
        self.receipt = self.data.parent / 'requests' / (self.key + '.json')

    def inspect(self, fence=False):
        return w.inspect_start_request(self.data, self.key, fence=fence)

    def test_absence_does_not_claim_no_launch_or_create_receipt(self):
        result = self.inspect()
        self.assertEqual(result['state'], 'ABSENT')
        self.assertNotIn('launch_started', result)
        self.assertFalse(self.receipt.exists())

    def test_fence_blocks_delayed_launch_and_repeated_fence_does_not_rewrite(self):
        result = self.inspect(fence=True)
        self.assertEqual(result['state'], 'FENCED')
        self.assertIs(result['launch_started'], False)
        before = self.receipt.stat()
        self.assertEqual(self.inspect(fence=True), result)
        self.assertEqual(self.receipt.stat().st_ino, before.st_ino)
        self.assertEqual(self.receipt.stat().st_mtime_ns, before.st_mtime_ns)
        prepare, start = Mock(), Mock()
        with self.assertRaises(w.RequestFenced):
            w.launch(self.data, self.key, self.task, start, prepare, signature='signature')
        prepare.assert_not_called()
        start.assert_not_called()

    def test_orphan_metadata_directory_or_lease_cannot_be_fenced(self):
        for kind in ('metadata', 'directory', 'lease', 'archive'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as root:
                data = Path(root) / 'tasks'
                if kind == 'lease':
                    write_json(data.parent / 'worker_leases.json', {'repo': {'task_id': self.task}})
                else:
                    directory = data / 'archives' / self.task if kind == 'archive' else data / self.task
                    directory.mkdir(parents=True)
                    if kind != 'directory':
                        write_json(directory / 'meta.json', {'id': self.task, 'launch_prepared': True})
                result = w.inspect_start_request(data, self.key, fence=True)
                self.assertEqual(result['state'], 'UNKNOWN')
                self.assertNotIn('launch_started', result)
                self.assertFalse((data.parent / 'requests' / (self.key + '.json')).exists())

    def test_busy_lock_never_waits_or_fences(self):
        for path in (self.data.parent / 'worker.lock', self.receipt.with_suffix('.lock')):
            with self.subTest(path=path), exclusive(path):
                self.assertEqual(self.inspect(fence=True)['state'], 'BUSY')
                self.assertFalse(self.receipt.exists())

    def test_accepted_and_prepared_receipts_are_never_fenced_or_relaunched(self):
        for status in ('PENDING', 'STARTED'):
            with self.subTest(status=status):
                write_json(self.receipt, {'task_id': self.task, 'status': status,
                                          'signature': 'signature', 'signature_version': 2})
                write_json(self.data / self.task / 'meta.json', dict(id=self.task, role='review',
                    runtime_generation='generation', launch_prepared=status == 'PENDING',
                    prompt='private payload', remote_command='secret command'))
                before = self.receipt.read_bytes()
                result = self.inspect(fence=True)
                self.assertEqual(result['state'], status)
                self.assertEqual(self.receipt.read_bytes(), before)
                self.assertNotIn('private payload', json.dumps(result))
                self.assertNotIn('secret command', json.dumps(result))
                start = Mock()
                with redirect_stdout(io.StringIO()):
                    w.launch(self.data, self.key, self.task, start, signature='signature', signature_version=2)
                start.assert_not_called()

    def test_corrupt_receipt_returns_payload_free_uncertainty(self):
        self.receipt.parent.mkdir(parents=True)
        self.receipt.write_text('private token malformed json')
        result = self.inspect(fence=True)
        self.assertEqual(result['state'], 'UNKNOWN')
        self.assertNotIn('private', json.dumps(result))
        self.assertEqual(self.receipt.read_text(), 'private token malformed json')

    def test_crash_after_pending_receipt_cannot_trigger_second_callback(self):
        def prepare():
            write_json(self.data / self.task / 'meta.json', {'id': self.task, 'role': 'review'})
        start = Mock(side_effect=OSError('lost response'))
        with self.assertRaises(OSError):
            w.launch(self.data, self.key, self.task, start, prepare, signature='signature')
        self.assertEqual(self.inspect()['state'], 'PENDING')
        self.assertEqual(self.inspect(fence=True)['state'], 'PENDING')
        with redirect_stdout(io.StringIO()):
            w.launch(self.data, self.key, self.task, start, prepare, signature='signature')
        self.assertEqual(start.call_count, 1)
        self.assertEqual(read_json(self.receipt)['status'], 'PENDING')


if __name__ == '__main__':
    unittest.main()
