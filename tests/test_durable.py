import tempfile
import unittest
from pathlib import Path
from unittest import mock

import durable


class WriteJsonIfChangedTests(unittest.TestCase):

    def test_missing_file_is_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = {"value": 1, "nested": {"ok": True}}

            changed = durable.write_json_if_changed(path, data)

            self.assertTrue(changed)
            self.assertTrue(path.exists())
            self.assertEqual(durable.read_json(path), data)

    def test_identical_state_does_not_rewrite_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = {"value": 1, "items": [1, 2, 3]}

            self.assertTrue(
                durable.write_json_if_changed(path, data)
            )

            before_bytes = path.read_bytes()
            before = path.stat()

            with mock.patch.object(
                durable,
                "write_json",
                wraps=durable.write_json,
            ) as write_json:
                changed = durable.write_json_if_changed(path, data)

            after = path.stat()

            self.assertFalse(changed)
            write_json.assert_not_called()

            # No atomic replacement happened.
            self.assertEqual(before.st_ino, after.st_ino)
            self.assertEqual(before_bytes, path.read_bytes())

    def test_changed_state_is_rewritten_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"

            old = {"revision": 1, "status": "RUNNING"}
            new = {"revision": 2, "status": "DONE"}

            self.assertTrue(
                durable.write_json_if_changed(path, old)
            )

            with mock.patch.object(
                durable,
                "write_json",
                wraps=durable.write_json,
            ) as write_json:
                changed = durable.write_json_if_changed(path, new)

            self.assertTrue(changed)
            write_json.assert_called_once()
            self.assertEqual(durable.read_json(path), new)

    def test_same_changed_state_is_not_written_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = {"revision": 2, "status": "DONE"}

            self.assertTrue(
                durable.write_json_if_changed(path, data)
            )

            with mock.patch.object(
                durable,
                "write_json",
                wraps=durable.write_json,
            ) as write_json:
                changed = durable.write_json_if_changed(path, data)

            self.assertFalse(changed)
            write_json.assert_not_called()

    def test_one_hundred_identical_saves_write_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = {
                "revision": 42,
                "status": "RUNNING",
                "payload": "x" * 65536,
            }

            with mock.patch.object(
                durable,
                "write_json",
                wraps=durable.write_json,
            ) as write_json:
                results = [
                    durable.write_json_if_changed(path, data)
                    for _ in range(100)
                ]

            self.assertEqual(results.count(True), 1)
            self.assertEqual(results.count(False), 99)
            self.assertEqual(write_json.call_count, 1)
            self.assertEqual(durable.read_json(path), data)

    def test_existing_write_json_contract_is_compatible(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = {
                "unicode": "Привет",
                "nested": {"a": [1, 2, 3]},
            }

            durable.write_json(path, data)

            # A file produced by the old durable writer must be recognized
            # as identical by the new no-op-aware writer.
            with mock.patch.object(
                durable,
                "write_json",
                wraps=durable.write_json,
            ) as write_json:
                changed = durable.write_json_if_changed(path, data)

            self.assertFalse(changed)
            write_json.assert_not_called()
            self.assertEqual(durable.read_json(path), data)


if __name__ == "__main__":
    unittest.main()
