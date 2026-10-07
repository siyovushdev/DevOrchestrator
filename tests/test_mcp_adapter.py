"""Adapter contract tests without importing/running the installed live MCP runtime."""
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


class AdapterTests(unittest.TestCase):
    def setUp(self):
        class Server:
            def __init__(self, name): pass
            def tool(self): return lambda fn: fn
        package = types.ModuleType('mcp'); server = types.ModuleType('mcp.server'); server.MCPServer = Server
        spec = importlib.util.spec_from_file_location('isolated_mcp_adapter', Path(__file__).resolve().parents[1] / 'mcp_server.py')
        self.adapter = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'mcp': package, 'mcp.server': server}): spec.loader.exec_module(self.adapter)
        self.registry = patch.object(self.adapter, 'load_projects', return_value={'test': {'machine': 'work', 'default_agent': 'codex'}})
        self.registry.start(); self.addCleanup(self.registry.stop)

    def test_start_preserves_real_report_on_receipt_replay(self):
        with patch.object(self.adapter, 'devctl', side_effect=['task\n', json.dumps({'id': 'task', 'status': 'DONE', 'turns': 1})]) as run:
            result = self.adapter.start_task('test', 'prompt', request_id='a' * 64, role='review')
        self.assertEqual(result['status'], 'DONE')
        self.assertIn('--request-id', run.call_args_list[0].args)
        self.assertEqual(run.call_args_list[0].args[-2:], ('--role', 'review'))

    def test_continue_passes_receipt_and_returns_report(self):
        with patch.object(self.adapter, 'devctl', side_effect=['task', json.dumps({'id': 'task', 'status': 'RUNNING', 'turns': 2})]) as run:
            result = self.adapter.continue_task('task', 'answer', 'b' * 64)
        self.assertEqual(result['turns'], 2)
        self.assertEqual(run.call_args_list[0].args[-2:], ('--request-id', 'b' * 64))

    def test_unregistered_project_cannot_start(self):
        with patch.object(self.adapter, 'devctl') as run:
            with self.assertRaises(ValueError): self.adapter.start_task('/arbitrary', 'prompt')
            run.assert_not_called()

    def test_list_does_not_wait_on_remote_network(self):
        with patch.object(self.adapter, 'devctl', return_value='[]') as run:
            self.assertEqual(self.adapter.list_tasks(), [])
        run.assert_called_once_with('tasks', '--json', '--cached')
