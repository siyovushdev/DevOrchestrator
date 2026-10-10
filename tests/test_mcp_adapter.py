"""Adapter contract tests without importing/running the installed live MCP runtime."""
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
import asyncio
import threading
import worker_runtime  # Keep exception identity stable across the isolated SDK import.
from unittest.mock import patch


class AdapterTests(unittest.TestCase):
    def setUp(self):
        handlers = {}
        class Server:
            def __init__(self, name): pass
            def tool(self):
                def register(fn):
                    handlers[fn.__name__] = fn
                    return fn
                return register
        package = types.ModuleType('mcp'); server = types.ModuleType('mcp.server'); server.MCPServer = Server
        spec = importlib.util.spec_from_file_location('isolated_mcp_adapter', Path(__file__).resolve().parents[1] / 'mcp_server.py')
        self.adapter = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'mcp': package, 'mcp.server': server}): spec.loader.exec_module(self.adapter)
        self.handlers = handlers
        for pool in self.adapter.TOOL_POOLS.values():
            self.addCleanup(pool.shutdown)
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


    def test_cursor_execution_fields_forwarded_and_resolved_report_agent_wins(self):
        execution = {'agent': 'cursor', 'model': 'compose', 'options': {'fast': True}}
        report = {'id': 'task', 'agent': 'cursor', 'model': 'compose', 'execution': execution}
        with patch.object(self.adapter, 'devctl', side_effect=['task', json.dumps(report)]) as run:
            result = self.adapter.start_task('test', 'prompt', agent='auto', model='auto',
                                             execution_profile='quick', execution_options={'fast': True})
        args = run.call_args_list[0].args
        self.assertEqual(args[args.index('--execution-profile') + 1], 'quick')
        self.assertEqual(json.loads(args[args.index('--execution-options') + 1]), {'fast': True})
        self.assertEqual(result['agent'], 'cursor')
        self.assertEqual(result['execution'], execution)

    def test_catalog_inventory_uses_read_only_command(self):
        with patch.object(self.adapter, 'devctl', return_value='{"catalog": {}}') as run:
            self.assertEqual(self.adapter.get_execution_catalog('test', True, 'cursor'), {'catalog': {}})
        run.assert_called_once_with('execution-catalog', '--project', 'test', '--discover', '--agent', 'cursor')


    def test_preflight_tool_returns_actionable_error_as_structured_result(self):
        payload = {'status': 'REJECTED', 'launch_started': False, 'error': 'Cursor model unavailable'}
        with patch.object(self.adapter, 'devctl', return_value=json.dumps(payload)) as run:
            result = self.adapter.preflight_task('start_task', {'project': 'test', 'role': 'review'})
        self.assertEqual(result, payload)
        self.assertEqual(run.call_args.args[:2], ('preflight-task', 'start_task'))

    def test_locked_worker_preflight_race_rejection_preserves_message(self):
        from subprocess import CompletedProcess
        payload = {'status': 'REJECTED', 'launch_started': False, 'error': 'Cursor model removed after preflight'}
        with patch.object(self.adapter, 'devctl_result', return_value=CompletedProcess([], 2, json.dumps(payload))):
            self.assertEqual(self.adapter.start_task('test', 'prompt'), payload)

    def test_generic_worker_error_is_not_treated_as_no_launch(self):
        from subprocess import CompletedProcess
        with patch.object(self.adapter, 'devctl_result', return_value=CompletedProcess([], 1, 'unknown worker crash')):
            with self.assertRaises(RuntimeError): self.adapter.start_task('test', 'prompt')

    def test_registered_async_handlers_keep_control_capacity_during_two_slow_launches(self):
        entered, release = threading.Event(), threading.Event()
        started = []
        def devctl(*args, **kwargs):
            if args[0] == 'run-project':
                started.append(args)
                if len(started) == 2:
                    entered.set()
                release.wait(5)
                return 'task'
            return '{"id":"task","status":"RUNNING"}'
        async def check():
            with patch.object(self.adapter, 'devctl', side_effect=devctl):
                launches = [asyncio.create_task(self.handlers['start_task']('test', 'prompt', request_id=str(i) * 64))
                            for i in (1, 2)]
                try:
                    await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
                    self.assertTrue(entered.is_set())
                    result = await asyncio.wait_for(self.handlers['get_task_report']('task'), 1)
                    self.assertEqual(result['id'], 'task')
                    self.assertTrue(all(not task.done() for task in launches))
                finally:
                    release.set()
                    await asyncio.gather(*launches)
        asyncio.run(check())

    def test_transition_capabilities_and_request_reconciliation_contract(self):
        self.assertEqual(self.adapter.get_transition_capabilities(),
                         {'schema_version': 1, 'review_binding': 1, 'start_request_reconciliation': 1})
        with patch.object(self.adapter, 'devctl', return_value='{"state":"BUSY"}') as run:
            self.assertEqual(self.adapter.inspect_start_request('a' * 64, fence=True), {'state': 'BUSY'})
        run.assert_called_once_with('inspect-start-request', 'a' * 64, '--fence')

    def test_review_binding_forwarded_and_no_launch_proofs_remain_distinct(self):
        from subprocess import CompletedProcess
        binding = {'schema_version': 1}
        with patch.object(self.adapter, 'devctl', side_effect=['task', '{"id":"task"}']) as run:
            self.adapter.start_task('test', 'prompt', request_id='a' * 64, role='review', review_binding=binding)
        args = run.call_args_list[0].args
        self.assertEqual(json.loads(args[args.index('--review-binding') + 1]), binding)
        for status in ('STALE_INPUT', 'FENCED'):
            payload = {'status': status, 'launch_started': False}
            with self.subTest(status=status), patch.object(self.adapter, 'devctl_result',
                    return_value=CompletedProcess([], 3, json.dumps(payload))):
                self.assertEqual(self.adapter.start_task('test', 'prompt'), payload)
