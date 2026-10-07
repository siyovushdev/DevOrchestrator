"""Installed-SDK wire semantics, without a live MCP service or worker effects."""
import asyncio
from contextlib import redirect_stdout
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import task_bridge as bridge
from scheduler import Scheduler


def task(index, status='DONE'):
    return dict(id=f'task-{index}', project='test', machine='work', agent='codex',
                status=status, turns=1, session_id=None, workspace='/fixture',
                created_at='2026-10-07', updated_at='2026-10-07', exit_code=0,
                archived=False)


def response(*values, content=None, is_error=False):
    # MCP 2.3.0 emits one TextContent per list element, not a JSON list envelope.
    if content is None:
        content = [SimpleNamespace(type='text', text=json.dumps(value)) for value in values]
    return SimpleNamespace(content=content, is_error=is_error)


class TransportTests(unittest.TestCase):
    def call(self, name, result):
        calls = []

        class Client:
            def __init__(self, url):
                self.url = url
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            async def call_tool(self, tool, arguments):
                calls.append((tool, arguments))
                return result

        with patch.dict(sys.modules, {'mcp': SimpleNamespace(Client=Client)}):
            decoded = asyncio.run(bridge.transport_call(name, {'fixture': True}))
        self.assertEqual(calls, [(name, {'fixture': True})])
        return decoded

    def test_installed_sdk_task_blocks_recover_every_task(self):
        # Include active work AFTER the first block: dropping it would free capacity.
        tasks = [task(0), task(1, 'RUNNING'), task(2, 'UNKNOWN')]
        decoded = self.call('list_tasks', response(*tasks))
        self.assertEqual(decoded, tasks)
        state = {'issues': {}}
        with patch('scheduler.project_identity', return_value=('work', 'repo')):
            Scheduler(state).recover(decoded)
        self.assertEqual({j['task_id'] for j in state['work_queue'].values()},
                         {'task-1', 'task-2'})
        self.assertEqual(state['work_queue']['external:task-1']['status'], 'RUNNING')
        self.assertEqual(state['work_queue']['external:task-2']['status'], 'UNKNOWN')

    def test_collection_cardinality_is_not_fixed_to_rollout_count(self):
        for count in (1, 2, 27, 31):
            with self.subTest(count=count):
                tasks = [task(i) for i in range(count)]
                self.assertEqual(self.call('list_tasks', response(*tasks)), tasks)

    def test_single_json_list_block(self):
        for tasks in ([], [task(0)], [task(0), task(1)]):
            with self.subTest(tasks=len(tasks)):
                self.assertEqual(self.call('list_tasks', response(tasks)), tasks)

    def test_sdk_empty_collection(self):
        self.assertEqual(self.call('list_tasks', response()), [])

    def test_object_tools_remain_objects(self):
        for name in ('list_projects', 'get_task_report', 'start_task', 'continue_task',
                     'adopt_task', 'orchestrator_health'):
            with self.subTest(name=name):
                value = {'id': 'task', 'status': 'RUNNING', 'result': 'text'}
                self.assertEqual(self.call(name, response(value)), value)

    def test_scalar_single_result_is_preserved(self):
        for value in ('text', 3, False, None):
            with self.subTest(value=value):
                self.assertEqual(self.call('scalar_fixture', response(value)), value)

    def test_single_result_rejects_multiple_blocks(self):
        for name in ('get_task_report', 'start_task', 'continue_task', 'scalar_fixture'):
            with self.subTest(name=name):
                with self.assertRaisesRegex(RuntimeError, name + '.*exactly one'):
                    self.call(name, response({'id': 'first'}, {'id': 'second'}))

    def test_object_tool_rejects_list_or_scalar(self):
        for value in ([task(0)], 'text', None, 3):
            with self.subTest(value=value):
                with self.assertRaisesRegex(RuntimeError, 'get_task_report.*object'):
                    self.call('get_task_report', response(value))

    def test_empty_single_result_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'get_task_report.*exactly one'):
            self.call('get_task_report', response())

    def test_bad_block_anywhere_rejects_whole_response_without_payload_leak(self):
        for bad in (SimpleNamespace(type='text', text='{SECRET'),
                    SimpleNamespace(type='text', text=''),
                    SimpleNamespace(type='image', text=json.dumps(task(9))),
                    SimpleNamespace(type='text', text=None)):
            for content in ([bad, response(task(0)).content[0]],
                            [response(task(0)).content[0], bad]):
                with self.subTest(bad=bad, content=content):
                    with self.assertRaisesRegex(RuntimeError, 'list_tasks.*block') as error:
                        self.call('list_tasks', response(content=content))
                    self.assertNotIn('SECRET', str(error.exception))

    def test_missing_or_invalid_content_fails_closed(self):
        for content in (None, {}, 'text'):
            with self.subTest(content=content):
                with self.assertRaisesRegex(RuntimeError, 'list_tasks.*content'):
                    self.call('list_tasks', SimpleNamespace(content=content))
        with self.assertRaisesRegex(RuntimeError, 'list_tasks.*content'):
            self.call('list_tasks', SimpleNamespace())

    def test_collection_rejects_mixed_or_nested_shapes(self):
        for values in (([task(0)], task(1)), ([task(0)], [task(1)]),
                       ('text',), (None,), ([task(0), 'text'],),
                       ({'result': [task(0)]},)):
            with self.subTest(values=values):
                with self.assertRaisesRegex(RuntimeError, 'list_tasks.*(collection|task)'):
                    self.call('list_tasks', response(*values))

    def test_malformed_task_rejects_collection_before_recovery_mutates_state(self):
        for invalid in ({}, {'id': 'a'}, {'id': '', 'status': 'RUNNING'},
                        {'id': 'a', 'status': None}, {'id': 1, 'status': 'RUNNING'}):
            state = {'issues': {}}
            before = copy.deepcopy(state)
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(RuntimeError, 'list_tasks.*task'):
                    decoded = self.call('list_tasks', response(task(0), invalid))
                    Scheduler(state).recover(decoded)
                self.assertEqual(state, before)

    def test_tool_error_flag_rejects_valid_json(self):
        for name in ('list_tasks', 'start_task'):
            for flag in ('is_error', 'isError'):
                with self.subTest(name=name, flag=flag):
                    result = response(task(0))
                    setattr(result, flag, True)
                    with self.assertRaisesRegex(RuntimeError, name + '.*error'):
                        self.call(name, result)

    def test_duplicate_task_ids_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'list_tasks.*duplicate id'):
            self.call('list_tasks', response(task(0), task(0)))

    def test_structured_collection_cannot_hide_missing_blocks(self):
        for structured in ({'result': [task(0)]}, {'result': 'unexpected'}, []):
            with self.subTest(structured=structured):
                result = response()
                result.structured_content = structured
                with self.assertRaisesRegex(RuntimeError, 'list_tasks.*disagrees'):
                    self.call('list_tasks', result)
        result = response(task(0), task(1))
        result.structured_content = {'result': [task(0), task(1)]}
        self.assertEqual(self.call('list_tasks', result), [task(0), task(1)])
        result = response()
        result.structured_content = {'result': []}
        self.assertEqual(self.call('list_tasks', result), [])

    def smoke(self, tasks_result):
        class Client:
            def __init__(self, url): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False
            async def list_tools(self):
                return SimpleNamespace(tools=[SimpleNamespace(name='list_tasks')])
            async def call_tool(self, name, arguments):
                if name == 'list_projects':
                    return response({'test': {'machine': 'work'}})
                if name == 'list_tasks':
                    return tasks_result
                raise AssertionError(name)

        spec = importlib.util.spec_from_file_location(
            'isolated_mcp_smoke', Path(__file__).resolve().parents[1] / 'mcp_smoke.py')
        smoke = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'mcp': SimpleNamespace(Client=Client)}):
            spec.loader.exec_module(smoke)
            asyncio.run(smoke.main())

    def test_smoke_requires_complete_task_collection(self):
        for count in (0, 3):
            with self.subTest(count=count), redirect_stdout(io.StringIO()) as output:
                self.smoke(response(*(task(i) for i in range(count))))
                self.assertIn(f'TASK_LIST_TRANSPORT: PASS ({count} tasks, {count} content blocks)',
                              output.getvalue())

    def test_smoke_does_not_print_pass_on_malformed_later_block(self):
        with redirect_stdout(io.StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, 'list_tasks.*block 2'):
                self.smoke(response(content=[response(task(0)).content[0],
                    SimpleNamespace(type='text', text='{malformed')]))
        self.assertNotIn('TASK_LIST_TRANSPORT: PASS', output.getvalue())
