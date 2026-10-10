"""The smoke latch refuses external effects before any subprocess or RPC."""
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import devctl
import external_effects
import worker_runtime
from external_effects import ENV, ExternalEffectBlocked
from publication import Repository


class EffectGateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._env = patch.dict(os.environ, {ENV: '1'})

    def block(self):
        self._env.start()
        self.addCleanup(self._env.stop)

    def test_unknown_and_mutating_github_commands_never_start(self):
        self.block()
        import task_bridge as bridge
        endpoint = ('repos/acme/widgets/issues?state=open&sort=created'
                    '&direction=asc&per_page=100')
        with patch('task_bridge.subprocess.run', side_effect=AssertionError('gh started')) as run:
            for args in (
                ('issue', 'comment', '1', '--body', 'x'),
                ('issue', 'close', '36'),
                ('api', '--method', 'PUT', 'repos/a/b/pulls/1/merge'),
                ('api', '--method=PATCH', 'repos/acme/widgets/issues/36'),
                ('api', 'repos/acme/widgets/issues/36', '-X', 'POST'),
                ('api', endpoint, '--paginate', '-X', 'POST'),
                ('api', endpoint, '-X', 'POST', '--paginate'),
                ('api', '--method', 'GET', endpoint, '--paginate'),
                ('api', endpoint, '-F', 'state=closed'),
                ('api', endpoint, '-fstate=closed'),
                ('api', 'repos/a/b/issues', '--paginate'),
                ('api', 'repos/acme/widgets/issues/36', '-f', 'state=closed'),
                ('pr', 'merge', '1'),
                ('pr', 'view', '1'),
                ('issue', 'view', '36'),
                ('issue', 'list'),
                ('api', 'graphql'),
                ('label', 'create', 'dev:done'),
            ):
                with self.subTest(args=args):
                    with self.assertRaises(ExternalEffectBlocked):
                        bridge.gh(*args)
        self.assertEqual(run.call_count, 0)

    def test_github_reads_remain_available(self):
        self.block()
        import task_bridge as bridge
        from github_transport import GitHubTransport
        endpoint = ('repos/acme/widgets/issues?state=open&sort=created'
                    '&direction=asc&per_page=100')
        reads = (
            ('api', endpoint, '--paginate'),
            ('api', 'repos/acme/widgets/issues/36/comments?per_page=100', '--paginate'),
            ('api', 'repos/acme/widgets/labels?per_page=100', '--paginate'),
            ('issue', 'view', '36', '-R', 'acme/widgets', '--json',
             'number,title,body,labels,url,state'),
            ('issue', 'view', '36', '-R', 'acme/widgets', '--json', 'number,labels'),
        )
        with patch('task_bridge.subprocess.run', return_value=type('R', (), {
                'returncode': 0, 'stdout': '[]', 'stderr': ''})()) as run:
            for args in reads:
                with self.subTest(args=args):
                    bridge.gh(*args)
        self.assertEqual(run.call_count, len(reads))
        issue = ('{"number":36,"title":"t","body":"b","labels":[],"state":"OPEN",'
                 '"url":"https://example.invalid/36"}')
        recorded = []

        def run_cli(*args):
            recorded.append(tuple(args))
            if args[:2] == ('issue', 'view'):
                return issue
            return '[]'

        transport = GitHubTransport('acme/widgets', run=run_cli, state_labels=('dev:running',))
        transport.poll_commands()
        transport.fetch_issue(transport.issue_ref(36))
        transport.fetch_labels(transport.issue_ref(36))
        transport.issue_comments(transport.issue_ref(36))
        transport.pages('repos/acme/widgets/labels?per_page=100')
        transport.post_comment(transport.issue_ref(36), 'hello')
        transport.transition_issue({'number': 36, 'labels': []}, 'dev:done')
        reads_seen = [args for args in recorded if external_effects.github_is_read(args)]
        writes_seen = [args for args in recorded if not external_effects.github_is_read(args)]
        self.assertTrue(reads_seen)
        self.assertTrue(writes_seen)
        self.assertTrue(any(args[:2] == ('issue', 'comment') for args in writes_seen))
        self.assertTrue(any(args[:2] == ('issue', 'edit') for args in writes_seen))

    def test_publication_subprocess_is_refused_before_capture(self):
        self.block()
        projects = {'test': {'machine': 'work', 'workspace': '/tmp/acceptance-workspace'}}
        with patch.object(devctl, 'load_projects', return_value=projects):
            repo = Repository('test', None)
        with patch.object(devctl, 'capture', side_effect=AssertionError('capture started')):
            with self.assertRaises(ExternalEffectBlocked):
                repo.run('gh', 'api', '--hostname', 'github.com', '--method', 'PUT',
                         'repos/example/repo/pulls/10/merge')
            with self.assertRaises(ExternalEffectBlocked):
                repo.run('git', 'status')

    def test_worker_spawn_and_fence_stop_before_write(self):
        self.block()
        with patch('devctl.subprocess.Popen', side_effect=AssertionError('worker started')):
            with open(os.devnull, 'w') as log:
                with self.assertRaises(ExternalEffectBlocked):
                    devctl.spawn_detached('echo', log)
        with self.assertRaises(ExternalEffectBlocked):
            devctl.run_agent('work', 'codex', '/tmp/acceptance-workspace', 'prompt')
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / 'tasks'
            data.mkdir()
            with self.assertRaises(ExternalEffectBlocked):
                worker_runtime.inspect_start_request(data, 'a' * 64, fence=True)
            self.assertFalse((Path(tmp) / 'requests').exists())

    async def test_worker_rpc_is_refused_before_the_client(self):
        self.block()
        import task_bridge as bridge
        seen = []

        async def transport(name, arguments):
            seen.append(name)
            return {'schema_version': 1, 'review_binding': 1, 'start_request_reconciliation': 1}

        with patch.object(bridge, '_transport_call', transport):
            for name, arguments in (
                ('start_task', {'project': 'test'}),
                ('continue_task', {'task_id': 'task'}),
                ('adopt_task', {'project': 'test', 'session_id': 'session'}),
                ('inspect_start_request', {'request_id': 'a' * 64, 'fence': True}),
            ):
                with self.assertRaises(ExternalEffectBlocked):
                    await bridge.transport_call(name, arguments)
            await bridge.transport_call('get_transition_capabilities', {})
        self.assertEqual(seen, ['get_transition_capabilities'])

    def test_capability_probe_cannot_select_another_tool(self):
        path = Path(__file__).resolve().parents[1] / 'docs' / 'acceptance' / 'capability_probe.py'
        spec = importlib.util.spec_from_file_location('acceptance_capability_probe', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.TOOL, 'get_transition_capabilities')
        module.require_tool(module.TOOL)
        with self.assertRaises(SystemExit):
            module.require_tool('start_task')
        with self.assertRaises(SystemExit):
            module.require_tool('inspect_start_request')

    def test_telegram_refuses_before_subprocess_and_network(self):
        self.block()
        import notifications
        from transition_executor import synchronous
        credentials = {'TELEGRAM_BOT_TOKEN': 'fake-token', 'TELEGRAM_CHAT_ID': '1'}
        with patch.dict(os.environ, credentials):
            with patch('notifications.subprocess.run', side_effect=AssertionError('telegram subprocess')) as run:
                with patch('urllib.request.urlopen', side_effect=AssertionError('telegram network')) as opened:
                    with self.assertRaises(ExternalEffectBlocked) as raised:
                        notifications.send('hello')
                    self.assertEqual(raised.exception.kind, 'telegram')
                    opener = unittest.mock.Mock(side_effect=AssertionError('opener'))
                    with self.assertRaises(ExternalEffectBlocked):
                        notifications.send('hello', opener=opener)
            self.assertEqual(run.call_count, 0)
            self.assertEqual(opened.call_count, 0)
            self.assertEqual(opener.call_count, 0)
        import task_bridge as bridge
        with patch.object(bridge, 'send_telegram', side_effect=AssertionError('dispatcher send')):
            with self.assertRaises(ExternalEffectBlocked):
                synchronous('telegram', {'text': 'hello'})
        with patch.object(bridge, 'raw_add_comment', side_effect=AssertionError('comment')):
            with self.assertRaises(ExternalEffectBlocked):
                synchronous('post_comment', {'number': '1', 'body': 'x'})
        with patch.object(bridge, 'raw_transition_issue', side_effect=AssertionError('label')):
            with self.assertRaises(ExternalEffectBlocked):
                synchronous('transition', {'issue': {'number': 1}, 'target': 'dev:done'})
        with patch.object(devctl, 'capture', side_effect=AssertionError('capture')):
            with self.assertRaises(ExternalEffectBlocked):
                synchronous('repository', {'method': 'snapshot', 'project': 'test'})

    def test_launch_adopt_and_continue_stop_before_receipts(self):
        self.block()
        import mcp_server
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / 'tasks'
            data.mkdir()
            with self.assertRaises(ExternalEffectBlocked):
                worker_runtime.launch(data, 'a' * 64, 'task', lambda: self.fail('launched'),
                                       signature='sig', signature_version=4)
            self.assertFalse((Path(tmp) / 'requests').exists())
        with self.assertRaises(ExternalEffectBlocked):
            devctl.adopt_task('test', 'session')
        with self.assertRaises(ExternalEffectBlocked):
            devctl.continue_task('task', 'prompt')
        with self.assertRaises(ExternalEffectBlocked):
            mcp_server.start_task('test', 'prompt')
        with self.assertRaises(ExternalEffectBlocked):
            mcp_server.continue_task('task', 'prompt')
        with self.assertRaises(ExternalEffectBlocked):
            mcp_server.adopt_task('test', 'session')
        with self.assertRaises(ExternalEffectBlocked):
            mcp_server.inspect_start_request('a' * 64, fence=True)

    async def test_dispatcher_refuses_fence_before_the_client(self):
        self.block()
        import task_bridge as bridge
        from transition_executor import execute
        from transition_io import Command
        seen = []

        async def transport(name, arguments):
            seen.append((name, bool(arguments.get('fence')) if isinstance(arguments, dict) else False))
            if name == 'get_transition_capabilities':
                return {'schema_version': 1, 'review_binding': 1, 'start_request_reconciliation': 1}
            raise AssertionError(name)

        with patch.object(bridge, '_transport_call', transport):
            with self.assertRaises(ExternalEffectBlocked):
                await execute(Command.create('legacy_review_inspect', {'request_id': 'a' * 64}))
            with self.assertRaises(ExternalEffectBlocked):
                await execute(Command.create('rpc', {
                    'name': 'inspect_start_request',
                    'arguments': {'request_id': 'a' * 64, 'fence': True}}))
        self.assertEqual(seen, [('get_transition_capabilities', False)])

    def test_latched_publication_does_not_open_a_journal(self):
        self.block()
        from publication import execute
        projects = {'test': {'machine': 'work', 'workspace': '/tmp/acceptance-workspace'}}
        item = {'task_id': 't'}
        saved = []
        with patch.object(devctl, 'load_projects', return_value=projects):
            repo = Repository('test', None)
        with patch('chief_review.approval_complete', return_value=True):
            with patch.object(devctl, 'capture', side_effect=AssertionError('capture started')) as capture:
                with self.assertRaises(ExternalEffectBlocked):
                    execute(repo, item, {}, {'action': 'COMMIT', 'message': 'x'}, lambda: saved.append(True))
        self.assertNotIn('publication_journal', item)
        self.assertEqual(saved, [])
        self.assertEqual(capture.call_count, 0)

    def test_latch_off_does_not_block_classification(self):
        os.environ.pop(ENV, None)
        self.assertFalse(external_effects.blocked())
        external_effects.guard_github(('issue', 'comment', '1'))
        external_effects.guard_worker_rpc('start_task', {})
        external_effects.refuse('publication')
