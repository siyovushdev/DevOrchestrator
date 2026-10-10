"""Smoke latch keeps retryable intent and does not cross a send boundary."""
import copy
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bridge_effects
import devctl
import external_effects
import task_bridge as bridge
import test_bridge_integration as integration
from external_effects import ENV, ExternalEffectBlocked
from transition_io import DRIVING


def _load_manifest():
    path = Path(__file__).resolve().parents[1] / 'docs' / 'acceptance' / 'source_manifest.py'
    spec = importlib.util.spec_from_file_location('acceptance_source_manifest', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class OutboxLatchTests(unittest.TestCase):
    def flush(self, kind, payload, *, status='PENDING', extra=None, comments=None, post=None, latch=True):
        state = {'issues': {'1': {}}, 'outbox': {}}
        key = bridge_effects.enqueue(state, '1', kind, payload)
        entry = state['outbox'][key]
        entry['status'] = status
        if extra:
            entry.update(extra)
        seen = []

        def persist():
            seen.append(entry['status'])

        def refused(*args):
            seen.append('dispatched')
            raise AssertionError('effect dispatched')

        callback = post or refused
        env = {ENV: '1'} if latch else {}
        with patch.dict(os.environ, env, clear=False):
            if not latch:
                os.environ.pop(ENV, None)
            bridge_effects.flush(
                state, persist, comments=comments or (lambda number: []),
                post_comment=callback, transition=callback, telegram=callback,
                eligible=lambda item: True, complete=lambda number: callback(number))
        return state['outbox'][key], seen

    def test_preexisting_runtime_effects_stay_pending_without_attempts_or_sending(self):
        for kind, payload in (
            ('comment', 'started'),
            ('chief_comment', 'evidence'),
            ('telegram', 'alert'),
            ('transition', {'issue': {'number': 1}, 'target': 'dev:running'}),
            ('completion', {'number': 1}),
        ):
            with self.subTest(kind=kind):
                entry, seen = self.flush(kind, payload)
                self.assertEqual(entry['status'], 'PENDING')
                self.assertEqual(entry['attempts'], 0)
                self.assertNotIn('diagnostic', entry)
                self.assertNotIn('SENDING', seen)
                self.assertNotIn('UNCERTAIN', seen)
                self.assertNotIn('dispatched', seen)
                if kind == 'completion':
                    self.assertNotIn('completion_comment', entry)

    def test_exact_comment_receipt_still_reconciles_while_latched(self):
        probe = {'issues': {'1': {}}, 'outbox': {}}
        key = bridge_effects.enqueue(probe, '1', 'chief_comment', 'evidence')
        body = 'evidence\n\n<!-- DEVORCHESTRATOR_EFFECT_V1 ' + key + ' -->'
        entry, seen = self.flush(
            'chief_comment', 'evidence',
            comments=lambda number: [{'id': '9', 'body': body}])
        self.assertEqual(entry['status'], 'DONE')
        self.assertNotIn('dispatched', seen)
        self.assertNotIn('SENDING', seen)

    def test_duplicate_receipt_stays_uncertain_and_is_not_resent(self):
        probe = {'issues': {'1': {}}, 'outbox': {}}
        key = bridge_effects.enqueue(probe, '1', 'comment', 'started')
        body = 'started\n\n<!-- DEVORCHESTRATOR_EFFECT_V1 ' + key + ' -->'
        comments = [{'id': '1', 'body': body}, {'id': '2', 'body': body}]
        entry, seen = self.flush('comment', 'started', comments=lambda number: comments)
        self.assertEqual(entry['status'], 'UNCERTAIN')
        self.assertEqual(entry['attempts'], 0)
        self.assertNotIn('dispatched', seen)

    def test_crash_sending_comment_reconciles_without_a_new_post(self):
        entry, seen = self.flush('chief_comment', 'evidence', status='SENDING')
        self.assertEqual(entry['status'], 'UNCERTAIN')
        self.assertEqual(entry['attempts'], 0)
        self.assertNotIn('dispatched', seen)

    def test_crash_sending_telegram_stays_uncertain_without_transport(self):
        entry, seen = self.flush('telegram', 'alert', status='SENDING')
        self.assertEqual(entry['status'], 'UNCERTAIN')
        self.assertEqual(entry['attempts'], 0)
        self.assertIn('automatic resend suppressed', entry['diagnostic'])
        self.assertNotIn('dispatched', seen)

    def test_blocked_exception_after_sending_returns_to_pending(self):
        def post(*args):
            raise ExternalEffectBlocked('github')

        entry, seen = self.flush('chief_comment', 'evidence', post=post, latch=False)
        self.assertEqual(entry['status'], 'PENDING')
        self.assertEqual(entry['attempts'], 0)
        self.assertEqual(seen, ['SENDING', 'PENDING'])

    def test_ambiguous_post_is_still_uncertain_when_the_latch_is_off(self):
        def post(*args):
            raise BlockingIOError('transport error after potential dispatch')

        entry, seen = self.flush('chief_comment', 'evidence', post=post, latch=False)
        self.assertEqual(entry['status'], 'UNCERTAIN')
        self.assertEqual(entry['attempts'], 1)
        self.assertIn('SENDING', seen)
        self.assertIn('exact receipt reconciliation required', entry['diagnostic'])

    def test_comment_not_dispatched_still_retries(self):
        def post(*args):
            raise bridge_effects.CommentNotDispatched()

        entry, _seen = self.flush('comment', 'started', post=post, latch=False)
        self.assertEqual(entry['status'], 'PENDING')
        self.assertEqual(entry['attempts'], 1)

    def test_completion_crash_receipt_is_reconciled_and_a_new_one_is_not_sent(self):
        issue = {'number': 1, 'title': 't', 'body': '', 'labels': [], 'state': 'OPEN',
                 'url': 'https://example.invalid/1'}
        calls = []

        def gh(*args):
            calls.append(tuple(args))
            if args[:2] == ('issue', 'comment'):
                raise AssertionError('completion posted')
            if args[0] == 'api':
                return '[]'
            raise AssertionError(args)

        def gh_json(*args):
            calls.append(tuple(args))
            return dict(issue)

        item = {'owner_state': 'MERGED', 'merged': True, 'task_id': 'task',
                'commit_sha': 'a' * 40, 'post_merge_convergence': {'status': 'DONE'}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'state.json'
            with patch.object(bridge, 'STATE_FILE', path), patch.object(bridge, 'gh', gh), \
                    patch.object(bridge, 'gh_json', gh_json), \
                    patch('task_bridge.subprocess.run', side_effect=AssertionError('gh started')):
                fresh = {'issues': {'1': copy.deepcopy(item)}, 'outbox': {}}
                key = bridge_effects.enqueue(fresh, '1', 'completion', {'number': 1})
                with patch.dict(os.environ, {ENV: '1'}):
                    bridge.flush_effects(fresh)
                self.assertEqual(fresh['outbox'][key]['status'], 'PENDING')
                self.assertEqual(fresh['outbox'][key]['attempts'], 0)
                self.assertNotIn('completion_comment', fresh['outbox'][key])
                self.assertEqual(calls, [])
                crashed = {'issues': {'1': copy.deepcopy(item)}, 'outbox': {}}
                crashed_key = bridge_effects.enqueue(crashed, '1', 'completion', {'number': 1})
                crashed['outbox'][crashed_key]['completion_comment'] = 'SENDING'
                with patch.dict(os.environ, {ENV: '1'}):
                    bridge.flush_effects(crashed)
                self.assertEqual(crashed['outbox'][crashed_key]['completion_comment'], 'UNCERTAIN')
                self.assertFalse(any(args[:2] == ('issue', 'comment') for args in calls))
                self.assertTrue(calls)


class LaunchAuthorityTests(unittest.IsolatedAsyncioTestCase):
    async def test_latched_start_and_continue_do_not_record_launch_authority(self):
        with patch.dict(os.environ, {ENV: '1'}):
            state = {'issues': {'9': {'queue_status': 'QUEUED'}}}
            item = state['issues']['9']
            bridge.WORK_CONTEXT = (state, '9', item)
            self.addCleanup(lambda: setattr(bridge, 'WORK_CONTEXT', None))
            token = DRIVING.set(True)
            try:
                with self.assertRaises(ExternalEffectBlocked):
                    await bridge.mcp_call('start_task', {'project': 'test', 'prompt': 'x', 'role': 'coding'})
                with self.assertRaises(ExternalEffectBlocked):
                    await bridge.mcp_call('continue_task', {'task_id': 'task', 'prompt': 'x'})
            finally:
                DRIVING.reset(token)
            self.assertNotIn('pending_rpc', item)
            self.assertNotIn('work_queue', state)
            queued = {'queue_status': 'QUEUED', 'pending_rpc': {
                'name': 'start_task', 'operation': 'initial',
                'arguments': {'request_id': 'c' * 64, 'project': 'test', 'prompt': 'keep'}}}
            preserved = copy.deepcopy(queued)
            holder = {'issues': {'4': queued}}
            issue = {'number': 4, 'state': 'OPEN', 'labels': [], 'title': 't', 'body': ''}
            await bridge.start_queued(issue, holder)
            self.assertEqual(holder['issues']['4'], preserved)
            self.assertNotIn('reconciliation_retry', holder['issues']['4'])
            self.assertNotIn('work_queue', holder)


class ControlLoopLatchTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    async def bundle(self):
        await bridge.tick(self.h.state)
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], integration.coding_result())
        await bridge.tick(self.h.state)
        self.h.finish(item['review_task_id'], integration.review_result())
        with patch.object(bridge, 'flush_effects'):
            await bridge.tick(self.h.state)
        keys = item['chief_review']['effects']
        self.assertTrue(keys)
        self.h.state['outbox'] = {key: self.h.state['outbox'][key] for key in keys}
        bridge.save_state(self.h.state)
        return item, keys

    async def test_latched_control_loop_does_not_dispatch_or_damage_receipts(self):
        item, keys = await self.bundle()
        self.h.issues[2] = self.h.issue(2)
        self.h.comments[2] = []
        self.h.state['issues']['2'] = {'queue_status': 'QUEUED'}
        receipt_dir = devctl.DATA_DIR.parent / 'requests'
        receipt_dir.mkdir(parents=True)
        receipt = receipt_dir / ('b' * 64 + '.json')
        receipt.write_bytes(b'{"status":"STARTED","task_id":"old-task"}\n')
        before = receipt.read_bytes()
        attempts = {key: self.h.state['outbox'][key]['attempts'] for key in keys}
        payloads = {key: self.h.state['outbox'][key]['payload'] for key in keys}
        starts = list(self.h.starts)
        bridge.save_state(self.h.state)
        gh_calls, rpc_calls = [], []

        def recording_gh(*args):
            external_effects.guard_github(args)
            gh_calls.append(tuple(args))
            if not external_effects.github_is_read(tuple(args)):
                raise AssertionError(args)
            return self.h.gh(*args)

        def recording_json(*args):
            external_effects.guard_github(args)
            gh_calls.append(tuple(args))
            if not external_effects.github_is_read(tuple(args)):
                raise AssertionError(args)
            return self.h.gh_json(*args)

        async def recording_transport(name, arguments):
            external_effects.guard_worker_rpc(name, arguments)
            rpc_calls.append(name)
            if name in external_effects.WORKER_TOOLS or (
                    name == 'inspect_start_request' and isinstance(arguments, dict) and arguments.get('fence')):
                raise AssertionError(name)
            return await self.h.transport(name, arguments)

        def recording_telegram(text):
            return __import__('notifications').send(text)

        from control_loop import ControlLoop

        async def exercise(loop):
            for _ in range(20):
                await loop.step()
                await loop.wait(0.05)

        with patch.dict(os.environ, {ENV: '1', 'TELEGRAM_BOT_TOKEN': 'fake-token', 'TELEGRAM_CHAT_ID': '1'}), \
                patch.object(bridge, 'gh', recording_gh), patch.object(bridge, 'gh_json', recording_json), \
                patch.object(bridge, 'transport_call', recording_transport), \
                patch.object(bridge, 'send_telegram', recording_telegram), \
                patch('task_bridge.subprocess.run', side_effect=AssertionError('gh started')) as gh_run, \
                patch('notifications.subprocess.run', side_effect=AssertionError('telegram started')), \
                patch('urllib.request.urlopen', side_effect=AssertionError('network')) as opened, \
                patch('devctl.subprocess.Popen', side_effect=AssertionError('popen')) as popen, \
                patch.object(devctl, 'capture', side_effect=AssertionError('capture')) as capture:
            loop = ControlLoop(self.h.state)
            loop.issues['1'] = copy.deepcopy(self.h.issues[1])
            loop.issues['2'] = copy.deepcopy(self.h.issues[2])
            loop.next_heavy = float('inf')
            try:
                await exercise(loop)
            finally:
                await loop.close()
            self.assertEqual(gh_run.call_count, 0)
            self.assertEqual(opened.call_count, 0)
            self.assertEqual(popen.call_count, 0)
            self.assertEqual(capture.call_count, 0)
            restarted = bridge.load_state()
            loop = ControlLoop(restarted)
            loop.issues['1'] = copy.deepcopy(self.h.issues[1])
            loop.issues['2'] = copy.deepcopy(self.h.issues[2])
            loop.next_heavy = float('inf')
            try:
                await exercise(loop)
            finally:
                await loop.close()
            self.assertEqual(capture.call_count, 0)
            self.assertEqual(gh_run.call_count, 0)
        state = bridge.load_state()
        for key in keys:
            self.assertEqual(state['outbox'][key]['status'], 'PENDING')
            self.assertEqual(state['outbox'][key]['attempts'], attempts[key])
            self.assertEqual(state['outbox'][key]['payload'], payloads[key])
        self.assertTrue(all(entry['status'] not in ('SENDING', 'UNCERTAIN') for entry in state['outbox'].values()))
        self.assertTrue(all(external_effects.github_is_read(args) for args in gh_calls))
        self.assertFalse(any(name in external_effects.WORKER_TOOLS or name == 'inspect_start_request'
                             for name in rpc_calls))
        queued = state['issues']['2']
        self.assertEqual(queued.get('queue_status'), 'QUEUED')
        self.assertNotIn('pending_rpc', queued)
        self.assertNotIn('task_id', queued)
        self.assertNotIn('task_spec', queued)
        self.assertNotIn('publication_journal', queued)
        self.assertNotIn('reconciliation_retry', queued)
        self.assertNotIn('pending_rpc', state['issues']['1'])
        self.assertFalse(state['issues']['1'].get('publication_journal'))
        self.assertNotIn('reconciliation_retry', state['issues']['1'])
        self.assertIsNone(state['issues']['1'].get('review_hold'))
        self.assertFalse(chief_approval(state['issues']['1']))
        self.assertEqual(self.h.starts, starts)
        self.assertEqual(receipt.read_bytes(), before)
        self.assertEqual(list(receipt_dir.iterdir()), [receipt])

    async def test_clearing_the_latch_sends_the_withheld_chief_comment(self):
        _item, keys = await self.bundle()
        attempts = {key: self.h.state['outbox'][key]['attempts'] for key in keys}
        with patch.dict(os.environ, {ENV: '1'}):
            with patch.object(devctl, 'capture', side_effect=AssertionError('capture')), \
                    patch.object(self.h, 'post_comment', side_effect=AssertionError('posted')):
                bridge.flush_effects(self.h.state, control_fresh={'1'})
        for key in keys:
            self.assertEqual(self.h.state['outbox'][key]['status'], 'PENDING')
            self.assertEqual(self.h.state['outbox'][key]['attempts'], attempts[key])
        self.assertIsNone(self.h.state['issues']['1'].get('review_hold'))
        os.environ.pop(ENV, None)
        bridge.flush_effects(self.h.state, control_fresh={'1'})
        for key in keys:
            self.assertEqual(self.h.state['outbox'][key]['status'], 'DONE')


def chief_approval(item):
    import chief_review
    return chief_review.approval_complete(item)


class SourceManifestTests(unittest.TestCase):
    def test_manifest_includes_the_guard_module_and_rejects_a_foreign_root(self):
        module = _load_manifest()
        root = Path(__file__).resolve().parents[1]
        rows = module.manifest(root)
        paths = [row['path'] for row in rows]
        self.assertIn('external_effects.py', paths)
        self.assertTrue(all(len(row['sha256']) == 64 for row in rows))
        with self.assertRaises(SystemExit):
            module.manifest(Path('/tmp'))
