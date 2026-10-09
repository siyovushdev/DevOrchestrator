"""Deterministic whole-bridge harness: no real agents, TaskBridge, GitHub or Telegram."""
import asyncio
import copy
from datetime import datetime, timezone
import json
import io
import os
from contextlib import redirect_stdout
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

import devctl
import scheduler
import task_bridge as b
from github_transport import GitHubTransport
from transports import TransportError
from durable import read_json, write_json
from worker_runtime import process_identity
from tests_helpers import coding_result, review_result


class QueueLabelTests(unittest.TestCase):
    def test_real_transition_removes_dedicated_work_queue(self):
        with patch.object(b, 'gh') as gh:
            b.raw_transition_issue({'number': 1, 'labels': ['work:queued', 'project:test']}, 'dev:running')
        gh.assert_called_once_with('issue', 'edit', '1', '-R', b.REPO,
                                   '--remove-label', 'work:queued', '--add-label', 'dev:running')


class Harness:
    def __init__(self, root):
        self.root = root
        self.repo = root / 'repo'; self.repo.mkdir()
        self.git('init', '-b', 'feature')
        self.git('config', 'user.name', 'Test'); self.git('config', 'user.email', 'test@example.invalid')
        (self.repo / 'file').write_text('baseline')
        self.git('add', '.'); self.git('commit', '-m', 'baseline')
        self.issues = {1: self.issue(1)}
        self.comments = {1: []}
        self.tasks = {}
        self.requests = {}
        self.starts = []
        self.continues = []
        self.fail_after_start = False
        self.telegram = Mock(return_value=True)
        self.state = {'issues': {}}

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], stderr=subprocess.DEVNULL).decode().strip()

    def issue(self, number):
        return {'number': number, 'title': f'Task {number}', 'url': f'https://example.invalid/{number}',
                'body': '<!-- DEVORCHESTRATOR_TASK_V1 {"project":"test","agent":"codex","mode":"autopilot"} -->\nImplement task',
                'labels': [b.QUEUE_LABEL], 'state': 'OPEN'}

    def comment(self, body, created='2050-01-01T00:00:00Z'):
        self.comments[1].append({'id': str(len(self.comments[1]) + 1), 'body': body,
                                 'author': {'login': 'siyovushdev'}, 'createdAt': created})

    def gh_json(self, *args):
        if args[:2] == ('issue', 'list'):
            return [copy.deepcopy(i) for i in reversed(list(self.issues.values()))]
        if args[:2] == ('issue', 'view'):
            number = int(args[2])
            return {**copy.deepcopy(self.issues[number]), 'comments': copy.deepcopy(self.comments[number])}
        raise AssertionError(args)

    def gh(self, *args):
        """Fake provider I/O; real GitHubTransport and V1 wrappers stay active."""
        if args[0] == 'api':
            endpoint = args[1]
            if '/labels?' in endpoint:
                return json.dumps([{'name': name} for name in b.STATE_LABELS])
            if '/comments?' in endpoint:
                number = int(endpoint.split('/issues/', 1)[1].split('/')[0])
                return json.dumps(self.comments[number])
            return json.dumps([i for i in self.issues.values() if i.get('state', 'OPEN').upper() == 'OPEN'])
        if args[:2] == ('issue', 'comment'):
            self.post_comment(args[2], Path(args[args.index('--body-file') + 1]).read_text())
            return ''
        if args[:2] == ('issue', 'edit'):
            labels = b.label_names(self.issues[int(args[2])])
            for index, value in enumerate(args):
                if value == '--remove-label': labels.discard(args[index + 1])
                if value == '--add-label': labels.add(args[index + 1])
            self.issues[int(args[2])]['labels'] = sorted(labels)
            return ''
        raise AssertionError(args)

    def post_comment(self, number, body):
        self.comments[int(number)].append({'id': 'effect-' + str(len(self.comments[int(number)])),
            'author': {'login': 'bridge'}, 'body': body})

    async def transport(self, name, args):
        if name == 'list_tasks':
            return [dict(t, project='test') for t in self.tasks.values()]
        if name == 'get_task_report':
            return copy.deepcopy(self.tasks[args['task_id']])
        key = args['request_id']
        if key in self.requests:
            task_id = self.requests[key]
            return {**copy.deepcopy(self.tasks[task_id]), 'task_id': task_id}
        if name == 'start_task':
            role = args.get('role', 'coding')
            task_id = key[:24]
            baseline = devctl.capture_git_baseline('work', str(self.repo))
            self.tasks[task_id] = dict(id=task_id, status='RUNNING', role=role, machine='work',
                workspace=str(self.repo), agent=args.get('agent', 'codex'), turns=1, exit_code=None,
                git_baseline=baseline, created_at='2026-01-01T00:00:00+00:00', result=None)
            if role == 'coding': self.last_coding_prompt = args['prompt']
            if role == 'coding': (self.repo / 'file').write_text('implemented')
            self.starts.append((role, task_id))
        elif name == 'continue_task':
            task_id = args['task_id']
            self.tasks[task_id].update(status='RUNNING', turns=self.tasks[task_id]['turns'] + 1,
                                       exit_code=None, result=None)
            self.continues.append((task_id, args['prompt']))
        else:
            raise AssertionError(name)
        self.requests[key] = task_id
        if self.fail_after_start:
            self.fail_after_start = False
            raise OSError('network interrupted after agent launch')
        return {**copy.deepcopy(self.tasks[task_id]), 'task_id': task_id}

    def finish(self, task_id, result, status='DONE'):
        self.tasks[task_id].update(status=status, exit_code=0 if status == 'DONE' else 1, result=result)

    def restart(self):
        self.state = b.load_state()


class BridgeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.h = Harness(self.root)
        patches = [patch.dict("os.environ", {"DEVORCHESTRATOR_OWNERS": "siyovushdev", "DEVORCHESTRATOR_MAX_CODING": "1",
                    "DEVORCHESTRATOR_MAX_REVIEW": "1", "DEVORCHESTRATOR_MAX_AGENTS": "2",
                    "DEVORCHESTRATOR_MACHINE_LIMITS": '{"work":1,"firefly":1}'}), patch.object(devctl, 'load_projects', return_value={'test': {'machine': 'work', 'workspace': str(self.h.repo)}}),
                   patch.object(devctl, 'DATA_DIR', self.root / 'state/tasks'),
                   patch.object(b, 'STATE_FILE', self.root / 'state/task_bridge_state.json'),
                   patch.object(b, 'transport_call', self.h.transport), patch.object(b, 'gh_json', self.h.gh_json),
                   patch.object(b, 'gh', self.h.gh),
                   patch.object(b, 'send_telegram', self.h.telegram)]
        for p in patches: p.start(); self.addCleanup(p.stop)
        self.addCleanup(lambda: setattr(b, 'WORK_CONTEXT', None))

    async def tick(self):
        # These downstream owner/recovery tests explicitly drive both quality
        # gates. Chief runtime tests use raw ticks to verify waiting/rejection.
        await b.tick(self.h.state)
        item = self.h.state['issues'].get('1', {})
        record = item.get('chief_review')
        if item.get('chief_status') == 'AWAITING_CHIEF_REVIEW' and record:
            for _ in range(3):
                if all(self.h.state['outbox'][key]['status'] == 'DONE' for key in record['effects']):
                    break
                await b.tick(self.h.state)
            if all(self.h.state['outbox'][key]['status'] == 'DONE' for key in record['effects']):
                self.h.comment('<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 ' + json.dumps(dict(
                    bundle_id=record['identity']['bundle_id'], verdict='PASS', summary='Chief reviewed', findings=[])) + ' -->')
                await b.tick(self.h.state)

    async def ready(self):
        await self.tick()
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        await self.tick()
        self.h.finish(item['review_task_id'], review_result())
        await self.tick()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        return item

    async def running_review(self):
        await self.tick()
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result())
        await self.tick()
        return item

    def legacy_running_report(self, task_id):
        directory = devctl.task_dir(task_id)
        directory.mkdir(parents=True)
        meta = dict(self.h.tasks[task_id], pid=os.getpid(), process_identity=process_identity(os.getpid()))
        write_json(directory / 'meta.json', meta)
        with redirect_stdout(io.StringIO()) as output:
            devctl.task_report(task_id, as_json=True)
        report = json.loads(output.getvalue())
        self.assertEqual(report['status'], 'RUNNING')
        self.assertEqual(report['liveness']['reason'], 'heartbeat_not_registered')
        self.assertFalse((directory / 'liveness.json').exists())
        return report

    async def test_real_legacy_running_coding_report_keeps_running_projection(self):
        await self.tick()
        item = self.h.state['issues']['1']
        self.h.tasks[item['task_id']].update(self.legacy_running_report(item['task_id']))
        notifications = self.h.telegram.call_count
        comments = len(self.h.comments[1])
        await self.tick()
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertIn('dev:running', b.label_names(self.h.issues[1]))
        self.assertFalse(item.get('worker_uncertain'))
        self.assertNotIn('coding', item.get('runtime_observations', {}))
        self.assertEqual(self.h.telegram.call_count, notifications)
        self.assertEqual(len(self.h.comments[1]), comments)
        self.assertEqual(len(self.h.starts), 1)
        self.assertTrue(self.h.state['repo_locks'])

    async def test_real_legacy_running_review_report_keeps_running_projection(self):
        item = await self.running_review()
        review_id = item['review_task_id']
        self.h.tasks[review_id].update(self.legacy_running_report(review_id))
        notifications = self.h.telegram.call_count
        comments = len(self.h.comments[1])
        await self.tick()
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertIn('dev:running', b.label_names(self.h.issues[1]))
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertFalse(item.get('review_uncertain'))
        self.assertEqual(self.h.telegram.call_count, notifications)
        self.assertEqual(len(self.h.comments[1]), comments)
        self.assertEqual(len(self.h.starts), 2)
        self.assertTrue(self.h.state['repo_locks'])

    async def test_runtime_loss_corrects_external_running_and_retains_uncertain_reservation(self):
        await self.tick()
        item = self.h.state['issues']['1']
        task_id = item['task_id']
        self.h.tasks[task_id].update(status='UNKNOWN', liveness=dict(
            state='LOST_ORPHANED', reason='lease_expired_without_owned_process',
            generation='runtime-turn', transition_sequence=1, lease_state='EXPIRED',
            heartbeat_age_seconds=60, worker_alive=None, process_alive=None, phase='CODING'))
        await self.tick()
        self.assertIn('dev:failed', b.label_names(self.h.issues[1]))
        first_comments = len(self.h.comments[1])
        first_notifications = self.h.telegram.call_count
        self.assertTrue(any('LOST_ORPHANED' in c['body'] for c in self.h.comments[1]))
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.comments[1]), first_comments)
        self.assertEqual(self.h.telegram.call_count, first_notifications)
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(self.h.continues, [])
        self.assertTrue(self.h.state['repo_locks'])
        self.assertEqual(next(iter(self.h.state['work_queue'].values()))['status'], 'UNKNOWN')
        self.assertIsNone(self.h.state['issues']['1'].get('owner_state'))

    async def test_runtime_suspected_reviewer_is_held_until_fresh_owned_heartbeat(self):
        item = await self.running_review()
        review_id = item['review_task_id']
        self.h.tasks[review_id].update(status='UNKNOWN', liveness=dict(
            state='SUSPECTED_STALL', reason='lease_stale_but_process_owned',
            generation='review-runtime', transition_sequence=1, lease_state='EXPIRED',
            heartbeat_age_seconds=60, worker_alive=True, process_alive=True, phase='REVIEW'))
        await self.tick()
        self.assertIn('dev:needs-owner', b.label_names(self.h.issues[1]))
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertTrue(item['review_uncertain'])
        self.assertEqual(len(self.h.starts), 2)
        self.assertEqual(self.h.continues, [])
        self.h.tasks[review_id].update(status='RUNNING', liveness=dict(
            state='RUNNING_HEALTHY', reason='lease_fresh_and_processes_owned',
            generation='review-runtime', transition_sequence=2, lease_state='FRESH',
            heartbeat_age_seconds=1, worker_alive=True, process_alive=True, phase='REVIEW'))
        await self.tick()
        self.assertIn('dev:running', b.label_names(self.h.issues[1]))
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertNotIn('review_uncertain', item)

    async def test_github_discovery_crosses_normalized_boundary_once_per_tick(self):
        observed = []
        poll = GitHubTransport.poll_commands
        def capture(adapter):
            result = poll(adapter)
            observed.extend(result)
            return result
        with patch.object(GitHubTransport, 'poll_commands', autospec=True, side_effect=capture) as polling, \
             patch.object(b, 'gh', wraps=self.h.gh) as io:
            await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(polling.call_count, 2)
        self.assertEqual(sum(c.args[:1] == ('api',) and '/comments?' not in c.args[1] for c in io.call_args_list), 2)
        self.assertEqual(len(self.h.starts), 1)
        first, replay = observed
        self.assertEqual(first.source_id, GitHubTransport(b.REPO).issue_ref(1).source_id())
        self.assertEqual(first.command_id, replay.command_id)
        self.assertEqual(first.payload['body'], self.h.issues[1]['body'])
        self.assertEqual(self.h.last_coding_prompt, b.coding_prompt(b.build_autopilot_prompt('Implement task')))
        self.assertEqual(self.h.state['issues']['1']['queue_status'], 'STARTED')

    async def test_github_poll_failure_is_closed_before_lifecycle_mutation(self):
        await self.tick()
        before = copy.deepcopy(self.h.state)
        with patch.object(b, 'gh', side_effect=OSError('credential-secret')), \
             patch.object(b, 'transport_call', new_callable=AsyncMock) as rpc:
            with self.assertRaises(TransportError) as error: await self.tick()
        self.assertNotIn('credential-secret', str(error.exception))
        rpc.assert_not_awaited()
        self.assertEqual({k: v for k, v in self.h.state.items() if k != 'timings'},
                         {k: v for k, v in before.items() if k != 'timings'})
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 1)

    async def test_github_comment_failure_does_not_consume_followup_or_approval(self):
        item = await self.ready()
        task_id = item['task_id']
        self.h.comment(b.FOLLOWUP_MARKER + '\nFix edge case')
        actual = self.h.gh
        def unavailable(*args):
            if args[0] == 'api' and '/comments?' in args[1]: raise OSError('credential-secret')
            return actual(*args)
        with patch.object(b, 'gh', side_effect=unavailable): await self.tick()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.assertFalse(item.get('processed_followups'))
        self.assertEqual(self.h.continues, [])
        self.h.restart()
        retry = self.h.state['issues']['1']['reconciliation_retry']['next_attempt'] + 1
        with patch.object(b.time, 'time', return_value=retry): await self.tick()
        self.assertEqual([value[0] for value in self.h.continues], [task_id])
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.continues), 1)
        self.assertIsNone(self.h.state['issues']['1']['owner_state'])

    async def test_transport_comments_preserve_all_owner_command_parsing_and_gates(self):
        item = await self.ready()
        actions = ('COMMIT', 'PUSH', 'CREATE_PR', 'MERGE')
        for action in actions:
            data = {'action': ' ' + action.lower() + ' '}
            if action == 'MERGE': data['method'] = 'merge'
            self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 ' + json.dumps(data) + ' -->',
                           created='2020-01-01T00:00:00Z')
        comments = b.issue_comments(1)[-4:]
        for action, comment in zip(actions, comments):
            parsed = b.parse_owner_command(comment['body'])
            self.assertEqual(parsed['action'], action)
            self.assertTrue(b.authorized_comment(comment))
            expected = 'ALLOWED' if action == 'COMMIT' else 'REJECTED'
            self.assertEqual(b.owner_action_plan(item['owner_state'], action)['status'], expected)
        self.assertEqual(b.parse_owner_command(comments[-1]['body'])['method'], 'merge')
        with patch.object(b, 'publish') as publish:
            await self.tick(); self.h.restart(); await self.tick()
        publish.assert_not_called()
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')
        self.assertTrue(all(c['id'] in self.h.state['issues']['1']['processed_owner_actions'] for c in comments))

    async def test_transport_accepted_comment_error_recovers_outbox_without_resend(self):
        key = b.bridge_effects.enqueue(self.h.state, 1, 'comment', 'Result\nExact V1 text')
        b.bridge_effects.enqueue(self.h.state, 1, 'transition', {'issue': self.h.issues[1], 'target': 'dev:running'})
        actual = self.h.gh
        def accepted_then_error(*args):
            result = actual(*args)
            if args[:2] == ('issue', 'comment'): raise OSError('lost response after acceptance')
            return result
        with patch.object(b, 'gh', side_effect=accepted_then_error) as io:
            b.flush_effects(self.h.state)
        self.assertEqual(self.h.state['outbox'][key]['status'], 'PENDING')
        self.assertEqual(sum(c.args[:2] == ('issue', 'comment') for c in io.call_args_list), 1)
        self.assertEqual(sum(c.args[:2] == ('issue', 'edit') for c in io.call_args_list), 1)
        self.assertEqual(self.h.comments[1][0]['body'], 'Result\nExact V1 text\n\n<!-- DEVORCHESTRATOR_EFFECT_V1 ' + key + ' -->')
        self.h.restart()
        with patch.object(b.time, 'time', return_value=self.h.state['outbox'][key]['next_attempt'] + 1), \
             patch.object(b, 'gh', wraps=self.h.gh) as io:
            b.flush_effects(self.h.state); b.flush_effects(self.h.state)
        self.assertFalse(any(c.args[:2] == ('issue', 'comment') for c in io.call_args_list))
        self.assertEqual(len(self.h.comments[1]), 1)
        self.assertEqual(self.h.issues[1]['labels'], ['dev:running'])
        self.assertTrue(all(e['status'] == 'DONE' for e in self.h.state['outbox'].values()))

    async def test_transport_accepted_label_error_reconciles_without_duplicate_effects(self):
        b.bridge_effects.enqueue(self.h.state, 1, 'comment', 'Result')
        key = b.bridge_effects.enqueue(self.h.state, 1, 'transition',
            {'issue': self.h.issues[1], 'target': 'dev:running'})
        actual = self.h.gh
        def accepted_then_error(*args):
            result = actual(*args)
            if args[:2] == ('issue', 'edit'): raise OSError('lost response after acceptance')
            return result
        with patch.object(b, 'gh', side_effect=accepted_then_error): b.flush_effects(self.h.state)
        self.assertEqual(self.h.state['outbox'][key]['status'], 'PENDING')
        self.assertEqual(self.h.issues[1]['labels'], ['dev:running'])
        self.h.restart()
        with patch.object(b.time, 'time', return_value=self.h.state['outbox'][key]['next_attempt'] + 1), \
             patch.object(b, 'gh', wraps=self.h.gh) as io:
            b.flush_effects(self.h.state); b.flush_effects(self.h.state)
        self.assertFalse(any(c.args[:2] in (('issue', 'edit'), ('issue', 'comment')) for c in io.call_args_list))
        self.assertEqual(len(self.h.comments[1]), 1)
        self.assertTrue(all(e['status'] == 'DONE' for e in self.h.state['outbox'].values()))

    async def test_transport_readonly_noop_completes_review_without_commit_invitation(self):
        await self.tick()
        item = self.h.state['issues']['1']
        # A read-only coding task leaves exactly its initial repository snapshot.
        (self.h.repo / 'file').write_text('baseline')
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.assertEqual(self.h.tasks[item['review_task_id']]['role'], 'review')
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.h.restart(); await self.tick(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.assertFalse(b.notifications.publication_delta_present(item, self.h.tasks[item['task_id']]))
        final = [c.args[0] for c in self.h.telegram.call_args_list if 'REVIEW_PASSED' in c.args[0]]
        # Operational notification hygiene: a no-op completion has no owner
        # action; it remains visible through durable/operator status only.
        self.assertEqual(final, [])
        markers = [c['body'].split('DEVORCHESTRATOR_EFFECT_V1 ')[1].split(' -->')[0]
                   for c in self.h.comments[1] if 'DEVORCHESTRATOR_EFFECT_V1 ' in c['body']]
        self.assertEqual(len(markers), len(set(markers)))
        self.assertEqual(self.h.issues[1]['labels'], ['dev:ready-for-review'])
        self.assertEqual(len(self.h.starts), 2)

    async def test_stale_ready_on_running_followup_waits_for_new_terminal_turn(self):
        item = await self.ready()
        old_reviewer = item['review_task_id']; coding_id = item['task_id']
        self.h.comment(b.FOLLOWUP_MARKER + '\nFix the coding/review race')
        async def transport(name, args):
            result = await self.h.transport(name, args)
            if name == 'continue_task' and args['task_id'] == coding_id:
                self.h.tasks[coding_id]['result'] = coding_result()
                result['result'] = coding_result()
            return result
        with patch.object(b, 'transport_call', transport):
            await self.tick()
        self.assertEqual(self.h.tasks[coding_id]['status'], 'RUNNING')
        self.assertIsNone(item['review_task_id'])
        self.assertIsNone(item['owner_state'])
        self.assertIsNone(item['reviewed_tree'])
        self.assertNotIn('review_coding_turn', item)
        self.assertEqual(len(self.h.starts), 2)
        self.h.restart(); await self.tick(); await self.tick()
        self.assertIsNone(self.h.state['issues']['1']['review_task_id'])
        self.assertEqual(len(self.h.starts), 2)
        self.h.finish(coding_id, coding_result()); await self.tick()
        item = self.h.state['issues']['1']; fresh = item['review_task_id']
        self.assertNotEqual(fresh, old_reviewer)
        self.assertEqual(item['review_coding_turn'], 2)
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 3)
        self.h.finish(fresh, review_result()); await self.tick()
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')

    async def test_old_reviewer_pass_is_ignored_while_coding_has_resumed(self):
        item = await self.running_review(); old = item['review_task_id']; coder = item['task_id']
        self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
        self.h.finish(old, review_result())
        self.h.restart(); await self.tick(); self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertIsNone(item['review_task_id'])
        self.assertIsNone(item['owner_state'])
        self.assertIn(old, item['stale_review_task_ids'])
        self.assertEqual(len(self.h.starts), 2)
        self.assertFalse(any('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list))
        self.h.finish(coder, coding_result()); await self.tick()
        self.assertNotEqual(item['review_task_id'], old)
        self.assertEqual(item['review_coding_turn'], 2)
        self.assertEqual(len(self.h.starts), 3)

    async def test_invalidated_reviewer_retains_lock_until_terminal(self):
        item = await self.running_review(); old = item['review_task_id']; coder = item['task_id']
        self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
        await self.tick()
        self.h.finish(coder, coding_result()); await self.tick()
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 2)
        self.assertFalse(scheduler.Scheduler(self.h.state).release('1'))
        self.assertIn(old, self.h.state['issues']['1']['stale_review_task_ids'])
        self.h.finish(old, review_result()); await self.tick()
        self.assertEqual(len(self.h.starts), 3)
        self.assertNotEqual(self.h.state['issues']['1']['review_task_id'], old)

    async def test_coding_resume_during_reviewer_report_prevents_pass(self):
        item = await self.running_review(); old = item['review_task_id']; coder = item['task_id']
        self.h.finish(old, review_result())
        async def transport(name, args):
            result = await self.h.transport(name, args)
            if name == 'get_task_report' and args['task_id'] == old:
                self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
            return result
        with patch.object(b, 'transport_call', transport):
            await self.tick()
        self.assertIsNone(item['review_task_id'])
        self.assertIsNone(item['owner_state'])
        self.assertIsNone(item['reviewed_tree'])
        self.assertFalse(any('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list))

    async def test_coding_resume_before_format_rpc_prevents_reviewer_continue(self):
        item = await self.running_review(); coder = item['task_id']
        self.h.finish(item['review_task_id'], 'Malformed review')
        coding_reads = 0
        async def transport(name, args):
            nonlocal coding_reads
            if name == 'get_task_report' and args['task_id'] == coder:
                coding_reads += 1
                if coding_reads == 4:
                    self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
            return await self.h.transport(name, args)
        with patch.object(b, 'transport_call', transport):
            await self.tick()
        self.assertEqual(self.h.continues, [])
        self.assertIsNone(item['review_task_id'])
        self.assertIsNone(item['owner_state'])

    async def test_pending_review_launch_cancelled_on_new_turn_uses_fresh_request(self):
        await self.tick(); item = self.h.state['issues']['1']; coder = item['task_id']
        self.h.finish(coder, coding_result()); self.h.fail_after_start = True
        await self.tick()
        old = self.h.starts[-1][1]
        self.assertEqual(item['pending_rpc']['operation'], 'review:1')
        next_attempt = item['reconciliation_retry']['next_attempt']
        self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertFalse(item.get('pending_rpc'))
        self.assertIn(old, item['stale_review_task_ids'])
        self.h.finish(old, review_result()); self.h.finish(coder, coding_result())
        with patch.object(b.time, 'time', return_value=next_attempt + 1):
            await self.tick()
        self.assertEqual(len(self.h.starts), 3)
        self.assertNotEqual(item['review_task_id'], old)
        self.assertEqual(item['review_round'], 2)
        self.assertEqual(item['review_coding_turn'], 2)
        self.assertIsNone(item['owner_state'])

    async def test_cancelled_format_rpc_drains_finished_turn_after_restart(self):
        item = await self.running_review(); old = item['review_task_id']; coder = item['task_id']
        self.h.finish(old, 'Malformed review')
        async def transport(name, args):
            if name == 'continue_task' and args.get('role') == 'review':
                # Crash after durable reservation, before dispatching the format RPC.
                raise asyncio.CancelledError()
            return await self.h.transport(name, args)
        with patch.object(b, 'transport_call', transport), self.assertRaises(asyncio.CancelledError):
            await self.tick()
        self.assertTrue(item['pending_rpc']['operation'].startswith('review-format:'))
        reserved = next(j for j in self.h.state['work_queue'].values() if j['status'] == 'RESERVED')
        self.assertTrue(reserved['launch_pending'])
        self.assertEqual(self.h.tasks[old]['turns'], 1)
        self.assertEqual(self.h.continues, [])
        self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertFalse(item.get('pending_rpc'))
        self.assertIsNone(item['review_task_id'])
        self.assertIn(old, item['stale_review_task_ids'])
        self.h.restart()
        self.h.finish(coder, coding_result()); await self.tick()
        item = self.h.state['issues']['1']; fresh = item['review_task_id']
        cancelled = [j for j in self.h.state['work_queue'].values() if j.get('cancelled')]
        self.assertEqual(len(cancelled), 1)
        self.assertEqual(cancelled[0]['status'], 'DONE')
        self.assertFalse(cancelled[0]['launch_pending'])
        self.assertFalse(item.get('stale_review_task_ids'))
        self.assertNotEqual(fresh, old)
        self.assertEqual(item['review_coding_turn'], 2)
        self.assertEqual(len(self.h.starts), 3)
        self.assertEqual(self.h.continues, [])
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 3)
        self.h.finish(fresh, review_result()); await self.tick()
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')
        self.assertTrue(scheduler.Scheduler(self.h.state).release('1'))

    async def test_cancelled_accepted_format_rpc_keeps_active_reviewer_locked(self):
        item = await self.running_review(); old = item['review_task_id']; coder = item['task_id']
        self.h.finish(old, 'Malformed review'); self.h.fail_after_start = True
        await self.tick()
        self.assertTrue(item['pending_rpc']['operation'].startswith('review-format:'))
        next_attempt = item['reconciliation_retry']['next_attempt']
        self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
        self.h.restart(); await self.tick()
        self.h.finish(coder, coding_result())
        with patch.object(b.time, 'time', return_value=next_attempt + 1):
            await self.tick()
        item = self.h.state['issues']['1']
        cancelled = next(j for j in self.h.state['work_queue'].values() if j.get('cancelled'))
        self.assertEqual(cancelled['status'], 'RUNNING')
        self.assertFalse(cancelled['launch_pending'])
        self.assertIn(old, item['stale_review_task_ids'])
        self.assertIsNone(item['review_task_id'])
        self.assertFalse(scheduler.Scheduler(self.h.state).release('1'))
        self.h.tasks[old]['status'] = 'UNKNOWN'
        self.h.restart(); await self.tick()
        self.assertIsNone(self.h.state['issues']['1']['review_task_id'])
        self.assertFalse(scheduler.Scheduler(self.h.state).release('1'))
        self.h.finish(old, review_result()); await self.tick()
        self.assertNotEqual(self.h.state['issues']['1']['review_task_id'], old)
        self.assertEqual(len(self.h.starts), 3)
        self.assertEqual(len(self.h.continues), 1)

    async def test_preupgrade_cancelled_format_job_drains_without_expected_turn(self):
        item = await self.running_review(); old = item['review_task_id']; coder = item['task_id']
        self.h.finish(old, 'Malformed review'); self.h.fail_after_start = True
        await self.tick()
        next_attempt = item['reconciliation_retry']['next_attempt']
        self.h.tasks[coder].update(status='RUNNING', turns=2, result=coding_result())
        await self.tick()
        # Reproduce persisted cancellation from the earlier bridge implementation.
        cancelled = next(j for j in self.h.state['work_queue'].values() if j.get('cancelled'))
        cancelled.pop('cancelled'); cancelled['cancelled_review_turn'] = 2
        self.h.tasks[old].update(status='DONE', turns=1, result=review_result())
        item['stale_review_diagnostic'] = 'Invalidated reviewer launch is uncertain'
        b.save_state(self.h.state); self.h.restart()
        self.h.finish(coder, coding_result())
        with patch.object(b.time, 'time', return_value=next_attempt + 1):
            await self.tick()
        item = self.h.state['issues']['1']
        self.assertNotEqual(item['review_task_id'], old)
        self.assertFalse(item.get('stale_review_task_ids'))
        self.assertFalse(item.get('stale_review_diagnostic'))
        self.assertFalse(any(j.get('cancelled_review_turn') for j in self.h.state['work_queue'].values()))
        self.assertEqual(len(self.h.starts), 3)

    async def test_task_list_outage_flushes_durable_github_and_telegram_effects(self):
        for error in (OSError('transport unavailable'), RuntimeError('Invalid MCP task collection')):
            with self.subTest(error=type(error).__name__):
                number = 1
                comment = 'Recovery diagnostic: ' + type(error).__name__
                keys = [b.bridge_effects.enqueue(self.h.state, number, 'comment', comment),
                        b.bridge_effects.enqueue(self.h.state, number, 'transition',
                            {'issue': self.h.issues[number], 'target': 'dev:running'}, comment),
                        b.bridge_effects.enqueue(self.h.state, number, 'telegram', comment)]
                uncertain = b.bridge_effects.enqueue(self.h.state, number, 'telegram', 'Already sending: ' + comment)
                self.h.state['outbox'][uncertain]['status'] = 'SENDING'
                b.save_state(self.h.state); self.h.restart()
                async def unavailable(name, args):
                    self.assertEqual(name, 'list_tasks')
                    raise error
                with patch.object(b, 'transport_call', unavailable):
                    with self.assertRaises(type(error)):
                        await self.tick()
                    self.h.restart()
                    with self.assertRaises(type(error)):
                        await self.tick()
                self.assertTrue(all(self.h.state['outbox'][k]['status'] == 'DONE' for k in keys))
                self.assertEqual(self.h.state['outbox'][uncertain]['status'], 'UNCERTAIN')
                self.assertFalse(any(c.args[0].startswith('Already sending:') for c in self.h.telegram.call_args_list))
                self.assertEqual(sum(comment in c['body'] for c in self.h.comments[number]), 1)
                self.assertEqual(sum(c.args[0] == comment for c in self.h.telegram.call_args_list), 1)
                self.assertEqual(self.h.issues[number]['labels'], ['dev:running'])
                self.assertEqual(self.h.starts, [])
                self.assertIsNone(b.WORK_CONTEXT)

    async def test_outage_defers_approval_notice_until_successful_reconciliation(self):
        await self.ready()
        approvals = [e for e in self.h.state['outbox'].values() if
                     (e['kind'] == 'telegram' and e['payload'].startswith('REVIEW_PASSED')) or
                     (e['kind'] == 'transition' and e['payload']['target'] == 'dev:ready-for-review')]
        for entry in approvals:
            entry['status'] = 'PENDING'
        self.h.telegram.reset_mock()
        other = b.bridge_effects.enqueue(self.h.state, 1, 'telegram', 'Independent recovery notice')
        with patch.object(b, 'transport_call', AsyncMock(side_effect=ValueError('Bad task list'))):
            with self.assertRaises(ValueError):
                await self.tick()
        self.assertTrue(all(e['status'] == 'PENDING' for e in approvals))
        self.assertTrue(all(e['attempts'] == 0 for e in approvals))
        self.assertEqual(self.h.state['outbox'][other]['status'], 'DONE')
        self.assertEqual(self.h.telegram.call_args_list[0].args[0], 'Independent recovery notice')
        self.h.restart(); await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(sum('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list), 1)

    async def test_outage_does_not_send_stale_approval_before_resumed_coding_is_observed(self):
        item = await self.ready()
        for entry in self.h.state['outbox'].values():
            if entry['kind'] == 'telegram' and entry['payload'].startswith('REVIEW_PASSED'):
                entry['status'] = 'PENDING'
        self.h.tasks[item['task_id']].update(status='RUNNING', turns=2, result=coding_result())
        self.h.telegram.reset_mock()
        with patch.object(b, 'transport_call', AsyncMock(side_effect=OSError('Task service outage'))):
            with self.assertRaises(OSError):
                await self.tick()
        self.h.telegram.assert_not_called()
        self.h.restart(); await self.tick()
        self.assertFalse(any('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list))
        self.assertIsNone(self.h.state['issues']['1']['owner_state'])

    async def test_new_terminal_turn_invalidates_review_without_running_poll(self):
        item = await self.running_review(); old = item['review_task_id']
        self.h.tasks[item['task_id']].update(status='DONE', turns=2, result=coding_result())
        self.h.finish(old, review_result())
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertNotEqual(item['review_task_id'], old)
        self.assertEqual(item['review_coding_turn'], 2)
        self.assertIsNone(item['owner_state'])
        self.assertFalse(any('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list))

    async def test_unbound_legacy_reviewer_72624bcb32db_cannot_approve(self):
        item = await self.running_review(); old = item['review_task_id']; legacy = '72624bcb32db'
        self.h.tasks[legacy] = self.h.tasks.pop(old); self.h.tasks[legacy]['id'] = legacy
        for job in self.h.state['work_queue'].values():
            if job.get('task_id') == old:
                job['task_id'] = legacy
        item['review_task_id'] = legacy; item.pop('review_coding_turn')
        self.h.finish(legacy, review_result())
        b.save_state(self.h.state); self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertNotEqual(item['review_task_id'], legacy)
        self.assertIsNone(item['owner_state'])
        self.assertEqual(len(self.h.starts), 3)
        self.assertFalse(any('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list))

    async def test_pending_pass_notice_suppressed_when_coding_is_uncertain(self):
        item = await self.ready()
        for entry in self.h.state['outbox'].values():
            if entry['kind'] == 'telegram' and entry['payload'].startswith('REVIEW_PASSED'):
                entry['status'] = 'PENDING'
        self.h.tasks[item['task_id']]['status'] = 'UNKNOWN'
        self.h.telegram.reset_mock()
        await self.tick()
        messages = [c.args[0] for c in self.h.telegram.call_args_list]
        self.assertFalse(any('REVIEW_PASSED' in m or 'Next owner action: COMMIT' in m for m in messages))

    async def test_malformed_review_turn_and_stale_ids_do_not_poison_tick(self):
        for number, fields in enumerate(({'coding_turn': 0}, {'review_coding_turn': 'invalid'},
                                        {'stale_review_task_ids': {'bad': 'shape'}},
                                        {'stale_review_task_ids': ['../unregistered']},
                                        {'review_task_id': ['bad']},
                                        {'pending_rpc': {'name': 'start_task', 'operation': 'review:1', 'arguments': {}}},
                                        {'pending_rpc': {'name': 'continue_task', 'operation': 'review-format:old:1', 'arguments': {}}}), start=5):
            item = self.legacy(number)
            item.update(fields)
        await self.tick(); self.h.restart(); await self.tick()
        for number in range(5, 12):
            self.assertEqual(self.h.state['issues'][str(number)]['compatibility_status'], 'QUARANTINED')
        self.assertEqual(self.h.state['issues']['1']['queue_status'], 'STARTED')
        self.assertEqual(len(self.h.starts), 1)

    async def test_historical_pass_without_coding_binding_holds_publication(self):
        item = await self.ready()
        item.pop('review_coding_turn'); item.pop('review_start_tree')
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        self.h.telegram.reset_mock()
        await self.tick(); self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertEqual(item['review_status'], 'FRESH_REVIEW_REQUIRED')
        self.assertIn('no verified coding-turn binding', item['review_hold']['diagnostic'])
        self.assertFalse(item.get('processed_owner_actions'))
        self.assertEqual(len(self.h.starts), 2)
        self.assertFalse(any('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list))

    async def pending_format_fallback(self):
        item = await self.running_review()
        reviewer = item['review_task_id']
        self.h.finish(reviewer, review_result() + ' extra trailing data')
        await self.tick()
        self.assertEqual(item['review_format_corrections'], 1)
        self.h.finish(reviewer, 'The review is complete, but the trailer is missing.')
        await self.tick()
        return item, reviewer

    async def test_same_session_format_correction_can_pass_without_fallback(self):
        item = await self.running_review()
        reviewer = item['review_task_id']
        self.h.finish(reviewer, 'DEVORCHESTRATOR_REVIEW: {invalid JSON')
        await self.tick()
        self.assertEqual(self.h.continues[0][0], reviewer)
        self.assertIsNone(item.get('owner_state'))
        self.assertNotIn('REVIEW_PASSED', '\n'.join(c.args[0] for c in self.h.telegram.call_args_list))
        self.h.finish(reviewer, review_result()); await self.tick()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.assertEqual(item['review_format_restarts'], 0)
        self.assertEqual(len(self.h.starts), 2)

    async def test_missing_trailer_after_retry_persists_fresh_reviewer_intent(self):
        item, old_reviewer = await self.pending_format_fallback()
        persisted = read_json(b.STATE_FILE)['issues']['1']
        self.assertEqual(persisted['review_format_restarts'], 1)
        self.assertEqual(persisted['review_status'], 'RUNNING')
        self.assertIsNone(persisted['review_task_id'])
        self.assertIsNone(persisted.get('owner_state'))
        self.assertIsNone(persisted.get('reviewed_tree'))
        self.assertTrue(persisted['review_start_tree'])
        self.assertFalse(persisted.get('pending_rpc'))
        self.assertEqual(len(self.h.starts), 2)
        self.assertEqual([task for task, _ in self.h.continues], [old_reviewer])
        self.assertEqual(self.h.issues[1]['labels'], ['dev:running'])
        self.assertFalse(scheduler.Scheduler(self.h.state).available(dict(
            owner='2', kind='coding', machine='work', repo=next(iter(self.h.state['repo_locks'])))))

    async def test_fresh_reviewer_fallback_passes_without_resuming_coding(self):
        item, old_reviewer = await self.pending_format_fallback()
        snapshot = copy.deepcopy(item['review_start_tree'])
        coding_id = item['task_id']
        with patch.object(b, 'transport_call', wraps=self.h.transport) as transport:
            await self.tick()
        starts = [c.args[1] for c in transport.call_args_list if c.args[0] == 'start_task']
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]['role'], 'review')
        self.assertEqual(starts[0]['agent'], 'cursor')
        self.assertIn(b.review_contract(), starts[0]['prompt'])
        self.assertIn('- Do NOT edit source files.', starts[0]['prompt'])
        self.assertIn('- Do NOT implement fixes.', starts[0]['prompt'])
        new_reviewer = item['review_task_id']
        self.assertNotEqual(new_reviewer, old_reviewer)
        self.assertEqual(item['review_round'], 2)
        self.assertEqual(item['review_format_corrections'], 0)
        self.assertEqual(item['review_start_tree'], snapshot)
        self.h.finish(new_reviewer, review_result()); await self.tick()
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertTrue(b.notifications.review_approval_complete(item))
        self.assertEqual(item['task_id'], coding_id)
        self.assertEqual(item['review_format_restarts'], 1)
        self.assertEqual([task for task, _ in self.h.continues], [old_reviewer])
        self.assertEqual(len(self.h.starts), 3)
        self.assertEqual(sum('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list), 1)
        self.assertFalse(self.h.state['repo_locks'])

    async def test_bridge_restart_preserves_pending_and_running_format_fallback(self):
        item, old_reviewer = await self.pending_format_fallback()
        self.h.issues[2] = self.h.issue(2); self.h.comments[2] = []
        self.h.comment(b.FOLLOWUP_MARKER + '\nWait for the fresh review')
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']; new_reviewer = item['review_task_id']
        self.h.restart(); await self.tick(); await self.tick()
        item = self.h.state['issues']['1']
        self.assertEqual(item['review_task_id'], new_reviewer)
        self.assertNotEqual(new_reviewer, old_reviewer)
        self.assertEqual(item['review_format_restarts'], 1)
        self.assertEqual(len(self.h.starts), 3)
        self.assertEqual([task for task, _ in self.h.continues], [old_reviewer])
        self.assertFalse(item.get('processed_owner_actions'))
        self.assertEqual(self.h.state['issues']['2']['queue_status'], 'QUEUED')
        self.assertFalse(scheduler.Scheduler(self.h.state).release('1'))

    async def test_fresh_reviewer_start_recovers_after_accepted_rpc_interruption(self):
        item, old_reviewer = await self.pending_format_fallback()
        self.h.fail_after_start = True
        await self.tick()
        self.assertEqual(item['pending_rpc']['operation'], 'review:2')
        next_attempt = item['reconciliation_retry']['next_attempt']
        self.h.restart()
        with patch.object(b.time, 'time', return_value=next_attempt + 1):
            await self.tick()
        item = self.h.state['issues']['1']
        self.assertFalse(item.get('pending_rpc'))
        self.assertEqual(item['review_format_restarts'], 1)
        self.assertNotEqual(item['review_task_id'], old_reviewer)
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 3)

    async def test_fresh_reviewer_receipt_survives_crash_before_completion_state_save(self):
        await self.pending_format_fallback()
        with patch.object(b, 'complete_rpc', side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await self.tick()
        persisted = read_json(b.STATE_FILE)['issues']['1']
        self.assertIsNone(persisted['review_task_id'])
        self.assertEqual(persisted['pending_rpc']['operation'], 'review:2')
        self.h.restart(); await self.tick()
        new_reviewer = self.h.state['issues']['1']['review_task_id']
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 3)
        self.assertEqual(self.h.state['issues']['1']['review_task_id'], new_reviewer)

    async def test_every_malformed_review_attempt_reaches_bounded_terminal_failure(self):
        item, old_reviewer = await self.pending_format_fallback()
        await self.tick(); new_reviewer = item['review_task_id']
        self.h.finish(new_reviewer, 'DEVORCHESTRATOR_REVIEW: {invalid JSON'); await self.tick()
        self.assertEqual(item['review_format_corrections'], 1)
        self.h.finish(new_reviewer, 'Still missing the required trailer'); await self.tick()
        self.assertEqual(item['review_status'], 'FAILED')
        self.assertIsNone(item.get('owner_state'))
        self.assertIsNone(item.get('reviewed_tree'))
        self.assertIn('2 independent sessions', item['owner_blocker'])
        self.assertEqual(item['review_format_restarts'], 1)
        for _ in range(3):
            self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 3)
        self.assertEqual([task for task, _ in self.h.continues], [old_reviewer, new_reviewer])
        self.assertEqual(self.h.issues[1]['labels'], ['dev:failed'])
        messages = [c.args[0] for c in self.h.telegram.call_args_list]
        self.assertEqual(sum(m.startswith('FAILED') for m in messages), 1)
        self.assertFalse(any('REVIEW_PASSED' in m for m in messages))
        self.assertFalse(self.h.state['repo_locks'])

    async def test_format_fallback_cannot_recapture_a_changed_review_tree(self):
        item, _ = await self.pending_format_fallback()
        snapshot = copy.deepcopy(item['review_start_tree'])
        (self.h.repo / 'file').write_text('silent change during malformed review')
        await self.tick()
        self.assertEqual(item['review_start_tree'], snapshot)
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.assertEqual(item['review_status'], 'FAILED')
        self.assertIsNone(item.get('owner_state'))
        self.assertIn('Repository changed', item['owner_blocker'])

    async def test_coding_correction_resets_format_fallback_budget_after_rpc_recovery(self):
        item, _ = await self.pending_format_fallback()
        await self.tick()
        self.h.finish(item['review_task_id'], review_result('CHANGES_REQUIRED', findings=['Fix defect']))
        self.h.fail_after_start = True
        await self.tick()
        next_attempt = item['reconciliation_retry']['next_attempt']
        self.h.restart()
        with patch.object(b.time, 'time', return_value=next_attempt + 1):
            await self.tick()
        item = self.h.state['issues']['1']
        self.assertEqual(item['review_format_restarts'], 0)
        self.assertEqual(item['review_corrections'], 1)
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        reviewer = item['review_task_id']
        for _ in range(2):
            self.h.finish(reviewer, 'No trailer'); await self.tick()
        self.assertEqual(item['review_format_restarts'], 1)
        self.assertIsNone(item['review_task_id'])
        self.assertEqual(item['review_status'], 'RUNNING')
        await self.tick()
        self.assertEqual(item['review_round'], 4)
        self.assertNotEqual(item['review_task_id'], reviewer)

    async def test_production_startup_admits_only_dedicated_work_queue(self):
        self.assertEqual(b.QUEUE_LABEL, 'work:queued')
        self.assertIn('work:queued', b.STATE_LABELS)
        self.assertNotIn('dev:queued', b.STATE_LABELS)
        self.h.issues[1]['labels'] = ['work:queued']
        for number, label in ((2, 'dev:queued'), (3, 'home:queued'), (4, 'unrelated:queued')):
            self.h.issues[number] = self.h.issue(number)
            self.h.issues[number]['labels'] = [label]
            self.h.comments[number] = []
        sleep = asyncio.sleep
        cycles = 0
        async def drive_cycle(delay):
            nonlocal cycles
            cycles += 1
            if self.h.starts or cycles > 20:
                raise asyncio.CancelledError
            await sleep(.01)
        with patch.dict('os.environ', {
            'DEVORCHESTRATOR_WORKER': 'work',
            'DEVORCHESTRATOR_WORK_HOST': 'siovush-Thin-15-B12UC',
            'DEVORCHESTRATOR_QUEUE_LABEL': 'work:queued',
        }), patch('lifecycle.socket.gethostname', return_value='siovush-Thin-15-B12UC'), \
             patch.object(b, 'load_state', return_value=self.h.state), \
             patch.object(b.asyncio, 'sleep', new=AsyncMock(side_effect=drive_cycle)):
            with self.assertRaises(asyncio.CancelledError):
                await b.main()
        self.assertEqual(set(self.h.state['issues']), {'1'})
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(self.h.issues[1]['labels'], ['dev:running'])

    async def test_invalid_startup_fails_before_loading_state_or_admitting_tasks(self):
        valid = {
            'DEVORCHESTRATOR_WORKER': 'work',
            'DEVORCHESTRATOR_WORK_HOST': 'siovush-Thin-15-B12UC',
            'DEVORCHESTRATOR_QUEUE_LABEL': 'work:queued',
        }
        for key, value in (
            ('DEVORCHESTRATOR_WORKER', None),
            ('DEVORCHESTRATOR_WORKER', 'home'),
            ('DEVORCHESTRATOR_WORK_HOST', None),
            ('DEVORCHESTRATOR_WORK_HOST', 'HOME'),
            ('DEVORCHESTRATOR_QUEUE_LABEL', None),
            ('DEVORCHESTRATOR_QUEUE_LABEL', 'dev:queued'),
            ('DEVORCHESTRATOR_QUEUE_LABEL', 'unrelated:queued'),
        ):
            env = dict(valid)
            if value is None:
                del env[key]
            else:
                env[key] = value
            with self.subTest(key=key, value=value), patch.dict('os.environ', env, clear=True), \
                 patch('lifecycle.socket.gethostname', return_value='siovush-Thin-15-B12UC'), \
                 patch.object(b, 'load_state') as load, patch.object(b, 'tick', new_callable=AsyncMock) as tick:
                with self.assertRaises(RuntimeError):
                    await b.main()
                load.assert_not_called()
                tick.assert_not_awaited()

    async def test_running_coding_restart_no_duplicate(self):
        await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(self.h.issues[1]['labels'], ['dev:running'])

    async def test_workspace_recovery_failure_does_not_abort_tick(self):
        resolve = scheduler.project_identity

        def resolve_project(project):
            if project == 'offline':
                raise OSError('registered worker unavailable')
            return resolve(project)

        async def transport(name, args):
            result = await self.h.transport(name, args)
            if name == 'list_tasks':
                result.append({'id': 'external-offline', 'project': 'offline', 'status': 'RUNNING'})
            return result

        with patch.dict('os.environ', {'DEVORCHESTRATOR_MAX_CODING': '2'}), \
             patch.object(devctl, 'load_projects', return_value={
                 'test': {'machine': 'work', 'workspace': str(self.h.repo)},
                 'offline': {'machine': 'firefly', 'workspace': '/registered/offline'}}), \
             patch.object(scheduler, 'project_identity', side_effect=resolve_project), \
             patch.object(b, 'transport_call', transport):
            await self.tick()
            item = self.h.state['issues']['1']
            self.assertEqual(len(self.h.starts), 1)
            self.assertEqual(self.h.state['work_queue']['external:external-offline']['status'], 'UNKNOWN')
            self.h.finish(item['task_id'], coding_result('NEEDS_OWNER', 'Choose database'))
            self.h.issues[2] = self.h.issue(2); self.h.comments[2] = []
            await self.tick()
            self.assertEqual(self.h.issues[1]['labels'], ['dev:needs-owner'])
            self.h.restart(); await self.tick()
            self.assertEqual(len(self.h.starts), 2)
            self.assertIn('task_id', self.h.state['issues']['2'])
            self.assertTrue(self.h.state['work_queue']['external:external-offline']['identity_pending'])

    async def test_start_rpc_interruption_recovered_without_duplicate(self):
        self.h.fail_after_start = True
        await self.tick(); self.h.restart()
        with patch.object(b.time, 'time', return_value=self.h.state['issues']['1']['reconciliation_retry']['next_attempt'] + 1):
            await self.tick()
        self.assertEqual(len(self.h.starts), 1)
        self.assertIn('task_id', self.h.state['issues']['1'])
        self.assertEqual(len([j for j in self.h.state['work_queue'].values() if j['status'] == 'RUNNING']), 1)

    async def test_review_running_and_pass_restart(self):
        await self.tick()
        item = self.h.state['issues']['1']; self.h.finish(item['task_id'], coding_result())
        self.h.restart(); await self.tick()
        item = self.h.state['issues']['1']; review_id = item['review_task_id']
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 2)
        self.h.finish(review_id, review_result()); await self.tick()
        self.h.restart(); await self.tick()
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')
        self.assertEqual(len(self.h.starts), 2)
        self.assertEqual(sum('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list), 1)

    async def test_coding_needs_owner_answer_full_loop_same_session(self):
        await self.tick(); item = self.h.state['issues']['1']; task_id = item['task_id']
        self.h.finish(task_id, coding_result('NEEDS_OWNER', 'Which database?')); await self.tick()
        self.assertEqual(self.h.issues[1]['labels'], ['dev:needs-owner'])
        self.assertIn('Which database?', self.h.telegram.call_args.args[0])
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"SQLite"} -->')
        await self.tick(); self.assertEqual(self.h.continues[0][0], task_id)
        self.h.finish(task_id, coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')

    async def test_reviewer_needs_owner_answer_full_loop(self):
        await self.tick(); item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result('NEEDS_OWNER', 'Choose schema')); await self.tick()
        self.assertEqual(self.h.issues[1]['labels'], ['dev:needs-owner'])
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Schema A"} -->')
        await self.tick(); self.assertIsNone(item['review_status'])
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.assertEqual(item['review_round'], 2)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')

    async def test_followup_interruption_invalidates_before_resume_and_restart(self):
        item = await self.ready()
        self.h.comment(b.FOLLOWUP_MARKER + '\nFix edge case')
        self.h.fail_after_start = True
        await self.tick()
        self.assertIsNone(item['owner_state'])
        self.assertIsNone(item['reviewed_tree'])
        self.h.restart()
        with patch.object(b.time, 'time', return_value=item['reconciliation_retry']['next_attempt'] + 1):
            await self.tick()
        self.assertEqual(len(self.h.continues), 1)
        item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.assertEqual(item['review_round'], 2)

    async def test_post_review_silent_change_prevents_commit(self):
        item = await self.ready(); head = self.h.git('rev-parse', 'HEAD')
        (self.h.repo / 'file').write_text('silent')
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        await self.tick()
        self.assertEqual(self.h.git('rev-parse', 'HEAD'), head)
        self.assertIsNone(item['owner_state'])
        self.assertEqual(item['chief_status'], 'STALE')

    async def test_unresolved_recovery_hold_prevents_owner_publication(self):
        item = await self.ready()
        head = self.h.git('rev-parse', 'HEAD')
        resolve = scheduler.project_identity

        def resolve_project(project):
            if project == 'offline':
                raise OSError('workspace lookup unavailable')
            return resolve(project)

        async def transport(name, args):
            result = await self.h.transport(name, args)
            if name == 'list_tasks':
                result.append({'id': 'unknown-repo', 'project': 'offline', 'status': 'RUNNING'})
            return result

        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        with patch.object(devctl, 'load_projects', return_value={
                 'test': {'machine': 'work', 'workspace': str(self.h.repo)},
                 'offline': {'machine': 'work', 'workspace': '/registered/offline'}}), \
             patch.object(scheduler, 'project_identity', side_effect=resolve_project), \
             patch.object(b, 'transport_call', transport):
            await self.tick()
        self.assertEqual(self.h.git('rev-parse', 'HEAD'), head)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.assertTrue(any('active or uncertain' in c['body'] for c in self.h.comments[1]))

    async def test_commit_restart_duplicate_and_push_skip(self):
        item = await self.ready()
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"PUSH"} -->'); await self.tick()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT","message":"Reviewed fix"} -->'); await self.tick()
        self.assertEqual(item['owner_state'], 'COMMITTED')
        sha = self.h.git('rev-parse', 'HEAD')
        self.h.restart(); await self.tick()
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->'); await self.tick()
        self.assertEqual(self.h.git('rev-parse', 'HEAD'), sha)
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'COMMITTED')

    async def test_old_owner_command_cannot_authorize_new_review(self):
        item = await self.ready()
        self.h.comment(b.FOLLOWUP_MARKER + '\nFix edge case', created='2020-01-01T00:00:00Z')
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->', created='2020-01-01T00:00:00Z')
        await self.tick(); self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result()); await self.tick(); await self.tick()
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')

    async def test_queue_same_repo_wait_and_release_after_failure(self):
        self.h.issues[2] = self.h.issue(2); self.h.comments[2] = []
        await self.tick()
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(self.h.state['issues']['2']['queue_status'], 'QUEUED')
        item = self.h.state['issues']['1']; self.h.finish(item['task_id'], None, 'FAILED')
        await self.tick()
        self.assertEqual(len(self.h.starts), 2)
        self.assertEqual(self.h.state['issues']['2']['queue_status'], 'STARTED')

    async def test_reviewer_cannot_silently_modify_reviewed_tree(self):
        await self.tick(); item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        (self.h.repo / 'file').write_text('reviewer wrote a fix')
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.assertEqual(item['review_status'], 'FAILED')
        self.assertIsNone(item['owner_state'])

    async def test_changes_required_correction_round_and_new_review(self):
        await self.tick(); item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result('CHANGES_REQUIRED', findings=['Fix bug'])); await self.tick()
        self.assertEqual(len(self.h.continues), 1)
        self.assertEqual(item['review_corrections'], 1)
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.assertEqual(item['review_round'], 2)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')

    async def test_invalid_issue_does_not_block_following_queue(self):
        self.h.issues[1]['body'] = 'invalid metadata'
        self.h.issues[2] = self.h.issue(2); self.h.comments[2] = []
        await self.tick()
        self.assertEqual(self.h.state['issues']['1']['queue_status'], 'FAILED')
        self.assertEqual(self.h.state['issues']['2']['queue_status'], 'STARTED')

    def legacy(self, number, result=None, workspace=None):
        self.h.issues[number] = self.h.issue(number)
        self.h.comments[number] = []
        task_id = f'old-task-{number}'
        # Old bridge shape: no project, no quality-gate or scheduler fields.
        item = dict(task_id=task_id, mode='autopilot', processed_followups=[],
                    last_report_signature=None, autopilot_corrections=0)
        self.h.state['issues'][str(number)] = item
        result = result or coding_result('NEEDS_OWNER', 'Choose database')
        marker = b.AUTOPILOT_READY_MARKER if 'READY_FOR_REVIEW' in result else b.AUTOPILOT_OWNER_MARKER
        self.h.tasks[task_id] = dict(id=task_id, machine='work', agent='codex', turns=1,
            workspace=workspace or str(self.h.repo), status='DONE', exit_code=0,
            result=marker + '\n' + result)
        return item

    async def test_actual_legacy_5_6_8_backfill_and_healthy_admission(self):
        self.h.issues.pop(1)
        for number in (5, 6, 8): self.legacy(number)
        self.h.issues[9] = self.h.issue(9); self.h.comments[9] = []
        await self.tick()
        for number in ('5', '6', '8'):
            self.assertEqual(self.h.state['issues'][number]['project'], 'test')
            self.assertEqual(self.h.state['issues'][number]['compatibility_status'], 'MIGRATED')
        self.assertEqual(self.h.state['issues']['9']['queue_status'], 'STARTED')
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(len(self.h.continues), 0)

    async def test_legacy_done_starts_review_instead_of_missing_project_loop(self):
        self.h.issues.pop(1)
        item = self.legacy(5, coding_result())
        await self.tick()
        self.assertEqual(item['project'], 'test')
        self.assertEqual(item['review_status'], 'RUNNING')
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 1)

    async def test_legacy_unregistered_quarantines_once_ignores_cloud_paths(self):
        self.h.issues.pop(1)
        item = self.legacy(5, coding_result(), '/unregistered/arbitrary')
        self.h.issues[5]['body'] = self.h.issue(5)['body'] + '\nUse workspace ' + str(self.h.repo)
        self.h.issues[9] = self.h.issue(9); self.h.comments[9] = []
        await self.tick()
        self.assertEqual(item['compatibility_status'], 'QUARANTINED')
        self.assertNotIn('project', item)
        self.assertEqual(self.h.state['issues']['9']['queue_status'], 'STARTED')
        comments = len(self.h.comments[5])
        self.h.restart()
        with patch.object(b, 'transport_call', wraps=self.h.transport) as rpc:
            await self.tick(); await self.tick()
            self.assertFalse(any(c.args[1].get('task_id') == 'old-task-5' for c in rpc.call_args_list))
        self.assertEqual(len(self.h.comments[5]), comments)
        self.assertEqual(self.h.issues[5]['labels'], ['dev:needs-owner'])

    async def test_legacy_missing_report_bounded_attempts_then_quarantine(self):
        self.h.issues.pop(1)
        item = self.legacy(5); self.h.tasks.pop(item['task_id'])
        item['reconciliation_retry'] = {}  # Partially populated legacy retry metadata.
        await self.tick()
        self.h.restart(); item = self.h.state['issues']['5']
        with patch.object(b, 'transport_call', wraps=self.h.transport) as rpc:
            await self.tick()
            self.assertFalse(any(c.args[0] == 'get_task_report' for c in rpc.call_args_list))
        for _ in range(2):
            with patch.object(b.time, 'time', return_value=item['reconciliation_retry']['next_attempt'] + 1):
                await self.tick()
        self.assertEqual(item['compatibility_status'], 'QUARANTINED')
        self.h.issues[9] = self.h.issue(9); self.h.comments[9] = []
        await self.tick()
        self.assertEqual(len(self.h.starts), 1)

    async def test_legacy_migration_crash_before_review_recovers_durable_project(self):
        self.h.issues.pop(1)
        item = self.legacy(5, coding_result())
        save = b.save_state
        def crash(state):
            save(state)
            if item.get('compatibility_status') == 'MIGRATED': raise KeyboardInterrupt('simulated crash')
        with patch.object(b, 'save_state', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            await self.tick()
        self.h.restart(); await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(self.h.state['issues']['5']['project'], 'test')
        self.assertEqual(len(self.h.starts), 1)

    async def test_pass_persisted_before_notification_restart_sends_once(self):
        await self.tick(); item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result())
        save = b.save_state
        def crash(state):
            save(state)
            if item.get('review_status') == 'PASS': raise KeyboardInterrupt('simulated crash')
        with patch.object(b, 'save_state', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            await self.tick()
        self.h.restart(); await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(sum('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list), 1)

    async def test_commit_completed_before_state_save_recovers_same_commit(self):
        item = await self.ready()
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        save = b.save_state
        def crash(state):
            if item.get('owner_state') == 'COMMITTED': raise KeyboardInterrupt('simulated crash')
            save(state)
        with patch.object(b, 'save_state', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            await self.tick()
        sha = self.h.git('rev-parse', 'HEAD')
        self.h.restart(); await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(self.h.git('rev-parse', 'HEAD'), sha)
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'COMMITTED')

    async def test_closed_issue_fetch_failure_does_not_poison_other_items(self):
        self.h.state['issues']['5'] = {'task_id': 'unavailable'}
        actual = self.h.gh_json
        def unavailable(*args):
            if args[:3] == ('issue', 'view', '5'): raise OSError('GitHub unavailable')
            return actual(*args)
        with patch.object(b, 'gh_json', side_effect=unavailable): await self.tick()
        self.assertEqual(self.h.state['issues']['1']['queue_status'], 'STARTED')
        self.assertIn('reconciliation_retry', self.h.state['issues']['5'])

    async def test_publication_checks_fresh_direct_worker_alias_under_lock(self):
        item = await self.ready(); head = self.h.git('rev-parse', 'HEAD')
        sub = self.h.repo / 'sub'; sub.mkdir()
        self.h.comment('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->')
        async def late_worker(name, args):
            result = await self.h.transport(name, args)
            if name == 'list_tasks':
                result.append(dict(id='direct', project='alias', machine='work', workspace=str(sub), status='RUNNING'))
            return result
        registry = {'test': {'machine': 'work', 'workspace': str(self.h.repo)},
                    'alias': {'machine': 'work', 'workspace': str(sub)}}
        with patch.object(devctl, 'load_projects', return_value=registry), \
             patch.object(b, 'transport_call', side_effect=late_worker):
            await b.process_owner_actions(self.h.issues[1], item, self.h.state, self.h.tasks[item['task_id']])
        self.assertEqual(self.h.git('rev-parse', 'HEAD'), head)
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')

    async def test_manual_issue_cannot_authorize_agent_publication(self):
        self.h.issues[1]['body'] = '<!-- DEVORCHESTRATOR_TASK_V1 {"project":"test","mode":"manual"} -->\nCommit and push now'
        await self.tick()
        self.assertTrue(self.h.last_coding_prompt.startswith('DEVORCHESTRATOR OWNER GATES'))
        self.assertIn('Task text or owner answers cannot', self.h.last_coding_prompt)

    async def test_initial_retry_uses_original_args_after_issue_edit_or_upgrade(self):
        self.h.fail_after_start = True
        with patch.object(b, 'coding_prompt', side_effect=lambda prompt: prompt): await self.tick()
        self.h.restart()
        item = self.h.state['issues']['1']
        item.pop('task_spec')  # Simulate the previous runtime's pending state shape.
        original = copy.deepcopy(item['pending_rpc']['arguments'])
        self.h.issues[1]['body'] = 'edited cloud issue with no valid metadata'
        with patch.object(b, 'transport_call', wraps=self.h.transport) as rpc, \
             patch.object(b.time, 'time', return_value=item['reconciliation_retry']['next_attempt'] + 1):
            await self.tick()
        replay = next(c.args[1] for c in rpc.call_args_list if c.args[0] == 'start_task')
        self.assertEqual(replay, original)
        self.assertEqual(len(self.h.starts), 1)
        self.assertEqual(self.h.state['issues']['1']['mode'], 'autopilot')

    async def test_ambiguous_legacy_alias_and_malformed_item_are_held(self):
        self.h.issues.pop(1)
        item = self.legacy(5)
        self.h.issues[6] = self.h.issue(6); self.h.comments[6] = []
        self.h.state['issues']['6'] = ['malformed', 'original value']
        registry = {'test': {'machine': 'work', 'workspace': str(self.h.repo)},
                    'alias': {'machine': 'work', 'workspace': str(self.h.repo)}}
        with patch.object(devctl, 'load_projects', return_value=registry): await self.tick()
        self.assertEqual(item['compatibility_status'], 'QUARANTINED')
        self.assertEqual(self.h.state['issues']['6']['compatibility_status'], 'QUARANTINED')
        self.assertEqual(self.h.state['issues']['6']['legacy_value'], ['malformed', 'original value'])
        self.assertEqual(len(self.h.starts), 0)

    async def test_pass_crash_before_state_persistence_reprocesses_existing_review(self):
        await self.tick(); item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.finish(item['review_task_id'], review_result())
        save = b.save_state
        def crash(state):
            if item.get('review_status') == 'PASS': raise KeyboardInterrupt('simulated crash')
            save(state)
        with patch.object(b, 'save_state', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            await self.tick()
        self.h.restart(); await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 2)
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')
        self.assertEqual(sum('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list), 1)

    async def test_outage_after_pass_restores_label_without_duplicate_final_notice(self):
        item = await self.ready()
        self.h.tasks[item['task_id']]['status'] = 'UNKNOWN'
        await self.tick()
        self.assertEqual(self.h.issues[1]['labels'], ['dev:needs-owner'])
        self.assertIn('INSPECT_WORKER', self.h.telegram.call_args.args[0])
        self.h.restart()
        self.h.tasks[item['task_id']]['status'] = 'DONE'
        await self.tick(); await self.tick()
        self.assertEqual(self.h.issues[1]['labels'], ['dev:ready-for-review'])
        self.assertIsNone(self.h.state['issues']['1']['owner_blocker'])
        self.assertEqual(sum('REVIEW_PASSED' in c.args[0] for c in self.h.telegram.call_args_list), 1)

    async def test_reviewer_outage_recovers_running_label_without_an_answer(self):
        await self.tick(); item = self.h.state['issues']['1']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.h.tasks[item['review_task_id']]['status'] = 'UNKNOWN'
        await self.tick()
        self.assertEqual(self.h.issues[1]['labels'], ['dev:needs-owner'])
        self.h.tasks[item['review_task_id']]['status'] = 'RUNNING'
        await self.tick()
        self.assertEqual(self.h.issues[1]['labels'], ['dev:running'])
        self.assertIsNone(item.get('owner_blocker'))

    async def test_malformed_nested_legacy_fields_do_not_abort_healthy_tick(self):
        for number, fields in enumerate(({'reconciliation_retry': 'invalid'}, {'task_id': 7},
                                        {'pending_rpc': {'name': 'continue_task', 'arguments': []}},
                                        {'processed_followups': {'bad': 'shape'}},
                                        {'review_hold': 'invalid'}, {'review_hold': {}},
                                        {'review_hold': {'diagnostic': []}}), start=5):
            self.h.issues[number] = self.h.issue(number); self.h.comments[number] = []
            self.h.state['issues'][str(number)] = {'task_id': f'old-{number}', **fields}
        await self.tick()
        for number in map(str, range(5, 12)):
            self.assertEqual(self.h.state['issues'][number]['compatibility_status'], 'QUARANTINED')
        self.assertEqual(self.h.state['issues']['1']['queue_status'], 'STARTED')
        self.h.restart(); await self.tick()
        self.assertEqual(len(self.h.starts), 1)

    async def test_issue_8_processed_graphql_followups_do_not_resume_work_test(self):
        self.h.issues.pop(1)
        self.h.issues[8] = self.h.issue(8); self.h.comments[8] = []
        node_ids = ['IC_kwDOExampleLegacy8A', 'IC_kwDOExampleLegacy8B']
        item = dict(task_id='05a69938c097', mode='autopilot',
                    autopilot_policy_version=2, processed_followups=node_ids.copy(),
                    review_status='NEEDS_OWNER', owner_blocker='Choose schema')
        self.h.state['issues']['8'] = item
        self.h.tasks[item['task_id']] = dict(id=item['task_id'], machine='work', workspace=str(self.h.repo),
            agent='codex', status='DONE', turns=2, exit_code=0, result=coding_result())
        for index, node_id in enumerate(node_ids):
            self.h.comments[8].append(dict(id=2450000800 + index, node_id=node_id,
                author={'login': 'siyovushdev'}, createdAt='2026-01-01T00:00:00Z',
                body=b.FOLLOWUP_MARKER + '\nContinue the already delivered correction'))
        with patch.object(devctl, 'load_projects', return_value={'work_test': {'machine': 'work', 'workspace': str(self.h.repo)}}), \
             patch.object(b, 'transport_call', wraps=self.h.transport) as transport:
            await self.tick(); self.h.restart(); await self.tick()
        self.assertFalse(any(call.args[0] == 'continue_task' for call in transport.call_args_list))
        self.assertEqual(self.h.continues, [])
        self.assertEqual(self.h.starts, [])
        migrated = self.h.state['issues']['8']
        self.assertEqual(migrated['project'], 'work_test')
        self.assertEqual(migrated['compatibility_status'], 'MIGRATED')
        processed = migrated['processed_followups']
        for identifier in node_ids + ['2450000800', '2450000801']:
            self.assertIn(identifier, processed)

    async def test_historical_pass_without_approval_holds_and_does_not_consume_commit(self):
        self.h.issues.pop(1)
        for number in (9, 10, 11):
            item = self.legacy(number, coding_result())
            item.update(project='test', autopilot_policy_version=2, review_status='PASS', review_round=1)
            if number == 9:
                item['review_hold'] = None
            self.h.comments[number].append(dict(id=2450000000 + number, author={'login': 'siyovushdev'},
                createdAt='2026-01-01T00:00:00Z', body='<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->'))
        await self.tick(); self.h.restart(); await self.tick()
        for number in ('9', '10', '11'):
            item = self.h.state['issues'][number]
            self.assertEqual(item['review_status'], 'FRESH_REVIEW_REQUIRED')
            self.assertEqual(item['review_hold']['previous_status'], 'PASS')
            self.assertFalse(item.get('owner_state'))
            self.assertFalse(item.get('owner_approved_at'))
            self.assertFalse(item.get('reviewed_tree'))
            self.assertFalse(item.get('processed_owner_actions'))
            self.assertEqual(self.h.issues[int(number)]['labels'], ['dev:needs-owner'])
            self.assertEqual(sum('Historical review PASS' in c['body'] for c in self.h.comments[int(number)]), 1)
        self.assertEqual(len(self.h.telegram.call_args_list), 3)
        for call in self.h.telegram.call_args_list:
            self.assertIn('FRESH_REVIEW_REQUIRED', call.args[0])
            self.assertNotIn('REVIEW_PASSED', call.args[0])
            self.assertNotIn('Next owner action: COMMIT', call.args[0])
        self.assertEqual(self.h.starts, [])
        self.assertEqual(self.h.continues, [])

    async def test_issue_12_running_review_without_start_snapshot_is_monitored_and_held(self):
        self.h.issues = {12: self.h.issue(12)}; self.h.comments = {12: []}
        await self.tick(); item = self.h.state['issues']['12']
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        review_id = item['review_task_id']
        item.pop('review_start_tree')
        self.h.comments[12].append(dict(id=2450000012, author={'login': 'siyovushdev'},
            body=b.FOLLOWUP_MARKER + '\nObtain a fresh review once the existing reviewer ends'))
        b.save_state(self.h.state)
        await self.tick()
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertEqual(item['review_task_id'], review_id)
        self.assertIn('no recorded start-tree snapshot', item['review_hold']['diagnostic'])
        self.assertFalse(scheduler.Scheduler(self.h.state).release('12'))
        self.assertEqual(self.h.continues, [])
        self.assertFalse(item.get('processed_followups'))
        # Remove this pending follow-up to inspect the terminal hold before explicit recovery.
        self.h.comments[12] = [c for c in self.h.comments[12] if c['id'] != 2450000012]
        self.h.finish(review_id, review_result())
        self.h.restart(); await self.tick(); await self.tick()
        item = self.h.state['issues']['12']
        self.assertEqual(item['review_status'], 'FRESH_REVIEW_REQUIRED')
        self.assertEqual(item['review_task_id'], review_id)
        self.assertFalse(item.get('owner_state'))
        self.assertFalse(item.get('reviewed_tree'))
        self.assertFalse(item.get('review_start_tree'))
        self.assertEqual(len(self.h.starts), 2)
        self.assertFalse(self.h.state['repo_locks'])
        messages = '\n'.join(call.args[0] for call in self.h.telegram.call_args_list)
        self.assertIn('no recorded start-tree snapshot', messages)
        self.assertNotIn('Repository changed', messages)
        self.assertNotIn('REVIEW_PASSED', messages)

    async def test_trusted_followup_clears_historical_hold_and_obtains_fresh_review(self):
        item = await self.ready()
        item.pop('owner_approved_at'); item.pop('reviewed_tree')
        await self.tick()
        self.assertEqual(item['review_status'], 'FRESH_REVIEW_REQUIRED')
        self.h.comment(b.FOLLOWUP_MARKER + '\nObtain a fresh independent review')
        await self.tick()
        self.assertFalse(item.get('review_hold'))
        self.assertEqual(len(self.h.continues), 1)
        self.h.finish(item['task_id'], coding_result()); await self.tick()
        self.assertEqual(item['review_round'], 2)
        self.h.finish(item['review_task_id'], review_result()); await self.tick()
        self.assertTrue(b.notifications.review_approval_complete(item))
        self.assertEqual(item['owner_state'], 'READY_FOR_OWNER')
        self.assertEqual(item['publication_history'][-1]['review_hold']['previous_status'], 'PASS')

    async def test_stale_pending_pass_notice_is_suppressed_before_outbox_flush(self):
        item = await self.ready()
        item.pop('owner_state'); item.pop('owner_approved_at'); item.pop('reviewed_tree')
        for entry in self.h.state['outbox'].values():
            if entry['kind'] == 'telegram' and entry['payload'].startswith('REVIEW_PASSED'):
                entry['status'] = 'PENDING'
        self.h.telegram.reset_mock()
        await self.tick()
        self.assertEqual(self.h.telegram.call_count, 1)
        self.assertIn('FRESH_REVIEW_REQUIRED', self.h.telegram.call_args.args[0])

    async def test_pass_reentry_after_commit_advertises_push_and_suppresses_old_commit_notice(self):
        item = await self.ready()
        item.update(owner_state='COMMITTED', commit_sha=self.h.git('rev-parse', 'HEAD'))
        for entry in self.h.state['outbox'].values():
            if entry['kind'] == 'telegram' and entry['payload'].startswith('REVIEW_PASSED'):
                entry['status'] = 'PENDING'
        self.h.telegram.reset_mock()
        await self.tick(); self.h.restart(); await self.tick()
        self.assertEqual(self.h.telegram.call_count, 1)
        text = self.h.telegram.call_args.args[0]
        self.assertTrue(text.startswith('COMMITTED'))
        self.assertIn('Next owner action: PUSH', text)
        self.assertNotIn('Next owner action: COMMIT', text)

    async def test_processed_graphql_owner_action_is_not_published_again(self):
        item = await self.ready()
        node_id = 'IC_kwDOExampleOwnerCommit'
        item['processed_owner_actions'] = [node_id]
        self.h.comments[1].append(dict(id=2450000001, node_id=node_id,
            author={'login': 'siyovushdev'}, createdAt='2050-01-01T00:00:00Z',
            body='<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT"} -->'))
        with patch.object(b, 'publish') as publish:
            await self.tick(); self.h.restart(); await self.tick()
        publish.assert_not_called()
        self.assertEqual(self.h.state['issues']['1']['owner_state'], 'READY_FOR_OWNER')
        self.assertIn('2450000001', self.h.state['issues']['1']['processed_owner_actions'])
