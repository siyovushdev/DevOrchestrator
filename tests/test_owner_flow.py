import json
import unittest
from unittest.mock import AsyncMock, patch
import task_bridge as b


def coding_result(state='READY_FOR_REVIEW', blocker=None):
    return 'DEVORCHESTRATOR_RESULT: ' + json.dumps(dict(state=state, blocker=blocker,
        implementation='DONE', verification='PASS', self_review='PASS', summary='tested'))


class OwnerFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_mutable_coding_with_stale_ready_never_enters_review(self):
        for status in ('RUNNING', 'STARTING', 'UNKNOWN'):
            with self.subTest(status=status):
                item = dict(task_id='coding', mode='autopilot', autopilot_policy_version=2)
                report = dict(id='coding', status=status, turns=2, exit_code=0, result=coding_result())
                with patch.object(b, 'mcp_call', new_callable=AsyncMock) as rpc, \
                     patch.object(b, 'notify') as notify, patch.object(b, 'save_state'):
                    await b.process_review_loop({'number': 1}, item, {'issues': {'1': item}}, report)
                rpc.assert_not_awaited()
                notify.assert_not_called()
                self.assertFalse(item.get('review_task_id'))
                self.assertFalse(item.get('owner_state'))

    async def test_review_requires_successful_terminal_current_coding_turn(self):
        for fields in ({'status': 'FAILED'}, {'exit_code': 1}, {'exit_code': None},
                       {'turns': 1}, {'turns': None}, {'id': 'other-task'}):
            with self.subTest(fields=fields):
                item = dict(task_id='coding', mode='autopilot', autopilot_policy_version=2, coding_turn=2)
                report = dict(id='coding', status='DONE', turns=2, exit_code=0, result=coding_result())
                report.update(fields)
                with patch.object(b, 'mcp_call', new_callable=AsyncMock) as rpc, patch.object(b, 'save_state'):
                    await b.process_review_loop({'number': 1}, item, {'issues': {'1': item}}, report)
                rpc.assert_not_awaited()
                self.assertFalse(item.get('review_task_id'))
        item = dict(task_id='coding', mode='autopilot')
        state = {'issues': {'1': item}}
        with patch.object(b, 'WORK_CONTEXT', (state, '1', item)), \
             patch.object(b, 'transport_call', new_callable=AsyncMock) as rpc:
            with self.assertRaisesRegex(ValueError, 'read-only review role'):
                await b.mcp_call('start_task', {'_operation': 'review:1', 'project': 'test'})
        rpc.assert_not_awaited()

    async def test_answer_same_session_invalidates_approval(self):
        item = {'task_id': 'same-session', 'mode': 'autopilot', 'owner_blocker': 'Choose database',
                'owner_question_at': '2026-01-01T00:00:00+00:00',
                'review_status': 'NEEDS_OWNER', 'owner_state': 'READY_FOR_OWNER',
                'reviewed_tree': {'fingerprint': 'old'}, 'review_format_restarts': 1}
        issue = {'number': 1}
        comment = {'id': 'answer', 'author': {'login': 'siyovushdev'},
            'createdAt': '2026-01-02T00:00:00Z',
            'body': '<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Use SQLite"} -->'}
        call = AsyncMock(return_value={'status': 'RUNNING'})
        with patch.object(b, 'mcp_call', call), patch.object(b, 'issue_comments', return_value=[comment]), \
             patch.object(b, 'save_state'), patch.object(b, 'transition_issue'), patch.object(b, 'add_comment'):
            result = await b.process_followup(issue, item, {'issues': {'1': item}}, {'status': 'DONE'})
            self.assertEqual(result['status'], 'RUNNING')
            self.assertEqual(call.call_args.args[1]['task_id'], 'same-session')
            self.assertIn('Use SQLite', call.call_args.args[1]['prompt'])
            self.assertIn(b.AUTOPILOT_RESULT_PREFIX, call.call_args.args[1]['prompt'])
            self.assertIsNone(item['owner_state'])
            self.assertIsNone(item['reviewed_tree'])
            self.assertEqual(item['review_format_restarts'], 0)
            await b.process_followup(issue, item, {}, {'status': 'DONE'})
            self.assertEqual(call.await_count, 1)

    def test_coding_needs_owner_contract(self):
        report = {'status': 'DONE', 'result': coding_result('NEEDS_OWNER', 'Which database?')}
        self.assertEqual(b.terminal_state(report, {'mode': 'autopilot', 'autopilot_policy_version': 2})[1], 'dev:needs-owner')

    def test_reviewer_needs_owner_contract(self):
        result = 'DEVORCHESTRATOR_REVIEW: ' + json.dumps(dict(verdict='NEEDS_OWNER', summary='decision', findings=[], blocker='Which database?'))
        data, error = b.parse_review_result(result)
        self.assertIsNone(error)
        self.assertEqual(data['blocker'], 'Which database?')

    def test_malformed_review_contract_never_accepts_pass(self):
        valid = dict(verdict='PASS', summary='Reviewed', findings=[], blocker=None)
        cases = [
            ('DEVORCHESTRATOR_REVIEW: {"verdict":', 'invalid review JSON'),
            ('DEVORCHESTRATOR_REVIEW: ' + json.dumps(valid) + ' extra data', 'invalid review JSON'),
            ('DEVORCHESTRATOR_REVIEW: ' + json.dumps(valid) + '\nExtra commentary', 'missing DEVORCHESTRATOR_REVIEW'),
            ('Review completed without a trailer', 'missing DEVORCHESTRATOR_REVIEW'),
        ]
        for verdict in ([], {}, None, 42):
            cases.append(('DEVORCHESTRATOR_REVIEW: ' + json.dumps({**valid, 'verdict': verdict}), 'invalid review verdict'))
        for text, diagnostic in cases:
            with self.subTest(text=text):
                data, error = b.parse_review_result(text)
                self.assertIsNone(data)
                self.assertIn(diagnostic, error)

    async def test_old_answer_is_consumed_and_cannot_resume_later_question(self):
        item = dict(task_id='same', mode='autopilot', owner_blocker=None)
        comment = dict(id='old', author={'login': 'siyovushdev'}, createdAt='2020-01-01T00:00:00Z',
            body='<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Old decision"} -->')
        rpc = AsyncMock()
        with patch.object(b, 'mcp_call', rpc), patch.object(b, 'issue_comments', return_value=[comment]), patch.object(b, 'save_state'):
            await b.process_followup({'number': 1}, item, {}, {'status': 'DONE', 'result': coding_result()})
            item.update(owner_blocker='New choice', owner_question_at='2026-01-01T00:00:00+00:00')
            await b.process_followup({'number': 1}, item, {}, {'status': 'DONE', 'result': coding_result('NEEDS_OWNER', 'New choice')})
            rpc.assert_not_awaited()
            self.assertIn('old', item['processed_followups'])

    async def test_answer_predating_current_or_legacy_question_is_rejected(self):
        for epoch in (None, '2026-01-01T00:00:00+00:00'):
            item = dict(task_id='same', owner_blocker='Choose', review_status='NEEDS_OWNER', owner_question_at=epoch)
            comment = dict(id='old', author={'login': 'siyovushdev'}, createdAt='2020-01-01T00:00:00Z',
                body='<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Old decision"} -->')
            rpc = AsyncMock()
            with patch.object(b, 'mcp_call', rpc), patch.object(b, 'issue_comments', return_value=[comment]), patch.object(b, 'save_state'):
                await b.process_followup({'number': 1}, item, {}, {'status': 'DONE'})
                rpc.assert_not_awaited()
                self.assertIn('old', item['processed_followups'])
