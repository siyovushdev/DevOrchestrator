import json
import unittest
from unittest.mock import AsyncMock, patch
import task_bridge as b


def coding_result(state='READY_FOR_REVIEW', blocker=None):
    return 'DEVORCHESTRATOR_RESULT: ' + json.dumps(dict(state=state, blocker=blocker,
        implementation='DONE', verification='PASS', self_review='PASS', summary='tested'))


class OwnerFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_answer_same_session_invalidates_approval(self):
        item = {'task_id': 'same-session', 'mode': 'autopilot', 'owner_blocker': 'Choose database',
                'owner_question_at': '2026-01-01T00:00:00+00:00',
                'review_status': 'NEEDS_OWNER', 'owner_state': 'READY_FOR_OWNER', 'reviewed_tree': {'fingerprint': 'old'}}
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
