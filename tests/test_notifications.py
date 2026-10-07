import json
import subprocess
import urllib.error
import unittest
from unittest.mock import Mock, patch
import notifications as n


class NotificationTests(unittest.TestCase):
    def test_review_pass_requires_each_approval_field(self):
        approved = dict(review_status='PASS', owner_state='READY_FOR_OWNER',
                        owner_approved_at='2026-10-06T00:00:00+00:00',
                        reviewed_tree={'fingerprint': 'reviewed-content'})
        text = n.terminal_text('REVIEW_PASSED / READY_FOR_OWNER', {'number': 1}, approved)
        self.assertIn('Next owner action: COMMIT', text)
        for field in ('owner_state', 'owner_approved_at', 'reviewed_tree'):
            with self.subTest(missing=field):
                item = {key: value for key, value in approved.items() if key != field}
                self.assertFalse(n.review_approval_complete(item))
                text = n.terminal_text('REVIEW_PASSED / READY_FOR_OWNER', {'number': 1}, item)
                self.assertTrue(text.startswith('FRESH_REVIEW_REQUIRED'))
                self.assertNotIn('REVIEW_PASSED', text)
                self.assertNotIn('Next owner action: COMMIT', text)
                self.assertIn('FOLLOWUP_FOR_FRESH_REVIEW', text)
        for fields in ({'reviewed_tree': {}}, {'owner_state': 'invalid'},
                       {'review_hold': {'diagnostic': 'Review held'}}, {'review_hold': {}},
                       {'review_status': 'RUNNING'}):
            with self.subTest(invalid=fields):
                self.assertFalse(n.review_approval_complete({**approved, **fields}))

    def test_review_pass_formatter_uses_completed_publication_stage(self):
        item = dict(review_status='PASS', owner_state='COMMITTED',
                    owner_approved_at='2026-10-06T00:00:00+00:00',
                    reviewed_tree={'fingerprint': 'reviewed-content'})
        text = n.terminal_text('REVIEW_PASSED / READY_FOR_OWNER', {'number': 1}, item)
        self.assertTrue(text.startswith('COMMITTED'))
        self.assertIn('Next owner action: PUSH', text)
        self.assertNotIn('Next owner action: COMMIT', text)

    @patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'secret', 'TELEGRAM_CHAT_ID': '1'})
    def test_transient_retry_and_bounds(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b'{"ok":true}'
        rejection = urllib.error.HTTPError('token', 429, 'retry', {}, None)
        opener = Mock(side_effect=[rejection, response])
        sleep = Mock()
        self.assertTrue(n.send('test', opener, sleep))
        self.assertEqual(opener.call_count, 2)
        self.assertEqual(sleep.call_count, 1)
        opener = Mock(side_effect=rejection)
        self.assertFalse(n.send('test', opener, sleep))
        self.assertEqual(opener.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list[-2:]], [0.5, 1.0])

    @patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'secret', 'TELEGRAM_CHAT_ID': '1'})
    def test_permanent_failure_does_not_throw(self):
        opener = Mock(side_effect=urllib.error.HTTPError('token', 401, 'bad', {}, None))
        self.assertFalse(n.send('test', opener, Mock()))
        self.assertEqual(opener.call_count, 1)

    def test_terminal_details(self):
        text = n.terminal_text('NEEDS_OWNER', {'number': 1, 'url': 'https://issue'},
            {'project': 'test', 'coding_agent': 'codex', 'reviewer_agent': 'cursor',
             'review_round': 2, 'publication_branch': 'feature', 'commit_sha': 'abc', 'pr_url': 'https://pr'},
            {'model': 'model', 'reasoning_effort': 'high', 'created_at': '2026-01-01T00:00:00+00:00',
             'result': 'DEVORCHESTRATOR_RESULT: ' + json.dumps({'verification': 'PASS', 'summary': 'tests passed'})},
            'Choose A or B')
        for value in ('test', 'codex', 'cursor', 'feature', 'abc', 'https://pr', 'https://issue', 'Choose A or B', 'Verification: PASS', 'Duration:'):
            self.assertIn(value, text)

    @patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'secret', 'TELEGRAM_CHAT_ID': '1'})
    def test_ambiguous_timeout_never_retries(self):
        opener = Mock(side_effect=OSError('POST accepted then connection lost'))
        sleep = Mock()
        self.assertIsNone(n.send('test', opener, sleep))
        self.assertEqual(opener.call_count, 1)
        sleep.assert_not_called()

    @patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'secret', 'TELEGRAM_CHAT_ID': '1'})
    def test_wall_clock_timeout_is_killable_and_uncertain(self):
        with patch.object(n.subprocess, 'run', side_effect=subprocess.TimeoutExpired('transport', 35)) as run:
            self.assertIsNone(n.send('test'))
            self.assertEqual(run.call_args.kwargs['timeout'], 35)
        with patch.object(n.subprocess, 'run', return_value=Mock(stdout='SENT\n')):
            self.assertTrue(n.send('test'))

    @patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'secret', 'TELEGRAM_CHAT_ID': '1'})
    def test_malformed_response_and_callback_exception_are_uncertain(self):
        response = Mock(); response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=False)
        response.read.return_value = b'incomplete'
        opener = Mock(return_value=response)
        self.assertIsNone(n.send('test', opener, Mock()))
        self.assertEqual(opener.call_count, 1)
        self.assertIsNone(n.send('test', Mock(side_effect=RuntimeError('unexpected transport failure')), Mock()))

    def test_long_result_cannot_hide_owner_action_or_task_identity(self):
        text = n.terminal_text('NEEDS_OWNER', {'number': 1, 'title': 'x' * 5000, 'url': 'https://issue'},
            {'project': 'test', 'task_id': 'task-id'},
            {'result': 'DEVORCHESTRATOR_RESULT: ' + json.dumps({'summary': 'y' * 5000})}, 'Which option? ' * 1000)
        for value in ('Next owner action: ANSWER', 'Owner question:', 'https://issue', 'task-id'):
            self.assertIn(value, text)
        self.assertLess(len(text), 4000)

    def test_invalid_agent_json_shape_cannot_break_terminal_notification(self):
        for data in ([], None, 'invalid', 42):
            text = n.terminal_text('FAILED', {'number': 1}, {'task_id': 'task'},
                                  {'result': 'DEVORCHESTRATOR_RESULT: ' + json.dumps(data)})
            self.assertIn('FAILED', text)
            self.assertIn('Task: task', text)

    @patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'secret', 'TELEGRAM_CHAT_ID': '1'})
    def test_gateway_failure_after_forwarded_post_is_uncertain(self):
        opener = Mock(side_effect=urllib.error.HTTPError('token', 502, 'gateway lost response', {}, None))
        self.assertIsNone(n.send('test', opener, Mock()))
        self.assertEqual(opener.call_count, 1)
