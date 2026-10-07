import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import bridge_effects as effects
import devctl
from redaction import redact
import task_bridge as b
from test_github_publication import FakeRepo
import publication as p


class BoundaryTests(unittest.TestCase):
    def test_paginated_json_collections_and_comments_author_mapping(self):
        pages = '[{"id":1,"node_id":"IC_kwDOExampleLegacy","user":{"login":"owner"},"created_at":"2026-01-01T00:00:00Z"}]\n[{"id":2}]'
        with patch.object(b, 'gh', return_value=pages) as gh:
            comments = b.issue_comments(1)
            self.assertEqual(len(comments), 2)
            self.assertEqual(comments[0]['author']['login'], 'owner')
            self.assertEqual(comments[0]['node_id'], 'IC_kwDOExampleLegacy')
            self.assertIn('--paginate', gh.call_args.args)
        with patch.object(b, 'gh', return_value='{"bad":"shape"}'):
            with self.assertRaises(RuntimeError): b.gh_pages('endpoint')

    def test_superseded_labels_never_overwrite_new_state(self):
        state = {}
        old = effects.enqueue(state, 1, 'transition', {'target': 'dev:running'})
        new = effects.enqueue(state, 1, 'transition', {'target': 'dev:needs-owner'})
        self.assertEqual(state['outbox'][old]['status'], 'DONE')
        self.assertEqual(state['outbox'][new]['status'], 'PENDING')

    @patch.dict(os.environ, {'TELEGRAM_BOT_TOKEN': 'secret-value-12345'})
    def test_secret_redaction(self):
        text = redact('secret-value-12345 https://user:password@github.com/o/r api.telegram.org/botTOKEN/sendMessage ghp_' + 'a' * 30)
        for value in ('secret-value-12345', 'password', 'TOKEN', 'ghp_'): self.assertNotIn(value, text)
        self.assertNotIn('random-token', redact('https://random-token@github.com/o/r'))

    def test_push_url_mismatch_is_blocked(self):
        repo = FakeRepo(); real_git = repo.git
        def git(*args):
            if '--push' in args: return 'git@github.com:evil/repo.git'
            return real_git(*args)
        repo.git = git
        item = dict(task_id='test', commit_sha='abc', publication_branch='feature', publication_origin=repo.current['origin'])
        with self.assertRaises(p.PublicationError): p.execute(repo, item, {}, {'action': 'PUSH'}, lambda: None)
        self.assertIsNone(repo.remote)

    def test_baseline_security_no_extra_task_fields(self):
        issue = {'body': '<!-- DEVORCHESTRATOR_TASK_V1 {"project":"test","workspace":"/evil","command":"rm"} -->\nTask'}
        result = b.parse_task(issue)
        self.assertEqual(result[0], 'test')
        self.assertEqual(result[-1], 'Task')
        self.assertNotIn('/evil', result)
