import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import devctl
import publication as p
import task_bridge as b


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.git('init', '-b', 'feature')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        (self.root / 'file').write_text('baseline')
        self.git('add', '.')
        self.git('commit', '-m', 'baseline')
        self.registry = patch.object(devctl, 'load_projects', return_value={
            'test': {'machine': 'work', 'workspace': str(self.root)}})
        self.registry.start()
        self.addCleanup(self.registry.stop)
        self.addCleanup(self.tmp.cleanup)
        self.repo = p.Repository('test')
        self.report = {'git_baseline': devctl.capture_git_baseline('work', str(self.root))}
        (self.root / 'file').write_text('reviewed')
        self.item = {'task_id': 'test-task', 'owner_state': b.OWNER_STATE_READY,
                     'reviewed_tree': self.repo.snapshot()}

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.root), *args], stderr=subprocess.DEVNULL).decode().strip()

    def commit(self):
        p.execute(self.repo, self.item, self.report, {'action': 'COMMIT'}, lambda: None)

    def test_transitions_and_replays(self):
        for action, (before, after) in b.OWNER_ACTION_RULES.items():
            self.assertEqual(b.owner_action_plan(before, action)['next_state'], after)
            self.assertEqual(b.owner_action_plan(after, action)['status'], 'ALREADY_DONE')
        self.assertEqual(b.owner_action_plan(b.OWNER_STATE_READY, 'PUSH')['status'], 'REJECTED')
        self.assertEqual(b.owner_action_plan(None, 'COMMIT')['status'], 'REJECTED')

    def test_commit_and_crash_replay(self):
        self.commit()
        sha = self.git('rev-parse', 'HEAD')
        self.commit()
        self.assertEqual(self.git('rev-parse', 'HEAD'), sha)
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual((self.root / 'file').read_text(), 'reviewed')

    def test_dirty_unknown_adopted(self):
        for report in ({}, {'adopted': True, **self.report},
                       {'git_baseline': {**self.report['git_baseline'], 'clean': False}}):
            with self.subTest(report=report), self.assertRaises(p.PublicationError):
                p.execute(self.repo, self.item, report, {'action': 'COMMIT'}, lambda: None)

    def test_silent_change_and_index_change(self):
        (self.root / 'file').write_text('silent')
        with self.assertRaises(p.PublicationError): self.commit()
        (self.root / 'file').write_text('reviewed')
        self.git('add', '.')
        with self.assertRaises(p.PublicationError): self.commit()

    def test_branch_head_detached(self):
        self.git('checkout', '-b', 'unexpected')
        with self.assertRaises(p.PublicationError): self.commit()
        self.git('checkout', 'feature')
        self.git('commit', '--allow-empty', '-m', 'unexpected HEAD')
        with self.assertRaises(p.PublicationError): self.commit()
        self.git('checkout', '--detach')
        with self.assertRaises(p.PublicationError): self.commit()

    def test_action_failure_keeps_gate(self):
        with patch.object(self.repo, 'git', side_effect=p.PublicationError('failure')):
            with self.assertRaises(p.PublicationError): self.commit()
        self.assertEqual(self.item['owner_state'], b.OWNER_STATE_READY)

    def test_invalidation(self):
        b.invalidate_quality_gates(self.item)
        self.assertIsNone(self.item['owner_state'])
        self.assertIsNone(self.item['reviewed_tree'])

    def test_contract_and_origin_validation(self):
        self.assertEqual(b.parse_owner_command('<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"commit"} -->')['action'], 'COMMIT')
        for origin in ('https://evil.example/repo', 'https://token@github.com/a/b', 'file:///tmp/repo'):
            with self.assertRaises(p.PublicationError): p.github_repo(origin)
        self.assertEqual(p.github_repo('git@github.com:owner/repo.git'), 'owner/repo')
        self.assertFalse(b.authorized_comment({'author': {'login': 'attacker'}}))
    def test_registered_subdirectory_alias_uses_same_lock(self):
        from scheduler import project_identity
        sub = self.root / 'sub'; sub.mkdir()
        alias = self.root.parent / (self.root.name + '-alias'); alias.symlink_to(self.root, target_is_directory=True)
        self.addCleanup(alias.unlink)
        registry = {'root': {'machine': 'work', 'workspace': str(self.root)},
                    'sub': {'machine': 'work', 'workspace': str(sub)},
                    'alias': {'machine': 'work', 'workspace': str(alias)}}
        with patch.object(devctl, 'load_projects', return_value=registry):
            self.assertEqual(project_identity('root'), project_identity('sub'))
            self.assertEqual(project_identity('root'), project_identity('alias'))

    def test_interruption_after_ref_update_recovers_one_commit(self):
        original = self.repo.git
        def interrupted(*args):
            if args[0] == 'read-tree': raise p.PublicationError('interrupted index reconciliation')
            return original(*args)
        with patch.object(self.repo, 'git', side_effect=interrupted):
            with self.assertRaises(p.PublicationError): self.commit()
        candidate = self.git('rev-parse', 'HEAD')
        self.commit()
        self.assertEqual(self.git('rev-parse', 'HEAD'), candidate)
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_push_cannot_publish_configured_tags_or_execute_local_hooks(self):
        with tempfile.TemporaryDirectory() as remote:
            subprocess.check_call(['git', 'init', '--bare', remote], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.git('remote', 'add', 'origin', remote)
            self.git('tag', '-a', 'private-tag', '-m', 'must remain local')
            self.git('config', 'push.followTags', 'true')
            sentinel = self.root / '.git/hook-ran'
            hook = self.root / '.git/hooks/pre-push'
            hook.write_text('#!/bin/sh\ntouch ' + str(sentinel) + '\n')
            hook.chmod(0o700)
            # Refresh the fixture review and baseline to include the local disposable remote.
            self.report['git_baseline']['origin'] = remote
            self.report['git_baseline']['push_urls'] = [remote]
            self.item['reviewed_tree'] = self.repo.snapshot()
            self.commit()
            original = self.repo.run
            def run(*args):
                if args[:3] == ('gh', 'repo', 'view'):
                    return json.dumps(dict(defaultBranchRef={'name': 'main'}, nameWithOwner='owner/repo', url='https://github.com/owner/repo'))
                return original(*args)
            with patch.object(self.repo, 'run', side_effect=run), patch.object(p, 'github_repo', return_value='owner/repo'):
                p.execute(self.repo, self.item, self.report, {'action': 'PUSH'}, lambda: None)
            self.assertFalse(sentinel.exists())
            refs = subprocess.check_output(['git', '-C', remote, 'show-ref']).decode().splitlines()
            self.assertEqual(refs, [self.item['commit_sha'] + ' refs/heads/feature'])
