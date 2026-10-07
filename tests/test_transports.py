"""Offline contract/mapping tests; no provider API, credentials or new workflow."""
from dataclasses import FrozenInstanceError, fields
import inspect
from pathlib import Path
import tempfile
from typing import get_type_hints
import unittest
from unittest.mock import patch

from control_plane import ControlCommand, ControlEvent, ControlJournal
from durable import write_json
from github_transport import GitHubTransport
import task_bridge as bridge
from transports import ExternalRef, ExternalTask, Transport, TransportError, TransportScope


NOW = '2026-10-07T00:00:00Z'


def command(ref, *, revision=None, **changes):
    data = dict(source_transport=ref.scope.kind, source_id=ref.source_id(revision),
                kind='task.followup', payload={'text': 'Implement task'},
                actor={'ref': 'local-owner'}, created_at=NOW, task_ref='local-task')
    data.update(changes)
    return ControlCommand(**data)


def event(**changes):
    data = dict(task_ref='local-task', kind='task.observed', revision=1,
                payload={'text': 'Ready for review'}, created_at=NOW)
    data.update(changes)
    return ControlEvent(**data)


class MemoryTransport:
    """A second provider-shaped implementation, confined to tests."""
    def __init__(self, scope, task, commands):
        self.scope = scope
        self.task = task
        self.commands = commands
        self.delivered = []

    def poll_commands(self) -> tuple[ControlCommand, ...]:
        return self.commands

    def fetch_task(self, target: ExternalRef) -> ExternalTask:
        if target != self.task.ref:
            raise TransportError('fetch', 'unknown_target')
        return self.task

    def publish_event(self, target: ExternalRef, event: ControlEvent) -> None:
        self.fetch_task(target)
        self.delivered.append(event)


class FixtureGitHubTransport(GitHubTransport):
    """Exercise pure GitHub mappings with in-memory I/O, never the V1 runtime."""
    def __init__(self):
        super().__init__('owner/tasks')
        self.task = self.map_task({'number': 12, 'title': 'Task', 'body': 'Implement task'})
        self.commands = (command(self.comment_ref(12, {'id': 91})),)
        self.delivered = []

    def poll_commands(self) -> tuple[ControlCommand, ...]:
        return self.commands

    def fetch_task(self, target: ExternalRef) -> ExternalTask:
        self._check_target(target)
        if target != self.task.ref:
            raise TransportError('fetch', 'unknown_target')
        return self.task

    def publish_event(self, target: ExternalRef, event: ControlEvent) -> None:
        self.fetch_task(target)
        self.delivered.append(event)


class TransportContractTests(unittest.TestCase):
    def test_contract_uses_m2a_types_and_no_provider_objects_or_local_state(self):
        self.assertEqual(get_type_hints(Transport.poll_commands)['return'], tuple[ControlCommand, ...])
        self.assertEqual(get_type_hints(Transport.fetch_task),
                         {'target': ExternalRef, 'return': ExternalTask})
        self.assertEqual(get_type_hints(Transport.publish_event),
                         {'target': ExternalRef, 'event': ControlEvent, 'return': type(None)})
        methods = {name for name, value in vars(Transport).items()
                   if not name.startswith('_') and (callable(value) or isinstance(value, property))}
        self.assertEqual(methods, {'scope', 'poll_commands', 'fetch_task', 'publish_event'})
        self.assertEqual({f.name for f in fields(ExternalTask)}, {'ref', 'title', 'body', 'url'})
        self.assertEqual(list(inspect.signature(Transport.publish_event).parameters),
                         ['self', 'target', 'event'])

    def test_two_implementations_can_share_normalized_commands_and_events(self):
        github = FixtureGitHubTransport()
        scope = TransportScope('clickup', 'workspace:one/list:two')
        target = ExternalRef(scope, 'task:abc')
        other = MemoryTransport(scope, ExternalTask(target, 'Task', 'Implement task'),
                                (command(ExternalRef(scope, 'message:91')),))
        observed = event()
        # The consumer has no provider branch, issue object or workflow callback.
        for adapter in (github, other):
            self.assertIsInstance(adapter, Transport)
            inbound = adapter.poll_commands()
            self.assertEqual(inbound[0].kind, 'task.followup')
            self.assertIs(type(inbound[0]), ControlCommand)
            self.assertIs(type(adapter.fetch_task(adapter.task.ref)), ExternalTask)
            adapter.publish_event(adapter.task.ref, observed)
            self.assertEqual(adapter.delivered, [observed])
        self.assertNotEqual(github.commands[0].command_id, other.commands[0].command_id)

    def test_visibility_is_only_an_outbound_projection_of_local_event(self):
        adapter = FixtureGitHubTransport()
        visible = event(kind='task.visibility', payload={'state': 'ready_for_review'})
        adapter.publish_event(adapter.task.ref, visible)
        self.assertEqual(adapter.delivered[0], visible)
        self.assertFalse(hasattr(adapter, 'owner_state'))

    def test_source_identity_is_stable_namespaced_and_revision_aware(self):
        ref = ExternalRef(TransportScope('github', 'github.com/owner/tasks'), 'issue:12/comment:91')
        restored = ExternalRef(TransportScope('github', 'github.com/owner/tasks'), 'issue:12/comment:91')
        self.assertEqual(ref.source_id(), restored.source_id())
        # Pin the persisted encoding, not just two calls to the same helper.
        self.assertEqual(ref.source_id(),
                         'v1:74851d0cb037cdd1d1ca75456dfc1420612959aaf70f2a5e17217cd61c0488a3')
        self.assertEqual(command(ref).command_id, command(restored, created_at='2026-10-08T00:00:00Z').command_id)
        distinct = [ExternalRef(TransportScope('github', 'github.com/owner/other'), ref.external_id),
                    ExternalRef(TransportScope('clickup', ref.scope.instance), ref.external_id),
                    ExternalRef(ref.scope, 'issue:13/comment:91'),
                    ExternalRef(ref.scope, 'issue:12/comment:92')]
        ids = [ref.source_id(), ref.source_id('edit:1'), ref.source_id('edit:2')]
        ids.extend(other.source_id() for other in distinct)
        self.assertEqual(len(set(ids)), len(ids))
        self.assertLessEqual(len(ref.source_id()), 512)
        with self.assertRaises(FrozenInstanceError):
            ref.external_id = 'changed'

    def test_malformed_references_fail_closed(self):
        for kind in ('GitHub', 'git hub', '', None):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                TransportScope(kind, 'instance')
        for value in ('', ' ', None, 1, 'x' * 513, '\ud800'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                TransportScope('github', value)
            with self.assertRaises(ValueError):
                ExternalRef(TransportScope('github', 'instance'), value)
        with self.assertRaises(ValueError):
            ExternalRef('github', 'task')
        ref = ExternalRef(TransportScope('github', 'instance'), 'task')
        with self.assertRaises(ValueError):
            ref.source_id('')

    def test_unimplemented_io_is_explicit_never_empty_or_success(self):
        adapter = GitHubTransport('owner/tasks')
        target = adapter.issue_ref(12)
        self.assertIsInstance(adapter, Transport)
        for operation in (adapter.poll_commands, lambda: adapter.fetch_task(target),
                          lambda: adapter.publish_event(target, event())):
            with self.assertRaises(TransportError) as error:
                operation()
            self.assertEqual(error.exception.code, 'not_implemented')
            self.assertFalse(error.exception.uncertain)
        for other in (GitHubTransport('owner/other').issue_ref(12), None, {'number': 12}):
            for operation in (lambda: adapter.fetch_task(other),
                              lambda: adapter.publish_event(other, event())):
                with self.assertRaises(TransportError) as error:
                    operation()
                self.assertEqual(error.exception.code, 'wrong_scope')
        with self.assertRaises(TransportError) as error:
            adapter.publish_event(target, {'owner_state': 'MERGED'})
        self.assertEqual(error.exception.code, 'invalid_event')

    def test_ambiguous_external_write_does_not_claim_success(self):
        class UncertainTransport(MemoryTransport):
            def publish_event(self, target, event):
                raise TransportError('publish', 'acceptance_unknown', uncertain=True)
        scope = TransportScope('clickup', 'test-only')
        task = ExternalTask(ExternalRef(scope, 'task'), 'Task', 'Body')
        adapter = UncertainTransport(scope, task, ())
        with self.assertRaises(TransportError) as error:
            adapter.publish_event(task.ref, event())
        self.assertTrue(error.exception.uncertain)
        self.assertEqual(adapter.delivered, [])

    def test_receipt_and_delivery_cannot_advance_local_owner_or_lifecycle(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'task_bridge_state.json'
            state = {'issues': {'12': {'task_id': 'local-task', 'owner_state': None,
                                     'review_status': None, 'queue_status': 'STARTED'}}}
            write_json(path, state)
            before = path.read_bytes()
            adapter = FixtureGitHubTransport()
            journal = ControlJournal(Path(root) / 'control.json')
            ref = adapter.comment_ref(12, {'id': 91})
            owner = command(ref, kind='owner.command', payload={'action': 'MERGE'},
                            actor={'ref': 'local-owner', 'approved': True})
            with patch.object(bridge, 'publish') as publish, patch.object(bridge, 'mcp_call') as rpc:
                journal.record_command(owner)
                journal.append_event(event(payload={'owner_state': 'MERGED'}))
                adapter.publish_event(adapter.task.ref, event(payload={'owner_state': 'MERGED'}))
                publish.assert_not_called()
                rpc.assert_not_called()
            self.assertEqual(path.read_bytes(), before)
            self.assertIsNone(state['issues']['12']['owner_state'])
            self.assertIsNone(state['issues']['12']['review_status'])
            self.assertNotIn('transports', vars(bridge))
            self.assertNotIn('github_transport', vars(bridge))


class GitHubMappingTests(unittest.TestCase):
    def setUp(self):
        self.adapter = GitHubTransport('OWNER/Tasks')

    def test_mapping_preserves_v1_issue_reference_and_unparsed_text(self):
        body = '<!-- DEVORCHESTRATOR_TASK_V1 {"project":"test"} -->\nImplement task'
        issue = {'number': 12, 'title': 'Task', 'body': body, 'labels': [{'name': 'work:queued'}],
                 'html_url': 'https://github.com/owner/tasks/issues/12', 'url': 'https://api.github.com/issue',
                 'node_id': 'opaque', 'unexpected_provider_metadata': {'a': 'b'}}
        mapped = self.adapter.map_task(issue)
        self.assertEqual(mapped, ExternalTask(self.adapter.issue_ref('12'), 'Task', body, issue['html_url']))
        self.assertEqual(mapped.ref.external_id, 'issue:12')
        self.assertEqual(bridge.parse_task({'body': mapped.body}), bridge.parse_task(issue))
        self.assertEqual(self.adapter.map_task({**issue, 'html_url': None}).url, issue['url'])
        self.assertEqual(self.adapter.scope, GitHubTransport('owner/tasks').scope)

    def test_rest_graphql_and_url_comment_aliases_map_to_same_stable_source(self):
        url = 'https://github.com/owner/tasks/issues/12#issuecomment-91'
        rest = {'id': 91, 'node_id': 'IC_legacy', 'html_url': url, 'body': 'Follow-up'}
        graphql = {'id': 'IC_legacy', 'url': url, 'body': 'Follow-up'}
        refs = [self.adapter.comment_ref(12, value) for value in
                (rest, graphql, {'id': '91'}, {'url': url})]
        self.assertTrue(all(ref == refs[0] for ref in refs))
        self.assertEqual(refs[0].external_id, 'issue:12/comment:' + bridge.comment_id(rest))
        self.assertEqual(len({ref.source_id() for ref in refs}), 1)
        processed = {'IC_legacy'}
        self.assertTrue(bridge.comment_already_processed(rest, processed))
        self.assertIn('91', processed)
        # Display/body edits do not manufacture a new source identity.
        self.assertEqual(self.adapter.comment_ref(12, {**rest, 'body': 'Edited'}), refs[0])

    def test_missing_or_malformed_stable_github_references_are_rejected(self):
        for value in ({}, {'id': True}, {'id': 0}, {'id': 'IC_only'}, {'id': '1' * 513},
                      {'body': 'No body-based identity', 'createdAt': NOW},
                      {'url': 'https://github.com/owner/other/issues/12#issuecomment-91'},
                      {'url': 'https://github.com/owner/tasks/issues/13#issuecomment-91'}):
            with self.subTest(value=value), self.assertRaises(TransportError):
                self.adapter.comment_ref(12, value)
        for value in ({}, {'number': True}, {'number': 0}, {'number': '1' * 513},
                      {'number': 12, 'pull_request': {}},
                      {'number': 12, 'body': {}}, {'number': 12, 'title': False}):
            with self.subTest(value=value), self.assertRaises(TransportError):
                self.adapter.map_task(value)
        with self.assertRaises(TransportError):
            self.adapter.issue_ref('1' * 513)

    def test_m2a_consumes_adapter_identities_and_rejects_content_conflicts(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'control.json'
            journal = ControlJournal(path)
            ref = self.adapter.comment_ref(12, {'id': 91})
            first = journal.record_command(command(ref))
            replay = ControlJournal(path).record_command(command(
                GitHubTransport('owner/tasks').comment_ref(12, {'id': '91'})))
            self.assertFalse(first.duplicate)
            self.assertTrue(replay.duplicate)
            self.assertEqual(first.identity, replay.identity)
            before = path.read_bytes()
            with self.assertRaises(ValueError):
                journal.record_command(command(ref, payload={'text': 'Edited'}))
            self.assertEqual(path.read_bytes(), before)
            self.assertFalse(journal.record_command(command(ref, revision='edit:1', payload={'text': 'Edited'})).duplicate)
            other = GitHubTransport('owner/other').comment_ref(12, {'id': 91})
            self.assertFalse(journal.record_command(command(other)).duplicate)

    def test_shared_logical_identity_still_deduplicates_across_transports(self):
        with tempfile.TemporaryDirectory() as root:
            journal = ControlJournal(Path(root) / 'control.json')
            github = command(self.adapter.comment_ref(12, {'id': 91}), logical_id='shared-command')
            other = command(ExternalRef(TransportScope('clickup', 'test-only'), 'message:91'),
                            logical_id='shared-command')
            first = journal.record_command(github)
            alias = journal.record_command(other)
            self.assertEqual((alias.identity, alias.sequence, alias.duplicate), (first.identity, 1, True))
            self.assertEqual(len(journal.snapshot()['receipts']), 2)
