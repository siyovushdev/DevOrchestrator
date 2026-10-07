"""Local contract, durable replay and unchanged V1 boundaries; no transport I/O."""
from concurrent.futures import ThreadPoolExecutor
import copy
from dataclasses import FrozenInstanceError
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import control_plane as c
from durable import read_json, write_json
import task_bridge as bridge


def command(**changes):
    data = dict(
        source_transport='github', source_id='repo:issue:comment-10',
        kind='task.submit', payload={'text': 'Implement task', 'options': [1, {'x': True}]},
        actor={'owner_ref': 'owner-1'}, created_at='2026-10-07T00:00:00Z',
        task_ref='task-1', project_ref='project-1',
    )
    data.update(changes)
    return c.ControlCommand(**data)


def changed_command(**changes):
    data = command().to_dict()
    data.pop('command_id')
    data.update(changes)
    return c.ControlCommand(**data)


def event(**changes):
    data = dict(task_ref='task-1', kind='task.observed', revision=1,
                payload={'result': ['ready']}, created_at='2026-10-07T00:00:00Z')
    data.update(changes)
    return c.ControlEvent(**data)


class EnvelopeTests(unittest.TestCase):
    def test_command_identity_is_deterministic_and_round_trips(self):
        original = command()
        reordered = dict(reversed(list(original.to_dict().items())))
        self.assertEqual(c.ControlCommand.from_dict(reordered), original)
        self.assertEqual(c.ControlCommand.from_dict(json.loads(json.dumps(original.to_dict()))), original)
        self.assertEqual(original.command_id, changed_command(
            payload={'options': [1, {'x': True}], 'text': 'Implement task'},
            created_at='2026-10-08T00:00:00Z', source_order=42).command_id)
        self.assertEqual(original.semantic_bytes(), changed_command(
            payload={'options': [1, {'x': True}], 'text': 'Implement task'}).semantic_bytes())
        # Pin the versioned identity encoding against accidental contract drift.
        self.assertEqual(original.command_id,
                         '2d8e730f43184dad08e572c69789d81b6531a5bc04dba8759e2e906030fe5696')

    def test_identity_uses_source_or_logical_id_never_text(self):
        original = command()
        self.assertNotEqual(original.command_id, changed_command(source_id='other').command_id)
        self.assertNotEqual(original.command_id, changed_command(source_transport='clickup').command_id)
        shared = changed_command(logical_id='shared-1')
        self.assertEqual(shared.command_id, changed_command(
            logical_id='shared-1', source_transport='clickup', source_id='cu-comment-5').command_id)
        self.assertNotEqual(shared.command_id, changed_command(logical_id='shared-2').command_id)
        self.assertNotEqual(shared.command_id, original.command_id)

    def test_envelopes_are_deeply_immutable_and_exports_detached(self):
        for envelope in (command(), event()):
            data = envelope.to_dict()
            restored = type(envelope).from_dict(data)
            data['payload']['new'] = 'caller mutation'
            data['payload'].clear()
            self.assertEqual(restored, envelope)
            with self.assertRaises(TypeError):
                envelope.payload['new'] = 'mutation'
            with self.assertRaises(FrozenInstanceError):
                envelope.kind = 'changed'
        value = command()
        with self.assertRaises(TypeError):
            value.payload['options'][1]['x'] = False
        with self.assertRaises(TypeError):
            value.actor['owner_ref'] = 'other'

    def test_missing_unknown_or_invalid_envelope_shapes_fail_closed(self):
        for envelope in (command(), event()):
            cls = type(envelope)
            data = envelope.to_dict()
            invalid = [None, [], 1, True, '', {}, {**data, 'workspace': '/cloud/path'}]
            invalid += [{key: value for key, value in data.items() if key != removed} for removed in data]
            for value in invalid:
                with self.subTest(cls=cls, value=value), self.assertRaises(ValueError):
                    cls.from_dict(value)

    def test_malformed_command_fields_fail_closed(self):
        invalid = {
            'schema_version': (True, 1.0, '1', 0, 2),
            'command_id': ('', 'a' * 64, None, 1),
            'source_transport': ('', 'GitHub', None, 'git hub', 1),
            'source_id': ('', ' ', None, 1, 'a' * 513),
            'logical_id': ('', False, [], 'a' * 513),
            'task_ref': ('', 1, [], 'a' * 513),
            'project_ref': ('', True, {}, 'a' * 513),
            'kind': ('', 'TASK', 'task submit', None, 'a' * 65),
            'actor': (None, [], '', {1: 'owner'}),
            'payload': (None, [], '', {1: 'text'}, {'x': float('nan')},
                        {'x': float('inf')}, {'x': object()}, {'x': (1, 2)}, {'x': '\ud800'}),
            'source_order': (True, -1, 1.0, '1', c.MAX_SEQUENCE + 1),
            'created_at': ('', None, '2026-02-30T00:00:00Z', '2026-10-07',
                           '2026-10-07T00:00:00', '2026-10-07T25:00:00Z'),
        }
        for name, values in invalid.items():
            for value in values:
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    c.ControlCommand.from_dict({**command().to_dict(), name: value})

    def test_malformed_event_fields_fail_closed(self):
        invalid = {
            'schema_version': (True, 1.0, '1', 0, 2),
            'event_id': ('', 'a' * 64, None, 1),
            'task_ref': ('', None, True, 'a' * 513),
            'kind': ('', 'TASK', None),
            'revision': (None, False, 0, -1, '1', 1.0, c.MAX_SEQUENCE + 1),
            'payload': (None, [], {'x': float('-inf')}, {'x': set()}),
            'created_at': (None, '', '2026-10-07T00:00:00+00:00'),
        }
        for name, values in invalid.items():
            for value in values:
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    c.ControlEvent.from_dict({**event().to_dict(), name: value})

    def test_direct_construction_validates_as_well(self):
        for changes in ({'schema_version': True}, {'payload': []}, {'source_id': ''},
                        {'actor': None}, {'command_id': 'forged'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                changed_command(**changes)
        for changes in ({'revision': True}, {'payload': []}, {'event_id': 'forged'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                event(**changes)

    def test_event_identity_binds_task_revision(self):
        original = event()
        self.assertEqual(c.ControlEvent.from_dict(original.to_dict()), original)
        self.assertNotEqual(original.event_id, event(revision=2).event_id)
        self.assertNotEqual(original.event_id, event(task_ref='task-2').event_id)
        self.assertEqual(original.event_id, event(payload={'changed': True}).event_id)
        self.assertEqual(original.event_id,
                         '17e73025462d49f4f2b676f40377d013f509a1b2bdd2eb43e2362141d167ddd2')


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'control_journal.json'
        self.journal = c.ControlJournal(self.path)

    def assert_rejected_without_write(self, callback):
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            callback()
        self.assertEqual(self.path.read_bytes(), before)

    def test_missing_journal_is_empty_without_initializing_v1(self):
        self.assertEqual(self.journal.snapshot(), {'schema_version': 1, 'receipts': [], 'events': []})
        self.assertFalse(self.path.exists())

    def test_restart_source_replay_is_noop_and_no_second_effect(self):
        effect = Mock()
        original = self.journal.record_command(command())
        if not original.duplicate:
            effect()
        before = self.path.read_bytes()
        with patch.object(c, 'write_json', side_effect=AssertionError('Replay must not write')):
            duplicate = c.ControlJournal(self.path).record_command(changed_command(
                created_at='2026-10-08T00:00:00Z', source_order=99))
        if not duplicate.duplicate:
            effect()
        self.assertEqual((original.sequence, duplicate.sequence), (1, 1))
        self.assertEqual(original.identity, duplicate.identity)
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(effect.call_count, 1)

    def test_cross_transport_dedupe_retains_alias_and_first_sequence(self):
        original = changed_command(logical_id='shared-1')
        alias = changed_command(logical_id='shared-1', source_transport='clickup', source_id='cu-1',
                                created_at='2026-10-08T00:00:00Z', source_order=1)
        first = self.journal.record_command(original)
        duplicate = self.journal.record_command(alias)
        self.assertFalse(first.duplicate)
        self.assertEqual(duplicate, c.JournalResult(first.identity, first.sequence, True))
        self.assertEqual(len(self.journal.snapshot()['receipts']), 2)
        with patch.object(c, 'write_json', side_effect=AssertionError('Replay must not write')):
            self.assertEqual(c.ControlJournal(self.path).record_command(alias), duplicate)
        # Remembering the alias prevents a changed/missing logical ID bypassing dedupe.
        self.assert_rejected_without_write(lambda: self.journal.record_command(changed_command(
            source_transport='clickup', source_id='cu-1')))

    def test_conflicting_logical_id_fails_closed_for_all_semantic_fields(self):
        self.journal.record_command(changed_command(logical_id='shared-1'))
        for changes in ({'payload': {'text': 'Other task'}}, {'kind': 'task.cancel'},
                        {'task_ref': 'other'}, {'project_ref': 'other'},
                        {'actor': {'owner_ref': 'other'}}):
            with self.subTest(changes=changes):
                self.assert_rejected_without_write(lambda: self.journal.record_command(changed_command(
                    logical_id='shared-1', source_transport='clickup', source_id='cu-1', **changes)))

    def test_source_id_cannot_be_reused_with_changed_content_or_logical_id(self):
        self.journal.record_command(command())
        for changes in ({'payload': {'text': 'Other'}}, {'logical_id': 'new'}, {'kind': 'task.cancel'}):
            with self.subTest(changes=changes):
                self.assert_rejected_without_write(lambda: self.journal.record_command(changed_command(**changes)))

    def test_same_text_on_distinct_sources_does_not_dedupe(self):
        results = [self.journal.record_command(value) for value in (
            command(), changed_command(source_id='other'),
            changed_command(source_transport='clickup'),
            changed_command(source_id='logical-source', logical_id='shared'))]
        self.assertEqual([result.sequence for result in results], [1, 2, 3, 4])
        self.assertTrue(all(not result.duplicate for result in results))
        self.assertEqual(len({result.identity for result in results}), 4)

    def test_events_have_global_order_and_contiguous_per_task_revision(self):
        values = [event(), event(task_ref='task-2'), event(revision=2), event(task_ref='task-2', revision=2)]
        results = [self.journal.append_event(value) for value in values]
        self.assertEqual([result.sequence for result in results], [1, 2, 3, 4])
        persisted = c.ControlJournal(self.path).snapshot()['events']
        self.assertEqual([entry['event'] for entry in persisted], [value.to_dict() for value in values])
        with patch.object(c, 'write_json', side_effect=AssertionError('Replay must not write')):
            self.assertEqual(self.journal.append_event(values[0]), c.JournalResult(values[0].event_id, 1, True))

    def test_event_rewrites_and_revision_gaps_fail_closed(self):
        self.journal.append_event(event())
        for value in (event(payload={'changed': True}), event(kind='task.other'),
                      event(created_at='2026-10-08T00:00:00Z'), event(revision=3),
                      event(task_ref='task-2', revision=2)):
            with self.subTest(value=value):
                self.assert_rejected_without_write(lambda: self.journal.append_event(value))

    def test_snapshot_mutation_cannot_change_stored_events(self):
        self.journal.append_event(event())
        snapshot = self.journal.snapshot()
        snapshot['events'][0]['event']['payload']['result'].append('changed')
        self.assertEqual(self.journal.snapshot()['events'][0]['event'], event().to_dict())

    def test_corrupt_json_and_structural_corruption_fail_closed_for_every_operation(self):
        self.journal.record_command(command())
        self.journal.append_event(event())
        good = read_json(self.path)
        mutations = [None, [], {}, {**good, 'schema_version': True}, {**good, 'schema_version': 2},
                     {**good, 'receipts': None}, {**good, 'events': {}}, {**good, 'extra': 'unknown'}]
        for collection, envelope, field, value in (
                ('receipts', 'command', 'command_id', 'forged'),
                ('receipts', 'command', 'payload', {'x': float('nan')}),
                ('events', 'event', 'event_id', 'forged'),
                ('events', 'event', 'revision', 2)):
            altered = copy.deepcopy(good)
            altered[collection][0][envelope][field] = value
            mutations.append(altered)
        for collection in ('receipts', 'events'):
            for sequence in (True, 0, 2):
                altered = copy.deepcopy(good)
                altered[collection][0]['sequence'] = sequence
                mutations.append(altered)
            altered = copy.deepcopy(good)
            repeated = copy.deepcopy(altered[collection][0])
            repeated['sequence'] = 2
            altered[collection].append(repeated)
            mutations.append(altered)
        raw_values = ['{corrupt', ''] + [json.dumps(value) for value in mutations]
        for raw in raw_values:
            self.path.write_text(raw)
            for operation in (self.journal.snapshot, lambda: self.journal.record_command(command()),
                              lambda: self.journal.append_event(event())):
                with self.subTest(raw=raw, operation=operation):
                    self.assert_rejected_without_write(operation)

    def test_persisted_conflicting_alias_and_order_corruption_fail_closed(self):
        self.journal.record_command(changed_command(logical_id='shared'))
        good = read_json(self.path)
        good['receipts'].append({'sequence': 2, 'command': changed_command(
            logical_id='shared', source_transport='clickup', payload={'text': 'conflict'}).to_dict()})
        write_json(self.path, good)
        self.assert_rejected_without_write(self.journal.snapshot)
        self.path.unlink()
        self.journal.append_event(event())
        self.journal.append_event(event(revision=2))
        data = read_json(self.path)
        data['events'].reverse()
        for index, row in enumerate(data['events'], 1):
            row['sequence'] = index
        write_json(self.path, data)
        self.assert_rejected_without_write(self.journal.snapshot)

    def test_duplicate_json_keys_are_corruption_including_nested_payloads(self):
        self.journal.record_command(command())
        good = self.path.read_text()
        corruptions = (
            good.replace('"schema_version": 1', '"schema_version": 2, "schema_version": 1', 1),
            good.replace('"text": "Implement task"', '"text": "conflict", "text": "Implement task"'),
        )
        for raw in corruptions:
            self.path.write_text(raw)
            for operation in (self.journal.snapshot, lambda: self.journal.record_command(command()),
                              lambda: self.journal.append_event(event())):
                self.assert_rejected_without_write(operation)

    def test_failed_save_does_not_report_success_or_cache_receipt(self):
        with patch.object(c, 'write_json', side_effect=OSError('storage unavailable')):
            with self.assertRaises(OSError):
                self.journal.record_command(command())
        self.assertFalse(self.path.exists())
        self.assertFalse(self.journal.record_command(command()).duplicate)
        before = self.path.read_bytes()
        with patch.object(c, 'write_json', side_effect=OSError('storage unavailable')):
            with self.assertRaises(OSError):
                self.journal.append_event(event())
        self.assertEqual(self.path.read_bytes(), before)
        self.assertFalse(self.journal.append_event(event()).duplicate)

    def test_failure_after_atomic_replacement_recovers_as_duplicate(self):
        # Model replacement succeeding before directory fsync/return fails.
        def uncertain_save(path, data):
            write_json(path, data)
            raise OSError('interrupted after replacement')

        for operation, value in (('record_command', command()), ('append_event', event())):
            with self.subTest(operation=operation):
                with patch.object(c, 'write_json', side_effect=uncertain_save):
                    with self.assertRaises(OSError):
                        getattr(self.journal, operation)(value)
                before = self.path.read_bytes()
                with patch.object(c, 'write_json', side_effect=AssertionError('Replay must not write')):
                    result = getattr(c.ControlJournal(self.path), operation)(value)
                self.assertTrue(result.duplicate)
                self.assertEqual(result.sequence, 1)
                self.assertEqual(self.path.read_bytes(), before)

    def test_concurrent_writers_record_one_new_command_and_event(self):
        def record(_):
            return c.ControlJournal(self.path).record_command(command())
        def append(_):
            return c.ControlJournal(self.path).append_event(event())
        with ThreadPoolExecutor(max_workers=4) as pool:
            receipts = list(pool.map(record, range(8)))
            events = list(pool.map(append, range(8)))
        self.assertEqual(sum(not result.duplicate for result in receipts), 1)
        self.assertEqual(sum(not result.duplicate for result in events), 1)
        self.assertEqual(len(self.journal.snapshot()['receipts']), 1)
        self.assertEqual(len(self.journal.snapshot()['events']), 1)

    def test_journal_rejects_raw_or_wrong_envelopes(self):
        for value in (None, command().to_dict(), event()):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.journal.record_command(value)
        for value in (None, event().to_dict(), command()):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.journal.append_event(value)
        self.assertFalse(self.path.exists())

    def test_existing_v1_state_and_old_task_issue_are_unchanged(self):
        legacy = {'issues': {'1': {'task_id': 'legacy-task', 'project': 'test',
                                 'processed_followups': ['old-comment'], 'owner_state': 'COMMITTED'}}}
        state_path = self.path.parent / 'task_bridge_state.json'
        write_json(state_path, legacy)
        before = state_path.read_bytes()
        issue = {'body': '<!-- DEVORCHESTRATOR_TASK_V1 {"project":"test"} -->\nTask'}
        parsed = bridge.parse_task(issue)
        self.journal.record_command(command())
        self.journal.append_event(event())
        with patch.object(bridge, 'STATE_FILE', state_path):
            self.assertEqual(bridge.load_state(), legacy)
            self.assertFalse(bridge.malformed_item(legacy['issues']['1']))
        self.assertEqual(bridge.parse_task(issue), parsed)
        self.assertEqual(state_path.read_bytes(), before)
        self.assertNotIn('control_plane', vars(bridge))
        # Accidentally pointing the new helper at V1 fails rather than migrating it.
        with self.assertRaises(ValueError):
            c.ControlJournal(state_path).record_command(command())
        self.assertEqual(state_path.read_bytes(), before)
