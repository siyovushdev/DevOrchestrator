"""Chief foundation contracts and the unchanged V1 boundary; no external I/O."""
import copy
from dataclasses import FrozenInstanceError
import json
import unittest

import chief_review as c
import notifications
import task_bridge as b


def reviewed_state():
    return dict(
        task_id='coding-task', coding_turn=2, coding_status='DONE',
        review_coding_turn=2, review_round=3, review_status='PASS',
        reviewed_tree={'fingerprint': 'a' * 64, 'root': '/trusted/local/repo'},
        owner_state='READY_FOR_OWNER', owner_approved_at='2026-10-07T00:00:00+00:00',
    )


class ChiefIdentityTests(unittest.TestCase):
    def setUp(self):
        self.item = reviewed_state()
        self.identity = c.build_identity(self.item)

    def test_deterministic_detached_json_round_trip(self):
        before = copy.deepcopy(self.item)
        reordered = dict(reversed(list(self.item.items())))
        self.assertEqual(self.identity, c.build_identity(reordered))
        data = json.loads(json.dumps(self.identity.to_dict(), sort_keys=True))
        self.assertEqual(c.validate_identity(data, self.item), self.identity)
        self.assertEqual(data['schema_version'], c.SCHEMA_VERSION)
        self.assertEqual(data['bundle_version'], c.BUNDLE_VERSION)
        # Pin the version-1 encoding so a future refactor cannot silently rebind it.
        self.assertEqual(data['bundle_id'],
                         '09d6277cdf7c3965624b18bd8e6a144e1ee56862c56546e7d3fd486b329062f7')
        data['task_id'] = 'changed-copy'
        self.assertEqual(self.identity.task_id, 'coding-task')
        self.assertEqual(self.item, before)

    def test_each_binding_field_changes_identity(self):
        changes = (
            {'task_id': 'other-task'},
            {'coding_turn': 3, 'review_coding_turn': 3},
            {'review_round': 4},
            {'reviewed_tree': {'fingerprint': 'b' * 64}},
        )
        ids = {self.identity.bundle_id}
        for change in changes:
            with self.subTest(change=change):
                current = {**self.item, **change}
                new = c.build_identity(current)
                self.assertNotEqual(new.bundle_id, self.identity.bundle_id)
                ids.add(new.bundle_id)
                with self.assertRaises(ValueError):
                    c.validate_identity(self.identity.to_dict(), current)
        self.assertEqual(len(ids), 5)

    def test_values_are_immutable(self):
        for name, value in (('task_id', 'changed'), ('bundle_id', 'forged')):
            with self.subTest(name=name), self.assertRaises(FrozenInstanceError):
                setattr(self.identity, name, value)

    def test_missing_identity_fields_fail_closed(self):
        for name in self.identity.to_dict():
            data = self.identity.to_dict()
            del data[name]
            with self.subTest(name=name), self.assertRaises(ValueError):
                c.validate_identity(data, self.item)

    def test_malformed_identity_fields_fail_closed(self):
        invalid = {
            'task_id': (None, '', 1, [], {}, '/cloud/path', '../task', 'a..b', 'a' * 129),
            'coding_turn': (None, True, 0, -1, '2', 2.0, [], {}, c.MAX_SEQUENCE + 1),
            'review_round': (None, False, 0, -1, '3', 3.0, [], {}, c.MAX_SEQUENCE + 1),
            'reviewed_tree_fingerprint': (None, '', [], {}, 'a' * 63, 'a' * 65, 'g' * 64, 'A' * 64),
            'schema_version': (None, True, '1', 1.0, 0, 2, [], {}),
            'bundle_version': (None, True, '1', 1.0, 0, 2, [], {}),
            'bundle_id': (None, '', [], {}, 'b' * 64, self.identity.bundle_id.upper()),
        }
        for name, values in invalid.items():
            for value in values:
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    c.validate_identity({**self.identity.to_dict(), name: value}, self.item)

    def test_invalid_shapes_and_unknown_fields_fail_closed(self):
        for value in (None, [], 'identity', 42, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                c.validate_identity(value, self.item)
        for name in ('workspace', 'verdict', 'extra'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                c.validate_identity({**self.identity.to_dict(), name: 'untrusted'}, self.item)

    def test_changed_fields_require_recomputed_bundle_id(self):
        data = self.identity.to_dict()
        data['review_round'] += 1
        current = {**self.item, 'review_round': data['review_round']}
        with self.assertRaisesRegex(ValueError, 'bundle id mismatch'):
            c.validate_identity(data, current)

    def test_missing_trusted_fields_fail_closed(self):
        for name in ('task_id', 'coding_turn', 'coding_status', 'review_coding_turn',
                     'review_round', 'review_status', 'reviewed_tree'):
            current = dict(self.item)
            del current[name]
            with self.subTest(name=name), self.assertRaises(ValueError):
                c.build_identity(current)

    def test_stale_or_malformed_trusted_review_evidence_fails_closed(self):
        changes = [
            {'coding_status': status} for status in ('RUNNING', 'STARTING', 'UNKNOWN', 'FAILED', None, [])
        ] + [
            {'review_status': status} for status in ('RUNNING', 'FAILED', 'NEEDS_OWNER', None, [])
        ] + [
            {'coding_turn': 3}, {'review_coding_turn': 1}, {'review_coding_turn': True},
            {'review_coding_turn': 2.0}, {'review_coding_turn': '2'},
            {'review_hold': {}}, {'review_hold': False},
            {'reviewed_tree': None}, {'reviewed_tree': []}, {'reviewed_tree': {}},
            {'reviewed_tree': {'fingerprint': True}},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                c.validate_identity(self.identity.to_dict(), {**self.item, **change})
        for value in (None, [], 42, 'state'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                c.build_identity(value)

    def test_v1_coding_invalidation_rejects_previous_identity(self):
        current = copy.deepcopy(self.item)
        b.invalidate_quality_gates(current)
        current.update(coding_turn=3, coding_status='RUNNING')
        with self.assertRaises(ValueError):
            c.validate_identity(self.identity.to_dict(), current)

    def test_only_existing_fingerprint_is_consumed(self):
        current = copy.deepcopy(self.item)
        current.update(workspace='/cloud/arbitrary', chief_verdict='PASS', owner_state=None)
        current['reviewed_tree'].update(root='/cloud/other', branch='changed', tree='unused')
        self.assertEqual(c.build_identity(current), self.identity)
        self.assertNotIn('root', self.identity.to_dict())
        self.assertNotIn('workspace', self.identity.to_dict())

    def test_v1_state_without_chief_fields_is_valid_and_unchanged(self):
        before = copy.deepcopy(self.item)
        self.assertFalse(b.malformed_item(self.item))
        self.assertTrue(notifications.review_approval_complete(self.item))
        c.validate_bundle(c.build_bundle(self.item).to_dict(), self.item)
        self.assertEqual(self.item, before)
        self.assertFalse(b.malformed_item(self.item))
        self.assertTrue(notifications.review_approval_complete(self.item))
        self.assertEqual(b.owner_action_plan(self.item['owner_state'], 'COMMIT')['status'], 'ALLOWED')
        # Older V1 records remain loadable; no missing turn evidence is invented.
        legacy = dict(self.item)
        del legacy['coding_turn']
        del legacy['review_coding_turn']
        self.assertFalse(b.malformed_item(legacy))
        self.assertTrue(notifications.review_approval_complete(legacy))
        with self.assertRaises(ValueError):
            c.build_identity(legacy)


class ChiefBundleTests(unittest.TestCase):
    def setUp(self):
        self.item = reviewed_state()
        self.bundle = c.build_bundle(self.item, 'Independent review complete')

    def test_bounded_envelope_round_trip_and_immutability(self):
        data = json.loads(json.dumps(self.bundle.to_dict()))
        self.assertEqual(set(data), {'identity', 'summary'})
        self.assertEqual(c.validate_bundle(data, self.item), self.bundle)
        with self.assertRaises(FrozenInstanceError):
            self.bundle.summary = 'changed'
        data['identity']['task_id'] = 'changed-copy'
        self.assertEqual(self.bundle.identity.task_id, self.item['task_id'])
        self.assertEqual(c.build_bundle(self.item).summary, '')
        self.assertEqual(c.build_bundle(self.item, 'x' * c.MAX_SUMMARY_LENGTH).identity,
                         self.bundle.identity)

    def test_envelope_rejects_missing_unknown_or_malformed_fields(self):
        data = self.bundle.to_dict()
        invalid = [None, [], 42, '', {}, {'identity': data['identity']},
                   {'summary': ''}, {**data, 'workspace': '/cloud/path'},
                   {**data, 'diff': 'unsupported'}]
        invalid += [{**data, 'identity': value} for value in (None, [], {}, 'id')]
        invalid += [{**data, 'summary': value} for value in (None, [], {}, True, 'x' * (c.MAX_SUMMARY_LENGTH + 1))]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                c.validate_bundle(value, self.item)

    def test_direct_construction_also_rejects_malformed_values(self):
        with self.assertRaises(ValueError):
            c.ChiefReviewIdentity('task', True, 1, 'a' * 64)
        with self.assertRaises(ValueError):
            c.EngineeringReviewBundle(self.bundle.identity.to_dict())
        for summary in (None, [], 'x' * (c.MAX_SUMMARY_LENGTH + 1)):
            with self.subTest(summary=summary), self.assertRaises(ValueError):
                c.build_bundle(self.item, summary)

    def test_bundle_rejects_stale_identity(self):
        current = {**self.item, 'review_round': 4}
        with self.assertRaisesRegex(ValueError, 'Stale'):
            c.validate_bundle(self.bundle.to_dict(), current)
