"""Pure launch protocol: immutable authority and crash-reconstructible phases."""
import copy
import json
import unittest

import review_transitions as t


class ReviewTransitionTests(unittest.TestCase):
    def setUp(self):
        self.item = {'task_id': 'coding', 'coding_turn': 2, 'review_round': 1}
        self.report = dict(id='coding', turns=2, status='DONE', exit_code=0,
                           runtime_generation='generation-2', result={'verdict': 'DONE'})
        self.inputs = {'project': 'test', 'reviewer': 'cursor', 'policy': {}}
        self.binding = dict(machine='work', repository='a' * 64, registry_digest='b' * 64,
                            snapshot={'fingerprint': 'c' * 64, 'tree': 'd' * 40})
        self.arguments = dict(project='test', agent='cursor', role='review', prompt='private prompt')
        self.record = t.begin('org/repo', '12', self.item, self.report, self.inputs)

    def launched(self):
        return t.authorize(self.record, self.arguments, self.binding)

    def test_stable_request_algorithm_and_abandoned_round_is_not_reused(self):
        import hashlib
        self.assertEqual(self.record['request_id'],
                         hashlib.sha256(b'org/repo:12:start_task:review:2').hexdigest())
        self.assertEqual(t.begin('org/repo', '12', self.item, self.report, self.inputs), self.record)
        self.item['review_launch_sequence'] = self.record['sequence']
        later = t.begin('org/repo', '12', self.item, self.report, self.inputs)
        self.assertEqual(later['operation'], 'review:3')
        self.assertNotEqual(later['request_id'], self.record['request_id'])

    def test_preparation_is_not_launch_authority_and_inputs_remain_untouched(self):
        before = copy.deepcopy((self.record, self.arguments, self.binding))
        launched, pending = self.launched()
        self.assertEqual((self.record, self.arguments, self.binding), before)
        self.assertIsNone(self.record['request_digest'])
        self.assertEqual(launched['phase'], 'LAUNCH')
        self.assertEqual(pending['arguments']['prompt'], 'private prompt')
        self.assertNotIn('private prompt', json.dumps(launched))
        self.binding['snapshot']['tree'] = 'changed'
        self.arguments['prompt'] = 'changed'
        self.assertEqual(launched['binding']['snapshot']['tree'], 'd' * 40)
        self.assertEqual(pending['arguments']['prompt'], 'private prompt')

    def test_restart_at_every_checkpoint_preserves_exact_request(self):
        launched, pending = self.launched()
        evidence = dict(task_id=launched['request_id'][:24], request_id=launched['request_id'],
                        runtime_generation='review-generation')
        verified = t.accept(launched, pending, evidence)
        self.assertEqual(t.accept(verified, pending, evidence), verified)
        with self.assertRaises(t.InvalidTransition):
            t.accept(verified, pending, dict(evidence, runtime_generation='different'))
        draining = t.invalidate(verified, pending)
        for record, envelope in ((self.record, None), (launched, pending),
                                 (verified, pending), (draining, pending)):
            with self.subTest(phase=record['phase']):
                restored, saved_pending = json.loads(json.dumps([record, envelope]))
                self.assertEqual(t.validate(restored, saved_pending), record)
                if envelope:
                    self.assertEqual(t.digest(saved_pending), launched['request_digest'])

    def test_invalidated_request_keeps_late_acceptance_without_reopening_review(self):
        launched, pending = self.launched()
        stale = t.invalidate(launched, pending)
        late = t.accept(stale, pending, dict(task_id=launched['request_id'][:24],
            request_id=launched['request_id'], runtime_generation='review-generation'))
        self.assertEqual(late['phase'], 'DRAIN')
        self.assertTrue(late['invalidated'])
        self.assertTrue(t.holds_repository({'review_transition': late}))
        self.assertFalse(t.source_matches(late, self.report, self.inputs))

    def test_invalidated_preparation_cannot_authorize(self):
        stale = t.invalidate(self.record)
        with self.assertRaises(t.InvalidTransition):
            t.authorize(stale, self.arguments, self.binding)

    def test_transient_observations_do_not_change_source_binding(self):
        before = copy.deepcopy(self.record)
        for attempt in range(100):
            report = dict(self.report, heartbeat_age=attempt, timings={'seconds': attempt})
            self.assertTrue(t.source_matches(self.record, report, self.inputs))
        self.assertEqual(self.record, before)
        for field, value in (('turns', 3), ('runtime_generation', 'new'),
                             ('status', 'RUNNING'), ('result', {'verdict': 'changed'})):
            self.assertFalse(t.source_matches(self.record, dict(self.report, **{field: value}), self.inputs))
        self.assertFalse(t.source_matches(self.record, self.report, dict(self.inputs, reviewer='codex')))

    def test_corrupt_or_unknown_protocol_cannot_become_authority(self):
        launched, pending = self.launched()
        for field, value in (('schema_version', 2), ('schema_version', True),
                             ('request_id', 'e' * 64), ('operation', 'review:9'),
                             ('phase', 'DISPATCHED'), ('phase', []), ('phase', 'PREPARE'),
                             ('phase', 'VERIFY'), ('phase', 'DRAIN'), ('request_digest', None)):
            with self.subTest(field=field, value=value), self.assertRaises(t.InvalidTransition):
                t.validate(dict(launched, **{field: value}), pending)
        for key, value in (('prompt', 'different'), ('role', 'coding'), ('request_id', 'f' * 64)):
            changed = copy.deepcopy(pending)
            changed['arguments'][key] = value
            with self.subTest(key=key), self.assertRaises(t.InvalidTransition):
                t.validate(launched, changed)
        self.assertTrue(t.holds_repository({'review_transition': {'schema_version': 999}}))

    def test_unbound_acceptance_is_rejected(self):
        launched, pending = self.launched()
        for evidence in ({}, {'task_id': 'other', 'request_id': launched['request_id'],
                              'runtime_generation': 'generation'},
                         {'task_id': launched['request_id'][:24], 'request_id': launched['request_id'],
                          'runtime_generation': None}):
            with self.subTest(evidence=evidence), self.assertRaises(t.InvalidTransition):
                t.accept(launched, pending, evidence)


if __name__ == '__main__':
    unittest.main()
