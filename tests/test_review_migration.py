import copy
import unittest
from unittest.mock import patch

import chief_review
from durable import read_json
from review_migration import LegacyReviewRecovery
from scheduler import Scheduler
import review_transitions as protocol
import test_review_coordinator as fixtures


class LegacyReviewMigrationTests(unittest.TestCase):
    persist = fixtures.ReviewCoordinatorTests.persist

    def setUp(self):
        fixtures.ReviewCoordinatorTests.setUp(self)
        self.coordinator.authorize('1', self.candidate)
        item = self.state['issues']['1']
        item.pop('review_transition')
        item['pending_rpc']['arguments'].pop('review_binding')
        self.original = copy.deepcopy(item['pending_rpc'])
        self.sent = {}
        class Dispatch:
            def submit(_, key, command):
                if key in self.sent:
                    return False
                self.sent[key] = command
                return True
        self.dispatcher = Dispatch()
        self.recovery = LegacyReviewRecovery(self.state, self.persist, 'org/repo', self.dispatcher)
        self.recovery.enroll()

    def restart(self):
        self.state = read_json(self.path)
        self.recovery = LegacyReviewRecovery(self.state, self.persist, 'org/repo', self.dispatcher)
        self.recovery.enroll()

    def inspection(self, **changes):
        response = dict(schema_version=1, state='STARTED', signature_version=2, request_id=self.key,
            task_id=self.key[:24], metadata=dict(id=self.key[:24], role='review', turns=1, machine='work',
            workspace='/registered', runtime_generation='legacy-generation'))
        response.update(changes)
        return response

    def result(self, value):
        self.recovery.next_attempt.clear()
        self.recovery.schedule('1')
        key = next(iter(self.sent))
        self.sent.pop(key)
        with patch('task_bridge.save_state', self.persist):
            self.recovery.complete(key, value, None)

    def test_enrollment_preserves_exact_args_request_id_and_repository_hold(self):
        item = self.state['issues']['1']
        self.assertEqual(item['pending_rpc'], self.original)
        self.assertEqual(item['review_migration']['phase'], 'INSPECT')
        self.assertEqual(item['review_launch_sequence'], 2)
        self.assertFalse(chief_review.approval_complete(item))
        self.assertFalse(Scheduler(self.state).release('1'))
        before = self.path.stat()
        self.recovery.enroll()
        self.assertEqual(before.st_ino, self.path.stat().st_ino)

    def test_restart_at_inspect_and_drain_never_replays_or_adopts_legacy_review(self):
        self.restart()
        self.result(self.inspection())
        self.assertEqual(self.state['issues']['1']['review_migration']['phase'], 'DRAIN')
        self.restart()
        report = dict(self.inspection()['metadata'], status='DONE', cleanup_state='CLEANED',
                      cleanup_generation='wrong-generation')
        self.result(report)
        self.assertIn('pending_rpc', self.state['issues']['1'])
        report['cleanup_generation'] = report['runtime_generation']
        self.result(report)
        item = self.state['issues']['1']
        self.assertNotIn('pending_rpc', item)
        self.assertNotIn('review_migration', item)
        self.assertFalse(item.get('review_task_id'))
        self.assertFalse(chief_review.approval_complete(item) and item.get('owner_state'))
        next_record = protocol.begin('org/repo', '1', item, self.report, self.inputs)
        self.assertEqual(next_record['sequence'], 3)
        self.assertNotEqual(next_record['request_id'], self.key)

    def test_absence_is_not_fence_but_durable_tombstone_settles(self):
        self.result(self.inspection(state='ABSENT'))
        self.assertEqual(self.state['issues']['1']['pending_rpc'], self.original)
        self.result(self.inspection(state='FENCED', launch_started=False))
        self.assertNotIn('pending_rpc', self.state['issues']['1'])
        self.assertEqual(self.state['work_queue'][self.key]['no_launch_proof'], 'FENCED')

    def test_prepared_legacy_launch_is_held_without_resume_or_observation_rewrites(self):
        response = self.inspection(state='PENDING')
        response['metadata']['launch_prepared'] = True
        self.result(response)
        self.restart()
        before = self.path.stat()
        for _ in range(10):
            self.result(response)
        self.assertEqual(before.st_ino, self.path.stat().st_ino)
        self.assertEqual(self.state['issues']['1']['review_migration']['phase'], 'HELD')
        self.assertEqual(self.state['issues']['1']['pending_rpc'], self.original)

    def test_missing_queue_evidence_and_corrupt_envelope_fail_closed(self):
        self.state['work_queue'].clear()
        self.state['repo_locks'].clear()
        self.assertTrue(Scheduler(self.state).repository_busy('2', 'work', 'a' * 64))
        with self.assertRaises(protocol.InvalidTransition):
            self.result(self.inspection(state='FENCED', launch_started=False))
        self.assertIn('pending_rpc', self.state['issues']['1'])
        self.state['issues']['1']['pending_rpc']['arguments']['prompt'] = 'changed'
        with self.assertRaises(protocol.InvalidTransition):
            self.recovery.schedule('1')


if __name__ == '__main__':
    unittest.main()
