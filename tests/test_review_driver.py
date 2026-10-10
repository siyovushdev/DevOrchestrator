"""Deterministic phase scheduling; no worker, network or live workflow state."""
import asyncio
import copy
import unittest
from unittest.mock import patch

from execution_profiles import PreflightRejected, RuntimeUnavailable
from review_driver import ReviewDriver, inputs
from transition_io import Dispatcher
import test_review_coordinator as coordinator_tests


class ReviewDriverTests(unittest.TestCase):
    persist = coordinator_tests.ReviewCoordinatorTests.persist
    restart = coordinator_tests.ReviewCoordinatorTests.restart
    inspection = coordinator_tests.ReviewCoordinatorTests.inspection
    worker_report = coordinator_tests.ReviewCoordinatorTests.worker_report
    authorized = coordinator_tests.ReviewCoordinatorTests.authorized

    def setUp(self):
        coordinator_tests.ReviewCoordinatorTests.setUp(self)
        self.issue = {'number': 1, 'body': 'task', 'state': 'OPEN'}
        self.state['issues']['1'].pop('review_transition')
        self.state['issues']['1'].pop('review_launch_sequence')
        self.inputs = inputs(self.issue, self.state['issues']['1'])
        self.record = self.coordinator.begin('1', self.report, self.inputs)
        self.candidate['inputs'] = self.inputs
        self.sent = {}
        class Dispatch:
            def submit(_, key, command):
                if key in self.sent:
                    return False
                self.sent[key] = command
                return True
        self.driver = ReviewDriver(self.state, self.persist, 'org/repo', Dispatch())

    def result(self, value=None, error=None):
        key = next(iter(self.sent))
        self.sent.pop(key)
        with patch('task_bridge.save_state', self.persist):
            self.driver.complete(key, value, error, self.issue)

    def test_prepare_launch_inspect_verify_are_distinct_writer_steps(self):
        self.driver.schedule('1', self.issue)
        self.assertEqual(next(iter(self.sent.values())).name, 'prepare_review')
        self.assertNotIn('pending_rpc', self.state['issues']['1'])
        self.result(copy.deepcopy(self.candidate))
        command = next(iter(self.sent.values()))
        self.assertEqual(command.arguments()['name'], 'start_task')
        self.assertEqual(self.state['issues']['1']['review_transition']['phase'], 'LAUNCH')
        self.result({'status': 'RUNNING'})
        self.assertIsNone(self.state['issues']['1'].get('review_task_id'))
        self.driver.schedule('1', self.issue)
        self.assertEqual(next(iter(self.sent.values())).arguments()['name'], 'inspect_start_request')
        self.result(self.inspection())
        self.driver.schedule('1', self.issue)
        self.assertEqual(next(iter(self.sent.values())).name, 'verify_review')
        self.result(dict(report=self.worker_report(), source_report=self.report,
                         snapshot=self.binding['snapshot'], issue=self.issue))
        self.assertNotIn('pending_rpc', self.state['issues']['1'])
        self.assertEqual(self.state['issues']['1']['review_task_id'], self.key[:24])
        self.assertEqual(len(self.state['outbox']), 2)

    def test_restart_after_authorization_inspects_before_same_id_replay(self):
        original = self.authorized()
        self.restart()
        self.driver.state = self.state
        self.driver.coordinator = self.coordinator
        self.driver.schedule('1', self.issue)
        self.assertEqual(next(iter(self.sent.values())).arguments()['name'], 'inspect_start_request')
        self.result(self.inspection(state='ABSENT'))
        command = next(iter(self.sent.values())).arguments()
        self.assertEqual(command, {'name': 'start_task', 'arguments': original['arguments']})

    def test_late_start_after_invalidation_never_adopts_or_replays(self):
        self.driver.schedule('1', self.issue)
        self.result(copy.deepcopy(self.candidate))
        self.coordinator.invalidate('1')
        self.result({'status': 'RUNNING'})
        self.driver.schedule('1', self.issue)
        self.assertTrue(next(iter(self.sent.values())).arguments()['arguments']['fence'])
        self.result(self.inspection())
        self.driver.schedule('1', self.issue)
        self.assertEqual(next(iter(self.sent.values())).arguments()['name'], 'get_task_report')
        self.result(self.worker_report(status='DONE', cleanup_state='CLEANED',
                                       cleanup_generation='review-generation'))
        self.assertNotIn('review_transition', self.state['issues']['1'])
        self.assertIsNone(self.state['issues']['1'].get('review_task_id'))

    def test_changed_issue_after_preparation_never_launches(self):
        self.driver.schedule('1', self.issue)
        self.issue['body'] = 'new task instructions'
        self.result(copy.deepcopy(self.candidate))
        self.assertFalse(self.sent)
        self.assertTrue(self.state['issues']['1']['review_transition']['invalidated'])
        self.assertNotIn('pending_rpc', self.state['issues']['1'])

    def test_preflight_transient_failure_retains_attempt_permanent_rejection_stops_it(self):
        self.driver.schedule('1', self.issue)
        self.result(error=RuntimeUnavailable('do not persist secret token'))
        item = self.state['issues']['1']
        self.assertEqual(item['review_transition']['request_id'], self.key)
        self.assertNotIn('secret', str(item))
        self.driver.next_attempt.clear()
        item['reconciliation_retry']['next_attempt'] = 0
        self.driver.schedule('1', self.issue)
        self.result(error=PreflightRejected('unsupported model'))
        self.assertNotIn('review_transition', item)
        self.assertEqual(item['review_status'], 'FAILED')

    def test_cancelled_launch_keeps_authority_for_recovery(self):
        self.driver.schedule('1', self.issue)
        self.result(copy.deepcopy(self.candidate))
        before = copy.deepcopy(self.state['issues']['1']['pending_rpc'])
        self.result(error=asyncio.CancelledError())
        self.assertEqual(self.state['issues']['1']['pending_rpc'], before)
        self.assertTrue(self.state['work_queue'][self.key]['launch_pending'])

    def test_rejected_replay_cannot_discard_an_older_delayed_launch(self):
        self.driver.schedule('1', self.issue)
        self.result(copy.deepcopy(self.candidate))
        self.result({'status': 'REJECTED', 'launch_started': False, 'error': 'unsupported model'})
        item = self.state['issues']['1']
        self.assertEqual(item['review_transition']['phase'], 'DRAIN')
        self.assertIn('pending_rpc', item)
        self.driver.schedule('1', self.issue)
        self.assertTrue(next(iter(self.sent.values())).arguments()['arguments']['fence'])
        self.result(self.inspection())  # Older transmission succeeded before fence.
        self.driver.schedule('1', self.issue)
        self.result(self.worker_report(status='DONE', cleanup_state='CLEANED',
                                       cleanup_generation='review-generation'))
        self.assertEqual(item['review_status'], 'FAILED')
        self.assertNotIn('pending_rpc', item)
        self.assertFalse(item.get('review_task_id'))

    def test_restart_at_verify_adopts_only_after_fresh_proof_without_launch(self):
        self.authorized()
        self.coordinator.apply_inspection('1', self.inspection())
        self.restart()
        self.driver.state = self.state
        self.driver.coordinator = self.coordinator
        self.driver.schedule('1', self.issue)
        self.assertEqual(next(iter(self.sent.values())).name, 'verify_review')
        self.result(dict(report=self.worker_report(), source_report=self.report,
                         snapshot=self.binding['snapshot'], issue=self.issue))
        self.assertEqual(self.state['issues']['1']['review_task_id'], self.key[:24])
        self.assertFalse(self.sent)

    def test_newer_control_issue_prevents_older_verify_read_from_adopting(self):
        self.authorized()
        self.coordinator.apply_inspection('1', self.inspection())
        self.driver.schedule('1', self.issue)
        old = copy.deepcopy(self.issue)
        self.issue['body'] = 'changed during verification'
        self.result(dict(report=self.worker_report(), source_report=self.report,
                         snapshot=self.binding['snapshot'], issue=old))
        item = self.state['issues']['1']
        self.assertEqual(item['review_transition']['phase'], 'DRAIN')
        self.assertFalse(item.get('review_task_id'))


if __name__ == '__main__':
    unittest.main()
