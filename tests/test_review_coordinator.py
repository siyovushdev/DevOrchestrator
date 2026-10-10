"""Writer checkpoints, request-bound acceptance, invalidation and lock retention."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from durable import read_json, write_json_if_changed
from review_coordinator import DurabilityFailure, ReviewCoordinator
import review_transitions as protocol
from scheduler import Limits, Scheduler
import chief_review
import task_bridge as bridge


class ReviewCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'state.json'
        self.state = {'issues': {'1': {'project': 'test', 'task_id': 'coding', 'coding_turn': 2,
                                      'review_round': 1, 'review_status': None}}}
        scheduler = Scheduler(self.state, Limits())
        scheduler.enqueue('coding', '1', 'coding', 'work', 'a' * 64)
        scheduler.reserve('coding')
        scheduler.started('coding', 'coding', 'DONE')
        self.report = dict(id='coding', turns=2, status='DONE', exit_code=0,
                           runtime_generation='coding-generation')
        self.inputs = {'project': 'test', 'reviewer': 'cursor'}
        self.coordinator = ReviewCoordinator(self.state, self.persist, 'org/repo')
        self.record = self.coordinator.begin('1', self.report, self.inputs)
        self.key = self.record['request_id']
        self.binding = dict(machine='work', repository='a' * 64, registry_digest='b' * 64,
                            snapshot={'fingerprint': 'c' * 64, 'tree': 'd' * 40})
        guard = dict(self.binding, schema_version=1, source_task_id='coding', source_turn=2,
                     source_generation='coding-generation', catalog_digest='e' * 64)
        self.arguments = dict(project='test', role='review', agent='cursor', prompt='private prompt',
                              review_binding=guard)
        self.candidate = dict(report=self.report, inputs=self.inputs, binding=self.binding,
                              arguments=self.arguments)

    def persist(self, state):
        return write_json_if_changed(self.path, state)

    def restart(self):
        self.state = read_json(self.path)
        self.coordinator = ReviewCoordinator(self.state, self.persist, 'org/repo')

    def inspection(self, **changes):
        result = dict(schema_version=1, request_id=self.key, task_id=self.key[:24], state='STARTED',
                      signature_version=3, metadata=dict(id=self.key[:24], role='review',
                      launch_request_id=self.key, review_binding=self.arguments['review_binding'],
                      runtime_generation='review-generation'))
        result.update(changes)
        return result

    def worker_report(self, **changes):
        result = dict(id=self.key[:24], role='review', launch_request_id=self.key, status='RUNNING',
            runtime_generation='review-generation', review_binding=self.arguments['review_binding'], turns=1)
        result.update(changes)
        return result

    def authorized(self):
        return self.coordinator.authorize('1', self.candidate)

    def accepted(self):
        self.authorized()
        self.assertEqual(self.coordinator.apply_inspection('1', self.inspection()), 'BOUND')

    def test_command_only_returns_after_pending_reservation_and_lock_are_durable(self):
        command = self.authorized()
        saved = read_json(self.path)
        self.assertEqual(saved['issues']['1']['review_transition']['phase'], 'LAUNCH')
        self.assertEqual(saved['issues']['1']['pending_rpc']['arguments'], command['arguments'])
        self.assertTrue(saved['work_queue'][self.key]['launch_pending'])
        self.assertEqual(saved['work_queue'][self.key]['status'], 'RESERVED')
        self.assertEqual(saved['repo_locks']['a' * 64]['key'], self.key)
        command['arguments']['prompt'] = 'mutated executor copy'
        self.assertEqual(self.state['issues']['1']['pending_rpc']['arguments']['prompt'], 'private prompt')

    def test_failed_persist_is_fatal_and_returns_no_dispatch_command(self):
        def failed(state):
            raise OSError('disk failed')
        self.coordinator.persist = failed
        with self.assertRaises(DurabilityFailure):
            self.authorized()
        self.assertNotIn('pending_rpc', read_json(self.path)['issues']['1'])

    def test_restart_reconciles_original_request_before_replay(self):
        command = self.authorized()
        self.restart()
        inspection = self.coordinator.inspection_command('1')
        self.assertEqual(inspection['arguments'], {'request_id': self.key, 'fence': False})
        self.assertEqual(self.coordinator.apply_inspection('1', self.inspection(state='ABSENT')), 'ABSENT')
        self.assertTrue(self.state['work_queue'][self.key]['launch_pending'])
        self.assertEqual(self.coordinator.replay_command('1'), command)

    def test_prepared_and_busy_receipts_do_not_rewrite_or_release(self):
        self.authorized()
        before = self.path.stat()
        for _ in range(10):
            self.assertEqual(self.coordinator.apply_inspection('1', self.inspection(state='BUSY')), 'BUSY')
            response = self.inspection(state='PENDING')
            response['metadata']['launch_prepared'] = True
            self.assertEqual(self.coordinator.apply_inspection('1', response), 'PREPARED')
        self.assertEqual(self.path.stat().st_ino, before.st_ino)
        self.assertFalse(Scheduler(self.state).release('1'))

    def test_stale_acceptance_is_drained_not_adopted_even_after_restart(self):
        self.authorized()
        self.coordinator.invalidate('1')
        self.restart()
        self.assertTrue(self.coordinator.inspection_command('1')['arguments']['fence'])
        with self.assertRaises(protocol.InvalidTransition):
            self.coordinator.replay_command('1')
        self.assertEqual(self.coordinator.apply_inspection('1', self.inspection()), 'BOUND')
        self.assertEqual(self.state['issues']['1']['review_transition']['phase'], 'DRAIN')
        self.assertIsNone(self.state['issues']['1'].get('review_task_id'))
        self.assertFalse(Scheduler(self.state).release('1'))
        self.assertFalse(self.coordinator.drain('1', self.worker_report(status='DONE')))
        self.assertFalse(self.coordinator.drain('1', self.worker_report(status='DONE', cleanup_state='CLEANED',
                                                                        cleanup_generation='old-generation')))
        self.assertTrue(self.coordinator.drain('1', self.worker_report(status='DONE', cleanup_state='CLEANED',
                                                                       cleanup_generation='review-generation')))
        self.assertNotIn('pending_rpc', self.state['issues']['1'])
        self.assertTrue(self.state['repo_locks'])  # Completion-to-next-phase ownership is retained.

    def test_adoption_and_effects_are_one_checkpoint_with_quality_gates_closed(self):
        self.accepted()
        effects = [{'kind': 'comment', 'payload': 'Review started'},
                   {'kind': 'transition', 'payload': {'target': 'dev:running'}}]
        self.assertTrue(self.coordinator.adopt('1', self.worker_report(), self.report, self.inputs,
                                              self.binding['snapshot'], effects))
        self.restart()
        item = self.state['issues']['1']
        self.assertEqual(item['review_task_id'], self.key[:24])
        self.assertEqual(item['review_round'], 2)
        self.assertEqual(item['review_status'], 'RUNNING')
        self.assertIsNone(item['owner_state'])
        self.assertFalse(item.get('chief_review'))
        self.assertNotIn('pending_rpc', item)
        self.assertNotIn('review_transition', item)
        self.assertEqual(len(self.state['outbox']), 2)
        self.assertTrue(all(e['status'] == 'PENDING' for e in self.state['outbox'].values()))
        self.assertFalse(Scheduler(self.state).release('1'))

    def test_changed_turn_or_tree_between_acceptance_and_adoption_closes_gates(self):
        for mutation in ('turn', 'tree'):
            with self.subTest(mutation=mutation):
                # Independent branch from the same durable authorized checkpoint.
                self.state['issues']['1']['review_transition'] = copy.deepcopy(self.record)
                self.state['issues']['1'].pop('pending_rpc', None)
                self.state['issues']['1']['coding_turn'] = 2
                self.state['work_queue'].pop(self.key, None)
                self.accepted()
                snapshot = copy.deepcopy(self.binding['snapshot'])
                if mutation == 'turn':
                    self.state['issues']['1']['coding_turn'] = 3
                else:
                    snapshot['fingerprint'] = 'f' * 64
                self.assertFalse(self.coordinator.adopt('1', self.worker_report(), self.report,
                                                        self.inputs, snapshot, []))
                self.assertEqual(self.state['issues']['1']['review_transition']['phase'], 'DRAIN')
                self.assertNotIn('review_task_id', self.state['issues']['1'])

    def test_invalidated_preparation_can_be_discarded_without_worker_fence(self):
        self.coordinator.invalidate('1')
        self.coordinator.discard_preparation('1')
        self.assertEqual(self.state['issues']['1']['review_launch_sequence'], 2)
        self.assertNotIn('review_transition', self.state['issues']['1'])
        self.assertTrue(self.state['repo_locks'])

    def test_absent_observation_does_not_settle_but_bound_fence_does(self):
        self.authorized()
        self.coordinator.invalidate('1')
        self.assertEqual(self.coordinator.apply_inspection('1', self.inspection(state='ABSENT')), 'ABSENT')
        self.assertIn('pending_rpc', self.state['issues']['1'])
        self.assertEqual(self.coordinator.apply_inspection('1', self.inspection(state='FENCED', launch_started=False)), 'FENCED')
        self.assertNotIn('pending_rpc', self.state['issues']['1'])
        self.assertFalse(self.state['work_queue'][self.key]['launch_pending'])

    def test_duplicate_acceptance_is_noop_and_conflicting_receipt_is_held(self):
        self.accepted()
        before = self.path.stat()
        self.coordinator.apply_inspection('1', self.inspection())
        self.assertEqual(self.path.stat().st_ino, before.st_ino)
        conflicting = self.inspection()
        conflicting['metadata']['runtime_generation'] = 'other'
        with self.assertRaises(protocol.InvalidTransition):
            self.coordinator.apply_inspection('1', conflicting)
        self.assertFalse(Scheduler(self.state).release('1'))

    def test_recovery_retains_transition_hold_even_with_terminal_worker_report(self):
        self.authorized()
        self.restart()
        Scheduler(self.state).recover([{'id': self.key[:24], 'status': 'DONE', 'cleanup_state': 'CLEANED'}])
        self.assertTrue(self.state['repo_locks'])
        self.assertTrue(self.state['work_queue'][self.key]['launch_pending'])

    def test_missing_lock_repaired_without_stealing_conflicting_owner(self):
        self.authorized()
        self.state['repo_locks'].clear()
        scheduler = Scheduler(self.state)
        self.assertTrue(scheduler.repository_busy('2', 'work', self.binding['repository']))
        scheduler.recover([])
        self.assertEqual(self.state['repo_locks'][self.binding['repository']]['owner'], '1')
        self.state['repo_locks'][self.binding['repository']] = {'owner': '2', 'key': 'other'}
        scheduler.recover([])
        self.assertEqual(self.state['repo_locks'][self.binding['repository']]['owner'], '2')
        self.assertTrue(scheduler.repository_busy('2', 'work', self.binding['repository']))

    def test_new_coding_generation_invalidates_without_losing_pending_intent(self):
        self.authorized()
        item = self.state['issues']['1']
        pending = copy.deepcopy(item['pending_rpc'])
        with patch.object(bridge, 'save_state', self.persist):
            bridge.observe_coding_turn(item, self.state, dict(self.report, turns=3,
                                                            runtime_generation='new-generation'))
        self.assertEqual(item['pending_rpc'], pending)
        self.assertEqual(item['review_transition']['phase'], 'DRAIN')
        self.assertFalse(chief_review.approval_complete(item))
        self.assertFalse(Scheduler(self.state).release('1'))

    def test_archive_preserves_authority_and_unreserved_preparation_can_settle(self):
        for authorized in (False, True):
            with self.subTest(authorized=authorized):
                if authorized:
                    self.state['issues']['1'].pop('runtime_exclusion', None)
                    self.state['issues']['1'].pop('issue_closed', None)
                    self.state['issues']['1']['review_transition'] = copy.deepcopy(self.record)
                    self.state['work_queue'].pop(self.key, None)
                    self.authorized()
                else:
                    Scheduler(self.state).enqueue(self.key, '1', 'review', 'work', self.binding['repository'])
                with patch.object(bridge, 'save_state', self.persist):
                    self.assertTrue(bridge.archive_issue({'number': 1, 'state': 'CLOSED'},
                                                        self.state['issues']['1'], self.state))
                self.assertFalse(Scheduler(self.state).release('1'))
                if authorized:
                    self.assertIn('pending_rpc', self.state['issues']['1'])
                    self.assertEqual(self.state['issues']['1']['review_transition']['phase'], 'DRAIN')
                else:
                    self.coordinator.discard_preparation('1')
                    self.assertNotIn('review_transition', self.state['issues']['1'])

    def test_transition_closes_even_legacy_exempt_chief_gate(self):
        item = self.state['issues']['1']
        self.assertFalse(chief_review.approval_complete(item))
        self.assertFalse(bridge.malformed_item(item))
        item['review_transition']['schema_version'] = 999
        self.assertTrue(bridge.malformed_item(item))
        self.assertFalse(chief_review.approval_complete(item))

    def test_archived_fenced_request_releases_only_after_coding_is_terminal(self):
        self.authorized()
        item = self.state['issues']['1']
        item['issue_closed'] = True
        self.coordinator.invalidate('1')
        self.coordinator.apply_inspection('1', self.inspection(state='FENCED', launch_started=False))
        scheduler = Scheduler(self.state)
        scheduler.jobs['coding']['status'] = 'UNKNOWN'
        scheduler.recover([])
        self.assertTrue(self.state['repo_locks'])
        scheduler.jobs['coding']['status'] = 'DONE'
        scheduler.recover([])
        self.assertFalse(self.state['repo_locks'])


if __name__ == '__main__':
    unittest.main()
