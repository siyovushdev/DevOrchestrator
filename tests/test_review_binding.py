"""Worker-bound review validation, receipt replay and preparation crash windows."""
from contextlib import redirect_stdout
import copy
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import devctl as d
import execution_profiles as e
import publication
import review_transitions as t
import scheduler
import worker_runtime as w
from durable import read_json, write_json


class ReviewBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root / 'tasks'
        self.cfg = {'machine': 'work', 'workspace': str(self.root / 'repo')}
        self.snapshot = {'fingerprint': 'c' * 64, 'tree': 'd' * 40}
        self.source = dict(id='coding', machine='work', workspace=self.cfg['workspace'],
            role='coding', agent='codex', turns=2, runtime_generation='source-generation',
            status='DONE', cleanup_state='ABSENT', cleanup_generation='source-generation')
        write_json(self.data / 'coding' / 'meta.json', self.source)
        for p in (patch.object(d, 'DATA_DIR', self.data),
                  patch.object(d, 'load_projects', return_value={'test': self.cfg}),
                  patch.object(scheduler, 'project_identity', return_value=('work', 'a' * 64)),
                  patch.object(publication.Repository, 'snapshot', return_value=self.snapshot),
                  patch.object(d, 'task_state', return_value=('DONE', 0)),
                  patch.object(e, 'discover', return_value={'version': 'fixture', 'help': '--model --config'})):
            p.start()
            self.addCleanup(p.stop)
        self.key = 'e' * 64
        self.binding = dict(schema_version=1, source_task_id='coding', source_turn=2,
            source_generation='source-generation', machine='work', repository='a' * 64,
            registry_digest=t.digest(self.cfg), catalog_digest=e.load_catalog(self.cfg)[1]['sha256'],
            snapshot=copy.deepcopy(self.snapshot))

    def run_review(self, binding=None):
        return d.run_project('test', 'review prompt', agent_override='codex', role='review',
            request_id=self.key, review_binding=self.binding if binding is None else binding)

    def fake_start(self, machine, agent, workspace, prompt, model, effort, task_id, role, execution=None):
        meta = d.load_meta(task_id)
        meta.pop('launch_prepared', None)
        meta.pop('prepared_request', None)
        meta.update(status='RUNNING', runtime_generation='review-generation')
        d.save_meta(d.task_dir(task_id), meta)
        return 0

    def test_bound_request_replays_without_launch_or_new_policy_discovery(self):
        with patch.object(d, '_run_agent', side_effect=self.fake_start) as start, redirect_stdout(io.StringIO()):
            self.run_review()
            receipt = read_json(self.data.parent / 'requests' / (self.key + '.json'))
            self.assertEqual(receipt['signature_version'], 3)
            with patch.object(e, 'discover', side_effect=AssertionError('Receipt replay must not rediscover')):
                self.run_review()
            self.assertEqual(start.call_count, 1)
        evidence = w.inspect_start_request(self.data, self.key)
        self.assertEqual(evidence['state'], 'STARTED')
        self.assertEqual(evidence['metadata']['review_binding'], self.binding)
        self.assertEqual(evidence['metadata']['launch_request_id'], self.key)

    def test_changed_source_fails_before_worker_admission_or_receipt(self):
        for field, value in (('turns', 3), ('runtime_generation', 'different'),
                             ('workspace', '/changed'), ('role', 'review')):
            with self.subTest(field=field):
                write_json(self.data / 'coding' / 'meta.json', dict(self.source, **{field: value}))
                with patch.object(w, 'admit') as admit, patch.object(d, '_run_agent') as start:
                    with self.assertRaises(e.StaleReviewInput):
                        self.run_review()
                admit.assert_not_called()
                start.assert_not_called()
                self.assertFalse((self.data / self.key[:24]).exists())
                self.assertEqual(w.inspect_start_request(self.data, self.key)['state'], 'ABSENT')

    def test_changed_tree_or_policy_fails_before_launch(self):
        for field, value in (('snapshot', {'fingerprint': 'f' * 64}),
                             ('registry_digest', 'f' * 64), ('catalog_digest', 'f' * 64),
                             ('repository', 'f' * 64)):
            with self.subTest(field=field), patch.object(d, '_run_agent') as start:
                with self.assertRaises(e.StaleReviewInput):
                    self.run_review(dict(self.binding, **{field: value}))
                start.assert_not_called()

    def test_failed_snapshot_is_not_a_confirmed_source_change(self):
        with patch.object(publication.Repository, 'snapshot', side_effect=OSError('temporary outage')):
            with self.assertRaises(OSError):
                self.run_review()
        self.assertEqual(w.inspect_start_request(self.data, self.key)['state'], 'ABSENT')

    def test_uncertain_coding_runtime_is_not_a_confirmed_source_change(self):
        with patch.object(d, 'task_state', return_value=('UNKNOWN', None)):
            with self.assertRaises(e.RuntimeUnavailable):
                self.run_review()
        self.assertEqual(w.inspect_start_request(self.data, self.key)['state'], 'ABSENT')

    def test_bound_request_cannot_replay_with_changed_or_missing_binding(self):
        with patch.object(d, '_run_agent', side_effect=self.fake_start), redirect_stdout(io.StringIO()):
            self.run_review()
            with self.assertRaises(ValueError):
                self.run_review(dict(self.binding, source_turn=3))
            with self.assertRaisesRegex(RuntimeError, 'original source binding'):
                d.run_project('test', 'review prompt', agent_override='codex', role='review', request_id=self.key)

    def test_prepared_receipt_revalidates_without_false_no_launch_proof(self):
        with patch.object(d, '_run_agent', side_effect=OSError('crash before start')):
            with self.assertRaises(OSError):
                self.run_review()
        self.assertTrue(d.load_meta(self.key[:24])['launch_prepared'])
        self.assertEqual(w.inspect_start_request(self.data, self.key)['state'], 'PENDING')
        write_json(self.data / 'coding' / 'meta.json', dict(self.source, turns=3))
        with patch.object(d, '_run_agent') as start, redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as caught:
                self.run_review()
        self.assertNotIsInstance(caught.exception, e.StaleReviewInput)
        start.assert_not_called()
        self.assertEqual(w.inspect_start_request(self.data, self.key, fence=True)['state'], 'PENDING')

    def test_prepared_receipt_can_resume_once_with_original_binding(self):
        with patch.object(d, '_run_agent', side_effect=OSError('crash before start')):
            with self.assertRaises(OSError):
                self.run_review()
        with patch.object(d, '_run_agent', side_effect=self.fake_start) as start, redirect_stdout(io.StringIO()):
            self.run_review()
            self.run_review()
        self.assertEqual(start.call_count, 1)

    def test_orphan_prepared_metadata_does_not_get_overwritten(self):
        directory = self.data / self.key[:24]
        write_json(directory / 'meta.json', {'id': self.key[:24], 'launch_prepared': True})
        before = (directory / 'meta.json').read_bytes()
        with patch.object(d, '_run_agent') as start:
            with self.assertRaisesRegex(RuntimeError, 'without a request receipt'):
                self.run_review()
        start.assert_not_called()
        self.assertEqual((directory / 'meta.json').read_bytes(), before)

    def test_real_start_metadata_reconstruction_preserves_binding(self):
        execution = e.resolve(self.cfg, agent='codex', role='review')
        write_json(self.data / self.key[:24] / 'meta.json', dict(id=self.key[:24],
            launch_prepared=True, prepared_request=self.key, review_binding=self.binding,
            launch_request_id=self.key))
        def spawn(directory, meta, command, log):
            log.close()
            return Mock(pid=0)
        with (patch.object(d, 'capture_git_baseline', return_value={}),
              patch.object(d, 'build_agent_command', return_value='true'),
              patch.object(d, 'spawn_runtime', side_effect=spawn), redirect_stdout(io.StringIO())):
            d._run_agent('work', 'codex', self.cfg['workspace'], 'review prompt',
                         task_id=self.key[:24], role='review', execution=execution)
        meta = d.load_meta(self.key[:24])
        self.assertEqual(meta['review_binding'], self.binding)
        self.assertEqual(meta['launch_request_id'], self.key)
        self.assertFalse(meta.get('launch_prepared'))


class RepositoryBindingTests(unittest.TestCase):
    setUp = ReviewBindingTests.setUp
    fake_start = ReviewBindingTests.fake_start

    def run_coding(self, binding=None):
        binding = binding if binding is not None else dict(machine='work', repository='a' * 64,
                                                         registry_digest=t.digest(self.cfg))
        return d.run_project('test', 'coding prompt', agent_override='codex', role='coding',
                             request_id=self.key, repository_binding=binding)

    def test_changed_repository_or_registry_cannot_cross_admission(self):
        binding = dict(machine='work', repository='a' * 64, registry_digest=t.digest(self.cfg))
        for field in ('machine', 'repository', 'registry_digest'):
            with self.subTest(field=field), patch.object(w, 'admit') as admit, patch.object(d, '_run_agent') as start:
                with self.assertRaises(e.StaleReviewInput):
                    self.run_coding(dict(binding, **{field: 'changed'}))
                admit.assert_not_called()
                start.assert_not_called()
                self.assertEqual(w.inspect_start_request(self.data, self.key)['state'], 'ABSENT')

    def test_accepted_replay_uses_original_binding_without_new_launch_or_discovery(self):
        binding = dict(machine='work', repository='a' * 64, registry_digest=t.digest(self.cfg))
        with patch.object(d, '_run_agent', side_effect=self.fake_start) as start, redirect_stdout(io.StringIO()):
            self.run_coding(binding)
            self.cfg['default_agent'] = 'cursor'
            with patch.object(e, 'discover', side_effect=AssertionError('Accepted receipt must replay')):
                self.run_coding(binding)
            self.assertEqual(start.call_count, 1)
        receipt = read_json(self.data.parent / 'requests' / (self.key + '.json'))
        self.assertEqual(receipt['signature_version'], 4)
        self.assertEqual(d.load_meta(self.key[:24])['repository_binding'], binding)

    def test_request_cannot_change_or_omit_binding(self):
        with patch.object(d, '_run_agent', side_effect=self.fake_start), redirect_stdout(io.StringIO()):
            self.run_coding()
            with self.assertRaises(ValueError):
                self.run_coding(dict(machine='work', repository='f' * 64, registry_digest=t.digest(self.cfg)))
            with self.assertRaisesRegex(RuntimeError, 'original repository binding'):
                d.run_project('test', 'coding prompt', agent_override='codex', request_id=self.key)

    def test_existing_unbound_request_cannot_retrofit_binding(self):
        with patch.object(d, '_run_agent', side_effect=self.fake_start), redirect_stdout(io.StringIO()):
            d.run_project('test', 'coding prompt', agent_override='codex', request_id=self.key)
            with self.assertRaisesRegex(RuntimeError, 'retrofit repository binding'):
                self.run_coding()

    def test_prepared_restart_resumes_exact_request_once(self):
        with patch.object(d, '_run_agent', side_effect=OSError('crash before start')):
            with self.assertRaises(OSError):
                self.run_coding()
        self.assertEqual(w.inspect_start_request(self.data, self.key)['state'], 'PENDING')
        with patch.object(d, '_run_agent', side_effect=self.fake_start) as start, redirect_stdout(io.StringIO()):
            self.run_coding()
            self.run_coding()
        self.assertEqual(start.call_count, 1)

    def test_changed_prepared_binding_is_uncertain_and_cannot_be_fenced(self):
        binding = dict(machine='work', repository='a' * 64, registry_digest=t.digest(self.cfg))
        with patch.object(d, '_run_agent', side_effect=OSError('crash before start')):
            with self.assertRaises(OSError):
                self.run_coding(binding)
        self.cfg['default_agent'] = 'cursor'
        with patch.object(d, '_run_agent') as start, redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, 'original request retained') as caught:
                self.run_coding(binding)
        self.assertNotIsInstance(caught.exception, e.StaleReviewInput)
        start.assert_not_called()
        self.assertEqual(w.inspect_start_request(self.data, self.key, fence=True)['state'], 'PENDING')

    def test_orphan_metadata_retains_ownership_without_second_start(self):
        path = self.data / self.key[:24] / 'meta.json'
        write_json(path, dict(id=self.key[:24], launch_prepared=True))
        before = path.read_bytes()
        with patch.object(d, '_run_agent') as start:
            with self.assertRaisesRegex(RuntimeError, 'without a request receipt'):
                self.run_coding()
        start.assert_not_called()
        self.assertEqual(path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
