"""Execution policy tests use a fake account-specific runtime and isolated workers."""
from contextlib import redirect_stdout
import copy
import io
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import devctl as d
import execution_profiles as e
import task_bridge as b


def fixture_catalog():
    return dict(schema_version=1, defaults={'agent': 'codex', 'models': {'codex': 'code', 'cursor': 'compose'},
                                          'review_profiles': {'codex': 'review'}},
                models=[dict(agent='cursor', model_id='compose', display_name='Compose', enabled=True,
                             supported_options={'fast': [False, True], 'context': ['200k', '1m']},
                             defaults={'fast': False, 'context': '200k'},
                             variants=[{'options': {'fast': fast, 'context': context},
                                        'runtime_model_id': 'compose' + ('-fast' if fast else '') + ('-1m' if context == '1m' else '')}
                                       for fast in (False, True) for context in ('200k', '1m')]),
                        dict(agent='cursor', model_id='other', display_name='Other', enabled=True,
                             supported_options={'fast': [False, True], 'context': ['200k', '1m']},
                             defaults={'fast': False, 'context': '200k'},
                             variants=[{'options': {'fast': fast, 'context': context},
                                        'runtime_model_id': 'other' + ('-fast' if fast else '') + ('-1m' if context == '1m' else '')}
                                       for fast in (False, True) for context in ('200k', '1m')]),
                        dict(agent='codex', model_id='code', display_name='Code', enabled=True,
                             supported_options={'effort': ['low', 'medium', 'high', 'xhigh']}, defaults={'effort': 'medium'})],
                profiles={'quick': {'agent': 'cursor', 'model': 'compose', 'options': {'fast': True}},
                          'deep': {'agent': 'cursor', 'model': 'compose', 'options': {'context': '1m'}},
                          'review': {'agent': 'codex', 'model': 'code', 'options': {'effort': 'high'}}})


class ExecutionProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.catalog_path = self.root / 'models.json'
        self.catalog = fixture_catalog(); self.write_catalog()
        p = patch.object(e, 'CATALOG_FILE', self.catalog_path); p.start(); self.addCleanup(p.stop)

    def write_catalog(self):
        self.catalog_path.write_text(json.dumps(self.catalog))

    def capabilities(self, machine, agent, capture):
        return dict(version='fixture-1', help='--model <model> --config <key=value>',
                    models=['compose', 'compose-fast', 'compose-1m', 'compose-fast-1m',
                            'other', 'other-fast', 'other-1m', 'other-fast-1m'])

    def resolved(self, **kwargs):
        return e.resolve(**kwargs)

    def test_named_profile_is_deterministic_and_options_reach_cursor_command(self):
        execution = self.resolved(execution_profile='quick')
        self.assertEqual(execution, self.resolved(execution_profile='quick'))
        self.assertEqual(execution['options'], {'fast': True, 'context': '200k'})
        command = shlex.split(d.build_agent_command('work', 'cursor', '/workspace', 'prompt', execution=execution))
        self.assertEqual(command[command.index('--model') + 1], 'compose-fast')
        self.assertIn('--sandbox', command)
        self.assertEqual(execution['decision_sources']['options']['fast'], 'profile')

    def test_explicit_model_and_options_override_profile_and_defaults(self):
        execution = self.resolved(execution_profile='quick', model='other', execution_options={'fast': False})
        self.assertEqual(execution['model'], 'other')
        self.assertEqual(execution['runtime_model'], 'other')
        self.assertEqual(execution['decision_sources']['model'], 'explicit')
        self.assertEqual(execution['decision_sources']['options']['fast'], 'explicit')

    def test_codex_model_effort_mapping_and_read_only_review(self):
        execution = self.resolved(agent='codex', model='code', effort='xhigh')
        args = shlex.split(d.build_agent_command('work', 'codex', '/workspace', 'prompt', execution=execution, role='review'))
        self.assertEqual(args[args.index('-m') + 1], 'code')
        self.assertEqual(args[args.index('-c') + 1], 'model_reasoning_effort="xhigh"')
        self.assertEqual(args[args.index('--sandbox') + 1], 'read-only')
        self.assertEqual(self.resolved(role='review')['options']['effort'], 'high')

    def test_auto_uses_configured_defaults_and_records_provenance(self):
        execution = self.resolved(agent='auto', model='auto', effort='auto')
        self.assertEqual((execution['agent'], execution['model'], execution['options']['effort']), ('codex', 'code', 'medium'))
        self.assertEqual(execution['decision_sources'], {'agent': 'auto', 'model': 'auto', 'options': {'effort': 'auto'}})

    def test_explicit_agent_wins_over_project_default_profile(self):
        execution = self.resolved(project={'execution_profile': 'quick'}, agent='codex')
        self.assertEqual(execution['agent'], 'codex')
        self.assertIsNone(execution['execution_profile'])
        with self.assertRaisesRegex(ValueError, 'explicit agent'):
            self.resolved(execution_profile='quick', agent='codex')

    def test_rejects_unsupported_options_combinations_disabled_and_unknown_models(self):
        for kwargs in [dict(agent='cursor', execution_options={'effort': 'high'}),
                       dict(agent='cursor', execution_options={'fast': 'true'}),
                       dict(agent='cursor', execution_options={'fast': 'auto', 'invented': 'auto'}),
                       dict(agent='codex', effort='max'), dict(agent='cursor', model='missing'),
                       dict(agent='cursor', effort='auto'), dict(execution_profile='missing'),
                       dict(execution_options=[], agent='cursor'),
                       dict(effort='high', execution_options={'effort': 'low'})]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError): self.resolved(**kwargs)
        self.catalog['models'][0]['enabled'] = False; self.write_catalog()
        with self.assertRaisesRegex(ValueError, 'disabled'): self.resolved(agent='cursor')

    def test_runtime_rejects_stale_variant_missing_model_flag_and_discovery_failure(self):
        execution = self.resolved(execution_profile='quick')
        capabilities = self.capabilities(None, None, None)
        capabilities['models'].remove('compose-fast')
        with self.assertRaisesRegex(ValueError, 'does not advertise'): e.validate_runtime(execution, capabilities)
        capabilities['help'] = '--print'
        with self.assertRaises(ValueError): e.validate_runtime(execution, capabilities)
        with self.assertRaisesRegex(e.RuntimeUnavailable, 'Cannot validate'):
            e.discover('work', 'cursor', lambda *args: subprocess.CompletedProcess([], 1, stdout='failed'))

    def test_failed_or_unreadable_discovery_is_unavailable_and_same_request_recovers(self):
        data = self.root / 'state/tasks'
        cfg = {'machine': 'work', 'workspace': str(self.root)}
        args = {'project': 'test', 'agent': 'cursor', 'model': 'compose', 'request_id': 'a' * 64}
        good = {'agent --version': 'fixture', 'agent --help': '--model <model>',
                'agent models': 'compose - Compose\nother - Other'}
        for command in good:
            for code, output in ((1, 'private prompt ghp_' + 'A' * 40), (0, ''), (0, 'unparsed output')):
                if command != 'agent models' and output == 'unparsed output':
                    continue
                with self.subTest(command=command, code=code, output=output):
                    def failed(machine, requested, **kwargs):
                        return subprocess.CompletedProcess([], code if requested == command else 0,
                            stdout=output if requested == command else good[requested])
                    with (patch.object(d, 'DATA_DIR', data),
                          patch.object(d, 'load_projects', return_value={'test': cfg}),
                          patch.object(d, 'capture_execution', side_effect=failed)):
                        result = d.preflight_task('start_task', args)
                    self.assertEqual(result, {'status': 'UNAVAILABLE', 'launch_started': False})
                    with self.assertRaises(e.RuntimeUnavailable):
                        e.check_preflight(result)
                    self.assertFalse(data.parent.exists())
                    with (patch.object(d, 'DATA_DIR', data),
                          patch.object(d, 'load_projects', return_value={'test': cfg}),
                          patch.object(d, 'capture_execution', side_effect=lambda machine, command, **kw:
                              subprocess.CompletedProcess([], 0, stdout=good[command]))):
                        self.assertEqual(d.preflight_task('start_task', args)['status'], 'READY')
                    self.assertFalse(data.parent.exists())

    def test_successful_inventory_excluding_requested_model_is_rejected(self):
        data = self.root / 'state/tasks'
        cfg = {'machine': 'work', 'workspace': str(self.root)}
        outputs = {'agent --version': 'fixture', 'agent --help': '--model <model>',
                   'agent models': 'other - Other'}
        with (patch.object(d, 'DATA_DIR', data),
              patch.object(d, 'load_projects', return_value={'test': cfg}),
              patch.object(d, 'capture_execution', side_effect=lambda machine, command, **kw:
                  subprocess.CompletedProcess([], 0, stdout=outputs[command]))):
            result = d.preflight_task('start_task', {'project': 'test', 'agent': 'cursor', 'model': 'compose'})
        self.assertEqual(result['status'], 'REJECTED')
        self.assertIs(result['launch_started'], False)
        with self.assertRaises(e.PreflightRejected):
            e.check_preflight(result)
        self.assertFalse(data.parent.exists())

    def test_locked_execution_validation_preserves_transient_classification(self):
        def failed(machine, command):
            return subprocess.CompletedProcess([], int(command == 'agent models'),
                stdout='--model <model>' if command == 'agent --help' else 'fixture')
        with self.assertRaises(e.RuntimeUnavailable):
            d.validated_execution({}, 'work', capture=failed, agent='cursor', model='compose')
        meta = {'execution': self.resolved(agent='cursor'), 'agent': 'cursor',
                'model': 'compose', 'reasoning_effort': None, 'machine': 'work'}
        with self.assertRaises(e.RuntimeUnavailable):
            e.resume_identity(meta, failed)

    def test_unavailability_requires_no_launch_proof(self):
        for result in ({'status': 'UNAVAILABLE'}, {'status': 'UNAVAILABLE', 'launch_started': True}):
            with self.subTest(result=result), self.assertRaises(RuntimeError) as caught:
                e.check_preflight(result)
            self.assertNotIsInstance(caught.exception, e.RuntimeUnavailable)

    def test_rejection_diagnostic_is_redacted_and_bounded(self):
        result = e.rejection('ghp_' + 'A' * 40 + 'x' * 1000)
        self.assertLessEqual(len(result['error']), 512)
        self.assertNotIn('ghp_', result['error'])

    def test_discovery_uses_execution_host_and_ignores_headers(self):
        calls = []
        def capture(machine, command):
            calls.append((machine, command))
            output = {'agent --version': 'fixture', 'agent --help': '--model <model>',
                      'agent models': 'Available models\ncompose - Compose (default)\ncompose-fast - Fast\nTip: other - none'}[command]
            return subprocess.CompletedProcess([], 0, stdout=output)
        capabilities = e.discover('remote-fixture', 'cursor', capture)
        self.assertEqual(capabilities['models'], ['compose', 'compose-fast'])
        self.assertEqual(calls, [('remote-fixture', 'agent --version'), ('remote-fixture', 'agent --help'), ('remote-fixture', 'agent models')])

    def test_runtime_probes_use_launch_environment_and_bounded_ssh(self):
        with patch.object(d.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, stdout='fixture')) as run:
            d.capture_execution('firefly', 'agent models')
        args = run.call_args.args[0]
        self.assertEqual(args[0], 'ssh')
        self.assertIn('ConnectTimeout=3', args)
        self.assertIn('NODE_USE_ENV_PROXY=1', args[-1])
        self.assertIn('HTTP_PROXY=', args[-1])
        self.assertEqual(run.call_args.kwargs['timeout'], d.PROBE_TIMEOUT)
        self.assertEqual(run.call_args.kwargs['stdin'], subprocess.DEVNULL)

    def test_malformed_catalog_fails_closed(self):
        mutations = [lambda x: x.update(schema_version=2), lambda x: x.update(models={}),
                     lambda x: x['models'].append(copy.deepcopy(x['models'][0])),
                     lambda x: x['models'][0].update(enabled='yes'),
                     lambda x: x['models'][0].update(defaults={'fast': 'yes'}),
                     lambda x: x['models'][0]['variants'].append(copy.deepcopy(x['models'][0]['variants'][0])),
                     lambda x: x['models'][0].update(variants=[]),
                     lambda x: x['profiles']['quick'].update(options={'invented': True}),
                     lambda x: x['defaults']['models'].update(cursor='missing'),
                     lambda x: x['models'][2]['supported_options'].update(fast=[True]),
                     lambda x: x['models'][0].update(model_id='compose[fast=true]'),
                     lambda x: x['models'][0].update(cli_flags=['--yolo'])]
        for mutation in mutations:
            catalog = fixture_catalog(); mutation(catalog)
            self.catalog_path.write_text(json.dumps(catalog))
            with self.subTest(catalog=catalog), self.assertRaisesRegex(ValueError, 'Invalid execution catalog'):
                self.resolved()
        self.catalog_path.write_text('{broken')
        with self.assertRaises(ValueError): self.resolved()

    def test_duplicate_json_fields_reserved_auto_and_invalid_codex_effort_fail_closed(self):
        self.catalog_path.write_text('{"schema_version":1,"schema_version":1}')
        with self.assertRaisesRegex(ValueError, 'Duplicate JSON field'): self.resolved()
        self.catalog = fixture_catalog()
        self.catalog['models'][2]['supported_options']['effort'] = [True]
        self.write_catalog()
        with self.assertRaisesRegex(ValueError, 'reasoning-effort names'): self.resolved()
        self.catalog = fixture_catalog()
        self.catalog['models'][0]['supported_options']['context'] += ['auto']
        self.write_catalog()
        with self.assertRaisesRegex(ValueError, 'reserved'): self.resolved()

    def test_review_defaults_are_independent_of_coding_profile(self):
        execution = self.resolved(project={'execution_profile': 'quick'}, agent='codex', role='review')
        self.assertEqual(execution['options'], {'effort': 'high'})
        self.assertEqual(execution['execution_profile'], 'review')

    def test_project_review_profile_agent_mismatch_fails_closed(self):
        project = {'review_execution_profiles': {'codex': 'quick'}}
        for agent in (None, 'auto', 'codex'):
            with self.subTest(agent=agent), self.assertRaisesRegex(ValueError, 'quick.*cursor.*codex'):
                self.resolved(project=project, agent=agent, role='review')

    def test_mismatched_review_mapping_never_probes_admits_or_spawns(self):
        data = self.root / 'state/tasks'
        registry = {'test': {'machine': 'work', 'workspace': str(self.root),
                             'review_execution_profiles': {'codex': 'quick'}}}
        with (patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value=registry),
              patch.object(e, 'discover') as probe, patch.object(d.worker_runtime, 'admit') as admit,
              patch.object(d, 'spawn_runtime') as spawn):
            with self.assertRaisesRegex(ValueError, 'quick.*cursor.*codex'):
                d.run_project('test', 'review', agent_override='codex', role='review')
            probe.assert_not_called(); admit.assert_not_called(); spawn.assert_not_called()
            self.assertFalse(data.exists())

    def test_valid_project_review_mapping_preserves_agent_and_options(self):
        project = {'review_execution_profiles': {'codex': 'review', 'cursor': 'quick'}}
        execution = self.resolved(project=project, agent='codex', role='review')
        self.assertEqual((execution['agent'], execution['execution_profile']), ('codex', 'review'))
        self.assertEqual(execution['options'], {'effort': 'high'})

    def test_catalog_model_id_auto_is_rejected_for_both_agents(self):
        for agent in ('cursor', 'codex'):
            catalog = fixture_catalog()
            entry = next(m for m in catalog['models'] if m['agent'] == agent)
            old_id = entry['model_id']
            entry['model_id'] = 'auto'
            catalog['defaults']['models'][agent] = 'auto'
            for profile in catalog['profiles'].values():
                if profile['agent'] == agent and profile['model'] == old_id:
                    profile['model'] = 'auto'
            with self.subTest(agent=agent), self.assertRaisesRegex(ValueError, 'model_id.*auto.*reserved'):
                e.validate_catalog(catalog)

    def test_shipped_cursor_auto_alias_is_explicit_and_reaches_runtime(self):
        project = {'execution_catalog': str(Path(e.__file__).with_name('execution_models.json'))}
        execution = self.resolved(project=project, agent='cursor', model='cursor-auto')
        self.assertEqual(execution['model'], 'cursor-auto')
        self.assertEqual(execution['runtime_model'], 'auto')
        self.assertEqual(execution['decision_sources']['model'], 'explicit')
        args = shlex.split(d.build_agent_command('work', 'cursor', '/workspace', 'prompt', execution=execution))
        self.assertEqual(args[args.index('--model') + 1], 'auto')
        default = self.resolved(project=project, agent='cursor', model='auto')
        self.assertEqual((default['model'], default['runtime_model']), ('cursor-auto', 'auto'))
        self.assertEqual(default['decision_sources']['model'], 'auto')
        # Auto selection still uses the named profile; an explicit provider Auto
        # model must reject that profile's unsupported fast option, never reroute.
        automatic = self.resolved(project=project, execution_profile='cursor-fast', model='auto')
        self.assertEqual(automatic['runtime_model'], 'composer-2.5-fast')
        with self.assertRaisesRegex(ValueError, 'cursor/cursor-auto.*does not support fast'):
            self.resolved(project=project, execution_profile='cursor-fast', model='cursor-auto')

    def test_explicit_cursor_auto_alias_overrides_profile_model(self):
        self.catalog['models'].append(dict(agent='cursor', model_id='cursor-auto', display_name='Provider Auto',
            enabled=True, supported_options={}, defaults={}, variants=[{'options': {}, 'runtime_model_id': 'auto'}]))
        self.catalog['profiles']['model-only'] = dict(agent='cursor', model='compose', options={})
        self.write_catalog()
        execution = self.resolved(execution_profile='model-only', model='cursor-auto')
        self.assertEqual((execution['model'], execution['runtime_model']), ('cursor-auto', 'auto'))
        self.assertEqual(execution['decision_sources']['model'], 'explicit')
        self.assertEqual(e.cli_options(execution), ['--model', 'auto'])

    def test_saved_provider_auto_identity_survives_catalog_id_change(self):
        # Pre-fix task snapshots used logical model auto; continuation must keep
        # their saved execution identity while new catalogs reject that ID.
        execution = self.resolved(agent='cursor')
        execution.update(model='auto', runtime_model='auto', options={})
        execution['decision_sources']['options'] = {}
        meta = dict(agent='cursor', machine='work', model='auto', reasoning_effort=None, execution=execution)
        self.catalog_path.write_text('{broken')
        with patch.object(e, 'discover', return_value={'version': 'fixture', 'help': '--model <model>', 'models': ['auto']}):
            self.assertEqual(e.resume_identity(meta, None), execution)
        self.assertEqual(e.cli_options(execution), ['--model', 'auto'])

    def test_probe_timeout_and_automatic_option_without_default_fail_closed(self):
        with self.assertRaisesRegex(e.RuntimeUnavailable, 'timed out'):
            e.discover('work', 'cursor', lambda *args: (_ for _ in ()).throw(subprocess.TimeoutExpired('agent', 5)))
        self.catalog['models'][0]['defaults'] = {}
        self.catalog['profiles'] = {'review': self.catalog['profiles']['review']}
        self.write_catalog()
        with self.assertRaisesRegex(ValueError, 'No configured default'):
            self.resolved(agent='cursor', execution_options={'fast': 'auto'})

    def test_constraint_applies_to_cursor_variants(self):
        self.catalog['models'][0]['constraints'] = {'allowed_combinations': [{'fast': False, 'context': '200k'}]}
        self.catalog['profiles'] = {'review': self.catalog['profiles']['review']}; self.write_catalog()
        with self.assertRaisesRegex(ValueError, 'Unsupported option combination'):
            self.resolved(agent='cursor', execution_options={'fast': True})

    def test_config_can_add_a_model_and_generic_option_without_code_changes(self):
        self.catalog['models'].append(dict(agent='cursor', model_id='new', display_name='New', enabled=True,
            supported_options={'quality': ['deep', 'fast']}, defaults={'quality': 'deep'},
            variants=[{'options': {'quality': 'deep'}, 'runtime_model_id': 'new-deep'},
                      {'options': {'quality': 'fast'}, 'runtime_model_id': 'new-fast'}]))
        self.catalog['profiles']['new-profile'] = dict(agent='cursor', model='new', options={'quality': 'fast'})
        self.write_catalog()
        execution = self.resolved(execution_profile='new-profile')
        self.assertEqual(e.cli_options(execution), ['--model', 'new-fast'])
        e.validate_runtime(execution, {'version': 'fixture', 'help': '--model <model>', 'models': ['new-fast']})

    def test_continuation_ignores_config_edits_and_revalidates_saved_runtime(self):
        execution = self.resolved(execution_profile='quick')
        meta = dict(agent='cursor', machine='work', model=execution['model'], reasoning_effort=None, execution=execution)
        self.catalog_path.write_text('{broken')
        with patch.object(e, 'discover', side_effect=self.capabilities):
            self.assertEqual(e.resume_identity(meta, None), execution)
            meta['model'] = 'other'
            with self.assertRaisesRegex(ValueError, 'disagrees'): e.resume_identity(meta, None)
        meta['model'] = execution['model']
        with patch.object(e, 'discover', return_value={'version': 'fixture', 'help': '--model <model>', 'models': []}):
            with self.assertRaises(ValueError): e.resume_identity(meta, None)

    def test_task_contract_keeps_legacy_parser_and_forwards_new_fields(self):
        meta = dict(project='test', agent='auto', model='auto', effort='auto', execution_profile='quick', execution_options={'fast': False})
        issue = dict(body='<!-- DEVORCHESTRATOR_TASK_V1 ' + json.dumps(meta) + ' -->\nDo work')
        self.assertEqual(b.parse_task(issue), ('test', 'auto', 'manual', 'auto', 'auto', 'Do work'))
        self.assertEqual(b.task_execution_request(issue), {'execution_profile': 'quick', 'execution_options': {'fast': False}})
        for change in ({'execution_options': []}, {'execution_profile': ''}):
            meta.update(change); issue['body'] = '<!-- DEVORCHESTRATOR_TASK_V1 ' + json.dumps(meta) + ' -->\nDo work'
            with self.assertRaises(ValueError): b.parse_task(issue)

    def test_report_format_exposes_execution_options_and_sources(self):
        execution = self.resolved(execution_profile='quick')
        report = b.format_report({'id': 'task', 'execution': execution, 'model': 'compose'})
        self.assertIn('compose-fast', report)
        self.assertIn('decision_sources', report)

    def test_worker_persists_and_replays_and_resumes_same_cursor_identity(self):
        repo = self.root / 'repo'; repo.mkdir()
        data = self.root / 'state/tasks'
        registry = {'test': {'machine': 'work', 'workspace': str(repo)}}
        commands = []
        class Process:
            pid = 0
        def spawn(directory, meta, command, log):
            commands.append(shlex.split(command))
            log.close()
            return Process()
        patches = [patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value=registry),
                   patch.object(e, 'discover', side_effect=self.capabilities),
                   patch.object(d, 'capture_git_baseline', return_value={'clean': True}),
                   patch.object(d, 'spawn_runtime', side_effect=spawn),
                   patch.object(d, 'machine_command', side_effect=lambda machine, command: shlex.split(command)),
                   patch.object(d, 'task_state', return_value=('DONE', 0)),
                   patch.object(d.worker_runtime, 'reconcile_task', return_value='ABSENT')]
        for p in patches: p.start(); self.addCleanup(p.stop)
        with redirect_stdout(io.StringIO()):
            d.run_project('test', 'prompt', agent_override='cursor', model='compose', execution_options={'fast': True}, request_id='a' * 64)
            task_id = 'a' * 24; meta = d.load_meta(task_id)
            first = meta['execution']
            self.assertEqual(commands[0][commands[0].index('--model') + 1], 'compose-fast')
            meta.update(session_id='session-test'); d.save_meta(d.task_dir(task_id), meta)
            (d.task_dir(task_id) / 'exit_code').write_text('0')
            self.catalog_path.write_text('{broken')
            d.run_project('test', 'prompt', agent_override='cursor', model='compose', execution_options={'fast': True}, request_id='a' * 64)
            self.assertEqual(len(commands), 1)
            with self.assertRaisesRegex(ValueError, 'different task arguments'):
                d.run_project('test', 'prompt', agent_override='cursor', model='compose', execution_options={'fast': False}, request_id='a' * 64)
            d.continue_task(task_id, 'followup', request_id='b' * 64)
        self.assertEqual(d.load_meta(task_id)['execution'], first)
        self.assertEqual(commands[-1][commands[-1].index('--model') + 1], 'compose-fast')
        self.assertEqual(commands[-1][commands[-1].index('--resume') + 1], 'session-test')
        (d.task_dir(task_id) / 'events.jsonl').write_text(json.dumps({'type': 'system', 'subtype': 'init', 'model': 'compose-fast'}))
        with patch.object(d, 'reconcile_workers'), patch.object(d, 'runtime_status', return_value={}):
            for reader in (d.task_report, d.status):
                stream = io.StringIO()
                with redirect_stdout(stream): reader(task_id, as_json=True)
                report = json.loads(stream.getvalue())
                self.assertEqual(report['model'], 'compose')
                self.assertEqual(report['observed_model'], 'compose-fast')
                self.assertEqual(report['execution'], first)
                self.assertEqual(report['execution_options'], {'fast': True, 'context': '200k'})
            stream = io.StringIO()
            with redirect_stdout(stream): d.list_tasks(as_json=True, cached=True)
            self.assertEqual(json.loads(stream.getvalue())[0]['execution'], first)

    def test_runtime_rejection_disabled_and_bad_catalog_never_admit_worker(self):
        data = self.root / 'state/tasks'
        registry = {'test': {'machine': 'work', 'workspace': str(self.root)}}
        with patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value=registry),                 patch.object(d.worker_runtime, 'admit') as admit, patch.object(d, 'spawn_runtime') as spawn:
            with patch.object(e, 'discover', return_value={'version': 'fixture', 'help': '--model <model>', 'models': []}):
                with self.assertRaisesRegex(ValueError, 'does not advertise'):
                    d.run_project('test', 'prompt', agent_override='cursor', execution_options={'fast': True})
            self.catalog['models'][0]['enabled'] = False; self.write_catalog()
            with self.assertRaisesRegex(ValueError, 'disabled'):
                d.run_project('test', 'prompt', agent_override='cursor')
            self.catalog_path.write_text('{broken')
            with self.assertRaisesRegex(ValueError, 'Invalid execution catalog'):
                d.run_project('test', 'prompt', agent_override='cursor')
            admit.assert_not_called(); spawn.assert_not_called()
            self.assertFalse(data.exists())

    def test_cli_forwards_execution_fields_and_read_only_catalog_command(self):
        argv = ['devctl', 'run-project', 'test', 'prompt', '--agent', 'auto', '--model', 'auto',
                '--execution-profile', 'quick', '--execution-options', '{"fast":false}']
        with patch.object(sys, 'argv', argv), patch.object(d, 'run_project', return_value=0) as run, self.assertRaises(SystemExit):
            d.main()
        self.assertEqual(run.call_args.kwargs, {'execution_profile': 'quick', 'execution_options': {'fast': False}})
        with patch.object(sys, 'argv', ['devctl', 'execution-catalog']),                 patch.object(d, 'execution_catalog', return_value=0) as inventory, self.assertRaises(SystemExit):
            d.main()
        inventory.assert_called_once_with(None, False, None)

    def test_request_signature_distinguishes_omitted_agent_from_explicit_default(self):
        data = self.root / 'state/tasks'
        cfg = {'machine': 'work', 'workspace': str(self.root), 'default_agent': 'codex', 'execution_profile': 'quick'}
        registry = {'test': cfg}; starts = []
        def start(machine, agent, workspace, prompt, model, effort, task_id, role, execution):
            meta = d.load_meta(task_id); meta.pop('launch_prepared', None); meta.pop('prepared_request', None)
            d.save_meta(d.task_dir(task_id), meta); starts.append(agent); return 0
        with (patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value=registry),
              patch.object(d, '_run_agent', side_effect=start), patch.object(e, 'discover', side_effect=self.capabilities),
              redirect_stdout(io.StringIO())):
            d.run_project('test', 'prompt', request_id='a' * 64)
            self.assertEqual(d.load_meta('a' * 24)['agent'], 'cursor')
            with self.assertRaisesRegex(ValueError, 'different task arguments'):
                d.run_project('test', 'prompt', agent_override='codex', request_id='a' * 64)
            self.assertEqual(starts, ['cursor'])
            # Project profile edits do not alter a receipt's requested inputs.
            cfg.pop('execution_profile'); self.catalog_path.write_text('{broken')
            d.run_project('test', 'prompt', request_id='a' * 64)
            self.assertEqual(starts, ['cursor'])
        # A fresh explicit default agent still wins over the project profile.
        cfg['execution_profile'] = 'quick'; self.write_catalog()
        with (patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value=registry),
              patch.object(d, '_run_agent', side_effect=start), patch.object(e, 'discover', side_effect=self.capabilities),
              patch.object(d, 'task_state', return_value=('DONE', 0)), redirect_stdout(io.StringIO())):
            d.run_project('test', 'prompt', agent_override='codex', request_id='b' * 64)
        self.assertEqual(starts, ['cursor', 'codex'])

    def test_preflight_rejects_review_mapping_and_runtime_without_worker_writes(self):
        data = self.root / 'state/tasks'
        cfg = {'machine': 'work', 'workspace': str(self.root), 'review_execution_profiles': {'codex': 'quick'}}
        with (patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value={'test': cfg}),
              patch.object(e, 'discover') as probe):
            result = d.preflight_task('start_task', {'project': 'test', 'agent': 'codex', 'role': 'review', 'request_id': 'a' * 64})
            self.assertEqual(result['status'], 'REJECTED')
            self.assertIn('quick', result['error']); self.assertIn('codex', result['error'])
            probe.assert_not_called(); self.assertFalse(data.parent.exists())
            probe.return_value = {'version': 'fixture', 'help': '--model <model>', 'models': []}
            result = d.preflight_task('start_task', {'project': 'test', 'agent': 'cursor', 'model': 'compose'})
            self.assertEqual(result['status'], 'REJECTED')
            self.assertIn('does not advertise', result['error'])
            self.assertFalse(data.parent.exists())

    def test_preflight_replays_existing_receipt_without_reapplying_catalog_policy(self):
        from durable import write_json
        data = self.root / 'state/tasks'
        write_json(data.parent / 'requests' / ('a' * 64 + '.json'), {'task_id': 'a' * 24, 'status': 'PENDING'})
        self.catalog_path.write_text('{broken')
        with patch.object(d, 'DATA_DIR', data), patch.object(e, 'discover') as probe:
            self.assertEqual(d.preflight_task('start_task', {'request_id': 'a' * 64}), {'status': 'REPLAY'})
        probe.assert_not_called()

    def test_legacy_receipt_replays_original_agent_intent_and_rejects_collision(self):
        from durable import write_json
        data = self.root / 'state/tasks'; request = 'a' * 64
        cfg = {'default_agent': 'codex', 'execution_profile': 'quick'}
        inputs = ['work', 'codex', str(self.root), 'prompt', None, None, 'coding']
        import hashlib
        signature = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
        write_json(data / request[:24] / 'meta.json', {'id': request[:24], 'execution': self.resolved(project=cfg)})
        write_json(data.parent / 'requests' / (request + '.json'), {'task_id': request[:24], 'signature': signature})
        with patch.object(d, 'DATA_DIR', data):
            self.assertEqual(d.start_signature(cfg, 'work', None, str(self.root), 'prompt', None, None,
                                               'coding', None, None, request), signature)
            with self.assertRaisesRegex(ValueError, 'different requested agent'):
                d.start_signature(cfg, 'work', 'codex', str(self.root), 'prompt', None, None,
                                  'coding', None, None, request)

    def test_preflight_bounds_whole_discovery_inside_control_budget(self):
        cfg = {'machine': 'work', 'workspace': str(self.root)}
        outputs = iter(['fixture', '--model <model>', 'compose - Compose'])
        def capture(machine, command, timeout):
            return subprocess.CompletedProcess([], 0, stdout=next(outputs))
        with (patch.object(d, 'DATA_DIR', self.root / 'state/tasks'),
              patch.object(d, 'load_projects', return_value={'test': cfg}),
              patch.object(d.time, 'monotonic', side_effect=[0, 1, 2, 4]),
              patch.object(d, 'capture_execution', side_effect=capture) as probe):
            result = d.preflight_task('start_task', {'project': 'test', 'agent': 'cursor'})
        self.assertEqual(result, {'status': 'UNAVAILABLE', 'launch_started': False})
        with self.assertRaises(e.RuntimeUnavailable):
            e.check_preflight(result)
        self.assertEqual([c.kwargs['timeout'] for c in probe.call_args_list], [2, 1])

    def test_preflight_does_not_claim_no_launch_for_orphaned_metadata(self):
        from durable import write_json
        data = self.root / 'state/tasks'; request = 'a' * 64
        write_json(data / request[:24] / 'meta.json', {'id': request[:24]})
        with patch.object(d, 'DATA_DIR', data), self.assertRaisesRegex(RuntimeError, 'ownership'):
            d.preflight_task('start_task', {'request_id': request})

    def test_rejection_requires_definitive_no_launch_proof(self):
        for result in ({'status': 'REJECTED', 'error': 'failure'},
                       {'status': 'REJECTED', 'launch_started': True, 'error': 'failure'},
                       {'status': 'REJECTED', 'launch_started': False, 'error': ''}):
            with self.subTest(result=result), self.assertRaises(RuntimeError): e.check_preflight(result)

    def test_invalid_worker_request_never_admits_or_spawns(self):
        data = self.root / 'state/tasks'
        registry = {'test': {'machine': 'work', 'workspace': str(self.root)}}
        with patch.object(d, 'DATA_DIR', data), patch.object(d, 'load_projects', return_value=registry), \
                patch.object(d.worker_runtime, 'admit') as admit, patch.object(d, 'spawn_runtime') as spawn:
            with self.assertRaises(ValueError):
                d.run_project('test', 'prompt', agent_override='cursor', execution_options={'made_up': True})
            admit.assert_not_called(); spawn.assert_not_called()
            self.assertFalse(data.exists())
