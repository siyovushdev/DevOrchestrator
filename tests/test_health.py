"""Deterministic health contracts; no live services, workers or registry access."""
from contextlib import redirect_stdout
import copy
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import devctl as d
import health
from task_bridge import decode_mcp_result


class HealthTests(unittest.TestCase):
    def setUp(self):
        class Server:
            def __init__(self, name): pass
            def tool(self): return lambda fn: fn
        spec = importlib.util.spec_from_file_location(
            'isolated_health_adapter', Path(__file__).resolve().parents[1] / 'mcp_server.py')
        self.adapter = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'mcp': SimpleNamespace(),
                                     'mcp.server': SimpleNamespace(MCPServer=Server)}):
            spec.loader.exec_module(self.adapter)

    def probe(self, failed=None, timed_out=None):
        def run(machine, command, timeout):
            if machine == timed_out:
                raise subprocess.TimeoutExpired('fixture probe', timeout)
            code = 255 if machine == failed else 0
            output = 'Connection refused' if code else 'HOST=fixture\nUSER=fixture\n'
            return subprocess.CompletedProcess([], code, output)
        return patch.object(d, 'doctor_probe', side_effect=run)

    def cli(self, as_json=True):
        argv = ['devctl', 'doctor'] + (['--json'] if as_json else [])
        with patch.object(sys, 'argv', argv), redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit) as exit_result:
                d.main()
        return subprocess.CompletedProcess(argv, exit_result.exception.code, output.getvalue())

    def test_cli_and_mcp_structured_health_for_all_states(self):
        cases = ((None, None, 'READY', 0, 'OK', 'AVAILABLE'),
                 ('firefly', None, 'DEGRADED', 0, 'OK', 'UNAVAILABLE'),
                 (None, 'firefly', 'DEGRADED', 0, 'OK', 'UNAVAILABLE'),
                 ('work', 'firefly', 'FAILED', 1, 'FAILED', 'UNAVAILABLE'),
                 ('work', None, 'FAILED', 1, 'FAILED', 'AVAILABLE'),
                 (None, 'work', 'FAILED', 1, 'FAILED', 'AVAILABLE'))
        for failed, timed_out, state, code, local, remote in cases:
            with self.subTest(failed=failed, timed_out=timed_out), self.probe(failed, timed_out) as probe:
                cli = self.cli()
                report = health.validate_health(json.loads(cli.stdout))
                self.assertEqual(cli.returncode, code)
                self.assertEqual(report['state'], state)
                self.assertEqual(report['ok'], code == 0)
                self.assertEqual(report['local_control_plane'], local)
                self.assertEqual(report['machines']['firefly']['status'], remote)
                self.assertEqual([call.args[0] for call in probe.call_args_list], ['work', 'firefly'])
                if remote == 'UNAVAILABLE':
                    self.assertTrue(report['machines']['firefly']['diagnostic'])
                # Execute the real CLI dispatch/formatter behind the MCP subprocess boundary.
                with patch.object(self.adapter, 'devctl_result', side_effect=lambda *args, **kw: self.cli()) as run:
                    result = self.adapter.orchestrator_health()
                run.assert_called_once_with('doctor', '--json', timeout=health.MCP_HEALTH_TIMEOUT)
                self.assertEqual(result, report)
                wire = SimpleNamespace(content=[SimpleNamespace(type='text', text=json.dumps(result))], is_error=False)
                self.assertEqual(decode_mcp_result('orchestrator_health', wire), report)

    def test_text_doctor_keeps_remote_outage_visible_and_succeeds(self):
        with self.probe(timed_out='firefly'):
            result = self.cli(as_json=False)
        self.assertEqual(result.returncode, 0)
        self.assertIn('STATE=DEGRADED LOCAL_CONTROL_PLANE=OK', result.stdout)
        self.assertIn('firefly: UNAVAILABLE', result.stdout)
        self.assertIn('SSH probe timed out after 5s', result.stdout)

    def test_probe_start_failures_are_isolated(self):
        for machine, state in (('firefly', 'DEGRADED'), ('work', 'FAILED')):
            def run(name, command, timeout):
                if name == machine: raise FileNotFoundError('fixture executable missing')
                return subprocess.CompletedProcess([], 0, 'healthy')
            with self.subTest(machine=machine), patch.object(d, 'doctor_probe', side_effect=run):
                result = d.doctor_health()
            self.assertEqual(result['state'], state)
            self.assertIn('could not start', result['machines'][machine]['diagnostic'])
            self.assertEqual(len(result['machines']), 2)

    def test_remote_timeout_and_total_budget_fit_mcp_timeout(self):
        with self.probe(timed_out='firefly') as probe:
            result = d.doctor_health()
        self.assertEqual(result['state'], 'DEGRADED')
        self.assertTrue(all(0 < call.args[2] <= health.PROBE_TIMEOUT for call in probe.call_args_list))
        self.assertLessEqual(sum(call.args[2] for call in probe.call_args_list), health.DOCTOR_TIMEOUT)
        self.assertLess(health.DOCTOR_TIMEOUT, health.MCP_HEALTH_TIMEOUT)
        # Remaining budget, rather than machine count, caps future additional probes.
        with self.probe(timed_out='firefly') as probe, patch.object(d.time, 'monotonic', side_effect=[0, 0, 19]):
            result = d.doctor_health()
        self.assertEqual(probe.call_args_list[1].args[2], 1)
        self.assertIn('after 1s', result['machines']['firefly']['diagnostic'])
        with self.probe() as probe, patch.object(d.time, 'monotonic', side_effect=[0, 0, 21]):
            result = d.doctor_health()
        self.assertEqual(probe.call_count, 1)
        self.assertEqual(result['state'], 'DEGRADED')
        self.assertIn('not probed', result['machines']['firefly']['diagnostic'])

    def test_ssh_options_and_timeout_cleanup_kill_probe_group(self):
        process = Mock(pid=12345, args=['fixture'], returncode=-9)
        process.communicate.side_effect = [subprocess.TimeoutExpired('fixture', 5), ('', None)]
        with patch.object(d.subprocess, 'Popen', return_value=process) as popen, patch.object(d.os, 'killpg') as kill:
            with self.assertRaises(subprocess.TimeoutExpired): d.doctor_probe('firefly', 'true', health.PROBE_TIMEOUT)
        argv = popen.call_args.args[0]
        for option in ('BatchMode=yes', 'ConnectTimeout=3', 'ConnectionAttempts=1',
                       'ServerAliveInterval=1', 'ServerAliveCountMax=2'):
            self.assertIn(option, argv)
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        self.assertEqual(popen.call_args.kwargs['stdin'], subprocess.DEVNULL)
        self.assertEqual(process.communicate.call_args_list[0].kwargs['timeout'], health.PROBE_TIMEOUT)
        kill.assert_called_once_with(12345, signal.SIGKILL)
        self.assertEqual(process.communicate.call_count, 2)

    def test_real_dummy_probe_timeout_cleans_up_child_process(self):
        # The dummy child inherits stdout: parent-only termination would hang cleanup.
        command = [sys.executable, '-c',
                   'import subprocess, sys; subprocess.Popen([sys.executable, "-c", '
                   '"import time; time.sleep(60)"]); import time; time.sleep(60)']
        with patch.object(d, 'machine_command', return_value=command):
            with self.assertRaises(subprocess.TimeoutExpired): d.doctor_probe('firefly', 'fixture', 0.2)

    def test_local_identity_probe_failure_is_fatal(self):
        # Exercise the actual shell script, with deterministic replacements for identity tools.
        with patch.object(d, 'MACHINES', {'work': {'type': 'local'}}):
            original = d.machine_command
            def command(machine, text, **kwargs):
                return original(machine, 'hostname() { return 1; }; ' + text, **kwargs)
            with patch.object(d, 'machine_command', side_effect=command):
                result = self.cli()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout)['state'], 'FAILED')

    def test_missing_work_cannot_be_ready(self):
        with patch.object(d, 'MACHINES', {'firefly': d.MACHINES['firefly']}), self.probe():
            result = self.cli()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout)['state'], 'FAILED')

    def test_mcp_doctor_execution_failure_is_structured_and_fail_closed(self):
        for error in (subprocess.TimeoutExpired('doctor', 30), FileNotFoundError('doctor missing')):
            with self.subTest(error=type(error)), patch.object(self.adapter, 'devctl_result', side_effect=error):
                result = health.validate_health(self.adapter.orchestrator_health())
            self.assertFalse(result['ok'])
            self.assertEqual(result['state'], 'FAILED')
            self.assertTrue(result['machines']['work']['diagnostic'])

    def test_mcp_malformed_or_inconsistent_reports_fail_closed(self):
        with self.probe(): valid = d.doctor_health()
        inconsistent = copy.deepcopy(valid); inconsistent['state'] = 'DEGRADED'
        missing_work = copy.deepcopy(valid); del missing_work['machines']['work']
        invalid_role = copy.deepcopy(valid); invalid_role['machines']['work']['required'] = False
        no_diagnostic = copy.deepcopy(valid)
        no_diagnostic['machines']['firefly']['status'] = 'UNAVAILABLE'
        for payload, code in (('not json', 0), ('[]', 0), (json.dumps(inconsistent), 0),
                              (json.dumps(missing_work), 0), (json.dumps(invalid_role), 0),
                              (json.dumps(no_diagnostic), 0), (json.dumps(valid), 1)):
            with self.subTest(payload=payload, code=code), patch.object(self.adapter, 'devctl_result',
                    return_value=subprocess.CompletedProcess([], code, payload)):
                result = health.validate_health(self.adapter.orchestrator_health())
            self.assertEqual(result['state'], 'FAILED')
            self.assertFalse(result['ok'])
