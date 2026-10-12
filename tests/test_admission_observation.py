"""Real-enforcement offline probes for untrusted coding output preparation."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import admission_policy as a
import devctl as d
import native_codex
from publication import Repository
import repository_safety as safe
import scheduler
from test_admission_policy import PolicyFixture

class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.f=PolicyFixture(Path(self.tmp.name));self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ,self.f.env()))
        self.stack.enter_context(patch.object(d,'DATA_DIR',self.f.root/'state/tasks'))
    def test_ambient_global_system_and_environment_filters_never_execute(self):
        marker=self.f.root/'filter-executed';evil=self.f.root/'evil-config'
        command='touch '+str(marker)+'; cat'
        evil.write_text('[filter "evil"]\n clean = '+command+'\n[core]\n fsmonitor = '+str(marker)+'\n')
        (self.f.repo/'.gitattributes').write_text('* filter=evil\n')
        (self.f.repo/'changed.txt').write_text('unfiltered source\n')
        for ambient in ({'GIT_CONFIG_GLOBAL':str(evil)}, {'GIT_CONFIG_SYSTEM':str(evil),'GIT_CONFIG_NOSYSTEM':'0'},
                        {'GIT_CONFIG_COUNT':'1','GIT_CONFIG_KEY_0':'filter.evil.clean','GIT_CONFIG_VALUE_0':command}):
            with self.subTest(ambient=list(ambient)),patch.dict(os.environ,ambient),a.execution(self.f.args):
                baseline=d.capture_git_baseline('work',str(self.f.repo))
                snap=Repository('test').snapshot();evidence=Repository('test').review_evidence(snap)
                self.assertEqual(snap['fingerprint'],baseline['snapshot_fingerprint'])
                self.assertEqual(scheduler.project_identity('test')[1],self.f.binding['repository'])
                self.assertIn('unfiltered source',evidence['patch'])
                self.assertFalse(marker.exists())
    def test_local_executable_filters_denied_by_every_observation(self):
        marker=self.f.root/'local-filter';(self.f.repo/'.gitattributes').write_text('* filter=evil\n')
        subprocess.check_call(['git','-C',str(self.f.repo),'config','filter.evil.clean','touch '+str(marker)+'; cat'])
        for call in (lambda:d.capture_git_baseline('work',str(self.f.repo)),lambda:Repository('test').snapshot(),lambda:scheduler.project_identity('test')):
            with self.assertRaises(RuntimeError):call()
        self.assertFalse(marker.exists())
    def test_sparse_ignored_oversize_rejected_before_hash_copy_git(self):
        (self.f.repo/'.gitignore').write_text('oversize\n')
        with (self.f.repo/'oversize').open('wb') as stream:stream.truncate(1<<40)
        target=self.f.root/'rejected-snapshot'
        with patch.object(safe,'stream',side_effect=AssertionError('Must reject before reading')):
            with self.assertRaises(ValueError):native_codex.snapshot(self.f.repo,target)
        self.assertFalse(target.exists())
        with self.assertRaises((a.AdmissionDenied,ValueError)):a.validate(self.f.args)
        with self.assertRaises(RuntimeError):Repository('test').snapshot()
    def test_aggregate_size_bound_before_copy(self):
        for i in range(5):
            with (self.f.repo/('large'+str(i))).open('wb') as stream:stream.truncate(safe.MAX_FILE)
        with self.assertRaises(ValueError):native_codex.snapshot(self.f.repo,self.f.root/'copy')
        self.assertFalse((self.f.root/'copy').exists())
    def test_entry_count_limit_before_copy(self):
        with patch.object(safe,'MAX_ENTRIES',10),self.assertRaises(ValueError):native_codex.snapshot(self.f.repo,self.f.root/'copy')
        self.assertFalse((self.f.root/'copy').exists())
    def test_preparation_expired_deadline_no_allocation(self):
        with self.assertRaisesRegex(ValueError,'deadline'):native_codex.snapshot(self.f.repo,self.f.root/'copy',until=safe.clock()-1)
        self.assertFalse((self.f.root/'copy').exists())
    def test_snapshot_hashing_streams_and_result_is_immutable(self):
        (self.f.repo/'large').write_bytes(b'x'*(2*safe.CHUNK+1))
        with patch.object(Path,'read_bytes',side_effect=AssertionError('Unbounded read forbidden')):
            files=native_codex.snapshot(self.f.repo,self.f.root/'copy',until=safe.clock()+5)
        self.assertIn('large',files);self.assertEqual((self.f.root/'copy/large').stat().st_mode&0o777,0o444)
        self.assertEqual(safe.manifest(self.f.root/'copy',safe.clock()+5),files)
    def test_reserved_preparation_inherits_work_deadline(self):
        with a.execution(self.f.args):
            a.reserve(d.DATA_DIR);path=d.DATA_DIR.parent/'admissions'/self.f.grant['workflow_id']/'claim.json'
            value=a.strict(path);value['work_deadline']=safe.clock()+.5;__import__('durable').write_json(path,value)
            self.assertLessEqual(safe.until(),value['work_deadline'])
            value['work_deadline']=safe.clock()-1;__import__('durable').write_json(path,value)
            with self.assertRaisesRegex(ValueError,'deadline'):safe.until()

    def test_real_git_probe_timeout_uses_pinned_child_fd_and_reaps(self):
        import signal,time,sys
        scope={};exec(safe.GIT_PREAMBLE,scope);scope['_git_end']=safe.clock()+.1
        popen=subprocess.Popen;children=[]
        def inert_child(argv,**kwargs):
            child=popen([sys.executable,'-c','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)'],
                **kwargs,start_new_session=True);children.append(child);return child
        start=time.monotonic()
        with patch.object(subprocess,'Popen',side_effect=inert_child),patch.object(signal,'pidfd_send_signal',wraps=signal.pidfd_send_signal) as send:
            with self.assertRaisesRegex(ValueError,'timed out'):scope['safe_git'](str(self.f.repo),['status'])
        self.assertLess(time.monotonic()-start,3)
        self.assertEqual(len(children),1);self.assertIsNotNone(children[0].returncode)
        self.assertEqual([call.args[1] for call in send.call_args_list],[signal.SIGTERM,signal.SIGKILL])

    def test_non_git_legacy_identity_is_observational_but_admission_denies(self):
        (self.f.repo/'.git').rename(self.f.repo/'saved-git')
        self.assertEqual(scheduler.project_identity('test')[1],self.f.binding['repository'])
        with self.assertRaises(a.AdmissionDenied):a.validate(self.f.args)

    def config_args(self,role='coding'):
        from native_permissions import overrides
        private=self.f.root/'runtime'/role;private.mkdir(mode=0o700)
        for leaf in ('home','tmp'):(private/leaf).mkdir(mode=0o700)
        cfg=__import__('durable').read_json(self.f.native)
        argv=[cfg['binary'],'exec','--json','--color','never','--ephemeral','--ignore-user-config','--ignore-rules','-m','gpt-6.1-sol','-C',str(self.f.repo)]
        for value in overrides(role,self.f.repo,cfg['binary'],private/'home',private/'tmp'):argv+=['-c',value]
        argv.append('INERT CONFIGURATION CHECK ONLY')
        return cfg,private,argv
    def test_real_command_construction_clamps_both_role_configuration_deadlines(self):
        import native_configuration
        from native_deadlines import initialize
        for role in ('coding','review'):
            cfg,private,argv=self.config_args(role)
            if not (private.parent/'budget.json').exists():initialize(private.parent)
            end=safe.clock()+12
            with a.execution(self.f.args),a.validation_budget(end),patch.object(native_configuration,'validate_with_cli',return_value={}) as check:
                native_codex.build_command(cfg,role,self.f.repo,'INERT',private)
            self.assertLessEqual(check.call_args.kwargs['until'],end)
    def test_configuration_watchdog_real_inert_timeout_reserves_cleanup(self):
        import native_configuration,native_process,time
        cfg,private,argv=self.config_args();actual=native_process.run;calls=[]
        def inert(command,folder,**kw):
            calls.append(kw['deadline'])
            return actual(['/usr/bin/python3','-I','-B','-c','import time;time.sleep(30)'],folder,**kw)
        end=safe.clock()+10.25;start=time.monotonic()
        with patch.object(native_configuration.bp,'run',side_effect=inert),self.assertRaisesRegex(RuntimeError,'timeout/unknown'):
            native_configuration.validate_with_cli(argv,'coding',private/'configuration',until=end,private=private)
        self.assertEqual(len(calls),1);self.assertLessEqual(calls[0],end-10)
        self.assertLess(time.monotonic()-start,2)
        result=__import__('durable').read_json(private/'configuration/result.json')
        self.assertTrue(result['watchdog']['cleanup_proven']);self.assertEqual(result['watchdog']['state'],'TIMEOUT')
    def test_exhausted_configuration_budget_never_creates_a_probe(self):
        import native_configuration
        cfg,private,argv=self.config_args()
        with patch.object(native_configuration.bp,'run',side_effect=AssertionError('No probe')) as run,self.assertRaises(ValueError):
            native_configuration.validate_with_cli(argv,'coding',private/'configuration',until=safe.clock()+.1,private=private)
        run.assert_not_called()
