"""Disposable CLI adapter: real argument parsing/admission, inert role executable."""
import subprocess
import sys
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parents[1])]
import admission_policy as a
import devctl as d
import execution_profiles
import native_codex
d.DATA_DIR=Path(a.policy()[1]['state_dir'])/'tasks'
execution_profiles.discover=lambda *args,**kwargs:{'version':'fixture','help':'--model --config'}
def command(cfg,role,repo,prompt,private):
    return [cfg['binary'],'-I','-B',str(Path(__file__).with_name('admission_dummy.py')),role,str(repo),'normal']
native_codex.build_command=command
try:d.main()
except execution_profiles.PreflightRejected as exc:
    import json
    print(json.dumps(execution_profiles.rejection(exc)));raise SystemExit(2)
