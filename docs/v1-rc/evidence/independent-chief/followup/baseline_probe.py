import json
import test_worker_integration as t
import devctl as d
case=t.WorkerIntegrationTests('test_git_baseline_fingerprint_detects_dirty_content_change')
case.setUp()
original=d.capture
observations=[]
def capture(machine,command,**kwargs):
 result=original(machine,command,**kwargs)
 if 'awk' in command: observations.append(dict(command=command,returncode=result.returncode,stdout=result.stdout,stderr=result.stderr))
 return result
d.capture=capture
try:
 result=d.capture_git_baseline('work',str(case.repo))
 print(json.dumps(dict(index=result['index_fingerprint'],snapshot=result['snapshot_fingerprint'],observations=observations)))
finally: case.doCleanups()
