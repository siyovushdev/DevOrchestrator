"""Inert model-free worker fixture. Outer test boundary hides production/network."""
import json
import os
from pathlib import Path
import signal
import sys
import time
role,workspace,mode=sys.argv[1:]
repo=Path(workspace)
if mode=='hang':
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    if os.fork()==0:
        os.setsid()
        while True:time.sleep(.05)
    while True:time.sleep(.05)
if mode=='crash':raise SystemExit(7)
if role=='coding':
    (repo/'port_parser.py').write_text('''def parse_port(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 5 or value[0] == "0" or any(c < "0" or c > "9" for c in value):
        raise ValueError("Invalid ASCII port")
    result = int(value)
    if result > 65535:
        raise ValueError("Port out of range")
    return result
''')
if mode=='oversize':
    (repo/'.gitignore').write_text('ignored-output\n')
    with (repo/'ignored-output').open('wb') as output:output.truncate(1<<40)
sys.path.insert(0,str(repo));from port_parser import parse_port
assert parse_port('1')==1 and parse_port('65535')==65535
for value in (None,1,True,'','0','01','+1',' 1','1 ','١','６','65536','999999'):
    try:parse_port(value)
    except ValueError:pass
    else:raise AssertionError('Invalid port accepted')
payload=(dict(state='READY_FOR_REVIEW',blocker=None,implementation='DONE',verification='PASS',self_review='PASS',summary='Inert fixture checks PASS')
    if role=='coding' else dict(verdict='PASS',blocker=None,summary='Independent immutable fixture checks PASS',findings=[]))
text=('DEVORCHESTRATOR_RESULT: ' if role=='coding' else 'DEVORCHESTRATOR_REVIEW: ')+json.dumps(payload)
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':text}}),flush=True)
