"""Finite boot-bound execution budget. Restart never resets an admitted budget."""
import os
from pathlib import Path
import time
from durable import read_json, write_json

LIMITS = dict(coding=480, review=480, contract=30, validation=60,
              overall=1200, term_grace=5, kill_grace=5, close=5)

def boot():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()

def clock():
    return time.clock_gettime(time.CLOCK_BOOTTIME)

def initialize(run):
    path=Path(run)/'budget.json'
    if path.exists():raise ValueError('Budget already exists; no restart/relaunch')
    value={'boot':boot(),'started':clock(),'limits':LIMITS,
           'automatic_retry':False,'state':'ADMITTED'}
    # Reserve cleanup time inside the overall hard budget, not after it.
    value['work_deadline']=value['started']+LIMITS['overall']-LIMITS['term_grace']-LIMITS['kill_grace']-2
    write_json(path,value)
    return value

def deadline(run,kind):
    value=read_json(Path(run)/'budget.json')
    if not value or value['boot']!=boot() or value['limits']!=LIMITS:
        raise ValueError('UNKNOWN: missing/changed budget or boot; no admission')
    end=min(value['work_deadline'],clock()+LIMITS[kind])
    if end<=clock():raise ValueError('Expired durable workflow budget; no admission')
    return end

def stop(run,reason):
    run=Path(run)
    write_json(run/'STOP_ADMISSION',{'reason':reason,'automatic_retry':False})

def timeout_records(run):
    return [read_json(p) for p in Path(run).rglob('bounded-result.json')
            if read_json(p).get('state')!='EXITED']

def fence_unknown(run):
    if timeout_records(run):
        stop(run,'Timeout/ownership diagnostic requires owner forward repair')
        raise ValueError('UNKNOWN/TIMEOUT: durable timeout fence; no native reconciliation or retry')
