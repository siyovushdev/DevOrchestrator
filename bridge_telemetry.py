"""Bounded display telemetry, independent of authoritative workflow recovery.

Only the known high-frequency liveness fields are removed from durable state.
Classifications, transition receipts and all unknown fields remain durable. The
worker's own liveness.json remains the authority for leases and fresh probes.
This sidecar is optional: recovery never reads it and status never trusts it to
change a classification or authorize an action.
"""
from copy import deepcopy
import hashlib
import json
import math
import time

from bridge_timing import LIMIT, NAMES
from durable import read_json, write_json_if_changed

VOLATILE_HEALTH = frozenset({'observed_at', 'heartbeat_age_seconds',
    'last_heartbeat_at', 'heartbeat_sequence', 'lease_expires_at', 'lease_state'})
SAMPLE_FIELDS = ('observed_at', 'last_heartbeat_at', 'heartbeat_sequence', 'lease_expires_at')
RUNTIME_LIMIT = 128  # Latest coding/review observations, not an event history.
MAX_BYTES = 128 * 1024


def semantic_health(health):
    return {key: value for key, value in health.items() if key not in VOLATILE_HEALTH}


def durable_state(state):
    """Copy only containers we edit; never mutate the live workflow object."""
    result = {key: value for key, value in state.items() if key != 'timings'}
    result['issues'] = dict(state['issues'])
    for number, item in state['issues'].items():
        if not isinstance(item, dict) or not isinstance(item.get('runtime_health'), dict):
            continue
        result['issues'][number] = dict(item, runtime_health={
            slot: semantic_health(health) if isinstance(health, dict) else health
            for slot, health in item['runtime_health'].items()})
    return result


def _number(value):
    return (type(value) in (int, float) and
            (not isinstance(value, int) or value.bit_length() <= 63) and
            math.isfinite(value) and value >= 0)


def timing_samples(timings):
    samples = timings.get('samples', []) if isinstance(timings, dict) else []
    if not isinstance(samples, list):
        return []
    return [{'stage': row['stage'], 'seconds': row['seconds']} for row in samples[-LIMIT:]
            if isinstance(row, dict) and isinstance(row.get('stage'), str) and
            row['stage'] in NAMES and _number(row.get('seconds'))]


def _identity(item, slot, health):
    task = item.get('task_id' if slot == 'coding' else 'review_task_id')
    return hashlib.sha256(json.dumps([task, semantic_health(health)],
                                    sort_keys=True).encode()).hexdigest()


def snapshot(state):
    observations = []
    for number, item in state['issues'].items():
        if not isinstance(number, str) or not number.isdigit() or len(number) > 20 or not isinstance(item, dict):
            continue
        healths = item.get('runtime_health')
        if not isinstance(healths, dict):
            continue
        for slot in ('coding', 'review'):
            health = healths.get(slot)
            if not isinstance(health, dict):
                continue
            values = {key: health[key] for key in SAMPLE_FIELDS if _number(health.get(key))}
            if values:
                observations.append(dict(issue=number, slot=slot,
                    identity=_identity(item, slot, health), values=values))
    observations.sort(key=lambda row: (row['values'].get('observed_at', 0), row['issue'], row['slot']))
    return dict(schema_version=1, timings={'samples': timing_samples(state.get('timings'))},
                runtime=observations[-RUNTIME_LIMIT:])


def telemetry_path(state_file):
    return state_file.with_name('task_bridge_telemetry.json')


def save(state_file, state):
    # Persist workflow first. Telemetry IO failure cannot interrupt a completed
    # durable transition or cause an RPC/publication operation to be replayed.
    try:
        write_json_if_changed(telemetry_path(state_file), snapshot(state))
    except OSError:
        pass


def load(state_file):
    path = telemetry_path(state_file)
    try:
        if path.stat().st_size > MAX_BYTES:
            return {}
        value = read_json(path)
        return value if isinstance(value, dict) and value.get('schema_version') == 1 else {}
    except (OSError, ValueError):
        return {}


def runtime_view(item, number, slot, telemetry=None, *, clock=None):
    healths = item.get('runtime_health') or {}
    health = deepcopy(healths.get(slot) or {'state': 'UNKNOWN'})
    rows = (telemetry or {}).get('runtime', [])
    if isinstance(rows, list):
        for row in rows[-RUNTIME_LIMIT:]:
            if (isinstance(row, dict) and row.get('issue') == str(number) and row.get('slot') == slot
                    and row.get('identity') == _identity(item, slot, health)
                    and isinstance(row.get('values'), dict)):
                health.update({key: row['values'][key] for key in SAMPLE_FIELDS
                               if _number(row['values'].get(key))})
    # Derived display values are never stored in either file. A cached sample
    # does not refresh observed_at or reclassify the runtime as healthy.
    now = time.time() if clock is None else clock
    heartbeat, expiry = health.get('last_heartbeat_at'), health.get('lease_expires_at')
    health['heartbeat_age_seconds'] = max(0, now - heartbeat) if _number(heartbeat) else None
    health['lease_state'] = ('UNKNOWN' if not _number(heartbeat) or not _number(expiry) or now < heartbeat
                             else 'FRESH' if now < expiry else 'EXPIRED')
    return health
