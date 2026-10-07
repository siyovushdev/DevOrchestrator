"""Shared doctor/MCP health contract and timeout budget."""

PROBE_TIMEOUT = 5
DOCTOR_TIMEOUT = 20
MCP_HEALTH_TIMEOUT = 30
SSH_CONNECT_TIMEOUT = 3


def health_result(machines):
    work = machines.get('work', {})
    local_ok = (work.get('type') == 'local' and work.get('status') == 'OK' and
                all(item['status'] == 'OK' for item in machines.values()
                    if item['type'] == 'local'))
    remote_ok = all(item['status'] == 'AVAILABLE' for item in machines.values()
                    if item['type'] == 'ssh')
    return {
        'ok': local_ok,
        'state': 'FAILED' if not local_ok else 'READY' if remote_ok else 'DEGRADED',
        'local_control_plane': 'OK' if local_ok else 'FAILED',
        'machines': machines,
    }


def failed_health(diagnostic):
    """Failure to verify the local doctor must never imply readiness."""
    return health_result({'work': {
        'type': 'local', 'required': True, 'status': 'FAILED',
        'diagnostic': diagnostic, 'details': '',
    }})


def validate_health(report):
    """Reject malformed or inconsistent subprocess reports at the MCP boundary."""
    if not isinstance(report, dict) or not isinstance(report.get('machines'), dict):
        raise ValueError('invalid doctor health object')
    machines = report['machines']
    if not isinstance(machines.get('work'), dict) or machines['work'].get('type') != 'local':
        raise ValueError('doctor health lacks local WORK')
    for name, item in machines.items():
        if not isinstance(name, str) or not isinstance(item, dict):
            raise ValueError('invalid machine health record')
        kind = item.get('type')
        if kind not in ('local', 'ssh') or item.get('required') is not (kind == 'local'):
            raise ValueError('invalid machine health role')
        statuses = ('OK', 'FAILED') if kind == 'local' else ('AVAILABLE', 'UNAVAILABLE')
        if item.get('status') not in statuses or not isinstance(item.get('details'), str):
            raise ValueError('invalid machine health status')
        diagnostic = item.get('diagnostic')
        if diagnostic is not None and not isinstance(diagnostic, str):
            raise ValueError('invalid machine health diagnostic')
        if item['status'] in ('FAILED', 'UNAVAILABLE') and not diagnostic:
            raise ValueError('missing machine failure diagnostic')
    expected = health_result(machines)
    if (type(report.get('ok')) is not bool or
            any(report.get(key) != expected[key] for key in ('ok', 'state', 'local_control_plane'))):
        raise ValueError('inconsistent doctor health summary')
    return report
