"""Versioned reviewer-launch checkpoints and pure transition decisions.

This module performs no I/O and never mutates its inputs. Only the bridge writer
may install returned records. PREPARE observations are disposable; LAUNCH and
later phases must retain the exact durable pending RPC until settled.
"""
from copy import deepcopy
import hashlib
import json
import re


VERSION = 1
PHASES = frozenset({'PREPARE', 'LAUNCH', 'VERIFY', 'DRAIN', 'HELD'})
AUTHORIZED_PHASES = frozenset({'LAUNCH', 'VERIFY', 'DRAIN'})


class InvalidTransition(ValueError):
    """Unverifiable durable state must be held, never guessed or reset."""


class SourceChanged(RuntimeError):
    """A successful observation proved a changed review source."""


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def request_id(namespace, number, operation):
    # Preserve the existing bridge algorithm, including its exact separators.
    return hashlib.sha256(f'{namespace}:{number}:start_task:{operation}'.encode()).hexdigest()


def _hash(value):
    return isinstance(value, str) and re.fullmatch(r'[a-f0-9]{64}', value) is not None


def _integer(value):
    return type(value) is int and value > 0


def source_identity(report):
    """Exclude polling/heartbeat age, timings and other transient observations."""
    return {key: deepcopy(report.get(key)) for key in (
        'id', 'turns', 'status', 'exit_code', 'agent', 'machine', 'workspace',
        'git_baseline', 'result', 'runtime_generation')}


def validate_launch_binding(binding):
    fields = {'schema_version', 'source_task_id', 'source_turn', 'source_generation',
              'machine', 'repository', 'registry_digest', 'catalog_digest', 'snapshot'}
    if (not isinstance(binding, dict) or set(binding) != fields or
            type(binding['schema_version']) is not int or binding['schema_version'] != VERSION or
            not isinstance(binding['source_task_id'], str) or
            re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', binding['source_task_id']) is None or
            '..' in binding['source_task_id'] or not _integer(binding['source_turn']) or
            not isinstance(binding['source_generation'], str) or not binding['source_generation'] or
            not isinstance(binding['machine'], str) or not binding['machine'] or
            any(not _hash(binding[key]) for key in ('repository', 'registry_digest', 'catalog_digest')) or
            not isinstance(binding['snapshot'], dict) or
            not _hash(binding['snapshot'].get('fingerprint'))):
        raise InvalidTransition('Invalid worker review source binding')
    return binding


def begin(namespace, number, item, report, inputs):
    """Allocate a logical attempt once. The caller durably retains its sequence."""
    number = str(number)
    if (not number.isdigit() or not namespace or item.get('pending_rpc') or
            holds_repository(item) or report.get('id') != item.get('task_id') or
            not _integer(report.get('turns')) or report['turns'] != item.get('coding_turn') or
            report.get('status') != 'DONE' or type(report.get('exit_code')) is not int or
            report['exit_code'] != 0):
        raise InvalidTransition('Review source is not eligible for a new launch')
    counters = [item.get('review_round', 0), item.get('review_launch_sequence', 0)]
    if any(type(value) is not int or value < 0 for value in counters):
        raise InvalidTransition('Invalid review sequence')
    sequence = max(counters) + 1
    operation = f'review:{sequence}'
    return dict(schema_version=VERSION, namespace=namespace, issue=number,
                sequence=sequence, operation=operation,
                request_id=request_id(namespace, number, operation), phase='PREPARE',
                invalidated=False, source=dict(task_id=report['id'], turn=report['turns'],
                    runtime_generation=report.get('runtime_generation'),
                    report_digest=digest(source_identity(report)), input_digest=digest(inputs)),
                binding=None, request_digest=None, accepted_task=None)


def validate(record, pending=None):
    """Validate persisted protocol state without repairing or advancing it."""
    fields = {'schema_version', 'namespace', 'issue', 'sequence', 'operation',
              'request_id', 'phase', 'invalidated', 'source', 'binding',
              'request_digest', 'accepted_task'}
    if not isinstance(record, dict) or set(record) != fields:
        raise InvalidTransition('Malformed review transition')
    if (type(record['schema_version']) is not int or record['schema_version'] != VERSION or
            not isinstance(record['phase'], str) or record['phase'] not in PHASES or
            type(record['invalidated']) is not bool or
            not isinstance(record['namespace'], str) or not record['namespace'] or
            not isinstance(record['issue'], str) or not record['issue'].isdigit() or
            not _integer(record['sequence']) or record['operation'] != f'review:{record["sequence"]}' or
            record['request_id'] != request_id(record['namespace'], record['issue'], record['operation'])):
        raise InvalidTransition('Unsupported or inconsistent review transition identity')
    source = record['source']
    if (not isinstance(source, dict) or set(source) != {
            'task_id', 'turn', 'runtime_generation', 'report_digest', 'input_digest'} or
            not isinstance(source['task_id'], str) or not source['task_id'] or
            not _integer(source['turn']) or not _hash(source['report_digest']) or
            not _hash(source['input_digest']) or
            (source['runtime_generation'] is not None and
             (not isinstance(source['runtime_generation'], str) or not source['runtime_generation']))):
        raise InvalidTransition('Invalid review source binding')
    authorized = record['request_digest'] is not None
    if record['phase'] in AUTHORIZED_PHASES and not authorized:
        raise InvalidTransition('Review phase lacks durable launch authority')
    if record['phase'] == 'PREPARE' and (authorized or pending is not None or record['accepted_task'] is not None):
        raise InvalidTransition('Preparation unexpectedly contains launch authority')
    if authorized:
        binding = record['binding']
        if (not _hash(record['request_digest']) or not isinstance(binding, dict) or
                set(binding) != {'machine', 'repository', 'registry_digest', 'snapshot'} or
                not isinstance(binding['machine'], str) or not binding['machine'] or
                not _hash(binding['repository']) or not _hash(binding['registry_digest']) or
                not isinstance(binding['snapshot'], dict) or
                not _hash(binding['snapshot'].get('fingerprint'))):
            raise InvalidTransition('Invalid review repository binding')
        if (not isinstance(pending, dict) or set(pending) != {'name', 'arguments', 'operation'} or
                pending['name'] != 'start_task' or pending['operation'] != record['operation'] or
                not isinstance(pending['arguments'], dict) or
                pending['arguments'].get('role') != 'review' or
                pending['arguments'].get('request_id') != record['request_id'] or
                digest(pending) != record['request_digest']):
            raise InvalidTransition('Pending review request identity changed')
    elif record['binding'] is not None or pending is not None or record['accepted_task'] is not None:
        raise InvalidTransition('Unauthorised review record contains launch evidence')
    accepted = record['accepted_task']
    if accepted is not None and (not isinstance(accepted, dict) or
            set(accepted) != {'task_id', 'request_id', 'runtime_generation'} or
            accepted['task_id'] != record['request_id'][:24] or
            accepted['request_id'] != record['request_id'] or
            not isinstance(accepted['runtime_generation'], str) or not accepted['runtime_generation']):
        raise InvalidTransition('Unbound review acceptance')
    if record['phase'] == 'VERIFY' and accepted is None:
        raise InvalidTransition('Verification lacks request-bound acceptance')
    if record['phase'] == 'DRAIN' and not record['invalidated']:
        raise InvalidTransition('Drain requires invalidated launch authority')
    return record


def authorize(record, arguments, binding):
    """Build one atomic checkpoint; the caller reserves and persists before I/O."""
    validate(record)
    if record['phase'] != 'PREPARE' or record['invalidated']:
        raise InvalidTransition('Review attempt cannot be authorized')
    arguments = deepcopy(arguments)
    if arguments.get('request_id', record['request_id']) != record['request_id']:
        raise InvalidTransition('Review request ID changed')
    arguments['request_id'] = record['request_id']
    pending = dict(name='start_task', operation=record['operation'], arguments=arguments)
    updated = deepcopy(record)
    updated.update(phase='LAUNCH', binding=deepcopy(binding), request_digest=digest(pending))
    validate(updated, pending)
    return updated, pending


def invalidate(record, pending=None):
    validate(record, pending)
    updated = deepcopy(record)
    updated['invalidated'] = True
    if updated['request_digest'] is not None:
        updated['phase'] = 'DRAIN'
    return updated


def accept(record, pending, evidence):
    """Record an external fact even after logical invalidation; never approve it."""
    validate(record, pending)
    if record['phase'] not in ('LAUNCH', 'VERIFY', 'DRAIN'):
        raise InvalidTransition('Unexpected review acceptance phase')
    if record['accepted_task'] is not None and record['accepted_task'] != evidence:
        raise InvalidTransition('Accepted review runtime identity changed')
    updated = deepcopy(record)
    updated['accepted_task'] = deepcopy(evidence)
    updated['phase'] = 'DRAIN' if updated['invalidated'] else 'VERIFY'
    validate(updated, pending)
    return updated


def source_matches(record, report, inputs):
    return (not record['invalidated'] and
            digest(source_identity(report)) == record['source']['report_digest'] and
            digest(inputs) == record['source']['input_digest'])


def holds_repository(item):
    # Unknown versions/shapes must also hold ownership pending inspection.
    return isinstance(item, dict) and (item.get('review_transition') is not None or item.get('review_migration') is not None)
