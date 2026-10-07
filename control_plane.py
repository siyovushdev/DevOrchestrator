"""V2 transport-neutral envelopes and an additive local receipt/event journal.

Deliberately unused by V1. This journal records identity, not task lifecycle,
authorization, execution or delivery status. Actor metadata is descriptive, not
owner approval. Only a trusted local caller chooses the journal path; envelope
references are opaque values and are never interpreted as paths or commands.
"""
from dataclasses import dataclass, fields
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
from types import MappingProxyType

from durable import exclusive, read_json, write_json


SCHEMA_VERSION = 1
MAX_SEQUENCE = (1 << 63) - 1


def _object(data, names):
    if type(data) is not dict or data.keys() != set(names):
        raise ValueError('Missing or unknown control-plane fields')


def _unique_object(pairs):
    # The standard decoder otherwise silently discards duplicate JSON keys.
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Repeated control journal JSON key')
        result[key] = value
    return result


def _text(value):
    if type(value) is not str or not value.strip() or len(value) > 512:
        raise ValueError('Invalid control-plane reference')


def _sequence(value, minimum=1):
    if type(value) is not int or not minimum <= value <= MAX_SEQUENCE:
        raise ValueError('Invalid control-plane sequence')


def _version(value):
    if type(value) is not int or value != SCHEMA_VERSION:
        raise ValueError('Unsupported control-plane version')


def _kind(value):
    if type(value) is not str or not re.fullmatch(r'[a-z][a-z0-9_.-]{0,63}', value):
        raise ValueError('Invalid control-plane kind')


def _timestamp(value):
    # One explicit UTC representation. No wall clock is sampled by these helpers.
    if type(value) is not str or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z', value):
        raise ValueError('Invalid control-plane UTC timestamp')
    try:
        datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('Invalid control-plane UTC timestamp') from exc


def _freeze(value):
    """Snapshot strict JSON into deeply immutable values; reject lossy coercion."""
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError('Control-plane JSON keys must be strings')
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_freeze(item) for item in value)
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    raise ValueError('Invalid control-plane JSON value')


def _thaw(value):
    if isinstance(value, MappingProxyType):
        return {key: _thaw(item) for key, item in value.items()}
    if type(value) is tuple:
        return [_thaw(item) for item in value]
    return value


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False).encode('utf-8')
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError('Invalid control-plane JSON encoding') from exc


def _digest(domain, value):
    return hashlib.sha256(_canonical([domain, SCHEMA_VERSION, value])).hexdigest()


class _Envelope:
    def to_dict(self):
        """Return detached JSON values, including all explicit optional fields."""
        return {item.name: _thaw(getattr(self, item.name)) for item in fields(self)}

    @classmethod
    def from_dict(cls, data):
        _object(data, (item.name for item in fields(cls)))
        result = cls(**data)
        # Persisted derived IDs are bindings, never trusted inputs.
        if result.to_dict() != data:
            raise ValueError('Control-plane identity mismatch')
        return result


@dataclass(frozen=True)
class ControlCommand(_Envelope):
    source_transport: str
    source_id: str
    kind: str
    payload: dict
    actor: dict
    created_at: str
    logical_id: str | None = None
    task_ref: str | None = None
    project_ref: str | None = None
    source_order: int | None = None
    schema_version: int = SCHEMA_VERSION
    command_id: str = ''

    def __post_init__(self):
        _version(self.schema_version)
        if type(self.source_transport) is not str or not re.fullmatch(
                r'[a-z][a-z0-9_-]{0,31}', self.source_transport):
            raise ValueError('Invalid control-plane transport')
        _text(self.source_id)
        _kind(self.kind)
        _timestamp(self.created_at)
        for value in (self.logical_id, self.task_ref, self.project_ref):
            if value is not None:
                _text(value)
        if self.source_order is not None:
            _sequence(self.source_order, minimum=0)
        for name in ('payload', 'actor'):
            if type(getattr(self, name)) is not dict:
                raise ValueError('Control-plane payload and actor must be objects')
            object.__setattr__(self, name, _freeze(getattr(self, name)))
        identity = (['logical', self.logical_id] if self.logical_id is not None else
                    ['source', self.source_transport, self.source_id])
        command_id = _digest('control-command', identity)
        if type(self.command_id) is not str or self.command_id not in ('', command_id):
            raise ValueError('Control-plane command identity mismatch')
        object.__setattr__(self, 'command_id', command_id)
        _canonical(self.to_dict())

    def semantic_bytes(self):
        """Cross-transport content binding; actor/target must be normalized too.

        Transport, source ID, timestamps and source order are provenance, excluded
        from this binding. Kind, payload, actor and task/project scope must match.
        Free-form text never supplies identity, even if payload contains text.
        """
        data = self.to_dict()
        return _canonical({name: data[name] for name in
                           ('schema_version', 'kind', 'payload', 'actor', 'task_ref', 'project_ref')})


@dataclass(frozen=True)
class ControlEvent(_Envelope):
    task_ref: str
    kind: str
    revision: int
    payload: dict
    created_at: str
    schema_version: int = SCHEMA_VERSION
    event_id: str = ''

    def __post_init__(self):
        _version(self.schema_version)
        _text(self.task_ref)
        _kind(self.kind)
        _sequence(self.revision)
        _timestamp(self.created_at)
        if type(self.payload) is not dict:
            raise ValueError('Control-plane payload must be an object')
        object.__setattr__(self, 'payload', _freeze(self.payload))
        event_id = _digest('control-event', [self.task_ref, self.revision])
        if type(self.event_id) is not str or self.event_id not in ('', event_id):
            raise ValueError('Control-plane event identity mismatch')
        object.__setattr__(self, 'event_id', event_id)
        _canonical(self.to_dict())


@dataclass(frozen=True)
class JournalResult:
    """Identity and first receipt/event sequence; duplicate grants no new work."""
    identity: str
    sequence: int
    duplicate: bool


def _command_receipt(command, sources, identities):
    source = (command.source_transport, command.source_id)
    previous = sources.get(source)
    first = identities.get(command.command_id)
    if previous is not None:
        old = previous[0]
        if old.command_id != command.command_id or old.semantic_bytes() != command.semantic_bytes():
            raise ValueError('Conflicting command source replay')
    elif first is not None:
        old = first[0]
        if (command.logical_id is None or old.logical_id != command.logical_id or
                old.semantic_bytes() != command.semantic_bytes()):
            raise ValueError('Conflicting logical command replay')
    return first


def _validate_journal(data):
    """Rebuild dedupe indexes from immutable records; never trust stored indexes."""
    _object(data, ('schema_version', 'receipts', 'events'))
    _version(data['schema_version'])
    if type(data['receipts']) is not list or type(data['events']) is not list:
        raise ValueError('Invalid control journal collections')
    sources, identities, revisions, events = {}, {}, {}, {}
    for sequence, entry in enumerate(data['receipts'], 1):
        _object(entry, ('sequence', 'command'))
        _sequence(entry['sequence'])
        if entry['sequence'] != sequence:
            raise ValueError('Invalid control receipt ordering')
        command = ControlCommand.from_dict(entry['command'])
        source = (command.source_transport, command.source_id)
        if source in sources:
            raise ValueError('Repeated control receipt source')
        _command_receipt(command, sources, identities)
        sources[source] = (command, sequence)
        identities.setdefault(command.command_id, (command, sequence))
    for sequence, entry in enumerate(data['events'], 1):
        _object(entry, ('sequence', 'event'))
        _sequence(entry['sequence'])
        if entry['sequence'] != sequence:
            raise ValueError('Invalid control event ordering')
        event = ControlEvent.from_dict(entry['event'])
        if event.revision != revisions.get(event.task_ref, 0) + 1:
            raise ValueError('Invalid control event revision ordering')
        revisions[event.task_ref] = event.revision
        events[event.event_id] = (event, sequence)
    return sources, identities, revisions, events


class ControlJournal:
    """A separate, caller-selected local JSON file, never the V1 bridge state.

    Receipt sequence orders first sightings of transport sources; cross-transport
    aliases are retained but return the original command's sequence as duplicate.
    Exact source replay does not write. Events have global append sequence and
    contiguous per-task revisions, independent of command receipt sequence.

    The lock covers read/validate/write so concurrent adapters cannot both record
    a new command. A receipt is not proof execution happened: crash recovery and
    effect delivery remain responsibilities of the existing pipeline. No callbacks
    or task status transitions are implemented here.
    """
    def __init__(self, path):
        self.path = Path(path)
        self.lock_path = self.path.with_name(self.path.name + '.lock')

    def _load(self):
        data = read_json(self.path, {'schema_version': SCHEMA_VERSION, 'receipts': [], 'events': []},
                         object_pairs_hook=_unique_object)
        indexes = _validate_journal(data)
        return data, indexes

    def snapshot(self):
        """Read a validated detached journal; a missing file is an empty journal."""
        with exclusive(self.lock_path, blocking=True):
            return self._load()[0]

    def record_command(self, command):
        if type(command) is not ControlCommand:
            raise ValueError('Expected a normalized ControlCommand')
        command = ControlCommand.from_dict(command.to_dict())
        with exclusive(self.lock_path, blocking=True):
            data, (sources, identities, _, _) = self._load()
            first = _command_receipt(command, sources, identities)
            if (command.source_transport, command.source_id) not in sources:
                sequence = len(data['receipts']) + 1
                _sequence(sequence)
                data['receipts'].append({'sequence': sequence, 'command': command.to_dict()})
                write_json(self.path, data)
            else:
                sequence = first[1]
            return JournalResult(command.command_id, first[1] if first else sequence, first is not None)

    def append_event(self, event):
        if type(event) is not ControlEvent:
            raise ValueError('Expected a normalized ControlEvent')
        event = ControlEvent.from_dict(event.to_dict())
        with exclusive(self.lock_path, blocking=True):
            data, (_, _, revisions, events) = self._load()
            previous = events.get(event.event_id)
            if previous is not None:
                if _canonical(previous[0].to_dict()) != _canonical(event.to_dict()):
                    raise ValueError('Conflicting immutable control event')
                return JournalResult(event.event_id, previous[1], True)
            if event.revision != revisions.get(event.task_ref, 0) + 1:
                raise ValueError('Control event revision must be next for its task')
            sequence = len(data['events']) + 1
            _sequence(sequence)
            data['events'].append({'sequence': sequence, 'event': event.to_dict()})
            write_json(self.path, data)
            return JournalResult(event.event_id, sequence, False)
