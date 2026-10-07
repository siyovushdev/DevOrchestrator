"""Pure Chief Review values; deliberately not wired into the V1 runtime.

Callers supply locally trusted task state after observing the current coding turn.
These helpers consume the existing reviewed-tree fingerprint, never repository
paths or contents. Validation detects stale state, not an unobserved repository
change. A bundle ID is a deterministic binding, not a signature or approval.
"""
from dataclasses import asdict, dataclass, field
import hashlib
import json
import re


SCHEMA_VERSION = 1
BUNDLE_VERSION = 1
MAX_SUMMARY_LENGTH = 2000
MAX_SEQUENCE = (1 << 63) - 1
_IDENTITY_FIELDS = (
    'schema_version', 'bundle_version', 'task_id', 'coding_turn',
    'review_round', 'reviewed_tree_fingerprint',
)


def _object(data, fields):
    if not isinstance(data, dict) or data.keys() != set(fields):
        raise ValueError('Missing or unknown Chief Review fields')


def _sequence(value):
    return type(value) is int and 1 <= value <= MAX_SEQUENCE


@dataclass(frozen=True)
class ChiefReviewIdentity:
    task_id: str
    coding_turn: int
    review_round: int
    reviewed_tree_fingerprint: str
    schema_version: int = SCHEMA_VERSION
    bundle_version: int = BUNDLE_VERSION
    bundle_id: str = field(init=False)

    def __post_init__(self):
        if (type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION or
                type(self.bundle_version) is not int or self.bundle_version != BUNDLE_VERSION):
            raise ValueError('Unsupported Chief Review version')
        # Same task identifier grammar as V1; this value is never used as a path.
        if (not isinstance(self.task_id, str) or
                not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', self.task_id) or
                '..' in self.task_id):
            raise ValueError('Invalid Chief Review task id')
        if not _sequence(self.coding_turn) or not _sequence(self.review_round):
            raise ValueError('Invalid Chief Review turn or round')
        if (not isinstance(self.reviewed_tree_fingerprint, str) or
                not re.fullmatch(r'[0-9a-f]{64}', self.reviewed_tree_fingerprint)):
            raise ValueError('Invalid reviewed-tree fingerprint')
        payload = {name: getattr(self, name) for name in _IDENTITY_FIELDS}
        canonical = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('ascii')
        object.__setattr__(self, 'bundle_id', hashlib.sha256(canonical).hexdigest())

    def to_dict(self):
        """Return a detached, JSON-compatible representation."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        """Parse exactly this version's fields and verify the supplied binding."""
        _object(data, (*_IDENTITY_FIELDS, 'bundle_id'))
        identity = cls(**{name: data[name] for name in _IDENTITY_FIELDS})
        if not isinstance(data['bundle_id'], str) or data['bundle_id'] != identity.bundle_id:
            raise ValueError('Chief Review bundle id mismatch')
        return identity


def build_identity(item):
    """Bind a completed independent review to its current coding turn.

    Missing legacy turn evidence cannot create a Chief identity. This has no
    effect on V1 state loading or owner gates, and requires no Chief state fields.
    Owner publication status is intentionally independent of this value.
    """
    if not isinstance(item, dict):
        raise ValueError('Invalid trusted task state')
    turn = item.get('coding_turn')
    bound_turn = item.get('review_coding_turn')
    if (item.get('review_status') != 'PASS' or item.get('coding_status') != 'DONE' or
            item.get('review_hold') is not None or not _sequence(turn) or
            not _sequence(bound_turn) or bound_turn != turn):
        raise ValueError('No current independent review PASS')
    tree = item.get('reviewed_tree')
    if not isinstance(tree, dict):
        raise ValueError('Missing trusted reviewed tree')
    return ChiefReviewIdentity(
        task_id=item.get('task_id'), coding_turn=turn,
        review_round=item.get('review_round'), reviewed_tree_fingerprint=tree.get('fingerprint'),
    )


def validate_identity(data, item):
    """Return a parsed identity, or raise ValueError for malformed/stale data."""
    identity = ChiefReviewIdentity.from_dict(data)
    if identity != build_identity(item):
        raise ValueError('Stale or mismatched Chief Review identity')
    return identity


@dataclass(frozen=True)
class EngineeringReviewBundle:
    """Bounded metadata envelope; no diff, files, paths, verdict or authority.

    Both version numbers live in the identity. Summary is descriptive metadata
    and is not part of the identity hash; this version contains no review payload.
    """
    identity: ChiefReviewIdentity
    summary: str = ''

    def __post_init__(self):
        if type(self.identity) is not ChiefReviewIdentity:
            raise ValueError('Invalid bundle identity')
        if not isinstance(self.summary, str) or len(self.summary) > MAX_SUMMARY_LENGTH:
            raise ValueError('Invalid or oversized bundle summary')

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        _object(data, ('identity', 'summary'))
        return cls(ChiefReviewIdentity.from_dict(data['identity']), data['summary'])


def build_bundle(item, summary=''):
    return EngineeringReviewBundle(build_identity(item), summary)


def validate_bundle(data, item):
    """Validate the envelope and its binding against current trusted state."""
    bundle = EngineeringReviewBundle.from_dict(data)
    validate_identity(bundle.identity.to_dict(), item)
    return bundle
