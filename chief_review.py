"""Deterministic Chief Review identities, bounded evidence and strict commands.

Identity helpers bind locally trusted task state after observing its coding turn.
Runtime rendering consumes locally extracted evidence; verdict commands cannot
supply repository paths or contents. Identity validation detects stale state,
not an unobserved repository change. A bundle ID is not a signature or approval.
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

# Runtime contracts preserve the M1 identity encoding and metadata envelope.
MAX_FINDINGS = 32
MAX_FINDING_LENGTH = 2000
CHUNK_BYTES = 40000
MAX_CHUNKS = 128
COMMAND_MARKER = 'DEVORCHESTRATOR_CHIEF_REVIEW_V1'
CHIEF_STATES = (None, 'NONE', 'NOT_REQUIRED', 'AWAITING_CHIEF_REVIEW',
                'PASS', 'CHANGES_REQUIRED', 'STALE', 'CONFLICT')


def is_evidence_comment(body):
    return isinstance(body, str) and body.lstrip().startswith((
        '<!-- DEVORCHESTRATOR_CHIEF_BUNDLE_V1 ', '<!-- DEVORCHESTRATOR_CHIEF_CHUNK_V1 '))


def sanitize(text):
    from redaction import redact
    text = re.sub(r'[\ud800-\udfff]', '\ufffd', redact(text))
    text = re.sub(r'-----BEGIN [^-\n]*PRIVATE KEY-----.*?-----END [^-\n]*PRIVATE KEY-----',
                  '[REDACTED PRIVATE KEY]', text, flags=re.S)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return text.replace('<', '&lt;').replace('>', '&gt;')


def validate_verdict(data):
    _object(data, ('bundle_id', 'verdict', 'summary', 'findings'))
    if not isinstance(data['bundle_id'], str) or not re.fullmatch(r'[0-9a-f]{64}', data['bundle_id']):
        raise ValueError('Invalid Chief bundle id')
    if not isinstance(data['verdict'], str) or data['verdict'] not in ('PASS', 'CHANGES_REQUIRED'):
        raise ValueError('Invalid Chief verdict')
    summary, findings = data['summary'], data['findings']
    if not isinstance(summary, str) or not summary.strip() or len(summary) > MAX_SUMMARY_LENGTH:
        raise ValueError('Invalid Chief summary')
    if (not isinstance(findings, list) or len(findings) > MAX_FINDINGS or
            any(not isinstance(f, str) or not f.strip() or len(f) > MAX_FINDING_LENGTH for f in findings)):
        raise ValueError('Invalid Chief findings')
    if (data['verdict'] == 'PASS') != (not findings):
        raise ValueError('Chief verdict/findings mismatch')
    try:
        summary.encode('utf-8')
        for finding in findings:
            finding.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError('Invalid Unicode in Chief metadata') from None
    clean_summary = sanitize(summary)[:MAX_SUMMARY_LENGTH]
    clean_findings = [sanitize(f)[:MAX_FINDING_LENGTH] for f in findings]
    if not clean_summary.strip() or any(not f.strip() for f in clean_findings):
        raise ValueError('Empty sanitized Chief metadata')
    return dict(data, summary=clean_summary, findings=clean_findings)


def parse_verdict(body):
    if not isinstance(body, str) or COMMAND_MARKER not in body:
        return None
    if len(body) > 70000:
        raise ValueError('Oversized Chief command')
    match = re.fullmatch(r'\s*<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 (\{.*\}) -->\s*', body, re.S)
    if not match:
        raise ValueError('Malformed Chief command')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate Chief field')
            result[key] = value
        return result

    try:
        return validate_verdict(json.loads(match[1], object_pairs_hook=unique))
    except (json.JSONDecodeError, RecursionError):
        raise ValueError('Malformed Chief JSON') from None


def evidence_comments(identity, project, repository, item, report, evidence):
    """Deterministic byte-bounded comments with ordered exact-content hashes."""
    from redaction import redact
    payload = json.dumps({'changed_files': evidence['files']}, sort_keys=True, indent=2, ensure_ascii=True)
    payload += '\n\nComplete textual patch (credentials redacted; binary/secret paths metadata only):\n'
    payload += evidence['patch']
    payload = redact(payload)
    chunks, remaining = [], payload.encode('utf-8')
    while remaining:
        part = remaining[:CHUNK_BYTES]
        while True:
            try:
                decoded = part.decode('utf-8')
                break
            except UnicodeDecodeError:
                part = part[:-1]
        chunks.append(decoded)
        remaining = remaining[len(part):]
        if len(chunks) > MAX_CHUNKS:
            raise ValueError('Chief evidence exceeds safe bundle bound')
    hashes = [hashlib.sha256(p.encode('utf-8')).hexdigest() for p in chunks]
    manifest = dict(identity=identity.to_dict(), project=sanitize(project), repository=sanitize(repository),
                    coding_agent=sanitize(str(item.get('coding_agent') or report.get('agent') or '')),
                    coding_session=sanitize(str(report.get('session_id') or ''))[:128],
                    independent_reviewer=sanitize(str(item.get('reviewer_agent') or '')),
                    independent_review_task=item.get('review_task_id'), independent_verdict='PASS',
                    independent_summary=sanitize(str(item.get('independent_review_summary') or ''))[:MAX_SUMMARY_LENGTH],
                    verification_summary=sanitize(str(item.get('chief_verification_summary') or report.get('result') or ''))[:MAX_SUMMARY_LENGTH],
                    changed_file_count=len(evidence['files']),
                    chunk_count=len(chunks), chunk_sha256=hashes,
                    evidence_sha256=hashlib.sha256(payload.encode('utf-8')).hexdigest())
    body = ('<!-- DEVORCHESTRATOR_CHIEF_BUNDLE_V1 ' + identity.bundle_id + ' -->\n'
            '## Engineering Review Bundle — AWAITING_CHIEF_REVIEW\n\n'
            'Read-only evidence; this bundle grants no publication authority.\n'
            'Review all chunks in order before issuing the strict Chief command.\n\n' +
            '```json\n' + json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True) + '\n```')
    comments = [body]
    for n, (part, digest) in enumerate(zip(chunks, hashes), 1):
        fence = '`' * (max([2] + [len(run[0]) for run in re.finditer(r'`+', part)]) + 1)
        comments.append(f'<!-- DEVORCHESTRATOR_CHIEF_CHUNK_V1 {identity.bundle_id} {n}/{len(chunks)} {digest} -->\n' +
                        fence + '\n' + part + '\n' + fence)
    if any(len(body.encode('utf-8')) > 50000 for body in comments):
        raise ValueError('Chief comment exceeds safe bound')
    return comments


def invalidate(item):
    if item.get('chief_review'):
        item.setdefault('chief_review_history', []).append(item['chief_review'])
    item.pop('chief_review', None)
    item.pop('chief_verification_summary', None)
    item.pop('independent_review_summary', None)
    if item.get('chief_required'):
        item['chief_status'] = 'STALE'


def malformed_runtime(item):
    """Validate durable shapes independently of current tree/turn observations."""
    if item.get('chief_status') not in CHIEF_STATES:
        return True
    if 'chief_required' in item and type(item['chief_required']) is not bool:
        return True
    record = item.get('chief_review')
    if record is None:
        return item.get('chief_status') in ('PASS', 'CHANGES_REQUIRED', 'CONFLICT')
    try:
        if item.get('chief_required') is not True or not isinstance(record, dict):
            return True
        required = {'identity', 'revision', 'effects', 'complete', 'verdict'}
        if not required <= record.keys() or record.keys() - required - {'conflict'}:
            return True
        identity = ChiefReviewIdentity.from_dict(record['identity'])
        keys = record['effects']
        if (type(record['revision']) is not int or record['revision'] != 1 or
                type(record['complete']) is not bool or
                not isinstance(keys, list) or not 2 <= len(keys) <= MAX_CHUNKS + 1 or
                any(not isinstance(k, str) or not re.fullmatch(r'[0-9a-f]{64}', k) for k in keys) or
                len(set(keys)) != len(keys) or type(record.get('conflict', False)) is not bool):
            return True
        verdict = record['verdict']
        if verdict is not None:
            from datetime import datetime
            _object(verdict, ('data', 'source', 'revision', 'timestamp', 'source_revision', 'source_timestamp', 'actor'))
            data = validate_verdict(verdict['data'])
            if (data['bundle_id'] != identity.bundle_id or
                    type(verdict['revision']) is not int or verdict['revision'] != 1 or
                    not isinstance(verdict['source'], str) or
                    not re.fullmatch(r'[1-9][0-9]{0,127}', verdict['source']) or
                    not isinstance(verdict['source_revision'], str) or
                    not re.fullmatch(r'[0-9a-f]{64}', verdict['source_revision']) or
                    not isinstance(verdict['actor'], str) or not verdict['actor'] or len(verdict['actor']) > 128 or
                    not isinstance(verdict['source_timestamp'], str) or len(verdict['source_timestamp']) > 128 or
                    not isinstance(verdict['timestamp'], str) or
                    datetime.fromisoformat(verdict['timestamp']).tzinfo is None):
                return True
        elif item.get('chief_status') in ('PASS', 'CHANGES_REQUIRED', 'CONFLICT'):
            return True
        return False
    except (ValueError, TypeError, KeyError, OverflowError):
        return True


def approval_complete(item):
    """A label, malformed state, or stale identity cannot authorize publication."""
    if malformed_runtime(item):
        return False
    if not item.get('chief_required'):
        return item.get('chief_status') in (None, 'NONE', 'NOT_REQUIRED')
    try:
        record = item['chief_review']
        validate_identity(record['identity'], item)
        verdict = validate_verdict(record['verdict']['data'])
        return (item.get('chief_status') == 'PASS' and record['complete'] is True and
                verdict['verdict'] == 'PASS' and not record.get('conflict'))
    except (ValueError, TypeError, KeyError):
        return False
