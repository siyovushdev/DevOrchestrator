"""V2 transport boundary, incrementally used by the GitHub V1 task bus.

Adapters exchange observations and externally visible effects, never local task
state. A command's actor is descriptive; authorization and all workflow decisions
stay in the local control/execution pipeline. Provider objects and cursors belong
inside adapters. There is no registry, dispatcher, journal or retry loop here.
"""
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Protocol, runtime_checkable

from control_plane import ControlCommand, ControlEvent


def _reference(value):
    if type(value) is not str or not value.strip() or len(value) > 512:
        raise ValueError('Invalid transport reference')
    try:
        value.encode('utf-8')
    except UnicodeError:
        raise ValueError('Invalid transport reference') from None


@dataclass(frozen=True)
class TransportScope:
    """Provider kind plus stable account/repository/list namespace, not a token.

    Multiple instances of the same provider must use distinct namespaces unless
    they intentionally observe the same external sources.
    """
    kind: str
    instance: str

    def __post_init__(self):
        if type(self.kind) is not str or not re.fullmatch(r'[a-z][a-z0-9_-]{0,31}', self.kind):
            raise ValueError('Invalid transport kind')
        _reference(self.instance)


@dataclass(frozen=True)
class ExternalRef:
    """Opaque adapter-owned resource ID, scoped for routing and M2A identity.

    This is an external address, not an authoritative local task/project ref.
    Never resolve it as a path, execution request or owner approval.
    """
    scope: TransportScope
    external_id: str

    def __post_init__(self):
        if type(self.scope) is not TransportScope:
            raise ValueError('Expected a TransportScope')
        _reference(self.external_id)

    def source_id(self, revision: str | None = None) -> str:
        """Stable bounded M2A source ID; text/timestamps never supply identity.

        An adapter may explicitly identify an edited message with a provider
        revision. Without one, changed content at this source conflicts in M2A.
        Preserve this versioned encoding across adapter/process restarts.
        """
        if revision is not None:
            _reference(revision)
        identity = ['transport-source', 1, self.scope.kind, self.scope.instance,
                    self.external_id, revision]
        encoded = json.dumps(identity, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        return 'v1:' + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ExternalTask:
    """Uninterpreted external task text; provider metadata/labels stay private.

    The local pipeline can parse legacy task/follow-up protocols separately.
    URL is a display reference only, not identity or an I/O instruction.
    """
    ref: ExternalRef
    title: str
    body: str
    url: str | None = None

    def __post_init__(self):
        if type(self.ref) is not ExternalRef or type(self.title) is not str or type(self.body) is not str:
            raise ValueError('Invalid normalized external task')
        if self.url is not None:
            _reference(self.url)


class TransportError(RuntimeError):
    """No successful read/delivery may be inferred from this failure.

    uncertain=True means an external write may have been accepted: reconcile
    before retrying. Diagnostic codes must be static and secret-free. Adapters
    must not expose provider exception text, credentials or raw responses here.
    """
    def __init__(self, operation: str, code: str, *, uncertain: bool = False):
        self.operation = operation
        self.code = code
        self.uncertain = uncertain
        super().__init__(f'Transport {operation} failed: {code}')


@runtime_checkable
class Transport(Protocol):
    """I/O-only contract. Failures raise TransportError, never empty success.

    Poll returns a complete validated batch of normalized M2A commands. Adapters
    bind source_transport/source_id to their stable scope and external sources;
    local callers still validate, journal, authorize and dispatch commands. No
    callback or mutable local lifecycle/owner state is passed to any method.

    Publish projects a caller-supplied ControlEvent into comments or visible
    state/labels as applicable. It cannot advance local state or authorize work.
    Unsupported event kinds fail explicitly. Return only after confirmed delivery
    (including an existing dedupe marker); ambiguous writes raise uncertain errors.
    Event IDs are delivery keys, not a replacement for the durable V1 outbox.
    Implementations must reject targets outside their scope.
    """
    @property
    def scope(self) -> TransportScope: ...

    def poll_commands(self) -> tuple[ControlCommand, ...]: ...

    def fetch_task(self, target: ExternalRef) -> ExternalTask: ...

    def publish_event(self, target: ExternalRef, event: ControlEvent) -> None: ...
