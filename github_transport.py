"""GitHub V2 mapping skeleton, with no CLI/API calls or production integration.

V1 retains discovery, parsing, owner gates, effects and dedupe unchanged. M2C may
implement the I/O methods once delivery/recovery wiring is reviewed. These pure
mapping helpers demonstrate the boundary without importing the workflow bridge.
"""
import re

from control_plane import ControlCommand, ControlEvent
from transports import ExternalRef, ExternalTask, TransportError, TransportScope


def _positive_id(value):
    # REST IDs may be JSON integers or strings; bool is not an ID.
    if type(value) is int and value > 0:
        try:
            return str(value)
        except ValueError:
            raise TransportError('map', 'invalid_external_id') from None
    if type(value) is str and re.fullmatch(r'[1-9][0-9]*', value):
        return value
    raise TransportError('map', 'invalid_external_id')


class GitHubTransport:
    def __init__(self, repository: str):
        if type(repository) is not str or not re.fullmatch(
                r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise ValueError('Expected a GitHub owner/repository')
        # GitHub repository identity is case-insensitive; credentials are absent.
        self._repository = repository.lower()
        self._scope = TransportScope('github', 'github.com/' + self._repository)

    @property
    def scope(self) -> TransportScope:
        return self._scope

    def issue_ref(self, number) -> ExternalRef:
        try:
            return ExternalRef(self.scope, 'issue:' + _positive_id(number))
        except ValueError:
            raise TransportError('map', 'invalid_external_id') from None

    def comment_ref(self, number, comment: dict) -> ExternalRef:
        """REST ID and GraphQL/web references converge on the numeric comment ID.

        V1 separately backfills id/node_id/url/html_url aliases. No legacy dedupe
        keys or RPC operation IDs are rewritten by this additive mapping. Unlike
        V1's last-resort text fallback, V2 refuses a source without a durable ID.
        A node-only GraphQL response must first be resolved by a future adapter.
        """
        target = self.issue_ref(number)
        if type(comment) is not dict:
            raise TransportError('map', 'invalid_comment')
        try:
            identity = _positive_id(comment.get('id'))
        except TransportError:
            url = comment.get('html_url') or comment.get('url')
            prefix = 'https://github.com/' + self._repository + '/issues/' + _positive_id(number)
            match = re.fullmatch(re.escape(prefix) + r'#issuecomment-([1-9][0-9]*)',
                                 url, re.I) if type(url) is str else None
            if not match:
                raise TransportError('map', 'missing_stable_comment_id') from None
            identity = match.group(1)
        try:
            return ExternalRef(self.scope, target.external_id + '/comment:' + identity)
        except ValueError:
            raise TransportError('map', 'invalid_external_id') from None

    def map_task(self, issue: dict) -> ExternalTask:
        """Preserve V1 issue number/title/body and html_url-or-url display semantics.

        Labels, REST/GraphQL metadata and marker parsing do not enter the generic
        task type. Pull requests are not task candidates, as in V1 open_issues().
        """
        if type(issue) is not dict or 'pull_request' in issue:
            raise TransportError('map', 'invalid_issue')
        try:
            title, body = issue.get('title'), issue.get('body')
            return ExternalTask(self.issue_ref(issue.get('number')),
                                '' if title is None else title, '' if body is None else body,
                                issue.get('html_url') or issue.get('url'))
        except ValueError:
            raise TransportError('map', 'invalid_issue') from None

    def poll_commands(self) -> tuple[ControlCommand, ...]:
        raise TransportError('poll', 'not_implemented')

    def fetch_task(self, target: ExternalRef) -> ExternalTask:
        self._check_target(target)
        raise TransportError('fetch', 'not_implemented')

    def publish_event(self, target: ExternalRef, event: ControlEvent) -> None:
        self._check_target(target)
        if type(event) is not ControlEvent:
            raise TransportError('publish', 'invalid_event')
        raise TransportError('publish', 'not_implemented')

    def _check_target(self, target):
        if type(target) is not ExternalRef or target.scope != self.scope:
            raise TransportError('route', 'wrong_scope')
