"""GitHub task-bus I/O and normalized identities, with V1 compatibility views.

The injected CLI functions perform I/O only. Parsing, authorization, lifecycle,
outbox markers and retries remain in TaskBridge. There is no second polling loop
or journal here. Compatibility views keep provider fields needed by V1 while
discovery and tracked task reads validate through ExternalTask first.
"""
from copy import deepcopy
import json
import re
import tempfile

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
    def __init__(self, repository: str, *, run=None, read_json=None, state_labels=()):
        if type(repository) is not str or not re.fullmatch(
                r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise ValueError('Expected a GitHub owner/repository')
        # GitHub repository identity is case-insensitive; credentials are absent.
        self._repository = repository.lower()
        self._scope = TransportScope('github', 'github.com/' + self._repository)
        # Retain the configured spelling in V1 CLI arguments/endpoints.
        self._cli_repository = repository
        self._run = run
        self._read_json = read_json
        self._state_labels = tuple(state_labels)
        self._issue_views = {}

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
        # Commit the compatibility batch only after all candidates validate.
        # Failed polls cannot expose stale/partial observations as fresh success.
        self._issue_views = {}
        rows = self.pages(f'repos/{self._cli_repository}/issues?state=open&sort=created&direction=asc&per_page=100')
        commands, views = [], {}
        for issue in rows:
            if type(issue) is not dict:
                raise TransportError('poll', 'invalid_issue')
            if 'pull_request' in issue:
                continue
            task = self.map_task(issue)
            try:
                command = ControlCommand(
                    source_transport=self.scope.kind, source_id=task.ref.source_id(),
                    kind='task.discovered',
                    payload={'external_id': task.ref.external_id, 'title': task.title,
                             'body': task.body, 'url': task.url}, actor={},
                    created_at=issue.get('created_at') or '1970-01-01T00:00:00Z')
            except ValueError:
                raise TransportError('poll', 'invalid_issue') from None
            commands.append(command)
            views[command.source_id] = (command, self._legacy_task(issue, task))
        self._issue_views = views
        return tuple(commands)

    def issue_view(self, command: ControlCommand) -> dict:
        """Ephemeral V1 metadata view of this validated poll, never a new read.

        It is not local lifecycle state. M2A identities do not replace issue-number
        state keys or V1 launch/delivery receipts. No command is dispatched here.
        """
        previous = self._issue_views.get(command.source_id) if type(command) is ControlCommand else None
        if previous is None or previous[0] != command:
            raise TransportError('map', 'unknown_observation')
        return deepcopy(previous[1])

    @staticmethod
    def _legacy_task(issue, task):
        view = dict(issue)
        # Preserve absent/null fields as V1 did; non-null text comes from the
        # normalized observation. URL precedence is identical to V1 discovery.
        for field in ('title', 'body'):
            if view.get(field) is not None:
                view[field] = getattr(task, field)
        view['url'] = task.url
        return view

    def fetch_task(self, target: ExternalRef) -> ExternalTask:
        return self.map_task(self.fetch_issue(target))

    def fetch_issue(self, target: ExternalRef) -> dict:
        number = self._issue_number(target)
        issue = self._json('issue', 'view', number, '-R', self._cli_repository,
                           '--json', 'number,title,body,labels,url,state')
        task = self.map_task(issue)
        if task.ref != target:
            raise TransportError('fetch', 'wrong_issue')
        return self._legacy_task(issue, task)

    def fetch_labels(self, target: ExternalRef) -> dict:
        number = self._issue_number(target)
        issue = self._json('issue', 'view', number, '-R', self._cli_repository,
                           '--json', 'number,labels')
        if type(issue) is not dict or self.issue_ref(issue.get('number')) != target:
            raise TransportError('fetch', 'wrong_issue')
        return issue

    def pages(self, endpoint: str) -> list:
        # Older gh versions emit adjacent arrays for --paginate (no --slurp).
        raw = self._call('read', 'api', endpoint, '--paginate')
        decoder, rows = json.JSONDecoder(), []
        try:
            if type(raw) is not str or not raw.strip():
                raise ValueError
            while raw.strip():
                page, position = decoder.raw_decode(raw.lstrip())
                if type(page) is not list:
                    raise ValueError
                rows.extend(page)
                raw = raw.lstrip()[position:]
        except (ValueError, TypeError, AttributeError):
            raise TransportError('read', 'invalid_collection') from None
        return rows

    def issue_comments(self, target: ExternalRef) -> list:
        number = self._issue_number(target)
        rows = self.pages(f'repos/{self._cli_repository}/issues/{number}/comments?per_page=100')
        if any(type(row) is not dict for row in rows):
            raise TransportError('read', 'invalid_comment')
        # V1 aliases, author fallback and time precedence are intentionally intact.
        # Do not impose V2 numeric comment identity on legacy dedupe/RPC keys.
        return [{**row, 'author': row.get('user') or row.get('author'),
                 'url': row.get('html_url') or row.get('url'),
                 'createdAt': row.get('created_at') or row.get('createdAt')} for row in rows]

    def post_comment(self, target: ExternalRef, body: str) -> None:
        number = self._issue_number(target)
        attempted = False
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as stream:
                stream.write(body)
                stream.flush()
                attempted = True
                self._call('write', 'issue', 'comment', number, '-R', self._cli_repository,
                           '--body-file', stream.name)
        except TransportError:
            raise
        except Exception:
            raise TransportError('write', 'comment_failed', uncertain=attempted) from None

    def transition_issue(self, issue: dict, target: str) -> None:
        number = self._issue_number(self.issue_ref(issue['number']))
        if target == 'dev:awaiting-chief-review':
            # Provision only the additive label. Concurrent creation retries via
            # the existing outbox, and existing label settings are left intact.
            labels = self.pages(f'repos/{self._cli_repository}/labels?per_page=100')
            if not any(isinstance(label, dict) and label.get('name') == target for label in labels):
                self._call('write', 'label', 'create', target, '-R', self._cli_repository,
                           '--color', '6F42C1', '--description', 'Awaiting a bound Chief Engineer verdict')
        current = self.label_names(issue)
        args = ['issue', 'edit', number, '-R', self._cli_repository]
        for label in self._state_labels:
            if label in current and label != target:
                args += ['--remove-label', label]
        if target not in current:
            args += ['--add-label', target]
        if len(args) > 5:
            self._call('write', *args)

    @staticmethod
    def label_names(issue):
        result = set()
        for label in issue.get('labels') or []:
            name = label.get('name') if isinstance(label, dict) else str(label)
            if name:
                result.add(name)
        return result

    def publish_event(self, target: ExternalRef, event: ControlEvent) -> None:
        self._check_target(target)
        if type(event) is not ControlEvent:
            raise TransportError('publish', 'invalid_event')
        # M2C delivers the V1 outbox via post_comment/transition_issue. Generic
        # event rendering is deliberately not a parallel delivery path.
        raise TransportError('publish', 'not_implemented')

    def _issue_number(self, target):
        self._check_target(target)
        match = re.fullmatch(r'issue:([1-9][0-9]*)', target.external_id)
        if not match:
            raise TransportError('route', 'not_issue')
        return match.group(1)

    def _call(self, operation, *args):
        if self._run is None:
            raise TransportError(operation, 'not_implemented')
        try:
            return self._run(*args)
        except Exception:
            raise TransportError(operation, 'github_io_failed', uncertain=operation == 'write') from None

    def _json(self, *args):
        if self._read_json is None:
            raw = self._call('read', *args)
            try:
                return json.loads(raw) if raw.strip() else None
            except (ValueError, TypeError, AttributeError):
                raise TransportError('read', 'invalid_json') from None
        try:
            return self._read_json(*args)
        except Exception:
            raise TransportError('read', 'github_io_failed') from None

    def _check_target(self, target):
        if type(target) is not ExternalRef or target.scope != self.scope:
            raise TransportError('route', 'wrong_scope')
