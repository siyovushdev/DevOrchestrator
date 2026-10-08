"""Concise terminal notifications with bounded, secret-safe transport retries."""
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from redaction import redact


def review_approval_complete(item):
    """Historical PASS alone cannot authorize publication or a COMMIT invitation."""
    tree = item.get('reviewed_tree')
    from chief_review import approval_complete
    return (approval_complete(item) and item.get('review_status') == 'PASS' and
            item.get('coding_status', 'DONE') == 'DONE' and
            ('coding_turn' not in item or item.get('review_coding_turn') == item['coding_turn']) and
            item.get('owner_state') in ('READY_FOR_OWNER', 'COMMITTED', 'PUSHED', 'PR_CREATED', 'MERGED') and
            isinstance(item.get('owner_approved_at'), str) and bool(item['owner_approved_at']) and
            isinstance(tree, dict) and isinstance(tree.get('fingerprint'), str) and bool(tree['fingerprint']) and
            item.get('review_hold') is None)


def publication_delta_present(item, report):
    """True/False when repository delta can be proven; None when unknown."""
    reviewed = item.get("reviewed_tree")
    baseline = (report or {}).get("git_baseline")

    if not isinstance(reviewed, dict) or not isinstance(baseline, dict):
        return None

    reviewed_fp = reviewed.get("fingerprint")
    baseline_fp = baseline.get("snapshot_fingerprint")

    if (
        isinstance(reviewed_fp, str)
        and reviewed_fp
        and isinstance(baseline_fp, str)
        and baseline_fp
    ):
        return reviewed_fp != baseline_fp

    # Safe compatibility fallback for tasks created before fingerprint support:
    # a clean baseline + identical HEAD + still-clean reviewed worktree proves no-op.
    if (
        baseline.get("clean") is True
        and reviewed.get("head") == baseline.get("head")
        and str(reviewed.get("status") or "").rstrip("\n") == ""
    ):
        return False

    # Dirty legacy baselines cannot be proven unchanged from porcelain alone.
    return None


def send(text, opener=None, sleep=time.sleep):
    """True=accepted, False=known undelivered, None=acceptance uncertain.

    The normal transport runs in a killable subprocess with a wall-clock bound;
    even a server trickling response bytes cannot keep the bridge blocked.
    """
    token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat:
        return False
    if opener is None:
        try:
            result = subprocess.run([sys.executable, os.path.abspath(__file__)], input=text,
                                    text=True, capture_output=True, timeout=35)
            return {'SENT': True, 'REJECTED': False}.get(result.stdout.strip())
        except (OSError, subprocess.TimeoutExpired):
            return None
    request = urllib.request.Request('https://api.telegram.org/bot' + token + '/sendMessage',
        data=urllib.parse.urlencode({'chat_id': chat, 'text': redact(text)[:4000]}).encode())
    for attempt in range(3):
        try:
            with opener(request, timeout=10) as response:
                result = json.loads(response.read(65536))
                if result.get('ok') is True:
                    return True
                if result.get('ok') is not False:
                    return None
                if result.get('error_code') not in (429, 500, 502, 503, 504):
                    return False
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 401, 403, 404):
                return False
            if exc.code != 429:
                # A gateway/server error after forwarding the POST can hide acceptance.
                return None
        except (OSError, ValueError):
            # Telegram may have accepted the POST before a timeout/broken response.
            return None
        except Exception:
            # Notification failures must never prevent task processing; do not log token-bearing URLs.
            return None
        if attempt < 2:
            sleep(0.5 * (2 ** attempt))
    return False


def terminal_text(status, issue, item, report=None, blocker=None, *, current_action=None):
    report = report or {}
    if status == 'REVIEW_PASSED / READY_FOR_OWNER':
        if not review_approval_complete(item):
            status = 'FRESH_REVIEW_REQUIRED'
            blocker = 'Historical review PASS lacks owner approval or reviewed-tree evidence. Send a trusted follow-up for a fresh independent review before publication.'
        elif item['owner_state'] != 'READY_FOR_OWNER':
            status = item['owner_state']
    no_publication_required = (
        status == 'REVIEW_PASSED / READY_FOR_OWNER'
        and publication_delta_present(item, report) is False
    )
    if no_publication_required:
        status = 'REVIEW_PASSED / COMPLETE'

    lines = [status, str(issue.get('title') or f"Task #{issue['number']}")[:200],
             f"Project: {item.get('project', '?')}",
             f"Coding agent: {item.get('coding_agent') or report.get('agent', '?')}"]
    next_action = {"REVIEW_PASSED / READY_FOR_OWNER": "COMMIT", "COMMITTED": "PUSH",
                   "PUSHED": "CREATE_PR", "PR_CREATED": "MERGE", "NEEDS_OWNER": "ANSWER",
                   "FRESH_REVIEW_REQUIRED": "FOLLOWUP_FOR_FRESH_REVIEW"}.get(status)
    if status == 'NEEDS_OWNER' and (item.get('worker_uncertain') or item.get('review_uncertain')):
        next_action = 'INSPECT_WORKER'
    if current_action is not None:
        next_action = None if current_action == 'NONE' else current_action
    if next_action:
        lines.append("Next owner action: " + next_action)
    if no_publication_required:
        lines.append("Publication: NOT_REQUIRED (no repository delta)")
    values = [('Model', report.get('model') or item.get('model')),
              ('Effort', report.get('reasoning_effort') or item.get('effort')),
              ('Reviewer', item.get('reviewer_agent')), ('Review round', item.get('review_round')),
              ('Branch', item.get('publication_branch') or (item.get('reviewed_tree') or {}).get('branch')
               or (report.get('git_baseline') or {}).get('branch')),
              ('Commit', item.get('commit_sha')), ('PR', item.get('pr_url'))]
    health = report.get('liveness')
    if isinstance(health, dict):
        values.extend([
            ('Runtime', f"{health.get('state')}: {health.get('reason')}"),
            ('Task', health.get('task_id')), ('Phase', health.get('phase')),
            ('Lease', health.get('lease_state')), ('Heartbeat age (seconds)', health.get('heartbeat_age_seconds')),
            ('Worker/process owned', f"{health.get('worker_alive')}/{health.get('process_alive')}"),
        ])
    created = report.get('created_at')
    if created:
        try:
            duration = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(created)).total_seconds()))
            values.append(('Duration', f'{duration // 60}m {duration % 60}s'))
        except (ValueError, TypeError):
            pass
    # Machine contract is a reliable verification summary; avoid heuristically claiming tests passed.
    for line in str(report.get('result') or '').splitlines():
        if line.startswith('DEVORCHESTRATOR_RESULT:'):
            try:
                data = json.loads(line.split(':', 1)[1])
                if isinstance(data, dict):
                    values.append(('Verification', data.get('verification')))
                    values.append(('Summary', data.get('summary')))
            except (ValueError, TypeError):
                pass
    lines.extend(f'{label}: {str(value)[:200]}' for label, value in values if value)
    if blocker:
        label = 'Review hold: ' if status == 'FRESH_REVIEW_REQUIRED' else 'Owner question: '
        lines.append(label + str(blocker)[:1000])
    if issue.get('url'):
        lines.append(str(issue['url'])[:300])
    if item.get('task_id'):
        lines.append('Task: ' + item['task_id'])
    return '\n'.join(lines)[:4000]


if __name__ == '__main__':
    result = send(sys.stdin.read(), opener=urllib.request.urlopen)
    print('SENT' if result is True else 'REJECTED' if result is False else 'UNCERTAIN')
