"""Durable external-effects outbox. GitHub comments carry stable deduplication markers."""
import hashlib
import json
import time


def enqueue(state, number, kind, payload, event=None):
    digest = hashlib.sha256(json.dumps([str(number), kind, event or payload], sort_keys=True).encode()).hexdigest()
    if kind == 'transition':
        for existing_key, existing in state.get('outbox', {}).items():
            if existing_key != digest and str(existing['number']) == str(number) and existing['kind'] == 'transition' and existing['status'] != 'DONE':
                existing['status'] = 'DONE'  # A newer desired state supersedes undelivered older labels.
    state.setdefault('outbox', {}).setdefault(digest, dict(number=number, kind=kind, payload=payload,
                                                         status='PENDING', attempts=0, next_attempt=0))
    return digest


def flush(state, persist, comments, post_comment, transition, telegram, now=None, eligible=None):
    now = time.time() if now is None else now
    started = time.monotonic()
    deliveries = 0
    for key, entry in list(state.get('outbox', {}).items()):
        chief_reconcile = entry['kind'] == 'chief_comment' and entry['status'] in ('SENDING', 'UNCERTAIN')
        if not chief_reconcile and (entry['status'] in ('DONE', 'UNCERTAIN', 'FAILED') or entry.get('next_attempt', 0) > now):
            continue
        if entry['kind'] == 'telegram' and entry['status'] == 'SENDING':
            # A crash crossed the send boundary: acceptance cannot be queried by key.
            entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
            persist()
            continue
        if eligible is not None and not eligible(entry):
            continue
        if deliveries >= 5 or time.monotonic() - started >= 5:
            break
        deliveries += 1
        if chief_reconcile:
            expected = entry['payload'] + '\n\n' + f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
            try:
                matches = [c for c in comments(entry['number']) if c.get('body') == expected]
                entry['status'] = 'DONE' if len(matches) == 1 else 'UNCERTAIN'
            except Exception:
                entry['status'] = 'UNCERTAIN'
            persist()
            continue
        if entry['kind'] == 'chief_comment':
            expected = entry['payload'] + '\n\n' + f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
            try:
                matches = [c for c in comments(entry['number']) if c.get('body') == expected]
            except Exception:
                # No POST was attempted: a failed preflight read is safe to retry.
                entry['attempts'] += 1
                entry['next_attempt'] = now + min(300, 2 ** min(entry['attempts'], 8))
                persist()
                continue
            if matches:
                entry['status'] = 'DONE' if len(matches) == 1 else 'UNCERTAIN'
                persist()
                continue
        if entry['kind'] in ('telegram', 'chief_comment'):
            entry['status'] = 'SENDING'
            persist()  # Must be durable before the POST; persistence errors must stop delivery.
        result = None
        try:
            if entry['kind'] == 'chief_comment':
                post_comment(entry['number'], expected)
            elif entry['kind'] == 'comment':
                marker = f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
                from chief_review import is_evidence_comment
                if not any(marker in (c.get('body') or '') and not is_evidence_comment(c.get('body'))
                           for c in comments(entry['number'])):
                    post_comment(entry['number'], entry['payload'] + '\n\n' + marker)
            elif entry['kind'] == 'transition':
                transition(entry['payload']['issue'], entry['payload']['target'])
            elif entry['kind'] == 'telegram':
                result = telegram(entry['payload'])
                if result is None:
                    entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
                    persist()
                    continue
                if not result:
                    raise OSError('Telegram unavailable')
            else:
                raise ValueError('Unknown external effect')
            entry['status'] = 'DONE'
        except Exception:
            # Never persist exception messages (URLs/tokens/credentials may appear in them).
            entry['attempts'] += 1
            entry['next_attempt'] = now + min(300, 2 ** min(entry['attempts'], 8))
            if entry['kind'] == 'chief_comment':
                entry.update(status='UNCERTAIN', diagnostic='Chief evidence acceptance uncertain; exact receipt reconciliation required')
            if entry['kind'] == 'telegram':
                # A thrown callback may have sent; a known False is safe to retry.
                if result is not False:
                    entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
                else:
                    entry['status'] = 'FAILED' if entry['attempts'] >= 5 else 'PENDING'
        persist()
