"""Durable external-effects outbox. GitHub comments carry stable deduplication markers."""
import hashlib
import json
import time


class CommentNotDispatched(Exception):
    """Trusted writer-local proof that the comment POST boundary was not crossed.

    Raise only around pre-dispatch work, never around the external call. An
    arbitrary transport exception (including BlockingIOError) is not this proof.
    """


def comment_matches(entry, key, observations):
    expected = entry['payload'] + '\n\n' + f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
    if entry['kind'] == 'chief_comment':
        return [c for c in observations if c.get('body') == expected]
    from chief_review import is_evidence_comment
    return [c for c in observations if not is_evidence_comment(c.get('body')) and (
        c.get('body') == expected or ('Task started:' in entry['payload'] and
        (c.get('body') or '').split('\n\n<!-- DEVORCHESTRATOR_EFFECT_V1 ')[0] == entry['payload']))]


def enqueue(state, number, kind, payload, event=None):
    from external_effects import blocked
    if blocked() and isinstance(state.get('issues', {}).get(str(number)), dict):
        hold_observation(state, number)
    digest = hashlib.sha256(json.dumps([str(number), kind, event or payload], sort_keys=True).encode()).hexdigest()
    previous = state.get('outbox', {}).get(digest)
    if previous and (previous.get('status') == 'QUARANTINED' or
                     previous.get('effect_origin') in ('safety_latch', 'legacy_unverified')):
        return digest  # A repeated desire cannot become a new authorization.
    held = blocked() or observation_held(state, number)
    if held:
        # A tombstone is durable even after the environment latch is removed.
        # Never supersede or reauthorize an existing legitimate operation.
        state.setdefault('outbox', {}).setdefault(digest, dict(
            number=number, kind=kind, payload=payload, status='QUARANTINED',
            attempts=0, next_attempt=0, delivery_version=2,
            effect_origin='safety_latch', diagnostic='Fresh authorization required'))
        return digest
    if kind == 'transition':
        for existing_key, existing in state.get('outbox', {}).items():
            if (existing_key != digest and str(existing['number']) == str(number) and
                    existing['kind'] == 'transition' and existing['status'] not in ('DONE', 'QUARANTINED')):
                existing['status'] = 'DONE'  # A newer desired state supersedes undelivered older labels.
    state.setdefault('outbox', {}).setdefault(digest, dict(number=number, kind=kind, payload=payload,
                                                         status='PENDING', attempts=0, next_attempt=0,
                                                         delivery_version=2, effect_origin='runtime'))
    return digest


def observation_held(state, number):
    item = state.get('issues', {}).get(str(number), {})
    return isinstance(item, dict) and bool(item.get('safety_latch_hold'))


def hold_observation(state, number):
    """Writer-local, sticky revocation; removal of the env latch is not approval.

    V1 entries contain no evidence distinguishing legitimate pending work from
    a prior smoke. Retain affected entries for audit, never delete them or guess
    their authority. Explicit runtime-origin entries and other issues survive.
    """
    item = state['issues'][str(number)]
    changed = not item.get('safety_latch_hold')
    item.setdefault('safety_latch_hold', dict(reason='Read-only observation; fresh authorization required'))
    for entry in state.get('outbox', {}).values():
        if (str(entry['number']) == str(number) and entry.get('status') == 'PENDING' and
                not entry.get('effect_origin') and
                entry.get('completion_comment') not in ('SENDING', 'UNCERTAIN', 'DONE')):
            entry.update(status='QUARANTINED', effect_origin='legacy_unverified',
                         diagnostic='Origin unavailable; retained for authorization audit')
            changed = True
    return changed


def _withheld(entry):
    """A new smoke-blocked effect has not crossed the send boundary.

    Crash receipts already marked SENDING or UNCERTAIN stay on the exact
    reconciliation path. Completion uses that marker on the entry itself.
    """
    from external_effects import blocked
    if not blocked() or entry.get('status') != 'PENDING':
        return False
    if entry.get('kind') == 'completion' and entry.get('completion_comment') in ('SENDING', 'UNCERTAIN', 'DONE'):
        return False
    return entry.get('kind') in ('comment', 'chief_comment', 'telegram', 'transition', 'completion')


def _flush_steps(state, persist, comments, post_comment, transition, telegram, now=None, eligible=None, complete=None, telegram_dispatch=None, telegram_inflight=(), selection=None, render_telegram=None):
    from external_effects import ExternalEffectBlocked, blocked
    if blocked():
        changed = False
        for key, entry in list(state.get('outbox', {}).items()):
            if selection is not None and key not in selection:
                continue
            item = state.get('issues', {}).get(str(entry['number']), {})
            if (isinstance(item, dict) and (item.get('task_id') or item.get('review_task_id')) and
                    entry.get('status') == 'PENDING' and not entry.get('effect_origin')):
                changed = hold_observation(state, entry['number']) or changed
        if changed:
            persist()  # Legacy migration is durable even through the generic flush API.
    now = time.time() if now is None else now
    started = time.monotonic()
    deliveries = 0
    # Prioritize labels and launch receipts within the existing bounded batch.
    entries = list(state.get('outbox', {}).items())
    entries.sort(key=lambda pair: 0 if pair[1]['kind'] == 'transition' else
                 1 if pair[1]['kind'] == 'comment' and 'Task started:' in pair[1]['payload'] else 2)
    for key, entry in entries:
        if entry.get('status') == 'QUARANTINED' or entry.get('effect_origin') in ('safety_latch', 'legacy_unverified'):
            continue  # No callbacks, retries or receipt-reset paths can authorize this entry.
        if selection is not None and key not in selection:
            continue
        comment = entry['kind'] in ('comment', 'chief_comment')
        reconcile_comment = comment and (entry['status'] in ('SENDING', 'UNCERTAIN') or
            (entry['kind'] == 'comment' and entry['status'] == 'PENDING' and entry.get('delivery_version') != 2))
        if not reconcile_comment and (entry['status'] in ('DONE', 'UNCERTAIN', 'FAILED') or entry.get('next_attempt', 0) > now):
            continue
        if entry['kind'] == 'telegram' and entry['status'] == 'SENDING' and key in telegram_inflight:
            continue
        if entry['kind'] == 'telegram' and entry['status'] == 'SENDING':
            # A crash crossed the send boundary: acceptance cannot be queried by key.
            entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
            persist()
            continue
        if deliveries >= 5 or time.monotonic() - started >= 5:
            break
        if eligible is not None and not (yield ('eligible', (entry,))):
            continue
        deliveries += 1
        if reconcile_comment:
            expected = entry['payload'] + '\n\n' + f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
            try:
                matches = comment_matches(entry, key, (yield ('comments', (entry['number'],))))
                entry['status'] = 'DONE' if len(matches) == 1 else 'UNCERTAIN'
            except ExternalEffectBlocked:
                raise
            except Exception:
                entry['status'] = 'UNCERTAIN'
            persist()
            continue
        if comment:
            expected = entry['payload'] + '\n\n' + f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
            try:
                matches = comment_matches(entry, key, (yield ('comments', (entry['number'],))))
            except ExternalEffectBlocked:
                raise
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
        if _withheld(entry):
            # The latch is proof the transport will not be entered. Leave the
            # retryable receipt where it is: no SENDING checkpoint, no attempt.
            continue
        if entry['kind'] == 'telegram':
            # Rendering is writer-local and occurs only after backoff/freshness
            # checks. Persist the stable event payload, never this send buffer.
            text = render_telegram(entry) if render_telegram is not None else entry['payload']
        if entry['kind'] in ('telegram', 'chief_comment', 'comment'):
            entry['status'] = 'SENDING'
            persist()  # Must be durable before the POST; persistence errors must stop delivery.
        result = None
        try:
            if comment:
                (yield ('post_comment', (entry['number'], expected)))
            elif entry['kind'] == 'transition':
                (yield ('transition', (entry['payload']['issue'], entry['payload']['target'])))
            elif entry['kind'] == 'completion':
                (yield ('complete', (entry['number'], key)))
            elif entry['kind'] == 'telegram':
                if telegram_dispatch is not None:
                    (yield ('telegram_dispatch', (key, text)))
                    continue  # SENDING is durable; the single writer applies acceptance later.
                result = (yield ('telegram', (text,)))
                if result is None:
                    entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
                    persist()
                    continue
                if not result:
                    raise OSError('Telegram unavailable')
            else:
                raise ValueError('Unknown external effect')
            entry['status'] = 'DONE'
        except ExternalEffectBlocked:
            # Raised before the transport. A SENDING checkpoint from this same
            # attempt is unsent and returns to the retryable receipt.
            if entry.get('status') == 'SENDING':
                entry['status'] = 'PENDING'
        except CommentNotDispatched:
            if not comment:
                raise
            # A durable no-send result permits retry. If this checkpoint fails
            # or a crash precedes it, SENDING still reconciles conservatively.
            entry['attempts'] += 1
            entry.update(status='PENDING',
                next_attempt=now + min(300, 2 ** min(entry['attempts'], 8)),
                diagnostic='Comment not dispatched; delivery deferred')
        except Exception:
            # Never persist exception messages (URLs/tokens/credentials may appear in them).
            entry['attempts'] += 1
            entry['next_attempt'] = now + min(300, 2 ** min(entry['attempts'], 8))
            if comment:
                entry.update(status='UNCERTAIN', diagnostic='Comment acceptance uncertain; exact receipt reconciliation required')
            if entry['kind'] == 'telegram':
                # A thrown callback may have sent; a known False is safe to retry.
                if result is not False:
                    entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
                else:
                    entry['status'] = 'FAILED' if entry['attempts'] >= 5 else 'PENDING'
        persist()



def flush(state, persist, comments, post_comment, transition, telegram, now=None, eligible=None, complete=None,
          telegram_dispatch=None, telegram_inflight=(), render_telegram=None):
    callbacks = dict(comments=comments, post_comment=post_comment, transition=transition, telegram=telegram,
                     eligible=eligible, complete=lambda number, key: complete(number), telegram_dispatch=telegram_dispatch)
    steps = _flush_steps(state, persist, comments, post_comment, transition, telegram, now, eligible, complete,
                         telegram_dispatch, telegram_inflight, render_telegram=render_telegram)
    value, error = None, None
    while True:
        try:
            name, args = steps.throw(error) if error is not None else steps.send(value)
        except StopIteration as result:
            return result.value
        try:
            value, error = callbacks[name](*args), None
        except Exception as exc:
            value, error = None, exc


async def flush_async(state, persist, comments, post_comment, transition, telegram, now=None, eligible=None,
                      complete=None, telegram_dispatch=None, telegram_inflight=(), selection=None, render_telegram=None):
    """Callbacks execute on the lifecycle writer and may yield copied I/O commands."""
    callbacks = dict(comments=comments, post_comment=post_comment, transition=transition, telegram=telegram,
                     eligible=eligible, complete=complete)
    async def dispatch(key, text):
        return telegram_dispatch(key, text)
    callbacks['telegram_dispatch'] = dispatch
    steps = _flush_steps(state, persist, comments, post_comment, transition, telegram, now, eligible, complete,
                         telegram_dispatch, telegram_inflight, selection, render_telegram)
    value, error = None, None
    while True:
        try:
            name, args = steps.throw(error) if error is not None else steps.send(value)
        except StopIteration as result:
            return result.value
        try:
            value, error = await callbacks[name](*args), None
        except Exception as exc:
            value, error = None, exc
