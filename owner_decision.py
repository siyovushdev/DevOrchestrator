"""Quality disposition only; never grants a publication capability."""
import hashlib
import json
import re
import chief_review

MARKER='DEVORCHESTRATOR_OWNER_DECISION_V1'

def parse(body):
    if not isinstance(body,str) or MARKER not in body:return None
    match=re.fullmatch(r'\s*<!-- '+MARKER+r' (\{.*\}) -->\s*',body,re.S)
    if not match:raise ValueError('Invalid owner decision envelope')
    def unique(pairs):
        value={}
        for k,v in pairs:
            if k in value:raise ValueError('Duplicate owner decision field')
            value[k]=v
        return value
    value=json.loads(match[1],object_pairs_hook=unique)
    if set(value)!={'decision','bundle_id'} or value['decision'] not in ('APPROVE','REJECT'):
        raise ValueError('Owner decision is APPROVE/REJECT only; no publication permissions')
    if not isinstance(value['bundle_id'],str) or not re.fullmatch('[0-9a-f]{64}',value['bundle_id']):
        raise ValueError('Bound Chief bundle required')
    return value

def apply(item,body,comment_id):
    from external_effects import refuse
    refuse('owner_decision')
    value=parse(body)
    if value is None:return None
    if not chief_review.approval_complete(item) or item.get('owner_state')!='READY_FOR_OWNER':
        raise ValueError('Current Chief PASS and READY_FOR_OWNER required')
    bundle=chief_review.build_identity(item).bundle_id
    if bundle!=value['bundle_id']:raise ValueError('Stale owner decision binding')
    fingerprint=hashlib.sha256(body.encode()).hexdigest();key=str(comment_id)
    history=item.setdefault('owner_decisions',{})
    if key in history:
        if history[key]['body_sha256']!=fingerprint:raise ValueError('Owner decision ID content changed')
        return 'REPLAY'
    row={**value,'body_sha256':fingerprint,'comment_id':key,'publication_permissions':[]}
    history[key]=row;item['owner_decision']=row
    return 'RECORDED'

def publication_allowed(item):
    decision=item.get('owner_decision')
    if decision is None:return True  # Preserve legacy explicit per-action authorization.
    try:return (decision['decision']=='APPROVE' and decision['bundle_id']==chief_review.build_identity(item).bundle_id)
    except (KeyError,ValueError,TypeError):return False
