"""Owner-mediated ChatGPT provenance validation; no model calls or publication.

The operator supplies the exact response and attests the conversation reference.
This is not cryptographic ChatGPT authentication. The accepted native reducer
still validates current tree, turn, review round and bundle on intake.
"""
import hashlib
from urllib.parse import urlsplit
import chief_review

def validate(intake,bundle,response,expected_bundle_id):
    if intake.get('source')!='OWNER_SUPPLIED_CHATGPT_EXPORT' or intake.get('owner_attested_real_chatgpt') is not True:
        raise ValueError('Owner-attested real ChatGPT source required')
    ref=urlsplit(intake.get('conversation_reference',''))
    if ref.scheme!='https' or ref.hostname!='chatgpt.com' or '/c/' not in ref.path or ref.username or ref.password or ref.fragment:
        raise ValueError('Actual ChatGPT conversation URL required')
    if intake.get('bundle_sha256')!=hashlib.sha256(bundle).hexdigest() or intake.get('response_sha256')!=hashlib.sha256(response).hexdigest():
        raise ValueError('Chief bundle/response digest mismatch')
    verdict=chief_review.parse_verdict(response.decode())
    if not verdict or verdict['bundle_id']!=expected_bundle_id or intake.get('bundle_id')!=expected_bundle_id:
        raise ValueError('Chief verdict binding mismatch')
    return verdict
