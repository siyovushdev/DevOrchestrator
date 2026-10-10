#!/usr/bin/env python3
"""One-shot capability check for a newly started MCP. No other tool can be called.

This client has no tool argument. It calls get_transition_capabilities and
compares the object. It does not import TaskBridge and does not start a worker,
publication, or GitHub mutation. Run it only after the new MCP is listening and
only with an explicit URL.
"""
import argparse
import asyncio
import json


TOOL = 'get_transition_capabilities'
EXPECTED = {'schema_version': 1, 'review_binding': 1, 'start_request_reconciliation': 1}


def require_tool(name):
    if (name,) != (TOOL,):
        raise SystemExit('capability probe refuses any tool other than get_transition_capabilities')


def decode_object(result):
    if getattr(result, 'is_error', False) or getattr(result, 'isError', False):
        raise SystemExit('capability tool returned an error')
    content = getattr(result, 'content', None)
    texts = [getattr(block, 'text', None) for block in content or ()]
    if len(texts) != 1 or not isinstance(texts[0], str):
        raise SystemExit('capability tool must return exactly one JSON object')
    value = json.loads(texts[0])
    if value != EXPECTED:
        raise SystemExit('capability object mismatch')
    return value


async def probe(url):
    require_tool(TOOL)
    from mcp import Client
    async with Client(url) as client:
        result = await client.call_tool(TOOL, {})
    return decode_object(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True, help='MCP endpoint, for example http://127.0.0.1:8787/mcp')
    args = parser.parse_args()
    if not args.url.startswith('http://127.0.0.1:') and not args.url.startswith('http://localhost:'):
        raise SystemExit('refusing a non-loopback MCP URL')
    print(json.dumps(asyncio.run(probe(args.url)), sort_keys=True))


if __name__ == '__main__':
    main()
