"""Bounded monotonic timings; fixed names and numeric values only."""
from contextlib import contextmanager
import math
import time

NAMES = frozenset({'control_total', 'github_command_read', 'targeted_control',
    'heavy_total', 'worker_reconciliation', 'tracked_issue', 'chief_evidence',
    'outbox_flush', 'telegram_callback', 'publication_action'})
LIMIT = 64


def record(state, name, seconds):
    if name not in NAMES or not isinstance(seconds, (int, float)) or not math.isfinite(seconds):
        return
    history = state.setdefault('timings', {}).setdefault('samples', [])
    history.append({'stage': name, 'seconds': round(max(0, seconds), 6)})
    del history[:-LIMIT]


@contextmanager
def measure(state, name):
    started = time.monotonic()
    try:
        yield
    finally:
        record(state, name, time.monotonic() - started)
