"""util.py.

Utilities for the bufferq module.
"""
import time

def diff_time() -> float:
    """Return a time suitable for 'timeout' comparisons."""
    return time.monotonic()
