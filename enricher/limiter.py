"""Rate limiting for MusicBrainz requests and pipeline serialization."""

import threading
import time

from . import config


class MBRateLimiter:
    """Ensures at least ``min_interval`` seconds between successive requests.

    Uses the monotonic clock, so wall-clock jumps (NTP, DST) cannot shrink the
    enforced gap. Thread-safe: the wait lock serializes callers, and the sleep
    happens while holding it so two callers can never both pass the check.
    """

    def __init__(self, min_interval=None):
        self._min_interval = min_interval if min_interval is not None else config.MB_RATE_LIMIT_SECONDS
        self._lock = threading.Lock()
        self._last = None  # monotonic() timestamp of the last request; None before the first

    def wait(self) -> None:
        """Block until the minimum interval has elapsed since the last call.

        The first call returns immediately.
        """
        with self._lock:
            now = time.monotonic()
            if self._last is not None:
                gap = self._last + self._min_interval - now
                if gap > 0:
                    time.sleep(gap)
                    now = time.monotonic()
            self._last = now


# The submit pipeline lock. The POST /1/submit-listens handler holds it across
# [enrich every payload item -> forward to Koito -> post-forward delay], so MB
# queries and Koito's downstream MusicBrainz queries never overlap with the
# middleware's next query.
pipeline = threading.Lock()
