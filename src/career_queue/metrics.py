import threading
from collections import Counter


class Metrics:
    """Tiny thread-safe counters. Swap for Prometheus later if needed."""

    def __init__(self):
        self._c = Counter()
        self._lock = threading.Lock()

    def inc(self, name: str, n: int = 1):
        with self._lock:
            self._c[name] += n

    def get(self, name: str) -> int:
        with self._lock:
            return self._c[name]

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._c)
