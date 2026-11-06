import threading
import time


class SystemClock:
    """Wall-clock time."""

    virtual = False

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(max(0.0, seconds))


class FakeClock:
    """Manually advanced time, so tests can cover retries, leases, and timers without real waits."""

    virtual = True

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += max(0.0, seconds)

    def advance_to(self, t: float) -> None:
        with self._lock:
            self._now = max(self._now, t)

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)
