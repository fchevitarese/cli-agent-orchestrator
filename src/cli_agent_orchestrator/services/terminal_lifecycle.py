"""In-process coordination for terminal runtime operations and teardown.

The coordinator is deliberately I/O-free.  It is the application-side barrier
between operations that use a live terminal runtime and the teardown use case;
backend, provider, FIFO, and database cleanup remain in their existing adapters.
"""

import threading
import time
from contextlib import contextmanager
from typing import Iterator


class TerminalTeardownInProgressError(RuntimeError):
    """Raised when an operation targets a runtime currently stopping."""


class TerminalLifecycleCoordinator:
    """Gate new runtime operations and drain operations already in flight."""

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._inflight: dict[str, int] = {}
        self._stopping: set[str] = set()

    def register_terminal(self, terminal_id: str) -> None:
        """Register a fresh runtime incarnation and clear an old tombstone."""
        with self._condition:
            self._stopping.discard(terminal_id)
            self._inflight.pop(terminal_id, None)
            self._condition.notify_all()

    @contextmanager
    def operation(self, terminal_id: str) -> Iterator[None]:
        """Hold a lease while code interacts with a live terminal runtime."""
        with self._condition:
            if terminal_id in self._stopping:
                raise TerminalTeardownInProgressError(
                    f"Terminal {terminal_id} is stopping; new operations are blocked"
                )
            self._inflight[terminal_id] = self._inflight.get(terminal_id, 0) + 1
        try:
            yield
        finally:
            with self._condition:
                remaining = self._inflight.get(terminal_id, 0) - 1
                if remaining > 0:
                    self._inflight[terminal_id] = remaining
                else:
                    self._inflight.pop(terminal_id, None)
                self._condition.notify_all()

    def start_teardown(self, terminal_id: str) -> bool:
        """Move the runtime to STOPPING; return whether this caller owns teardown."""
        with self._condition:
            if terminal_id in self._stopping:
                return False
            self._stopping.add(terminal_id)
            self._condition.notify_all()
            return True

    def wait_for_idle(self, terminal_id: str, timeout: float) -> bool:
        """Wait until all leases for ``terminal_id`` have been released."""
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._inflight.get(terminal_id, 0) > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def wait_until_stopped(self, terminal_id: str, timeout: float) -> bool:
        """Wait for the teardown owner to publish completion."""
        deadline = time.monotonic() + timeout
        with self._condition:
            while terminal_id in self._stopping:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return terminal_id not in self._stopping

    def finish_teardown(self, terminal_id: str) -> None:
        """Publish teardown completion and release coordinator bookkeeping."""
        with self._condition:
            self._stopping.discard(terminal_id)
            self._inflight.pop(terminal_id, None)
            self._condition.notify_all()


terminal_lifecycle = TerminalLifecycleCoordinator()
