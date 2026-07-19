"""Deterministic concurrency tests for terminal lifecycle coordination."""

import threading

import pytest

from cli_agent_orchestrator.services.terminal_lifecycle import (
    TerminalLifecycleCoordinator,
    TerminalTeardownInProgressError,
)


def test_teardown_blocks_new_operations_and_waits_for_existing_lease():
    lifecycle = TerminalLifecycleCoordinator()
    lifecycle.register_terminal("t1")
    operation_started = threading.Event()
    release_operation = threading.Event()

    def _operation() -> None:
        with lifecycle.operation("t1"):
            operation_started.set()
            assert release_operation.wait(timeout=1)

    worker = threading.Thread(target=_operation)
    worker.start()
    assert operation_started.wait(timeout=1)

    assert lifecycle.start_teardown("t1") is True
    with pytest.raises(TerminalTeardownInProgressError):
        with lifecycle.operation("t1"):
            pass
    assert lifecycle.wait_for_idle("t1", timeout=0.01) is False

    release_operation.set()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert lifecycle.wait_for_idle("t1", timeout=1) is True
    lifecycle.finish_teardown("t1")


def test_only_one_concurrent_caller_owns_teardown():
    lifecycle = TerminalLifecycleCoordinator()
    lifecycle.register_terminal("t1")

    assert lifecycle.start_teardown("t1") is True
    assert lifecycle.start_teardown("t1") is False

    completed = threading.Event()

    def _waiter() -> None:
        assert lifecycle.wait_until_stopped("t1", timeout=1) is True
        completed.set()

    waiter = threading.Thread(target=_waiter)
    waiter.start()
    assert not completed.wait(timeout=0.01)

    lifecycle.finish_teardown("t1")
    waiter.join(timeout=1)

    assert completed.is_set()


def test_fresh_registration_after_teardown_accepts_operations():
    lifecycle = TerminalLifecycleCoordinator()
    assert lifecycle.start_teardown("t1") is True
    lifecycle.finish_teardown("t1")

    lifecycle.register_terminal("t1")

    with lifecycle.operation("t1"):
        pass
    assert lifecycle.wait_for_idle("t1", timeout=0) is True
