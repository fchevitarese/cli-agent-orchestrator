"""Cross-service teardown concurrency regressions."""

import threading
from unittest.mock import MagicMock, patch

import pytest

from cli_agent_orchestrator.services import terminal_service
from cli_agent_orchestrator.services.terminal_lifecycle import TerminalLifecycleCoordinator


def test_delete_blocks_new_input_and_waits_for_input_already_inflight(tmp_path):
    lifecycle = TerminalLifecycleCoordinator()
    lifecycle.register_terminal("t1")
    send_started = threading.Event()
    release_send = threading.Event()
    teardown_started = threading.Event()
    thread_errors: list[BaseException] = []
    delete_result: list[bool] = []

    metadata = {
        "id": "t1",
        "tmux_session": "cao-session",
        "tmux_window": "worker-one",
        "provider": "claude_code",
        "agent_profile": "developer",
        "allowed_tools": None,
        "caller_id": None,
    }
    backend = MagicMock()
    backend.get_history.return_value = "scrollback"
    backend.get_pane_working_directory.return_value = str(tmp_path)

    def _blocking_send(*_args, **_kwargs) -> None:
        send_started.set()
        assert release_send.wait(timeout=2)

    backend.send_keys.side_effect = _blocking_send
    provider = MagicMock()
    provider.paste_enter_count = 1
    provider.paste_submit_delay = 0.0
    provider.blocks_orchestrated_input_while_waiting_user_answer = False
    provider_manager = MagicMock()
    provider_manager.get_provider.return_value = provider
    status_monitor = MagicMock()
    status_monitor.wait_for_terminal_idle.return_value = True
    status_monitor.clear_terminal.side_effect = lambda _terminal_id: teardown_started.set()

    def _send() -> None:
        try:
            terminal_service.send_input("t1", "hello")
        except BaseException as exc:  # pragma: no cover - assertion reports below
            thread_errors.append(exc)

    def _delete() -> None:
        try:
            delete_result.append(terminal_service.delete_terminal("t1"))
        except BaseException as exc:  # pragma: no cover - assertion reports below
            thread_errors.append(exc)

    with (
        patch.object(terminal_service, "terminal_lifecycle", lifecycle),
        patch.object(terminal_service, "status_monitor", status_monitor),
        patch.object(terminal_service, "provider_manager", provider_manager),
        patch.object(terminal_service, "get_terminal_metadata", return_value=metadata),
        patch.object(terminal_service, "get_backend", return_value=backend),
        patch.object(terminal_service, "fifo_manager"),
        patch.object(terminal_service, "db_delete_terminal", return_value=True),
        patch.object(terminal_service, "get_herdr_inbox_service", return_value=None),
        patch.object(terminal_service, "inject_memory_context", side_effect=lambda msg, _tid: msg),
        patch.object(terminal_service, "update_last_active"),
        patch.object(terminal_service, "TERMINAL_LOG_DIR", tmp_path),
    ):
        sender = threading.Thread(target=_send)
        sender.start()
        assert send_started.wait(timeout=1)

        deleter = threading.Thread(target=_delete)
        deleter.start()
        assert teardown_started.wait(timeout=1)

        # Teardown fenced the runtime but cannot remove the provider/DB until
        # the send operation that already held a lease has returned.
        provider_manager.cleanup_provider.assert_not_called()
        with pytest.raises(terminal_service.TerminalInputBlockedError):
            terminal_service.send_input("t1", "too late")

        release_send.set()
        sender.join(timeout=2)
        deleter.join(timeout=2)

    assert not sender.is_alive()
    assert not deleter.is_alive()
    assert thread_errors == []
    assert delete_result == [True]
    provider_manager.cleanup_provider.assert_called_once_with("t1")
