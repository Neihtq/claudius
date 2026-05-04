import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
from claudius.session.server import create_session_app

def _runner_mock():
    mock = MagicMock()
    mock.is_done = False
    mock.inject_message = MagicMock()
    mock.graceful_stop = AsyncMock(return_value=True)
    return mock

def test_health_returns_ok():
    runner = _runner_mock()
    app = create_session_app(runner=runner, session_id="sess-1")
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

def test_message_injects_into_runner():
    runner = _runner_mock()
    app = create_session_app(runner=runner, session_id="sess-1")
    client = TestClient(app)
    payload = {
        "channel": "email",
        "sender": "user@example.com",
        "thread_id": "thread-1",
        "subject": None,
        "body": "Here is my reply",
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    resp = client.post("/message", json=payload)
    assert resp.status_code == 200
    runner.inject_message.assert_called_once()
    injected = runner.inject_message.call_args[0][0]
    assert injected.body == "Here is my reply"


def test_shutdown_requires_auth_when_token_configured():
    runner = _runner_mock()
    app = create_session_app(runner=runner, session_id="sess-1", session_token="token-123")
    client = TestClient(app)

    resp = client.post("/shutdown")

    assert resp.status_code == 401
    runner.graceful_stop.assert_not_awaited()


def test_shutdown_calls_runner_graceful_stop():
    runner = _runner_mock()
    app = create_session_app(runner=runner, session_id="sess-1", session_token="token-123")
    client = TestClient(app)

    resp = client.post("/shutdown", headers={"Authorization": "Bearer token-123"})

    assert resp.status_code == 200
    assert resp.json() == {"status": "stopping", "graceful": True}
    runner.graceful_stop.assert_awaited_once()
