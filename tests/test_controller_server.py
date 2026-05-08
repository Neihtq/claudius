import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
from claudius.controller.attachments import LocalAttachmentStore
from claudius.controller.server import create_controller_app
from claudius.controller.proxy import mint_token
from claudius.controller.session_manager import BusySessionError, ClosedSessionError
from claudius.controller.sse import SSEBroker
from claudius.channels.resend import ResendChannel
from claudius.models import ClaudeSummary, Execution, InboundMessage, Session, SessionState

RESEND_PAYLOAD = {
    "type": "email.received",
    "data": {
        "from": "user@example.com",
        "to": ["inbox@claudius.example.com"],
        "subject": "Hello",
        "message_id": "<msg-001@mail.example.com>",
        "email_id": "email_001",
    }
}


def _fake_session(session_id="sess-001"):
    return Session(
        session_id=session_id,
        thread_id="thread-001",
        channel="email",
        workflow_name="echo",
        state=SessionState.ACTIVE,
        workspace_path="/tmp",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
        claude_summary=ClaudeSummary(num_turns=3, duration_ms=1500, total_cost_usd=0.01, total_tokens=120),
    )


def test_health_endpoint():
    manager = AsyncMock()
    channel = MagicMock(spec=ResendChannel)
    app = create_controller_app(session_manager=manager, channels={"email": channel}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200


def test_resend_webhook_calls_handle_message():
    manager = AsyncMock()
    channel = AsyncMock(spec=ResendChannel)
    channel.parse_webhook.return_value = InboundMessage(
        channel="email", sender="user@example.com", recipients=["inbox@claudius.example.com"], thread_id="msg-001",
        subject="Hello", body="Do the thing", attachments=[],
        received_at=datetime.now(timezone.utc),
    )
    app = create_controller_app(session_manager=manager, channels={"email": channel}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post("/webhook/resend", json=RESEND_PAYLOAD)
    assert resp.status_code == 200
    manager.handle_message.assert_called_once()


def test_sessions_list_endpoint():
    manager = AsyncMock()
    manager.list_sessions.return_value = [_fake_session()]
    channel = MagicMock()
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/sessions")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["claude_summary"]["num_turns"] == 3
    assert data[0]["claude_summary"]["total_cost_usd"] == 0.01
    assert data[0]["claude_summary"]["total_tokens"] == 120


def test_get_workflows():
    manager = AsyncMock()
    manager.list_workflow_names = MagicMock(return_value=["wf-a", "wf-b"])
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/workflows")
    assert resp.status_code == 200
    assert resp.json() == ["wf-a", "wf-b"]


def test_get_session_by_id():
    manager = AsyncMock()
    manager.get_session.return_value = _fake_session("sess-001")
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/sessions/sess-001")
    assert resp.status_code == 200
    assert resp.json()["session_id"] == "sess-001"
    assert resp.json()["claude_summary"]["duration_ms"] == 1500
    assert resp.json()["claude_summary"]["total_cost_usd"] == 0.01


def test_get_session_not_found():
    manager = AsyncMock()
    manager.get_session.return_value = None
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/sessions/missing")
    assert resp.status_code == 404


def test_get_session_messages():
    manager = AsyncMock()
    manager.get_messages.return_value = [
        {
            "message_id": "msg-1",
            "direction": "inbound",
            "body": "hello",
            "received_at": "2026-01-01T00:00:00+00:00",
            "attachments": [{"filename": "report.pdf", "content_type": "application/pdf"}],
            "acknowledged_at": None,
            "delivery_status": "pending",
        }
    ]
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    resp = client.get("/sessions/sess-001/messages")
    assert resp.status_code == 200
    assert len(resp.json()) == 1
    assert resp.json()[0]["delivery_status"] == "pending"
    assert resp.json()[0]["acknowledged_at"] is None
    attachment = resp.json()[0]["attachments"][0]
    assert attachment["filename"] == "report.pdf"
    assert attachment["content_type"] == "application/pdf"
    assert attachment["path"].startswith(
        "/sessions/sess-001/messages/msg-1/attachments/report.pdf?token="
    )


def test_get_session_history_requires_auth():
    manager = AsyncMock()
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    resp = client.get("/sessions/sess-001/history")
    assert resp.status_code == 401


def test_get_session_history_returns_filtered_results():
    manager = AsyncMock()
    manager.get_session_history.return_value = [
        {
            "message_id": "msg-1",
            "role": "user",
            "content": "hello",
            "received_at": "2026-01-01T00:00:00+00:00",
            "attachments": [
                {
                    "message_id": "msg-1",
                    "filename": "report.pdf",
                    "content_type": "application/pdf",
                }
            ],
        }
    ]
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    token = mint_token("sess-001", "secret")
    resp = client.get(
        "/sessions/sess-001/history",
        params={"side": "user", "limit": 5, "query": "hello"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    manager.get_session_history.assert_called_once_with(
        "sess-001",
        side="user",
        limit=5,
        query="hello",
    )
    data = resp.json()
    assert len(data) == 1
    assert data[0]["message_id"] == "msg-1"
    assert data[0]["role"] == "user"
    assert data[0]["content"] == "hello"
    assert data[0]["received_at"] == "2026-01-01T00:00:00+00:00"
    assert data[0]["attachments"][0]["filename"] == "report.pdf"
    assert data[0]["attachments"][0]["content_type"] == "application/pdf"
    assert data[0]["attachments"][0]["path"].startswith(
        "/sessions/sess-001/messages/msg-1/attachments/report.pdf?token="
    )


def test_post_session_outbound():
    manager = AsyncMock()
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post(
        "/sessions/sess-001/outbound",
        json={
            "body": "Agent reply",
            "attachments": [
                {
                    "filename": "report.txt",
                    "content_type": "text/plain",
                    "data": "aGVsbG8=",
                }
            ],
        },
    )
    assert resp.status_code == 200
    manager.receive_outbound.assert_called_once()
    args = manager.receive_outbound.await_args.args
    assert args[0] == "sess-001"
    assert args[1] == "Agent reply"
    assert len(args[2]) == 1
    assert args[2][0].filename == "report.txt"
    assert args[2][0].content_type == "text/plain"
    assert args[2][0].data == b"hello"


def test_delete_pending_message_endpoint():
    manager = AsyncMock()
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.delete("/sessions/sess-001/messages/msg-1")
    assert resp.status_code == 204
    manager.delete_pending_message.assert_awaited_once_with("sess-001", "msg-1")


def test_resend_outbound_message_endpoint():
    manager = AsyncMock()
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post("/sessions/sess-001/messages/msg-1/resend")
    assert resp.status_code == 200
    manager.resend_outbound_message.assert_awaited_once_with("sess-001", "msg-1")


def test_post_dev_inject():
    manager = AsyncMock()
    manager.handle_message.return_value = _fake_session("sess-new")
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post("/dev/inject", json={
        "channel": "email",
        "sender": "dev@localhost",
        "subject": "Test",
        "body": "Hello",
        "attachments": [],
    })
    assert resp.status_code == 200
    assert resp.json()["session_id"] == "sess-new"
    manager.handle_message.assert_called_once()


def test_post_session_message():
    manager = AsyncMock()
    manager.continue_session.return_value = {
        "message": {
            "message_id": "msg-1",
            "direction": "inbound",
            "body": "follow up",
            "received_at": "2026-01-01T00:00:00+00:00",
            "attachments": [],
            "acknowledged_at": None,
            "delivery_status": "pending",
        },
        "followup_action": "interrupt_after_turn",
    }
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post("/sessions/sess-001/message", json={"body": "follow up", "attachments": []})
    assert resp.status_code == 200
    manager.continue_session.assert_called_once_with("sess-001", "follow up", [])
    assert resp.json()["followup_action"] == "interrupt_after_turn"
    assert resp.json()["message"]["message_id"] == "msg-1"


def test_post_session_message_rejects_busy_workflow():
    manager = AsyncMock()
    manager.continue_session.side_effect = BusySessionError("sess-001")
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post("/sessions/sess-001/message", json={"body": "follow up", "attachments": []})
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "session_busy"


def test_post_session_message_rejects_closed_session():
    manager = AsyncMock()
    manager.continue_session.side_effect = ClosedSessionError("sess-001")
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post("/sessions/sess-001/message", json={"body": "follow up", "attachments": []})
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "session_closed"


def test_close_session_requires_auth():
    manager = AsyncMock()
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    resp = client.post("/sessions/sess-001/close")
    assert resp.status_code == 401


def test_close_session_accepts_bearer_token():
    manager = AsyncMock()
    manager.close_session.return_value = True
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    token = mint_token("sess-001", "secret")
    resp = client.post(
        "/sessions/sess-001/close",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "closed"}
    manager.close_session.assert_called_once_with("sess-001")


def test_close_session_rejects_wrong_token():
    manager = AsyncMock()
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    token = mint_token("other-session", "secret")
    resp = client.post(
        "/sessions/sess-001/close",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403


def test_get_attachment_requires_auth(tmp_path):
    manager = AsyncMock()
    manager._db = AsyncMock()
    manager._db.get_attachment_by_name.return_value = {
        "storage_key": "sess-001/msg-1/report.html",
        "content_type": "text/html",
    }
    store = LocalAttachmentStore(tmp_path)
    (tmp_path / "sess-001" / "msg-1").mkdir(parents=True)
    (tmp_path / "sess-001" / "msg-1" / "report.html").write_text("<html>ok</html>")
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
        attachment_store=store,
    )
    client = TestClient(app)
    resp = client.get("/sessions/sess-001/messages/msg-1/attachments/report.html")
    assert resp.status_code == 401


def test_get_attachment_accepts_query_token(tmp_path):
    manager = AsyncMock()
    manager._db = AsyncMock()
    manager._db.get_attachment_by_name.return_value = {
        "storage_key": "sess-001/msg-1/report.html",
        "content_type": "text/html",
    }
    store = LocalAttachmentStore(tmp_path)
    (tmp_path / "sess-001" / "msg-1").mkdir(parents=True)
    (tmp_path / "sess-001" / "msg-1" / "report.html").write_text("<html>ok</html>")
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
        attachment_store=store,
    )
    client = TestClient(app)
    token = mint_token("sess-001", "secret")
    resp = client.get(
        f"/sessions/sess-001/messages/msg-1/attachments/report.html?token={token}"
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "ok" in resp.text


def test_get_attachment_accepts_bearer_token(tmp_path):
    manager = AsyncMock()
    manager._db = AsyncMock()
    manager._db.get_attachment_by_name.return_value = {
        "storage_key": "sess-001/msg-1/report.html",
        "content_type": "text/html",
    }
    store = LocalAttachmentStore(tmp_path)
    (tmp_path / "sess-001" / "msg-1").mkdir(parents=True)
    (tmp_path / "sess-001" / "msg-1" / "report.html").write_text("<html>ok</html>")
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
        attachment_store=store,
    )
    client = TestClient(app)
    token = mint_token("sess-001", "secret")
    resp = client.get(
        "/sessions/sess-001/messages/msg-1/attachments/report.html",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "ok" in resp.text


def test_get_attachment_rejects_wrong_session_token(tmp_path):
    manager = AsyncMock()
    manager._db = AsyncMock()
    manager._db.get_attachment_by_name.return_value = {
        "storage_key": "sess-001/msg-1/report.html",
        "content_type": "text/html",
    }
    store = LocalAttachmentStore(tmp_path)
    (tmp_path / "sess-001" / "msg-1").mkdir(parents=True)
    (tmp_path / "sess-001" / "msg-1" / "report.html").write_text("<html>ok</html>")
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
        attachment_store=store,
    )
    client = TestClient(app)
    token = mint_token("other-session", "secret")
    resp = client.get(
        f"/sessions/sess-001/messages/msg-1/attachments/report.html?token={token}"
    )
    assert resp.status_code == 403


def test_get_session_executions():
    manager = AsyncMock()
    manager.list_executions.return_value = [
        Execution(
            execution_id="exec-1", session_id="sess-001",
            worker_address="172.0.0.1:8080",
            started_at=datetime.now(timezone.utc),
            halted_at=None, halt_reason=None,
            claude_num_turns=4,
            claude_duration_ms=1200,
            claude_total_cost_usd=0.02,
            claude_input_tokens=100,
            claude_output_tokens=20,
            claude_total_tokens=120,
        )
    ]
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/sessions/sess-001/executions")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["execution_id"] == "exec-1"
    assert data[0]["halted_at"] is None
    assert data[0]["claude_total_tokens"] == 120


def test_get_execution_logs():
    manager = AsyncMock()
    manager.list_execution_logs.return_value = [
        {"logged_at": "2026-01-01T12:00:00+00:00", "stream": "stdout", "body": "hello"},
    ]
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/executions/exec-1/logs")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["stream"] == "stdout"
    assert data[0]["body"] == "hello"


def test_get_proxy_logs():
    manager = AsyncMock()
    manager.list_proxy_logs.return_value = [
        {
            "logged_at": "2026-01-01T12:00:00+00:00",
            "stage": "request_in",
            "method": "POST",
            "path": "v1/messages",
            "upstream_url": "http://upstream/v1/messages",
            "status_code": None,
            "content_type": "application/json",
            "body": "{}",
            "meta": {"rewrite_applied": False},
            "input_tokens": None,
            "output_tokens": None,
            "cache_creation_input_tokens": None,
            "cache_read_input_tokens": None,
            "total_tokens": None,
            "total_cost_usd": None,
        },
    ]
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/executions/exec-1/proxy-logs")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["stage"] == "request_in"
    assert data[0]["method"] == "POST"
    assert data[0]["total_tokens"] is None


def test_get_conversation_events():
    manager = AsyncMock()
    manager.list_conversation_events.return_value = [
        {
            "execution_id": "exec-1",
            "logged_at": "2026-01-01T12:00:00+00:00",
            "seq": 1,
            "source": "local",
            "event_type": "input",
            "event_subtype": "initial_message",
            "payload": {"body": "hello"},
        },
    ]
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.get("/executions/exec-1/conversation-events")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["event_type"] == "input"


def test_post_conversation_event():
    manager = AsyncMock()
    manager.append_active_execution_conversation_event.return_value = {
        "execution_id": "exec-1",
        "logged_at": "2026-01-01T12:00:00+00:00",
        "seq": 2,
        "source": "claude",
        "event_type": "result",
        "event_subtype": "success",
        "payload": {"result": "done"},
    }
    app = create_controller_app(session_manager=manager, channels={}, broker=SSEBroker())
    client = TestClient(app)
    resp = client.post(
        "/sessions/sess-001/conversation-events",
        json={
            "source": "claude",
            "event_type": "result",
            "event_subtype": "success",
            "payload": {"result": "done"},
        },
    )
    assert resp.status_code == 200
    manager.append_active_execution_conversation_event.assert_called_once()


def test_report_fatal_error_requires_auth():
    manager = AsyncMock()
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    resp = client.post(
        "/sessions/sess-001/fatal-error",
        json={"category": "tool_failure", "reason": "Tool broke."},
    )
    assert resp.status_code == 401


def test_report_fatal_error_accepts_bearer_token():
    manager = AsyncMock()
    manager.record_agent_fatal_error.return_value = None
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    token = mint_token("sess-001", "secret")
    resp = client.post(
        "/sessions/sess-001/fatal-error",
        json={"category": "tool_failure", "reason": "Tool broke."},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "recorded"}
    manager.record_agent_fatal_error.assert_called_once_with(
        "sess-001", category="tool_failure", reason="Tool broke."
    )


def test_report_fatal_error_rejects_wrong_token():
    manager = AsyncMock()
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    token = mint_token("other-session", "secret")
    resp = client.post(
        "/sessions/sess-001/fatal-error",
        json={"category": "tool_failure", "reason": "boom"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403


def test_report_fatal_error_returns_404_when_no_active_execution():
    manager = AsyncMock()
    manager.record_agent_fatal_error.side_effect = ValueError("No active execution for session sess-001")
    app = create_controller_app(
        session_manager=manager,
        channels={},
        broker=SSEBroker(),
        proxy_secret="secret",
    )
    client = TestClient(app)
    token = mint_token("sess-001", "secret")
    resp = client.post(
        "/sessions/sess-001/fatal-error",
        json={"category": "unexpected_error", "reason": "oops"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404
