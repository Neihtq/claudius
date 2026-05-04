import pytest
from datetime import datetime, timezone
import base64
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI, Request
from claudius.channels.resend import ResendChannel
from claudius.models import Attachment

RESEND_PAYLOAD = {
    "type": "email.received",
    "data": {
        "from": "sender@example.com",
        "to": ["inbox@claudius.example.com"],
        "subject": "Hello World",
        "text": "Body of the email",
        "headers": [{"name": "Message-ID", "value": "<abc123@mail.example.com>"}],
        "email_id": "email_abc123",
    }
}

@pytest.mark.asyncio
async def test_parse_webhook_basic():
    channel = ResendChannel(api_key="test-key", from_address="claudius@example.com")
    app = FastAPI()

    @app.post("/test")
    async def endpoint(request: Request):
        msg = await channel.parse_webhook(request)
        return {
            "sender": msg.sender,
            "thread_id": msg.thread_id,
            "subject": msg.subject,
            "body": msg.body,
        }

    client = TestClient(app)
    resp = client.post("/test", json=RESEND_PAYLOAD)
    assert resp.status_code == 200
    data = resp.json()
    assert data["sender"] == "sender@example.com"
    assert data["subject"] == "Hello World"
    assert data["body"] == "Body of the email"
    assert data["thread_id"] != ""

@pytest.mark.asyncio
async def test_send_message_calls_resend_api():
    channel = ResendChannel(api_key="test-key", from_address="claudius@example.com")
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value.status_code = 200
        await channel.send_message(
            to="user@example.com",
            body="Hello back",
            thread_id="thread-123",
            attachments=[Attachment(filename="report.txt", content_type="text/plain", data=b"hello")],
        )
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs.args[1]
        assert payload["to"] == ["user@example.com"]
        assert payload["text"] == "Hello back"
        assert payload["attachments"] == [
            {"filename": "report.txt", "content": base64.b64encode(b"hello").decode("ascii")}
        ]
