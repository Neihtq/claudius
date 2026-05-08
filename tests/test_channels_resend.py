import base64
import json
import sys
import types
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException, Request
from starlette.requests import Request as StarletteRequest

from claudius.channels.resend import ResendChannel
from claudius.models import Attachment

RESEND_PAYLOAD = {
    "type": "email.received",
    "data": {
        "from": "Sender Name <sender@example.com>",
        "to": ["edit@rosenstein.app"],
        "subject": "Hello World",
        "message_id": "<abc123@mail.example.com>",
        "email_id": "56761188-7520-42d8-8898-ff6fc54ce618",
    }
}

RESEND_EMAIL_RESPONSE = {
    "object": "email",
    "id": "56761188-7520-42d8-8898-ff6fc54ce618",
    "from": "Sender Name <sender@example.com>",
    "to": ["edit@rosenstein.app"],
    "subject": "Hello World",
    "text": "Body of the email",
    "message_id": "<abc123@mail.example.com>",
    "headers": {},
}


def _request_for(payload: dict) -> Request:
    body = json.dumps(payload).encode("utf-8")

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return StarletteRequest(
        {
            "type": "http",
            "method": "POST",
            "path": "/test",
            "headers": [(b"content-type", b"application/json")],
        },
        receive,
    )

@pytest.mark.asyncio
async def test_parse_webhook_basic():
    channel = ResendChannel(api_key="test-key", from_address="claudius@example.com")
    request = _request_for(RESEND_PAYLOAD)

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = [
            httpx.Response(
                200,
                json=RESEND_EMAIL_RESPONSE,
                request=httpx.Request("GET", "https://api.resend.com/emails/receiving/test"),
            ),
            httpx.Response(
                200,
                json={"object": "list", "data": []},
                request=httpx.Request("GET", "https://api.resend.com/emails/receiving"),
            ),
        ]
        msg = await channel.parse_webhook(request)
        assert msg.sender == "sender@example.com"
        assert msg.recipients == ["edit@rosenstein.app"]
        assert msg.subject == "Hello World"
        assert msg.body == "Body of the email"
        assert msg.thread_id == "abc123@mail.example.com"
        assert mock_get.await_count == 2


@pytest.mark.asyncio
async def test_parse_webhook_reply_uses_root_received_email_id_for_thread():
    channel = ResendChannel(api_key="test-key", from_address="claudius@example.com")
    request = _request_for({
        "type": "email.received",
        "data": {
            "from": "sender@example.com",
            "to": ["edit@rosenstein.app"],
            "subject": "Re: Hello World",
            "message_id": "<reply@mail.example.com>",
            "email_id": "reply-email-id",
        },
    })

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = [
            httpx.Response(
                200,
                json={
                    "object": "email",
                    "id": "reply-email-id",
                    "from": "sender@example.com",
                    "to": ["edit@rosenstein.app"],
                    "subject": "Re: Hello World",
                    "text": "Reply body",
                    "message_id": "<reply@mail.example.com>",
                    "headers": {
                        "references": "[\"<root@mail.example.com>\",\"<mid@mail.example.com>\"]",
                        "in-reply-to": "<mid@mail.example.com>",
                    },
                },
                request=httpx.Request("GET", "https://api.resend.com/emails/receiving/reply-email-id"),
            ),
            httpx.Response(
                200,
                json={
                    "object": "list",
                    "data": [
                        {
                            "id": "root-email-id",
                            "message_id": "<root@mail.example.com>",
                        }
                    ],
                },
                request=httpx.Request("GET", "https://api.resend.com/emails/receiving"),
            ),
        ]
        msg = await channel.parse_webhook(request)
        assert msg.body == "Reply body"
        assert msg.thread_id == "root-email-id"


@pytest.mark.asyncio
async def test_parse_webhook_without_api_key_uses_inline_body_if_present():
    channel = ResendChannel(api_key="", from_address="claudius@example.com")
    request = _request_for({
        "type": "email.received",
        "data": {
            "from": "sender@example.com",
            "to": ["edit@rosenstein.app"],
            "subject": "Hello World",
            "text": "Inline body",
            "message_id": "<abc123@mail.example.com>",
            "email_id": "56761188-7520-42d8-8898-ff6fc54ce618",
        },
    })

    msg = await channel.parse_webhook(request)
    assert msg.body == "Inline body"
    assert msg.recipients == ["edit@rosenstein.app"]


@pytest.mark.asyncio
async def test_parse_webhook_without_api_key_and_without_inline_body_raises_clear_error():
    channel = ResendChannel(api_key="", from_address="claudius@example.com")
    request = _request_for(RESEND_PAYLOAD)

    with pytest.raises(HTTPException) as exc:
        await channel.parse_webhook(request)
    assert exc.value.status_code == 503
    assert "RESEND_API_KEY" in str(exc.value.detail)

@pytest.mark.asyncio
async def test_send_message_calls_resend_api():
    channel = ResendChannel(api_key="test-key", from_address="claudius@example.com")
    fake_markdown = types.SimpleNamespace(
        markdown=lambda body, extensions: f"<p data-exts='{','.join(extensions)}'>{body}</p>"
    )
    with (
        patch.dict(sys.modules, {"markdown": fake_markdown}),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
    ):
        mock_post.return_value = httpx.Response(
            200,
            json={"id": "sent-1"},
            request=httpx.Request("POST", "https://api.resend.com/emails"),
        )
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
        assert payload["html"] == (
            "<p data-exts='fenced_code,tables,sane_lists,nl2br'>Hello back</p>"
        )
        assert payload["attachments"] == [
            {"filename": "report.txt", "content": base64.b64encode(b"hello").decode("ascii")}
        ]


@pytest.mark.asyncio
async def test_send_message_uses_received_email_uuid_for_reply_headers():
    channel = ResendChannel(api_key="test-key", from_address="claudius@example.com")
    fake_markdown = types.SimpleNamespace(
        markdown=lambda body, extensions: f"<p data-exts='{','.join(extensions)}'>{body}</p>"
    )
    with (
        patch.dict(sys.modules, {"markdown": fake_markdown}),
        patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get,
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
    ):
        mock_get.return_value = httpx.Response(
            200,
            json={
                "object": "email",
                "id": "71a8ee5c-42e5-470d-ac28-21f900751b5d",
                "subject": "GitLab: Theme overhaul",
                "message_id": "<root@mail.example.com>",
            },
            request=httpx.Request(
                "GET",
                "https://api.resend.com/emails/receiving/71a8ee5c-42e5-470d-ac28-21f900751b5d",
            ),
        )
        mock_post.return_value = httpx.Response(
            200,
            json={"id": "sent-2"},
            request=httpx.Request("POST", "https://api.resend.com/emails"),
        )

        await channel.send_message(
            to="user@example.com",
            body="Hello back",
            thread_id="71a8ee5c-42e5-470d-ac28-21f900751b5d",
        )

        mock_get.assert_awaited_once()
        payload = mock_post.call_args.kwargs["json"]
        assert payload["subject"] == "Re: GitLab: Theme overhaul"
        assert payload["headers"] == {
            "In-Reply-To": "<root@mail.example.com>",
            "References": "<root@mail.example.com>",
        }
