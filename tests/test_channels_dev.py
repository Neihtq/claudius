import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from claudius.channels.dev import DevChannel
from claudius.models import Attachment


@pytest.mark.asyncio
async def test_send_message_posts_to_callback():
    channel = DevChannel(callback_url="http://controller:8000", session_id="sess-abc")

    with patch("claudius.channels.dev.httpx") as mock_httpx:
        mock_client = AsyncMock()
        mock_httpx.AsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_httpx.AsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock()

        await channel.send_message(
            to="user@example.com",
            body="Hello!",
            thread_id="t1",
            attachments=[Attachment(filename="report.txt", content_type="text/plain", data=b"hello")],
        )

    mock_client.post.assert_called_once_with(
        "http://controller:8000/sessions/sess-abc/outbound",
        json={
            "body": "Hello!",
            "attachments": [
                {
                    "filename": "report.txt",
                    "content_type": "text/plain",
                    "data": "aGVsbG8=",
                }
            ],
        },
        timeout=5.0,
    )


def test_parse_webhook_raises():
    channel = DevChannel(callback_url="http://controller:8000", session_id="sess-abc")
    with pytest.raises(NotImplementedError):
        asyncio.run(channel.parse_webhook(None))  # type: ignore
