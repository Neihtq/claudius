import base64
import httpx
from fastapi import Request
from claudius.channels.base import AbstractChannel
from claudius.models import Attachment, InboundMessage


class DevChannel(AbstractChannel):
    def __init__(self, callback_url: str, session_id: str):
        self._callback_url = callback_url.rstrip("/")
        self._session_id = session_id

    async def parse_webhook(self, request: Request) -> InboundMessage:
        raise NotImplementedError("DevChannel does not parse webhooks")

    async def send_message(
        self,
        to: str,
        body: str,
        thread_id: str,
        attachments: list[Attachment] | None = None,
    ) -> None:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{self._callback_url}/sessions/{self._session_id}/outbound",
                json={
                    "body": body,
                    "attachments": [
                        {
                            "filename": attachment.filename,
                            "content_type": attachment.content_type,
                            "data": base64.b64encode(attachment.data).decode("ascii"),
                        }
                        for attachment in (attachments or [])
                    ],
                },
                timeout=5.0,
            )
