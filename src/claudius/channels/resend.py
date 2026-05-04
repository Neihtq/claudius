from datetime import datetime, timezone
import base64
import httpx
from fastapi import Request
from claudius.channels.base import AbstractChannel
from claudius.models import InboundMessage, Attachment

RESEND_API_URL = "https://api.resend.com"


class ResendChannel(AbstractChannel):
    def __init__(self, api_key: str, from_address: str):
        self._api_key = api_key
        self._from_address = from_address

    async def parse_webhook(self, request: Request) -> InboundMessage:
        payload = await request.json()
        data = payload.get("data", payload)

        sender = data.get("from", "")
        subject = data.get("subject")
        body = data.get("text") or data.get("html") or ""

        # Derive thread_id: prefer Message-ID header, fall back to email_id
        thread_id = data.get("email_id", "")
        for header in data.get("headers", []):
            if header.get("name", "").lower() == "message-id":
                thread_id = header["value"].strip("<>")
                break

        return InboundMessage(
            channel="email",
            sender=sender,
            thread_id=thread_id,
            subject=subject,
            body=body,
            attachments=[],
            received_at=datetime.now(timezone.utc),
        )

    async def send_message(
        self,
        to: str,
        body: str,
        thread_id: str,
        attachments: list[Attachment] | None = None,
    ) -> None:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{RESEND_API_URL}/emails",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={
                    "from": self._from_address,
                    "to": [to],
                    "subject": "Re: (claudius)",
                    "text": body,
                    "attachments": [
                        {
                            "filename": attachment.filename,
                            "content": base64.b64encode(attachment.data).decode("ascii"),
                        }
                        for attachment in (attachments or [])
                    ],
                },
            )
