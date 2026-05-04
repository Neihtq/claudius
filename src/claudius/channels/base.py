from abc import ABC, abstractmethod
from fastapi import Request
from claudius.models import Attachment, InboundMessage


class AbstractChannel(ABC):
    @abstractmethod
    async def parse_webhook(self, request: Request) -> InboundMessage:
        """Parse an inbound webhook request into an InboundMessage."""

    @abstractmethod
    async def send_message(
        self,
        to: str,
        body: str,
        thread_id: str,
        attachments: list[Attachment] | None = None,
    ) -> None:
        """Send a message back to a recipient."""
