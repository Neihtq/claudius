from datetime import datetime
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from claudius.models import InboundMessage
from claudius.session.runner import SessionRunner


class MessagePayload(BaseModel):
    channel: str
    sender: str
    recipients: list[str] = []
    thread_id: str
    subject: str | None
    body: str
    received_at: datetime
    attachments: list[dict] = []


def create_session_app(
    runner: SessionRunner,
    session_id: str,
    *,
    session_token: str = "",
) -> FastAPI:
    app = FastAPI()

    def _require_token(authorization: str | None) -> None:
        expected = session_token.strip()
        if not expected:
            return
        token = (authorization or "").removeprefix("Bearer ").strip()
        if token != expected:
            raise HTTPException(status_code=401, detail="Authorization header required")

    @app.get("/health")
    async def health():
        return {"status": "ok", "session_id": session_id, "done": runner.is_done}

    @app.post("/message")
    async def receive_message(payload: MessagePayload):
        msg = InboundMessage(
            channel=payload.channel,
            sender=payload.sender,
            recipients=payload.recipients,
            thread_id=payload.thread_id,
            subject=payload.subject,
            body=payload.body,
            attachments=[],
            received_at=payload.received_at,
        )
        if not runner.inject_message(msg):
            raise HTTPException(status_code=503, detail="Worker is shutting down")
        return {"status": "delivered"}

    @app.post("/shutdown")
    async def shutdown(authorization: str | None = Header(default=None)):
        _require_token(authorization)
        graceful = await runner.graceful_stop()
        return {"status": "stopping", "graceful": graceful}

    return app
