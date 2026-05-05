import asyncio
import base64
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from claudius.channels.base import AbstractChannel
from claudius.controller.attachments import AttachmentStore
from claudius.controller.proxy import (
    ProxyUpstream,
    build_proxy_upstream,
    mint_token,
    verify_token,
)
from claudius.controller.session_manager import (
    BusySessionError,
    ClosedSessionError,
    NoWorkflowMatch,
    SessionManager,
)
from claudius.controller.sse import SSEBroker
from claudius.models import Attachment, InboundMessage


_UI_DIR = Path(__file__).parent.parent.parent.parent / "ui" / "dist"


class _SPAStaticFiles(StaticFiles):
    """StaticFiles that falls back to index.html for unknown paths (SPA routing)."""

    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404:
                return await super().get_response("index.html", scope)
            raise


class _OutboundRequest(BaseModel):
    body: str
    attachments: list[dict] = []


class _MessageRequest(BaseModel):
    body: str
    attachments: list[dict] = []


class _ConversationEventRequest(BaseModel):
    source: str
    event_type: str
    event_subtype: str | None = None
    payload: dict
    logged_at: str | None = None


class _DevInjectRequest(BaseModel):
    channel: str
    sender: str
    subject: str | None = None
    body: str
    attachments: list[dict] = []


class _FatalErrorRequest(BaseModel):
    category: str
    reason: str


def _get_session_token(request: Request) -> str:
    header_token = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
    if header_token:
        return header_token
    token = request.query_params.get("token", "").strip()
    if token:
        return token
    return ""


def _require_session_token(request: Request, session_id: str, proxy_secret: str) -> None:
    """Raise 401/403 if the session JWT doesn't match the requested session."""
    if not proxy_secret:
        raise HTTPException(status_code=501, detail="Authentication not configured")
    token = _get_session_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Authorization header required")
    jwt_session_id = verify_token(token, proxy_secret)
    if jwt_session_id != session_id:
        raise HTTPException(status_code=403, detail="Token does not match session")


def _tokenized_session_path(path: str, session_id: str, proxy_secret: str) -> str:
    if not proxy_secret:
        return path
    return f"{path}?token={quote(mint_token(session_id, proxy_secret))}"


def _serialize_claude_summary(summary) -> dict:
    if summary is None:
        return {
            "executions_count": 0,
            "completed_executions_count": 0,
            "num_turns": 0,
            "duration_ms": 0,
            "total_cost_usd": 0.0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "total_tokens": 0,
        }
    return {
        "executions_count": summary.executions_count,
        "completed_executions_count": summary.completed_executions_count,
        "num_turns": summary.num_turns,
        "duration_ms": summary.duration_ms,
        "total_cost_usd": summary.total_cost_usd,
        "input_tokens": summary.input_tokens,
        "output_tokens": summary.output_tokens,
        "cache_creation_input_tokens": summary.cache_creation_input_tokens,
        "cache_read_input_tokens": summary.cache_read_input_tokens,
        "total_tokens": summary.total_tokens,
    }


def _serialize_session(session) -> dict:
    return {
        "session_id": session.session_id,
        "thread_id": session.thread_id,
        "channel": session.channel,
        "workflow_name": session.workflow_name,
        "state": session.state.value,
        "created_at": session.created_at.isoformat(),
        "last_message_at": session.last_message_at.isoformat(),
        "last_execution_result": session.last_execution_result,
        "claude_summary": _serialize_claude_summary(session.claude_summary),
    }


def _serialize_execution(execution) -> dict:
    return {
        "execution_id": execution.execution_id,
        "session_id": execution.session_id,
        "worker_address": execution.worker_address,
        "started_at": execution.started_at.isoformat(),
        "halted_at": execution.halted_at.isoformat() if execution.halted_at else None,
        "halt_reason": execution.halt_reason,
        "phase": execution.phase.value,
        "runtime_container": execution.runtime_container.to_dict() if execution.runtime_container else None,
        "worker_container": execution.worker_container.to_dict() if execution.worker_container else None,
        "exit_code": execution.exit_code,
        "claude_session_id": execution.claude_session_id,
        "claude_num_turns": execution.claude_num_turns,
        "claude_duration_ms": execution.claude_duration_ms,
        "claude_total_cost_usd": execution.claude_total_cost_usd,
        "claude_input_tokens": execution.claude_input_tokens,
        "claude_output_tokens": execution.claude_output_tokens,
        "claude_cache_creation_input_tokens": execution.claude_cache_creation_input_tokens,
        "claude_cache_read_input_tokens": execution.claude_cache_read_input_tokens,
        "claude_total_tokens": execution.claude_total_tokens,
        "agent_error_category": execution.agent_error_category,
        "agent_error_reason": execution.agent_error_reason,
    }


def _serialize_message(message: dict, session_id: str, proxy_secret: str) -> dict:
    attachments = []
    for attachment in message.get("attachments", []):
        filename = attachment.get("filename", "")
        path = None
        if filename and message.get("message_id"):
            raw_path = (
                f"/sessions/{session_id}/messages/"
                f"{message.get('message_id', '')}/attachments/{filename}"
            )
            path = _tokenized_session_path(raw_path, session_id, proxy_secret)
        attachments.append({
            "filename": filename,
            "content_type": attachment.get("content_type", "application/octet-stream"),
            **({"path": path} if path else {}),
        })
    return {
        **message,
        "attachments": attachments,
    }


def create_controller_app(
    session_manager: SessionManager,
    channels: dict[str, AbstractChannel],
    broker: SSEBroker,
    inbound_webhooks: dict[str, str] | None = None,
    proxy_secret: str = "",
    proxy_upstream: ProxyUpstream | None = None,
    attachment_store: AttachmentStore | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        broker.close()

    app = FastAPI(lifespan=lifespan)
    inbound_webhooks = inbound_webhooks or {}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/workflows")
    async def list_workflows():
        return session_manager.list_workflow_names()

    async def _handle_inbound_webhook(request: Request, channel_name: str):
        channel = channels.get(channel_name)
        if channel is None:
            raise HTTPException(status_code=404, detail=f"Channel not configured: {channel_name}")
        message = await channel.parse_webhook(request)
        try:
            await session_manager.handle_message(message)
        except NoWorkflowMatch as e:
            return {"status": "no_match", "detail": str(e)}
        return {"status": "accepted"}

    def _make_webhook_handler(channel_name: str):
        async def _webhook_handler(request: Request):
            return await _handle_inbound_webhook(request, channel_name)

        return _webhook_handler

    for webhook_path, channel_name in inbound_webhooks.items():
        app.add_api_route(
            webhook_path,
            _make_webhook_handler(channel_name),
            methods=["POST"],
        )

    @app.get("/sessions")
    async def list_sessions():
        sessions = await session_manager.list_sessions()
        return [_serialize_session(s) for s in sessions]

    @app.get("/sessions/{session_id}")
    async def get_session(session_id: str):
        session = await session_manager.get_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        return _serialize_session(session)

    @app.get("/sessions/{session_id}/messages")
    async def get_messages(session_id: str):
        messages = await session_manager.get_messages(session_id)
        return [_serialize_message(message, session_id, proxy_secret) for message in messages]

    @app.get("/sessions/{session_id}/history")
    async def get_session_history(
        session_id: str,
        request: Request,
        side: str = "both",
        limit: int = 20,
        query: str = "",
    ):
        _require_session_token(request, session_id, proxy_secret)
        history = await session_manager.get_session_history(
            session_id,
            side=side,
            limit=limit,
            query=query,
        )
        for message in history:
            attachments = []
            for attachment in message.get("attachments", []):
                filename = attachment.get("filename", "")
                message_id = attachment.get("message_id", "")
                path = f"/sessions/{session_id}/messages/{message_id}/attachments/{filename}"
                attachments.append({
                    "filename": filename,
                    "content_type": attachment.get("content_type", "application/octet-stream"),
                    "path": _tokenized_session_path(path, session_id, proxy_secret),
                })
            message["attachments"] = attachments
        return history

    @app.get("/sessions/{session_id}/events")
    async def session_events(session_id: str):
        async def generate():
            q: asyncio.Queue = asyncio.Queue()
            broker.subscribe(session_id, q)
            try:
                while True:
                    try:
                        data = await asyncio.wait_for(q.get(), timeout=15.0)
                        if data is None:
                            return
                        event = json.loads(data)
                        if event.get("type") == "message":
                            event = _serialize_message(event, session_id, proxy_secret)
                            data = json.dumps(event)
                        yield f"data: {data}\n\n"
                    except asyncio.TimeoutError:
                        yield ": ping\n\n"
            finally:
                broker.unsubscribe(session_id, q)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/sessions/{session_id}/outbound")
    async def session_outbound(session_id: str, req: _OutboundRequest):
        attachments = []
        for a in req.attachments:
            try:
                data = base64.b64decode(a.get("data", ""))
            except Exception:
                data = b""
            attachments.append(Attachment(
                filename=a.get("filename", "attachment"),
                content_type=a.get("content_type", "application/octet-stream"),
                data=data,
            ))
        await session_manager.receive_outbound(session_id, req.body, attachments)
        return {"status": "ok"}

    @app.post("/sessions/{session_id}/message")
    async def session_message(session_id: str, req: _MessageRequest):
        attachments = []
        for a in req.attachments:
            try:
                data = base64.b64decode(a.get("data", ""))
            except Exception:
                data = b""
            attachments.append(Attachment(
                filename=a.get("filename", "attachment"),
                content_type=a.get("content_type", "application/octet-stream"),
                data=data,
            ))
        try:
            result = await session_manager.continue_session(session_id, req.body, attachments)
        except BusySessionError:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_busy",
                    "message": "This workflow does not accept follow-up messages while active.",
                },
            )
        except ClosedSessionError:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_closed",
                    "message": "This session has already been closed.",
                },
            )
        return {
            "status": "ok",
            "message": _serialize_message(result["message"], session_id, proxy_secret),
            "followup_action": result["followup_action"],
        }

    @app.post("/dev/inject")
    async def dev_inject(req: _DevInjectRequest):
        thread_id = str(uuid.uuid4())
        attachments = []
        for a in req.attachments:
            try:
                data = base64.b64decode(a.get("data", ""))
            except Exception:
                data = b""
            attachments.append(Attachment(
                filename=a.get("filename", "attachment"),
                content_type=a.get("content_type", "application/octet-stream"),
                data=data,
            ))
        message = InboundMessage(
            channel=req.channel,
            sender=req.sender,
            thread_id=thread_id,
            subject=req.subject,
            body=req.body,
            attachments=attachments,
            received_at=datetime.now(timezone.utc),
        )
        try:
            session = await session_manager.handle_message(message)
        except NoWorkflowMatch as e:
            raise HTTPException(status_code=422, detail=str(e))
        return {"session_id": session.session_id, "thread_id": thread_id}

    @app.delete("/sessions/{session_id}/execution")
    async def stop_execution(session_id: str):
        stopped = await session_manager.stop_execution(session_id)
        if not stopped:
            raise HTTPException(status_code=404, detail="No active execution")
        return {"status": "stopped"}

    @app.post("/sessions/{session_id}/fatal-error")
    async def report_fatal_error(session_id: str, req: _FatalErrorRequest, request: Request):
        _require_session_token(request, session_id, proxy_secret)
        try:
            await session_manager.record_agent_fatal_error(
                session_id, category=req.category, reason=req.reason
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"status": "recorded"}

    @app.post("/sessions/{session_id}/close")
    async def close_session(session_id: str, request: Request):
        _require_session_token(request, session_id, proxy_secret)
        closed = await session_manager.close_session(session_id)
        if not closed:
            raise HTTPException(status_code=404, detail="Session not found")
        return {"status": "closed"}

    @app.get("/sessions/{session_id}/executions")
    async def list_executions(session_id: str):
        executions = await session_manager.list_executions(session_id)
        return [_serialize_execution(e) for e in executions]

    @app.get("/executions/{execution_id}/logs")
    async def get_execution_logs(execution_id: str):
        return await session_manager.list_execution_logs(execution_id)

    @app.get("/executions/{execution_id}/proxy-logs")
    async def get_proxy_logs(execution_id: str):
        return await session_manager.list_proxy_logs(execution_id)

    @app.get("/executions/{execution_id}/conversation-events")
    async def get_conversation_events(execution_id: str):
        return await session_manager.list_conversation_events(execution_id)

    @app.post("/sessions/{session_id}/conversation-events")
    async def post_conversation_event(session_id: str, req: _ConversationEventRequest):
        logged_at = datetime.fromisoformat(req.logged_at) if req.logged_at else None
        try:
            event = await session_manager.append_active_execution_conversation_event(
                session_id,
                source=req.source,
                event_type=req.event_type,
                event_subtype=req.event_subtype,
                payload=req.payload,
                logged_at=logged_at,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return event

    @app.get("/sessions/{session_id}/conversation")
    async def get_conversation(session_id: str, request: Request):
        _require_session_token(request, session_id, proxy_secret)
        return await session_manager._db.get_conversation(session_id)

    @app.get("/sessions/{session_id}/messages/{message_id}/attachments/{filename:path}")
    async def get_attachment(session_id: str, message_id: str, filename: str, request: Request):
        _require_session_token(request, session_id, proxy_secret)
        if attachment_store is None:
            raise HTTPException(status_code=501, detail="No attachment store configured")
        att = await session_manager._db.get_attachment_by_name(message_id, filename)
        if att is None:
            raise HTTPException(status_code=404, detail="Attachment not found")
        try:
            data = await attachment_store.read(att["storage_key"])
        except Exception:
            raise HTTPException(status_code=404, detail="Attachment data not found in store")
        return Response(content=data, media_type=att["content_type"])

    @app.get("/")
    async def root_redirect():
        return RedirectResponse(url="/ui/")

    if proxy_secret:
        from claudius.controller.proxy import create_proxy_routes
        create_proxy_routes(
            app,
            session_manager._db,
            proxy_upstream or build_proxy_upstream("anthropic", "https://api.anthropic.com", "", "x-api-key"),
            proxy_secret,
            broker=broker,
            log_conversation=session_manager._log_conversation,
        )

    if _UI_DIR.exists():
        app.mount("/ui", _SPAStaticFiles(directory=str(_UI_DIR), html=True), name="ui")

    return app
