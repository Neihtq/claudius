import asyncio
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
from loguru import logger

from claudius.channels.base import AbstractChannel
from claudius.config.template import TemplateContext
from claudius.config.schema import WorkflowConfig
from claudius.controller.attachments import AttachmentStore
from claudius.controller.backends.base import AbstractBackend, ExecutionStartupError
from claudius.controller.db import Database
from claudius.controller.proxy import mint_token
from claudius.controller.router import match_workflow
from claudius.controller.sse import SSEBroker
from claudius.models import (
    Attachment,
    ContainerLifecycleStatus,
    Execution,
    ExecutionContainer,
    ExecutionPhase,
    InboundMessage,
    LogLine,
    Session,
    SessionState,
    ToolMount,
)
from claudius.runtime import (
    EnvironmentSecretProvider,
    SecretProvider,
    build_runtime_sidecar_payload,
    new_runtime_auth_token,
    resolve_runtime,
    write_external_mcp_config,
    write_runtime_bridge_files,
)
from claudius.session.workspace import Workspace


class NoWorkflowMatch(Exception):
    pass


class BusySessionError(Exception):
    def __init__(self, session_id: str):
        super().__init__(f"Session {session_id} is busy")
        self.session_id = session_id


class ClosedSessionError(Exception):
    def __init__(self, session_id: str):
        super().__init__(f"Session {session_id} is closed")
        self.session_id = session_id


_USER_SAFE_LAUNCH_ERROR = (
    "I couldn’t start the workspace for this request. Please try again in a moment."
)

_USER_SAFE_DELIVERY_ERROR = (
    "Claude never received this message because the workspace failed to start."
)

_RUNTIME_MCP_FAILURE_ERROR = (
    "I couldn’t start the workflow tools for this request, so I stopped before making unsafe "
    "changes. Please try again in a moment."
)

_GRACEFUL_STOP_TIMEOUT_SECONDS = 10.0
_FORCED_STOP_TAIL_DRAIN_SECONDS = 1.0
_WORKER_STOP_HTTP_TIMEOUT_SECONDS = 2.0
# Perpetual workflows auto-restart their loop after an execution ends, but only
# if the execution ran at least this long — guarding against crash-loop spin.
_PERPETUAL_MIN_RUNTIME_SECONDS = 30.0


@dataclass
class _StopRequest:
    reason: str
    restart_if_pending: bool
    final_state: SessionState


class SessionManager:
    def __init__(
        self,
        db: Database,
        backend: AbstractBackend,
        workflows: list[WorkflowConfig],
        workspaces_path: str,
        channels: dict[str, AbstractChannel],
        broker: SSEBroker | None = None,
        callback_url: str = "",
        proxy_secret: str = "",
        attachment_store: AttachmentStore | None = None,
        log_conversation: bool = False,
        secret_provider: SecretProvider | None = None,
        resend_api_key: str = "",
        resend_from_address: str = "claudius@example.com",
    ):
        self._db = db
        self._backend = backend
        self._workflows = workflows
        self._workspaces_path = str(Path(workspaces_path).expanduser().resolve())
        self._channels = channels
        self._broker = broker
        self._callback_url = callback_url
        self._proxy_secret = proxy_secret
        self._attachment_store = attachment_store
        self._log_conversation = log_conversation
        self._secret_provider = secret_provider or EnvironmentSecretProvider()
        self._resend_api_key = resend_api_key
        self._resend_from_address = resend_from_address
        self._tail_tasks: dict[str, asyncio.Task] = {}
        self._idle_stop_tasks: dict[str, asyncio.Task] = {}
        self._interrupt_after_turn: dict[str, str] = {}
        self._agent_fatal_errors: set[str] = set()
        self._stop_requests: dict[str, _StopRequest] = {}

    # -------------------------------------------------------------------------
    # Startup recovery
    # -------------------------------------------------------------------------

    async def shutdown(self, timeout: float = 5.0) -> None:
        tasks = [*self._tail_tasks.values(), *self._idle_stop_tasks.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=timeout)

    async def recover(self) -> None:
        active = await self._db.list_active_executions()
        for execution in active:
            if execution.phase == ExecutionPhase.STARTING:
                logger.warning(
                    "failing unrecoverable startup execution execution_id={} session_id={}",
                    execution.execution_id,
                    execution.session_id,
                )
                try:
                    await self._backend.delete_execution(execution)
                except Exception as exc:
                    logger.warning(
                        "failed to delete unrecoverable startup execution execution_id={} error={}",
                        execution.execution_id,
                        exc,
                    )
                await self._db.halt_execution(execution.execution_id, "startup_failed")
                await self._db.update_session_last_execution_result(execution.session_id, "failed")
                await self._db.update_session_state(execution.session_id, SessionState.ERROR)
                continue
            since = await self._db.get_last_log_timestamp(execution.execution_id)
            logger.info(
                f"recovering execution execution_id={execution.execution_id} since={since}"
            )
            self._start_log_tailer(execution, since=since)

    # -------------------------------------------------------------------------
    # Log tailer
    # -------------------------------------------------------------------------

    def _start_log_tailer(self, execution: Execution, since: datetime | None = None) -> None:
        task = asyncio.create_task(self._tail_task(execution, since=since))
        self._tail_tasks[execution.execution_id] = task

    def _clear_interrupt_after_turn(self, execution_id: str | None) -> None:
        if execution_id is None:
            return
        self._interrupt_after_turn.pop(execution_id, None)

    def _clear_idle_stop(self, execution_id: str | None) -> None:
        if execution_id is None:
            return
        task = self._idle_stop_tasks.pop(execution_id, None)
        if task and task is not asyncio.current_task():
            task.cancel()

    def _clear_execution_bookkeeping(self, execution_id: str | None) -> None:
        self._clear_interrupt_after_turn(execution_id)
        self._clear_idle_stop(execution_id)
        if execution_id is not None:
            self._agent_fatal_errors.discard(execution_id)
            self._stop_requests.pop(execution_id, None)

    def _workflow_idle_timeout_seconds(self, workflow_name: str) -> float:
        workflow = next((w for w in self._workflows if w.name == workflow_name), None)
        if workflow is None:
            return 60.0
        return float(workflow.session.idle_timeout_seconds)

    def _arm_idle_stop(self, execution: Execution, timeout_seconds: float) -> None:
        self._clear_idle_stop(execution.execution_id)
        task = asyncio.create_task(
            self._idle_stop_task(
                execution_id=execution.execution_id,
                session_id=execution.session_id,
                timeout_seconds=timeout_seconds,
            )
        )
        self._idle_stop_tasks[execution.execution_id] = task

    async def _idle_stop_task(
        self,
        *,
        execution_id: str,
        session_id: str,
        timeout_seconds: float,
    ) -> None:
        try:
            await asyncio.sleep(max(0.0, timeout_seconds))
            active_execution = await self._db.get_active_execution(session_id)
            if active_execution is None or active_execution.execution_id != execution_id:
                return
            await self.stop_execution(
                session_id,
                reason="idle_timeout",
                restart_if_pending=True,
            )
        except asyncio.CancelledError:
            raise
        finally:
            task = self._idle_stop_tasks.get(execution_id)
            if task is asyncio.current_task():
                self._idle_stop_tasks.pop(execution_id, None)

    async def _tail_task(self, execution: Execution, since: datetime | None = None) -> None:
        try:
            async for line in self._backend.tail_logs(execution, since=since):
                if since is not None and line.logged_at <= since:
                    continue
                await self._db.append_execution_log(execution.execution_id, line)
                if self._broker:
                    await self._broker.publish(execution.session_id, {
                        "type": "log",
                        "execution_id": execution.execution_id,
                        "logged_at": line.logged_at.isoformat(),
                        "stream": line.stream,
                        "body": line.body,
                    })
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(
                f"log tail error execution_id={execution.execution_id} error={e}"
            )
            halted_at = datetime.now(timezone.utc)
            execution.halted_at = halted_at
            execution.halt_reason = "tail_error"
            execution.phase = ExecutionPhase.FAILED
            await self._db.halt_execution(execution.execution_id, "tail_error")
            await self._db.update_session_last_execution_result(execution.session_id, "failed")
            await self._db.update_session_state(execution.session_id, SessionState.ERROR)
            if self._broker:
                await self._broker.publish(execution.session_id, {
                    "type": "execution",
                    "execution": self._serialize_execution(execution),
                })
                await self._broker.publish(execution.session_id, {
                    "type": "status",
                    "state": SessionState.ERROR.value,
                })
            self._clear_execution_bookkeeping(execution.execution_id)
            self._tail_tasks.pop(execution.execution_id, None)
            return

        # Natural exit
        exit_code = await self._backend.get_exit_code(execution)
        stop_request = self._stop_requests.get(execution.execution_id)
        halt_reason = stop_request.reason if stop_request else "exited"
        last_result = (
            "failed"
            if halt_reason == "agent_fatal_error"
            else ("ok" if stop_request else ("ok" if exit_code == 0 else "failed"))
        )
        logger.info(
            f"execution exited execution_id={execution.execution_id} "
            f"exit_code={exit_code} result={last_result} halt_reason={halt_reason}"
        )
        halted_at = datetime.now(timezone.utc)
        await self._apply_fallback_execution_metadata(execution.execution_id, halted_at)
        await self._db.halt_execution(execution.execution_id, halt_reason, exit_code=exit_code)
        await self._db.update_session_last_execution_result(execution.session_id, last_result)
        refreshed_execution = await self._db.get_execution(execution.execution_id)
        if refreshed_execution is None:
            refreshed_execution = execution
        refreshed_execution.halted_at = halted_at
        refreshed_execution.halt_reason = halt_reason
        refreshed_execution.exit_code = exit_code
        refreshed_execution.phase = (
            ExecutionPhase.FINISHED
            if halt_reason == "exited" and exit_code == 0
            else ExecutionPhase.FAILED
        )

        if self._broker:
            await self._broker.publish(execution.session_id, {
                "type": "execution",
                "execution": self._serialize_execution(refreshed_execution),
            })

        try:
            await self._backend.delete_execution(refreshed_execution)
        except Exception as e:
            logger.warning(
                f"failed to delete execution execution_id={execution.execution_id} error={e}"
            )
        finally:
            self._clear_execution_bookkeeping(execution.execution_id)
            self._tail_tasks.pop(execution.execution_id, None)

        if stop_request:
            if stop_request.restart_if_pending:
                await self._resume_or_transition(
                    execution.session_id,
                    stop_request.final_state,
                )
            else:
                await self._db.update_session_state(execution.session_id, stop_request.final_state)
                if self._broker:
                    await self._broker.publish(execution.session_id, {
                        "type": "status",
                        "state": stop_request.final_state.value,
                    })
            return

        await self._maybe_enqueue_perpetual_continuation(
            execution.session_id, refreshed_execution, last_result
        )
        await self._resume_or_transition(execution.session_id, SessionState.HIBERNATED)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def handle_message(self, message: InboundMessage) -> Session:
        session = await self._db.get_session_by_thread(message.thread_id)

        if session is None:
            return await self._create_session(message)

        await self._refresh_session_channel_metadata(session.session_id, message)
        session = await self._db.get_session(session.session_id) or session

        if session.state == SessionState.HIBERNATED:
            await self._store_inbound_message(
                session.session_id,
                message.body,
                message.attachments,
                sender=message.sender,
            )
            session = await self._resume_session(session)
            return session

        if session.state in (SessionState.WAITING, SessionState.ACTIVE):
            await self._handle_existing_session_followup(
                session,
                body=message.body,
                attachments=message.attachments,
                sender=message.sender,
            )
            return session

        # CLOSED / ERROR / NEW / other terminal-ish states — store but don't route
        await self._store_inbound_message(
            session.session_id,
            message.body,
            message.attachments,
            sender=message.sender,
        )
        return session

    async def stop_execution(
        self,
        session_id: str,
        *,
        reason: str = "stopped",
        restart_if_pending: bool = True,
        final_state: SessionState = SessionState.HIBERNATED,
    ) -> bool:
        """Stop the active execution for a session. Returns True if an execution was stopped."""
        execution = await self._db.get_active_execution(session_id)
        if execution is None:
            return False
        self._stop_requests[execution.execution_id] = _StopRequest(
            reason=reason,
            restart_if_pending=restart_if_pending,
            final_state=final_state,
        )
        tail_task = self._tail_tasks.get(execution.execution_id)

        await self._request_worker_stop(execution)
        if tail_task:
            try:
                await asyncio.wait_for(
                    asyncio.shield(tail_task),
                    timeout=_GRACEFUL_STOP_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

        if not await self._is_execution_active(session_id, execution.execution_id):
            return True

        try:
            await self._backend.delete_execution(execution)
        except Exception as e:
            logger.warning(f"stop_execution: delete failed execution_id={execution.execution_id} error={e}")

        tail_task = self._tail_tasks.get(execution.execution_id)
        if tail_task:
            try:
                await asyncio.wait_for(
                    asyncio.shield(tail_task),
                    timeout=_FORCED_STOP_TAIL_DRAIN_SECONDS,
                )
            except asyncio.TimeoutError:
                self._tail_tasks.pop(execution.execution_id, None)
                tail_task.cancel()
                try:
                    await tail_task
                except (asyncio.CancelledError, Exception):
                    pass

        if not await self._is_execution_active(session_id, execution.execution_id):
            return True

        halted_at = datetime.now(timezone.utc)
        await self._apply_fallback_execution_metadata(execution.execution_id, halted_at)
        await self._db.halt_execution(execution.execution_id, reason)
        last_result = "failed" if reason == "agent_fatal_error" else "ok"
        await self._db.update_session_last_execution_result(session_id, last_result)
        refreshed_execution = await self._db.get_execution(execution.execution_id)
        if refreshed_execution is None:
            refreshed_execution = execution
        refreshed_execution.halted_at = halted_at
        refreshed_execution.halt_reason = reason
        refreshed_execution.phase = ExecutionPhase.FAILED
        self._clear_execution_bookkeeping(execution.execution_id)
        if self._broker:
            await self._broker.publish(session_id, {
                "type": "execution",
                "execution": self._serialize_execution(refreshed_execution),
            })
        if restart_if_pending:
            await self._resume_or_transition(session_id, final_state)
        else:
            await self._db.update_session_state(session_id, final_state)
            if self._broker:
                await self._broker.publish(session_id, {
                    "type": "status",
                    "state": final_state.value,
                })
        return True

    async def get_session(self, session_id: str) -> Session | None:
        return await self._db.get_session(session_id)

    async def close_session(self, session_id: str) -> bool:
        session = await self._db.get_session(session_id)
        if session is None:
            return False
        if session.state == SessionState.CLOSED:
            return True

        stopped = await self.stop_execution(
            session_id,
            reason="closed",
            restart_if_pending=False,
            final_state=SessionState.CLOSED,
        )
        if not stopped:
            await self._db.update_session_state(session_id, SessionState.CLOSED)
            if self._broker:
                await self._broker.publish(session_id, {
                    "type": "status",
                    "state": SessionState.CLOSED.value,
                })
        return True

    async def get_messages(self, session_id: str) -> list[dict]:
        return await self._db.list_messages(session_id)

    async def delete_pending_message(self, session_id: str, message_id: str) -> None:
        messages = await self._db.list_messages(session_id)
        message = next((item for item in messages if item["message_id"] == message_id), None)
        if message is None:
            raise ValueError(f"Message {message_id} not found")
        if message["direction"] != "inbound" or message["delivery_status"] != "pending":
            raise RuntimeError("Only pending inbound messages can be deleted")

        deleted = await self._db.delete_pending_message(session_id, message_id)
        if deleted is None:
            raise RuntimeError("Message is no longer pending")

        if self._attachment_store is not None:
            for storage_key in deleted.get("storage_keys", []):
                try:
                    await self._attachment_store.delete(storage_key)
                except Exception as exc:
                    logger.warning(
                        "failed to delete attachment for pending message session_id={} message_id={} key={} error={}",
                        session_id,
                        message_id,
                        storage_key,
                        exc,
                    )

        if self._broker:
            await self._broker.publish(session_id, {
                "type": "message_deleted",
                "message_id": message_id,
            })

    async def resend_outbound_message(self, session_id: str, message_id: str) -> None:
        message = await self._db.get_message(session_id, message_id)
        if message is None:
            raise ValueError(f"Message {message_id} not found")
        if message["direction"] != "outbound":
            raise RuntimeError("Only outbound messages can be resent")

        session = await self._db.get_session(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found")
        channel = self._channels.get(session.channel)
        if channel is None:
            raise RuntimeError(f"Channel {session.channel!r} is not configured")

        messages = await self._db.list_messages(session_id)
        reply_to = next(
            (
                item["sender"]
                for item in messages
                if item["direction"] == "inbound" and item.get("sender")
            ),
            "",
        )
        if not reply_to:
            raise RuntimeError("No inbound sender found for this session")

        attachments: list[Attachment] = []
        for attachment in message.get("attachments", []):
            filename = attachment.get("filename")
            if not isinstance(filename, str) or not filename:
                continue
            if self._attachment_store is None:
                raise RuntimeError("No attachment store configured")
            stored = await self._db.get_attachment_by_name(message_id, filename)
            if stored is None:
                raise RuntimeError(f"Attachment {filename!r} not found for message")
            data = await self._attachment_store.read(stored["storage_key"])
            attachments.append(Attachment(
                filename=filename,
                content_type=stored["content_type"],
                data=data,
            ))

        await channel.send_message(
            to=reply_to,
            body=message["body"],
            thread_id=session.thread_id,
            attachments=attachments,
        )

    async def get_session_history(
        self,
        session_id: str,
        *,
        side: str = "both",
        limit: int = 20,
        query: str = "",
    ) -> list[dict]:
        return await self._db.get_session_history(
            session_id,
            side=side,
            limit=limit,
            query=query,
        )

    def list_workflow_names(self) -> list[str]:
        return [w.name for w in self._workflows]

    async def list_executions(self, session_id: str) -> list[Execution]:
        return await self._db.list_executions(session_id)

    async def list_execution_logs(self, execution_id: str) -> list[dict]:
        return await self._db.list_execution_logs(execution_id)

    async def list_proxy_logs(self, execution_id: str) -> list[dict]:
        return await self._db.list_proxy_logs(execution_id)

    async def list_conversation_events(self, execution_id: str) -> list[dict]:
        return await self._db.list_conversation_events(execution_id)

    async def append_active_execution_conversation_event(
        self,
        session_id: str,
        *,
        source: str,
        event_type: str,
        payload: dict,
        event_subtype: str | None = None,
        logged_at: datetime | None = None,
    ) -> dict:
        execution = await self._db.get_active_execution(session_id)
        if execution is None:
            raise ValueError(f"No active execution for session {session_id}")
        session = await self._db.get_session(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found")
        if source == "claude" and event_type == "system" and payload.get("subtype") == "init":
            claude_session_id = payload.get("session_id")
            if isinstance(claude_session_id, str) and claude_session_id:
                await self._db.update_execution_claude_session_id(
                    execution.execution_id, claude_session_id
                )
                execution.claude_session_id = claude_session_id
        if source == "claude" and event_type == "result":
            num_turns = payload.get("num_turns")
            duration_ms = payload.get("duration_ms")
            await self._db.update_execution_claude_result(
                execution.execution_id,
                num_turns=num_turns if isinstance(num_turns, int) else None,
                duration_ms=duration_ms if isinstance(duration_ms, int) else None,
            )
            refreshed_execution = await self._db.get_execution(execution.execution_id)
            if refreshed_execution is not None:
                await self._publish_execution_event(refreshed_execution)
        event = await self._db.append_conversation_event(
            execution.execution_id,
            source=source,
            event_type=event_type,
            event_subtype=event_subtype,
            payload=payload,
            logged_at=logged_at,
        )
        await self._publish_conversation_event(session_id, event)
        if (
            source == "claude"
            and event_type == "result"
            and execution.execution_id in self._interrupt_after_turn
        ):
            is_agent_error = execution.execution_id in self._agent_fatal_errors
            halt_reason = "agent_fatal_error" if is_agent_error else "interrupt_after_turn"
            self._clear_execution_bookkeeping(execution.execution_id)
            await self.stop_execution(
                session_id,
                reason=halt_reason,
                restart_if_pending=not is_agent_error,
                final_state=SessionState.ERROR if is_agent_error else SessionState.HIBERNATED,
            )
            return event
        if source == "claude" and event_type == "result":
            self._arm_idle_stop(
                execution,
                timeout_seconds=self._workflow_idle_timeout_seconds(session.workflow_name),
            )
        if (
            source == "claude"
            and event_type == "system"
            and payload.get("subtype") == "init"
            and self._workflow_requires_runtime_tools(session.workflow_name)
        ):
            failure = self._runtime_mcp_failure(payload)
            if failure is not None:
                await self._record_runtime_mcp_failure(
                    session_id,
                    execution.execution_id,
                    payload={
                        "message": (
                            f"Runtime MCP server {failure['name']!r} was unavailable at Claude "
                            f"startup (status={failure['status']!r})."
                        ),
                        "mcp_server": failure["name"],
                        "status": failure["status"],
                    },
                )
                await self._fail_active_execution(
                    session,
                    execution,
                    halt_reason="runtime_mcp_failed",
                    user_message=_RUNTIME_MCP_FAILURE_ERROR,
                )
        return event

    async def record_agent_fatal_error(
        self, session_id: str, *, category: str, reason: str
    ) -> None:
        """Record a fatal error reported by the agent and schedule turn-end interruption."""
        execution = await self._db.get_active_execution(session_id)
        if execution is None:
            raise ValueError(f"No active execution for session {session_id}")
        await self._db.update_execution_agent_error(execution.execution_id, category, reason)
        execution.agent_error_category = category
        execution.agent_error_reason = reason
        self._agent_fatal_errors.add(execution.execution_id)
        self._interrupt_after_turn[execution.execution_id] = session_id

    async def increment_active_execution_token_usage(
        self,
        session_id: str,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_creation_input_tokens: int = 0,
        cache_read_input_tokens: int = 0,
        total_cost_usd: float | None = None,
    ) -> None:
        execution = await self._db.get_active_execution(session_id)
        if execution is None:
            return
        await self._db.increment_execution_token_usage(
            execution.execution_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            total_cost_usd=total_cost_usd,
        )
        refreshed_execution = await self._db.get_execution(execution.execution_id)
        if refreshed_execution is not None:
            await self._publish_execution_event(refreshed_execution)

    async def _request_worker_stop(self, execution: Execution) -> bool:
        if not execution.worker_address:
            return False
        headers: dict[str, str] = {}
        if self._proxy_secret:
            headers["Authorization"] = f"Bearer {mint_token(execution.session_id, self._proxy_secret)}"
        try:
            async with httpx.AsyncClient(timeout=_WORKER_STOP_HTTP_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"http://{execution.worker_address}/shutdown",
                    headers=headers,
                )
            return response.status_code < 400
        except Exception as exc:
            logger.info(
                "worker graceful stop failed session_id={} execution_id={} error={}",
                execution.session_id,
                execution.execution_id,
                exc,
            )
            return False

    async def _is_execution_active(self, session_id: str, execution_id: str) -> bool:
        active_execution = await self._db.get_active_execution(session_id)
        return active_execution is not None and active_execution.execution_id == execution_id

    async def _apply_fallback_execution_metadata(
        self,
        execution_id: str,
        halted_at: datetime,
    ) -> None:
        execution = await self._db.get_execution(execution_id)
        if execution is None or execution.claude_duration_ms is not None:
            return
        duration_ms = max(
            0,
            int((halted_at - execution.started_at).total_seconds() * 1000),
        )
        await self._db.update_execution_claude_result(
            execution_id,
            duration_ms=duration_ms,
        )

    async def receive_outbound(
        self, session_id: str, body: str, attachments: list[Attachment] | None = None
    ) -> None:
        atts = attachments or []
        att_dicts = [{"filename": a.filename, "content_type": a.content_type} for a in atts]
        message_id = await self._db.store_message(session_id, "outbound", body, att_dicts)
        await self._store_message_attachments(session_id, message_id, atts)
        if self._broker:
            await self._publish_message_event(
                session_id,
                {
                    "message_id": message_id,
                    "direction": "outbound",
                    "body": body,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "attachments": att_dicts,
                    "acknowledged_at": None,
                    "delivery_status": "acknowledged",
                },
            )

        session = await self._db.get_session(session_id)
        if session is None:
            logger.warning("cannot deliver outbound message for missing session session_id={}", session_id)
            return
        channel = self._channels.get(session.channel)
        if channel is None:
            logger.warning(
                "cannot deliver outbound message: channel not configured session_id={} channel={}",
                session_id,
                session.channel,
            )
            return

        messages = await self._db.list_messages(session_id)
        reply_to = next(
            (
                message["sender"]
                for message in messages
                if message["direction"] == "inbound" and message.get("sender")
            ),
            "",
        )
        if not reply_to:
            logger.warning(
                "cannot deliver outbound message: no inbound sender found session_id={}",
                session_id,
            )
            return

        try:
            await channel.send_message(
                to=reply_to,
                body=body,
                thread_id=session.thread_id,
                attachments=atts,
            )
            logger.info(
                "outbound message delivered session_id={} channel={} to={}",
                session_id,
                session.channel,
                reply_to,
            )
        except Exception as exc:
            logger.exception(
                "failed to deliver outbound message session_id={} channel={} to={} error={}",
                session_id,
                session.channel,
                reply_to,
                exc,
            )

    async def continue_session(
        self, session_id: str, body: str, attachments: list | None = None
    ) -> dict:
        session = await self._db.get_session(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found")
        if session.state == SessionState.CLOSED:
            raise ClosedSessionError(session_id)
        if session.state == SessionState.HIBERNATED:
            message_record = await self._store_inbound_message(
                session_id,
                body,
                attachments or [],
                sender="dev@localhost",
            )
            await self._resume_session(session)
            return {"message": message_record, "followup_action": "resumed"}
        message_record, followup_action = await self._handle_existing_session_followup(
            session,
            body=body,
            attachments=attachments or [],
            sender="dev@localhost",
        )
        return {"message": message_record, "followup_action": followup_action}

    def _workflow_for_session(self, session: Session) -> WorkflowConfig:
        workflow = next((w for w in self._workflows if w.name == session.workflow_name), None)
        if workflow is None:
            raise NoWorkflowMatch(f"Workflow {session.workflow_name!r} no longer exists")
        return workflow

    async def _store_inbound_message(
        self,
        session_id: str,
        body: str,
        attachments: list[Attachment],
        *,
        sender: str,
    ) -> dict:
        att_dicts = [{"filename": a.filename, "content_type": a.content_type} for a in attachments]
        received_at = datetime.now(timezone.utc).isoformat()
        message_id = await self._db.store_message(
            session_id,
            "inbound",
            body,
            att_dicts,
            sender=sender,
        )
        await self._store_message_attachments(session_id, message_id, attachments)
        message_record = {
            "message_id": message_id,
            "direction": "inbound",
            "body": body,
            "received_at": received_at,
            "attachments": att_dicts,
            "acknowledged_at": None,
            "delivery_status": "pending",
        }
        await self._db.update_session_last_message_at(session_id, received_at)
        if self._broker:
            await self._publish_message_event(session_id, message_record)
        return message_record

    async def _handle_existing_session_followup(
        self,
        session: Session,
        *,
        body: str,
        attachments: list[Attachment],
        sender: str,
    ) -> tuple[dict, str]:
        workflow = self._workflow_for_session(session)
        policy = workflow.session.active_followup_policy
        active_execution = await self._db.get_active_execution(session.session_id)
        if active_execution is None:
            if policy == "reject":
                raise BusySessionError(session.session_id)
            message_record = await self._store_inbound_message(
                session.session_id,
                body,
                attachments,
                sender=sender,
            )
            await self._resume_session(session)
            return message_record, "resumed"

        if policy == "reject":
            raise BusySessionError(session.session_id)

        self._clear_idle_stop(active_execution.execution_id)
        message_record = await self._store_inbound_message(
            session.session_id,
            body,
            attachments,
            sender=sender,
        )
        followup_action = "queued"
        if policy == "interrupt":
            followup_action = "interrupting"
            await self._publish_followup_event(session.session_id, followup_action)
            await self.stop_execution(
                session.session_id,
                reason="interrupted",
                restart_if_pending=True,
            )
        elif policy == "interrupt_after_turn":
            self._interrupt_after_turn[active_execution.execution_id] = session.session_id
            followup_action = "interrupt_after_turn"
            await self._publish_followup_event(session.session_id, followup_action)
        else:
            await self._publish_followup_event(session.session_id, followup_action)
        return message_record, followup_action

    async def list_sessions(self) -> list[Session]:
        return await self._db.list_sessions()

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    async def _store_message_attachments(
        self, session_id: str, message_id: str, attachments: list
    ) -> None:
        if self._attachment_store is None:
            return
        for att in attachments:
            if not att.data:
                continue
            key = f"{session_id}/{message_id}/{att.filename}"
            await self._attachment_store.write(key, att.data)
            await self._db.store_attachment(message_id, att.filename, att.content_type, key)

    async def _create_session(self, message: InboundMessage) -> Session:
        workflow = match_workflow(message, self._workflows)
        if workflow is None:
            raise NoWorkflowMatch(
                "No workflow matches "
                f"channel={message.channel} "
                f"sender={message.sender} "
                f"recipients={message.recipients} "
                f"subject={message.subject!r}"
            )

        session_id = str(uuid.uuid4())
        workspace = Workspace(base_path=self._workspaces_path, session_id=session_id)
        workspace.setup()

        session = Session(
            session_id=session_id,
            thread_id=message.thread_id,
            channel=message.channel,
            workflow_name=workflow.name,
            state=SessionState.NEW,
            workspace_path=str(workspace.path),
            created_at=datetime.now(timezone.utc),
            last_message_at=datetime.now(timezone.utc),
            channel_metadata=self._channel_metadata_for_message(message),
        )
        await self._db.create_session(session)
        logger.info(
            f"session created session_id={session_id} workflow={workflow.name} channel={message.channel}"
        )

        attachments = [
            {"filename": a.filename, "content_type": a.content_type}
            for a in message.attachments
        ]
        message_id = await self._db.store_message(
            session_id, "inbound", message.body, attachments, sender=message.sender
        )
        await self._store_message_attachments(session_id, message_id, message.attachments)
        if self._broker:
            await self._publish_message_event(
                session_id,
                {
                    "message_id": message_id,
                    "direction": "inbound",
                    "body": message.body,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "attachments": attachments,
                    "acknowledged_at": None,
                    "delivery_status": "pending",
                },
            )

        try:
            self._ensure_output_dir(workspace.path, message_id)
            initial_prompt = self._format_message_for_claude(
                message.body,
                message_id=message_id,
                attachments=attachments,
            )
            extra_env = self._build_worker_env(
                workflow,
                message,
                session_id,
                conversation_text=initial_prompt,
                output_message_id=message_id,
            )
            tool_mounts = self._build_runtime_tool_mounts_for_backend(
                workflow,
                message,
                session_id,
                session.workspace_path,
                hook_phases=["session_start", "execution_start"],
                worker_env=extra_env,
            )
            execution = self._new_execution(session_id)
            self._prime_execution_containers(execution, session_id, tool_mounts)
            await self._db.create_execution(execution)
            await self._publish_execution_event(execution)
            logger.info(f"launching execution session_id={session_id} workflow={workflow.name}")
            execution = await self._backend.create_execution(
                session,
                workflow,
                tool_mounts,
                extra_env=extra_env,
                execution_id=execution.execution_id,
                started_at=execution.started_at,
            )
            await self._db.update_execution_worker_address(
                execution.execution_id,
                execution.worker_address,
            )
            await self._db.update_execution_status(
                execution.execution_id,
                phase=execution.phase,
                runtime_container=execution.runtime_container,
                worker_container=execution.worker_container,
            )
            self._start_log_tailer(execution)
            await self._record_local_input_event(
                session_id,
                execution.execution_id,
                message.body,
                attachments,
                sender=message.sender,
                kind="initial_message",
                subject=message.subject,
            )
            acknowledged_at = await self._db.acknowledge_message(message_id)
            await self._publish_message_acknowledged(session_id, message_id, acknowledged_at)
            logger.info(
                f"execution started session_id={session_id} "
                f"execution_id={execution.execution_id} "
                f"worker_address={execution.worker_address}"
            )
            await self._db.update_session_state(session_id, SessionState.ACTIVE)
            if self._broker:
                await self._publish_execution_event(execution)
                await self._broker.publish(session_id, {"type": "status", "state": "active"})
            session.state = SessionState.ACTIVE
        except Exception as e:
            logger.exception(f"launch failed session_id={session_id} error={e}")
            tail_task = self._tail_tasks.pop(execution.execution_id, None) if 'execution' in locals() else None
            if tail_task:
                tail_task.cancel()
                try:
                    await tail_task
                except (asyncio.CancelledError, Exception):
                    pass
            if 'execution' in locals():
                await self._record_startup_failure_logs(execution, e)
                await self._halt_failed_start_execution(execution)
            await self._mark_message_delivery_failed(
                session_id,
                {
                    "message_id": message_id,
                    "direction": "inbound",
                    "body": message.body,
                    "received_at": session.last_message_at.isoformat(),
                    "attachments": attachments,
                    "acknowledged_at": None,
                },
            )
            await self._record_launch_failure(session_id, session)

        return session

    async def _resume_session(self, session: Session) -> Session:
        workflow = self._workflow_for_session(session)

        pending = await self._db.get_pending_inbound_messages(session.session_id)
        if not pending:
            logger.warning(f"_resume_session called with no pending messages session_id={session.session_id}")
            return session

        first = pending[0]
        initial_msg = InboundMessage(
            channel=session.channel,
            sender=first["sender"],
            recipients=[],
            thread_id=session.thread_id,
            subject=None,
            body=first["body"],
            attachments=[],
            received_at=datetime.fromisoformat(first["received_at"]),
        )

        try:
            acknowledged_at = await self._db.acknowledge_messages([p["message_id"] for p in pending])

            workspace = Workspace(base_path=self._workspaces_path, session_id=session.session_id)
            workspace.restore()

            latest_execution = await self._db.get_latest_execution(session.session_id)
            resume_claude_session_id = latest_execution.claude_session_id if latest_execution else None
            output_message_id = pending[-1]["message_id"]
            self._ensure_output_dir(workspace.path, output_message_id)
            conversation_text = await self._build_resume_prompt(pending)
            extra_env = self._build_worker_env(
                workflow,
                initial_msg,
                session.session_id,
                conversation_text=conversation_text,
                claude_resume_session_id=resume_claude_session_id,
                output_message_id=output_message_id,
            )
            tool_mounts = self._build_runtime_tool_mounts_for_backend(
                workflow,
                initial_msg,
                session.session_id,
                session.workspace_path,
                hook_phases=["execution_start"],
                worker_env=extra_env,
            )
            execution = self._new_execution(session.session_id)
            self._prime_execution_containers(execution, session.session_id, tool_mounts)
            await self._db.create_execution(execution)
            await self._publish_execution_event(execution)
            logger.info(f"resuming session session_id={session.session_id}")
            execution = await self._backend.create_execution(
                session,
                workflow,
                tool_mounts,
                extra_env=extra_env,
                execution_id=execution.execution_id,
                started_at=execution.started_at,
            )
            await self._db.update_execution_worker_address(
                execution.execution_id,
                execution.worker_address,
            )
            await self._db.update_execution_status(
                execution.execution_id,
                phase=execution.phase,
                runtime_container=execution.runtime_container,
                worker_container=execution.worker_container,
            )
            self._start_log_tailer(execution)
            for pending_message in pending:
                await self._record_local_input_event(
                    session.session_id,
                    execution.execution_id,
                    pending_message["body"],
                    pending_message["attachments"],
                    sender=pending_message["sender"],
                    kind="hitl_message",
                )
            for pending_message in pending:
                await self._publish_message_acknowledged(
                    session.session_id,
                    pending_message["message_id"],
                    acknowledged_at,
                )
            logger.info(
                f"execution started session_id={session.session_id} "
                f"execution_id={execution.execution_id} "
                f"worker_address={execution.worker_address}"
            )
            await self._db.update_session_state(session.session_id, SessionState.ACTIVE)
            if self._broker:
                await self._publish_execution_event(execution)
                await self._broker.publish(session.session_id, {"type": "status", "state": "active"})
            session.state = SessionState.ACTIVE
            return session
        except Exception as e:
            logger.exception(f"resume failed session_id={session.session_id} error={e}")
            tail_task = self._tail_tasks.pop(execution.execution_id, None) if 'execution' in locals() else None
            if tail_task:
                tail_task.cancel()
                try:
                    await tail_task
                except (asyncio.CancelledError, Exception):
                    pass
            if 'execution' in locals():
                await self._record_startup_failure_logs(execution, e)
                await self._halt_failed_start_execution(execution)
            for pending_message in pending:
                await self._mark_message_delivery_failed(session.session_id, pending_message)
            await self._record_launch_failure(session.session_id, session)
            raise

    def _build_worker_env(
        self,
        workflow: WorkflowConfig,
        message: InboundMessage,
        session_id: str,
        *,
        conversation_text: str,
        claude_resume_session_id: str | None = None,
        output_message_id: str | None = None,
    ) -> dict[str, str]:
        env: dict[str, str] = {
            "HOME": "/workspace/.claude-home",
            "CLAUDIUS_WORKFLOW": workflow.model_dump_json(by_alias=True),
            "CLAUDIUS_INITIAL_MESSAGE": json.dumps({
                "channel": message.channel,
                "sender": message.sender,
                "recipients": message.recipients,
                "thread_id": message.thread_id,
                "subject": message.subject,
                "body": message.body,
                "received_at": message.received_at.isoformat(),
            }),
            "CLAUDIUS_CONVERSATION_TEXT": conversation_text,
            "CLAUDIUS_WORKSPACE_PATH": "/workspace",
            "RESEND_API_KEY": self._resend_api_key,
            "RESEND_FROM_ADDRESS": self._resend_from_address,
            "CLAUDIUS_SESSION_ID": session_id,
        }
        if output_message_id:
            env["CLAUDIUS_OUTPUT_DIR"] = f"/workspace/outputs/{output_message_id}"
        if claude_resume_session_id:
            env["CLAUDIUS_CLAUDE_RESUME_SESSION_ID"] = claude_resume_session_id
        if self._callback_url and self._proxy_secret:
            session_token = mint_token(session_id, self._proxy_secret)
            env["ANTHROPIC_API_KEY"] = session_token
            env["ANTHROPIC_BASE_URL"] = f"{self._callback_url.rstrip('/')}/proxy"
            env["CLAUDIUS_SESSION_TOKEN"] = session_token
        else:
            for key in ("ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"):
                if value := os.environ.get(key):
                    env[key] = value
        if self._callback_url:
            env["CLAUDIUS_CALLBACK_URL"] = self._callback_url
        if self._log_conversation:
            env["CLAUDIUS_LOG_CONVERSATION"] = "1"
        return env

    def _build_runtime_tool_mounts_for_backend(
        self,
        workflow: WorkflowConfig,
        message: InboundMessage,
        session_id: str,
        workspace_path: str,
        *,
        hook_phases: list[str],
        worker_env: dict[str, str],
    ) -> list[ToolMount]:
        """Resolve runtime tool mounts when the controller owns runtime preparation.

        For backends that drive a pre-created worker without a shared filesystem
        (e.g. StaticBackend), the worker resolves its own runtime and writes the
        MCP bridge files locally. In that case we only forward which hook phases
        the worker should run, and return no tool mounts.
        """
        if not self._backend.prepares_runtime_in_controller():
            if workflow.runtime.pre_launch or workflow.runtime.tools or workflow.mcp_servers:
                worker_env["CLAUDIUS_RUNTIME_HOOK_PHASES"] = json.dumps(hook_phases)
            return []
        return self._build_runtime_tool_mounts(
            workflow,
            message,
            session_id,
            workspace_path,
            hook_phases=hook_phases,
            worker_env=worker_env,
        )

    def _build_runtime_tool_mounts(
        self,
        workflow: WorkflowConfig,
        message: InboundMessage,
        session_id: str,
        workspace_path: str,
        *,
        hook_phases: list[str],
        worker_env: dict[str, str],
    ) -> list[ToolMount]:
        runtime = resolve_runtime(
            workflow.runtime,
            template_ctx=TemplateContext(
                request=message,
                session_id=session_id,
                channel_name=message.channel,
            ),
            secret_provider=self._secret_provider,
        )
        extra_mcp_servers = {
            name: server.to_claude_config() for name, server in workflow.mcp_servers.items()
        }
        if not runtime.has_tools() and not runtime.has_hooks() and not extra_mcp_servers:
            return []

        mount = ToolMount()
        if runtime.has_tools():
            for name in (
                "CLAUDIUS_CALLBACK_URL",
                "CLAUDIUS_SESSION_ID",
                "CLAUDIUS_SESSION_TOKEN",
            ):
                value = worker_env.get(name, "").strip()
                if value:
                    runtime.context_env[name] = value
        if runtime.has_hooks() or runtime.has_tools():
            sidecar_name = f"claudius-runtime-{session_id[:8]}"
            auth_token = new_runtime_auth_token()
        if runtime.has_tools():
            endpoint_url = f"http://{sidecar_name}:8090"
            callback_token = worker_env.get("CLAUDIUS_SESSION_TOKEN", "")
            _, mcp_config_path = write_runtime_bridge_files(
                workspace_path,
                spec=runtime,
                endpoint_url=endpoint_url,
                auth_token=auth_token,
                callback_url=self._callback_url or "",
                callback_token=callback_token,
                session_id=session_id,
                extra_mcp_servers=extra_mcp_servers,
            )
            worker_env["CLAUDIUS_MCP_CONFIG"] = f"/workspace/.claudius-runtime/{mcp_config_path.name}"
        elif extra_mcp_servers:
            mcp_config_path = write_external_mcp_config(workspace_path, extra_mcp_servers)
            worker_env["CLAUDIUS_MCP_CONFIG"] = f"/workspace/.claudius-runtime/{mcp_config_path.name}"
            if not runtime.has_hooks():
                return []
        if runtime.has_hooks() or runtime.has_tools():
            mount.sidecars.append({
                "name": sidecar_name,
                "hostname": sidecar_name,
                "command": [
                    "runtime-sidecar",
                    *[item for phase in hook_phases for item in ("--phase", phase)],
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "8090",
                ],
                "environment": {
                    "HOME": "/workspace",
                    "CLAUDIUS_RUNTIME_SPEC_JSON": json.dumps(
                        build_runtime_sidecar_payload(runtime, auth_token)
                    ),
                },
            })
        return [mount]

    async def _build_conversation_text(self, session_id: str) -> str:
        messages = await self._db.list_messages(session_id)
        lines: list[str] = []
        for message in messages:
            if message["direction"] not in {"inbound", "outbound"}:
                continue
            role = "User" if message["direction"] == "inbound" else "Assistant"
            if message["direction"] == "inbound":
                body = self._format_message_for_claude(
                    message["body"],
                    message_id=message.get("message_id"),
                    attachments=message.get("attachments", []),
                ).strip()
            else:
                body = message["body"].strip()
            if not body:
                continue
            lines.append(f"{role}: {body}")
        return "\n\n".join(lines)

    async def _build_resume_prompt(self, pending: list[dict]) -> str:
        if len(pending) == 1:
            message = pending[0]
            return self._format_message_for_claude(
                message["body"],
                message_id=message["message_id"],
                attachments=message.get("attachments", []),
            )
        lines = [
            "The user sent multiple follow-up messages while you were not running.",
            "Process them in this order:",
            "",
        ]
        for index, message in enumerate(pending, start=1):
            formatted = self._format_message_for_claude(
                message["body"],
                message_id=message["message_id"],
                attachments=message.get("attachments", []),
            ).replace("\n", "\n   ")
            lines.append(f"{index}. {formatted}")
        return "\n".join(lines)

    def _format_message_for_claude(
        self,
        body: str,
        *,
        message_id: str | None,
        attachments: list[dict] | list[Attachment],
    ) -> str:
        text = body.strip()
        attachment_lines: list[str] = []
        if message_id:
            for attachment in attachments:
                filename = getattr(attachment, "filename", None)
                if filename is None and isinstance(attachment, dict):
                    filename = attachment.get("filename")
                if not isinstance(filename, str) or not filename:
                    continue
                attachment_lines.append(f"- attachments/{message_id}/{filename}")
            attachment_lines.append(f"- outputs/{message_id}/")
        if not attachment_lines:
            return text
        suffix = "Attachments and outputs:\n" + "\n".join(attachment_lines)
        return f"{text}\n\n{suffix}" if text else suffix

    def _ensure_output_dir(self, workspace_path: Path, message_id: str) -> Path:
        output_dir = workspace_path / "outputs" / message_id
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    async def _record_local_input_event(
        self,
        session_id: str,
        execution_id: str,
        body: str,
        attachments: list[dict],
        *,
        sender: str,
        kind: str,
        subject: str | None = None,
    ) -> None:
        event = await self._db.append_conversation_event(
            execution_id,
            source="local",
            event_type="input",
            event_subtype=kind,
            payload={
                "kind": kind,
                "sender": sender,
                "subject": subject,
                "body": body,
                "attachments": attachments,
            },
        )
        await self._publish_conversation_event(session_id, event)

    async def _maybe_enqueue_perpetual_continuation(
        self,
        session_id: str,
        execution: Execution,
        last_result: str,
    ) -> None:
        """For perpetual workflows, queue a synthetic continuation so the loop
        restarts via the normal resume path.

        This is a safety net: perpetual agents are expected to loop within a
        single long-running execution. If that execution ever ends, we re-arm the
        loop — but only when it ran long enough, so a crashing execution does not
        spin. Failed runs are never auto-restarted.
        """
        session = await self._db.get_session(session_id)
        if session is None:
            return
        workflow = next((w for w in self._workflows if w.name == session.workflow_name), None)
        if workflow is None or not workflow.session.perpetual:
            return
        if last_result != "ok":
            return
        ran_seconds = 0.0
        if execution.halted_at is not None:
            ran_seconds = (execution.halted_at - execution.started_at).total_seconds()
        if ran_seconds < _PERPETUAL_MIN_RUNTIME_SECONDS:
            logger.warning(
                "skipping perpetual restart (ran {:.0f}s < {:.0f}s) session_id={}",
                ran_seconds,
                _PERPETUAL_MIN_RUNTIME_SECONDS,
                session_id,
            )
            return
        if await self._db.get_pending_inbound_messages(session_id):
            return
        logger.info("perpetual restart: enqueuing continuation session_id={}", session_id)
        await self._store_inbound_message(
            session_id,
            "Continue the loop.",
            [],
            sender="perpetual@claudius",
        )

    async def _resume_or_transition(
        self,
        session_id: str,
        final_state: SessionState,
    ) -> None:
        pending = await self._db.get_pending_inbound_messages(session_id)
        if pending:
            logger.info(
                f"found {len(pending)} pending message(s), restarting session_id={session_id}"
            )
            session = await self._db.get_session(session_id)
            if session is not None:
                try:
                    await self._resume_session(session)
                    return
                except Exception as e:
                    logger.error(
                        f"failed to restart for pending messages session_id={session_id} error={e}"
                    )
        await self._db.update_session_state(session_id, final_state)
        if self._broker:
            await self._broker.publish(session_id, {
                "type": "status",
                "state": final_state.value,
            })

    def _channel_metadata_for_message(self, message: InboundMessage) -> dict:
        metadata: dict[str, object] = {"thread_id": message.thread_id}
        if message.sender:
            metadata["sender"] = message.sender
        if message.recipients:
            metadata["recipients"] = list(message.recipients)
        if message.subject:
            metadata["subject"] = message.subject
        return metadata

    async def _refresh_session_channel_metadata(self, session_id: str, message: InboundMessage) -> None:
        session = await self._db.get_session(session_id)
        if session is None:
            return
        next_metadata = dict(session.channel_metadata)
        next_metadata.update(self._channel_metadata_for_message(message))
        if next_metadata != session.channel_metadata:
            await self._db.update_session_channel_metadata(session_id, next_metadata)

    async def _publish_message_event(self, session_id: str, message: dict) -> None:
        if self._broker:
            await self._broker.publish(session_id, {
                "type": "message",
                **message,
            })

    async def _publish_message_acknowledged(
        self,
        session_id: str,
        message_id: str,
        acknowledged_at: str,
    ) -> None:
        if self._broker:
            await self._broker.publish(session_id, {
                "type": "message_acknowledged",
                "message_id": message_id,
                "acknowledged_at": acknowledged_at,
                "delivery_status": "acknowledged",
            })

    async def _mark_message_delivery_failed(self, session_id: str, message: dict) -> None:
        acknowledged_at = await self._db.fail_message_delivery(
            message["message_id"],
            _USER_SAFE_DELIVERY_ERROR,
        )
        if self._broker:
            await self._publish_message_event(
                session_id,
                {
                    "message_id": message["message_id"],
                    "direction": message.get("direction", "inbound"),
                    "body": message.get("body", ""),
                    "received_at": message.get("received_at", datetime.now(timezone.utc).isoformat()),
                    "attachments": message.get("attachments", []),
                    "acknowledged_at": acknowledged_at,
                    "delivery_status": "failed",
                    "delivery_error": _USER_SAFE_DELIVERY_ERROR,
                },
            )

    async def _record_launch_failure(self, session_id: str, session: Session) -> None:
        error_message_id = await self._db.store_message(
            session_id, "error", _USER_SAFE_LAUNCH_ERROR
        )
        if self._broker:
            await self._publish_message_event(
                session_id,
                {
                    "message_id": error_message_id,
                    "direction": "error",
                    "body": _USER_SAFE_LAUNCH_ERROR,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "attachments": [],
                    "acknowledged_at": None,
                    "delivery_status": "acknowledged",
                },
            )
        await self._db.update_session_state(session_id, SessionState.ERROR)
        if self._broker:
            await self._broker.publish(session_id, {"type": "status", "state": "error"})
        session.state = SessionState.ERROR

    def _new_execution(self, session_id: str) -> Execution:
        return Execution(
            execution_id=str(uuid.uuid4()),
            session_id=session_id,
            worker_address=None,
            started_at=datetime.now(timezone.utc),
            halted_at=None,
            halt_reason=None,
            phase=ExecutionPhase.STARTING,
        )

    def _prime_execution_containers(
        self,
        execution: Execution,
        session_id: str,
        tool_mounts: list[ToolMount],
    ) -> None:
        sidecar_name = None
        for mount in tool_mounts:
            if mount.sidecars:
                sidecar_name = mount.sidecars[0]["name"]
                break
        if sidecar_name:
            execution.runtime_container = ExecutionContainer(
                name=sidecar_name,
                status=ContainerLifecycleStatus.STARTING,
            )
        execution.worker_container = ExecutionContainer(
            name=f"claudius-session-{session_id[:8]}",
            status=ContainerLifecycleStatus.PENDING,
        )

    async def _publish_execution_event(self, execution: Execution) -> None:
        if self._broker:
            await self._broker.publish(execution.session_id, {
                "type": "execution",
                "execution": self._serialize_execution(execution),
            })

    def _serialize_execution(self, execution: Execution) -> dict[str, object]:
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

    async def _publish_log_line(self, execution: Execution, line: LogLine) -> None:
        await self._db.append_execution_log(execution.execution_id, line)
        if self._broker:
            await self._broker.publish(execution.session_id, {
                "type": "log",
                "execution_id": execution.execution_id,
                "logged_at": line.logged_at.isoformat(),
                "stream": line.stream,
                "body": line.body,
            })

    async def _record_startup_failure_logs(self, execution: Execution, exc: Exception) -> None:
        if not isinstance(exc, ExecutionStartupError):
            return
        existing = await self._db.get_last_log_timestamp(execution.execution_id)
        if existing is not None:
            return
        logged_at = datetime.now(timezone.utc)
        lines = exc.log_lines or [str(exc)]
        for body in lines:
            await self._publish_log_line(
                execution,
                LogLine(logged_at=logged_at, stream="stderr", body=body),
            )

    async def _halt_failed_start_execution(self, execution: Execution) -> None:
        halted_at = datetime.now(timezone.utc)
        await self._db.halt_execution(execution.execution_id, "startup_failed")
        await self._db.update_session_last_execution_result(execution.session_id, "failed")
        execution.halted_at = halted_at
        execution.halt_reason = "startup_failed"
        execution.phase = ExecutionPhase.FAILED
        await self._publish_execution_event(execution)

    async def _publish_followup_event(self, session_id: str, action: str) -> None:
        if self._broker:
            await self._broker.publish(session_id, {
                "type": "followup",
                "action": action,
            })

    async def _publish_conversation_event(self, session_id: str, event: dict) -> None:
        if self._broker:
            await self._broker.publish(session_id, {
                "type": "conversation_event",
                **event,
            })

    def _workflow_requires_runtime_tools(self, workflow_name: str) -> bool:
        workflow = next((item for item in self._workflows if item.name == workflow_name), None)
        if workflow is None:
            return False
        return bool(workflow.runtime and workflow.runtime.tools)

    def _runtime_mcp_failure(self, payload: dict) -> dict[str, str] | None:
        mcp_servers = payload.get("mcp_servers")
        if not isinstance(mcp_servers, list):
            return {"name": "claudius-runtime", "status": "missing"}
        for server in mcp_servers:
            if not isinstance(server, dict):
                continue
            if server.get("name") != "claudius-runtime":
                continue
            status = server.get("status")
            if isinstance(status, str) and status.lower() in {
                "connected",
                "ready",
                "ok",
                "pending",
            }:
                return None
            return {
                "name": "claudius-runtime",
                "status": status if isinstance(status, str) and status else "unknown",
            }
        return {"name": "claudius-runtime", "status": "missing"}

    async def _record_runtime_mcp_failure(
        self,
        session_id: str,
        execution_id: str,
        *,
        payload: dict,
    ) -> None:
        event = await self._db.append_conversation_event(
            execution_id,
            source="local",
            event_type="system",
            event_subtype="runtime_mcp_failed",
            payload=payload,
        )
        await self._publish_conversation_event(session_id, event)

    async def _fail_active_execution(
        self,
        session: Session,
        execution: Execution,
        *,
        halt_reason: str,
        user_message: str,
    ) -> None:
        logger.error(
            "failing execution session_id={} execution_id={} halt_reason={}",
            session.session_id,
            execution.execution_id,
            halt_reason,
        )
        tail_task = self._tail_tasks.pop(execution.execution_id, None)
        if tail_task:
            tail_task.cancel()
            try:
                await tail_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self._backend.delete_execution(execution)
        except Exception as exc:
            logger.warning(
                "failed to delete execution during fail-fast session_id={} execution_id={} error={}",
                session.session_id,
                execution.execution_id,
                exc,
            )
        self._clear_execution_bookkeeping(execution.execution_id)
        halted_at = datetime.now(timezone.utc)
        execution.halted_at = halted_at
        execution.halt_reason = halt_reason
        execution.phase = ExecutionPhase.FAILED
        await self._db.halt_execution(execution.execution_id, halt_reason)
        await self._db.update_session_last_execution_result(session.session_id, "failed")
        error_message_id = await self._db.store_message(session.session_id, "error", user_message)
        await self._db.update_session_state(session.session_id, SessionState.ERROR)
        if self._broker:
            await self._publish_message_event(
                session.session_id,
                {
                    "message_id": error_message_id,
                    "direction": "error",
                    "body": user_message,
                    "received_at": halted_at.isoformat(),
                    "attachments": [],
                    "acknowledged_at": None,
                    "delivery_status": "acknowledged",
                },
            )
            await self._broker.publish(session.session_id, {
                "type": "execution",
                "execution": {
                    "execution_id": execution.execution_id,
                    "session_id": session.session_id,
                    "started_at": execution.started_at.isoformat(),
                    "halted_at": halted_at.isoformat(),
                    "halt_reason": halt_reason,
                    "exit_code": None,
                    "claude_session_id": execution.claude_session_id,
                },
            })
            await self._broker.publish(session.session_id, {
                "type": "status",
                "state": SessionState.ERROR.value,
            })
