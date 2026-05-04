import asyncio
import json
import uuid
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

from claudius.config.schema import WorkflowConfig
from claudius.controller.backends.base import AbstractBackend
from claudius.controller.db import Database
from claudius.controller.session_manager import (
    BusySessionError,
    ClosedSessionError,
    NoWorkflowMatch,
    SessionManager,
)
from claudius.controller.sse import SSEBroker
from claudius.models import Attachment, Execution, InboundMessage, Session, SessionState
from claudius.runtime import SecretProvider


def _message(thread_id="t1", body="Do a task"):
    return InboundMessage(
        channel="email",
        sender="user@example.com",
        thread_id=thread_id,
        subject="Hello",
        body=body,
        attachments=[],
        received_at=datetime.now(timezone.utc),
    )


def _workflow(
    name="test-wf",
    active_followup_policy="interrupt_after_turn",
    idle_timeout_seconds=60,
):
    return WorkflowConfig.model_validate({
        "name": name,
        "routing": {"channels": ["email"]},
        "claude": {"system_prompt": "test"},
        "response": {"channel": "email"},
        "session": {
            "active_followup_policy": active_followup_policy,
            "idle_timeout_seconds": idle_timeout_seconds,
        },
    })


class _SecretProvider(SecretProvider):
    def read(self, ref: str, *, field: str | None = None) -> str:
        return {
            ("vault/ref", None): "secret-value",
            ("vault/ref", "token"): "secret-value",
            ("env:GITLAB_TOKEN", None): "secret-value",
        }[(ref, field)]


def _execution(session_id="sess-1"):
    return Execution(
        execution_id="exec-1",
        session_id=session_id,
        worker_address=None,
        started_at=datetime.now(timezone.utc),
        halted_at=None,
        halt_reason=None,
    )


class _BackendStub(AbstractBackend):
    def __init__(self, execution: Execution | None = None):
        self.execution = execution or _execution()
        self.create_execution_calls: list[tuple] = []
        self.created_executions: list[Execution] = []
        self.deleted_executions: list[Execution] = []
        self._create_count = 0

    async def create_execution(
        self,
        session,
        workflow,
        tool_mounts,
        extra_env=None,
        *,
        execution_id=None,
        started_at=None,
    ):
        self._create_count += 1
        self.create_execution_calls.append((session, workflow, tool_mounts, extra_env))
        execution = Execution(
            execution_id=execution_id or f"exec-generated-{self._create_count}-{uuid.uuid4().hex[:8]}",
            session_id=session.session_id,
            worker_address=self.execution.worker_address,
            started_at=started_at or datetime.now(timezone.utc),
            halted_at=None,
            halt_reason=None,
            claude_session_id=self.execution.claude_session_id,
        )
        self.created_executions.append(execution)
        return execution

    async def delete_execution(self, execution):
        self.deleted_executions.append(execution)
        return None

    async def get_exit_code(self, execution):
        return 0

    async def tail_logs(self, execution, since=None):
        if False:
            yield


class _GracefulTailBackend(_BackendStub):
    def __init__(self):
        super().__init__()
        self.stop_event = asyncio.Event()

    async def tail_logs(self, execution, since=None):
        await self.stop_event.wait()
        if False:
            yield


class _FailingBackendStub(AbstractBackend):
    async def create_execution(
        self,
        session,
        workflow,
        tool_mounts,
        extra_env=None,
        *,
        execution_id=None,
        started_at=None,
    ):
        raise RuntimeError("secret token leaked in exception details")

    async def delete_execution(self, execution):
        return None

    async def tail_logs(self, execution, since=None):
        if False:
            yield


@pytest.fixture
async def db(tmp_path):
    database = Database(tmp_path / "test.db")
    await database.init()
    yield database
    await database.close()


@pytest.mark.asyncio
async def test_handle_new_message_creates_session(db, tmp_path):
    backend = _BackendStub()

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await asyncio.wait_for(manager.handle_message(_message(thread_id="new-thread")), timeout=1.0)

    session = await db.get_session_by_thread("new-thread")
    assert session is not None
    assert session.state == SessionState.ACTIVE
    assert len(backend.create_execution_calls) == 1
    extra_env = backend.create_execution_calls[0][3]
    assert "Do a task" in extra_env["CLAUDIUS_CONVERSATION_TEXT"]
    assert extra_env["CLAUDIUS_WORKSPACE_PATH"] == "/workspace"
    assert "CLAUDIUS_OUTPUT_DIR" in extra_env
    events = await db.list_conversation_events(backend.created_executions[0].execution_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "input"
    assert events[0]["payload"]["body"] == "Do a task"


@pytest.mark.asyncio
async def test_handle_new_message_builds_runtime_tool_mounts(db, tmp_path):
    backend = _BackendStub()
    workflow = WorkflowConfig.model_validate({
        "name": "test-wf",
        "routing": {"channels": ["email"]},
        "claude": {"system_prompt": "test"},
        "runtime": {
            "tool_context": [
                {"name": "BRANCH", "value": "claudius/${{request.thread_id}}"},
                {"name": "GITLAB_TOKEN", "env": "GITLAB_TOKEN"},
            ],
            "pre_launch": [
                {"type": "shell", "when": "execution_start", "run": "git fetch"}
            ],
            "tools": [
                {
                    "type": "shell",
                    "name": "git_push",
                    "description": "Push branch",
                    "run": "git push origin \"$BRANCH:$BRANCH\"",
                }
            ],
        },
    })

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[workflow],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        secret_provider=_SecretProvider(),
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.handle_message(_message(thread_id="thread-runtime"))

    tool_mounts = backend.create_execution_calls[0][2]
    extra_env = backend.create_execution_calls[0][3]
    assert len(tool_mounts) == 1
    assert tool_mounts[0].pre_launch_commands == []
    assert tool_mounts[0].sidecars
    assert extra_env["CLAUDIUS_MCP_CONFIG"] == "/workspace/.claudius-runtime/mcp.json"


@pytest.mark.asyncio
async def test_initial_prompt_lists_attachment_paths(db, tmp_path):
    backend = _BackendStub()
    attachment = Attachment(
        filename="report.pdf",
        content_type="application/pdf",
        data=b"%PDF-1.7",
    )

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.handle_message(InboundMessage(
        channel="email",
        sender="user@example.com",
        thread_id="thread-attachments",
        subject="Hello",
        body="Review this",
        attachments=[attachment],
        received_at=datetime.now(timezone.utc),
    ))

    extra_env = backend.create_execution_calls[0][3]
    prompt = extra_env["CLAUDIUS_CONVERSATION_TEXT"]
    assert "Review this" in prompt
    assert "Attachments and outputs:" in prompt
    assert "- attachments/" in prompt
    assert "- outputs/" in prompt
    assert "/report.pdf" in prompt


@pytest.mark.asyncio
async def test_handle_message_for_active_session_queues_until_worker_exits(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-active",
        thread_id="thread-active",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-active"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-active"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="queue")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.handle_message(_message(thread_id="thread-active", body="follow up"))

    assert backend.create_execution_calls == []
    pending = await db.get_pending_inbound_messages("sess-active")
    assert len(pending) == 1
    assert pending[0]["body"] == "follow up"


@pytest.mark.asyncio
async def test_interrupt_policy_stops_and_restarts_immediately(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-int",
        thread_id="thread-int",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-int"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-int"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="interrupt")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    result = await manager.continue_session("sess-int", "interrupt now")

    assert result["followup_action"] == "interrupting"
    assert len(backend.deleted_executions) == 1
    assert len(backend.create_execution_calls) == 1
    messages = await db.list_messages("sess-int")
    assert messages[-1]["acknowledged_at"] is not None
    assert messages[-1]["delivery_status"] == "acknowledged"
    executions = await db.list_executions("sess-int")
    assert len(executions) == 2
    assert executions[0].halt_reason == "interrupted"
    assert executions[1].halted_at is None


@pytest.mark.asyncio
async def test_interrupt_after_turn_waits_for_result_boundary(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-turn",
        thread_id="thread-turn",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-turn"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-turn"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="interrupt_after_turn")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    result = await manager.continue_session("sess-turn", "wait for turn end")
    assert result["followup_action"] == "interrupt_after_turn"
    assert len(backend.deleted_executions) == 0
    assert len(backend.create_execution_calls) == 0

    await manager.append_active_execution_conversation_event(
        "sess-turn",
        source="claude",
        event_type="assistant",
        payload={
            "message": {
                "content": [{"type": "tool_use", "id": "tool-1", "name": "bash", "input": {}}],
            }
        },
    )
    assert len(backend.deleted_executions) == 0
    assert len(backend.create_execution_calls) == 0

    await manager.append_active_execution_conversation_event(
        "sess-turn",
        source="claude",
        event_type="assistant",
        payload={
            "message": {
                "content": [{"type": "text", "text": "Done with the current turn."}],
            }
        },
    )
    assert len(backend.deleted_executions) == 0
    assert len(backend.create_execution_calls) == 0

    await manager.append_active_execution_conversation_event(
        "sess-turn",
        source="claude",
        event_type="result",
        event_subtype="success",
        payload={"result": "Done with the current turn."},
    )
    assert len(backend.deleted_executions) == 1
    assert len(backend.create_execution_calls) == 1
    messages = await db.list_messages("sess-turn")
    assert messages[-1]["acknowledged_at"] is not None


@pytest.mark.asyncio
async def test_stop_execution_restarts_when_pending_messages_exist(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-stop",
        thread_id="thread-stop",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-stop"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-stop"))
    await db.store_message("sess-stop", "inbound", "queued follow up", sender="dev@localhost")

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="queue")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    stopped = await manager.stop_execution("sess-stop")

    assert stopped is True
    assert len(backend.deleted_executions) == 1
    assert len(backend.create_execution_calls) == 1
    resumed_messages = await db.list_messages("sess-stop")
    assert resumed_messages[-1]["acknowledged_at"] is not None


@pytest.mark.asyncio
async def test_stop_execution_preserves_result_metadata_when_graceful_stop_completes(
    db, tmp_path
):
    backend = _GracefulTailBackend()
    session = Session(
        session_id="sess-graceful-stop",
        thread_id="thread-graceful-stop",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-graceful-stop"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    execution = _execution(session_id="sess-graceful-stop")
    execution.worker_address = "127.0.0.1:8080"
    await db.create_execution(execution)

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="queue")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer(execution)

    async def _request_worker_stop(_execution):
        await manager.append_active_execution_conversation_event(
            "sess-graceful-stop",
            source="claude",
            event_type="result",
            event_subtype="success",
            payload={"num_turns": 3, "duration_ms": 1200, "total_cost_usd": 0.04},
        )
        backend.stop_event.set()
        return True

    manager._request_worker_stop = _request_worker_stop

    stopped = await manager.stop_execution(
        "sess-graceful-stop",
        reason="interrupted",
        restart_if_pending=False,
        final_state=SessionState.HIBERNATED,
    )

    assert stopped is True
    execution = await db.get_latest_execution("sess-graceful-stop")
    assert execution is not None
    assert execution.halt_reason == "interrupted"
    assert execution.claude_num_turns == 3
    assert execution.claude_duration_ms == 1200
    assert execution.claude_total_cost_usd is None  # cost comes from proxy, not result event
    session = await db.get_session("sess-graceful-stop")
    assert session is not None
    assert session.state == SessionState.HIBERNATED


@pytest.mark.asyncio
async def test_stop_execution_synthesizes_duration_on_forced_stop(db, tmp_path):
    backend = _BackendStub()
    session = Session(
        session_id="sess-forced-stop",
        thread_id="thread-forced-stop",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-forced-stop"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    execution = _execution(session_id="sess-forced-stop")
    execution.started_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    execution.worker_address = "127.0.0.1:8080"
    await db.create_execution(execution)
    await db.increment_execution_token_usage(
        execution.execution_id,
        input_tokens=10,
        output_tokens=4,
        total_cost_usd=0.02,
    )

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="queue")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._request_worker_stop = AsyncMock(return_value=False)

    stopped = await manager.stop_execution(
        "sess-forced-stop",
        reason="interrupted",
        restart_if_pending=False,
        final_state=SessionState.HIBERNATED,
    )

    assert stopped is True
    execution = await db.get_latest_execution("sess-forced-stop")
    assert execution is not None
    assert execution.halt_reason == "interrupted"
    assert execution.claude_duration_ms is not None
    assert execution.claude_duration_ms >= 1500
    assert execution.claude_total_cost_usd == 0.02
    assert execution.claude_total_tokens == 14


@pytest.mark.asyncio
async def test_result_event_arms_idle_timeout_and_hibernates_session(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-idle",
        thread_id="thread-idle",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-idle"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-idle"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="queue", idle_timeout_seconds=0)],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.append_active_execution_conversation_event(
        "sess-idle",
        source="claude",
        event_type="result",
        event_subtype="success",
        payload={"result": "Finished for now."},
    )

    async def _wait_for_idle_stop():
        for _ in range(50):
            executions = await db.list_executions("sess-idle")
            current_session = await db.get_session("sess-idle")
            if (
                executions[0].halt_reason == "idle_timeout"
                and current_session is not None
                and current_session.state == SessionState.HIBERNATED
            ):
                return executions[0], current_session
            await asyncio.sleep(0.01)
        raise AssertionError("idle timeout did not stop execution")

    execution, current_session = await _wait_for_idle_stop()
    assert execution.halt_reason == "idle_timeout"
    assert current_session.state == SessionState.HIBERNATED
    assert len(backend.deleted_executions) == 1


@pytest.mark.asyncio
async def test_reject_policy_refuses_active_followups(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-reject",
        thread_id="thread-reject",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-reject"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-reject"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow(active_followup_policy="reject")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    with pytest.raises(BusySessionError):
        await manager.continue_session("sess-reject", "not allowed")

    messages = await db.list_messages("sess-reject")
    assert messages == []


@pytest.mark.asyncio
async def test_close_session_stops_active_execution_and_marks_closed(db, tmp_path):
    backend = _BackendStub()
    broker = SSEBroker()

    session = Session(
        session_id="sess-close",
        thread_id="thread-close",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-close"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-close"))

    queue: asyncio.Queue = asyncio.Queue()
    broker.subscribe("sess-close", queue)
    try:
        manager = SessionManager(
            db=db,
            backend=backend,
            workflows=[_workflow(active_followup_policy="queue")],
            workspaces_path=str(tmp_path / "workspaces"),
            channels={},
            broker=broker,
        )
        manager._start_log_tailer = lambda *args, **kwargs: None

        closed = await manager.close_session("sess-close")

        assert closed is True
        assert len(backend.deleted_executions) == 1
        updated = await db.get_session("sess-close")
        assert updated is not None
        assert updated.state == SessionState.CLOSED
        executions = await db.list_executions("sess-close")
        assert executions[0].halt_reason == "closed"

        events = [
            json.loads(await asyncio.wait_for(queue.get(), timeout=1.0)),
            json.loads(await asyncio.wait_for(queue.get(), timeout=1.0)),
        ]
        assert any(event["type"] == "execution" for event in events)
        assert {"type": "status", "state": "closed"} in events
    finally:
        broker.unsubscribe("sess-close", queue)


@pytest.mark.asyncio
async def test_close_session_is_idempotent(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-closed",
        thread_id="thread-closed",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.CLOSED,
        workspace_path=str(tmp_path / "workspaces" / "sess-closed"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    closed = await manager.close_session("sess-closed")

    assert closed is True
    assert backend.deleted_executions == []


@pytest.mark.asyncio
async def test_continue_session_rejects_closed_session(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-closed-continue",
        thread_id="thread-closed-continue",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.CLOSED,
        workspace_path=str(tmp_path / "workspaces" / "sess-closed-continue"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    with pytest.raises(ClosedSessionError):
        await manager.continue_session("sess-closed-continue", "please reopen")


@pytest.mark.asyncio
async def test_handle_message_for_closed_session_stores_without_restarting(db, tmp_path):
    backend = _BackendStub()

    session = Session(
        session_id="sess-closed-message",
        thread_id="thread-closed-message",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.CLOSED,
        workspace_path=str(tmp_path / "workspaces" / "sess-closed-message"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    returned = await manager.handle_message(
        _message(thread_id="thread-closed-message", body="thanks again")
    )

    assert returned.state == SessionState.CLOSED
    assert backend.create_execution_calls == []
    messages = await db.list_messages("sess-closed-message")
    assert messages[-1]["body"] == "thanks again"


@pytest.mark.asyncio
async def test_hibernated_session_resumes_with_full_conversation(db, tmp_path):
    backend = _BackendStub(execution=_execution(session_id="sess-h"))

    session = Session(
        session_id="sess-h",
        thread_id="thread-h",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.HIBERNATED,
        workspace_path=str(tmp_path / "workspaces" / "sess-h"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.store_message("sess-h", "inbound", "first task", sender="user@example.com")
    await db.store_message("sess-h", "outbound", "done")

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.handle_message(_message(thread_id="thread-h", body="please fix one more thing"))

    assert len(backend.create_execution_calls) == 1
    extra_env = backend.create_execution_calls[0][3]
    assert "The user sent multiple follow-up messages while you were not running." in extra_env["CLAUDIUS_CONVERSATION_TEXT"]
    assert "1. first task" in extra_env["CLAUDIUS_CONVERSATION_TEXT"]
    assert "2. please fix one more thing" in extra_env["CLAUDIUS_CONVERSATION_TEXT"]
    messages = await db.list_messages("sess-h")
    assert extra_env["CLAUDIUS_OUTPUT_DIR"] == f"/workspace/outputs/{messages[-1]['message_id']}"
    events = await db.list_conversation_events(backend.created_executions[0].execution_id)
    assert len(events) == 2
    assert all(event["event_subtype"] == "hitl_message" for event in events)
    assert [event["payload"]["body"] for event in events] == [
        "first task",
        "please fix one more thing",
    ]


@pytest.mark.asyncio
async def test_resume_prompt_lists_attachment_paths(db, tmp_path):
    backend = _BackendStub(execution=_execution(session_id="sess-h"))

    session = Session(
        session_id="sess-h",
        thread_id="thread-h",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.HIBERNATED,
        workspace_path=str(tmp_path / "workspaces" / "sess-h"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    attachment = Attachment(
        filename="report.pdf",
        content_type="application/pdf",
        data=b"%PDF-1.7",
    )

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.continue_session("sess-h", "Please review the attachment", [attachment])

    extra_env = backend.create_execution_calls[0][3]
    prompt = extra_env["CLAUDIUS_CONVERSATION_TEXT"]
    assert "Please review the attachment" in prompt
    assert "Attachments and outputs:" in prompt
    assert "- attachments/" in prompt
    assert "- outputs/" in prompt
    assert "/report.pdf" in prompt


@pytest.mark.asyncio
async def test_no_matching_workflow_raises(db, tmp_path):
    backend = _BackendStub()
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    with pytest.raises(NoWorkflowMatch):
        await manager.handle_message(InboundMessage(
            channel="whatsapp",
            sender="+1234",
            thread_id="wa-1",
            subject=None,
            body="Hello",
            attachments=[],
            received_at=datetime.now(timezone.utc),
        ))


@pytest.mark.asyncio
async def test_handle_message_returns_session(db, tmp_path):
    backend = _BackendStub()

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None
    session = await manager.handle_message(_message(thread_id="t-return"))

    assert session is not None
    assert session.thread_id == "t-return"


@pytest.mark.asyncio
async def test_launch_failure_stores_user_safe_error_message(db, tmp_path):
    manager = SessionManager(
        db=db,
        backend=_FailingBackendStub(),
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    session = await manager.handle_message(_message(thread_id="launch-failure"))

    assert session.state == SessionState.ERROR
    stored = await db.list_messages(session.session_id)
    inbound_messages = [message for message in stored if message["direction"] == "inbound"]
    assert len(inbound_messages) == 1
    assert inbound_messages[0]["delivery_status"] == "failed"
    assert "Claude never received this message" in inbound_messages[0]["delivery_error"]
    error_messages = [message for message in stored if message["direction"] == "error"]
    assert len(error_messages) == 1
    assert error_messages[0]["body"] == (
        "I couldn’t start the workspace for this request. Please try again in a moment."
    )
    assert "secret token leaked" not in error_messages[0]["body"]
    executions = await db.list_executions(session.session_id)
    assert len(executions) == 1
    assert executions[0].halt_reason == "startup_failed"


@pytest.mark.asyncio
async def test_resume_failure_marks_followup_as_failed_and_sets_session_error(db, tmp_path):
    session = Session(
        session_id="sess-resume-fail",
        thread_id="thread-resume-fail",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.HIBERNATED,
        workspace_path=str(tmp_path / "workspaces" / "sess-resume-fail"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    manager = SessionManager(
        db=db,
        backend=_FailingBackendStub(),
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    with pytest.raises(RuntimeError, match="secret token leaked"):
        await manager.continue_session("sess-resume-fail", "follow up after hibernation")

    updated = await db.get_session("sess-resume-fail")
    assert updated is not None
    assert updated.state == SessionState.ERROR
    stored = await db.list_messages("sess-resume-fail")
    inbound_messages = [message for message in stored if message["direction"] == "inbound"]
    assert len(inbound_messages) == 1
    assert inbound_messages[0]["delivery_status"] == "failed"
    assert "Claude never received this message" in inbound_messages[0]["delivery_error"]
    error_messages = [message for message in stored if message["direction"] == "error"]
    assert len(error_messages) == 1
    assert error_messages[0]["body"] == (
        "I couldn’t start the workspace for this request. Please try again in a moment."
    )
    executions = await db.list_executions("sess-resume-fail")
    assert len(executions) == 1
    assert executions[0].halt_reason == "startup_failed"


class _StartupErrorBackendStub(AbstractBackend):
    async def create_execution(
        self,
        session,
        workflow,
        tool_mounts,
        extra_env=None,
        *,
        execution_id=None,
        started_at=None,
    ):
        from claudius.controller.backends.base import ExecutionStartupError

        raise ExecutionStartupError(
            "runtime sidecar failed before becoming ready",
            log_lines=["[runtime-sidecar] cloning repo", "[runtime-sidecar] auth failed"],
        )

    async def delete_execution(self, execution):
        return None

    async def tail_logs(self, execution, since=None):
        if False:
            yield


@pytest.mark.asyncio
async def test_launch_startup_failure_persists_execution_logs(db, tmp_path):
    manager = SessionManager(
        db=db,
        backend=_StartupErrorBackendStub(),
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    session = await manager.handle_message(_message(thread_id="launch-startup-logs"))

    execution = await db.get_latest_execution(session.session_id)
    assert execution is not None
    assert execution.halt_reason == "startup_failed"
    logs = await db.list_execution_logs(execution.execution_id)
    assert [item["body"] for item in logs] == [
        "[runtime-sidecar] cloning repo",
        "[runtime-sidecar] auth failed",
    ]


@pytest.mark.asyncio
async def test_mark_message_delivery_failed_tolerates_partial_message_dict(db, tmp_path):
    session = Session(
        session_id="sess-partial-fail",
        thread_id="thread-partial-fail",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.HIBERNATED,
        workspace_path=str(tmp_path / "workspaces" / "sess-partial-fail"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    message_id = await db.store_message(
        "sess-partial-fail",
        "inbound",
        "follow up",
        sender="user@example.com",
    )

    manager = SessionManager(
        db=db,
        backend=_BackendStub(),
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    await manager._mark_message_delivery_failed(
        "sess-partial-fail",
        {"message_id": message_id},
    )

    stored = await db.list_messages("sess-partial-fail")
    assert len(stored) == 1
    assert stored[0]["delivery_status"] == "failed"


@pytest.mark.asyncio
async def test_worker_env_includes_session_token_when_proxy_enabled(db, tmp_path, monkeypatch):
    backend = _BackendStub()
    monkeypatch.setenv("RESEND_API_KEY", "resend-key")

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        callback_url="http://controller",
        proxy_secret="secret-123",
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.handle_message(_message(thread_id="token-thread"))

    extra_env = backend.create_execution_calls[0][3]
    assert extra_env["CLAUDIUS_SESSION_TOKEN"]
    assert extra_env["ANTHROPIC_API_KEY"] == extra_env["CLAUDIUS_SESSION_TOKEN"]
    assert extra_env["ANTHROPIC_BASE_URL"] == "http://controller/proxy"


@pytest.mark.asyncio
async def test_receive_outbound_stores_message_and_publishes(db, tmp_path):
    backend = _BackendStub()
    broker = SSEBroker()

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        broker=broker,
    )
    manager._start_log_tailer = lambda *args, **kwargs: None
    await manager.handle_message(_message(thread_id="t-out"))
    session = await db.get_session_by_thread("t-out")

    q = asyncio.Queue()
    broker.subscribe(session.session_id, q)
    await manager.receive_outbound(
        session.session_id,
        "Agent reply",
        [Attachment(filename="report.txt", content_type="text/plain", data=b"hello")],
    )

    msgs = await db.list_messages(session.session_id)
    outbound = [m for m in msgs if m["direction"] == "outbound"]
    assert len(outbound) == 1
    assert outbound[0]["body"] == "Agent reply"
    assert outbound[0]["attachments"] == [{"filename": "report.txt", "content_type": "text/plain"}]

    event = await asyncio.wait_for(q.get(), timeout=1.0)
    data = json.loads(event)
    assert data["type"] == "message"
    assert data["body"] == "Agent reply"
    assert data["attachments"] == [{"filename": "report.txt", "content_type": "text/plain"}]


@pytest.mark.asyncio
async def test_output_directory_created_for_new_session(db, tmp_path):
    backend = _BackendStub()
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.handle_message(_message(thread_id="output-thread"))

    extra_env = backend.create_execution_calls[0][3]
    output_dir = extra_env["CLAUDIUS_OUTPUT_DIR"].removeprefix("/workspace/")
    session = await db.get_session_by_thread("output-thread")
    assert session is not None
    assert (Path(session.workspace_path) / output_dir).is_dir()


@pytest.mark.asyncio
async def test_append_active_execution_conversation_event_updates_claude_session_id(db, tmp_path):
    backend = _BackendStub()
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        broker=SSEBroker(),
    )
    manager._start_log_tailer = lambda *args, **kwargs: None
    await manager.handle_message(_message(thread_id="thread-transcript"))
    session = await db.get_session_by_thread("thread-transcript")

    event = await manager.append_active_execution_conversation_event(
        session.session_id,
        source="claude",
        event_type="system",
        event_subtype="init",
        payload={"subtype": "init", "session_id": "claude-session-1"},
    )

    assert event["event_type"] == "system"
    execution = await db.get_latest_execution(session.session_id)
    assert execution is not None
    assert execution.claude_session_id == "claude-session-1"


@pytest.mark.asyncio
async def test_append_active_execution_conversation_event_updates_execution_summary_with_cost(
    db, tmp_path
):
    backend = _BackendStub()
    broker = SSEBroker()
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        broker=broker,
    )
    manager._start_log_tailer = lambda *args, **kwargs: None
    await manager.handle_message(_message(thread_id="thread-summary"))
    session = await db.get_session_by_thread("thread-summary")
    q = asyncio.Queue()
    broker.subscribe(session.session_id, q)

    await manager.append_active_execution_conversation_event(
        session.session_id,
        source="claude",
        event_type="result",
        event_subtype="success",
        payload={"num_turns": 7, "duration_ms": 3200, "total_cost_usd": 0.2},
    )

    execution = await db.get_latest_execution(session.session_id)
    assert execution is not None
    assert execution.claude_num_turns == 7
    assert execution.claude_duration_ms == 3200
    assert execution.claude_total_cost_usd is None  # cost comes from proxy, not result event
    event = json.loads(await asyncio.wait_for(q.get(), timeout=1.0))
    assert event["type"] == "execution"
    assert event["execution"]["claude_num_turns"] == 7
    assert event["execution"]["claude_total_cost_usd"] is None
    await manager.shutdown()


@pytest.mark.asyncio
async def test_increment_active_execution_token_usage_updates_execution_cost(db, tmp_path):
    backend = _BackendStub()
    broker = SSEBroker()
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        broker=broker,
    )
    manager._start_log_tailer = lambda *args, **kwargs: None
    await manager.handle_message(_message(thread_id="thread-token-usage"))
    session = await db.get_session_by_thread("thread-token-usage")
    q = asyncio.Queue()
    broker.subscribe(session.session_id, q)

    await manager.increment_active_execution_token_usage(
        session.session_id,
        input_tokens=13,
        output_tokens=8,
        cache_creation_input_tokens=5,
        cache_read_input_tokens=3,
        total_cost_usd=0.015,
    )

    execution = await db.get_latest_execution(session.session_id)
    assert execution is not None
    assert execution.claude_input_tokens == 13
    assert execution.claude_output_tokens == 8
    assert execution.claude_cache_creation_input_tokens == 5
    assert execution.claude_cache_read_input_tokens == 3
    assert execution.claude_total_tokens == 29
    assert execution.claude_total_cost_usd == 0.015
    event = json.loads(await asyncio.wait_for(q.get(), timeout=1.0))
    assert event["type"] == "execution"
    assert event["execution"]["claude_total_tokens"] == 29
    assert event["execution"]["claude_total_cost_usd"] == 0.015
    await manager.shutdown()


@pytest.mark.asyncio
async def test_append_active_execution_conversation_event_fails_fast_on_runtime_mcp_failure(
    db, tmp_path
):
    backend = _BackendStub()
    workflow = WorkflowConfig.model_validate({
        "name": "runtime-wf",
        "routing": {"channels": ["email"]},
        "claude": {"system_prompt": "test"},
        "runtime": {
            "tools": [
                {
                    "type": "shell",
                    "name": "git_push",
                    "description": "Push branch",
                    "run": "git push origin HEAD",
                }
            ]
        },
        "response": {"channel": "email"},
    })
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[workflow],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
        broker=SSEBroker(),
    )
    manager._start_log_tailer = lambda *args, **kwargs: None
    await manager.handle_message(_message(thread_id="thread-runtime-failed"))
    session = await db.get_session_by_thread("thread-runtime-failed")

    event = await manager.append_active_execution_conversation_event(
        session.session_id,
        source="claude",
        event_type="system",
        event_subtype="init",
        payload={
            "subtype": "init",
            "session_id": "claude-session-2",
            "mcp_servers": [{"name": "claudius-runtime", "status": "failed"}],
        },
    )

    assert event["event_subtype"] == "init"
    assert len(backend.deleted_executions) == 1
    updated_session = await db.get_session(session.session_id)
    assert updated_session is not None
    assert updated_session.state == SessionState.ERROR
    assert updated_session.last_execution_result == "failed"
    execution = await db.get_latest_execution(session.session_id)
    assert execution is not None
    assert execution.halt_reason == "runtime_mcp_failed"
    assert execution.claude_session_id == "claude-session-2"
    events = await db.list_conversation_events(execution.execution_id)
    assert [item["event_subtype"] for item in events] == [
        "initial_message",
        "init",
        "runtime_mcp_failed",
    ]
    assert events[-1]["payload"]["mcp_server"] == "claudius-runtime"
    assert events[-1]["payload"]["status"] == "failed"
    messages = await db.list_messages(session.session_id)
    assert messages[-1]["direction"] == "error"
    assert "workflow tools" in messages[-1]["body"]


@pytest.mark.asyncio
async def test_list_workflow_names(db, tmp_path):
    backend = _BackendStub()
    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow("wf-a"), _workflow("wf-b")],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    assert manager.list_workflow_names() == ["wf-a", "wf-b"]


@pytest.mark.asyncio
async def test_record_agent_fatal_error_stores_fields_and_schedules_interrupt(db, tmp_path):
    backend = _BackendStub()
    session = Session(
        session_id="sess-fe",
        thread_id="thread-fe",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-fe"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-fe"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.record_agent_fatal_error(
        "sess-fe", category="tool_failure", reason="Could not reach API."
    )

    execution = await db.get_active_execution("sess-fe")
    assert execution is not None
    assert execution.agent_error_category == "tool_failure"
    assert execution.agent_error_reason == "Could not reach API."
    assert execution.execution_id in manager._interrupt_after_turn
    assert execution.execution_id in manager._agent_fatal_errors


@pytest.mark.asyncio
async def test_agent_fatal_error_sets_session_to_error_on_turn_end(db, tmp_path):
    backend = _BackendStub()
    session = Session(
        session_id="sess-fe2",
        thread_id="thread-fe2",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "workspaces" / "sess-fe2"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.create_execution(_execution(session_id="sess-fe2"))

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )
    manager._start_log_tailer = lambda *args, **kwargs: None

    await manager.record_agent_fatal_error(
        "sess-fe2", category="permission_error", reason="No write access."
    )
    await manager.append_active_execution_conversation_event(
        "sess-fe2",
        source="claude",
        event_type="result",
        event_subtype="success",
        payload={"result": "I'm unable to continue."},
    )

    updated_session = await db.get_session("sess-fe2")
    assert updated_session.state == SessionState.ERROR
    assert updated_session.last_execution_result == "failed"
    halted = await db.get_active_execution("sess-fe2")
    assert halted is None
    assert len(backend.create_execution_calls) == 0


@pytest.mark.asyncio
async def test_record_agent_fatal_error_raises_when_no_active_execution(db, tmp_path):
    backend = _BackendStub()
    session = Session(
        session_id="sess-fe3",
        thread_id="thread-fe3",
        channel="email",
        workflow_name="test-wf",
        state=SessionState.HIBERNATED,
        workspace_path=str(tmp_path / "workspaces" / "sess-fe3"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    manager = SessionManager(
        db=db,
        backend=backend,
        workflows=[_workflow()],
        workspaces_path=str(tmp_path / "workspaces"),
        channels={},
    )

    with pytest.raises(ValueError, match="No active execution"):
        await manager.record_agent_fatal_error(
            "sess-fe3", category="tool_failure", reason="boom"
        )
