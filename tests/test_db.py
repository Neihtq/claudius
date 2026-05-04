import pytest
from datetime import datetime, timezone
from claudius.controller.db import Database
from claudius.models import Session, Execution, SessionState, InboundMessage

@pytest.fixture
async def db(tmp_path):
    database = Database(tmp_path / "test.db")
    await database.init()
    yield database
    await database.close()

@pytest.mark.asyncio
async def test_create_and_get_session(db):
    session = Session(
        session_id="sess-1",
        thread_id="thread-1",
        channel="email",
        workflow_name="test",
        state=SessionState.NEW,
        workspace_path="/workspaces/sess-1",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    fetched = await db.get_session_by_thread(thread_id="thread-1")
    assert fetched is not None
    assert fetched.session_id == "sess-1"
    assert fetched.state == SessionState.NEW

@pytest.mark.asyncio
async def test_update_session_state(db):
    session = Session(
        session_id="sess-2",
        thread_id="thread-2",
        channel="email",
        workflow_name="test",
        state=SessionState.NEW,
        workspace_path="/workspaces/sess-2",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.update_session_state("sess-2", SessionState.ACTIVE)
    fetched = await db.get_session_by_thread("thread-2")
    assert fetched.state == SessionState.ACTIVE

@pytest.mark.asyncio
async def test_create_and_get_execution(db):
    session = Session(
        session_id="sess-3", thread_id="thread-3", channel="email",
        workflow_name="test", state=SessionState.ACTIVE,
        workspace_path="/workspaces/sess-3",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    execution = Execution(
        execution_id="exec-1",
        session_id="sess-3",
        worker_address="172.17.0.2:8080",
        started_at=datetime.now(timezone.utc),
        halted_at=None,
        halt_reason=None,
    )
    await db.create_execution(execution)
    fetched = await db.get_active_execution("sess-3")
    assert fetched is not None
    assert fetched.worker_address == "172.17.0.2:8080"

@pytest.mark.asyncio
async def test_store_and_list_messages(db):
    session = Session(
        session_id="sess-4", thread_id="thread-4", channel="email",
        workflow_name="test", state=SessionState.ACTIVE,
        workspace_path="/workspaces/sess-4",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.store_message("sess-4", "inbound", "hello")
    messages = await db.list_messages("sess-4")
    assert len(messages) == 1
    assert messages[0]["message_id"]
    assert messages[0]["body"] == "hello"
    assert messages[0]["acknowledged_at"] is None
    assert messages[0]["delivery_status"] == "pending"


@pytest.mark.asyncio
async def test_failed_message_delivery_is_persisted(db):
    session = Session(
        session_id="sess-4b", thread_id="thread-4b", channel="email",
        workflow_name="test", state=SessionState.ACTIVE,
        workspace_path="/workspaces/sess-4b",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    message_id = await db.store_message("sess-4b", "inbound", "hello")
    await db.fail_message_delivery(
        message_id,
        "Claude never received this message because the workspace failed to start.",
    )

    messages = await db.list_messages("sess-4b")
    assert messages[0]["acknowledged_at"] is not None
    assert messages[0]["delivery_status"] == "failed"
    assert "Claude never received this message" in messages[0]["delivery_error"]


@pytest.mark.asyncio
async def test_get_pending_inbound_messages_includes_direction(db):
    session = Session(
        session_id="sess-pending", thread_id="thread-pending", channel="email",
        workflow_name="test", state=SessionState.HIBERNATED,
        workspace_path="/workspaces/sess-pending",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.store_message("sess-pending", "inbound", "hello", sender="user@example.com")

    pending = await db.get_pending_inbound_messages("sess-pending")

    assert len(pending) == 1
    assert pending[0]["direction"] == "inbound"


@pytest.mark.asyncio
async def test_get_session_history_filters_and_limits(db):
    session = Session(
        session_id="sess-5", thread_id="thread-5", channel="email",
        workflow_name="test", state=SessionState.ACTIVE,
        workspace_path="/workspaces/sess-5",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)
    await db.store_message("sess-5", "inbound", "first question", sender="user@example.com")
    await db.store_message("sess-5", "outbound", "first answer")
    await db.store_message("sess-5", "inbound", "second question", sender="user@example.com")

    user_history = await db.get_session_history("sess-5", side="user", limit=10)
    assert [message["content"] for message in user_history] == [
        "first question",
        "second question",
    ]

    query_history = await db.get_session_history("sess-5", side="both", limit=10, query="ANSWER")
    assert len(query_history) == 1
    assert query_history[0]["role"] == "assistant"
    assert query_history[0]["content"] == "first answer"

    limited_history = await db.get_session_history("sess-5", side="both", limit=2)
    assert [message["content"] for message in limited_history] == [
        "first answer",
        "second question",
    ]


@pytest.mark.asyncio
async def test_update_execution_agent_error(db):
    session = Session(
        session_id="s-ae", thread_id="t-ae", channel="dev", workflow_name="test",
        state=SessionState.ACTIVE, workspace_path="/tmp",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await db.create_session(session)

    execution = Execution(
        execution_id="e-ae", session_id="s-ae", worker_address=None,
        started_at=datetime.now(timezone.utc), halted_at=None, halt_reason=None,
    )
    await db.create_execution(execution)

    await db.update_execution_agent_error("e-ae", "tool_failure", "Could not reach the API.")

    row = await db.get_active_execution("s-ae")
    assert row.agent_error_category == "tool_failure"
    assert row.agent_error_reason == "Could not reach the API."


def test_execution_dataclass_has_agent_error_fields():
    e = Execution(
        execution_id="e1", session_id="s1", worker_address=None,
        started_at=datetime.now(timezone.utc), halted_at=None, halt_reason=None,
        agent_error_category="tool_failure",
        agent_error_reason="The tool failed.",
    )
    assert e.agent_error_category == "tool_failure"
    assert e.agent_error_reason == "The tool failed."
