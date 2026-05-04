import pytest
from datetime import datetime, timezone
from claudius.controller.db import Database
from claudius.models import Execution, LogLine, Session, SessionState


def _session(session_id="sess-1"):
    now = datetime.now(timezone.utc)
    return Session(
        session_id=session_id, thread_id=f"t-{session_id}",
        channel="email", workflow_name="wf",
        state=SessionState.ACTIVE, workspace_path="/tmp",
        created_at=now, last_message_at=now,
    )


def _execution(execution_id="exec-1", session_id="sess-1"):
    return Execution(
        execution_id=execution_id, session_id=session_id,
        worker_address="172.0.0.1:8080",
        started_at=datetime.now(timezone.utc),
        halted_at=None, halt_reason=None,
    )


@pytest.fixture
async def db(tmp_path):
    database = Database(tmp_path / "test.db")
    await database.init()
    yield database
    await database.close()


@pytest.mark.asyncio
async def test_append_and_list_execution_logs(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    await db.append_execution_log("exec-1", LogLine(logged_at=t1, stream="stdout", body="hello"))
    await db.append_execution_log("exec-1", LogLine(logged_at=t2, stream="stderr", body="error"))

    rows = await db.list_execution_logs("exec-1")
    assert len(rows) == 2
    assert rows[0]["stream"] == "stdout"
    assert rows[0]["body"] == "hello"
    assert rows[1]["stream"] == "stderr"


@pytest.mark.asyncio
async def test_append_and_list_proxy_logs(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    t1 = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 12, 1, 1, tzinfo=timezone.utc)
    await db.append_proxy_log(
        "exec-1",
        logged_at=t1,
        stage="request_in",
        method="POST",
        path="v1/messages",
        upstream_url="http://upstream/v1/messages",
        content_type="application/json",
        body='{"a":1}',
        meta={"rewrite_applied": True},
    )
    await db.append_proxy_log(
        "exec-1",
        logged_at=t2,
        stage="response_out",
        method="POST",
        path="v1/messages",
        upstream_url="http://upstream/v1/messages",
        status_code=200,
        content_type="application/json",
        body='{"ok":true}',
        input_tokens=12,
        output_tokens=4,
        total_tokens=16,
        total_cost_usd=0.0125,
    )

    rows = await db.list_proxy_logs("exec-1")
    assert len(rows) == 2
    assert rows[0]["stage"] == "request_in"
    assert rows[0]["meta"]["rewrite_applied"] is True
    assert rows[1]["stage"] == "response_out"
    assert rows[1]["status_code"] == 200
    assert rows[1]["input_tokens"] == 12
    assert rows[1]["output_tokens"] == 4
    assert rows[1]["total_tokens"] == 16
    assert rows[1]["total_cost_usd"] == 0.0125


@pytest.mark.asyncio
async def test_list_executions_for_session(db):
    await db.create_session(_session())
    await db.create_execution(_execution("exec-1", "sess-1"))
    await db.create_execution(_execution("exec-2", "sess-1"))

    execs = await db.list_executions("sess-1")
    assert len(execs) == 2
    assert {e.execution_id for e in execs} == {"exec-1", "exec-2"}


@pytest.mark.asyncio
async def test_list_active_executions(db):
    await db.create_session(_session("sess-1"))
    await db.create_session(_session("sess-2"))
    await db.create_execution(_execution("exec-1", "sess-1"))
    await db.create_execution(_execution("exec-2", "sess-2"))
    await db.halt_execution("exec-2", "exited")

    active = await db.list_active_executions()
    assert len(active) == 1
    assert active[0].execution_id == "exec-1"


@pytest.mark.asyncio
async def test_get_last_log_timestamp_none_when_empty(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    ts = await db.get_last_log_timestamp("exec-1")
    assert ts is None


@pytest.mark.asyncio
async def test_get_last_log_timestamp_returns_max(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    await db.append_execution_log("exec-1", LogLine(logged_at=t1, stream="stdout", body="a"))
    await db.append_execution_log("exec-1", LogLine(logged_at=t2, stream="stdout", body="b"))

    ts = await db.get_last_log_timestamp("exec-1")
    assert ts == t2


@pytest.mark.asyncio
async def test_append_and_list_conversation_events(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    first = await db.append_conversation_event(
        "exec-1",
        source="local",
        event_type="input",
        event_subtype="initial_message",
        payload={"body": "please fix it"},
    )
    second = await db.append_conversation_event(
        "exec-1",
        source="claude",
        event_type="result",
        payload={"result": "done"},
    )

    rows = await db.list_conversation_events("exec-1")
    assert len(rows) == 2
    assert first["seq"] == 1
    assert second["seq"] == 2
    assert rows[0]["event_type"] == "input"
    assert rows[1]["payload"]["result"] == "done"


@pytest.mark.asyncio
async def test_update_and_read_execution_claude_session_id(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    await db.update_execution_claude_session_id("exec-1", "claude-session-1")

    execution = await db.get_latest_execution("sess-1")
    assert execution is not None
    assert execution.claude_session_id == "claude-session-1"


@pytest.mark.asyncio
async def test_update_execution_claude_result_and_proxy_usage(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    await db.update_execution_claude_result(
        "exec-1",
        num_turns=4,
        duration_ms=2500,
    )
    await db.increment_execution_token_usage(
        "exec-1",
        input_tokens=100,
        output_tokens=40,
        cache_creation_input_tokens=20,
        cache_read_input_tokens=10,
        total_cost_usd=0.125,
    )

    execution = await db.get_latest_execution("sess-1")
    assert execution is not None
    assert execution.claude_num_turns == 4
    assert execution.claude_duration_ms == 2500
    assert execution.claude_total_cost_usd == 0.125
    assert execution.claude_input_tokens == 100
    assert execution.claude_output_tokens == 40
    assert execution.claude_cache_creation_input_tokens == 20
    assert execution.claude_cache_read_input_tokens == 10
    assert execution.claude_total_tokens == 170


@pytest.mark.asyncio
async def test_get_session_claude_summary_aggregates_executions(db):
    await db.create_session(_session())
    await db.create_execution(_execution("exec-1", "sess-1"))
    await db.create_execution(_execution("exec-2", "sess-1"))
    await db.update_execution_claude_result(
        "exec-1",
        num_turns=3,
        duration_ms=2000,
    )
    await db.increment_execution_token_usage(
        "exec-1",
        input_tokens=10,
        output_tokens=5,
        total_cost_usd=0.05,
    )
    await db.update_execution_claude_result(
        "exec-2",
        num_turns=2,
        duration_ms=1000,
    )
    await db.increment_execution_token_usage(
        "exec-2",
        input_tokens=20,
        output_tokens=10,
        cache_read_input_tokens=4,
        total_cost_usd=0.025,
    )
    await db.halt_execution("exec-1", "exited")

    summary = await db.get_session_claude_summary("sess-1")
    assert summary.executions_count == 2
    assert summary.completed_executions_count == 1
    assert summary.num_turns == 5
    assert summary.duration_ms == 3000
    assert summary.total_cost_usd == 0.075
    assert summary.input_tokens == 30
    assert summary.output_tokens == 15
    assert summary.cache_read_input_tokens == 4
    assert summary.total_tokens == 49


@pytest.mark.asyncio
async def test_increment_execution_token_usage_accumulates_proxy_cost(db):
    await db.create_session(_session())
    await db.create_execution(_execution())

    await db.increment_execution_token_usage("exec-1", total_cost_usd=0.01)
    await db.increment_execution_token_usage("exec-1", total_cost_usd=0.015)

    execution = await db.get_latest_execution("sess-1")
    assert execution is not None
    assert execution.claude_total_cost_usd == 0.025
