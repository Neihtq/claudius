import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from claudius.config.schema import WorkflowConfig
from claudius.controller.backends.base import AbstractBackend
from claudius.controller.db import Database
from claudius.controller.session_manager import SessionManager
from claudius.controller.sse import SSEBroker
from claudius.models import Execution, LogLine, Session, SessionState


def _workflow():
    return WorkflowConfig.model_validate({
        "name": "wf",
        "routing": {"channels": ["email"]},
        "claude": {"system_prompt": "test"},
        "response": {"channel": "email"},
    })


def _make_execution(execution_id="exec-1", session_id="sess-1"):
    return Execution(
        execution_id=execution_id, session_id=session_id,
        worker_address="172.0.0.1:8080",
        started_at=datetime.now(timezone.utc),
        halted_at=None, halt_reason=None,
    )


def _make_session(session_id="sess-1", tmp_path=None):
    now = datetime.now(timezone.utc)
    return Session(
        session_id=session_id, thread_id=f"t-{session_id}",
        channel="email", workflow_name="wf", state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / session_id) if tmp_path else "/tmp",
        created_at=now, last_message_at=now,
    )


@pytest.fixture
async def db(tmp_path):
    database = Database(tmp_path / "test.db")
    await database.init()
    yield database
    await database.close()


@pytest.mark.asyncio
async def test_tail_task_stores_log_lines(db, tmp_path):
    t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)

    async def fake_tail(execution, since=None):
        yield LogLine(logged_at=t1, stream="stdout", body="hello")
        yield LogLine(logged_at=t2, stream="stderr", body="error")

    backend = MagicMock(spec=AbstractBackend)
    backend.tail_logs = fake_tail
    backend.get_exit_code = AsyncMock(return_value=0)
    backend.delete_execution = AsyncMock()

    await db.create_session(_make_session(tmp_path=tmp_path))
    exec_ = _make_execution()
    await db.create_execution(exec_)

    manager = SessionManager(
        db=db, backend=backend, workflows=[_workflow()],
        workspaces_path=str(tmp_path), channels={},
    )
    manager._start_log_tailer(exec_)
    await asyncio.sleep(0.1)

    logs = await db.list_execution_logs("exec-1")
    assert len(logs) == 2
    assert logs[0]["body"] == "hello"
    assert logs[1]["body"] == "error"


@pytest.mark.asyncio
async def test_tail_task_halts_and_deletes_on_stream_end(db, tmp_path):
    async def fake_tail(execution, since=None):
        yield LogLine(logged_at=datetime.now(timezone.utc), stream="stdout", body="done")

    backend = MagicMock(spec=AbstractBackend)
    backend.tail_logs = fake_tail
    backend.get_exit_code = AsyncMock(return_value=0)
    backend.delete_execution = AsyncMock()

    await db.create_session(_make_session(tmp_path=tmp_path))
    exec_ = _make_execution()
    await db.create_execution(exec_)

    manager = SessionManager(
        db=db, backend=backend, workflows=[_workflow()],
        workspaces_path=str(tmp_path), channels={},
    )
    manager._start_log_tailer(exec_)
    await asyncio.sleep(0.1)

    active = await db.get_active_execution("sess-1")
    assert active is None
    backend.delete_execution.assert_called_once()


@pytest.mark.asyncio
async def test_tail_task_publishes_sse_log_and_execution_events(db, tmp_path):
    t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    async def fake_tail(execution, since=None):
        yield LogLine(logged_at=t1, stream="stdout", body="from container")

    backend = MagicMock(spec=AbstractBackend)
    backend.tail_logs = fake_tail
    backend.get_exit_code = AsyncMock(return_value=0)
    backend.delete_execution = AsyncMock()

    broker = SSEBroker()
    q: asyncio.Queue = asyncio.Queue()
    broker.subscribe("sess-1", q)

    await db.create_session(_make_session(tmp_path=tmp_path))
    exec_ = _make_execution()
    await db.create_execution(exec_)

    manager = SessionManager(
        db=db, backend=backend, workflows=[_workflow()],
        workspaces_path=str(tmp_path), channels={}, broker=broker,
    )
    manager._start_log_tailer(exec_)
    await asyncio.sleep(0.1)

    events = []
    while not q.empty():
        events.append(json.loads(await q.get()))

    log_events = [e for e in events if e["type"] == "log"]
    assert len(log_events) >= 1
    assert log_events[0]["body"] == "from container"
    assert log_events[0]["stream"] == "stdout"

    exec_events = [e for e in events if e["type"] == "execution"]
    assert any(e["execution"]["halted_at"] is not None for e in exec_events)
    assert any(e["execution"]["exit_code"] == 0 for e in exec_events)


@pytest.mark.asyncio
async def test_tail_task_sets_last_execution_result(db, tmp_path):
    async def fake_tail_ok(execution, since=None):
        yield LogLine(logged_at=datetime.now(timezone.utc), stream="stdout", body="ok")

    async def fake_tail_fail(execution, since=None):
        yield LogLine(logged_at=datetime.now(timezone.utc), stream="stdout", body="fail")

    for exit_code, expected_result in [(0, "ok"), (1, "failed"), (None, "failed")]:
        session_id = f"sess-{exit_code}"
        exec_id = f"exec-{exit_code}"
        sess = Session(
            session_id=session_id, thread_id=f"t-{exit_code}",
            channel="email", workflow_name="wf", state=SessionState.ACTIVE,
            workspace_path=str(tmp_path / session_id),
            created_at=datetime.now(timezone.utc), last_message_at=datetime.now(timezone.utc),
        )
        await db.create_session(sess)
        exec_ = Execution(
            execution_id=exec_id, session_id=session_id,
            worker_address="172.0.0.1:8080",
            started_at=datetime.now(timezone.utc), halted_at=None, halt_reason=None,
        )
        await db.create_execution(exec_)

        backend = MagicMock(spec=AbstractBackend)
        backend.tail_logs = fake_tail_ok
        backend.get_exit_code = AsyncMock(return_value=exit_code)
        backend.delete_execution = AsyncMock()

        manager = SessionManager(
            db=db, backend=backend, workflows=[_workflow()],
            workspaces_path=str(tmp_path), channels={},
        )
        manager._start_log_tailer(exec_)
        await asyncio.sleep(0.1)

        session = await db.get_session(session_id)
        assert session.last_execution_result == expected_result, \
            f"exit_code={exit_code}: expected {expected_result!r}, got {session.last_execution_result!r}"


@pytest.mark.asyncio
async def test_recover_resumes_active_executions(db, tmp_path):
    call_args = []

    async def fake_tail(execution, since=None):
        call_args.append((execution.execution_id, since))
        return
        yield

    backend = MagicMock(spec=AbstractBackend)
    backend.tail_logs = fake_tail
    backend.get_exit_code = AsyncMock(return_value=0)
    backend.delete_execution = AsyncMock()

    now = datetime.now(timezone.utc)
    for i in range(2):
        session = Session(
            session_id=f"sess-{i}", thread_id=f"t{i}", channel="email",
            workflow_name="wf", state=SessionState.ACTIVE,
            workspace_path=str(tmp_path / f"sess-{i}"),
            created_at=now, last_message_at=now,
        )
        await db.create_session(session)
        await db.create_execution(Execution(
            execution_id=f"exec-{i}", session_id=f"sess-{i}",
            worker_address="172.0.0.1:8080",
            started_at=now, halted_at=None, halt_reason=None,
        ))

    manager = SessionManager(
        db=db, backend=backend, workflows=[_workflow()],
        workspaces_path=str(tmp_path), channels={},
    )
    await manager.recover()
    await asyncio.sleep(0.1)

    assert len(call_args) == 2
    assert {a[0] for a in call_args} == {"exec-0", "exec-1"}
