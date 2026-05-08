from datetime import datetime, timezone

import pytest

from claudius.controller.db import Database
from claudius.models import Session, SessionState


@pytest.mark.asyncio
async def test_list_messages_includes_sender(tmp_path):
    database = Database(tmp_path / "test.db")
    await database.init()

    session = Session(
        session_id="sess-1",
        thread_id="thread-1",
        channel="email",
        workflow_name="wf",
        state=SessionState.ACTIVE,
        workspace_path=str(tmp_path / "ws"),
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )
    await database.create_session(session)
    await database.store_message(
        "sess-1",
        "inbound",
        "hello",
        sender="user@example.com",
    )

    messages = await database.list_messages("sess-1")

    assert messages[0]["sender"] == "user@example.com"
