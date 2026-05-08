from datetime import datetime, timezone
from claudius.models import (
    InboundMessage, Session, Execution, SessionState, ToolMount, Attachment
)

def test_inbound_message_fields():
    msg = InboundMessage(
        channel="email",
        sender="user@example.com",
        recipients=["edit@example.com"],
        thread_id="thread-123",
        subject="Hello",
        body="Hi there",
        attachments=[],
        received_at=datetime.now(timezone.utc),
    )
    assert msg.channel == "email"
    assert msg.thread_id == "thread-123"

def test_session_state_values():
    assert SessionState.ACTIVE == "active"
    assert SessionState.HIBERNATED == "hibernated"
    assert SessionState.WAITING == "waiting"

def test_tool_mount_defaults():
    mount = ToolMount()
    assert mount.env_vars == {}
    assert mount.volumes == []
    assert mount.sidecars == []
    assert mount.pre_launch_commands == []
