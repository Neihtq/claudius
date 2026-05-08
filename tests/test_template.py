from datetime import datetime, timezone
from claudius.config.template import expand, TemplateContext
from claudius.models import InboundMessage

def _ctx():
    msg = InboundMessage(
        channel="email",
        sender="user@example.com",
        recipients=["edit@example.com"],
        thread_id="thread-abc123",
        subject="GitLab: fix bug",
        body="Please fix it",
        attachments=[],
        received_at=datetime.now(timezone.utc),
    )
    return TemplateContext(request=msg, session_id="sess-xyz", channel_name="email")

def test_expand_request_thread_id():
    assert expand("claudius/${{request.thread_id}}", _ctx()) == "claudius/thread-abc123"

def test_expand_session_id():
    assert expand("branch-${{session.id}}", _ctx()) == "branch-sess-xyz"

def test_expand_channel_name():
    assert expand("via-${{channel.name}}", _ctx()) == "via-email"

def test_expand_request_sender():
    assert expand("from:${{request.sender}}", _ctx()) == "from:user@example.com"

def test_expand_request_subject():
    assert expand("subj:${{request.subject}}", _ctx()) == "subj:GitLab: fix bug"

def test_expand_unknown_leaves_original():
    assert expand("${{unknown.field}}", _ctx()) == "${{unknown.field}}"

def test_expand_no_templates():
    assert expand("plain string", _ctx()) == "plain string"
