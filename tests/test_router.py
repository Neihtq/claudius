from datetime import datetime, timezone
import pytest
from claudius.controller.router import match_workflow
from claudius.config.schema import WorkflowConfig
from claudius.models import InboundMessage

def _msg(channel="email", sender="user@example.com", subject="Hello", thread_id="t1"):
    return InboundMessage(
        channel=channel, sender=sender, recipients=["edit@example.com"], thread_id=thread_id,
        subject=subject, body="body", attachments=[],
        received_at=datetime.now(timezone.utc),
    )

def _workflow(channels=["email"], from_=[], to=[], subject_patterns=[], name="wf"):
    return WorkflowConfig.model_validate({
        "name": name,
        "routing": {
            "channels": channels,
            "from": from_,
            "to": to,
            "subject_patterns": subject_patterns,
        },
        "claude": {"system_prompt": "test"},
        "response": {"channel": "email"},
    })

def test_match_by_channel():
    wf = _workflow(channels=["email"])
    assert match_workflow(_msg(channel="email"), [wf]) == wf
    assert match_workflow(_msg(channel="whatsapp"), [wf]) is None

def test_match_exact_sender():
    wf = _workflow(from_=["specific@example.com"])
    assert match_workflow(_msg(sender="specific@example.com"), [wf]) == wf
    assert match_workflow(_msg(sender="other@example.com"), [wf]) is None

def test_match_glob_sender():
    wf = _workflow(from_=["*@company.com"])
    assert match_workflow(_msg(sender="alice@company.com"), [wf]) == wf
    assert match_workflow(_msg(sender="alice@other.com"), [wf]) is None

def test_match_exact_recipient():
    wf = _workflow(to=["edit@example.com"])
    assert match_workflow(_msg(), [wf]) == wf
    assert match_workflow(
        InboundMessage(
            channel="email",
            sender="user@example.com",
            recipients=["other@example.com"],
            thread_id="t1",
            subject="Hello",
            body="body",
            attachments=[],
            received_at=datetime.now(timezone.utc),
        ),
        [wf],
    ) is None

def test_match_glob_recipient():
    wf = _workflow(to=["edit@*.app"])
    assert match_workflow(
        InboundMessage(
            channel="email",
            sender="user@example.com",
            recipients=["edit@rosenstein.app"],
            thread_id="t1",
            subject="Hello",
            body="body",
            attachments=[],
            received_at=datetime.now(timezone.utc),
        ),
        [wf],
    ) == wf

def test_match_subject_pattern():
    wf = _workflow(subject_patterns=["GitLab:*"])
    assert match_workflow(_msg(subject="GitLab: fix bug"), [wf]) == wf
    assert match_workflow(_msg(subject="Hello"), [wf]) is None

def test_first_match_wins():
    wf1 = _workflow(name="first", channels=["email"])
    wf2 = _workflow(name="second", channels=["email"])
    assert match_workflow(_msg(), [wf1, wf2]) == wf1

def test_no_from_filter_matches_all_senders():
    wf = _workflow(from_=[])
    assert match_workflow(_msg(sender="anyone@anywhere.com"), [wf]) == wf

def test_no_subject_filter_matches_all_subjects():
    wf = _workflow(subject_patterns=[])
    assert match_workflow(_msg(subject="anything"), [wf]) == wf
