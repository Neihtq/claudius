import asyncio
import json
import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from claudius.config.schema import WorkflowConfig
from claudius.models import InboundMessage
from claudius.session.runner import SessionRunner, _ERROR_REPORTING_PROMPT


class _FakeStream:
    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)

    async def read(self, _: int) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


class _FakeProcess:
    def __init__(self, stdout: list[bytes], stderr: list[bytes], returncode: int):
        self.stdout = _FakeStream(stdout)
        self.stderr = _FakeStream(stderr)
        self._returncode = returncode
        self.returncode = returncode
        self.sent_signals: list[int] = []
        self.terminated = False

    async def wait(self) -> int:
        return self._returncode

    def send_signal(self, sig: int) -> None:
        self.sent_signals.append(sig)
        self.returncode = self._returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = self._returncode

    def kill(self) -> None:
        self.returncode = self._returncode


class _WaitableProcess:
    def __init__(self):
        self.returncode = None
        self.killed = False
        self.sent_signals: list[int] = []
        self._wait_event = asyncio.Event()

    async def wait(self) -> int:
        await self._wait_event.wait()
        return 0

    def send_signal(self, sig: int) -> None:
        self.sent_signals.append(sig)

    def terminate(self) -> None:
        self.sent_signals.append(-1)

    def kill(self) -> None:
        self.killed = True
        self.returncode = 137
        self._wait_event.set()


def _workflow():
    return WorkflowConfig.model_validate({
        "name": "test",
        "routing": {"channels": ["email"]},
        "claude": {"system_prompt": "You are a test assistant."},
        "response": {"channel": "email"},
    })


def _message(body="Please do a task"):
    return InboundMessage(
        channel="email",
        sender="user@example.com",
        recipients=["edit@example.com"],
        thread_id="t1",
        subject="Task",
        body=body,
        attachments=[],
        received_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_runner_invokes_claude_and_sends_output():
    mock_channel = AsyncMock()
    stdout = [
        (json.dumps({
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "Task complete!"}]},
        }) + "\n").encode(),
        (json.dumps({"type": "result", "subtype": "success", "result": "Task complete!"}) + "\n").encode(),
    ]
    fake_process = _FakeProcess(stdout, [], 0)

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)) as mock_exec,
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=0)) as mock_sync,
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
            conversation_text="Please do a task",
        )
        await runner.run()

    mock_channel.send_message.assert_called_once()
    mock_sync.assert_awaited_once()
    assert mock_channel.send_message.call_args.kwargs["body"] == "Task complete!"
    assert mock_exec.await_args.kwargs["cwd"] == "/workspace"
    cmd = mock_exec.await_args.args
    assert cmd[0] == "claude"
    assert "--print" in cmd
    assert "--output-format" in cmd
    assert "stream-json" in cmd
    assert "--dangerously-skip-permissions" in cmd
    assert "--system-prompt" in cmd
    assert runner.is_done


@pytest.mark.asyncio
async def test_runner_adds_resume_flag_when_present():
    mock_channel = AsyncMock()
    stdout = [(json.dumps({"type": "result", "subtype": "success", "result": ""}) + "\n").encode()]
    fake_process = _FakeProcess(stdout, [], 0)

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)) as mock_exec,
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=0)),
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
            claude_resume_session_id="claude-session-123",
        )
        await runner.run()

    cmd = list(mock_exec.await_args.args)
    assert "--resume" in cmd
    assert "claude-session-123" in cmd


@pytest.mark.asyncio
async def test_runner_adds_mcp_config_flags(monkeypatch):
    mock_channel = AsyncMock()
    stdout = [(json.dumps({"type": "result", "subtype": "success", "result": ""}) + "\n").encode()]
    fake_process = _FakeProcess(stdout, [], 0)
    monkeypatch.setenv("CLAUDIUS_MCP_CONFIG", "/workspace/.claudius-runtime/mcp.json")

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)) as mock_exec,
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=0)),
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
        )
        await runner.run()

    cmd = list(mock_exec.await_args.args)
    assert "--mcp-config" in cmd
    assert "/workspace/.claudius-runtime/mcp.json" in cmd
    assert "--strict-mcp-config" in cmd


@pytest.mark.asyncio
async def test_runner_sends_failure_message_when_cli_fails_without_stdout():
    mock_channel = AsyncMock()
    fake_process = _FakeProcess([], [b"fatal: clone failed\n"], 1)

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)),
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=0)),
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
        )
        with pytest.raises(RuntimeError, match="status 1"):
            await runner.run()

    mock_channel.send_message.assert_called_once()
    body = mock_channel.send_message.call_args.kwargs["body"]
    assert "Repository task failed." in body
    assert "fatal: clone failed" in body


@pytest.mark.asyncio
async def test_runner_rejects_injected_messages():
    runner = SessionRunner(
        workflow=_workflow(),
        initial_message=_message(),
        channel=AsyncMock(),
        workspace_path="/workspace",
    )
    assert runner.inject_message(_message("follow up")) is False


@pytest.mark.asyncio
async def test_runner_syncs_attachments_with_session_token():
    mock_channel = AsyncMock()
    stdout = [(json.dumps({"type": "result", "subtype": "success", "result": ""}) + "\n").encode()]
    fake_process = _FakeProcess(stdout, [], 0)

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)),
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=2)) as mock_sync,
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
            callback_url="http://controller",
            session_id="sess-1",
            session_token="token-123",
        )
        await runner.run()

    mock_sync.assert_awaited_once_with(
        callback_url="http://controller",
        session_id="sess-1",
        session_token="token-123",
        workspace_path="/workspace",
    )


@pytest.mark.asyncio
async def test_runner_handles_oversized_stdout_line_without_newline():
    mock_channel = AsyncMock()
    large_text = "x" * 70000
    result_line = json.dumps({"type": "result", "subtype": "success", "result": large_text}).encode()
    fake_process = _FakeProcess([result_line[:35000], result_line[35000:]], [], 0)

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)),
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=0)),
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
        )
        await runner.run()

    mock_channel.send_message.assert_called_once()
    assert mock_channel.send_message.call_args.kwargs["body"] == large_text


@pytest.mark.asyncio
async def test_runner_attaches_output_directory_files(tmp_path, monkeypatch):
    mock_channel = AsyncMock()
    output_dir = tmp_path / "outputs" / "msg-1"
    output_dir.mkdir(parents=True)
    (output_dir / "summary.html").write_text("<html>ok</html>")
    nested = output_dir / "nested"
    nested.mkdir()
    (nested / "data.json").write_text('{"ok":true}')
    stdout = [(json.dumps({"type": "result", "subtype": "success", "result": "done"}) + "\n").encode()]
    fake_process = _FakeProcess(stdout, [], 0)
    monkeypatch.setenv("CLAUDIUS_OUTPUT_DIR", str(output_dir))

    with (
        patch("claudius.session.runner.asyncio.create_subprocess_exec", AsyncMock(return_value=fake_process)),
        patch("claudius.session.runner.sync_workspace_attachments", AsyncMock(return_value=0)),
    ):
        runner = SessionRunner(
            workflow=_workflow(),
            initial_message=_message(),
            channel=mock_channel,
            workspace_path="/workspace",
        )
        await runner.run()

    attachments = mock_channel.send_message.call_args.kwargs["attachments"]
    assert [attachment.filename for attachment in attachments] == ["nested/data.json", "summary.html"]
    assert [attachment.content_type for attachment in attachments] == ["application/json", "text/html"]


def test_runner_prompt_mentions_session_history_helper_when_available():
    runner = SessionRunner(
        workflow=_workflow(),
        initial_message=_message(),
        channel=AsyncMock(),
        workspace_path="/workspace",
        callback_url="http://controller",
        session_id="sess-1",
        session_token="token-123",
    )

    prompt = runner._build_prompt()
    assert "claudius session-history" in prompt
    assert "Use it only when necessary" in prompt


def test_build_command_appends_error_reporting_prompt_when_mcp_configured(monkeypatch):
    monkeypatch.setenv("CLAUDIUS_MCP_CONFIG", "/workspace/.claudius-runtime/mcp.json")
    runner = SessionRunner(
        workflow=_workflow(),
        initial_message=_message(),
        channel=AsyncMock(),
        workspace_path="/workspace",
    )
    cmd = list(runner._build_command())
    idx = cmd.index("--system-prompt")
    system_prompt = cmd[idx + 1]
    assert "You are a test assistant." in system_prompt
    assert "report_fatal_error" in system_prompt
    assert _ERROR_REPORTING_PROMPT in system_prompt


def test_build_command_does_not_append_error_reporting_prompt_without_mcp(monkeypatch):
    monkeypatch.delenv("CLAUDIUS_MCP_CONFIG", raising=False)
    runner = SessionRunner(
        workflow=_workflow(),
        initial_message=_message(),
        channel=AsyncMock(),
        workspace_path="/workspace",
    )
    cmd = list(runner._build_command())
    idx = cmd.index("--system-prompt")
    system_prompt = cmd[idx + 1]
    assert system_prompt == "You are a test assistant."
    assert "report_fatal_error" not in system_prompt


@pytest.mark.asyncio
async def test_runner_graceful_stop_sends_sigterm_and_waits_for_exit():
    runner = SessionRunner(
        workflow=_workflow(),
        initial_message=_message(),
        channel=AsyncMock(),
        workspace_path="/workspace",
    )
    process = _WaitableProcess()
    runner._process = process

    stop_task = asyncio.create_task(runner.graceful_stop(timeout=0.1))
    await asyncio.sleep(0)
    process.returncode = 0
    process._wait_event.set()
    graceful = await stop_task

    assert graceful is True
    assert process.sent_signals
    assert runner._accepting is False


@pytest.mark.asyncio
async def test_runner_graceful_stop_escalates_to_kill_after_timeout():
    runner = SessionRunner(
        workflow=_workflow(),
        initial_message=_message(),
        channel=AsyncMock(),
        workspace_path="/workspace",
    )
    process = _WaitableProcess()
    runner._process = process

    graceful = await runner.graceful_stop(timeout=0.01)

    assert graceful is False
    assert process.killed is True
