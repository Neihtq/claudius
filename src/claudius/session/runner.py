import asyncio
import json
import mimetypes
import os
import shlex
import signal
import sys
from contextlib import suppress
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
from loguru import logger

from claudius.channels.base import AbstractChannel
from claudius.config.schema import WorkflowConfig
from claudius.models import Attachment, InboundMessage
from claudius.session.attachments import sync_workspace_attachments


_NO_IMAGE_SUPPORT_PROMPT = """
## Image inputs not supported

The model you are running on does not support image inputs. Do not use the Read
tool on image files (PNG, JPG, JPEG, GIF, WEBP, BMP, SVG, or any other image
format). Describe what an image is expected to contain based on context instead.
""".strip()

_ERROR_REPORTING_PROMPT = """
## Reporting irrecoverable errors

If you encounter an error that prevents you from completing this task — such as a
tool failure, missing permission, unavailable resource, or configuration problem —
call the `report_fatal_error` tool with:
- `category`: one of tool_failure, permission_error, resource_unavailable,
  configuration_error, or unexpected_error
- `reason`: a concise explanation of why the task cannot continue

After calling `report_fatal_error`, send the user a brief, non-technical closing
message and stop. Do not attempt to retry or work around the error.
""".strip()


class SessionRunner:
    _STREAM_READ_SIZE = 65536

    def __init__(
        self,
        workflow: WorkflowConfig,
        initial_message: InboundMessage,
        channel: AbstractChannel,
        workspace_path: str,
        idle_timeout: float = 60.0,
        conversation_text: str = "",
        log_conversation: bool = False,
        callback_url: str = "",
        session_id: str = "",
        session_token: str = "",
        claude_resume_session_id: str | None = None,
    ):
        self._workflow = workflow
        self._initial_message = initial_message
        self._channel = channel
        self._workspace_path = workspace_path
        self._conversation_text = conversation_text.strip()
        self._idle_timeout = idle_timeout
        self._log_conversation = log_conversation
        self._callback_url = callback_url.rstrip("/")
        self._session_id = session_id
        self._session_token = session_token
        self._claude_resume_session_id = claude_resume_session_id
        self._accepting = True
        self._stopping = False
        self.is_done = False
        self._assistant_messages: list[str] = []
        self._process: asyncio.subprocess.Process | None = None

    def inject_message(self, message: InboundMessage) -> bool:
        del message
        return False

    async def graceful_stop(self, timeout: float = 10.0) -> bool:
        self._accepting = False
        self._stopping = True
        process = self._process
        if process is None or process.returncode is not None:
            return True
        try:
            process.send_signal(signal.SIGTERM)
        except (AttributeError, ProcessLookupError):
            with suppress(ProcessLookupError):
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            with suppress(ProcessLookupError):
                process.kill()
            with suppress(ProcessLookupError):
                await process.wait()
            return False

    async def run(self) -> None:
        self.is_done = False
        try:
            body, stderr, returncode = await self._run_claude_code()
            if returncode != 0 and not body:
                detail = stderr.strip() or f"Claude Code exited with status {returncode}."
                body = f"Repository task failed.\n\n{detail}"
            if body:
                await self._channel.send_message(
                    to=self._initial_message.sender,
                    body=body,
                    thread_id=self._initial_message.thread_id,
                    attachments=self._collect_output_attachments(),
                )
            if returncode != 0:
                raise RuntimeError(f"Claude Code exited with status {returncode}")
            self.is_done = True
        finally:
            self._accepting = False

    async def _run_claude_code(self) -> tuple[str, str, int]:
        await sync_workspace_attachments(
            callback_url=self._callback_url,
            session_id=self._session_id,
            session_token=self._session_token,
            workspace_path=self._workspace_path,
        )
        cmd = self._build_command()
        logger.info("running Claude Code: {}", shlex.join(cmd))
        env = os.environ.copy()
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=self._workspace_path,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._process = process

        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        final_result: dict[str, Any] = {}
        try:
            await asyncio.gather(
                self._drain_stdout(process.stdout, stdout_lines, sys.stdout, final_result),
                self._drain_stream(process.stderr, stderr_lines, sys.stderr),
            )
        finally:
            if process.returncode is None:
                process.kill()
                with suppress(ProcessLookupError):
                    await process.wait()
            self._process = None
        returncode = process.returncode if process.returncode is not None else await process.wait()
        return self._final_body(final_result), "".join(stderr_lines), returncode

    def _final_body(self, final_result: dict[str, Any]) -> str:
        result_text = final_result.get("result")
        if isinstance(result_text, str) and result_text.strip():
            return result_text.strip()
        if self._assistant_messages:
            return "\n\n".join(part for part in self._assistant_messages if part).strip()
        return ""

    async def _drain_stdout(
        self,
        stream: asyncio.StreamReader | None,
        sink: list[str],
        output,
        final_result: dict[str, Any],
    ) -> None:
        if stream is None:
            return
        async for text in self._iter_stream_lines(stream, sink, output):
            await self._handle_stdout_line(text, final_result)

    async def _drain_stream(
        self,
        stream: asyncio.StreamReader | None,
        sink: list[str],
        output,
    ) -> None:
        if stream is None:
            return
        async for _ in self._iter_stream_lines(stream, sink, output):
            pass

    async def _iter_stream_lines(
        self,
        stream: asyncio.StreamReader,
        sink: list[str],
        output,
    ):
        pending = bytearray()
        while True:
            chunk = await stream.read(self._STREAM_READ_SIZE)
            if not chunk:
                break
            pending.extend(chunk)
            while True:
                newline_index = pending.find(b"\n")
                if newline_index < 0:
                    break
                line = bytes(pending[: newline_index + 1])
                del pending[: newline_index + 1]
                text = line.decode(errors="replace")
                sink.append(text)
                output.write(text)
                output.flush()
                yield text
        if pending:
            text = pending.decode(errors="replace")
            sink.append(text)
            output.write(text)
            output.flush()
            yield text

    async def _handle_stdout_line(self, line: str, final_result: dict[str, Any]) -> None:
        stripped = line.strip()
        if not stripped:
            return
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            return
        event_type = payload.get("type")
        event_subtype = payload.get("subtype")
        if isinstance(event_type, str):
            await self._post_conversation_event(
                event_type=event_type,
                event_subtype=event_subtype if isinstance(event_subtype, str) else None,
                payload=payload,
            )
            if event_type == "assistant":
                text = self._extract_assistant_text(payload)
                if text:
                    self._assistant_messages.append(text)
            elif event_type == "result":
                final_result.clear()
                final_result.update(payload)

    async def _post_conversation_event(
        self, *, event_type: str, payload: dict[str, Any], event_subtype: str | None
    ) -> None:
        if not self._callback_url or not self._session_id:
            return
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{self._callback_url}/sessions/{self._session_id}/conversation-events",
                    json={
                        "source": "claude",
                        "event_type": event_type,
                        "event_subtype": event_subtype,
                        "payload": payload,
                    },
                    timeout=5.0,
                )
        except Exception:
            return

    def _extract_assistant_text(self, payload: dict[str, Any]) -> str:
        message = payload.get("message")
        if isinstance(message, dict):
            return self._extract_text_from_content(message.get("content"))
        return self._extract_text_from_content(payload.get("content"))

    def _extract_text_from_content(self, content: Any) -> str:
        if not isinstance(content, list):
            return ""
        parts: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "text":
                continue
            text = block.get("text")
            if isinstance(text, str) and text:
                parts.append(text)
        return "".join(parts).strip()

    def _build_command(self) -> Sequence[str]:
        prompt = self._build_prompt()
        mcp_config = os.environ.get("CLAUDIUS_MCP_CONFIG", "").strip()
        system_prompt = self._workflow.claude.system_prompt
        if not self._workflow.claude.supports_images:
            system_prompt = system_prompt + "\n\n" + _NO_IMAGE_SUPPORT_PROMPT
        if mcp_config:
            system_prompt = system_prompt + "\n\n" + _ERROR_REPORTING_PROMPT
        cmd: list[str] = [
            os.environ.get("CLAUDIUS_CLAUDE_BIN", "claude"),
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--dangerously-skip-permissions",
            "--add-dir",
            self._workspace_path,
            "--model",
            self._workflow.claude.model,
            "--system-prompt",
            system_prompt,
        ]
        if self._claude_resume_session_id:
            cmd.extend(["--resume", self._claude_resume_session_id])
        if mcp_config:
            cmd.extend(["--mcp-config", mcp_config, "--strict-mcp-config"])
        cmd.append(prompt)
        return cmd

    def _collect_output_attachments(self) -> list[Attachment]:
        output_dir = os.environ.get("CLAUDIUS_OUTPUT_DIR", "").strip()
        if not output_dir:
            return []
        root = Path(output_dir)
        if not root.is_dir():
            return []
        attachments: list[Attachment] = []
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            filename = path.relative_to(root).as_posix()
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            attachments.append(Attachment(
                filename=filename,
                content_type=content_type,
                data=path.read_bytes(),
            ))
        return attachments

    def _build_prompt(self) -> str:
        history_hint = ""
        if self._callback_url and self._session_token and self._session_id:
            history_hint = (
                "If you need earlier session context that is not already available, "
                "you may run `claudius session-history --side both --limit 20` to fetch "
                "stored session messages. Use it only when necessary; you can filter with "
                "`--side user|assistant|both` and `--query <text>`.\n\n"
            )
        output_dir = os.environ.get("CLAUDIUS_OUTPUT_DIR", "").strip()
        output_hint = ""
        if output_dir:
            output_hint = (
                f"Any files you want returned to the user must be written into the directory "
                f"given by the environment variable CLAUDIUS_OUTPUT_DIR "
                f"(currently {output_dir!r}). "
                f"Always read this variable at runtime — never hardcode the path — "
                f"because it changes between executions. "
                f"In shell scripts use $CLAUDIUS_OUTPUT_DIR, in Node.js use "
                f"process.env.CLAUDIUS_OUTPUT_DIR, in Python use "
                f"os.environ['CLAUDIUS_OUTPUT_DIR'].\n\n"
            )
        return (
            "Work only inside /workspace. Clone public HTTPS repositories explicitly when needed, "
            "make the requested changes locally, run relevant checks, and end with a concise summary "
            "of what you changed and any remaining issues. User attachments, when present, are "
            "available under /workspace/attachments.\n\n"
            f"{output_hint}"
            f"{history_hint}"
            f"{self._conversation_text or self._initial_message.body}\n"
        )
