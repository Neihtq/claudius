import json
import mimetypes
from abc import ABC
from copy import deepcopy
from typing import Any, AsyncIterator


_ANTHROPIC_HEADER_PREFIX = "anthropic-"
_SUPPORTED_DOC_TYPES = {
    "application/pdf",
    "text/plain",
    "text/html",
    "text/csv",
    "text/xml",
}
_FINISH_REASON_MAP = {
    "stop": "end_turn",
    "tool_calls": "tool_use",
    "length": "max_tokens",
    "content_filter": "stop_sequence",
}


def _data_url(media_type: str, data: str) -> str:
    return f"data:{media_type};base64,{data}"


def _guess_filename(media_type: str) -> str:
    if media_type == "application/pdf":
        return "document.pdf"
    suffix = mimetypes.guess_extension(media_type) or ".bin"
    return f"attachment{suffix}"


def _sse_event(event_type: str, data: dict) -> bytes:
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n".encode("utf-8")


class MessageFormatAdapter(ABC):
    """Adapts between Anthropic format (what Claude Code sends/expects) and an upstream API format."""

    def adapt_request(
        self,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[str, dict[str, str], bytes]:
        """Return (adapted_path, adapted_headers, adapted_body)."""
        return path, headers, body

    async def adapt_response_stream(
        self,
        content_type: str | None,
        chunks: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        """Wrap upstream byte stream, yielding Anthropic-format bytes."""
        async for chunk in chunks:
            yield chunk


class AnthropicAdapter(MessageFormatAdapter):
    """Identity adapter — upstream speaks Anthropic format natively."""
    pass


class OpenAIAdapter(MessageFormatAdapter):
    """Full bidirectional Anthropic ↔ OpenAI chat-completions conversion."""

    def adapt_request(
        self,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[str, dict[str, str], bytes]:
        adapted_path = "chat/completions" if path.strip("/") == "v1/messages" else path

        adapted_headers = {
            k: v for k, v in headers.items()
            if not k.lower().startswith(_ANTHROPIC_HEADER_PREFIX)
        }

        if (
            body
            and "application/json" in headers.get("content-type", "").lower()
            and path.strip("/") == "v1/messages"
        ):
            try:
                payload = json.loads(body)
                if isinstance(payload, dict):
                    body = json.dumps(_convert_anthropic_to_openai_request(payload)).encode("utf-8")
            except (json.JSONDecodeError, ValueError):
                pass

        return adapted_path, adapted_headers, body

    async def adapt_response_stream(
        self,
        content_type: str | None,
        chunks: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        ct = (content_type or "").lower()

        if "text/event-stream" in ct:
            async for chunk in _openai_sse_to_anthropic_sse(chunks):
                yield chunk
        elif "application/json" in ct:
            parts: list[bytes] = []
            async for chunk in chunks:
                parts.append(chunk)
            raw = b"".join(parts)
            try:
                converted = _convert_openai_to_anthropic_response(json.loads(raw))
                yield json.dumps(converted).encode("utf-8")
            except Exception:
                yield raw
        else:
            async for chunk in chunks:
                yield chunk


# ── Request conversion: Anthropic → OpenAI ────────────────────────────────────

def _convert_anthropic_to_openai_request(payload: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key in ("model", "max_tokens", "temperature", "top_p", "frequency_penalty", "presence_penalty"):
        if key in payload:
            result[key] = payload[key]

    messages: list[dict[str, Any]] = []

    system = payload.get("system")
    if system:
        if isinstance(system, str):
            messages.append({"role": "system", "content": system})
        elif isinstance(system, list):
            text = "\n".join(
                b.get("text", "") for b in system
                if isinstance(b, dict) and b.get("type") == "text"
            )
            if text:
                messages.append({"role": "system", "content": text})

    for msg in payload.get("messages", []):
        converted = _convert_anthropic_message(msg)
        if isinstance(converted, list):
            messages.extend(converted)
        else:
            messages.append(converted)

    result["messages"] = messages

    tools = payload.get("tools")
    if tools:
        result["tools"] = [_convert_anthropic_tool(t) for t in tools]

    tool_choice = payload.get("tool_choice")
    if tool_choice:
        result["tool_choice"] = _convert_anthropic_tool_choice(tool_choice)

    stop_sequences = payload.get("stop_sequences")
    if stop_sequences:
        result["stop"] = stop_sequences

    if "stream" in payload:
        result["stream"] = payload["stream"]
        if payload["stream"]:
            result["stream_options"] = {"include_usage": True}

    return result


def _convert_anthropic_message(msg: dict[str, Any]) -> dict[str, Any] | list[dict[str, Any]]:
    role = msg.get("role", "user")
    content = msg.get("content")

    if isinstance(content, str):
        return {"role": role, "content": content}

    if not isinstance(content, list):
        return {"role": role, "content": content}

    if role == "assistant":
        return _convert_assistant_message(content)

    if role == "user":
        return _convert_user_message(content)

    converted = [_convert_content_block(b) for b in content if isinstance(b, dict)]
    return {"role": role, "content": converted}


def _convert_assistant_message(content: list[Any]) -> dict[str, Any]:
    text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []

    for block in content:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text":
            text_parts.append(block.get("text", ""))
        elif block_type == "tool_use":
            tool_calls.append({
                "id": block.get("id", ""),
                "type": "function",
                "function": {
                    "name": block.get("name", ""),
                    "arguments": json.dumps(block.get("input", {})),
                },
            })

    result: dict[str, Any] = {"role": "assistant"}
    result["content"] = "\n".join(text_parts) if text_parts else None
    if tool_calls:
        result["tool_calls"] = tool_calls
    return result


def _convert_user_message(content: list[Any]) -> dict[str, Any] | list[dict[str, Any]]:
    has_tool_results = any(
        isinstance(b, dict) and b.get("type") == "tool_result"
        for b in content
    )

    if not has_tool_results:
        converted = [_convert_content_block(b) for b in content if isinstance(b, dict)]
        if len(converted) == 1 and isinstance(converted[0], dict) and converted[0].get("type") == "text":
            return {"role": "user", "content": converted[0]["text"]}
        return {"role": "user", "content": converted}

    messages: list[dict[str, Any]] = []
    pending_text: list[dict[str, Any]] = []

    for block in content:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")

        if block_type == "tool_result":
            if pending_text:
                text = "\n".join(b["text"] for b in pending_text if b.get("type") == "text")
                messages.append({"role": "user", "content": text})
                pending_text = []

            tool_content = block.get("content")
            if isinstance(tool_content, list):
                text = "\n".join(
                    b.get("text", "") for b in tool_content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
            elif isinstance(tool_content, str):
                text = tool_content
            else:
                text = ""

            messages.append({
                "role": "tool",
                "tool_call_id": block.get("tool_use_id", ""),
                "content": text,
            })
        else:
            pending_text.append(_convert_content_block(block))

    if pending_text:
        text = "\n".join(b["text"] for b in pending_text if isinstance(b, dict) and b.get("type") == "text")
        messages.append({"role": "user", "content": text})

    return messages if len(messages) != 1 else messages[0]


def _convert_content_block(block: dict[str, Any]) -> dict[str, Any]:
    block_type = block.get("type")

    if block_type == "text":
        return {"type": "text", "text": block.get("text", "")}

    if block_type == "image":
        source = block.get("source", {})
        return {
            "type": "image_url",
            "image_url": {"url": _data_url(source.get("media_type", ""), source.get("data", ""))},
        }

    if block_type == "document":
        source = block.get("source", {})
        media_type = source.get("media_type", "")
        return {
            "type": "file",
            "file": {
                "filename": block.get("title") or _guess_filename(media_type),
                "file_data": _data_url(media_type, source.get("data", "")),
            },
        }

    return deepcopy(block)


def _convert_anthropic_tool(tool: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": tool.get("name", ""),
            "description": tool.get("description", ""),
            "parameters": tool.get("input_schema", {}),
        },
    }


def _convert_anthropic_tool_choice(tool_choice: dict[str, Any]) -> Any:
    tc_type = tool_choice.get("type")
    if tc_type == "auto":
        return "auto"
    if tc_type == "any":
        return "required"
    if tc_type == "tool":
        return {"type": "function", "function": {"name": tool_choice.get("name", "")}}
    return "auto"


# ── Response conversion: OpenAI → Anthropic ───────────────────────────────────

def _convert_openai_to_anthropic_response(response: dict[str, Any]) -> dict[str, Any]:
    choices = response.get("choices") or []
    choice = choices[0] if choices else {}
    message = choice.get("message") or {}
    finish_reason = choice.get("finish_reason") or "stop"

    content_blocks: list[dict[str, Any]] = []

    text_content = message.get("content")
    if text_content:
        content_blocks.append({"type": "text", "text": text_content})

    for tc in message.get("tool_calls") or []:
        fn = tc.get("function") or {}
        args_str = fn.get("arguments") or "{}"
        try:
            args = json.loads(args_str)
        except json.JSONDecodeError:
            args = {}
        content_blocks.append({
            "type": "tool_use",
            "id": tc.get("id", ""),
            "name": fn.get("name", ""),
            "input": args,
        })

    usage = response.get("usage") or {}
    stop_reason = _FINISH_REASON_MAP.get(finish_reason, "end_turn")

    return {
        "id": response.get("id", ""),
        "type": "message",
        "role": "assistant",
        "model": response.get("model", ""),
        "content": content_blocks,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
        },
    }


# ── Streaming SSE conversion: OpenAI → Anthropic ─────────────────────────────

async def _openai_sse_to_anthropic_sse(
    chunks: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    buf = ""
    message_started = False
    message_id = ""
    model_name = ""
    current_block_index = -1
    current_block_type: str | None = None
    # map from OpenAI tool_call index → Anthropic block index
    tool_block_map: dict[int, int] = {}
    input_tokens = 0
    output_tokens = 0

    async for raw_chunk in chunks:
        buf += raw_chunk.decode("utf-8", errors="replace")

        while "\n\n" in buf:
            raw_event, buf = buf.split("\n\n", 1)
            event_data: dict[str, Any] | None = None

            for line in raw_event.splitlines():
                if line.startswith("data:"):
                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        return
                    try:
                        event_data = json.loads(data_str)
                    except json.JSONDecodeError:
                        pass

            if not isinstance(event_data, dict):
                continue

            if not message_started:
                message_id = event_data.get("id", "")
                model_name = event_data.get("model", "")
                first_usage = event_data.get("usage") or {}
                input_tokens = first_usage.get("prompt_tokens", 0)
                yield _sse_event("message_start", {
                    "type": "message_start",
                    "message": {
                        "id": message_id,
                        "type": "message",
                        "role": "assistant",
                        "content": [],
                        "model": model_name,
                        "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": input_tokens, "output_tokens": 1},
                    },
                })
                message_started = True

            # top-level usage update (some providers send usage-only events)
            top_usage = event_data.get("usage")
            if top_usage and isinstance(top_usage, dict):
                input_tokens = top_usage.get("prompt_tokens", input_tokens)
                output_tokens = top_usage.get("completion_tokens", output_tokens)

            choices = event_data.get("choices") or []
            if not choices:
                continue

            choice = choices[0]
            delta = choice.get("delta") or {}
            finish_reason = choice.get("finish_reason")

            # text content delta
            text = delta.get("content")
            if isinstance(text, str) and text:
                if current_block_type != "text":
                    if current_block_index >= 0:
                        yield _sse_event("content_block_stop", {
                            "type": "content_block_stop",
                            "index": current_block_index,
                        })
                    current_block_index += 1
                    current_block_type = "text"
                    yield _sse_event("content_block_start", {
                        "type": "content_block_start",
                        "index": current_block_index,
                        "content_block": {"type": "text", "text": ""},
                    })
                yield _sse_event("content_block_delta", {
                    "type": "content_block_delta",
                    "index": current_block_index,
                    "delta": {"type": "text_delta", "text": text},
                })

            # tool call deltas
            for tc_delta in (delta.get("tool_calls") or []):
                tc_idx = tc_delta.get("index", 0)

                if tc_idx not in tool_block_map:
                    # close any open text block
                    if current_block_index >= 0 and current_block_type in ("text", "tool_use"):
                        yield _sse_event("content_block_stop", {
                            "type": "content_block_stop",
                            "index": current_block_index,
                        })
                    current_block_index += 1
                    current_block_type = "tool_use"
                    tool_block_map[tc_idx] = current_block_index

                    tc_id = tc_delta.get("id", "")
                    tc_name = (tc_delta.get("function") or {}).get("name", "")
                    yield _sse_event("content_block_start", {
                        "type": "content_block_start",
                        "index": current_block_index,
                        "content_block": {
                            "type": "tool_use",
                            "id": tc_id,
                            "name": tc_name,
                            "input": {},
                        },
                    })

                block_idx = tool_block_map[tc_idx]
                args_fragment = (tc_delta.get("function") or {}).get("arguments", "")
                if args_fragment:
                    yield _sse_event("content_block_delta", {
                        "type": "content_block_delta",
                        "index": block_idx,
                        "delta": {"type": "input_json_delta", "partial_json": args_fragment},
                    })

            if finish_reason:
                if current_block_index >= 0:
                    yield _sse_event("content_block_stop", {
                        "type": "content_block_stop",
                        "index": current_block_index,
                    })
                    current_block_index = -1
                    current_block_type = None

                stop_reason = _FINISH_REASON_MAP.get(finish_reason, "end_turn")
                yield _sse_event("message_delta", {
                    "type": "message_delta",
                    "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                    "usage": {"output_tokens": output_tokens},
                })
                yield _sse_event("message_stop", {"type": "message_stop"})
