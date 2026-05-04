import json
import pytest

from claudius.controller.proxy_adapters import (
    AnthropicAdapter,
    OpenAIAdapter,
    _convert_anthropic_to_openai_request,
    _convert_openai_to_anthropic_response,
    _openai_sse_to_anthropic_sse,
)


# ── Request conversion ────────────────────────────────────────────────────────

def test_system_prompt_prepended_as_system_message():
    payload = {
        "model": "m",
        "system": "You are helpful.",
        "messages": [{"role": "user", "content": "hi"}],
    }
    result = _convert_anthropic_to_openai_request(payload)
    assert result["messages"][0] == {"role": "system", "content": "You are helpful."}
    assert result["messages"][1] == {"role": "user", "content": "hi"}


def test_stop_sequences_renamed():
    payload = {"model": "m", "messages": [], "stop_sequences": ["END", "STOP"]}
    result = _convert_anthropic_to_openai_request(payload)
    assert result["stop"] == ["END", "STOP"]
    assert "stop_sequences" not in result


def test_tools_converted_to_openai_format():
    payload = {
        "model": "m",
        "messages": [],
        "tools": [
            {
                "name": "bash",
                "description": "Run a shell command",
                "input_schema": {"type": "object", "properties": {"cmd": {"type": "string"}}},
            }
        ],
    }
    result = _convert_anthropic_to_openai_request(payload)
    assert result["tools"] == [
        {
            "type": "function",
            "function": {
                "name": "bash",
                "description": "Run a shell command",
                "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}},
            },
        }
    ]


def test_tool_choice_auto():
    payload = {"model": "m", "messages": [], "tool_choice": {"type": "auto"}}
    assert _convert_anthropic_to_openai_request(payload)["tool_choice"] == "auto"


def test_tool_choice_any():
    payload = {"model": "m", "messages": [], "tool_choice": {"type": "any"}}
    assert _convert_anthropic_to_openai_request(payload)["tool_choice"] == "required"


def test_tool_choice_specific_tool():
    payload = {"model": "m", "messages": [], "tool_choice": {"type": "tool", "name": "bash"}}
    assert _convert_anthropic_to_openai_request(payload)["tool_choice"] == {
        "type": "function",
        "function": {"name": "bash"},
    }


def test_assistant_message_with_tool_use():
    payload = {
        "model": "m",
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll run that."},
                    {
                        "type": "tool_use",
                        "id": "call_1",
                        "name": "bash",
                        "input": {"cmd": "ls"},
                    },
                ],
            }
        ],
    }
    result = _convert_anthropic_to_openai_request(payload)
    msg = result["messages"][0]
    assert msg["role"] == "assistant"
    assert msg["content"] == "I'll run that."
    assert msg["tool_calls"] == [
        {
            "id": "call_1",
            "type": "function",
            "function": {"name": "bash", "arguments": '{"cmd": "ls"}'},
        }
    ]


def test_user_message_with_tool_result():
    payload = {
        "model": "m",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "call_1", "content": "file.txt"}
                ],
            }
        ],
    }
    result = _convert_anthropic_to_openai_request(payload)
    msg = result["messages"][0]
    assert msg["role"] == "tool"
    assert msg["tool_call_id"] == "call_1"
    assert msg["content"] == "file.txt"


def test_mixed_user_message_with_text_and_tool_result_splits():
    payload = {
        "model": "m",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "call_1", "content": "result"},
                    {"type": "text", "text": "What do you think?"},
                ],
            }
        ],
    }
    result = _convert_anthropic_to_openai_request(payload)
    messages = result["messages"]
    assert len(messages) == 2
    assert messages[0]["role"] == "tool"
    assert messages[0]["tool_call_id"] == "call_1"
    assert messages[1]["role"] == "user"
    assert messages[1]["content"] == "What do you think?"


def test_image_block_converted_to_image_url():
    payload = {
        "model": "m",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": "abc123",
                        },
                    }
                ],
            }
        ],
    }
    result = _convert_anthropic_to_openai_request(payload)
    content = result["messages"][0]["content"]
    assert content[0] == {
        "type": "image_url",
        "image_url": {"url": "data:image/png;base64,abc123"},
    }


def test_document_block_converted_to_file():
    payload = {
        "model": "m",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": "cGRmZGF0YQ==",
                        },
                    }
                ],
            }
        ],
    }
    result = _convert_anthropic_to_openai_request(payload)
    content = result["messages"][0]["content"]
    assert content[0] == {
        "type": "file",
        "file": {
            "filename": "document.pdf",
            "file_data": "data:application/pdf;base64,cGRmZGF0YQ==",
        },
    }


def test_stream_options_added_when_streaming():
    payload = {"model": "m", "messages": [], "stream": True}
    result = _convert_anthropic_to_openai_request(payload)
    assert result["stream"] is True
    assert result["stream_options"] == {"include_usage": True}


def test_stream_options_not_added_when_not_streaming():
    payload = {"model": "m", "messages": []}
    result = _convert_anthropic_to_openai_request(payload)
    assert "stream_options" not in result


# ── Response conversion (non-streaming) ──────────────────────────────────────

def test_text_response_converted():
    openai_resp = {
        "id": "chatcmpl-1",
        "model": "gpt-4",
        "choices": [{"message": {"role": "assistant", "content": "Hello!"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }
    result = _convert_openai_to_anthropic_response(openai_resp)
    assert result["id"] == "chatcmpl-1"
    assert result["type"] == "message"
    assert result["role"] == "assistant"
    assert result["content"] == [{"type": "text", "text": "Hello!"}]
    assert result["stop_reason"] == "end_turn"
    assert result["usage"] == {"input_tokens": 10, "output_tokens": 5}


def test_tool_call_response_converted():
    openai_resp = {
        "id": "chatcmpl-2",
        "model": "gpt-4",
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "bash", "arguments": '{"cmd": "ls"}'},
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 20, "completion_tokens": 10},
    }
    result = _convert_openai_to_anthropic_response(openai_resp)
    assert result["stop_reason"] == "tool_use"
    assert result["content"] == [
        {"type": "tool_use", "id": "call_1", "name": "bash", "input": {"cmd": "ls"}}
    ]


def test_finish_reason_length_mapped():
    openai_resp = {
        "id": "x",
        "model": "m",
        "choices": [{"message": {"role": "assistant", "content": "hi"}, "finish_reason": "length"}],
        "usage": {},
    }
    assert _convert_openai_to_anthropic_response(openai_resp)["stop_reason"] == "max_tokens"


# ── Streaming SSE conversion ──────────────────────────────────────────────────

async def _collect_sse(chunks):
    """Helper: collect adapted SSE bytes and parse events."""
    parts = []
    async for chunk in _openai_sse_to_anthropic_sse(_async_iter(chunks)):
        parts.append(chunk.decode("utf-8"))
    raw = "".join(parts)
    events = []
    for block in raw.strip().split("\n\n"):
        if not block.strip():
            continue
        event_type = None
        data = None
        for line in block.splitlines():
            if line.startswith("event:"):
                event_type = line[6:].strip()
            elif line.startswith("data:"):
                data = json.loads(line[5:].strip())
        if event_type and data is not None:
            events.append((event_type, data))
    return events


async def _async_iter(items):
    for item in items:
        if isinstance(item, str):
            yield item.encode("utf-8")
        else:
            yield item


@pytest.mark.asyncio
async def test_text_streaming_emits_anthropic_events():
    chunks = [
        'data: {"id":"c1","model":"m","choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}\n\n',
        'data: {"id":"c1","choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}\n\n',
        'data: {"id":"c1","choices":[{"delta":{"content":" world"},"finish_reason":null}]}\n\n',
        'data: {"id":"c1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3}}\n\n',
        "data: [DONE]\n\n",
    ]
    events = await _collect_sse(chunks)
    types = [e for e, _ in events]
    assert types[0] == "message_start"
    assert "content_block_start" in types
    assert "content_block_delta" in types
    assert "content_block_stop" in types
    assert "message_delta" in types
    assert "message_stop" in types

    deltas = [d for e, d in events if e == "content_block_delta"]
    texts = [d["delta"]["text"] for d in deltas]
    assert "Hello" in texts
    assert " world" in texts

    msg_delta = next(d for e, d in events if e == "message_delta")
    assert msg_delta["delta"]["stop_reason"] == "end_turn"
    assert msg_delta["usage"]["output_tokens"] == 3


@pytest.mark.asyncio
async def test_tool_call_streaming_emits_tool_use_events():
    chunks = [
        'data: {"id":"c2","model":"m","choices":[{"delta":{"role":"assistant","content":null,"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"bash","arguments":""}}]},"finish_reason":null}]}\n\n',
        'data: {"id":"c2","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"cmd\\":"}}]},"finish_reason":null}]}\n\n',
        'data: {"id":"c2","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\\"ls\\"}"}}]},"finish_reason":null}]}\n\n',
        'data: {"id":"c2","choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":8}}\n\n',
        "data: [DONE]\n\n",
    ]
    events = await _collect_sse(chunks)
    types = [e for e, _ in events]

    assert "message_start" in types
    assert "content_block_start" in types
    assert "content_block_delta" in types
    assert "content_block_stop" in types

    block_start = next(d for e, d in events if e == "content_block_start")
    assert block_start["content_block"]["type"] == "tool_use"
    assert block_start["content_block"]["name"] == "bash"
    assert block_start["content_block"]["id"] == "call_1"

    deltas = [d for e, d in events if e == "content_block_delta"]
    assert all(d["delta"]["type"] == "input_json_delta" for d in deltas)

    msg_delta = next(d for e, d in events if e == "message_delta")
    assert msg_delta["delta"]["stop_reason"] == "tool_use"


@pytest.mark.asyncio
async def test_done_sentinel_ends_stream():
    chunks = ["data: [DONE]\n\n"]
    events = await _collect_sse(chunks)
    assert events == []


# ── AnthropicAdapter passthrough ─────────────────────────────────────────────

def test_anthropic_adapter_request_passthrough():
    adapter = AnthropicAdapter()
    path, headers, body = adapter.adapt_request("v1/messages", {"content-type": "application/json"}, b"hello")
    assert path == "v1/messages"
    assert headers == {"content-type": "application/json"}
    assert body == b"hello"


@pytest.mark.asyncio
async def test_anthropic_adapter_response_stream_passthrough():
    adapter = AnthropicAdapter()
    chunks = [b"chunk1", b"chunk2", b"chunk3"]
    result = []
    async for chunk in adapter.adapt_response_stream("application/json", _async_iter(chunks)):
        result.append(chunk)
    assert result == chunks
