"""Tests for the Bedrock upstream's request rewriting and SSE re-framing.

Network calls are not exercised here — those need real AWS credentials and
live in integration tests downstream. We assert the protocol-level translation
that's stateless and worth catching at unit-test speed.
"""

from __future__ import annotations

import json

import pytest

from claudius.config.schema import UpstreamLLMConfig
from claudius.controller.bedrock import (
    _bedrock_stream_to_anthropic_sse,
    adapt_anthropic_to_bedrock_body,
    bedrock_invoke_path,
)


# ---- request rewriting ---------------------------------------------------


def test_adapt_request_strips_model_and_stream_and_sets_anthropic_version():
    body = json.dumps({
        "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 64,
        "stream": True,
    }).encode()
    adapted, model = adapt_anthropic_to_bedrock_body(body)
    payload = json.loads(adapted)
    assert "model" not in payload
    assert "stream" not in payload
    assert payload["anthropic_version"] == "bedrock-2023-05-31"
    assert payload["messages"] == [{"role": "user", "content": "hi"}]
    assert model == "anthropic.claude-3-5-sonnet-20241022-v2:0"


def test_adapt_request_preserves_existing_anthropic_version():
    body = json.dumps({
        "model": "m",
        "anthropic_version": "custom",
        "messages": [],
    }).encode()
    adapted, _ = adapt_anthropic_to_bedrock_body(body)
    assert json.loads(adapted)["anthropic_version"] == "custom"


def test_adapt_request_handles_empty_body():
    assert adapt_anthropic_to_bedrock_body(b"") == (b"", None)


def test_invoke_path_url_encodes_model_id_and_picks_streaming_suffix():
    # AWS model IDs contain colons and slashes that must be URL-encoded.
    p = bedrock_invoke_path("anthropic.claude-3-5-sonnet:0", streaming=True)
    assert p == "/model/anthropic.claude-3-5-sonnet%3A0/invoke-with-response-stream"
    p = bedrock_invoke_path("foo/bar", streaming=False)
    assert p == "/model/foo%2Fbar/invoke"


# ---- response stream re-framing ------------------------------------------


async def _collect(aiter):
    out = []
    async for c in aiter:
        out.append(c)
    return b"".join(out)


@pytest.mark.asyncio
async def test_bedrock_chunk_is_reframed_as_anthropic_sse():
    payload = json.dumps({"type": "message_start", "message": {"id": "m1"}}).encode()

    async def gen():
        yield {"chunk": {"bytes": payload}}

    out = await _collect(_bedrock_stream_to_anthropic_sse(gen()))
    # event: <type>\ndata: <json>\n\n
    assert out == b"event: message_start\ndata: " + payload + b"\n\n"


@pytest.mark.asyncio
async def test_bedrock_error_event_emitted_as_sse_error():
    async def gen():
        yield {"modelStreamErrorException": {"message": "boom"}}

    out = await _collect(_bedrock_stream_to_anthropic_sse(gen()))
    assert out.startswith(b"event: error\ndata: ")
    body = json.loads(out.split(b"data: ", 1)[1].rstrip(b"\n"))
    assert body == {"error": {"modelStreamErrorException": {"message": "boom"}}}


# ---- schema validation ---------------------------------------------------


def test_schema_protocol_bedrock_defaults_to_sigv4_and_clears_unused_fields():
    cfg = UpstreamLLMConfig.model_validate({"protocol": "bedrock"})
    assert cfg.auth_mode == "sigv4"
    assert cfg.bedrock.region == ""


def test_schema_bedrock_rejects_base_url_and_api_key():
    with pytest.raises(ValueError, match="base_url is not used"):
        UpstreamLLMConfig.model_validate({"protocol": "bedrock", "base_url": "x"})
    with pytest.raises(ValueError, match="api_key_env is not used"):
        UpstreamLLMConfig.model_validate({"protocol": "bedrock", "api_key_env": "FOO"})


def test_schema_sigv4_only_with_bedrock():
    with pytest.raises(ValueError, match="sigv4"):
        UpstreamLLMConfig.model_validate(
            {"protocol": "anthropic", "auth_mode": "sigv4"}
        )


def test_schema_bedrock_rejects_explicit_non_sigv4_auth():
    with pytest.raises(ValueError, match="must be 'sigv4'"):
        UpstreamLLMConfig.model_validate(
            {"protocol": "bedrock", "auth_mode": "x-api-key"}
        )


def test_schema_bedrock_region_passes_through():
    cfg = UpstreamLLMConfig.model_validate(
        {"protocol": "bedrock", "bedrock": {"region": "us-east-1"}}
    )
    assert cfg.bedrock.region == "us-east-1"
