import json
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import httpx
import jwt
from fastapi import FastAPI

from claudius.controller.proxy import (
    _extract_reported_cost_usd,
    _extract_request_model,
    _extract_response_id,
    _extract_usage_counts,
    _format_body_for_log,
    build_proxy_upstream,
    create_proxy_routes,
    mint_token,
    verify_token,
)


SECRET = "testsecret" * 4  # 40 chars


def test_mint_and_verify_roundtrip():
    token = mint_token("sess-123", SECRET)
    assert isinstance(token, str)
    assert token.startswith("ey")
    session_id = verify_token(token, SECRET)
    assert session_id == "sess-123"


def test_verify_wrong_secret():
    token = mint_token("sess-123", SECRET)
    assert verify_token(token, "wrongsecret") is None


def test_verify_expired_token():
    payload = {
        "sub": "sess-123",
        "exp": datetime.now(timezone.utc) - timedelta(seconds=1),
    }
    token = jwt.encode(payload, SECRET, algorithm="HS256")
    assert verify_token(token, SECRET) is None


def test_verify_malformed_token():
    assert verify_token("not.a.token", SECRET) is None
    assert verify_token("", SECRET) is None


# ── Proxy route tests ──────────────────────────────────────────────────────────

def _make_app(is_active: bool = True) -> tuple[FastAPI, AsyncMock]:
    app = FastAPI()
    db = AsyncMock()
    db.is_execution_active = AsyncMock(return_value=is_active)
    create_proxy_routes(
        app,
        db,
        build_proxy_upstream("anthropic", "https://api.anthropic.com", "real-key", "x-api-key"),
        secret=SECRET,
    )
    return app, db


def _make_logged_app() -> tuple[FastAPI, AsyncMock]:
    app = FastAPI()
    db = AsyncMock()
    db.is_execution_active = AsyncMock(return_value=True)
    db.get_active_execution = AsyncMock(return_value=type("ExecutionRef", (), {"execution_id": "exec-1"})())
    db.get_execution = AsyncMock(return_value=type(
        "ExecutionPayload",
        (),
        {
            "execution_id": "exec-1",
            "session_id": "sess-xyz",
            "worker_address": None,
            "started_at": datetime.now(timezone.utc),
            "halted_at": None,
            "halt_reason": None,
            "phase": type("Phase", (), {"value": "starting"})(),
            "runtime_container": None,
            "worker_container": None,
            "exit_code": None,
            "claude_session_id": None,
            "claude_num_turns": None,
            "claude_duration_ms": None,
            "claude_total_cost_usd": 0.0125,
            "claude_input_tokens": 101,
            "claude_output_tokens": 22,
            "claude_cache_creation_input_tokens": 7,
            "claude_cache_read_input_tokens": 3,
            "claude_total_tokens": 133,
            "agent_error_category": None,
            "agent_error_reason": None,
        },
    )())
    create_proxy_routes(
        app,
        db,
        build_proxy_upstream("anthropic", "https://api.anthropic.com", "real-key", "x-api-key"),
        secret=SECRET,
        log_conversation=True,
    )
    return app, db


@pytest.mark.asyncio
async def test_proxy_rejects_invalid_token():
    app, _ = _make_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.post("/proxy/v1/messages", headers={"x-api-key": "bad-token"}, json={})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_proxy_rejects_no_active_execution():
    app, _ = _make_app(is_active=False)
    token = mint_token("sess-xyz", SECRET)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.post("/proxy/v1/messages", headers={"x-api-key": token}, json={})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_proxy_records_provider_reported_cost_and_usage(monkeypatch):
    app, db = _make_logged_app()
    token = mint_token("sess-xyz", SECRET)
    real_async_client = httpx.AsyncClient
    response_payload = {
        "id": "msg_123",
        "usage": {
            "input_tokens": 101,
            "output_tokens": 22,
            "cache_creation_input_tokens": 7,
            "cache_read_input_tokens": 3,
        },
        "total_cost_usd": 0.0125,
    }

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        def build_request(self, *, method, url, headers, content, params):
            return httpx.Request(method=method, url=url, headers=headers, content=content, params=params)

        async def send(self, request, stream=True):
            return httpx.Response(200, json=response_payload, request=request)

        async def aclose(self):
            return None

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)

    transport = httpx.ASGITransport(app=app)
    async with real_async_client(transport=transport, base_url="http://testserver") as client:
        resp = await client.post("/proxy/v1/messages", headers={"x-api-key": token}, json={"model": "x"})

    assert resp.status_code == 200
    assert resp.json() == response_payload
    db.increment_execution_token_usage.assert_awaited_once_with(
        "exec-1",
        input_tokens=101,
        output_tokens=22,
        cache_creation_input_tokens=7,
        cache_read_input_tokens=3,
        total_cost_usd=0.0125,
    )
    response_in_call = next(
        call for call in db.append_proxy_log.await_args_list if call.kwargs.get("stage") == "response_in"
    )
    assert response_in_call.kwargs["input_tokens"] == 101
    assert response_in_call.kwargs["output_tokens"] == 22
    assert response_in_call.kwargs["cache_creation_input_tokens"] == 7
    assert response_in_call.kwargs["cache_read_input_tokens"] == 3
    assert response_in_call.kwargs["total_tokens"] == 133
    assert response_in_call.kwargs["total_cost_usd"] == 0.0125


def test_openai_upstream_strips_anthropic_headers():
    upstream = build_proxy_upstream("openai", "http://localhost:4000", "", "x-api-key")
    headers = {
        "content-type": "application/json",
        "Anthropic-Beta": "pdfs-2024-09-25",
        "anthropic-version": "2023-06-01",
    }
    body = json.dumps({"model": "m", "messages": []}).encode()
    _, adapted_headers, _ = upstream.adapter.adapt_request("v1/messages", headers, body)
    assert all(not k.lower().startswith("anthropic-") for k in adapted_headers)


def test_openai_upstream_rewrites_path():
    upstream = build_proxy_upstream("openai", "https://api.cerebras.ai/v1", "", "bearer")
    adapted_path, _, _ = upstream.adapter.adapt_request("v1/messages", {}, b"")
    assert adapted_path == "chat/completions"
    assert upstream.url_for(adapted_path) == "https://api.cerebras.ai/v1/chat/completions"


def test_openai_upstream_rewrites_pdf_document_blocks():
    payload = {
        "model": "m",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "summarize this"},
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": "cGRmZGF0YQ==",
                        },
                    },
                ],
            }
        ],
    }
    headers = {"content-type": "application/json", "anthropic-beta": "pdfs-2024-09-25"}
    upstream = build_proxy_upstream("openai", "http://localhost:4000", "", "x-api-key")
    _, _, adapted_body = upstream.adapter.adapt_request("v1/messages", headers, json.dumps(payload).encode())
    forwarded = json.loads(adapted_body)
    content = forwarded["messages"][0]["content"]
    assert content[0] == {"type": "text", "text": "summarize this"}
    assert content[1] == {
        "type": "file",
        "file": {
            "filename": "document.pdf",
            "file_data": "data:application/pdf;base64,cGRmZGF0YQ==",
        },
    }


def test_anthropic_upstream_preserves_headers_and_body():
    payload = {
        "model": "claude-sonnet-4-5",
        "messages": [{"role": "user", "content": "hi"}],
    }
    headers = {
        "content-type": "application/json",
        "anthropic-beta": "pdfs-2024-09-25",
    }
    upstream = build_proxy_upstream("anthropic", "https://openrouter.ai/api", "openrouter-key", "bearer")
    _, adapted_headers, adapted_body = upstream.adapter.adapt_request(
        "v1/messages", headers, json.dumps(payload).encode()
    )
    authed = upstream.apply_auth(adapted_headers)
    assert authed["anthropic-beta"] == "pdfs-2024-09-25"
    assert authed["authorization"] == "Bearer openrouter-key"
    assert json.loads(adapted_body) == payload


def test_openai_upstream_sets_bearer_auth():
    upstream = build_proxy_upstream("openai", "https://api.cerebras.ai/v1", "cerebras-key", "bearer")
    _, adapted_headers, _ = upstream.adapter.adapt_request("v1/messages", {"content-type": "application/json"}, b"")
    authed = upstream.apply_auth(adapted_headers)
    assert authed["authorization"] == "Bearer cerebras-key"
    assert authed["accept-encoding"] == "identity"


def test_anthropic_upstream_forces_identity_encoding():
    upstream = build_proxy_upstream("anthropic", "https://api.anthropic.com", "real-key", "x-api-key")
    authed = upstream.apply_auth({"accept-encoding": "gzip, br"})
    assert authed["accept-encoding"] == "identity"


def test_format_body_for_log_pretty_prints_json():
    body = b'{"z":1,"a":2}'
    formatted = _format_body_for_log(body, "application/json")
    assert '"a": 2' in formatted
    assert '"z": 1' in formatted


def test_format_body_for_log_omits_binary_payloads():
    body = b"\x89PNG\r\n"
    formatted = _format_body_for_log(body, "image/png")
    assert "non-text body omitted" in formatted


def test_extract_usage_counts_reads_anthropic_usage_payload():
    body = json.dumps({
        "id": "msg_123",
        "usage": {
            "input_tokens": 101,
            "output_tokens": 22,
            "cache_creation_input_tokens": 7,
            "cache_read_input_tokens": 3,
        },
    }).encode()

    usage = _extract_usage_counts(body, "application/json")
    assert usage == {
        "input_tokens": 101,
        "output_tokens": 22,
        "cache_creation_input_tokens": 7,
        "cache_read_input_tokens": 3,
    }


def test_extract_request_model_reads_json_model():
    body = json.dumps({"model": "qwen-3-235b-a22b-instruct-2507"}).encode()
    assert _extract_request_model(body, "application/json") == "qwen-3-235b-a22b-instruct-2507"


def test_extract_response_id_from_json():
    body = json.dumps({"id": "gen-abc123", "usage": {"input_tokens": 10}}).encode()
    assert _extract_response_id(body) == "gen-abc123"


def test_extract_response_id_from_sse_message_start():
    sse = (
        'data: {"type":"message_start","message":{"id":"gen-sse99","role":"assistant"}}\n\n'
        'data: {"type":"content_block_start"}\n\n'
    ).encode()
    assert _extract_response_id(sse) == "gen-sse99"


def test_extract_response_id_from_sse_fallback_top_level():
    sse = (
        'data: {"id":"gen-top42","object":"chat.completion.chunk"}\n\n'
    ).encode()
    assert _extract_response_id(sse) == "gen-top42"


def test_extract_response_id_returns_none_for_empty():
    assert _extract_response_id(b"") is None
    assert _extract_response_id(b"{}") is None


@pytest.mark.asyncio
async def test_openrouter_fetch_cost_usd_calls_generation_endpoint(monkeypatch):
    upstream = build_proxy_upstream("anthropic", "https://openrouter.ai/api", "or-key", "bearer")

    generation_response = {"data": {"id": "gen-xyz", "total_cost": 0.00456}}

    class _FakeGetClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def get(self, url, *, params, headers):
            assert params == {"id": "gen-xyz"}
            assert headers["authorization"] == "Bearer or-key"
            return httpx.Response(200, json=generation_response, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: _FakeGetClient())
    monkeypatch.setattr("claudius.controller.proxy._OPENROUTER_GENERATION_RETRY_DELAY", 0)

    cost = await upstream.fetch_cost_usd("gen-xyz")
    assert cost == pytest.approx(0.00456)


@pytest.mark.asyncio
async def test_openrouter_fetch_cost_usd_retries_on_missing_cost(monkeypatch):
    from claudius.controller.proxy import _OPENROUTER_GENERATION_RETRIES
    upstream = build_proxy_upstream("anthropic", "https://openrouter.ai/api", "or-key", "bearer")

    call_count = 0

    class _FakeGetClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def get(self, url, *, params, headers):
            nonlocal call_count
            call_count += 1
            if call_count < _OPENROUTER_GENERATION_RETRIES:
                return httpx.Response(200, json={"data": {}}, request=httpx.Request("GET", url))
            return httpx.Response(200, json={"data": {"total_cost": 0.001}}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: _FakeGetClient())
    monkeypatch.setattr("claudius.controller.proxy._OPENROUTER_GENERATION_RETRY_DELAY", 0)

    cost = await upstream.fetch_cost_usd("gen-retry")
    assert cost == pytest.approx(0.001)
    assert call_count == _OPENROUTER_GENERATION_RETRIES


@pytest.mark.asyncio
async def test_openrouter_fetch_cost_usd_returns_none_when_no_id():
    upstream = build_proxy_upstream("anthropic", "https://openrouter.ai/api", "or-key", "bearer")
    cost = await upstream.fetch_cost_usd(None)
    assert cost is None


def test_extract_reported_cost_usd_reads_top_level_field():
    body = json.dumps({
        "id": "msg_123",
        "total_cost_usd": 0.0125,
    }).encode()

    assert _extract_reported_cost_usd(body, "application/json") == 0.0125


def test_extract_reported_cost_usd_reads_nested_cost_usd_field():
    body = json.dumps({
        "id": "msg_123",
        "cost": {"usd": 0.03125},
    }).encode()

    assert _extract_reported_cost_usd(body, "application/json") == 0.03125


@pytest.mark.asyncio
async def test_proxy_computes_local_cost_from_model_pricing(monkeypatch):
    app, db = _make_logged_app()
    token = mint_token("sess-xyz", SECRET)
    real_async_client = httpx.AsyncClient
    response_payload = {
        "id": "chatcmpl_123",
        "usage": {
            "prompt_tokens": 1200,
            "completion_tokens": 300,
            "total_tokens": 1500,
        },
    }

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        def build_request(self, *, method, url, headers, content, params):
            return httpx.Request(method=method, url=url, headers=headers, content=content, params=params)

        async def send(self, request, stream=True):
            return httpx.Response(200, json=response_payload, request=request)

        async def aclose(self):
            return None

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)

    app = FastAPI()
    db = AsyncMock()
    db.is_execution_active = AsyncMock(return_value=True)
    db.get_active_execution = AsyncMock(return_value=type("ExecutionRef", (), {"execution_id": "exec-1"})())
    db.get_execution = AsyncMock(return_value=type(
        "ExecutionPayload",
        (),
        {
            "execution_id": "exec-1",
            "session_id": "sess-xyz",
            "worker_address": None,
            "started_at": datetime.now(timezone.utc),
            "halted_at": None,
            "halt_reason": None,
            "phase": type("Phase", (), {"value": "running"})(),
            "runtime_container": None,
            "worker_container": None,
            "exit_code": None,
            "claude_session_id": None,
            "claude_num_turns": None,
            "claude_duration_ms": None,
            "claude_total_cost_usd": None,
            "claude_input_tokens": 0,
            "claude_output_tokens": 0,
            "claude_cache_creation_input_tokens": 0,
            "claude_cache_read_input_tokens": 0,
            "claude_total_tokens": 0,
            "agent_error_category": None,
            "agent_error_reason": None,
        },
    )())
    create_proxy_routes(
        app,
        db,
        build_proxy_upstream(
            "openai",
            "https://api.cerebras.ai/v1",
            "cerebras-key",
            "bearer",
            model_pricing={
                "qwen-3-235b-a22b-instruct-2507": {
                    "input_cost_per_million_tokens_usd": 0.6,
                    "output_cost_per_million_tokens_usd": 1.2,
                }
            },
        ),
        secret=SECRET,
        log_conversation=True,
    )

    transport = httpx.ASGITransport(app=app)
    async with real_async_client(transport=transport, base_url="http://testserver") as client:
        resp = await client.post(
            "/proxy/v1/messages",
            headers={"x-api-key": token},
            json={"model": "qwen-3-235b-a22b-instruct-2507", "messages": []},
        )

    assert resp.status_code == 200
    db.increment_execution_token_usage.assert_awaited_once_with(
        "exec-1",
        input_tokens=1200,
        output_tokens=300,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        total_cost_usd=pytest.approx(0.00108),
    )


@pytest.mark.asyncio
async def test_proxy_rejects_unknown_model_when_model_pricing_is_configured():
    app = FastAPI()
    db = AsyncMock()
    db.is_execution_active = AsyncMock(return_value=True)
    create_proxy_routes(
        app,
        db,
        build_proxy_upstream(
            "openai",
            "https://api.cerebras.ai/v1",
            "cerebras-key",
            "bearer",
            model_pricing={
                "known-model": {
                    "input_cost_per_million_tokens_usd": 0.6,
                    "output_cost_per_million_tokens_usd": 1.2,
                }
            },
        ),
        secret=SECRET,
    )
    token = mint_token("sess-xyz", SECRET)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.post(
            "/proxy/v1/messages",
            headers={"x-api-key": token},
            json={"model": "unknown-model", "messages": []},
        )

    assert resp.status_code == 400
    assert "unknown upstream model 'unknown-model'" in resp.json()["detail"]
