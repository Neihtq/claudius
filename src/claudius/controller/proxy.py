import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import jwt

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from loguru import logger

from claudius.controller.proxy_adapters import AnthropicAdapter, MessageFormatAdapter, OpenAIAdapter
from claudius.controller.bedrock import (
    BedrockUpstreamClient,
    adapt_anthropic_to_bedrock_body,
    bedrock_invoke_path,
)

_ALG = "HS256"
_TTL_HOURS = 24
_JSON_CT = "application/json"
_OPENROUTER_GENERATION_RETRIES = 3
_OPENROUTER_GENERATION_RETRY_DELAY = 1.0
_TEXTUAL_CONTENT_TYPES = ("application/json", "text/")


class UnknownUpstreamModelError(ValueError):
    pass


class ProxyUpstream:
    def __init__(
        self,
        kind: str,
        base_url: str,
        api_key: str,
        auth_mode: str,
        adapter: MessageFormatAdapter,
        model_pricing: dict[str, dict[str, float]] | None = None,
        bedrock_client: "BedrockUpstreamClient | None" = None,
    ):
        self.kind = kind
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.auth_mode = auth_mode
        self.adapter = adapter
        self.model_pricing = dict(model_pricing or {})
        # Set when kind == "bedrock". The proxy hot path uses this instead of
        # the generic httpx client so we don't have to teach the forwarder
        # SigV4 + AWS event-stream framing.
        self.bedrock_client = bedrock_client

    @property
    def is_bedrock(self) -> bool:
        return self.kind == "bedrock"

    def url_for(self, path: str) -> str:
        return f"{self.base_url}/{path}"

    def apply_auth(self, headers: dict[str, str]) -> dict[str, str]:
        return _apply_api_key_auth(dict(headers), self.api_key, self.auth_mode)

    async def fetch_cost_usd(self, response_id: str | None) -> float | None:
        if not _is_openrouter_base_url(self.base_url) or not response_id:
            return None
        generation_url = f"{self.base_url}/v1/generation"
        headers = _apply_api_key_auth({}, self.api_key, self.auth_mode)
        for attempt in range(_OPENROUTER_GENERATION_RETRIES):
            if attempt > 0:
                await asyncio.sleep(_OPENROUTER_GENERATION_RETRY_DELAY)
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(generation_url, params={"id": response_id}, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    cost = data.get("data", {}).get("total_cost")
                    if isinstance(cost, (int, float)):
                        return float(cost)
            except Exception as exc:
                logger.debug(f"openrouter cost fetch attempt={attempt} id={response_id!r} error={exc!r}")
        return None

    def ensure_known_model(self, model_name: str | None) -> str | None:
        if not self.model_pricing:
            return model_name
        normalized_model = (model_name or "").strip()
        if not normalized_model:
            raise UnknownUpstreamModelError("request model is required when upstream_llm.model_pricing is set")
        if normalized_model not in self.model_pricing:
            raise UnknownUpstreamModelError(
                f"unknown upstream model {normalized_model!r}; configure it under upstream_llm.model_pricing"
            )
        return normalized_model

    def compute_cost_usd(self, model_name: str | None, usage_counts: dict[str, int] | None) -> float | None:
        if not usage_counts:
            return None
        normalized_model = self.ensure_known_model(model_name)
        if normalized_model is None:
            return None
        pricing = self.model_pricing.get(normalized_model)
        if pricing is None:
            return None
        input_tokens = usage_counts["input_tokens"] + usage_counts["cache_creation_input_tokens"]
        output_tokens = usage_counts["output_tokens"]
        return (
            (input_tokens / 1_000_000) * pricing["input_cost_per_million_tokens_usd"]
            + (output_tokens / 1_000_000) * pricing["output_cost_per_million_tokens_usd"]
        )


def build_proxy_upstream(
    kind: str,
    base_url: str,
    api_key: str,
    auth_mode: str = "none",
    model_pricing: dict[str, dict[str, float]] | None = None,
    bedrock_region: str = "",
) -> ProxyUpstream:
    bedrock_client: BedrockUpstreamClient | None = None
    if kind == "bedrock":
        # Bedrock streams emerge from boto3 already shaped as Anthropic SSE,
        # so the upstream adapter is the identity AnthropicAdapter — no body
        # rewriting on the response side.
        adapter: MessageFormatAdapter = AnthropicAdapter()
        bedrock_client = BedrockUpstreamClient(region=bedrock_region)
    elif kind == "anthropic":
        adapter = AnthropicAdapter()
    else:
        adapter = OpenAIAdapter()
    return ProxyUpstream(
        kind=kind,
        base_url=base_url,
        api_key=api_key,
        auth_mode=auth_mode,
        adapter=adapter,
        model_pricing=model_pricing,
        bedrock_client=bedrock_client,
    )


def _apply_api_key_auth(headers: dict[str, str], api_key: str, auth_mode: str) -> dict[str, str]:
    rewritten_headers = dict(headers)
    # We strip content-encoding on proxied responses, so ask upstreams for identity
    # encoding to keep error and streaming payloads readable end-to-end.
    rewritten_headers["accept-encoding"] = "identity"
    if auth_mode == "x-api-key":
        if api_key:
            rewritten_headers["x-api-key"] = api_key
        return rewritten_headers
    if auth_mode == "bearer":
        rewritten_headers["authorization"] = f"Bearer {api_key or 'placeholder'}"
        return rewritten_headers
    if auth_mode == "none":
        return rewritten_headers
    raise ValueError(f"unsupported auth_mode: {auth_mode}")


def _is_openrouter_base_url(base_url: str) -> bool:
    return "openrouter.ai" in base_url.lower()


def mint_token(session_id: str, secret: str) -> str:
    payload = {
        "sub": session_id,
        "exp": datetime.now(timezone.utc) + timedelta(hours=_TTL_HOURS),
    }
    return jwt.encode(payload, secret, algorithm=_ALG)


def verify_token(token: str, secret: str) -> str | None:
    """Return session_id if token is valid and unexpired, else None."""
    try:
        data = jwt.decode(token, secret, algorithms=[_ALG])
        return data["sub"]
    except Exception:
        return None



def _format_body_for_log(body: bytes, content_type: str | None) -> str:
    content_type = (content_type or "").lower()
    if not body:
        return ""
    if _JSON_CT in content_type:
        try:
            return json.dumps(json.loads(body), indent=2, sort_keys=True)
        except Exception:
            pass
    if any(content_type.startswith(prefix) for prefix in _TEXTUAL_CONTENT_TYPES) or not content_type:
        return body.decode("utf-8", errors="replace")
    return f"[non-text body omitted: {content_type or 'unknown content-type'}; {len(body)} bytes]"


def _extract_request_model(body: bytes, content_type: str | None) -> str | None:
    content_type = (content_type or "").lower()
    if _JSON_CT not in content_type:
        return None
    try:
        payload = json.loads(body)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    model_name = payload.get("model")
    return model_name if isinstance(model_name, str) and model_name.strip() else None


def _extract_request_streaming(body: bytes, content_type: str | None) -> bool:
    """Anthropic Messages convention: clients set ``stream: true`` for SSE."""
    content_type = (content_type or "").lower()
    if _JSON_CT not in content_type or not body:
        return False
    try:
        payload = json.loads(body)
    except Exception:
        return False
    return isinstance(payload, dict) and bool(payload.get("stream"))


def _extract_usage_counts(body: bytes, content_type: str | None) -> dict[str, int] | None:
    content_type = (content_type or "").lower()

    def _int(value: object) -> int:
        return int(value) if isinstance(value, (int, float)) else 0

    def _nonzero(counts: dict[str, int]) -> dict[str, int] | None:
        if all(v == 0 for v in counts.values()):
            return None
        return counts

    if _JSON_CT in content_type:
        try:
            payload = json.loads(body)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        usage = payload.get("usage")
        if not isinstance(usage, dict):
            return None
        return _nonzero({
            "input_tokens": _int(usage.get("input_tokens")),
            "output_tokens": _int(usage.get("output_tokens")),
            "cache_creation_input_tokens": _int(usage.get("cache_creation_input_tokens")),
            "cache_read_input_tokens": _int(usage.get("cache_read_input_tokens")),
        })

    if "text/event-stream" in content_type:
        counts: dict[str, int] = {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
        }
        try:
            text = body.decode("utf-8", errors="replace")
            for line in text.splitlines():
                if not line.startswith("data:"):
                    continue
                data_str = line[5:].strip()
                if data_str == "[DONE]":
                    continue
                try:
                    event = json.loads(data_str)
                    if not isinstance(event, dict):
                        continue
                    event_type = event.get("type")
                    if event_type == "message_start":
                        msg = event.get("message", {})
                        usage = msg.get("usage", {}) if isinstance(msg, dict) else {}
                        if isinstance(usage, dict):
                            counts["input_tokens"] += _int(usage.get("input_tokens"))
                            counts["cache_creation_input_tokens"] += _int(usage.get("cache_creation_input_tokens"))
                            counts["cache_read_input_tokens"] += _int(usage.get("cache_read_input_tokens"))
                    elif event_type == "message_delta":
                        usage = event.get("usage", {})
                        if isinstance(usage, dict):
                            counts["output_tokens"] += _int(usage.get("output_tokens"))
                except Exception:
                    continue
        except Exception:
            return None
        return _nonzero(counts)

    return None


def _extract_reported_cost_usd(body: bytes, content_type: str | None) -> float | None:
    content_type = (content_type or "").lower()
    if _JSON_CT not in content_type:
        return None
    try:
        payload = json.loads(body)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    direct_value = payload.get("total_cost_usd")
    if isinstance(direct_value, (int, float)):
        return float(direct_value)

    nested_cost = payload.get("cost")
    if isinstance(nested_cost, dict):
        usd_value = nested_cost.get("usd")
        if isinstance(usd_value, (int, float)):
            return float(usd_value)

    return None


def _extract_response_id(body: bytes) -> str | None:
    # Non-streaming: top-level id field
    try:
        payload = json.loads(body)
        if isinstance(payload, dict):
            response_id = payload.get("id")
            if isinstance(response_id, str) and response_id:
                return response_id
    except Exception:
        pass
    # Streaming SSE: Anthropic message_start event carries message.id
    try:
        text = body.decode("utf-8", errors="replace")
        for line in text.splitlines():
            if not line.startswith("data:"):
                continue
            data_str = line[5:].strip()
            if data_str == "[DONE]":
                continue
            try:
                event = json.loads(data_str)
                if not isinstance(event, dict):
                    continue
                if event.get("type") == "message_start":
                    msg = event.get("message", {})
                    if isinstance(msg, dict):
                        response_id = msg.get("id")
                        if isinstance(response_id, str) and response_id:
                            return response_id
                response_id = event.get("id")
                if isinstance(response_id, str) and response_id:
                    return response_id
            except Exception:
                continue
    except Exception:
        pass
    return None


async def _resolve_cost_usd(
    upstream: ProxyUpstream,
    request_model: str | None,
    response_body: bytes,
    content_type: str | None,
    usage_counts: dict[str, int] | None,
) -> float | None:
    reported_cost_usd = _extract_reported_cost_usd(response_body, content_type)
    if reported_cost_usd is not None:
        return reported_cost_usd
    reported_cost_usd = await upstream.fetch_cost_usd(_extract_response_id(response_body))
    if reported_cost_usd is not None:
        return reported_cost_usd
    return upstream.compute_cost_usd(request_model, usage_counts)


async def _record_proxy_log(
    db,
    broker,
    execution_id: str | None,
    session_id: str,
    *,
    stage: str,
    body: str,
    method: str,
    path: str,
    upstream_url: str,
    status_code: int | None = None,
    content_type: str | None = None,
    meta: dict[str, Any] | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_creation_input_tokens: int | None = None,
    cache_read_input_tokens: int | None = None,
    total_tokens: int | None = None,
    total_cost_usd: float | None = None,
    request_id: str | None = None,
) -> None:
    if execution_id is None:
        return
    logged_at = datetime.now(timezone.utc)
    await db.append_proxy_log(
        execution_id,
        stage=stage,
        body=body,
        method=method,
        path=path,
        upstream_url=upstream_url,
        status_code=status_code,
        content_type=content_type,
        meta=meta,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
        total_tokens=total_tokens,
        total_cost_usd=total_cost_usd,
        logged_at=logged_at,
        request_id=request_id,
    )
    if broker:
        await broker.publish(session_id, {
            "type": "proxy_log",
            "execution_id": execution_id,
            "logged_at": logged_at.isoformat(),
            "stage": stage,
            "method": method,
            "path": path,
            "upstream_url": upstream_url,
            "status_code": status_code,
            "content_type": content_type,
            "body": body,
            "meta": meta or {},
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": cache_creation_input_tokens,
            "cache_read_input_tokens": cache_read_input_tokens,
            "total_tokens": total_tokens,
            "total_cost_usd": total_cost_usd,
            "request_id": request_id,
        })


def _serialize_execution(execution) -> dict[str, Any]:
    return {
        "execution_id": execution.execution_id,
        "session_id": execution.session_id,
        "worker_address": execution.worker_address,
        "started_at": execution.started_at.isoformat(),
        "halted_at": execution.halted_at.isoformat() if execution.halted_at else None,
        "halt_reason": execution.halt_reason,
        "phase": execution.phase.value,
        "runtime_container": execution.runtime_container.to_dict() if execution.runtime_container else None,
        "worker_container": execution.worker_container.to_dict() if execution.worker_container else None,
        "exit_code": execution.exit_code,
        "claude_session_id": execution.claude_session_id,
        "claude_num_turns": execution.claude_num_turns,
        "claude_duration_ms": execution.claude_duration_ms,
        "claude_total_cost_usd": execution.claude_total_cost_usd,
        "claude_input_tokens": execution.claude_input_tokens,
        "claude_output_tokens": execution.claude_output_tokens,
        "claude_cache_creation_input_tokens": execution.claude_cache_creation_input_tokens,
        "claude_cache_read_input_tokens": execution.claude_cache_read_input_tokens,
        "claude_total_tokens": execution.claude_total_tokens,
        "agent_error_category": execution.agent_error_category,
        "agent_error_reason": execution.agent_error_reason,
    }


async def _publish_execution_update(db, broker, execution_id: str | None, session_id: str) -> None:
    if execution_id is None or broker is None:
        return
    execution = await db.get_execution(execution_id)
    if execution is None:
        return
    await broker.publish(session_id, {
        "type": "execution",
        "execution": _serialize_execution(execution),
    })


def create_proxy_routes(
    app: FastAPI,
    db,
    upstream: ProxyUpstream,
    secret: str,
    broker=None,
    log_conversation: bool = False,
) -> None:
    logger.info(
        f"proxy registered upstream_kind={upstream.kind} upstream={upstream.base_url} "
        f"auth={'key' if upstream.api_key else 'placeholder'}",
    )

    @app.api_route("/proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    async def proxy(request: Request, path: str):
        token = request.headers.get("x-api-key", "")
        session_id = verify_token(token, secret)
        if not session_id:
            logger.warning(f"proxy rejected invalid token path={path}")
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        if not await db.is_execution_active(session_id):
            logger.warning(f"proxy rejected inactive session session_id={session_id} path={path}")
            raise HTTPException(status_code=401, detail="No active execution for session")

        execution = await db.get_active_execution(session_id) if log_conversation else None
        execution_id = execution.execution_id if execution is not None else None
        request_id = str(uuid.uuid4())
        body = await request.body()
        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in ("host", "x-api-key", "authorization", "content-length")
        }
        original_headers = dict(headers)
        original_body = body
        adapted_path, adapted_headers, adapted_body = upstream.adapter.adapt_request(path, headers, body)
        # For Bedrock the request shape changes again: drop "model" + "stream"
        # from the body and remember them — boto3 takes them as separate args.
        bedrock_streaming = False
        bedrock_model_id: str | None = None
        if upstream.is_bedrock:
            bedrock_streaming = _extract_request_streaming(adapted_body, adapted_headers.get("content-type"))
            adapted_body, bedrock_model_id = adapt_anthropic_to_bedrock_body(adapted_body)
            request_model = bedrock_model_id
        else:
            request_model = _extract_request_model(adapted_body, adapted_headers.get("content-type"))
        try:
            request_model = upstream.ensure_known_model(request_model)
        except UnknownUpstreamModelError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        authed_headers = upstream.apply_auth(adapted_headers) if not upstream.is_bedrock else adapted_headers
        upstream_url = (
            f"bedrock://{request_model or ''}{bedrock_invoke_path(request_model or '', streaming=bedrock_streaming)}"
            if upstream.is_bedrock
            else upstream.url_for(adapted_path)
        )
        if log_conversation:
            await _record_proxy_log(
                db,
                broker,
                execution_id,
                session_id,
                stage="request_in",
                body=_format_body_for_log(original_body, original_headers.get("content-type")),
                method=request.method,
                path=path,
                upstream_url=upstream_url,
                content_type=original_headers.get("content-type"),
                request_id=request_id,
            )
            rewrite_applied = adapted_body != original_body or adapted_path != path
            await _record_proxy_log(
                db,
                broker,
                execution_id,
                session_id,
                stage="request_out",
                body=_format_body_for_log(adapted_body, authed_headers.get("content-type")),
                method=request.method,
                path=path,
                upstream_url=upstream_url,
                content_type=authed_headers.get("content-type"),
                meta={"rewrite_applied": rewrite_applied},
                request_id=request_id,
            )
        headers, body = authed_headers, adapted_body

        logger.debug(f"proxy forwarding session_id={session_id} method={request.method} upstream={upstream_url}")
        client: httpx.AsyncClient | None = None
        if upstream.is_bedrock:
            # Bypass the generic httpx forwarder: boto3 handles SigV4 + AWS
            # event-stream framing and we get back an httpx-shaped response.
            assert upstream.bedrock_client is not None
            try:
                resp = await upstream.bedrock_client.invoke(
                    model_id=bedrock_model_id or "",
                    body=body,
                    streaming=bedrock_streaming,
                )
            except Exception as e:
                logger.warning(f"proxy bedrock error session_id={session_id} model={bedrock_model_id!r} error={e!r}")
                if log_conversation:
                    await _record_proxy_log(
                        db,
                        broker,
                        execution_id,
                        session_id,
                        stage="response_in",
                        body=str(e),
                        method=request.method,
                        path=path,
                        upstream_url=upstream_url,
                        content_type="text/plain",
                        meta={"transport_error": True},
                        request_id=request_id,
                    )
                raise HTTPException(status_code=502, detail="Upstream unreachable")
        else:
            client = httpx.AsyncClient(timeout=httpx.Timeout(None))
            try:
                req = client.build_request(
                    method=request.method,
                    url=upstream_url,
                    headers=headers,
                    content=body,
                    params=dict(request.query_params),
                )
                resp = await client.send(req, stream=True)
            except Exception as e:
                if log_conversation:
                    await _record_proxy_log(
                        db,
                        broker,
                        execution_id,
                        session_id,
                        stage="response_in",
                        body=str(e),
                        method=request.method,
                        path=path,
                        upstream_url=upstream_url,
                        content_type="text/plain",
                        meta={"transport_error": True},
                        request_id=request_id,
                    )
                await client.aclose()
                logger.warning(f"proxy upstream error session_id={session_id} upstream={upstream_url} error={e!r}")
                raise HTTPException(status_code=502, detail="Upstream unreachable")

        if resp.status_code >= 400:
            error_body = await resp.aread()
            if log_conversation:
                formatted_body = _format_body_for_log(error_body, resp.headers.get("content-type"))
                await _record_proxy_log(
                    db,
                    broker,
                    execution_id,
                    session_id,
                    stage="response_in",
                    body=formatted_body,
                    method=request.method,
                    path=path,
                    upstream_url=upstream_url,
                    status_code=resp.status_code,
                    content_type=resp.headers.get("content-type"),
                    meta={"error": True},
                    request_id=request_id,
                )
                await _record_proxy_log(
                    db,
                    broker,
                    execution_id,
                    session_id,
                    stage="response_out",
                    body=formatted_body,
                    method=request.method,
                    path=path,
                    upstream_url=upstream_url,
                    status_code=resp.status_code,
                    content_type=resp.headers.get("content-type"),
                    meta={"error": True},
                    request_id=request_id,
                )
            await resp.aclose()
            if client is not None:
                await client.aclose()
            logger.warning(
                f"proxy upstream error session_id={session_id} upstream={upstream_url} "
                f"status={resp.status_code} body={error_body.decode(errors='replace')[:500]}"
            )
            return StreamingResponse(
                iter([error_body]),
                status_code=resp.status_code,
                headers={k: v for k, v in resp.headers.items()
                         if k.lower() not in {"transfer-encoding", "content-encoding", "content-length", "connection"}},
            )

        skip = {"transfer-encoding", "content-encoding", "content-length", "connection"}
        resp_headers = {k: v for k, v in resp.headers.items() if k.lower() not in skip}
        logger.debug(f"proxy success session_id={session_id} status={resp.status_code}")
        response_chunks: list[bytes] = []

        async def body_iter():
            adapted_chunks = upstream.adapter.adapt_response_stream(
                resp.headers.get("content-type"),
                resp.aiter_bytes(),
            )
            try:
                async for chunk in adapted_chunks:
                    if log_conversation:
                        response_chunks.append(chunk)
                    yield chunk
            finally:
                if log_conversation:
                    response_body = b"".join(response_chunks)
                    content_type = resp.headers.get("content-type")
                    usage_counts = _extract_usage_counts(response_body, content_type)
                    reported_cost_usd = await _resolve_cost_usd(
                        upstream,
                        request_model,
                        response_body,
                        content_type,
                        usage_counts,
                    )
                    total_tokens = (
                        usage_counts["input_tokens"]
                        + usage_counts["output_tokens"]
                        + usage_counts["cache_creation_input_tokens"]
                        + usage_counts["cache_read_input_tokens"]
                        if usage_counts
                        else None
                    )
                    if execution_id is not None and (usage_counts or reported_cost_usd is not None):
                        await db.increment_execution_token_usage(
                            execution_id,
                            input_tokens=usage_counts["input_tokens"] if usage_counts else 0,
                            output_tokens=usage_counts["output_tokens"] if usage_counts else 0,
                            cache_creation_input_tokens=(
                                usage_counts["cache_creation_input_tokens"] if usage_counts else 0
                            ),
                            cache_read_input_tokens=(
                                usage_counts["cache_read_input_tokens"] if usage_counts else 0
                            ),
                            total_cost_usd=reported_cost_usd,
                        )
                        await _publish_execution_update(db, broker, execution_id, session_id)
                    formatted_body = _format_body_for_log(response_body, content_type)
                    await _record_proxy_log(
                        db,
                        broker,
                        execution_id,
                        session_id,
                        stage="response_in",
                        body=formatted_body,
                        method=request.method,
                        path=path,
                        upstream_url=upstream_url,
                        status_code=resp.status_code,
                        content_type=content_type,
                        input_tokens=usage_counts["input_tokens"] if usage_counts else None,
                        output_tokens=usage_counts["output_tokens"] if usage_counts else None,
                        cache_creation_input_tokens=(
                            usage_counts["cache_creation_input_tokens"] if usage_counts else None
                        ),
                        cache_read_input_tokens=(
                            usage_counts["cache_read_input_tokens"] if usage_counts else None
                        ),
                        total_tokens=total_tokens,
                        total_cost_usd=reported_cost_usd,
                        request_id=request_id,
                    )
                    await _record_proxy_log(
                        db,
                        broker,
                        execution_id,
                        session_id,
                        stage="response_out",
                        body=formatted_body,
                        method=request.method,
                        path=path,
                        upstream_url=upstream_url,
                        status_code=resp.status_code,
                        content_type=content_type,
                        input_tokens=usage_counts["input_tokens"] if usage_counts else None,
                        output_tokens=usage_counts["output_tokens"] if usage_counts else None,
                        cache_creation_input_tokens=(
                            usage_counts["cache_creation_input_tokens"] if usage_counts else None
                        ),
                        cache_read_input_tokens=(
                            usage_counts["cache_read_input_tokens"] if usage_counts else None
                        ),
                        total_tokens=total_tokens,
                        total_cost_usd=reported_cost_usd,
                        request_id=request_id,
                    )
                await resp.aclose()
                if client is not None:
                    await client.aclose()

        return StreamingResponse(body_iter(), status_code=resp.status_code, headers=resp_headers)
