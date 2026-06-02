"""AWS Bedrock upstream for the controller's Anthropic proxy.

Bedrock speaks a wire-format the rest of the proxy doesn't: SigV4 signing for
auth and AWS event-stream framing for streaming responses. Rather than teach
the generic httpx-based proxy two new tricks, this module hides Bedrock behind
a thin client whose `invoke()` returns an object that *quacks like* an
``httpx.Response`` opened with ``stream=True``. That lets the proxy handler
keep its single hot-path: read body, log, stream it back through the
``MessageFormatAdapter``.

Auth: AWS credentials come from the standard boto3 chain (env vars, IAM role,
``~/.aws/...``). Region falls back to the boto3 session default.

Translation pipeline:

    Anthropic JSON request (from Claude Code)
        |
        | adapt_request   (drop "model", inject anthropic_version, drop "stream")
        v
    Bedrock JSON request (passed to invoke_model_with_response_stream)
        |
        | (boto3 SigV4-signs and streams)
        v
    EventStream of {"chunk": {"bytes": <base64 anthropic-SSE-event-json>}, ...}
        |
        | _bedrock_stream_to_anthropic_sse
        v
    text/event-stream bytes (forwarded to Claude Code unchanged by AnthropicAdapter)

Bedrock returns Anthropic-shaped event JSON inside its ``chunk.bytes`` payloads
(``message_start``, ``content_block_*``, ``message_delta``, ``message_stop``),
so we just re-frame each one as ``event: <type>\\ndata: <json>\\n\\n``. No
content translation is needed.
"""

from __future__ import annotations

import asyncio
import json
import urllib.parse
from typing import AsyncIterator, Optional

try:  # boto3 is an optional dependency — only needed for protocol: bedrock.
    import boto3
    from botocore.config import Config as BotoConfig
except ImportError:  # pragma: no cover
    boto3 = None
    BotoConfig = None


_STREAM_PATH_SUFFIX = "/invoke-with-response-stream"
_NON_STREAM_PATH_SUFFIX = "/invoke"


class BedrockNotInstalledError(RuntimeError):
    pass


def _require_boto3():
    if boto3 is None:
        raise BedrockNotInstalledError(
            "boto3 is required for upstream_llm.protocol='bedrock'. "
            "Install it in the controller's environment."
        )


def adapt_anthropic_to_bedrock_body(body: bytes) -> tuple[bytes, str | None]:
    """Translate a Claude Code Messages request body into a Bedrock body.

    Returns ``(adapted_body, model_id)``. ``model_id`` is what Claude Code
    asked for; the caller URL-encodes it into the Bedrock invoke path.

    The Anthropic Messages format is *almost* the same as Bedrock's. Diffs:
      - Bedrock requires ``anthropic_version: "bedrock-2023-05-31"``.
      - Bedrock infers streaming from the URL, so ``stream`` in the body is
        ignored — drop it for a clean payload.
      - Bedrock's invoke URL carries the model id; the body must NOT.
    """
    if not body:
        return body, None
    payload = json.loads(body)
    if not isinstance(payload, dict):
        return body, None
    model = payload.pop("model", None)
    payload.pop("stream", None)
    payload.setdefault("anthropic_version", "bedrock-2023-05-31")
    return json.dumps(payload).encode("utf-8"), model if isinstance(model, str) else None


def bedrock_invoke_path(model_id: str, *, streaming: bool) -> str:
    """The path the Bedrock SDK targets, for logging only.

    boto3 builds the actual URL itself; we record this string in the proxy log
    so operators can see what the request shape was.
    """
    suffix = _STREAM_PATH_SUFFIX if streaming else _NON_STREAM_PATH_SUFFIX
    return f"/model/{urllib.parse.quote(model_id, safe='')}{suffix}"


def _sse(event_type: str, data_bytes: bytes) -> bytes:
    """Format a chunk as text/event-stream bytes."""
    return b"event: " + event_type.encode("ascii") + b"\ndata: " + data_bytes + b"\n\n"


async def _bedrock_stream_to_anthropic_sse(
    event_iter: AsyncIterator[dict],
) -> AsyncIterator[bytes]:
    """Re-frame Bedrock event-stream chunks as Anthropic SSE bytes.

    Bedrock yields events shaped like::

        {"chunk": {"bytes": <base64-decoded-bytes>}}
        {"internalServerException": {...}}
        {"modelStreamErrorException": {...}}

    where the ``chunk.bytes`` payload is the JSON of a standard Anthropic
    streaming event (``{"type": "message_start", ...}``). We forward each one
    as ``event: <type>\\ndata: <json>\\n\\n``, which is exactly what
    Claude Code expects from ``api.anthropic.com``.
    """
    async for event in event_iter:
        chunk = event.get("chunk")
        if chunk is None:
            # Surface any error event as an SSE error frame so the agent can log it.
            yield _sse("error", json.dumps({"error": event}).encode("utf-8"))
            continue
        raw = chunk.get("bytes")
        if not isinstance(raw, (bytes, bytearray)):
            continue
        try:
            parsed = json.loads(raw)
            event_type = parsed.get("type", "message")
        except Exception:
            event_type = "message"
        yield _sse(event_type, bytes(raw))


class _BedrockHeaders(dict):
    """httpx-style headers: dict-like with ``.items()`` keeping case."""


class BedrockResponse:
    """Quacks like ``httpx.Response`` (stream=True) for the proxy hot path.

    The proxy reads ``status_code``, ``headers``, ``aread()`` (on error),
    ``aiter_bytes()``, and ``aclose()``. We implement just those — the body
    is the SSE-reframed event stream.
    """

    def __init__(
        self,
        status_code: int,
        headers: dict[str, str],
        chunks: AsyncIterator[bytes],
        on_close: Optional[callable] = None,
    ) -> None:
        self.status_code = status_code
        self.headers = _BedrockHeaders(headers)
        self._chunks = chunks
        self._on_close = on_close
        self._closed = False
        self._buffered: Optional[bytes] = None

    async def aread(self) -> bytes:
        if self._buffered is not None:
            return self._buffered
        parts: list[bytes] = []
        async for c in self._chunks:
            parts.append(c)
        self._buffered = b"".join(parts)
        return self._buffered

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        if self._buffered is not None:
            yield self._buffered
            return
        async for c in self._chunks:
            yield c

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._on_close is not None:
            try:
                self._on_close()
            except Exception:
                pass


class BedrockUpstreamClient:
    """Thin wrapper around bedrock-runtime for the proxy.

    Constructed once at startup; ``invoke()`` is called per request.
    """

    def __init__(self, region: str = "") -> None:
        _require_boto3()
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        # The bedrock-runtime SDK call is synchronous; wrap it in a thread.
        # Read timeouts are generous because Anthropic streams can be long.
        kwargs["config"] = BotoConfig(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
        self._client = boto3.client("bedrock-runtime", **kwargs)

    async def invoke(self, *, model_id: str, body: bytes, streaming: bool) -> BedrockResponse:
        """Issue the request and return an httpx-shaped response.

        For streaming, the response body is a text/event-stream that the
        AnthropicAdapter forwards verbatim. For non-streaming, it's raw
        JSON in the Anthropic Messages shape (Bedrock returns this directly
        in the ``body`` envelope of ``invoke_model``).
        """
        loop = asyncio.get_running_loop()
        try:
            if streaming:
                resp = await loop.run_in_executor(
                    None,
                    lambda: self._client.invoke_model_with_response_stream(
                        modelId=model_id,
                        contentType="application/json",
                        accept="application/json",
                        body=body,
                    ),
                )
                event_stream = resp["body"]
                event_iter = _sync_event_stream_to_async(event_stream, loop)
                sse_iter = _bedrock_stream_to_anthropic_sse(event_iter)
                return BedrockResponse(
                    status_code=200,
                    headers={"content-type": "text/event-stream"},
                    chunks=sse_iter,
                    on_close=lambda: event_stream.close()
                    if hasattr(event_stream, "close") else None,
                )
            else:
                resp = await loop.run_in_executor(
                    None,
                    lambda: self._client.invoke_model(
                        modelId=model_id,
                        contentType="application/json",
                        accept="application/json",
                        body=body,
                    ),
                )
                payload = resp["body"].read()
                async def _one_shot():
                    yield payload
                return BedrockResponse(
                    status_code=200,
                    headers={"content-type": "application/json"},
                    chunks=_one_shot(),
                )
        except Exception as exc:
            # boto3 raises ClientError with structured info; surface it as a
            # 5xx-shaped response so the proxy's existing error path logs it.
            err_body = json.dumps({"error": {"type": exc.__class__.__name__, "message": str(exc)}}).encode("utf-8")
            async def _one_shot_err():
                yield err_body
            status = 500
            # botocore.exceptions.ClientError carries an HTTP status in metadata
            metadata = getattr(exc, "response", None)
            if isinstance(metadata, dict):
                http_status = metadata.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if isinstance(http_status, int):
                    status = http_status
            return BedrockResponse(
                status_code=status,
                headers={"content-type": "application/json"},
                chunks=_one_shot_err(),
            )


async def _sync_event_stream_to_async(stream, loop) -> AsyncIterator[dict]:
    """Bridge boto3's synchronous EventStream iterator into asyncio.

    boto3's EventStream is a generator that blocks on socket reads, so we
    pull each event off in a thread to keep the controller event loop free.
    """
    iterator = iter(stream)

    def _next():
        try:
            return next(iterator)
        except StopIteration:
            return None

    while True:
        event = await loop.run_in_executor(None, _next)
        if event is None:
            return
        yield event
