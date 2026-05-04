import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from claudius.runtime_mcp_bridge import _handle_message, _BUILTIN_REPORT_FATAL_ERROR


_TOOLS = {
    "git_status": {
        "name": "git_status",
        "description": "Show git status",
        "inputSchema": {"type": "object", "properties": {}},
    }
}
_ENDPOINT = "http://sidecar:8090"
_AUTH = "sidecar-token"
_CALLBACK = "http://controller:8080"
_CTOKEN = "jwt-abc"
_SESSION = "sess-42"


def _call(message, *, with_callback=True):
    return _handle_message(
        message,
        _TOOLS,
        _ENDPOINT,
        _AUTH,
        callback_url=_CALLBACK if with_callback else "",
        callback_token=_CTOKEN if with_callback else "",
        session_id=_SESSION if with_callback else "",
    )


def test_tools_list_includes_builtin_when_callback_configured():
    resp = _call({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = [t["name"] for t in resp["result"]["tools"]]
    assert "report_fatal_error" in names
    assert names[0] == "report_fatal_error"


def test_tools_list_omits_builtin_without_callback():
    resp = _call({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, with_callback=False)
    names = [t["name"] for t in resp["result"]["tools"]]
    assert "report_fatal_error" not in names


def test_report_fatal_error_posts_to_controller():
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()

    with patch("claudius.runtime_mcp_bridge.httpx.post", return_value=mock_resp) as mock_post:
        resp = _call({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "report_fatal_error",
                "arguments": {"category": "tool_failure", "reason": "Could not reach API."},
            },
        })

    mock_post.assert_called_once_with(
        f"{_CALLBACK}/sessions/{_SESSION}/fatal-error",
        json={"category": "tool_failure", "reason": "Could not reach API."},
        headers={"Authorization": f"Bearer {_CTOKEN}"},
        timeout=10.0,
    )
    assert resp["result"]["isError"] is False
    assert resp["result"]["content"][0]["text"] == "Fatal error recorded."


def test_report_fatal_error_returns_error_when_callback_not_configured():
    resp = _call(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "report_fatal_error",
                "arguments": {"category": "tool_failure", "reason": "x"},
            },
        },
        with_callback=False,
    )
    assert "error" in resp
    assert "callback not configured" in resp["error"]["message"]


def test_report_fatal_error_http_error_returns_mcp_error():
    with patch(
        "claudius.runtime_mcp_bridge.httpx.post",
        side_effect=httpx.ConnectError("connection refused"),
    ):
        resp = _call({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "report_fatal_error",
                "arguments": {"category": "unexpected_error", "reason": "boom"},
            },
        })
    assert "error" in resp
    assert "could not reach controller" in resp["error"]["message"]


def test_unknown_tool_returns_error():
    resp = _call({
        "jsonrpc": "2.0",
        "id": 8,
        "method": "tools/call",
        "params": {"name": "no_such_tool", "arguments": {}},
    })
    assert "error" in resp
    assert resp["error"]["code"] == -32602
