import json
import sys
from pathlib import Path
from typing import Any

import httpx


_BUILTIN_REPORT_FATAL_ERROR = {
    "name": "report_fatal_error",
    "description": (
        "Report an irrecoverable error to the Claudius controller. "
        "Call this when a required tool, resource, or permission is permanently unavailable "
        "and the task cannot proceed. After calling this tool, send the user a brief, "
        "non-technical closing message and stop."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "enum": [
                    "tool_failure",
                    "permission_error",
                    "resource_unavailable",
                    "configuration_error",
                    "unexpected_error",
                ],
                "description": "Machine-readable category for the error.",
            },
            "reason": {
                "type": "string",
                "description": "Human-readable explanation of why the task cannot continue.",
            },
        },
        "required": ["category", "reason"],
    },
}


def run_stdio_bridge(spec_path: str) -> int:
    spec = json.loads(Path(spec_path).read_text())
    endpoint_url = spec["endpoint_url"].rstrip("/")
    auth_token = spec["auth_token"]
    callback_url = spec.get("callback_url", "").rstrip("/")
    callback_token = spec.get("callback_token", "")
    session_id = spec.get("session_id", "")
    tools = {tool["name"]: tool for tool in spec.get("tools", [])}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        response = _handle_message(
            message,
            tools,
            endpoint_url,
            auth_token,
            callback_url=callback_url,
            callback_token=callback_token,
            session_id=session_id,
        )
        if response is not None:
            sys.stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            sys.stdout.flush()
    return 0


def _handle_message(
    message: dict[str, Any],
    tools: dict[str, dict[str, Any]],
    endpoint_url: str,
    auth_token: str,
    *,
    callback_url: str = "",
    callback_token: str = "",
    session_id: str = "",
) -> dict[str, Any] | None:
    method = message.get("method")
    msg_id = message.get("id")

    if method == "notifications/initialized":
        return None

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "claudius-runtime-bridge", "version": "0.1.0"},
            },
        }

    if method == "tools/list":
        listed = list(tools.values())
        if callback_url and callback_token and session_id:
            listed = [_BUILTIN_REPORT_FATAL_ERROR] + listed
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "tools": [
                    {
                        "name": tool["name"],
                        "description": tool.get("description", ""),
                        "inputSchema": tool.get("inputSchema", {}),
                    }
                    for tool in listed
                ]
            },
        }

    if method == "tools/call":
        params = message.get("params", {})
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if tool_name == "report_fatal_error":
            return _handle_report_fatal_error(
                msg_id, arguments, callback_url, callback_token, session_id
            )

        if tool_name not in tools:
            return _error(msg_id, -32602, f"unknown tool {tool_name!r}")
        try:
            response = httpx.post(
                f"{endpoint_url}/invoke/{tool_name}",
                json={"params": arguments},
                headers={"Authorization": f"Bearer {auth_token}"},
                timeout=60.0,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text.strip() or exc.response.reason_phrase
            return _error(msg_id, -32000, f"tool call failed: {detail}")
        except httpx.HTTPError as exc:
            return _error(msg_id, -32000, f"tool call failed: {exc}")
        result = response.json()
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": _format_tool_result(tool_name, result),
                    }
                ],
                "isError": result.get("exit_code", 1) != 0,
            },
        }

    if msg_id is None:
        return None
    return _error(msg_id, -32601, f"method not found: {method}")


def _handle_report_fatal_error(
    msg_id: Any,
    arguments: dict[str, Any],
    callback_url: str,
    callback_token: str,
    session_id: str,
) -> dict[str, Any]:
    if not (callback_url and callback_token and session_id):
        return _error(msg_id, -32000, "report_fatal_error: callback not configured")

    category = arguments.get("category", "")
    reason = arguments.get("reason", "")
    if not category or not reason:
        return _error(msg_id, -32602, "report_fatal_error: category and reason are required")

    try:
        response = httpx.post(
            f"{callback_url}/sessions/{session_id}/fatal-error",
            json={"category": category, "reason": reason},
            headers={"Authorization": f"Bearer {callback_token}"},
            timeout=10.0,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip() or exc.response.reason_phrase
        return _error(msg_id, -32000, f"report_fatal_error: controller rejected request: {detail}")
    except httpx.HTTPError as exc:
        return _error(msg_id, -32000, f"report_fatal_error: could not reach controller: {exc}")

    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "result": {
            "content": [{"type": "text", "text": "Fatal error recorded."}],
            "isError": False,
        },
    }


def _format_tool_result(tool_name: str, result: dict[str, Any]) -> str:
    stdout = result.get("stdout", "")
    stderr = result.get("stderr", "")
    exit_code = result.get("exit_code", 1)
    parts = [f"tool: {tool_name}", f"exit_code: {exit_code}"]
    if stdout:
        parts.append(f"stdout:\n{stdout}")
    if stderr:
        parts.append(f"stderr:\n{stderr}")
    return "\n\n".join(parts)


def _error(msg_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": code, "message": message},
    }
