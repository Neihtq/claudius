import json
from datetime import datetime, timezone

from claudius.config.schema import RuntimeConfig
from claudius.config.template import TemplateContext
from claudius.models import InboundMessage
from claudius.runtime import (
    EnvironmentSecretProvider,
    SecretProvider,
    build_runtime_bridge_payload,
    input_env_name,
    resolve_runtime,
    write_runtime_bridge_files,
)


class _SecretProvider(SecretProvider):
    def read(self, ref: str, *, field: str | None = None) -> str:
        values = {
            ("vault/ref", None): "secret-token",
            ("vault/ref", "token"): "field-secret-token",
            ("env:GITLAB_TOKEN", None): "env-secret-token",
        }
        return values[(ref, field)]


def _ctx() -> TemplateContext:
    return TemplateContext(
        request=InboundMessage(
            channel="email",
            sender="user@example.com",
            recipients=["edit@example.com"],
            thread_id="thread-123",
            subject="hello",
            body="body",
            attachments=[],
            received_at=datetime.now(timezone.utc),
        ),
        session_id="sess-1",
        channel_name="email",
    )


def test_resolve_runtime_expands_templates_and_secrets():
    runtime = RuntimeConfig.model_validate({
        "pre_launch": [{"type": "shell", "when": "execution_start", "run": "echo $BRANCH"}],
        "tool_context": [
            {"name": "BRANCH", "value": "claudius/${{request.thread_id}}"},
            {"name": "GITLAB_TOKEN", "env": "GITLAB_TOKEN"},
            {"name": "API_TOKEN", "vault_read": {"path": "vault/ref"}},
            {"name": "API_FIELD_TOKEN", "vault_read": {"path": "vault/ref", "field": "token"}},
        ],
        "tools": [
            {
                "type": "shell",
                "name": "create_mr",
                "description": "Create MR",
                "run": "glab mr create --title \"$INPUT_TITLE\"",
                "params": {"title": {"type": "string", "description": "MR title"}},
            }
        ],
    })

    resolved = resolve_runtime(runtime, template_ctx=_ctx(), secret_provider=_SecretProvider())

    assert resolved.context_env["BRANCH"] == "claudius/thread-123"
    assert resolved.context_env["GITLAB_TOKEN"] == "env-secret-token"
    assert resolved.context_env["API_TOKEN"] == "secret-token"
    assert resolved.context_env["API_FIELD_TOKEN"] == "field-secret-token"
    assert resolved.hooks[0].when == "execution_start"
    assert resolved.tools[0].input_env_map == {"title": "INPUT_TITLE"}
    assert resolved.tools[0].input_schema()["properties"]["title"]["type"] == "string"


def test_input_env_name_normalizes_param_names():
    assert input_env_name("title") == "INPUT_TITLE"
    assert input_env_name("merge-request_title") == "INPUT_MERGE_REQUEST_TITLE"


def test_write_runtime_bridge_files_uses_container_spec_path(tmp_path):
    runtime = RuntimeConfig.model_validate({
        "tools": [
            {
                "type": "shell",
                "name": "git_status",
                "description": "Show git status",
                "run": "git status --short",
            }
        ]
    })

    resolved = resolve_runtime(runtime, template_ctx=_ctx(), secret_provider=_SecretProvider())

    bridge_spec_path, mcp_config_path = write_runtime_bridge_files(
        tmp_path,
        spec=resolved,
        endpoint_url="http://claudius-runtime-test:8090",
        auth_token="token-123",
    )

    assert bridge_spec_path.exists()
    mcp_config = json.loads(mcp_config_path.read_text())
    server = mcp_config["mcpServers"]["claudius-runtime"]
    assert server["args"] == [
        "runtime-mcp-bridge",
        "--spec",
        "/workspace/.claudius-runtime/bridge-spec.json",
    ]


def test_resolve_runtime_falls_back_to_context_env_for_structured_ref(monkeypatch):
    monkeypatch.setenv("GITLAB_TOKEN", "env-fallback-token")
    runtime = RuntimeConfig.model_validate({
        "tool_context": [
            {
                "name": "GITLAB_TOKEN",
                "vault_read": {"path": "gitlab.com/config/default", "field": "token"},
            }
        ]
    })

    resolved = resolve_runtime(runtime, template_ctx=_ctx(), secret_provider=EnvironmentSecretProvider())

    assert resolved.context_env["GITLAB_TOKEN"] == "env-fallback-token"


def test_build_runtime_bridge_payload_includes_callback_fields():
    runtime = RuntimeConfig.model_validate({
        "tools": [{"type": "shell", "name": "git_status", "description": "Show git status", "run": "git status"}]
    })
    resolved = resolve_runtime(runtime, template_ctx=_ctx(), secret_provider=_SecretProvider())

    payload = build_runtime_bridge_payload(
        resolved,
        endpoint_url="http://sidecar:8090",
        auth_token="tok",
        callback_url="http://controller:8080",
        callback_token="jwt-abc",
        session_id="sess-42",
    )

    assert payload["callback_url"] == "http://controller:8080"
    assert payload["callback_token"] == "jwt-abc"
    assert payload["session_id"] == "sess-42"


def test_build_runtime_bridge_payload_omits_callback_fields_when_empty():
    runtime = RuntimeConfig.model_validate({
        "tools": [{"type": "shell", "name": "git_status", "description": "Show git status", "run": "git status"}]
    })
    resolved = resolve_runtime(runtime, template_ctx=_ctx(), secret_provider=_SecretProvider())

    payload = build_runtime_bridge_payload(
        resolved,
        endpoint_url="http://sidecar:8090",
        auth_token="tok",
    )

    assert "callback_url" not in payload
    assert "callback_token" not in payload
    assert "session_id" not in payload
