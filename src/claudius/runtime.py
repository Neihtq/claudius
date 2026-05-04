import json
import os
import re
import secrets
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from claudius.config.schema import RuntimeConfig, RuntimeToolParamConfig
from claudius.config.template import TemplateContext, expand


class SecretProvider:
    def read(self, ref: str, *, field: str | None = None) -> str:
        raise NotImplementedError


class EnvironmentSecretProvider(SecretProvider):
    """Minimal v1 secret provider.

    Supports `env:NAME` explicitly and falls back to using the full ref as an
    environment-variable name when it already matches an env key.
    """

    def read(self, ref: str, *, field: str | None = None) -> str:
        if field is not None:
            raise KeyError(f"field selection is unsupported for env secret reference: {ref!r}")
        if ref.startswith("env:"):
            name = ref.split(":", 1)[1]
            try:
                return os.environ[name]
            except KeyError as exc:
                raise KeyError(f"missing secret env var {name!r}") from exc
        if ref in os.environ:
            return os.environ[ref]
        raise KeyError(f"unsupported secret reference: {ref!r}")


@dataclass
class ResolvedRuntimeHook:
    when: str
    run: str


@dataclass
class ResolvedRuntimeTool:
    name: str
    description: str
    run: str
    params: dict[str, dict[str, str]]
    input_env_map: dict[str, str]

    def input_schema(self) -> dict[str, Any]:
        properties: dict[str, Any] = {}
        required: list[str] = []
        for name, spec in self.params.items():
            properties[name] = {
                "type": spec["type"],
                "description": spec.get("description", ""),
            }
            required.append(name)
        return {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        }


@dataclass
class ResolvedRuntimeSpec:
    context_env: dict[str, str] = field(default_factory=dict)
    hooks: list[ResolvedRuntimeHook] = field(default_factory=list)
    tools: list[ResolvedRuntimeTool] = field(default_factory=list)

    def has_tools(self) -> bool:
        return bool(self.tools)

    def has_hooks(self) -> bool:
        return bool(self.hooks)


def resolve_runtime(
    runtime: RuntimeConfig,
    *,
    template_ctx: TemplateContext,
    secret_provider: SecretProvider,
) -> ResolvedRuntimeSpec:
    context_env: dict[str, str] = {}
    for item in runtime.tool_context:
        if item.value is not None:
            context_env[item.name] = expand(item.value, template_ctx)
        elif item.env is not None:
            env_name = expand(item.env, template_ctx)
            context_env[item.name] = secret_provider.read(f"env:{env_name}")
        else:
            ref = expand(item.vault_read.path, template_ctx)
            field_name = (
                expand(item.vault_read.field, template_ctx)
                if item.vault_read.field is not None
                else None
            )
            context_env[item.name] = _read_context_secret(
                secret_provider,
                context_name=item.name,
                ref=ref,
                field_name=field_name,
            )

    hooks = [
        ResolvedRuntimeHook(when=hook.when, run=expand(hook.run, template_ctx))
        for hook in runtime.pre_launch
    ]

    tools: list[ResolvedRuntimeTool] = []
    for tool in runtime.tools:
        input_env_map = {
            name: input_env_name(name)
            for name in tool.params
        }
        tools.append(
            ResolvedRuntimeTool(
                name=tool.name,
                description=tool.description,
                run=expand(tool.run, template_ctx),
                params={
                    name: _param_spec_dict(spec)
                    for name, spec in tool.params.items()
                },
                input_env_map=input_env_map,
            )
        )

    return ResolvedRuntimeSpec(context_env=context_env, hooks=hooks, tools=tools)


def input_env_name(name: str) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_")
    return f"INPUT_{normalized}"


def build_runtime_sidecar_payload(spec: ResolvedRuntimeSpec, auth_token: str) -> dict[str, Any]:
    return {
        "auth_token": auth_token,
        "context_env": spec.context_env,
        "hooks": [asdict(item) for item in spec.hooks],
        "tools": [
            {
                "name": tool.name,
                "description": tool.description,
                "run": tool.run,
                "params": tool.params,
                "input_env_map": tool.input_env_map,
            }
            for tool in spec.tools
        ],
    }


def build_runtime_bridge_payload(
    spec: ResolvedRuntimeSpec,
    *,
    endpoint_url: str,
    auth_token: str,
    callback_url: str = "",
    callback_token: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "endpoint_url": endpoint_url,
        "auth_token": auth_token,
        "tools": [
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema(),
            }
            for tool in spec.tools
        ],
    }
    if callback_url and callback_token and session_id:
        payload["callback_url"] = callback_url
        payload["callback_token"] = callback_token
        payload["session_id"] = session_id
    return payload


def write_runtime_bridge_files(
    workspace_path: str | Path,
    *,
    spec: ResolvedRuntimeSpec,
    endpoint_url: str,
    auth_token: str,
    callback_url: str = "",
    callback_token: str = "",
    session_id: str = "",
) -> tuple[Path, Path]:
    runtime_dir = Path(workspace_path) / ".claudius-runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    bridge_spec_path = runtime_dir / "bridge-spec.json"
    mcp_config_path = runtime_dir / "mcp.json"
    container_runtime_dir = Path("/workspace/.claudius-runtime")

    bridge_spec_path.write_text(
        json.dumps(
            build_runtime_bridge_payload(
                spec,
                endpoint_url=endpoint_url,
                auth_token=auth_token,
                callback_url=callback_url,
                callback_token=callback_token,
                session_id=session_id,
            ),
            indent=2,
        )
    )
    mcp_config_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "claudius-runtime": {
                        "type": "stdio",
                        "command": "claudius",
                        "args": [
                            "runtime-mcp-bridge",
                            "--spec",
                            str(container_runtime_dir / bridge_spec_path.name),
                        ],
                    }
                }
            },
            indent=2,
        )
    )
    return bridge_spec_path, mcp_config_path


def new_runtime_auth_token() -> str:
    return secrets.token_hex(16)


def _param_spec_dict(spec: RuntimeToolParamConfig) -> dict[str, str]:
    return {
        "type": spec.type,
        "description": spec.description,
    }


def _read_context_secret(
    secret_provider: SecretProvider,
    *,
    context_name: str,
    ref: str,
    field_name: str | None,
) -> str:
    try:
        return secret_provider.read(ref, field=field_name)
    except KeyError:
        # The built-in provider is environment-backed and cannot interpret
        # structured vault paths. Fall back to the target env name so example
        # workflows can use vault_read in config while sourcing secrets locally
        # from process env vars such as GITLAB_TOKEN.
        if isinstance(secret_provider, EnvironmentSecretProvider) and context_name in os.environ:
            return secret_provider.read(f"env:{context_name}")
        raise
