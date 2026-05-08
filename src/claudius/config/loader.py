from pathlib import Path

import yaml
from pydantic import ValidationError

from claudius.config.schema import StartupConfig, WorkflowConfig


DEFAULT_STARTUP_CONFIG_PATH = Path("config/claudius.yaml")
UPSTREAM_PROTOCOL_DEFAULTS = {
    "anthropic": {
        "base_url": "https://api.anthropic.com",
        "api_key_env": "ANTHROPIC_API_KEY",
        "auth_mode": "x-api-key",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "api_key_env": "OPENAI_API_KEY",
        "auth_mode": "bearer",
    },
}


class ConfigError(Exception):
    pass


def load_startup_config(path: Path = DEFAULT_STARTUP_CONFIG_PATH) -> StartupConfig | None:
    path = Path(path)
    if not path.exists():
        return None
    raw = yaml.safe_load(path.read_text()) or {}
    try:
        return StartupConfig.model_validate(raw)
    except ValidationError as e:
        raise ConfigError(f"Invalid startup config in {path}: {e}") from e


def resolve_startup_config(
    file_config: StartupConfig | None,
    cli_overrides: dict[str, object] | None = None,
) -> StartupConfig:
    data = (file_config or StartupConfig()).model_dump()
    overrides = cli_overrides or {}
    upstream_data = dict(data["upstream_llm"])

    for key, value in overrides.items():
        if key == "proxy_upstream_kind":
            upstream_data["protocol"] = value
        elif key == "proxy_upstream_url":
            upstream_data["base_url"] = value
        elif key == "proxy_upstream_api_key_env":
            upstream_data["api_key_env"] = value
        else:
            data[key] = value

    protocol = str(upstream_data["protocol"]).strip().lower()
    defaults = UPSTREAM_PROTOCOL_DEFAULTS[protocol]
    upstream_data["protocol"] = protocol
    upstream_data["base_url"] = str(upstream_data.get("base_url", "") or defaults["base_url"]).rstrip("/")
    upstream_data["api_key_env"] = str(upstream_data.get("api_key_env", "") or defaults["api_key_env"]).strip()
    upstream_data["auth_mode"] = str(upstream_data.get("auth_mode", "") or defaults["auth_mode"]).strip().lower()
    data["upstream_llm"] = upstream_data
    return StartupConfig.model_validate(data)


def load_workflows(path: Path) -> list[WorkflowConfig]:
    return load_workflows_with_defaults(path, None)


def load_workflows_with_defaults(
    path: Path,
    startup_config: StartupConfig | None,
) -> list[WorkflowConfig]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config path not found: {path}")

    if path.is_file():
        files = [path]
    else:
        files = sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))

    workflows = []
    default_email_to = None
    if startup_config is not None and startup_config.channels.email.provider == "resend":
        default_email_to = startup_config.channels.email.from_address
    for f in files:
        raw = yaml.safe_load(f.read_text())
        try:
            workflow = WorkflowConfig.model_validate(raw)
        except ValidationError as e:
            raise ConfigError(f"Invalid workflow config in {f}: {e}") from e
        if default_email_to and "email" in workflow.routing.channels and not workflow.routing.to:
            workflow.routing.to = [default_email_to]
        workflows.append(workflow)
    return workflows
