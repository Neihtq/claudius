import re

from pydantic import BaseModel, Field, model_validator


_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_TOOL_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")
DOCKER_PROBE_MODES = (
    "host_port",
    "container_ip",
)
UPSTREAM_PROTOCOL_KINDS = (
    "anthropic",
    "openai",
)
UPSTREAM_AUTH_MODES = (
    "x-api-key",
    "bearer",
    "none",
)
_BUILTIN_TOOL_NAMES = {
    "bash",
    "edit",
    "glob",
    "grep",
    "ls",
    "multiedit",
    "read",
    "task",
    "todoread",
    "todowrite",
    "webfetch",
    "websearch",
    "write",
}


class RoutingConfig(BaseModel):
    channels: list[str]
    from_: list[str] = Field(default_factory=list, alias="from")
    subject_patterns: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class ClaudeConfig(BaseModel):
    model: str = "claude-opus-4-7"
    system_prompt: str
    tools: list[dict] = Field(default_factory=list)
    supports_images: bool = True


class RuntimeHookConfig(BaseModel):
    type: str = "shell"
    when: str
    run: str

    @model_validator(mode="after")
    def validate_hook(self) -> "RuntimeHookConfig":
        if self.type != "shell":
            raise ValueError("runtime.pre_launch only supports type='shell'")
        if self.when not in {"session_start", "execution_start"}:
            raise ValueError("runtime.pre_launch.when must be 'session_start' or 'execution_start'")
        if not self.run.strip():
            raise ValueError("runtime.pre_launch.run must not be empty")
        return self


class RuntimeVaultReadConfig(BaseModel):
    path: str
    field: str | None = None

    @model_validator(mode="after")
    def validate_vault_read(self) -> "RuntimeVaultReadConfig":
        if not self.path.strip():
            raise ValueError("runtime.tool_context vault_read.path must not be empty")
        if self.field is not None and not self.field.strip():
            raise ValueError("runtime.tool_context vault_read.field must not be empty")
        return self


class RuntimeToolContextConfig(BaseModel):
    name: str
    value: str | None = None
    env: str | None = None
    vault_read: RuntimeVaultReadConfig | None = None

    @model_validator(mode="after")
    def validate_context(self) -> "RuntimeToolContextConfig":
        if not _ENV_NAME_RE.match(self.name):
            raise ValueError(f"invalid runtime.tool_context name: {self.name}")
        configured_sources = [
            self.value is not None,
            self.env is not None,
            self.vault_read is not None,
        ]
        if sum(configured_sources) != 1:
            raise ValueError(
                "runtime.tool_context entries must set exactly one of value, env, or vault_read"
            )
        if self.env is not None and not self.env.strip():
            raise ValueError("runtime.tool_context env must not be empty")
        return self


class RuntimeToolParamConfig(BaseModel):
    type: str
    description: str = ""

    @model_validator(mode="after")
    def validate_param(self) -> "RuntimeToolParamConfig":
        if self.type not in {"string", "number", "integer", "boolean"}:
            raise ValueError("runtime.tools params only support string, number, integer, boolean")
        return self


class RuntimeToolConfig(BaseModel):
    type: str = "shell"
    name: str
    description: str
    run: str
    params: dict[str, RuntimeToolParamConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_tool(self) -> "RuntimeToolConfig":
        if self.type != "shell":
            raise ValueError("runtime.tools only supports type='shell'")
        if not _TOOL_NAME_RE.match(self.name):
            raise ValueError(f"invalid runtime.tools name: {self.name}")
        if self.name.lower() in _BUILTIN_TOOL_NAMES:
            raise ValueError(f"runtime.tools name collides with built-in Claude tool: {self.name}")
        if not self.description.strip():
            raise ValueError("runtime.tools description must not be empty")
        if not self.run.strip():
            raise ValueError("runtime.tools run must not be empty")
        seen_env_names: set[str] = set()
        for key in self.params:
            normalized = "INPUT_" + re.sub(r"[^A-Z0-9]+", "_", key.upper()).strip("_")
            if not normalized or normalized == "INPUT_":
                raise ValueError(f"invalid runtime.tools param name: {key}")
            if normalized in seen_env_names:
                raise ValueError(
                    f"runtime.tools params collide after INPUT_* normalization: {key}"
                )
            seen_env_names.add(normalized)
        return self


class RuntimeConfig(BaseModel):
    pre_launch: list[RuntimeHookConfig] = Field(default_factory=list)
    tool_context: list[RuntimeToolContextConfig] = Field(default_factory=list)
    tools: list[RuntimeToolConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_runtime(self) -> "RuntimeConfig":
        names = [tool.name for tool in self.tools]
        if len(names) != len(set(names)):
            raise ValueError("runtime.tools names must be unique")
        context_names = [item.name for item in self.tool_context]
        if len(context_names) != len(set(context_names)):
            raise ValueError("runtime.tool_context names must be unique")
        return self


class ResponseConfig(BaseModel):
    channel: str = "email"


class SessionConfig(BaseModel):
    timeout_minutes: int = 60
    max_messages: int = 50
    idle_timeout_seconds: int = 60
    active_followup_policy: str = "interrupt_after_turn"

    @model_validator(mode="after")
    def validate_active_followup_policy(self) -> "SessionConfig":
        if self.active_followup_policy not in {
            "queue",
            "interrupt",
            "interrupt_after_turn",
            "reject",
        }:
            raise ValueError(
                "session.active_followup_policy must be one of "
                "'queue', 'interrupt', 'interrupt_after_turn', or 'reject'"
            )
        return self


class WorkflowConfig(BaseModel):
    name: str
    description: str = ""
    routing: RoutingConfig
    claude: ClaudeConfig
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    response: ResponseConfig = Field(default_factory=ResponseConfig)
    session: SessionConfig = Field(default_factory=SessionConfig)


class UpstreamModelPricingConfig(BaseModel):
    input_cost_per_million_tokens_usd: float
    output_cost_per_million_tokens_usd: float

    @model_validator(mode="after")
    def validate_pricing(self) -> "UpstreamModelPricingConfig":
        if self.input_cost_per_million_tokens_usd < 0:
            raise ValueError(
                "upstream_llm.model_pricing input_cost_per_million_tokens_usd must be >= 0"
            )
        if self.output_cost_per_million_tokens_usd < 0:
            raise ValueError(
                "upstream_llm.model_pricing output_cost_per_million_tokens_usd must be >= 0"
            )
        return self


class UpstreamLLMConfig(BaseModel):
    protocol: str = "anthropic"
    base_url: str = ""
    api_key_env: str = ""
    auth_mode: str | None = None
    model_pricing: dict[str, UpstreamModelPricingConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_upstream(self) -> "UpstreamLLMConfig":
        self.protocol = self.protocol.strip().lower()
        if self.protocol not in UPSTREAM_PROTOCOL_KINDS:
            raise ValueError(
                "upstream_llm.protocol must be one of "
                + ", ".join(repr(kind) for kind in UPSTREAM_PROTOCOL_KINDS)
            )
        self.base_url = self.base_url.strip()
        self.api_key_env = self.api_key_env.strip()
        if self.auth_mode is None:
            return self
        self.auth_mode = self.auth_mode.strip().lower()
        if self.auth_mode not in UPSTREAM_AUTH_MODES:
            raise ValueError(
                "upstream_llm.auth_mode must be one of "
                + ", ".join(repr(mode) for mode in UPSTREAM_AUTH_MODES)
            )
        normalized_model_pricing: dict[str, UpstreamModelPricingConfig] = {}
        for model_name, pricing in self.model_pricing.items():
            normalized_name = model_name.strip()
            if not normalized_name:
                raise ValueError("upstream_llm.model_pricing keys must not be empty")
            normalized_model_pricing[normalized_name] = pricing
        self.model_pricing = normalized_model_pricing
        return self


class StartupConfig(BaseModel):
    config_dir: str = "config/workflows"
    db_path: str = "claudius.db"
    workspaces_path: str = "/workspaces"
    image: str = "claudius:latest"
    docker_probe_mode: str = "host_port"
    host: str = "0.0.0.0"
    port: int = 8000
    callback_url: str = ""
    attachments: str = "file:///tmp/claudius-attachments"
    log_conversation: bool = False
    upstream_llm: UpstreamLLMConfig = Field(default_factory=UpstreamLLMConfig)

    @model_validator(mode="after")
    def validate_startup(self) -> "StartupConfig":
        self.config_dir = self.config_dir.strip()
        self.db_path = self.db_path.strip()
        self.workspaces_path = self.workspaces_path.strip()
        self.image = self.image.strip()
        self.docker_probe_mode = self.docker_probe_mode.strip().lower()
        self.host = self.host.strip()
        self.callback_url = self.callback_url.strip()
        self.attachments = self.attachments.strip()
        if self.docker_probe_mode not in DOCKER_PROBE_MODES:
            raise ValueError(
                "docker_probe_mode must be one of "
                + ", ".join(repr(mode) for mode in DOCKER_PROBE_MODES)
            )
        return self
