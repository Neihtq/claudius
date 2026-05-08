from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal


class SessionState(str, Enum):
    NEW = "new"
    ACTIVE = "active"
    WAITING = "waiting"
    HIBERNATED = "hibernated"
    CLOSED = "closed"
    ERROR = "error"


class ExecutionPhase(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class ContainerLifecycleStatus(str, Enum):
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    EXITED = "exited"
    FAILED = "failed"


@dataclass
class ExecutionContainer:
    name: str
    status: ContainerLifecycleStatus
    healthy: bool = False
    host: str | None = None
    port: int | None = None
    probe_mode: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "ExecutionContainer | None":
        if not payload:
            return None
        return cls(
            name=str(payload["name"]),
            status=ContainerLifecycleStatus(str(payload["status"])),
            healthy=bool(payload.get("healthy", False)),
            host=payload.get("host"),
            port=int(payload["port"]) if payload.get("port") is not None else None,
            probe_mode=payload.get("probe_mode"),
        )


@dataclass
class ClaudeSummary:
    executions_count: int = 0
    completed_executions_count: int = 0
    num_turns: int = 0
    duration_ms: int = 0
    total_cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    total_tokens: int = 0


@dataclass
class Attachment:
    filename: str
    content_type: str
    data: bytes


@dataclass
class InboundMessage:
    channel: str
    sender: str
    recipients: list[str]
    thread_id: str
    subject: str | None
    body: str
    attachments: list[Attachment]
    received_at: datetime


@dataclass
class Session:
    session_id: str
    thread_id: str
    channel: str
    workflow_name: str
    state: SessionState
    workspace_path: str
    created_at: datetime
    last_message_at: datetime
    last_execution_result: str | None = None  # 'ok' | 'failed' | None
    claude_summary: ClaudeSummary | None = None
    channel_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Execution:
    execution_id: str
    session_id: str
    worker_address: str | None
    started_at: datetime
    halted_at: datetime | None
    halt_reason: str | None
    phase: ExecutionPhase = ExecutionPhase.STARTING
    runtime_container: ExecutionContainer | None = None
    worker_container: ExecutionContainer | None = None
    exit_code: int | None = None
    claude_session_id: str | None = None
    claude_num_turns: int | None = None
    claude_duration_ms: int | None = None
    claude_total_cost_usd: float | None = None
    claude_input_tokens: int | None = None
    claude_output_tokens: int | None = None
    claude_cache_creation_input_tokens: int | None = None
    claude_cache_read_input_tokens: int | None = None
    claude_total_tokens: int | None = None
    agent_error_category: str | None = None
    agent_error_reason: str | None = None


@dataclass
class LogLine:
    logged_at: datetime
    stream: Literal['stdout', 'stderr']
    body: str


@dataclass
class ToolMount:
    env_vars: dict[str, str] = field(default_factory=dict)
    volumes: list[dict[str, str]] = field(default_factory=list)
    sidecars: list[dict[str, Any]] = field(default_factory=list)
    pre_launch_commands: list[dict[str, Any]] = field(default_factory=list)
