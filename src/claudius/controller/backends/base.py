from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime

from claudius.config.schema import WorkflowConfig
from claudius.models import Execution, LogLine, Session, ToolMount


class ExecutionStartupError(RuntimeError):
    def __init__(self, message: str, *, log_lines: list[str] | None = None):
        super().__init__(message)
        self.log_lines = log_lines or []


class AbstractBackend(ABC):
    def prepares_runtime_in_controller(self) -> bool:
        """Whether the controller resolves runtime tools/hooks and writes the MCP
        bridge files before launching the worker.

        Docker-style backends share the workspace volume with the worker, so the
        controller prepares the runtime. Backends that drive a pre-created,
        separately-scheduled worker (no shared filesystem) return False; the worker
        resolves its own runtime and writes its bridge files locally instead.
        """
        return True

    @abstractmethod
    async def create_execution(
        self,
        session: Session,
        workflow: WorkflowConfig,
        tool_mounts: list[ToolMount],
        extra_env: dict[str, str] | None = None,
        *,
        execution_id: str | None = None,
        started_at: datetime | None = None,
    ) -> Execution:
        """Launch a new execution container/pod for the given session."""

    @abstractmethod
    async def delete_execution(self, execution: Execution) -> None:
        """Stop and remove the execution container/pod."""

    async def get_exit_code(self, execution: Execution) -> int | None:
        """Return the container/pod exit code after it has stopped, or None if unavailable."""
        return None

    def tail_logs(
        self, execution: Execution, since: datetime | None = None
    ) -> AsyncIterator[LogLine]:
        """Async generator yielding LogLine instances from the execution's container/pod.

        Implementations must override this as `async def tail_logs(...): yield ...`.
        Pass `since` to resume from a previous position after a backend restart.
        """
        raise NotImplementedError

    async def tail_startup_logs(
        self, execution: Execution, since: datetime | None = None
    ) -> AsyncIterator[LogLine]:
        if False:
            yield
