"""Backend that drives a pre-created, long-lived session "pod" over HTTP.

Unlike :class:`~claudius.controller.backends.docker.DockerBackend`, this backend
does not spawn containers and never imports docker. It targets a single
pre-created session pod (deployed alongside the controller, e.g. by Rise) that
runs both the session worker and a co-located runtime sidecar. The controller
pushes the per-execution configuration to the pod's ``/configure`` endpoint; the
pod resolves its own runtime, writes its MCP bridge files locally, runs the
agent, and exposes ``/status`` for the controller to observe completion.

Only one exposed port per pod is required (the worker on ``:8080``); the runtime
sidecar lives on ``localhost:8090`` inside the same pod and is reached by the
worker, never by the controller.
"""

import os
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from urllib.parse import urlsplit

import asyncio
import httpx
from loguru import logger

from claudius.config.schema import WorkflowConfig
from claudius.controller.backends.base import AbstractBackend, ExecutionStartupError
from claudius.models import (
    ContainerLifecycleStatus,
    Execution,
    ExecutionContainer,
    ExecutionPhase,
    LogLine,
    Session,
    ToolMount,
)

_CONFIGURE_TIMEOUT_SECONDS = 30.0
_STATUS_POLL_INTERVAL_SECONDS = 1.0
_SHUTDOWN_TIMEOUT_SECONDS = 10.0
_TERMINAL_STATES = {"finished", "failed"}


class StaticBackend(AbstractBackend):
    def __init__(
        self,
        session_endpoint: str = "",
        configure_token: str = "",
    ):
        endpoint = (session_endpoint or os.environ.get("CLAUDIUS_SESSION_ENDPOINT", "")).strip()
        if not endpoint:
            host = os.environ.get("RISE_CONTAINER_HOST__SESSION", "").strip()
            endpoint = f"http://{host}" if host else "http://localhost:8080"
        if "://" not in endpoint:
            endpoint = f"http://{endpoint}"
        self._endpoint = endpoint.rstrip("/")
        self._configure_token = (
            configure_token or os.environ.get("CLAUDIUS_CONFIGURE_TOKEN", "")
        ).strip()
        self._exit_codes: dict[str, int | None] = {}
        logger.info("static backend targeting session endpoint {}", self._endpoint)

    def prepares_runtime_in_controller(self) -> bool:
        return False

    async def wait_until_ready(self, timeout_seconds: float = 120.0) -> bool:
        """Poll the session pod's /health until it responds, up to a timeout.

        Returns True if the pod became reachable, False on timeout. Used before
        auto-starting a session so the first /configure doesn't race the pod boot.
        """
        elapsed = 0.0
        interval = 1.0
        while elapsed < timeout_seconds:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(f"{self._endpoint}/health")
                if response.status_code == 200:
                    return True
            except httpx.HTTPError:
                pass
            await asyncio.sleep(interval)
            elapsed += interval
        logger.warning("session pod at {} not ready after {}s", self._endpoint, timeout_seconds)
        return False

    @property
    def _worker_address(self) -> str:
        split = urlsplit(self._endpoint)
        return split.netloc or split.path

    def _auth_headers(self) -> dict[str, str]:
        if self._configure_token:
            return {"Authorization": f"Bearer {self._configure_token}"}
        return {}

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
        worker_env: dict[str, str] = dict(extra_env or {})
        for mount in tool_mounts:
            worker_env.update(mount.env_vars)

        payload = {
            "execution_id": execution_id,
            "session_id": session.session_id,
            "worker_env": worker_env,
        }
        try:
            async with httpx.AsyncClient(timeout=_CONFIGURE_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"{self._endpoint}/configure",
                    json=payload,
                    headers=self._auth_headers(),
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text.strip() or str(exc)
            raise ExecutionStartupError(
                f"session pod rejected configure: {detail}", log_lines=[detail]
            ) from exc
        except httpx.HTTPError as exc:
            raise ExecutionStartupError(
                f"session pod unreachable at {self._endpoint}: {exc}"
            ) from exc

        self._exit_codes[execution_id or ""] = None
        has_runtime = bool(workflow.runtime.tools or workflow.runtime.pre_launch)
        runtime_container = None
        if has_runtime:
            runtime_container = ExecutionContainer(
                name=f"claudius-runtime-{session.session_id[:8]}",
                status=ContainerLifecycleStatus.RUNNING,
                healthy=True,
            )
        return Execution(
            execution_id=execution_id or "",
            session_id=session.session_id,
            worker_address=self._worker_address,
            started_at=started_at or datetime.now(timezone.utc),
            halted_at=None,
            halt_reason=None,
            phase=ExecutionPhase.RUNNING,
            runtime_container=runtime_container,
            worker_container=ExecutionContainer(
                name=f"claudius-session-{session.session_id[:8]}",
                status=ContainerLifecycleStatus.RUNNING,
                healthy=True,
                host=urlsplit(self._endpoint).hostname,
                port=urlsplit(self._endpoint).port,
            ),
        )

    async def get_exit_code(self, execution: Execution) -> int | None:
        return self._exit_codes.get(execution.execution_id)

    async def delete_execution(self, execution: Execution) -> None:
        # Rise owns the pod lifecycle; we only ask the worker to stop the current
        # run so the pod returns to idle. The pod itself is never removed.
        try:
            async with httpx.AsyncClient(timeout=_SHUTDOWN_TIMEOUT_SECONDS) as client:
                await client.post(
                    f"{self._endpoint}/shutdown",
                    headers=self._auth_headers(),
                )
        except httpx.HTTPError as exc:
            logger.info(
                "static backend shutdown request failed session_id={} error={}",
                execution.session_id,
                exc,
            )
        self._exit_codes.pop(execution.execution_id, None)

    async def tail_logs(
        self, execution: Execution, since: datetime | None = None
    ) -> AsyncIterator[LogLine]:
        log_offset = 0
        while True:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(
                        f"{self._endpoint}/status",
                        params={"log_offset": log_offset},
                        headers=self._auth_headers(),
                    )
                response.raise_for_status()
                status = response.json()
            except httpx.HTTPError as exc:
                logger.warning(
                    "static backend status poll failed session_id={} error={}",
                    execution.session_id,
                    exc,
                )
                await asyncio.sleep(_STATUS_POLL_INTERVAL_SECONDS)
                continue

            for line in status.get("logs", []) or []:
                yield LogLine(
                    logged_at=datetime.now(timezone.utc),
                    stream="stderr" if line.get("stream") == "stderr" else "stdout",
                    body=f"[worker] {line.get('body', '')}",
                )
            log_offset = status.get("log_offset", log_offset)

            current = status.get("execution_id")
            state = status.get("state")
            if current == execution.execution_id and state in _TERMINAL_STATES:
                # Final drain: catch any lines emitted between the status read above
                # and the execution reaching its terminal state.
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        final = (
                            await client.get(
                                f"{self._endpoint}/status",
                                params={"log_offset": log_offset},
                                headers=self._auth_headers(),
                            )
                        ).json()
                    for line in final.get("logs", []) or []:
                        yield LogLine(
                            logged_at=datetime.now(timezone.utc),
                            stream="stderr" if line.get("stream") == "stderr" else "stdout",
                            body=f"[worker] {line.get('body', '')}",
                        )
                except httpx.HTTPError:
                    pass
                self._exit_codes[execution.execution_id] = status.get("exit_code")
                return
            if current != execution.execution_id and current is not None:
                # The pod moved on to a different execution; ours is done.
                return
            await asyncio.sleep(_STATUS_POLL_INTERVAL_SECONDS)
