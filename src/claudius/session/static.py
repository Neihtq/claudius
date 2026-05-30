"""Long-lived "session pod" for pre-created (static) deployments.

In a static deployment (e.g. Rise), the session worker and its runtime sidecar
are scheduled once, ahead of time, instead of being spawned per-execution by the
controller. This module runs both in a single process:

* a worker API on ``--port`` (default 8080), exposed to the controller, that
  accepts ``POST /configure`` to start one execution and exposes ``/status`` for
  the controller to observe completion; and
* the runtime sidecar API on ``127.0.0.1:--runtime-port`` (default 8090), reached
  only by the in-workspace MCP bridge.

Because both share one process and one filesystem, the worker resolves the
workflow's runtime locally, writes its own MCP bridge files (pointing the bridge
at ``localhost:8090``), runs pre-launch hooks in-process, then runs Claude Code.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Header, HTTPException
from loguru import logger
from pydantic import BaseModel

from claudius.config.schema import WorkflowConfig
from claudius.config.template import TemplateContext
from claudius.models import InboundMessage
from claudius.runtime import (
    EnvironmentSecretProvider,
    build_runtime_sidecar_payload,
    new_runtime_auth_token,
    resolve_runtime,
    write_runtime_bridge_files,
)
from claudius.runtime_sidecar import RuntimeSidecar, create_app as create_runtime_app
from claudius.session.runner import SessionRunner


@dataclass
class _PodState:
    state: str = "idle"  # idle | starting | running | finished | failed
    execution_id: str | None = None
    exit_code: int | None = None
    runner: SessionRunner | None = None
    task: asyncio.Task | None = None
    detail: str = ""


class ConfigureRequest(BaseModel):
    session_id: str
    worker_env: dict[str, str]
    execution_id: str | None = None


def _require_token(authorization: str | None, expected: str) -> None:
    if not expected:
        return
    token = (authorization or "").removeprefix("Bearer ").strip()
    if token != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


def _initial_message_from_env(env: dict[str, str]) -> InboundMessage:
    data = json.loads(env["CLAUDIUS_INITIAL_MESSAGE"])
    return InboundMessage(
        channel=data["channel"],
        sender=data["sender"],
        recipients=list(data.get("recipients") or []),
        thread_id=data["thread_id"],
        subject=data.get("subject"),
        body=data["body"],
        attachments=[],
        received_at=datetime.fromisoformat(data["received_at"]),
    )


def _prepare_runtime(
    workflow: WorkflowConfig,
    message: InboundMessage,
    session_id: str,
    workspace_path: str,
    sidecar: RuntimeSidecar,
) -> list[str]:
    """Resolve the workflow runtime locally, configure the in-process sidecar, and
    write the MCP bridge config (merging any external mcp_servers). Returns the
    hook phases that still need to run."""
    resolved = resolve_runtime(
        workflow.runtime,
        template_ctx=TemplateContext(
            request=message,
            session_id=session_id,
            channel_name=message.channel,
        ),
        secret_provider=EnvironmentSecretProvider(),
    )
    extra_mcp_servers = {
        name: server.to_claude_config() for name, server in workflow.mcp_servers.items()
    }

    callback_url = os.environ.get("CLAUDIUS_CALLBACK_URL", "")
    callback_token = os.environ.get("CLAUDIUS_SESSION_TOKEN", "")

    if resolved.has_tools():
        auth_token = new_runtime_auth_token()
        sidecar.configure(build_runtime_sidecar_payload(resolved, auth_token))
        _, mcp_config_path = write_runtime_bridge_files(
            workspace_path,
            spec=resolved,
            endpoint_url="http://localhost:8090",
            auth_token=auth_token,
            callback_url=callback_url,
            callback_token=callback_token,
            session_id=session_id,
            extra_mcp_servers=extra_mcp_servers,
        )
        os.environ["CLAUDIUS_MCP_CONFIG"] = f"/workspace/.claudius-runtime/{mcp_config_path.name}"
    else:
        # No runtime tools: still load hooks/context into the sidecar so they can
        # run, and write an MCP config containing only the external servers.
        sidecar.configure(build_runtime_sidecar_payload(resolved, new_runtime_auth_token()))
        if extra_mcp_servers:
            from claudius.runtime import write_external_mcp_config

            mcp_config_path = write_external_mcp_config(workspace_path, extra_mcp_servers)
            os.environ["CLAUDIUS_MCP_CONFIG"] = (
                f"/workspace/.claudius-runtime/{mcp_config_path.name}"
            )

    phases = []
    raw_phases = os.environ.get("CLAUDIUS_RUNTIME_HOOK_PHASES", "")
    if raw_phases:
        try:
            phases = list(json.loads(raw_phases))
        except json.JSONDecodeError:
            phases = []
    return phases


def create_pod_app(
    sidecar: RuntimeSidecar,
    *,
    workspace_path: str,
    configure_token: str = "",
) -> FastAPI:
    app = FastAPI()
    state = _PodState()

    async def _run_execution(req: ConfigureRequest) -> None:
        try:
            # Surface the controller-pushed environment to this process and the
            # Claude Code subprocess it will spawn.
            os.environ.update(req.worker_env)
            Path(workspace_path).mkdir(parents=True, exist_ok=True)
            home_dir = os.environ.get("HOME", "")
            if home_dir:
                Path(home_dir).mkdir(parents=True, exist_ok=True)
            output_dir = os.environ.get("CLAUDIUS_OUTPUT_DIR", "")
            if output_dir:
                Path(output_dir).mkdir(parents=True, exist_ok=True)
            workflow = WorkflowConfig.model_validate_json(req.worker_env["CLAUDIUS_WORKFLOW"])
            message = _initial_message_from_env(req.worker_env)

            phases = _prepare_runtime(workflow, message, req.session_id, workspace_path, sidecar)
            if phases:
                await sidecar.run_hooks(phases)

            callback_url = os.environ.get("CLAUDIUS_CALLBACK_URL", "")
            from claudius.channels.dev import DevChannel

            channel = DevChannel(callback_url=callback_url, session_id=req.session_id)
            runner = SessionRunner(
                workflow=workflow,
                initial_message=message,
                channel=channel,
                workspace_path=workspace_path,
                idle_timeout=float(workflow.session.idle_timeout_seconds),
                conversation_text=os.environ.get("CLAUDIUS_CONVERSATION_TEXT", ""),
                log_conversation=bool(os.environ.get("CLAUDIUS_LOG_CONVERSATION")),
                callback_url=callback_url,
                session_id=req.session_id,
                session_token=os.environ.get("CLAUDIUS_SESSION_TOKEN", ""),
                claude_resume_session_id=os.environ.get("CLAUDIUS_CLAUDE_RESUME_SESSION_ID") or None,
            )
            state.runner = runner
            state.state = "running"
            logger.info("session pod execution running execution_id={}", req.execution_id)
            await runner.run()
            state.state = "finished"
            state.exit_code = 0
            logger.info("session pod execution finished execution_id={}", req.execution_id)
        except asyncio.CancelledError:
            state.state = "failed"
            state.exit_code = 1
            state.detail = "cancelled"
            raise
        except Exception as exc:
            state.state = "failed"
            state.exit_code = 1
            state.detail = str(exc)
            logger.exception("session pod execution failed execution_id={} error={}", req.execution_id, exc)
        finally:
            state.runner = None

    @app.get("/health")
    async def health():
        return {"status": "ok", "state": state.state}

    @app.post("/configure")
    async def configure(req: ConfigureRequest, authorization: str | None = Header(default=None)):
        _require_token(authorization, configure_token)
        if state.state in ("starting", "running"):
            raise HTTPException(status_code=409, detail="session pod is busy")
        state.state = "starting"
        state.execution_id = req.execution_id
        state.exit_code = None
        state.detail = ""
        state.task = asyncio.create_task(_run_execution(req))
        return {"status": "accepted", "execution_id": req.execution_id}

    @app.get("/status")
    async def status(authorization: str | None = Header(default=None)):
        _require_token(authorization, configure_token)
        return {
            "state": state.state,
            "execution_id": state.execution_id,
            "exit_code": state.exit_code,
            "detail": state.detail,
            "logs": [],
        }

    @app.post("/shutdown")
    async def shutdown(authorization: str | None = Header(default=None)):
        _require_token(authorization, configure_token)
        runner = state.runner
        if runner is not None:
            await runner.graceful_stop()
        return {"status": "stopping"}

    return app


def run_session_pod(
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
    runtime_host: str = "127.0.0.1",
    runtime_port: int = 8090,
    workspace_path: str | None = None,
) -> None:
    workspace_path = workspace_path or os.environ.get("CLAUDIUS_WORKSPACE_PATH", "/workspace")
    Path(workspace_path).mkdir(parents=True, exist_ok=True)
    configure_token = os.environ.get("CLAUDIUS_CONFIGURE_TOKEN", "").strip()

    # One shared, initially-empty sidecar object: the worker mutates it on
    # /configure and runs hooks in-process; the :8090 HTTP app serves /invoke for
    # the MCP bridge against the same object.
    sidecar = RuntimeSidecar.empty()
    runtime_app = create_runtime_app(sidecar, startup_phases=[])
    pod_app = create_pod_app(sidecar, workspace_path=workspace_path, configure_token=configure_token)

    async def _serve() -> None:
        runtime_server = uvicorn.Server(
            uvicorn.Config(runtime_app, host=runtime_host, port=runtime_port, log_level="warning")
        )
        pod_server = uvicorn.Server(
            uvicorn.Config(pod_app, host=host, port=port, log_level="info")
        )
        logger.info(
            "session pod listening worker={}:{} runtime={}:{} workspace={}",
            host,
            port,
            runtime_host,
            runtime_port,
            workspace_path,
        )
        await asyncio.gather(runtime_server.serve(), pod_server.serve())

    asyncio.run(_serve())
