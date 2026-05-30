import asyncio
import json
import os
import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path

import click
import httpx
import uvicorn
from click.core import ParameterSource
from loguru import logger

from claudius.controller.proxy import build_proxy_upstream


_SERVE_PROXY_PROTOCOL_CHOICES = ["anthropic", "openai"]


def _collect_explicit_serve_overrides(ctx: click.Context, option_values: dict[str, object]) -> dict[str, object]:
    overrides: dict[str, object] = {}
    for key, value in option_values.items():
        if ctx.get_parameter_source(key) == ParameterSource.COMMANDLINE:
            overrides[key] = value
    return overrides


def _validate_workflow_models_against_upstream_pricing(workflows, pricing: dict[str, object]) -> None:
    if not pricing:
        return
    unknown_models = sorted({workflow.claude.model for workflow in workflows if workflow.claude.model not in pricing})
    if not unknown_models:
        return
    raise click.ClickException(
        "Unknown workflow model(s) for upstream_llm.model_pricing: "
        + ", ".join(repr(model) for model in unknown_models)
    )


@click.group()
def cli():
    """Claudius — message-driven agentic platform."""


async def _autostart_sessions(manager, workflows, backend) -> None:
    """Seed one session per autostart workflow on boot, if none exists yet.

    For always-on agents (e.g. a Twitch live-coding loop) that aren't driven by
    inbound messages, this removes the need to manually POST /dev/inject after a
    controller (re)start. Failed sessions are not re-seeded automatically — that's
    left to the perpetual-restart guard or a manual inject — to avoid crash loops.
    """
    from claudius.models import InboundMessage, SessionState

    autostart = [w for w in workflows if w.session.autostart]
    if not autostart:
        return

    # For a static backend, wait for the pre-created session pod to be reachable
    # so the first /configure doesn't race the pod's boot.
    wait_ready = getattr(backend, "wait_until_ready", None)
    if wait_ready is not None:
        await wait_ready()

    for wf in autostart:
        try:
            sessions = await manager.list_sessions()
        except Exception as exc:
            logger.warning("autostart: failed to list sessions: {}", exc)
            return
        if any(
            s.workflow_name == wf.name and s.state != SessionState.CLOSED
            for s in sessions
        ):
            logger.info("autostart: workflow {} already has a session; skipping", wf.name)
            continue
        channel = wf.routing.channels[0] if wf.routing.channels else "dev"
        message = InboundMessage(
            channel=channel,
            sender="autostart@claudius",
            recipients=[],
            thread_id=str(uuid.uuid4()),
            subject=None,
            body=wf.session.autostart_prompt,
            attachments=[],
            received_at=datetime.now(timezone.utc),
        )
        logger.info("autostart: seeding session for workflow {} on channel {}", wf.name, channel)
        try:
            await manager.handle_message(message)
        except Exception as exc:
            logger.warning("autostart: failed to seed workflow {}: {}", wf.name, exc)


@cli.command()
@click.pass_context
@click.option("--config-dir", default="config/workflows", show_default=True)
@click.option("--db-path", default="claudius.db", show_default=True)
@click.option("--workspaces-path", default="/workspaces", show_default=True)
@click.option("--image", default="claudius:latest", show_default=True)
@click.option(
    "--backend",
    type=click.Choice(["docker", "static"], case_sensitive=False),
    default="docker",
    show_default=True,
    help="Execution backend: 'docker' spawns containers; 'static' drives a pre-created session pod.",
)
@click.option(
    "--session-endpoint",
    default="",
    help="Static backend: base URL of the pre-created session pod (defaults to "
    "$CLAUDIUS_SESSION_ENDPOINT or $RISE_CONTAINER_HOST__SESSION).",
)
@click.option(
    "--docker-probe-mode",
    type=click.Choice(["host_port", "container_ip"], case_sensitive=False),
    default="host_port",
    show_default=True,
    help="How the controller reaches runtime and worker containers.",
)
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
@click.option("--callback-url", default="", show_default=True,
              help="URL of this controller reachable by worker containers (enables DevChannel).")
@click.option("--attachments", default="file:///tmp/claudius-attachments", show_default=True,
              help="Attachment store URI: file:///path or s3://bucket/prefix.")
@click.option("--log-conversation", is_flag=True, default=False,
              help="Log fetched conversation and messages/responses in worker containers.")
@click.option(
    "--proxy-upstream-kind",
    type=click.Choice(_SERVE_PROXY_PROTOCOL_CHOICES, case_sensitive=False),
    default="anthropic",
    show_default=True,
    help="Which upstream protocol adapter the controller proxy uses.",
)
@click.option(
    "--proxy-upstream-url",
    default="",
    help="Override the upstream base URL. Defaults depend on --proxy-upstream-kind.",
)
@click.option(
    "--proxy-upstream-api-key-env",
    default="",
    help="Environment variable name containing the upstream API key. Defaults depend on --proxy-upstream-kind.",
)
def serve(
    ctx,
    config_dir,
    db_path,
    workspaces_path,
    image,
    backend,
    session_endpoint,
    docker_probe_mode,
    host,
    port,
    callback_url,
    attachments,
    log_conversation,
    proxy_upstream_kind,
    proxy_upstream_url,
    proxy_upstream_api_key_env,
):
    """Start the Claudius controller."""
    from claudius.channels.resend import ResendChannel
    from claudius.config.loader import (
        load_startup_config,
        load_workflows_with_defaults,
        resolve_startup_config,
    )
    from claudius.controller.attachments import create_store
    from claudius.controller.db import Database
    from claudius.controller.server import create_controller_app
    from claudius.controller.session_manager import SessionManager
    from claudius.controller.sse import SSEBroker

    startup_config_path = Path("config/claudius.yaml")
    startup_config = load_startup_config(startup_config_path)
    serve_config = resolve_startup_config(
        startup_config,
        _collect_explicit_serve_overrides(ctx, {
            "config_dir": config_dir,
            "db_path": db_path,
            "workspaces_path": workspaces_path,
            "image": image,
            "backend": backend,
            "session_endpoint": session_endpoint,
            "docker_probe_mode": docker_probe_mode,
            "host": host,
            "port": port,
            "callback_url": callback_url,
            "attachments": attachments,
            "log_conversation": log_conversation,
            "proxy_upstream_kind": proxy_upstream_kind,
            "proxy_upstream_url": proxy_upstream_url,
            "proxy_upstream_api_key_env": proxy_upstream_api_key_env,
        }),
    )

    workflows = load_workflows_with_defaults(Path(serve_config.config_dir), serve_config)
    _validate_workflow_models_against_upstream_pricing(
        workflows,
        serve_config.upstream_llm.model_pricing,
    )
    db_path = serve_config.db_path
    workspaces_path = str(Path(serve_config.workspaces_path).expanduser().resolve())
    image = serve_config.image
    docker_probe_mode = serve_config.docker_probe_mode
    host = serve_config.host
    port = serve_config.port
    # Allow deployments (e.g. Rise) to inject the externally-reachable controller
    # URL via env when it isn't known at config-authoring time. As a last resort,
    # derive it from the Rise-injected host of this controller container.
    callback_url = serve_config.callback_url or os.environ.get("CLAUDIUS_CALLBACK_URL", "")
    if not callback_url:
        rise_self_host = os.environ.get("RISE_CONTAINER_HOST__CLAUDIUS", "").strip()
        if rise_self_host:
            callback_url = f"http://{rise_self_host}"
    attachments = serve_config.attachments
    log_conversation = serve_config.log_conversation
    proxy_upstream_kind = serve_config.upstream_llm.protocol
    proxy_upstream_url = serve_config.upstream_llm.base_url
    proxy_upstream_api_key_env = serve_config.upstream_llm.api_key_env
    upstream_api_key = os.environ.get(proxy_upstream_api_key_env, "") if proxy_upstream_api_key_env else ""
    proxy_upstream = build_proxy_upstream(
        kind=proxy_upstream_kind,
        base_url=proxy_upstream_url,
        api_key=upstream_api_key,
        auth_mode=serve_config.upstream_llm.auth_mode,
        model_pricing={
            model_name: pricing.model_dump()
            for model_name, pricing in serve_config.upstream_llm.model_pricing.items()
        },
    )

    email_channel_config = serve_config.channels.email
    resend_provider = serve_config.providers.resend
    if email_channel_config.provider != "resend":
        raise click.ClickException(
            f"Unsupported channels.email.provider: {email_channel_config.provider!r}"
        )
    resend_api_key = os.environ.get(resend_provider.api_key_env, "")
    resend_from = email_channel_config.from_address
    channel = ResendChannel(api_key=resend_api_key, from_address=resend_from)
    channels = {"email": channel}
    inbound_webhooks = {resend_provider.webhook_path: "email"}

    backend_kind = serve_config.backend
    if backend_kind == "static":
        from claudius.controller.backends.static import StaticBackend
        backend = StaticBackend(session_endpoint=serve_config.session_endpoint)
    else:
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image=image,
            workspaces_path=workspaces_path,
            probe_mode=docker_probe_mode.lower(),
        )
    broker = SSEBroker()

    async def run():
        db = Database(db_path)
        await db.init()
        proxy_secret = os.environ.get("CLAUDIUS_PROXY_SECRET", "")
        if callback_url and not proxy_secret:
            proxy_secret = secrets.token_hex(32)
        attachment_store = create_store(attachments) if attachments else None

        logger.info("--- Claudius controller starting ---")
        logger.info(f"  db            {db_path}")
        logger.info(
            f"  global config {startup_config_path if startup_config_path.exists() else '(none)'}"
        )
        logger.info(f"  workflows dir {serve_config.config_dir}")
        logger.info(f"  workspaces    {workspaces_path}")
        logger.info(f"  backend       {backend_kind}")
        logger.info(f"  docker image  {image}")
        logger.info(f"  probe mode    {docker_probe_mode}")
        logger.info(f"  listen        {host}:{port}")
        logger.info(f"  callback url  {callback_url or '(none)'}")
        logger.info(f"  api proxy     {'enabled' if proxy_secret else 'disabled'}")
        logger.info(f"  proxy kind    {proxy_upstream.kind}")
        logger.info(f"  proxy target  {proxy_upstream.base_url}")
        logger.info(f"  proxy key env {proxy_upstream_api_key_env or '(none)'}")
        logger.info(f"  attachments   {attachment_store.describe() if attachment_store else '(none)'}")
        logger.info(f"  email channel {email_channel_config.provider}")
        logger.info(f"  resend key    {resend_provider.api_key_env}")
        logger.info(f"  email from    {resend_from}")
        logger.info(f"  webhook path  {resend_provider.webhook_path}")
        logger.info(f"  workflows     {', '.join(w.name for w in workflows) or '(none)'}")

        manager = SessionManager(
            db=db,
            backend=backend,
            workflows=workflows,
            workspaces_path=workspaces_path,
            channels=channels,
            broker=broker,
            callback_url=callback_url,
            proxy_secret=proxy_secret,
            attachment_store=attachment_store,
            log_conversation=log_conversation,
            resend_api_key=resend_api_key,
            resend_from_address=resend_from,
        )
        await manager.recover()
        app = create_controller_app(
            session_manager=manager,
            channels=channels,
            broker=broker,
            inbound_webhooks=inbound_webhooks,
            proxy_secret=proxy_secret,
            proxy_upstream=proxy_upstream,
            attachment_store=attachment_store,
            ui_admin_secret=os.environ.get("CLAUDIUS_UI_ADMIN_SECRET", ""),
        )
        config = uvicorn.Config(app, host=host, port=port, log_level="info", timeout_graceful_shutdown=3)
        server = uvicorn.Server(config)

        async def _close_broker_on_exit():
            while not server.should_exit:
                await asyncio.sleep(0.05)
            broker.close()

        watcher = asyncio.create_task(_close_broker_on_exit())
        autostart_task = asyncio.create_task(_autostart_sessions(manager, workflows, backend))
        try:
            await server.serve()
        finally:
            watcher.cancel()
            autostart_task.cancel()
            await manager.shutdown()
            await db.close()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


@cli.command()
@click.argument("session_id")
@click.option("--workspaces-path", default="/workspaces", show_default=True)
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8080, show_default=True, type=int)
@click.option("--idle-timeout", default=None, type=float,
              help="Seconds to stay alive after last response. Overrides workflow config.")
def session(session_id, workspaces_path, host, port, idle_timeout):
    """Start a session worker for SESSION_ID."""
    from claudius.config.schema import WorkflowConfig
    from claudius.models import InboundMessage
    from claudius.session.runner import SessionRunner
    from claudius.session.server import create_session_app

    workspaces_path = str(Path(workspaces_path).expanduser().resolve())
    workflow_json = os.environ["CLAUDIUS_WORKFLOW"]
    message_json = os.environ["CLAUDIUS_INITIAL_MESSAGE"]
    callback_url = os.environ.get("CLAUDIUS_CALLBACK_URL", "")
    conversation_text = os.environ.get("CLAUDIUS_CONVERSATION_TEXT", "")
    session_token = os.environ.get("CLAUDIUS_SESSION_TOKEN", "")
    claude_resume_session_id = os.environ.get("CLAUDIUS_CLAUDE_RESUME_SESSION_ID") or None

    workflow = WorkflowConfig.model_validate_json(workflow_json)
    message_data = json.loads(message_json)
    initial_message = InboundMessage(
        channel=message_data["channel"],
        sender=message_data["sender"],
        recipients=list(message_data.get("recipients") or []),
        thread_id=message_data["thread_id"],
        subject=message_data.get("subject"),
        body=message_data["body"],
        attachments=[],
        received_at=datetime.fromisoformat(message_data["received_at"]),
    )

    log_conversation = bool(os.environ.get("CLAUDIUS_LOG_CONVERSATION"))

    if callback_url:
        from claudius.channels.dev import DevChannel
        channel = DevChannel(callback_url=callback_url, session_id=session_id)
    else:
        from claudius.channels.resend import ResendChannel
        resend_api_key = os.environ["RESEND_API_KEY"]
        resend_from = os.environ["RESEND_FROM_ADDRESS"]
        channel = ResendChannel(api_key=resend_api_key, from_address=resend_from)

    workspace_path = os.environ.get("CLAUDIUS_WORKSPACE_PATH", "/workspace")

    async def run():
        runner = SessionRunner(
            workflow=workflow,
            initial_message=initial_message,
            channel=channel,
            workspace_path=workspace_path,
            idle_timeout=idle_timeout if idle_timeout is not None else float(workflow.session.idle_timeout_seconds),
            conversation_text=conversation_text,
            log_conversation=log_conversation,
            callback_url=callback_url,
            session_id=session_id,
            session_token=session_token,
            claude_resume_session_id=claude_resume_session_id,
        )
        app = create_session_app(runner, session_id, session_token=session_token)
        config = uvicorn.Config(app, host=host, port=port, log_level="warning", timeout_graceful_shutdown=3)
        server = uvicorn.Server(config)
        server_task = asyncio.create_task(server.serve())
        try:
            while not server.started and not server_task.done():
                await asyncio.sleep(0.01)
            await runner.run()
        finally:
            server.should_exit = True
            await server_task

    asyncio.run(run())


@cli.command("session-pod")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8080, show_default=True, type=int)
@click.option("--runtime-host", default="127.0.0.1", show_default=True)
@click.option("--runtime-port", default=8090, show_default=True, type=int)
@click.option("--workspace-path", default=None,
              help="Workspace directory (defaults to $CLAUDIUS_WORKSPACE_PATH or /workspace).")
def session_pod(host, port, runtime_host, runtime_port, workspace_path):
    """Run a long-lived session pod (worker + co-located runtime sidecar).

    For static, pre-created deployments: the controller's static backend pushes
    per-execution config to this pod's POST /configure endpoint.
    """
    from claudius.session.static import run_session_pod

    run_session_pod(
        host=host,
        port=port,
        runtime_host=runtime_host,
        runtime_port=runtime_port,
        workspace_path=workspace_path,
    )


@cli.command("runtime-sidecar")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8090, show_default=True, type=int)
@click.option("--phase", "phases", multiple=True)
def runtime_sidecar(host, port, phases):
    """Start the isolated runtime tool sidecar."""
    from claudius.runtime_sidecar import RuntimeSidecar, create_app

    sidecar = RuntimeSidecar.from_env()
    logger.info(
        "starting runtime sidecar host={} port={} phases={} tools={}",
        host,
        port,
        list(phases),
        sorted(sidecar.tools),
    )
    app = create_app(sidecar, startup_phases=list(phases))
    uvicorn.run(app, host=host, port=port, log_level="warning")


@cli.command("runtime-run-hooks")
@click.option("--phase", "phases", multiple=True, required=True)
def runtime_run_hooks(phases):
    """Run configured runtime pre-launch hooks from the container environment."""
    from claudius.runtime_sidecar import run_hook_phases_from_env

    run_hook_phases_from_env(list(phases))


@cli.command("runtime-mcp-bridge")
@click.option("--spec", required=True, type=click.Path(exists=True, dir_okay=False))
def runtime_mcp_bridge(spec):
    """Expose runtime tools to Claude as a local MCP stdio server."""
    from claudius.runtime_mcp_bridge import run_stdio_bridge

    raise SystemExit(run_stdio_bridge(spec))


@cli.command("session-history")
@click.option(
    "--side",
    type=click.Choice(["both", "user", "assistant"], case_sensitive=False),
    default="both",
    show_default=True,
)
@click.option("--limit", default=20, show_default=True, type=int)
@click.option("--query", default="", show_default=True)
def session_history(side, limit, query):
    """Fetch prior stored messages for the current session."""
    callback_url = os.environ.get("CLAUDIUS_CALLBACK_URL", "").rstrip("/")
    session_id = os.environ.get("CLAUDIUS_SESSION_ID", "")
    session_token = os.environ.get("CLAUDIUS_SESSION_TOKEN", "")

    if not callback_url:
        raise click.ClickException("CLAUDIUS_CALLBACK_URL is required")
    if not session_id:
        raise click.ClickException("CLAUDIUS_SESSION_ID is required")
    if not session_token:
        raise click.ClickException("CLAUDIUS_SESSION_TOKEN is required")

    response = httpx.get(
        f"{callback_url}/sessions/{session_id}/history",
        params={
            "side": side.lower(),
            "limit": limit,
            "query": query,
        },
        headers={"Authorization": f"Bearer {session_token}"},
        timeout=10.0,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip() or str(exc)
        raise click.ClickException(f"history fetch failed: {detail}") from exc
    click.echo(json.dumps(response.json(), indent=2))
