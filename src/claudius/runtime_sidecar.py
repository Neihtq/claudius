import asyncio
import json
import os
import shlex
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from loguru import logger


MAX_OUTPUT_CHARS = 20_000


@dataclass
class RuntimeSidecar:
    auth_token: str
    context_env: dict[str, str]
    hooks: list[dict[str, str]]
    tools: dict[str, dict[str, Any]]

    @classmethod
    def from_env(cls) -> "RuntimeSidecar":
        payload = json.loads(os.environ["CLAUDIUS_RUNTIME_SPEC_JSON"])
        tools = {tool["name"]: tool for tool in payload.get("tools", [])}
        return cls(
            auth_token=payload["auth_token"],
            context_env=payload.get("context_env", {}),
            hooks=payload.get("hooks", []),
            tools=tools,
        )

    async def run_hooks(self, phases: list[str]) -> None:
        logger.info("running runtime hook phases={}", phases)
        for hook in self.hooks:
            if hook.get("when") not in phases:
                continue
            logger.info("running runtime hook when={} command={}", hook.get("when"), hook["run"])
            result = await self._run_command(hook["run"], extra_env={})
            if result["exit_code"] != 0:
                detail = result["stderr"] or result["stdout"]
                logger.error(
                    "runtime hook failed when={} exit_code={}",
                    hook.get("when"),
                    result["exit_code"],
                )
                raise RuntimeError(
                    f"hook {hook.get('when')} failed with exit code {result['exit_code']}: {detail}"
                )
            logger.info("runtime hook finished when={} exit_code=0", hook.get("when"))

    async def invoke(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        tool = self.tools.get(tool_name)
        if tool is None:
            raise KeyError(tool_name)
        input_env_map = tool.get("input_env_map", {})
        typed_params = _validate_params(tool.get("params", {}), params)
        extra_env = {
            input_env_map[name]: _coerce_env_value(value)
            for name, value in typed_params.items()
        }
        logger.info("invoking runtime tool name={} params={}", tool_name, sorted(typed_params))
        return await self._run_command(tool["run"], extra_env=extra_env)

    async def _run_command(self, command: str, *, extra_env: dict[str, str]) -> dict[str, Any]:
        env = _base_env()
        env.update(self.context_env)
        env.update(extra_env)
        cwd = "/workspace" if os.path.isdir("/workspace") else os.getcwd()
        logger.info("runtime command start cwd={} command={}", cwd, shlex.join(["bash", "-lc", command]))
        process = await asyncio.create_subprocess_exec(
            "bash",
            "-lc",
            f"set -euo pipefail\n{command}",
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        await asyncio.gather(
            _drain_stream(process.stdout, stdout_chunks, sys.stdout),
            _drain_stream(process.stderr, stderr_chunks, sys.stderr),
        )
        returncode = await process.wait()
        logger.info("runtime command finished exit_code={}", returncode)
        return {
            "exit_code": returncode,
            "stdout": _truncate("".join(stdout_chunks)),
            "stderr": _truncate("".join(stderr_chunks)),
        }


class HookRequest(BaseModel):
    phases: list[str]


class InvokeRequest(BaseModel):
    params: dict[str, Any] = {}


def create_app(
    sidecar: RuntimeSidecar | None = None,
    *,
    startup_phases: list[str] | None = None,
) -> FastAPI:
    runtime = sidecar or RuntimeSidecar.from_env()
    phases = list(startup_phases or [])
    startup_task: asyncio.Task[None] | None = None
    startup_error: str | None = None

    async def _run_startup_hooks() -> None:
        nonlocal startup_error
        try:
            logger.info("runtime sidecar startup hooks begin phases={}", phases)
            await runtime.run_hooks(phases)
            logger.info("runtime sidecar startup hooks complete phases={}", phases)
        except Exception as exc:
            startup_error = str(exc)
            logger.error("runtime sidecar startup hooks failed error={}", startup_error)

    async def _ensure_started() -> None:
        if startup_task is not None and not startup_task.done():
            raise HTTPException(status_code=503, detail="runtime sidecar is still starting")
        if startup_error is not None:
            raise HTTPException(status_code=500, detail=startup_error)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal startup_task
        if phases:
            startup_task = asyncio.create_task(_run_startup_hooks())
        yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        await _ensure_started()
        return {"status": "ok"}

    @app.post("/hooks/run")
    async def run_hooks(
        request: HookRequest,
        authorization: str | None = Header(default=None),
    ) -> dict[str, str]:
        await _ensure_started()
        _check_auth(runtime, authorization)
        await runtime.run_hooks(request.phases)
        return {"status": "ok"}

    @app.post("/invoke/{tool_name}")
    async def invoke(
        tool_name: str,
        request: InvokeRequest,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        await _ensure_started()
        _check_auth(runtime, authorization)
        try:
            result = await runtime.invoke(tool_name, request.params)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"unknown tool {tool_name!r}") from None
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return result

    return app


def run_hook_phases_from_env(phases: list[str]) -> None:
    asyncio.run(RuntimeSidecar.from_env().run_hooks(phases))


def _check_auth(sidecar: RuntimeSidecar, authorization: str | None) -> None:
    expected = f"Bearer {sidecar.auth_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


def _base_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for key in ("HOME", "LANG", "LC_ALL", "PATH", "PYTHONPATH"):
        value = os.environ.get(key)
        if value:
            env[key] = value
    return env


def _truncate(text: str) -> str:
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    return text[:MAX_OUTPUT_CHARS] + "\n...[truncated]..."


async def _drain_stream(
    stream: asyncio.StreamReader | None,
    sink: list[str],
    output: Any,
) -> None:
    if stream is None:
        return
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            break
        text = chunk.decode(errors="replace")
        sink.append(text)
        output.write(text)
        output.flush()


def _coerce_env_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _validate_params(specs: dict[str, dict[str, str]], params: dict[str, Any]) -> dict[str, Any]:
    expected = set(specs)
    actual = set(params)
    unexpected = actual - expected
    missing = expected - actual
    if unexpected:
        raise ValueError(f"unexpected params: {', '.join(sorted(unexpected))}")
    if missing:
        raise ValueError(f"missing params: {', '.join(sorted(missing))}")
    validated: dict[str, Any] = {}
    for name, spec in specs.items():
        validated[name] = _validate_param_value(name, spec["type"], params[name])
    return validated


def _validate_param_value(name: str, kind: str, value: Any) -> Any:
    if kind == "string":
        if not isinstance(value, str):
            raise ValueError(f"param {name!r} must be a string")
        return value
    if kind == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"param {name!r} must be a boolean")
        return value
    if kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"param {name!r} must be an integer")
        return value
    if kind == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"param {name!r} must be a number")
        return value
    raise ValueError(f"unsupported param type: {kind}")
