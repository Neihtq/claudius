import asyncio
import io
import os
import time
from contextlib import redirect_stderr, redirect_stdout

from fastapi.testclient import TestClient

from claudius.runtime_sidecar import RuntimeSidecar, create_app


class _StubSidecar:
    def __init__(self, *, fail: bool = False):
        self.auth_token = "token"
        self._fail = fail

    async def run_hooks(self, phases):
        assert phases == ["execution_start"]
        await asyncio.sleep(0.05)
        if self._fail:
            raise RuntimeError("hook execution_start failed")

    async def invoke(self, tool_name, params):
        return {"tool_name": tool_name, "params": params}


def test_health_is_ok_without_startup_hooks():
    client = TestClient(create_app(_StubSidecar()))
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_health_reports_startup_failure():
    with TestClient(create_app(_StubSidecar(fail=True), startup_phases=["execution_start"])) as client:
        deadline = time.time() + 1.0
        response = None
        while time.time() < deadline:
            response = client.get("/health")
            if response.status_code == 500:
                break
            time.sleep(0.01)

        assert response is not None
        assert response.status_code == 500
        assert response.json()["detail"] == "hook execution_start failed"


def test_run_command_streams_output_to_container_logs():
    sidecar = RuntimeSidecar(
        auth_token="token",
        context_env={},
        hooks=[],
        tools={},
    )
    stdout = io.StringIO()
    stderr = io.StringIO()
    original_path = os.environ.get("PATH")
    os.environ["PATH"] = original_path or "/usr/bin:/bin"
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = asyncio.run(
                sidecar._run_command(
                    "printf 'hello stdout\\n'; printf 'hello stderr\\n' >&2",
                    extra_env={},
                )
            )
    finally:
        if original_path is None:
            os.environ.pop("PATH", None)
        else:
            os.environ["PATH"] = original_path

    assert result["exit_code"] == 0
    assert result["stdout"] == "hello stdout\n"
    assert result["stderr"] == "hello stderr\n"
    assert "hello stdout\n" in stdout.getvalue()
    assert "hello stderr\n" in stderr.getvalue()
