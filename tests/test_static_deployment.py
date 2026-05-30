"""Tests for the static (pre-created) deployment: StaticBackend + session pod."""

import json

import pytest
from fastapi.testclient import TestClient

from claudius.controller.backends.base import AbstractBackend
from claudius.controller.backends.static import StaticBackend
from claudius.runtime_sidecar import RuntimeSidecar
from claudius.session import static as static_mod
from claudius.session.static import create_pod_app


WORKFLOW_JSON = json.dumps(
    {
        "name": "twitch-vibes",
        "routing": {"channels": ["dev"]},
        "claude": {"model": "anthropic/claude-sonnet-4.5", "system_prompt": "loop"},
        "mcp_servers": {"playwright": {"command": "npx", "args": ["-y", "@playwright/mcp@latest"]}},
        "session": {"perpetual": True, "idle_timeout_seconds": 3600},
    }
)

WORKER_ENV = {
    "CLAUDIUS_WORKFLOW": WORKFLOW_JSON,
    "CLAUDIUS_INITIAL_MESSAGE": json.dumps(
        {
            "channel": "dev",
            "sender": "sre@local",
            "recipients": [],
            "thread_id": "t1",
            "subject": None,
            "body": "start",
            "received_at": "2026-01-01T00:00:00+00:00",
        }
    ),
    "CLAUDIUS_SESSION_ID": "abcd1234",
    "HOME": "/tmp/claudius-home-test",
}


def test_static_backend_resolves_endpoint_and_defers_runtime(monkeypatch):
    monkeypatch.setenv("CLAUDIUS_SESSION_ENDPOINT", "http://session:8080")
    backend = StaticBackend()
    assert isinstance(backend, AbstractBackend)
    assert backend.prepares_runtime_in_controller() is False
    assert backend._worker_address == "session:8080"


def test_static_backend_endpoint_from_rise_host(monkeypatch):
    monkeypatch.delenv("CLAUDIUS_SESSION_ENDPOINT", raising=False)
    monkeypatch.setenv("RISE_CONTAINER_HOST__SESSION", "session-host:8080")
    backend = StaticBackend()
    assert backend._endpoint == "http://session-host:8080"


def test_pod_configure_requires_token(monkeypatch, tmp_path):
    # Avoid spawning Claude or running real hooks.
    monkeypatch.setattr(static_mod, "_prepare_runtime", lambda *a, **k: [])

    async def _noop_run(self):
        return None

    monkeypatch.setattr(static_mod.SessionRunner, "run", _noop_run, raising=True)

    sidecar = RuntimeSidecar.empty()
    app = create_pod_app(sidecar, workspace_path=str(tmp_path), configure_token="secret")
    client = TestClient(app)

    body = {"session_id": "abcd1234", "worker_env": WORKER_ENV, "execution_id": "e1"}

    # Missing/wrong token rejected.
    assert client.post("/configure", json=body).status_code == 401
    assert client.post(
        "/configure", json=body, headers={"Authorization": "Bearer nope"}
    ).status_code == 401

    # Correct token accepted; the execution runs to completion (noop runner).
    resp = client.post(
        "/configure", json=body, headers={"Authorization": "Bearer secret"}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_id"] == "e1"

    status = client.get("/status", headers={"Authorization": "Bearer secret"}).json()
    assert status["execution_id"] == "e1"
    assert status["state"] in {"starting", "running", "finished"}


def test_pod_health_open_without_token(tmp_path):
    sidecar = RuntimeSidecar.empty()
    app = create_pod_app(sidecar, workspace_path=str(tmp_path), configure_token="secret")
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
