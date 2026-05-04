import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from pathlib import Path
import docker
import httpx
from claudius.models import Session, SessionState, ToolMount, Execution

def _session():
    return Session(
        session_id="sess-abc",
        thread_id="thread-1",
        channel="email",
        workflow_name="test",
        state=SessionState.ACTIVE,
        workspace_path="/workspaces/sess-abc",
        created_at=datetime.now(timezone.utc),
        last_message_at=datetime.now(timezone.utc),
    )

def _workflow():
    from claudius.config.schema import WorkflowConfig
    return WorkflowConfig.model_validate({
        "name": "test",
        "routing": {"channels": ["email"]},
        "claude": {"system_prompt": "test"},
        "response": {"channel": "email"},
    })

@pytest.mark.asyncio
async def test_create_execution_runs_container():
    mock_client = MagicMock()
    mock_container = MagicMock()
    mock_container.attrs = {
        "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49123"}]}}
    }
    mock_client.containers.run.return_value = mock_container

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="/workspaces",
            network="claudius",
        )

    execution = await backend.create_execution(
        _session(), _workflow(), [], extra_env={"HOME": "/workspace/.claude-home"}
    )
    assert execution.worker_address == "127.0.0.1:49123"
    assert execution.session_id == "sess-abc"
    mock_client.containers.run.assert_called_once()
    kwargs = mock_client.containers.run.call_args.kwargs
    assert kwargs["user"] == "1000:1000"
    assert kwargs["environment"]["HOME"] == "/workspace/.claude-home"

@pytest.mark.asyncio
async def test_create_execution_resolves_relative_volume_sources():
    mock_client = MagicMock()
    mock_container = MagicMock()
    mock_container.attrs = {
        "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49123"}]}}
    }
    mock_client.containers.run.return_value = mock_container

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="./data/workspaces",
            network="claudius",
        )

    tool_mounts = [
        ToolMount(volumes=[{"host": "./data/attachments/sess-abc", "container": "/attachments"}])
    ]

    await backend.create_execution(_session(), _workflow(), tool_mounts)

    volumes = mock_client.containers.run.call_args.kwargs["volumes"]
    assert str(Path("./data/workspaces").resolve() / "sess-abc") in volumes
    assert str(Path("./data/attachments/sess-abc").resolve()) in volumes

@pytest.mark.asyncio
async def test_create_execution_preserves_named_volumes():
    mock_client = MagicMock()
    mock_container = MagicMock()
    mock_container.attrs = {
        "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49123"}]}}
    }
    mock_client.containers.run.return_value = mock_container

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="/workspaces",
            network="claudius",
        )

    tool_mounts = [
        ToolMount(volumes=[{"host": "named-volume", "container": "/attachments"}])
    ]

    await backend.create_execution(_session(), _workflow(), tool_mounts)

    volumes = mock_client.containers.run.call_args.kwargs["volumes"]
    assert "named-volume" in volumes


@pytest.mark.asyncio
async def test_create_execution_runs_sidecar_with_hook_phases_before_worker():
    mock_client = MagicMock()
    mock_worker_container = MagicMock()
    mock_worker_container.attrs = {
        "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49123"}]}}
    }
    mock_sidecar_container = MagicMock()
    mock_client.containers.run.side_effect = [
        mock_sidecar_container,
        mock_worker_container,
    ]

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="/workspaces",
            network="claudius",
        )
    backend._wait_for_sidecar_ready = AsyncMock()

    tool_mounts = [
        ToolMount(
            sidecars=[{
                "name": "claudius-runtime-sess-abc",
                "hostname": "claudius-runtime-sess-abc",
                "command": ["runtime-sidecar", "--phase", "execution_start"],
                "environment": {"CLAUDIUS_RUNTIME_SPEC_JSON": "{}"},
            }],
        )
    ]

    await backend.create_execution(_session(), _workflow(), tool_mounts)

    assert mock_client.containers.run.call_count == 2
    sidecar_kwargs = mock_client.containers.run.call_args_list[0].kwargs
    assert sidecar_kwargs["command"] == ["runtime-sidecar", "--phase", "execution_start"]
    assert sidecar_kwargs["name"] == "claudius-runtime-sess-abc"
    assert sidecar_kwargs["labels"]["claudius.role"] == "runtime-sidecar"
    backend._wait_for_sidecar_ready.assert_awaited_once_with(mock_sidecar_container)
    worker_kwargs = mock_client.containers.run.call_args_list[1].kwargs
    assert worker_kwargs["labels"]["claudius.role"] == "worker"


@pytest.mark.asyncio
async def test_create_execution_retries_after_stale_sidecar_name_conflict():
    mock_client = MagicMock()
    stale_sidecar = MagicMock()
    stale_sidecar.labels = {
        "claudius.session_id": "sess-abc",
        "claudius.role": "runtime-sidecar",
    }
    new_sidecar = MagicMock()
    worker_container = MagicMock()
    worker_container.attrs = {
        "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49123"}]}}
    }
    conflict_response = MagicMock(status_code=409)
    conflict = docker.errors.APIError(
        "conflict",
        response=conflict_response,
        explanation=(
            'Conflict ("Conflict. The container name "/claudius-runtime-sess-abc" '
            'is already in use.")'
        ),
    )
    mock_client.containers.run.side_effect = [conflict, new_sidecar, worker_container]
    mock_client.containers.get.return_value = stale_sidecar

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors.APIError = docker.errors.APIError
        mock_docker.errors.NotFound = docker.errors.NotFound
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="/workspaces",
            network="claudius",
        )
    backend._wait_for_sidecar_ready = AsyncMock()

    tool_mounts = [
        ToolMount(
            sidecars=[{
                "name": "claudius-runtime-sess-abc",
                "hostname": "claudius-runtime-sess-abc",
                "command": ["runtime-sidecar", "--phase", "execution_start"],
                "environment": {"CLAUDIUS_RUNTIME_SPEC_JSON": "{}"},
            }],
        )
    ]

    execution = await backend.create_execution(_session(), _workflow(), tool_mounts)

    assert execution.worker_address == "127.0.0.1:49123"
    stale_sidecar.stop.assert_called_once_with(timeout=10)
    stale_sidecar.remove.assert_called_once_with(force=True)
    assert mock_client.containers.run.call_count == 3


@pytest.mark.asyncio
async def test_create_execution_retries_after_stale_worker_name_conflict():
    mock_client = MagicMock()
    stale_worker = MagicMock()
    stale_worker.labels = {
        "claudius.session_id": "sess-abc",
        "claudius.role": "worker",
    }
    worker_container = MagicMock()
    worker_container.attrs = {
        "NetworkSettings": {"Ports": {"8080/tcp": [{"HostPort": "49123"}]}}
    }
    conflict_response = MagicMock(status_code=409)
    conflict = docker.errors.APIError(
        "conflict",
        response=conflict_response,
        explanation=(
            'Conflict ("Conflict. The container name "/claudius-session-sess-abc" '
            'is already in use.")'
        ),
    )
    mock_client.containers.run.side_effect = [conflict, worker_container]
    mock_client.containers.get.return_value = stale_worker

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        mock_docker.errors.APIError = docker.errors.APIError
        mock_docker.errors.NotFound = docker.errors.NotFound
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="/workspaces",
            network="claudius",
        )

    execution = await backend.create_execution(_session(), _workflow(), [])

    assert execution.worker_address == "127.0.0.1:49123"
    stale_worker.stop.assert_called_once_with(timeout=10)
    stale_worker.remove.assert_called_once_with(force=True)
    assert mock_client.containers.run.call_count == 2

@pytest.mark.asyncio
async def test_delete_execution_stops_container():
    mock_client = MagicMock()
    mock_container = MagicMock()
    mock_client.containers.get.return_value = mock_container
    mock_client.containers.list.return_value = []

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(
            image="claudius:latest",
            workspaces_path="/workspaces",
            probe_mode="container_ip",
        )

    execution = Execution(
        execution_id="exec-1", session_id="sess-abc",
        worker_address="172.17.0.5:8080",
        started_at=datetime.now(timezone.utc),
        halted_at=None, halt_reason=None,
    )
    await backend.delete_execution(execution)
    mock_container.stop.assert_called_once()
    mock_container.remove.assert_called_once()


@pytest.mark.asyncio
async def test_wait_for_sidecar_ready_raises_sidecar_http_failure():
    mock_client = MagicMock()

    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_docker.from_env.return_value = mock_client
        from claudius.controller.backends.docker import DockerBackend
        backend = DockerBackend(image="claudius:latest", workspaces_path="/workspaces")

    container = MagicMock()
    container.attrs = {
        "State": {"Status": "running"},
        "NetworkSettings": {"Networks": {backend._network: {"IPAddress": "172.18.0.10"}}},
    }

    response = httpx.Response(500, text="hook execution_start failed")

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            return response

    with patch("claudius.controller.backends.docker.httpx.AsyncClient", return_value=_Client()):
        with pytest.raises(RuntimeError, match="hook execution_start failed"):
            await backend._wait_for_sidecar_ready(container, timeout_seconds=0.5)
