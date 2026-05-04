import pytest
import docker
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from claudius.controller.backends.docker import DockerBackend
from claudius.models import Execution


def _execution():
    return Execution(
        execution_id="exec-1",
        session_id="sess-abcdef12",
        worker_address="172.0.0.1:8080",
        started_at=datetime.now(timezone.utc),
        halted_at=None, halt_reason=None,
    )


@pytest.mark.asyncio
async def test_tail_logs_yields_stdout_and_stderr():
    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_client = MagicMock()
        mock_docker.from_env.return_value = mock_client
        mock_client.networks.get.return_value = MagicMock()
        mock_docker.errors.NotFound = docker.errors.NotFound
        mock_client.containers.list.return_value = []

        mock_container = MagicMock()
        mock_container.name = "claudius-session-sess-abc"
        mock_container.attrs = {"State": {"Running": False}}
        mock_client.containers.get.return_value = mock_container

        def fake_logs(stdout=True, stderr=True, stream=True, follow=True,
                      timestamps=True, since=None):
            if stdout and not stderr:
                return iter([b"2026-01-01T12:00:00.000000000Z hello stdout\n"])
            elif stderr and not stdout:
                return iter([b"2026-01-01T12:00:01.000000000Z hello stderr\n"])
            return iter([])

        mock_container.logs.side_effect = fake_logs

        backend = DockerBackend(image="claudius:latest", workspaces_path="/workspaces")
        lines = []
        async for line in backend.tail_logs(_execution()):
            lines.append(line)

        streams = {l.stream for l in lines}
        assert "stdout" in streams
        assert "stderr" in streams
        bodies = {l.body for l in lines}
        assert "[worker] hello stdout" in bodies
        assert "[worker] hello stderr" in bodies


@pytest.mark.asyncio
async def test_tail_logs_includes_runtime_sidecar_containers():
    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_client = MagicMock()
        mock_docker.from_env.return_value = mock_client
        mock_client.networks.get.return_value = MagicMock()
        mock_docker.errors.NotFound = docker.errors.NotFound

        mock_worker = MagicMock()
        mock_worker.name = "claudius-session-sess-abc"
        mock_worker.attrs = {"State": {"Running": False}}
        mock_sidecar = MagicMock()
        mock_sidecar.name = "claudius-runtime-sess-abcd"
        mock_client.containers.get.return_value = mock_worker
        mock_client.containers.list.return_value = [mock_sidecar]

        def worker_logs(stdout=True, stderr=True, stream=True, follow=True, timestamps=True, since=None):
            if stdout and not stderr:
                return iter([b"2026-01-01T12:00:00.000000000Z worker out\n"])
            if stderr and not stdout:
                return iter([])
            return iter([])

        def sidecar_logs(stdout=True, stderr=True, stream=True, follow=True, timestamps=True, since=None):
            if stderr and not stdout:
                return iter([b"2026-01-01T12:00:01.000000000Z sidecar err\n"])
            if stdout and not stderr:
                return iter([])
            return iter([])

        mock_worker.logs.side_effect = worker_logs
        mock_sidecar.logs.side_effect = sidecar_logs

        backend = DockerBackend(image="claudius:latest", workspaces_path="/workspaces")
        lines = []
        async for line in backend.tail_logs(_execution()):
            lines.append(line)

        bodies = {l.body for l in lines}
        assert "[worker] worker out" in bodies
        assert "[runtime-sidecar:claudius-runtime-sess-abcd] sidecar err" in bodies


@pytest.mark.asyncio
async def test_tail_logs_returns_immediately_for_missing_container():
    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_client = MagicMock()
        mock_docker.from_env.return_value = mock_client
        mock_client.networks.get.return_value = MagicMock()
        mock_docker.errors.NotFound = docker.errors.NotFound
        mock_client.containers.get.side_effect = docker.errors.NotFound("gone")
        mock_client.containers.list.return_value = []

        backend = DockerBackend(image="claudius:latest", workspaces_path="/workspaces")
        lines = []
        async for line in backend.tail_logs(_execution()):
            lines.append(line)

        assert lines == []


@pytest.mark.asyncio
async def test_tail_logs_returns_after_worker_streams_finish_even_if_sidecar_lingers():
    with patch("claudius.controller.backends.docker.docker") as mock_docker:
        mock_client = MagicMock()
        mock_docker.from_env.return_value = mock_client
        mock_client.networks.get.return_value = MagicMock()
        mock_docker.errors.NotFound = docker.errors.NotFound

        mock_worker = MagicMock()
        mock_worker.name = "claudius-session-sess-abc"
        mock_worker.attrs = {"State": {"Running": False}}
        mock_sidecar = MagicMock()
        mock_sidecar.name = "claudius-runtime-sess-abcd"

        worker_get_calls = iter([mock_worker, docker.errors.NotFound("gone"), docker.errors.NotFound("gone")])

        def get_worker(name):
            result = next(worker_get_calls, docker.errors.NotFound("gone"))
            if isinstance(result, Exception):
                raise result
            return result

        sidecar_lists = iter([[mock_sidecar], [mock_sidecar], [mock_sidecar]])
        mock_client.containers.get.side_effect = get_worker
        mock_client.containers.list.side_effect = lambda *args, **kwargs: next(sidecar_lists, [mock_sidecar])

        def worker_logs(stdout=True, stderr=True, stream=True, follow=True, timestamps=True, since=None):
            if stdout and not stderr:
                return iter([b"2026-01-01T12:00:00.000000000Z worker out\n"])
            if stderr and not stdout:
                return iter([])
            return iter([])

        def sidecar_logs(stdout=True, stderr=True, stream=True, follow=True, timestamps=True, since=None):
            return iter([])

        mock_worker.logs.side_effect = worker_logs
        mock_sidecar.logs.side_effect = sidecar_logs

        backend = DockerBackend(image="claudius:latest", workspaces_path="/workspaces")
        lines = []
        async for line in backend.tail_logs(_execution()):
            lines.append(line)

        assert [line.body for line in lines] == ["[worker] worker out"]
