import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
import os
from pathlib import Path
import queue
import threading
import time
import uuid
from typing import Any

import docker
import docker.errors
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


class DockerBackend(AbstractBackend):
    def __init__(
        self,
        image: str,
        workspaces_path: str,
        network: str = "claudius",
        probe_mode: str = "host_port",
    ):
        self._client = docker.from_env()
        self._image = image
        self._workspaces_path = str(Path(workspaces_path).expanduser().resolve())
        self._network = network
        self._user = f"{os.getuid()}:{os.getgid()}"
        self._probe_mode = probe_mode
        self._ensure_network(network)

    def _ensure_network(self, name: str) -> None:
        try:
            self._client.networks.get(name)
        except docker.errors.NotFound:
            self._client.networks.create(name, driver="bridge")

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
        env_vars: dict[str, str] = {}
        if extra_env:
            env_vars.update(extra_env)
        volumes = self._workspace_volumes(session, tool_mounts)

        for mount in tool_mounts:
            env_vars.update(mount.env_vars)

        started_sidecars = await self._start_sidecars(session, tool_mounts, volumes)

        container_name = f"claudius-session-{session.session_id[:8]}"
        try:
            container = self._run_container_with_conflict_retry(
                self._image,
                session.session_id,
                "worker",
                command=["session", session.session_id],
                environment=env_vars,
                volumes=volumes,
                network=self._network,
                ports={"8080/tcp": None},
                detach=True,
                name=container_name,
                user=self._user,
                labels={
                    "claudius.session_id": session.session_id,
                    "claudius.role": "worker",
                },
                extra_hosts={"host.docker.internal": "host-gateway"},
            )
        except Exception:
            self._stop_sidecars(started_sidecars)
            raise
        container.reload()
        worker_host, worker_port = _container_target(container, self._network, "8080/tcp", self._probe_mode)
        runtime_container = None
        if started_sidecars:
            runtime_host, runtime_port = _container_target(
                started_sidecars[0],
                self._network,
                "8090/tcp",
                self._probe_mode,
            )
            runtime_container = ExecutionContainer(
                name=started_sidecars[0].name,
                status=ContainerLifecycleStatus.RUNNING,
                healthy=True,
                host=runtime_host,
                port=runtime_port,
                probe_mode=self._probe_mode,
            )
        return Execution(
            execution_id=execution_id or str(uuid.uuid4()),
            session_id=session.session_id,
            worker_address=f"{worker_host}:{worker_port}",
            started_at=started_at or datetime.now(timezone.utc),
            halted_at=None,
            halt_reason=None,
            phase=ExecutionPhase.RUNNING,
            runtime_container=runtime_container,
            worker_container=ExecutionContainer(
                name=container.name,
                status=ContainerLifecycleStatus.RUNNING,
                healthy=True,
                host=worker_host,
                port=worker_port,
                probe_mode=self._probe_mode,
            ),
        )

    def _workspace_volumes(
        self,
        session: Session,
        tool_mounts: list[ToolMount],
    ) -> dict[str, dict[str, str]]:
        volumes: dict[str, dict[str, str]] = {
            f"{self._workspaces_path}/{session.session_id}": {
                "bind": "/workspace",
                "mode": "rw",
            }
        }
        for mount in tool_mounts:
            for v in mount.volumes:
                volumes[_resolve_volume_source(v["host"])] = {
                    "bind": v["container"],
                    "mode": v.get("mode", "rw"),
                }
        return volumes

    async def _start_sidecars(
        self,
        session: Session,
        tool_mounts: list[ToolMount],
        volumes: dict[str, dict[str, str]],
    ) -> list[Any]:
        containers: list[Any] = []
        for mount in tool_mounts:
            for sidecar in mount.sidecars:
                run_kwargs: dict[str, Any] = {
                    "command": sidecar["command"],
                    "environment": sidecar.get("environment", {}),
                    "volumes": volumes,
                    "network": self._network,
                    "detach": True,
                    "name": sidecar["name"],
                    "hostname": sidecar.get("hostname"),
                    "user": self._user,
                    "labels": {
                        "claudius.session_id": session.session_id,
                        "claudius.role": "runtime-sidecar",
                    },
                    "extra_hosts": {"host.docker.internal": "host-gateway"},
                }
                if self._probe_mode == "host_port":
                    run_kwargs["ports"] = {"8090/tcp": None}
                container = self._run_container_with_conflict_retry(
                    sidecar.get("image", self._image),
                    session.session_id,
                    "runtime-sidecar",
                    **run_kwargs,
                )
                containers.append(container)
                try:
                    await self._wait_for_sidecar_ready(container)
                except Exception:
                    self._stop_sidecars(containers)
                    raise
        return containers

    async def _wait_for_sidecar_ready(self, container: Any, timeout_seconds: float = 300.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        async with httpx.AsyncClient(timeout=1.0) as client:
            while time.monotonic() < deadline:
                container.reload()
                state = container.attrs.get("State", {}).get("Status")
                if state in {"exited", "dead"}:
                    logs = _container_log_lines(container)
                    detail = f": {' | '.join(logs)}" if logs else ""
                    raise ExecutionStartupError(
                        f"runtime sidecar failed before becoming ready{detail}",
                        log_lines=logs,
                    )
                try:
                    host, port = _container_target(
                        container,
                        self._network,
                        "8090/tcp",
                        self._probe_mode,
                    )
                except RuntimeError:
                    host = None
                    port = None
                if host and port:
                    try:
                        probe_url = f"http://{host}:{port}/health"
                        response = await client.get(probe_url)
                        if response.status_code == 200:
                            logger.info(
                                "runtime sidecar ready name={} probe_mode={} target={}",
                                container.name,
                                self._probe_mode,
                                probe_url,
                            )
                            return
                        if response.status_code == 500:
                            detail = response.text.strip()
                            if detail:
                                raise ExecutionStartupError(
                                    f"runtime sidecar failed before becoming ready: {detail}",
                                    log_lines=[detail],
                                )
                    except httpx.HTTPError:
                        pass
                await asyncio.sleep(0.25)
        logs = _container_log_lines(container)
        detail = f": {' | '.join(logs)}" if logs else ""
        raise ExecutionStartupError(
            f"runtime sidecar did not become ready before timeout{detail}",
            log_lines=logs,
        )

    def _stop_sidecars(self, containers: list[Any]) -> None:
        for container in containers:
            try:
                container.stop(timeout=10)
            except Exception:
                pass
            try:
                container.remove()
            except Exception:
                pass

    def _run_container_with_conflict_retry(
        self,
        image: str,
        session_id: str,
        role: str,
        **kwargs: Any,
    ) -> Any:
        try:
            return self._client.containers.run(image, **kwargs)
        except docker.errors.APIError as exc:
            if not self._is_name_conflict(exc):
                raise
            name = kwargs.get("name")
            if not isinstance(name, str) or not self._remove_managed_container(name, session_id, role):
                raise
            return self._client.containers.run(image, **kwargs)

    def _is_name_conflict(self, exc: docker.errors.APIError) -> bool:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        explanation = getattr(exc, "explanation", "") or ""
        return status_code == 409 and "is already in use" in explanation

    def _remove_managed_container(self, name: str, session_id: str, role: str) -> bool:
        try:
            container = self._client.containers.get(name)
        except docker.errors.NotFound:
            return False

        labels = container.labels or {}
        if (
            labels.get("claudius.session_id") != session_id
            or labels.get("claudius.role") != role
        ):
            return False

        try:
            container.stop(timeout=10)
        except Exception:
            pass
        container.remove(force=True)
        return True

    async def get_exit_code(self, execution: Execution) -> int | None:
        container_name = f"claudius-session-{execution.session_id[:8]}"
        try:
            container = self._client.containers.get(container_name)
            container.reload()
            return container.attrs["State"]["ExitCode"]
        except Exception:
            return None

    async def delete_execution(self, execution: Execution) -> None:
        container_name = f"claudius-session-{execution.session_id[:8]}"
        try:
            container = self._client.containers.get(container_name)
            container.stop(timeout=10)
            container.remove()
        except docker.errors.NotFound:
            pass
        for container in self._client.containers.list(
            all=True,
            filters={
                "label": [
                    f"claudius.session_id={execution.session_id}",
                    "claudius.role=runtime-sidecar",
                ]
            },
        ):
            try:
                container.stop(timeout=10)
            except Exception:
                pass
            try:
                container.remove()
            except Exception:
                pass

    async def tail_logs(
        self, execution: Execution, since: datetime | None = None
    ) -> AsyncIterator[LogLine]:
        work_queue: queue.Queue[Any] = queue.Queue()
        attached: set[tuple[str, str]] = set()
        done_streams: set[tuple[str, str]] = set()
        first_seen_deadline = time.monotonic() + 300.0
        worker_name = f"claudius-session-{execution.session_id[:8]}"
        saw_worker = False
        saw_any_container = False
        worker_finished_deadline: float | None = None

        def _stream(container: Any, stream_name: str, source_label: str) -> None:
            kwargs: dict[str, Any] = {
                "stdout": stream_name == "stdout",
                "stderr": stream_name == "stderr",
                "stream": True,
                "follow": True,
                "timestamps": True,
            }
            if since is not None:
                kwargs["since"] = int(since.timestamp())
            try:
                for chunk in container.logs(**kwargs):
                    for raw in chunk.decode(errors="replace").splitlines():
                        raw = raw.strip()
                        if not raw:
                            continue
                        parts = raw.split(" ", 1)
                        ts_raw, body = parts if len(parts) == 2 else ("", raw)
                        work_queue.put(
                            LogLine(
                                logged_at=_parse_docker_ts(ts_raw),
                                stream=stream_name,
                                body=f"[{source_label}] {body}",
                            )
                        )
            except Exception:
                pass
            finally:
                work_queue.put(("done", container.name, stream_name))

        while True:
            containers = self._containers_for_execution(execution.session_id)
            container_names = {container.name for container, _ in containers}
            worker_container = next(
                (container for container, _ in containers if container.name == worker_name),
                None,
            )
            if containers:
                saw_any_container = True
            if worker_name in container_names:
                saw_worker = True
            for container, source_label in containers:
                for stream_name in ("stdout", "stderr"):
                    key = (container.name, stream_name)
                    if key in attached:
                        continue
                    attached.add(key)
                    threading.Thread(
                        target=_stream,
                        args=(container, stream_name, source_label),
                        daemon=True,
                    ).start()

            try:
                item = await asyncio.to_thread(work_queue.get, True, 0.25)
            except queue.Empty:
                item = None

            if isinstance(item, LogLine):
                yield item
            elif isinstance(item, tuple) and item[:1] == ("done",):
                done_streams.add((item[1], item[2]))

            containers = self._containers_for_execution(execution.session_id)
            container_names = {container.name for container, _ in containers}
            worker_container = next(
                (container for container, _ in containers if container.name == worker_name),
                None,
            )
            if not saw_any_container and not containers and execution.worker_address:
                return
            if not saw_any_container and time.monotonic() >= first_seen_deadline:
                return
            worker_streams = {
                key for key in attached if key[0] == worker_name
            }
            worker_streams_drained = (
                not worker_streams or worker_streams.issubset(done_streams)
            )
            if saw_worker and worker_container is not None and worker_streams_drained:
                try:
                    worker_container.reload()
                    if not worker_container.attrs.get("State", {}).get("Running", False):
                        if worker_finished_deadline is None:
                            worker_finished_deadline = time.monotonic() + 0.5
                except Exception:
                    pass
            if saw_worker and worker_name not in container_names:
                if worker_streams_drained:
                    if worker_finished_deadline is None:
                        worker_finished_deadline = time.monotonic() + 0.5
            if worker_finished_deadline is not None:
                if attached == done_streams or time.monotonic() >= worker_finished_deadline:
                    return
            if saw_any_container and not containers and attached == done_streams:
                return

    def _containers_for_execution(self, session_id: str) -> list[tuple[Any, str]]:
        containers: list[tuple[Any, str]] = []
        worker_name = f"claudius-session-{session_id[:8]}"
        try:
            worker = self._client.containers.get(worker_name)
            containers.append((worker, "worker"))
        except docker.errors.NotFound:
            pass
        for container in self._client.containers.list(
            all=True,
            filters={
                "label": [
                    f"claudius.session_id={session_id}",
                    "claudius.role=runtime-sidecar",
                ]
            },
        ):
            containers.append((container, f"runtime-sidecar:{container.name}"))
        return containers


def _parse_docker_ts(ts_raw: str) -> datetime:
    """Parse Docker RFC3339 nanosecond timestamp, truncating to microseconds."""
    try:
        ts = ts_raw.replace("Z", "+00:00")
        if "." in ts and "+" in ts:
            dot = ts.index(".")
            plus = ts.index("+", dot)
            ts = ts[:dot + 7] + ts[plus:]
        return datetime.fromisoformat(ts)
    except ValueError:
        return datetime.now(timezone.utc)


def _container_log_lines(container: Any) -> list[str]:
    raw = container.logs(stdout=True, stderr=True).decode(errors="replace")
    return [line.strip() for line in raw.splitlines() if line.strip()]


def _container_target(container: Any, network: str, port_key: str, probe_mode: str) -> tuple[str, int]:
    if probe_mode == "host_port":
        host_port = (
            container.attrs.get("NetworkSettings", {})
            .get("Ports", {})
            .get(port_key, [{}])[0]
            .get("HostPort")
        )
        if host_port is None:
            raise RuntimeError(f"missing published host port for {container.name} {port_key}")
        return "127.0.0.1", int(host_port)
    if probe_mode == "container_ip":
        ip_address = (
            container.attrs.get("NetworkSettings", {})
            .get("Networks", {})
            .get(network, {})
            .get("IPAddress")
        )
        if not ip_address:
            raise RuntimeError(f"missing container ip for {container.name}")
        return ip_address, int(port_key.split("/", 1)[0])
    raise RuntimeError(f"unsupported docker probe mode: {probe_mode}")






def _resolve_volume_source(source: str) -> str:
    if _looks_like_host_path(source):
        return str(Path(source).expanduser().resolve())
    return source


def _looks_like_host_path(source: str) -> bool:
    return source.startswith(("/", ".", "~")) or "/" in source
