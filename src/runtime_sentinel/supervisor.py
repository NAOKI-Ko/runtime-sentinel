"""Structured-concurrency worker supervisor."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from .errors import WorkerFailedError
from .events import Event, EventBus, EventKind
from .graph import DependencyGraph
from .metrics import InMemoryMetrics, Metrics
from .models import Health, HealthStatus, WorkerId, WorkerSnapshot, WorkerState
from .storage import Storage
from .worker import Worker, WorkerContext, WorkerSpec

FaultInjector = Callable[[WorkerId, int], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class SupervisorOptions:
    concurrency_limit: int = 16
    shutdown_timeout: float = 10.0
    health_check_interval: float = 1.0

    def __post_init__(self) -> None:
        if self.concurrency_limit < 1:
            raise ValueError("concurrency_limit must be positive")
        if self.shutdown_timeout <= 0 or self.health_check_interval <= 0:
            raise ValueError("timeouts must be positive")


class Supervisor:
    def __init__(
        self,
        storage: Storage,
        *,
        options: SupervisorOptions | None = None,
        event_bus: EventBus | None = None,
        metrics: Metrics | None = None,
        fault_injector: FaultInjector | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._storage = storage
        self._options = options or SupervisorOptions()
        self.events = event_bus or EventBus()
        self.metrics = metrics or InMemoryMetrics()
        self._fault_injector = fault_injector
        self._logger = logger or logging.getLogger("runtime_sentinel")
        self._specs: dict[WorkerId, WorkerSpec] = {}
        self._snapshots: dict[WorkerId, WorkerSnapshot] = {}
        self._ready: dict[WorkerId, asyncio.Event] = {}
        self._graph = DependencyGraph()
        self._stop_event = asyncio.Event()
        self._run_task: asyncio.Task[None] | None = None

    def register(self, spec: WorkerSpec) -> None:
        if self._run_task is not None:
            raise RuntimeError("cannot register workers after the supervisor starts")
        self._graph.add(spec.worker_id, spec.dependencies, spec.priority)
        self._specs[spec.worker_id] = spec
        self._snapshots[spec.worker_id] = WorkerSnapshot(spec.worker_id)
        self._ready[spec.worker_id] = asyncio.Event()

    @property
    def snapshots(self) -> Mapping[WorkerId, WorkerSnapshot]:
        return dict(self._snapshots)

    async def start(self) -> None:
        if self._run_task is not None:
            raise RuntimeError("supervisor already started")
        self._graph.topological_order()
        await self._storage.initialize()
        recovered = {snapshot.worker_id: snapshot for snapshot in await self._storage.load_all()}
        for worker_id in self._specs:
            detail = (
                f"recovered from {recovered[worker_id].state.value}"
                if worker_id in recovered
                else ""
            )
            self._snapshots[worker_id] = WorkerSnapshot(worker_id, detail=detail)
            await self._storage.save_snapshot(self._snapshots[worker_id])
            await self._emit(EventKind.REGISTERED, worker_id, {"recovered": worker_id in recovered})
        self._run_task = asyncio.create_task(self._run(), name="runtime-sentinel")
        await asyncio.sleep(0)

    async def _run(self) -> None:
        semaphore = asyncio.Semaphore(self._options.concurrency_limit)
        async with asyncio.TaskGroup() as group:
            group.create_task(self._monitor_health(), name="sentinel-health-monitor")
            for worker_id in self._graph.topological_order():
                group.create_task(
                    self._run_worker(self._specs[worker_id], semaphore), name=str(worker_id)
                )

    async def _run_worker(self, spec: WorkerSpec, semaphore: asyncio.Semaphore) -> None:
        for dependency in spec.dependencies:
            await self._ready[dependency].wait()
            if self._snapshots[dependency].state not in {WorkerState.RUNNING, WorkerState.STOPPED}:
                self._snapshots[spec.worker_id] = WorkerSnapshot(
                    spec.worker_id,
                    state=WorkerState.FAILED,
                    detail=f"dependency failed: {dependency}",
                )
                await self._storage.save_snapshot(self._snapshots[spec.worker_id])
                self._ready[spec.worker_id].set()
                raise WorkerFailedError(f"{spec.worker_id}: dependency {dependency} failed")

        async with semaphore:
            attempt = 0
            while not self._stop_event.is_set():
                attempt += 1
                await self._transition(spec.worker_id, WorkerState.STARTING, f"attempt {attempt}")
                self._snapshots[spec.worker_id] = replace(
                    self._snapshots[spec.worker_id], attempts=attempt
                )
                await self._transition(spec.worker_id, WorkerState.RUNNING)
                await self._heartbeat(spec.worker_id)
                self._ready[spec.worker_id].set()
                started = time.perf_counter()
                context = WorkerContext(
                    spec.worker_id, self._stop_event, lambda: self._heartbeat(spec.worker_id)
                )
                try:
                    if self._fault_injector is not None:
                        await self._fault_injector(spec.worker_id, attempt)
                    await spec.worker.run(context)
                except asyncio.CancelledError:
                    await self._finish(spec.worker_id)
                    raise
                except Exception as error:
                    elapsed = time.perf_counter() - started
                    self.metrics.observe(
                        "worker.run.seconds", elapsed, {"worker": str(spec.worker_id)}
                    )
                    await self._emit(
                        EventKind.FAILED, spec.worker_id, {"attempt": attempt, "error": repr(error)}
                    )
                    if attempt >= spec.restart.retry.max_attempts:
                        await self._transition(spec.worker_id, WorkerState.FAILED, repr(error))
                        self.metrics.increment("worker.failures", {"worker": str(spec.worker_id)})
                        raise WorkerFailedError(f"{spec.worker_id} exhausted retries") from error
                    await self._transition(spec.worker_id, WorkerState.BACKING_OFF, repr(error))
                    delay = spec.restart.retry.delay_for(attempt, random.random())
                    await self._emit(
                        EventKind.RETRIED, spec.worker_id, {"attempt": attempt, "delay": delay}
                    )
                    self.metrics.increment("worker.retries", {"worker": str(spec.worker_id)})
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                    except TimeoutError:
                        continue
                    break
                else:
                    self.metrics.observe(
                        "worker.run.seconds",
                        time.perf_counter() - started,
                        {"worker": str(spec.worker_id)},
                    )
                    break
            await self._finish(spec.worker_id)

    async def _finish(self, worker_id: WorkerId) -> None:
        state = self._snapshots[worker_id].state
        if state in {
            WorkerState.RUNNING,
            WorkerState.UNHEALTHY,
            WorkerState.FAILED,
            WorkerState.BACKING_OFF,
        }:
            await self._transition(worker_id, WorkerState.STOPPING)
            await self._transition(worker_id, WorkerState.STOPPED)

    async def _heartbeat(self, worker_id: WorkerId) -> None:
        snapshot = self._snapshots[worker_id].heartbeat()
        was_unhealthy = snapshot.state is WorkerState.UNHEALTHY
        self._snapshots[worker_id] = snapshot
        await self._storage.save_snapshot(snapshot)
        await self._emit(EventKind.HEARTBEAT, worker_id, {})
        if was_unhealthy:
            await self._transition(worker_id, WorkerState.RUNNING, "heartbeat recovered")
            await self._emit(EventKind.RECOVERED, worker_id, {})

    async def _monitor_health(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._options.health_check_interval
                )
                return
            except TimeoutError:
                pass
            now = datetime.now(UTC)
            for worker_id, snapshot in tuple(self._snapshots.items()):
                if snapshot.state is not WorkerState.RUNNING or snapshot.last_heartbeat is None:
                    continue
                timeout = self._specs[worker_id].restart.heartbeat_timeout
                if (now - snapshot.last_heartbeat).total_seconds() > timeout:
                    await self._transition(worker_id, WorkerState.UNHEALTHY, "heartbeat timed out")
                    await self._emit(
                        EventKind.UNHEALTHY, worker_id, {"reason": "heartbeat_timeout"}
                    )
                    self.metrics.increment("worker.unhealthy", {"worker": str(worker_id)})

    async def health(self) -> Health:
        results: dict[WorkerId, Health] = {}

        async def probe(worker_id: WorkerId, worker: Worker) -> None:
            try:
                results[worker_id] = await worker.health()
            except Exception as error:
                results[worker_id] = Health(HealthStatus.UNHEALTHY, repr(error))

        async with asyncio.TaskGroup() as group:
            for worker_id, spec in self._specs.items():
                group.create_task(probe(worker_id, spec.worker))
        statuses = {health.status for health in results.values()}
        if HealthStatus.UNHEALTHY in statuses:
            return Health(HealthStatus.UNHEALTHY, "one or more workers are unhealthy")
        if HealthStatus.DEGRADED in statuses:
            return Health(HealthStatus.DEGRADED, "one or more workers are degraded")
        return Health(HealthStatus.HEALTHY, f"{len(results)} workers healthy")

    async def _transition(self, worker_id: WorkerId, state: WorkerState, detail: str = "") -> None:
        snapshot = self._snapshots[worker_id].transition(state, detail)
        self._snapshots[worker_id] = snapshot
        await self._storage.save_snapshot(snapshot)
        if state is WorkerState.RUNNING:
            await self._emit(EventKind.STARTED, worker_id, {"attempt": snapshot.attempts})
        self._logger.info(
            "worker_state_changed",
            extra={"worker_id": str(worker_id), "state": state.value, "detail": detail},
        )

    async def _emit(
        self, kind: EventKind, worker_id: WorkerId, payload: Mapping[str, object]
    ) -> None:
        event = Event(kind, worker_id, payload)
        await self._storage.append_event(event)
        await self.events.publish(event)

    async def stop(self) -> None:
        if self._run_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._run_task, self._options.shutdown_timeout)
        except TimeoutError:
            self._run_task.cancel()
            await asyncio.gather(self._run_task, return_exceptions=True)
        except ExceptionGroup as error:
            self._logger.error("supervisor_failed", extra={"errors": repr(error)})
        finally:
            for worker_id, spec in self._specs.items():
                snapshot = self._snapshots[worker_id]
                failed_dependencies = [
                    dependency
                    for dependency in spec.dependencies
                    if self._snapshots[dependency].state is WorkerState.FAILED
                ]
                if failed_dependencies and snapshot.state is not WorkerState.FAILED:
                    self._snapshots[worker_id] = replace(
                        snapshot,
                        state=WorkerState.FAILED,
                        detail=f"dependency failed: {failed_dependencies[0]}",
                        updated_at=datetime.now(UTC),
                    )
                    await self._storage.save_snapshot(self._snapshots[worker_id])
                elif snapshot.state in {
                    WorkerState.RUNNING,
                    WorkerState.UNHEALTHY,
                    WorkerState.BACKING_OFF,
                }:
                    await self._finish(worker_id)
            for worker_id, snapshot in tuple(self._snapshots.items()):
                if snapshot.state is WorkerState.STOPPED:
                    await self._emit(EventKind.STOPPED, worker_id, {})
            self._run_task = None

    async def __aenter__(self) -> Supervisor:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
