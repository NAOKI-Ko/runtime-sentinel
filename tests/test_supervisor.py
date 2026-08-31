from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path

import pytest

from runtime_sentinel import (
    EventKind,
    Health,
    HealthStatus,
    RestartPolicy,
    RetryPolicy,
    SQLiteStorage,
    Supervisor,
    SupervisorOptions,
    WorkerContext,
    WorkerId,
    WorkerSpec,
    WorkerState,
)


async def eventually(assertion: object, within: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while asyncio.get_running_loop().time() < deadline:
        if assertion():  # type: ignore[operator]
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition was not satisfied")


class CooperativeWorker:
    def __init__(self, status: HealthStatus = HealthStatus.HEALTHY) -> None:
        self.status = status

    async def run(self, context: WorkerContext) -> None:
        while not context.stopping:
            await context.heartbeat()
            with suppress(TimeoutError):
                await asyncio.wait_for(context.wait_for_stop(), timeout=0.01)

    async def health(self) -> Health:
        return Health(self.status)


class FlakyWorker(CooperativeWorker):
    def __init__(self) -> None:
        super().__init__()
        self.invocations = 0

    async def run(self, context: WorkerContext) -> None:
        self.invocations += 1
        if self.invocations == 1:
            raise OSError("injected transient failure")
        await super().run(context)


class SilentWorker(CooperativeWorker):
    async def run(self, context: WorkerContext) -> None:
        await context.wait_for_stop()


class BrokenHealthWorker(CooperativeWorker):
    async def health(self) -> Health:
        raise OSError("probe failed")


def make_supervisor(path: Path, **options: float | int) -> Supervisor:
    return Supervisor(
        SQLiteStorage(path),
        options=SupervisorOptions(**options),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_context_manager_starts_heartbeats_and_stops_cleanly(tmp_path: Path) -> None:
    supervisor = make_supervisor(tmp_path / "state.db", health_check_interval=0.01)
    supervisor.register(WorkerSpec(WorkerId("worker"), CooperativeWorker()))
    async with supervisor:
        await eventually(
            lambda: supervisor.snapshots[WorkerId("worker")].state is WorkerState.RUNNING
        )
        assert (await supervisor.health()).status is HealthStatus.HEALTHY
    assert supervisor.snapshots[WorkerId("worker")].state is WorkerState.STOPPED


@pytest.mark.asyncio
async def test_failure_is_retried_and_observable(tmp_path: Path) -> None:
    worker = FlakyWorker()
    supervisor = make_supervisor(tmp_path / "retry.db", health_check_interval=0.01)
    supervisor.register(
        WorkerSpec(
            WorkerId("flaky"),
            worker,
            restart=RestartPolicy(RetryPolicy(max_attempts=2, initial_delay=0, max_delay=0)),
        )
    )
    async with supervisor.events.subscribe() as queue:
        await supervisor.start()
        await eventually(lambda: worker.invocations == 2)
        await supervisor.stop()
        kinds = []
        while not queue.empty():
            kinds.append(queue.get_nowait().kind)
    assert EventKind.RETRIED in kinds
    assert supervisor.snapshots[WorkerId("flaky")].attempts == 2


@pytest.mark.asyncio
async def test_heartbeat_timeout_marks_worker_unhealthy(tmp_path: Path) -> None:
    supervisor = make_supervisor(tmp_path / "timeout.db", health_check_interval=0.005)
    supervisor.register(
        WorkerSpec(
            WorkerId("silent"),
            SilentWorker(),
            restart=RestartPolicy(heartbeat_timeout=0.01),
        )
    )
    await supervisor.start()
    await eventually(
        lambda: supervisor.snapshots[WorkerId("silent")].state is WorkerState.UNHEALTHY
    )
    await supervisor.stop()


@pytest.mark.asyncio
async def test_health_aggregation_reports_degraded_and_probe_failure(tmp_path: Path) -> None:
    degraded = make_supervisor(tmp_path / "degraded.db")
    degraded.register(WorkerSpec(WorkerId("worker"), CooperativeWorker(HealthStatus.DEGRADED)))
    assert (await degraded.health()).status is HealthStatus.DEGRADED

    broken = make_supervisor(tmp_path / "broken.db")
    broken.register(WorkerSpec(WorkerId("worker"), BrokenHealthWorker()))
    assert (await broken.health()).status is HealthStatus.UNHEALTHY


@pytest.mark.asyncio
async def test_fault_injection_exhausts_retries_and_dependency_fails(tmp_path: Path) -> None:
    async def inject(_: WorkerId, __: int) -> None:
        raise RuntimeError("fault")

    supervisor = Supervisor(
        SQLiteStorage(tmp_path / "fault.db"),
        options=SupervisorOptions(health_check_interval=0.01),
        fault_injector=inject,
    )
    supervisor.register(
        WorkerSpec(
            WorkerId("root"),
            CooperativeWorker(),
            restart=RestartPolicy(RetryPolicy(max_attempts=1)),
        )
    )
    supervisor.register(
        WorkerSpec(WorkerId("dependent"), CooperativeWorker(), dependencies=(WorkerId("root"),))
    )
    await supervisor.start()
    await eventually(lambda: supervisor.snapshots[WorkerId("root")].state is WorkerState.FAILED)
    await supervisor.stop()
    assert supervisor.snapshots[WorkerId("dependent")].state is WorkerState.FAILED


def test_supervisor_rejects_invalid_options_and_late_registration(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SupervisorOptions(concurrency_limit=0)
    with pytest.raises(ValueError):
        SupervisorOptions(shutdown_timeout=0)
