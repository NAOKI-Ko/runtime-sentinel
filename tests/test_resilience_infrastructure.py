from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from runtime_sentinel import (
    CircuitBreaker,
    CircuitOpenError,
    Event,
    EventBus,
    EventKind,
    SQLiteStorage,
    TokenBucket,
    WorkerId,
    WorkerSnapshot,
    WorkerState,
)
from runtime_sentinel.errors import PoolClosedError
from runtime_sentinel.pool import AsyncResourcePool


@pytest.mark.asyncio
async def test_circuit_breaker_opens_and_recovers() -> None:
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0)
    calls = 0

    async def fail() -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("transient")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fail)
    assert breaker.state.value == "half_open"

    async def succeed() -> int:
        return 42

    assert await breaker.call(succeed) == 42
    assert breaker.state.value == "closed"


@pytest.mark.asyncio
async def test_open_circuit_rejects_calls() -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=60)

    async def fail() -> None:
        raise RuntimeError

    with pytest.raises(RuntimeError):
        await breaker.call(fail)
    with pytest.raises(CircuitOpenError):
        await breaker.call(fail)


@pytest.mark.parametrize("arguments", [(0, 1), (1, 0), (-1, 1)])
def test_invalid_token_bucket_configuration(arguments: tuple[float, float]) -> None:
    with pytest.raises(ValueError):
        TokenBucket(*arguments)


@pytest.mark.parametrize("arguments", [(0, 1), (-1, 1), (1, -1)])
def test_invalid_circuit_configuration(arguments: tuple[float, float]) -> None:
    with pytest.raises(ValueError):
        CircuitBreaker(int(arguments[0]), arguments[1])


@pytest.mark.asyncio
async def test_token_bucket_refills_and_validates_request() -> None:
    bucket = TokenBucket(capacity=1, refill_rate=1000)
    await bucket.acquire()
    await asyncio.wait_for(bucket.acquire(), timeout=0.1)
    with pytest.raises(ValueError):
        await bucket.acquire(0)
    with pytest.raises(ValueError):
        await bucket.acquire(2)


@pytest.mark.asyncio
async def test_event_bus_fans_out_and_unsubscribes() -> None:
    bus = EventBus(subscriber_capacity=1)
    event = Event(EventKind.STARTED, WorkerId("worker"), {"attempt": 1})
    async with bus.subscribe() as first, bus.subscribe() as second:
        await bus.publish(event)
        assert await first.get() == event
        assert await second.get() == event
    await bus.publish(event)
    with pytest.raises(ValueError):
        EventBus(0)


@pytest.mark.asyncio
async def test_sqlite_round_trip_and_event_log(tmp_path: pytest.TempPathFactory) -> None:
    storage = SQLiteStorage(tmp_path / "sentinel.db")  # type: ignore[operator]
    await storage.initialize()
    snapshot = WorkerSnapshot(WorkerId("worker")).transition(WorkerState.STARTING)
    snapshot = replace(snapshot, attempts=2)
    await storage.save_snapshot(snapshot)
    assert await storage.load_snapshot(WorkerId("worker")) == snapshot
    assert await storage.load_snapshot(WorkerId("missing")) is None
    assert await storage.load_all() == (snapshot,)
    await storage.append_event(Event(EventKind.STARTED, WorkerId("worker"), {"attempt": 2}))


@pytest.mark.asyncio
async def test_resource_pool_bounds_reuses_and_closes() -> None:
    created: list[int] = []
    closed: list[int] = []

    async def factory() -> int:
        created.append(len(created) + 1)
        return created[-1]

    async def closer(resource: int) -> None:
        closed.append(resource)

    pool = AsyncResourcePool(factory, closer, size=1)
    async with pool:
        async with pool.resource() as first:
            assert first == 1
        async with pool.resource() as reused:
            assert reused == first
    assert closed == [1]
    with pytest.raises(PoolClosedError):
        async with pool.resource():
            pass
    with pytest.raises(ValueError):
        AsyncResourcePool(factory, closer, size=0)
