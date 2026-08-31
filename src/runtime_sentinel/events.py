"""Fan-out event bus with bounded subscriber queues."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from .models import WorkerId


class EventKind(StrEnum):
    REGISTERED = "registered"
    STARTED = "started"
    STOPPED = "stopped"
    FAILED = "failed"
    RETRIED = "retried"
    UNHEALTHY = "unhealthy"
    RECOVERED = "recovered"
    HEARTBEAT = "heartbeat"


@dataclass(frozen=True, slots=True)
class Event[P]:
    kind: EventKind
    worker_id: WorkerId
    payload: P
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class EventBus:
    def __init__(self, subscriber_capacity: int = 256) -> None:
        if subscriber_capacity < 1:
            raise ValueError("subscriber_capacity must be positive")
        self._capacity = subscriber_capacity
        self._subscribers: set[asyncio.Queue[Event[Mapping[str, object]]]] = set()
        self._lock = asyncio.Lock()

    async def publish(self, event: Event[Mapping[str, object]]) -> None:
        async with self._lock:
            subscribers = tuple(self._subscribers)
        for queue in subscribers:
            await queue.put(event)

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[asyncio.Queue[Event[Mapping[str, object]]]]:
        queue: asyncio.Queue[Event[Mapping[str, object]]] = asyncio.Queue(self._capacity)
        async with self._lock:
            self._subscribers.add(queue)
        try:
            yield queue
        finally:
            async with self._lock:
                self._subscribers.discard(queue)
