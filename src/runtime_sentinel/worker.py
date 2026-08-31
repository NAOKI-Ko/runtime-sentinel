"""Worker contracts and registration metadata."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from .models import Health, RestartPolicy, WorkerId

Heartbeat = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class WorkerContext:
    worker_id: WorkerId
    stop_event: asyncio.Event
    _heartbeat: Heartbeat = field(repr=False)

    async def heartbeat(self) -> None:
        await self._heartbeat()

    async def wait_for_stop(self) -> None:
        await self.stop_event.wait()

    @property
    def stopping(self) -> bool:
        return self.stop_event.is_set()


@runtime_checkable
class Worker(Protocol):
    async def run(self, context: WorkerContext) -> None: ...

    async def health(self) -> Health: ...


@dataclass(frozen=True, slots=True)
class WorkerSpec:
    worker_id: WorkerId
    worker: Worker
    dependencies: tuple[WorkerId, ...] = ()
    priority: int = 0
    restart: RestartPolicy = field(default_factory=RestartPolicy)
