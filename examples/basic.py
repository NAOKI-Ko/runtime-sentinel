"""Supervise one cooperative worker."""

import asyncio
import tempfile
from pathlib import Path

from runtime_sentinel import (
    Health,
    HealthStatus,
    SQLiteStorage,
    Supervisor,
    WorkerContext,
    WorkerId,
    WorkerSpec,
)


class ClockWorker:
    async def run(self, context: WorkerContext) -> None:
        while not context.stopping:
            await context.heartbeat()
            await asyncio.sleep(0.05)

    async def health(self) -> Health:
        return Health(HealthStatus.HEALTHY, "clock is ticking")


async def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        supervisor = Supervisor(SQLiteStorage(Path(directory) / "state.db"))
        supervisor.register(WorkerSpec(WorkerId("clock"), ClockWorker()))
        async with supervisor:
            await asyncio.sleep(0.2)
            print((await supervisor.health()).status.value)


if __name__ == "__main__":
    asyncio.run(main())
