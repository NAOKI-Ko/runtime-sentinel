"""Observe a transient failure and bounded retry."""

import asyncio
import tempfile
from pathlib import Path

from runtime_sentinel import (
    Health,
    HealthStatus,
    RestartPolicy,
    RetryPolicy,
    SQLiteStorage,
    Supervisor,
    WorkerContext,
    WorkerId,
    WorkerSpec,
)


class FlakyWorker:
    def __init__(self) -> None:
        self.attempt = 0

    async def run(self, context: WorkerContext) -> None:
        self.attempt += 1
        if self.attempt == 1:
            raise ConnectionError("simulated upstream reset")
        await context.wait_for_stop()

    async def health(self) -> Health:
        return Health(HealthStatus.HEALTHY)


async def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        supervisor = Supervisor(SQLiteStorage(Path(directory) / "state.db"))
        supervisor.register(
            WorkerSpec(
                WorkerId("flaky"),
                FlakyWorker(),
                restart=RestartPolicy(RetryPolicy(max_attempts=3, initial_delay=0.05)),
            )
        )
        async with supervisor.events.subscribe() as events:
            await supervisor.start()
            while (event := await events.get()).kind.value != "retried":
                pass
            print(event.kind.value, event.payload)
            await supervisor.stop()


if __name__ == "__main__":
    asyncio.run(main())
