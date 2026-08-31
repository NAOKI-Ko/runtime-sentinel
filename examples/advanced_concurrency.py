"""Plan a priority DAG and exercise a token bucket concurrently."""

import asyncio

from runtime_sentinel import DependencyGraph, TokenBucket, WorkerId


async def main() -> None:
    graph = DependencyGraph()
    graph.add(WorkerId("fetch"), priority=5)
    graph.add(WorkerId("warm-cache"), priority=10)
    graph.add(WorkerId("index"), (WorkerId("fetch"), WorkerId("warm-cache")))
    print("layers:", graph.layers())

    limiter = TokenBucket(capacity=2, refill_rate=100)

    async def request(number: int) -> None:
        await limiter.acquire()
        print("admitted", number)

    async with asyncio.TaskGroup() as group:
        for index in range(5):
            group.create_task(request(index))


if __name__ == "__main__":
    asyncio.run(main())
