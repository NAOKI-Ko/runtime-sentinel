"""Bounded asynchronous resource pool."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TypeVar

from .errors import PoolClosedError

T = TypeVar("T")


class AsyncResourcePool[T]:
    def __init__(
        self,
        factory: Callable[[], Awaitable[T]],
        closer: Callable[[T], Awaitable[None]],
        size: int,
    ) -> None:
        if size < 1:
            raise ValueError("size must be positive")
        self._factory = factory
        self._closer = closer
        self._size = size
        self._resources: asyncio.LifoQueue[T] = asyncio.LifoQueue(size)
        self._created: set[T] = set()
        self._create_lock = asyncio.Lock()
        self._closed = False

    async def _acquire(self) -> T:
        if self._closed:
            raise PoolClosedError("resource pool is closed")
        try:
            return self._resources.get_nowait()
        except asyncio.QueueEmpty:
            async with self._create_lock:
                if len(self._created) < self._size:
                    resource = await self._factory()
                    self._created.add(resource)
                    return resource
            return await self._resources.get()

    @asynccontextmanager
    async def resource(self) -> AsyncIterator[T]:
        resource = await self._acquire()
        try:
            yield resource
        finally:
            if not self._closed:
                await self._resources.put(resource)

    async def close(self) -> None:
        self._closed = True
        await asyncio.gather(*(self._closer(resource) for resource in tuple(self._created)))
        self._created.clear()

    async def __aenter__(self) -> AsyncResourcePool[T]:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
