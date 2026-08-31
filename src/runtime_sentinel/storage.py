"""Persistence port and an asyncio-friendly SQLite adapter."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .events import Event
from .models import WorkerId, WorkerSnapshot, WorkerState


class Storage(Protocol):
    async def initialize(self) -> None: ...

    async def save_snapshot(self, snapshot: WorkerSnapshot) -> None: ...

    async def load_snapshot(self, worker_id: WorkerId) -> WorkerSnapshot | None: ...

    async def load_all(self) -> tuple[WorkerSnapshot, ...]: ...

    async def append_event(self, event: Event[Mapping[str, object]]) -> None: ...


class SQLiteStorage:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    async def initialize(self) -> None:
        def create() -> None:
            with closing(self._connect()) as connection, connection:
                connection.executescript(
                    """
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS worker_snapshots (
                        worker_id TEXT PRIMARY KEY,
                        state TEXT NOT NULL,
                        attempts INTEGER NOT NULL,
                        last_heartbeat TEXT,
                        detail TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS events (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        kind TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        occurred_at TEXT NOT NULL
                    );
                    """
                )

        async with self._lock:
            await asyncio.to_thread(create)

    async def save_snapshot(self, snapshot: WorkerSnapshot) -> None:
        def save() -> None:
            with closing(self._connect()) as connection, connection:
                connection.execute(
                    """INSERT INTO worker_snapshots VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(worker_id) DO UPDATE SET
                      state=excluded.state, attempts=excluded.attempts,
                      last_heartbeat=excluded.last_heartbeat, detail=excluded.detail,
                      updated_at=excluded.updated_at""",
                    (
                        snapshot.worker_id,
                        snapshot.state.value,
                        snapshot.attempts,
                        snapshot.last_heartbeat.isoformat() if snapshot.last_heartbeat else None,
                        snapshot.detail,
                        snapshot.updated_at.isoformat(),
                    ),
                )

        async with self._lock:
            await asyncio.to_thread(save)

    @staticmethod
    def _snapshot(row: sqlite3.Row) -> WorkerSnapshot:
        heartbeat = datetime.fromisoformat(row["last_heartbeat"]) if row["last_heartbeat"] else None
        return WorkerSnapshot(
            worker_id=WorkerId(row["worker_id"]),
            state=WorkerState(row["state"]),
            attempts=row["attempts"],
            last_heartbeat=heartbeat,
            detail=row["detail"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    async def load_snapshot(self, worker_id: WorkerId) -> WorkerSnapshot | None:
        def load() -> WorkerSnapshot | None:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM worker_snapshots WHERE worker_id = ?", (worker_id,)
                ).fetchone()
                return self._snapshot(row) if row else None

        async with self._lock:
            return await asyncio.to_thread(load)

    async def load_all(self) -> tuple[WorkerSnapshot, ...]:
        def load() -> tuple[WorkerSnapshot, ...]:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    "SELECT * FROM worker_snapshots ORDER BY worker_id"
                ).fetchall()
                return tuple(self._snapshot(row) for row in rows)

        async with self._lock:
            return await asyncio.to_thread(load)

    async def append_event(self, event: Event[Mapping[str, object]]) -> None:
        def append() -> None:
            with closing(self._connect()) as connection, connection:
                connection.execute(
                    "INSERT INTO events(kind, worker_id, payload, occurred_at) VALUES (?, ?, ?, ?)",
                    (
                        event.kind.value,
                        event.worker_id,
                        json.dumps(event.payload, default=str, sort_keys=True),
                        event.occurred_at.astimezone(UTC).isoformat(),
                    ),
                )

        async with self._lock:
            await asyncio.to_thread(append)
