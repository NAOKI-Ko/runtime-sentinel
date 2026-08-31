"""Command-line inspection tools."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .storage import SQLiteStorage


async def _status(database: Path) -> int:
    storage = SQLiteStorage(database)
    await storage.initialize()
    snapshots = await storage.load_all()
    print(
        json.dumps(
            [
                {
                    "worker_id": snapshot.worker_id,
                    "state": snapshot.state.value,
                    "attempts": snapshot.attempts,
                    "last_heartbeat": snapshot.last_heartbeat.isoformat()
                    if snapshot.last_heartbeat
                    else None,
                    "detail": snapshot.detail,
                }
                for snapshot in snapshots
            ],
            indent=2,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="runtime-sentinel")
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status", help="print persisted worker state as JSON")
    status.add_argument("database", type=Path)
    arguments = parser.parse_args()
    if arguments.command == "status":
        return asyncio.run(_status(arguments.database))
    return 2
