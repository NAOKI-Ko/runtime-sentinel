"""Immutable domain models and lifecycle rules."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import NewType

from .errors import InvalidTransitionError

WorkerId = NewType("WorkerId", str)


class WorkerState(StrEnum):
    REGISTERED = "registered"
    STARTING = "starting"
    RUNNING = "running"
    BACKING_OFF = "backing_off"
    UNHEALTHY = "unhealthy"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True, slots=True)
class Health:
    status: HealthStatus
    detail: str = ""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay: float = 0.05
    max_delay: float = 5.0
    multiplier: float = 2.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.initial_delay < 0 or self.max_delay < self.initial_delay:
            raise ValueError("retry delays are invalid")
        if self.multiplier < 1 or not 0 <= self.jitter <= 1:
            raise ValueError("multiplier or jitter is invalid")

    def delay_for(self, attempt: int, random_value: float = 0.5) -> float:
        """Return bounded exponential delay with symmetric proportional jitter."""
        base = min(self.max_delay, self.initial_delay * self.multiplier ** max(0, attempt - 1))
        factor = 1 + self.jitter * (2 * random_value - 1)
        return max(0.0, min(self.max_delay, base * factor))


@dataclass(frozen=True, slots=True)
class RestartPolicy:
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    heartbeat_timeout: float = 30.0

    def __post_init__(self) -> None:
        if self.heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")


_TRANSITIONS: dict[WorkerState, frozenset[WorkerState]] = {
    WorkerState.REGISTERED: frozenset({WorkerState.STARTING, WorkerState.STOPPED}),
    WorkerState.STARTING: frozenset({WorkerState.RUNNING, WorkerState.FAILED}),
    WorkerState.RUNNING: frozenset(
        {WorkerState.BACKING_OFF, WorkerState.UNHEALTHY, WorkerState.STOPPING, WorkerState.FAILED}
    ),
    WorkerState.BACKING_OFF: frozenset({WorkerState.STARTING, WorkerState.STOPPING}),
    WorkerState.UNHEALTHY: frozenset(
        {WorkerState.BACKING_OFF, WorkerState.RUNNING, WorkerState.STOPPING, WorkerState.FAILED}
    ),
    WorkerState.STOPPING: frozenset({WorkerState.STOPPED}),
    WorkerState.STOPPED: frozenset({WorkerState.STARTING}),
    WorkerState.FAILED: frozenset({WorkerState.STARTING, WorkerState.STOPPING}),
}


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    worker_id: WorkerId
    state: WorkerState = WorkerState.REGISTERED
    attempts: int = 0
    last_heartbeat: datetime | None = None
    detail: str = ""
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def transition(self, target: WorkerState, detail: str = "") -> WorkerSnapshot:
        if target not in _TRANSITIONS[self.state]:
            raise InvalidTransitionError(f"{self.state.value} -> {target.value}")
        return replace(self, state=target, detail=detail, updated_at=datetime.now(UTC))

    def heartbeat(self) -> WorkerSnapshot:
        return replace(self, last_heartbeat=datetime.now(UTC), updated_at=datetime.now(UTC))
