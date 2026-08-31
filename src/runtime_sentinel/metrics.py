"""Minimal metrics port and deterministic in-memory adapter."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Protocol


class Metrics(Protocol):
    def increment(self, name: str, labels: Mapping[str, str] | None = None) -> None: ...

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None: ...


class InMemoryMetrics:
    def __init__(self) -> None:
        self.counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = defaultdict(int)
        self.observations: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = defaultdict(
            list
        )

    @staticmethod
    def _key(
        name: str, labels: Mapping[str, str] | None
    ) -> tuple[str, tuple[tuple[str, str], ...]]:
        return name, tuple(sorted((labels or {}).items()))

    def increment(self, name: str, labels: Mapping[str, str] | None = None) -> None:
        self.counters[self._key(name, labels)] += 1

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        self.observations[self._key(name, labels)].append(value)
