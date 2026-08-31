"""Deterministic dependency graph validation and scheduling."""

from __future__ import annotations

import heapq
from collections.abc import Iterable

from .errors import DependencyCycleError, UnknownDependencyError
from .models import WorkerId


class DependencyGraph:
    """A mutable builder that produces deterministic immutable execution layers."""

    def __init__(self) -> None:
        self._dependencies: dict[WorkerId, frozenset[WorkerId]] = {}
        self._priorities: dict[WorkerId, int] = {}

    def add(
        self, worker_id: WorkerId, dependencies: Iterable[WorkerId] = (), priority: int = 0
    ) -> None:
        if worker_id in self._dependencies:
            raise ValueError(f"worker already registered: {worker_id}")
        self._dependencies[worker_id] = frozenset(dependencies)
        self._priorities[worker_id] = priority

    @property
    def dependencies(self) -> dict[WorkerId, frozenset[WorkerId]]:
        return dict(self._dependencies)

    def topological_order(self) -> tuple[WorkerId, ...]:
        unknown = {
            dependency
            for dependencies in self._dependencies.values()
            for dependency in dependencies
            if dependency not in self._dependencies
        }
        if unknown:
            raise UnknownDependencyError(", ".join(sorted(unknown)))

        indegree = {node: len(dependencies) for node, dependencies in self._dependencies.items()}
        dependents: dict[WorkerId, list[WorkerId]] = {node: [] for node in self._dependencies}
        for node, dependencies in self._dependencies.items():
            for dependency in dependencies:
                dependents[dependency].append(node)

        ready = [
            (-self._priorities[node], str(node), node)
            for node, degree in indegree.items()
            if degree == 0
        ]
        heapq.heapify(ready)
        result: list[WorkerId] = []
        while ready:
            _, _, node = heapq.heappop(ready)
            result.append(node)
            for dependent in dependents[node]:
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    heapq.heappush(ready, (-self._priorities[dependent], str(dependent), dependent))
        if len(result) != len(self._dependencies):
            cyclic = sorted(str(node) for node, degree in indegree.items() if degree > 0)
            raise DependencyCycleError(f"dependency cycle contains: {', '.join(cyclic)}")
        return tuple(result)

    def layers(self) -> tuple[tuple[WorkerId, ...], ...]:
        order = self.topological_order()
        depth: dict[WorkerId, int] = {}
        for node in order:
            depth[node] = (
                0
                if not self._dependencies[node]
                else 1 + max(depth[parent] for parent in self._dependencies[node])
            )
        return tuple(
            tuple(node for node in order if depth[node] == level)
            for level in range(max(depth.values(), default=-1) + 1)
        )
