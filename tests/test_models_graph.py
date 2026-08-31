from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from runtime_sentinel import (
    DependencyCycleError,
    DependencyGraph,
    InvalidTransitionError,
    RetryPolicy,
    WorkerId,
    WorkerSnapshot,
    WorkerState,
)
from runtime_sentinel.errors import UnknownDependencyError
from runtime_sentinel.models import RestartPolicy


def test_worker_lifecycle_is_explicit_and_immutable() -> None:
    registered = WorkerSnapshot(WorkerId("indexer"))
    starting = registered.transition(WorkerState.STARTING)
    running = starting.transition(WorkerState.RUNNING).heartbeat()
    assert registered.state is WorkerState.REGISTERED
    assert running.state is WorkerState.RUNNING
    assert running.last_heartbeat is not None
    with pytest.raises(InvalidTransitionError):
        registered.transition(WorkerState.RUNNING)


@given(
    attempt=st.integers(min_value=1, max_value=100),
    random_value=st.floats(min_value=0, max_value=1, allow_nan=False),
)
def test_retry_delay_is_always_bounded(attempt: int, random_value: float) -> None:
    policy = RetryPolicy(initial_delay=0.2, max_delay=3, multiplier=2, jitter=0.4)
    assert 0 <= policy.delay_for(attempt, random_value) <= 3


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RetryPolicy(max_attempts=0),
        lambda: RetryPolicy(initial_delay=-1),
        lambda: RetryPolicy(initial_delay=2, max_delay=1),
        lambda: RetryPolicy(multiplier=0.5),
        lambda: RetryPolicy(jitter=1.5),
        lambda: RestartPolicy(heartbeat_timeout=0),
    ],
)
def test_invalid_policies_are_rejected(factory: object) -> None:
    with pytest.raises(ValueError):
        factory()  # type: ignore[operator]


def test_graph_respects_dependencies_and_priority() -> None:
    graph = DependencyGraph()
    graph.add(WorkerId("fetch"), priority=1)
    graph.add(WorkerId("audit"), priority=10)
    graph.add(WorkerId("index"), [WorkerId("fetch")])
    assert graph.topological_order() == (
        WorkerId("audit"),
        WorkerId("fetch"),
        WorkerId("index"),
    )
    assert graph.layers() == (
        (WorkerId("audit"), WorkerId("fetch")),
        (WorkerId("index"),),
    )
    assert graph.dependencies[WorkerId("index")] == frozenset({WorkerId("fetch")})


@given(size=st.integers(min_value=1, max_value=30))
def test_linear_dag_property(size: int) -> None:
    graph = DependencyGraph()
    for index in range(size):
        dependencies = () if index == 0 else (WorkerId(str(index - 1)),)
        graph.add(WorkerId(str(index)), dependencies)
    assert graph.topological_order() == tuple(WorkerId(str(index)) for index in range(size))


def test_graph_rejects_cycles_unknown_nodes_and_duplicates() -> None:
    cycle = DependencyGraph()
    cycle.add(WorkerId("a"), [WorkerId("b")])
    cycle.add(WorkerId("b"), [WorkerId("a")])
    with pytest.raises(DependencyCycleError):
        cycle.topological_order()

    unknown = DependencyGraph()
    unknown.add(WorkerId("a"), [WorkerId("missing")])
    with pytest.raises(UnknownDependencyError):
        unknown.topological_order()
    with pytest.raises(ValueError):
        unknown.add(WorkerId("a"))
