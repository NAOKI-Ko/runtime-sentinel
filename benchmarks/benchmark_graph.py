"""Reproducible dependency-planning microbenchmark."""

import argparse
import statistics
import time

from runtime_sentinel import DependencyGraph, WorkerId


def build_graph(nodes: int) -> DependencyGraph:
    graph = DependencyGraph()
    for index in range(nodes):
        dependencies = () if index == 0 else (WorkerId(str((index - 1) // 2)),)
        graph.add(WorkerId(str(index)), dependencies)
    return graph


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=10_000)
    parser.add_argument("--rounds", type=int, default=20)
    arguments = parser.parse_args()
    samples = []
    for _ in range(arguments.rounds):
        graph = build_graph(arguments.nodes)
        started = time.perf_counter()
        graph.topological_order()
        samples.append(time.perf_counter() - started)
    print(f"nodes={arguments.nodes} median_seconds={statistics.median(samples):.6f}")


if __name__ == "__main__":
    main()
