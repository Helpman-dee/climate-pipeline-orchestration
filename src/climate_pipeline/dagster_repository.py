"""Persistent Dagster code location for ordered single-workflow benchmarks."""
from __future__ import annotations

try:
    from dagster import Definitions, graph, multiprocess_executor

    from .orchestrators.dagster_assets import ordered_partitions, partition_work_unit

    _WORKLOAD_COUNTS = {"small": 1, "medium": 25, "large": 120}

    def _benchmark_job(workload: str, partition_count: int):
        steps = [partition_work_unit(workload, index) for index in range(partition_count)]

        @graph(name=f"climate_{workload}_benchmark_graph")
        def benchmark_graph():
            partitions = ordered_partitions()
            previous = None
            for step in steps:
                if previous is None:
                    previous = step(partitions=partitions)
                else:
                    previous = step(partitions=partitions, previous=previous)

        # The static predecessor chain and this explicit executor cap both
        # enforce one partition work unit at a time.
        return benchmark_graph.to_job(
            name=f"climate_{workload}_benchmark_job",
            executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
        )

    climate_small_benchmark_job = _benchmark_job("small", _WORKLOAD_COUNTS["small"])
    climate_medium_benchmark_job = _benchmark_job("medium", _WORKLOAD_COUNTS["medium"])
    climate_large_benchmark_job = _benchmark_job("large", _WORKLOAD_COUNTS["large"])
    definitions = Definitions(jobs=[climate_small_benchmark_job, climate_medium_benchmark_job, climate_large_benchmark_job])
except ImportError:  # keeps the shared core importable without Dagster installed
    definitions = None
