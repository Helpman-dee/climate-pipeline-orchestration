"""Static, ordered single-DAG benchmark definitions for each workload size."""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, get_current_context, task

    from .common import run_adapter_partition

    _WORKLOAD_COUNTS = {"small": 1, "medium": 25, "large": 120}

    def _benchmark_dag(workload: str, partition_count: int):
        @dag(
            dag_id=f"nigeria_climate_pipeline_{workload}",
            schedule=None,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            max_active_runs=1,
            max_active_tasks=1,
            tags=["climate", "benchmark", "single-workflow-v2", workload],
        )
        def pipeline():
            @task(retries=2, retry_delay=timedelta(seconds=1))
            def partition(index: int):
                """One ordered work unit from this single DAG-run payload."""
                conf = get_current_context()["dag_run"].conf or {}
                partitions = conf.get("partitions")
                if not isinstance(partitions, list) or len(partitions) != partition_count:
                    raise ValueError(f"{workload} DAG requires exactly {partition_count} ordered partitions")
                item = partitions[index]
                return run_adapter_partition(
                    str(item["location_id"]), int(item["year"]), str(item["run_id"]),
                    str(item["replay_base_url"]), item.get("fault_scenario"), int(item["seed"]),
                    str(item["data_root"]),
                )

            previous = None
            for index in range(partition_count):
                current = partition.override(task_id=f"partition_{index:03d}")(index)
                if previous is not None:
                    previous >> current
                previous = current

        return pipeline()

    airflow_dag_small = _benchmark_dag("small", _WORKLOAD_COUNTS["small"])
    airflow_dag_medium = _benchmark_dag("medium", _WORKLOAD_COUNTS["medium"])
    airflow_dag_large = _benchmark_dag("large", _WORKLOAD_COUNTS["large"])
    # Compatibility export for DAG-discovery code; all three globals above
    # are discovered by Airflow.
    airflow_dag = airflow_dag_small
except ImportError:
    airflow_dag = None
    airflow_dag_small = None
    airflow_dag_medium = None
    airflow_dag_large = None
