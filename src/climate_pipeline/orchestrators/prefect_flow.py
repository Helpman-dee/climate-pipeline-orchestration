from __future__ import annotations
from .common import run_adapter_partition
try:
    from prefect import flow, task
    from prefect.task_runners import ThreadPoolTaskRunner

    @task(retries=2, retry_delay_seconds=1)
    def partition_task(**kwargs): return run_adapter_partition(**kwargs)

    @flow(name="nigeria_climate_pipeline", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=1))
    def climate_flow(partitions: list[dict], parallelism: int = 1):
        """One flow containing an explicitly ordered dependency chain."""
        if parallelism != 1:
            raise ValueError("The controlled benchmark requires partition concurrency of one")
        if not partitions:
            raise ValueError("The controlled benchmark requires at least one partition")
        futures = []
        previous = None
        for item in partitions:
            # The dependency chain supplies deterministic ordering, while the
            # one-worker task runner makes the concurrency limit explicit.
            if previous is None:
                current = partition_task.submit(**item)
            else:
                current = partition_task.submit(**item, wait_for=[previous])
            futures.append(current)
            previous = current
        return [future.result() for future in futures]
except ImportError:
    climate_flow = None
