"""Ordered Dagster work-unit ops for the v2 single-workflow topology."""
from __future__ import annotations

try:
    from dagster import In, Nothing, Out, RetryPolicy, op

    from .common import run_adapter_partition

    @op
    def ordered_partitions(context) -> list[dict]:
        partitions = context.op_config.get("partitions")
        expected_count = context.op_config.get("expected_partition_count")
        if not isinstance(partitions, list) or not isinstance(expected_count, int) or len(partitions) != expected_count:
            raise ValueError("Dagster job requires its complete ordered partition list")
        return partitions

    def partition_work_unit(workload: str, index: int):
        """Create a static op so each later unit depends on its predecessor."""
        @op(
            # Definitions share one repository, so workload prefixes avoid
            # collisions while keeping a static op per partition work unit.
            name=f"{workload}_partition_{index:03d}",
            ins={"partitions": In(), "previous": In(Nothing)},
            out=Out(Nothing),
            retry_policy=RetryPolicy(max_retries=2, delay=1),
        )
        def _partition(context, partitions: list[dict]):
            item = partitions[index]
            run_adapter_partition(**item)
        return _partition
except ImportError:
    ordered_partitions = None
    partition_work_unit = None
