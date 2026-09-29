"""Versioned benchmark-topology metadata and deterministic partition order."""
from __future__ import annotations

from typing import Any


LEGACY_TOPOLOGY_VERSION = "legacy_per_partition_top_level_v1"
PRIMARY_TOPOLOGY_VERSION = "single_workflow_partition_units_v2"
PARTITION_CONCURRENCY = 1


def ordered_partition_list(items: list[dict[str, Any]]) -> list[dict[str, int | str]]:
    """Return the submitted configured-location/year ordering without secrets."""
    return [{"location_id": str(item["location_id"]), "year": int(item["year"])} for item in items]


def topology_metadata(items: list[dict[str, Any]], observed_max_partition_concurrency: int | None = None) -> dict[str, Any]:
    return {
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "top_level_workflow_run_count": 1,
        "partition_work_unit_count": len(items),
        "configured_partition_concurrency": PARTITION_CONCURRENCY,
        # The submitting client cannot observe worker execution. The harness
        # replaces this with the value calculated from audit timestamps.
        "observed_max_partition_concurrency": observed_max_partition_concurrency,
        "ordered_partition_list": ordered_partition_list(items),
    }
