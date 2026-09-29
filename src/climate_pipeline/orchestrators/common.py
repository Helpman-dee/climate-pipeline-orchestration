from __future__ import annotations
from ..models import PartitionRequest
from ..models import record
from ..pipeline import run_partition

def run_adapter_partition(location_id: str, year: int, run_id: str, replay_base_url: str, fault_scenario: str | None = None, seed: int = 0, data_root: str = "data"):
    result = run_partition(PartitionRequest(location_id, year, run_id, replay_base_url, fault_scenario, seed), data_root)
    if result.status != "success":
        raise RuntimeError(result.error or "Shared pipeline failed")
    return record(result)
