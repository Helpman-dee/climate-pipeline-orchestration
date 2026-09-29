from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

@dataclass(frozen=True)
class PartitionRequest:
    location_id: str
    year: int
    run_id: str
    replay_base_url: str
    fault_scenario: str | None = None
    seed: int = 0
    timeout_seconds: float = 10.0

@dataclass
class QualityResult:
    severity: str
    code: str
    message: str
    location_id: str
    year: int
    row_count: int = 0

@dataclass
class PartitionResult:
    run_id: str
    location_id: str
    year: int
    status: str
    row_count: int = 0
    checksum: str | None = None
    quality_results: list[QualityResult] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: str | None = None
    error: str | None = None

@dataclass
class DatasetManifest:
    dataset_version: str
    entries: list[dict[str, Any]]
    created_at: str

@dataclass
class VerificationResult:
    run_id: str
    status: str
    expected_partition_count: int
    actual_partition_count: int
    failures: list[str] = field(default_factory=list)

def record(value: Any) -> dict[str, Any]:
    return asdict(value) if hasattr(value, "__dataclass_fields__") else value

