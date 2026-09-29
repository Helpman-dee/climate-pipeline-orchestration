from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import json
import requests

from .config import load_config
from .checksum import logical_content_checksum
from .acquisition import canonical_path
from .models import PartitionRequest, PartitionResult
from .parsing import parse_power_payload
from .storage import read_history, record_result, write_partition
from .transform import create_features
from .util import partition_dates
from .validation import hard_failures, validate_observations


def run_canonical_partition(location_id: str, year: int, run_id: str, data_root: str | Path = "data", config_path: str | Path = "configs/dataset.yaml") -> PartitionResult:
    """Process one immutable canonical NASA POWER snapshot without an orchestration adapter."""
    config = load_config(config_path)
    result = PartitionResult(run_id, location_id, year, "running")
    issues = []
    try:
        source_path = canonical_path(data_root, config, location_id, year)
        content = source_path.read_bytes()
        frame = parse_power_payload(json.loads(content), location_id)
        issues = validate_observations(frame, location_id, year, config)
        if hard_failures(issues):
            result.status, result.error = "quality_failed", "Hard data-quality gate failed"
        else:
            featured = create_features(frame, read_history(data_root, location_id, year))
            write_partition(featured, data_root, location_id, year)
            result.status, result.row_count = "success", len(featured)
            result.checksum = logical_content_checksum(featured)
    except Exception as exc:
        result.status, result.error = "failed", f"{type(exc).__name__}: {exc}"
    result.finished_at = datetime.now(timezone.utc).isoformat()
    result.quality_results = issues
    record_result(data_root, result, issues)
    return result

def run_partition(request: PartitionRequest, data_root: str | Path = "data", config_path: str | Path = "configs/dataset.yaml") -> PartitionResult:
    """Execute all business logic for one location-year; adapter-independent and retry-safe."""
    config = load_config(config_path)
    result = PartitionResult(request.run_id, request.location_id, request.year, "running")
    issues = []
    try:
        start, end = partition_dates(request.year)
        params = {"parameters": ",".join(config["parameters"]), "community": config["source"]["community"], "start": start, "end": end, "format": "JSON", "time-standard": config["source"]["time_standard"], "location_id": request.location_id, "fault_scenario": request.fault_scenario or "", "seed": request.seed}
        response = requests.get(f"{request.replay_base_url.rstrip('/')}/api/temporal/daily/point", params=params, timeout=request.timeout_seconds)
        response.raise_for_status()
        content = response.content
        raw_path = Path(data_root) / "raw" / "runs" / request.run_id / request.location_id / f"{request.year}.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if raw_path.exists() and raw_path.read_bytes() != content:
            raise FileExistsError(f"Run raw input changed on retry: {raw_path}")
        if not raw_path.exists():
            raw_path.write_bytes(content)
        frame = parse_power_payload(json.loads(content), request.location_id)
        issues = validate_observations(frame, request.location_id, request.year, config)
        if hard_failures(issues):
            result.status, result.error = "quality_failed", "Hard data-quality gate failed"
        else:
            if request.fault_scenario == "worker_failure":
                raise RuntimeError("Injected worker failure")
            featured = create_features(frame, read_history(data_root, request.location_id, request.year))
            if request.fault_scenario == "storage_failure" or response.headers.get("X-Climate-Fault") == "transient_storage":
                raise OSError("Injected downstream storage failure")
            write_partition(featured, data_root, request.location_id, request.year)
            result.status, result.row_count = "success", len(featured)
            result.checksum = logical_content_checksum(featured)
    except Exception as exc:
        result.status, result.error = "failed", f"{type(exc).__name__}: {exc}"
    result.finished_at = datetime.now(timezone.utc).isoformat()
    result.quality_results = issues
    record_result(data_root, result, issues)
    return result
