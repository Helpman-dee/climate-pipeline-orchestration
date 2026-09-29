from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
from .acquisition import canonical_path
from .config import load_config
from .parsing import parse_power_file
from .models import VerificationResult
from .storage import audit_db, curated_path, initialise
from .checksum import source_observation_checksum

def _observation_checksum(frame: pd.DataFrame) -> str:
    return source_observation_checksum(frame)

def create_expected_manifest(run_id: str, partitions: list[tuple[str, int]], data_root: str | Path = "data", config_path: str | Path = "configs/dataset.yaml", canonical_data_root: str | Path | None = None) -> None:
    """Record expected output from canonical/curated-independent parsed source data in orchestration setup."""
    initialise(data_root)
    config = load_config(config_path)
    with duckdb.connect(str(audit_db(data_root))) as db:
        for location_id, year in partitions:
            source_root = canonical_data_root if canonical_data_root is not None else data_root
            frame = parse_power_file(canonical_path(source_root, config, location_id, year), location_id)
            db.execute("INSERT OR REPLACE INTO expected_partitions VALUES (?, ?, ?, ?, ?)", [run_id, location_id, year, len(frame), _observation_checksum(frame)])

def verify_run(run_id: str, data_root: str | Path = "data") -> VerificationResult:
    initialise(data_root)
    failures: list[str] = []
    with duckdb.connect(str(audit_db(data_root))) as db:
        expected = db.execute("SELECT location_id, year, row_count, checksum FROM expected_partitions WHERE run_id=? ORDER BY 1,2", [run_id]).fetchall()
    expected_keys = {(location_id, year) for location_id, year, _, _ in expected}
    for location_id, year, expected_rows, expected_checksum in expected:
        path = curated_path(data_root, location_id, year)
        if not path.exists():
            failures.append(f"missing partition {location_id}/{year}")
            continue
        frame = pd.read_parquet(path)
        if len(frame) != expected_rows: failures.append(f"unexpected row count {location_id}/{year}: {len(frame)}")
        if frame.duplicated(["location_id", "observation_date"]).any(): failures.append(f"duplicate natural keys {location_id}/{year}")
        if frame[["location_id", "observation_date"]].isna().any().any(): failures.append(f"required nulls {location_id}/{year}")
        if _observation_checksum(frame) != expected_checksum: failures.append(f"source-value checksum mismatch {location_id}/{year}")
    actual_paths = list((Path(data_root) / "curated" / "observations").glob("location_id=*/year=*/data.parquet")) if (Path(data_root) / "curated" / "observations").exists() else []
    actual_keys = {(path.parent.parent.name.split("=", 1)[1], int(path.parent.name.split("=", 1)[1])) for path in actual_paths}
    if actual_keys - expected_keys: failures.append(f"unexpected partitions: {sorted(actual_keys - expected_keys)}")
    actual = len(actual_paths)
    result = VerificationResult(run_id, "passed" if not failures else "failed", len(expected), actual, failures)
    with duckdb.connect(str(audit_db(data_root))) as db:
        db.execute("DELETE FROM verification_results WHERE run_id=?", [run_id])
        db.execute("INSERT INTO verification_results VALUES (?, ?, ?, ?, ?)", [run_id, result.status, result.expected_partition_count, result.actual_partition_count, str(result.failures)])
    return result
