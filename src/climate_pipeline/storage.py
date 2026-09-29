from __future__ import annotations
import os
from pathlib import Path
import duckdb
import pandas as pd
from .models import QualityResult

def audit_db(data_root: str | Path) -> Path:
    return Path(data_root) / "audit.duckdb"

def initialise(data_root: str | Path) -> None:
    path = audit_db(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as db:
        db.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id VARCHAR, location_id VARCHAR, year INTEGER, status VARCHAR, row_count INTEGER, checksum VARCHAR, started_at VARCHAR, finished_at VARCHAR, error VARCHAR, PRIMARY KEY(run_id, location_id, year))")
        db.execute("CREATE TABLE IF NOT EXISTS quality_incidents (run_id VARCHAR, location_id VARCHAR, year INTEGER, severity VARCHAR, code VARCHAR, message VARCHAR, row_count INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS expected_partitions (run_id VARCHAR, location_id VARCHAR, year INTEGER, row_count INTEGER, checksum VARCHAR, PRIMARY KEY(run_id, location_id, year))")
        db.execute("CREATE TABLE IF NOT EXISTS verification_results (run_id VARCHAR, status VARCHAR, expected_partition_count INTEGER, actual_partition_count INTEGER, failures_json VARCHAR)")

def write_partition(frame: pd.DataFrame, data_root: str | Path, location_id: str, year: int) -> Path:
    target = Path(data_root) / "curated" / "observations" / f"location_id={location_id}" / f"year={year}" / "data.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name("data.parquet.staging")
    frame.to_parquet(staging, index=False)
    os.replace(staging, target)
    return target

def curated_path(data_root: str | Path, location_id: str, year: int) -> Path:
    return Path(data_root) / "curated" / "observations" / f"location_id={location_id}" / f"year={year}" / "data.parquet"

def read_history(data_root: str | Path, location_id: str, year: int, days: int = 89) -> pd.DataFrame:
    frames = []
    for previous in (year - 1, year):
        path = curated_path(data_root, location_id, previous)
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    history = pd.concat(frames).sort_values("observation_date")
    return history[history["observation_date"] < pd.Timestamp(year=year, month=1, day=1)].tail(days)

def record_result(data_root: str | Path, result, issues: list[QualityResult]) -> None:
    initialise(data_root)
    with duckdb.connect(str(audit_db(data_root))) as db:
        db.execute("INSERT OR REPLACE INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [result.run_id, result.location_id, result.year, result.status, result.row_count, result.checksum, result.started_at, result.finished_at, result.error])
        db.execute("DELETE FROM quality_incidents WHERE run_id=? AND location_id=? AND year=?", [result.run_id, result.location_id, result.year])
        for issue in issues:
            db.execute("INSERT INTO quality_incidents VALUES (?, ?, ?, ?, ?, ?, ?)", [result.run_id, issue.location_id, issue.year, issue.severity, issue.code, issue.message, issue.row_count])

