"""Isolated, reproducible fault-recovery and data-quality experiment matrix."""
from __future__ import annotations

import json
import ast
from dataclasses import asdict
from pathlib import Path
from typing import Any

import duckdb

from .benchmark import (
    PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT,
    _load_yaml,
    _prepare_persistent_stack,
    _replay_attempts,
    _run_persistent_one,
    _stop_persistent_stack,
    validate_benchmark_plan,
    workload_partitions,
)
from .topology import PARTITION_CONCURRENCY, PRIMARY_TOPOLOGY_VERSION


FAULT_ROOT = Path("data/benchmarks/fault_recovery")
FAULT_RAW_ROOT = FAULT_ROOT / "raw"
FAULT_STATE_ROOT = FAULT_ROOT / "state"
FAULT_EVALUATION_ROOT = FAULT_ROOT / "evaluations"
EXPECTED_MEDIUM_CHECKSUM = "d2d7ba2ddcee843ca27216cec361873203ec3165459705afd37f1b2317257dbb"
CONDITIONS = ("transient_http", "transient_storage", "invalid_observation")
FRAMEWORKS = ("airflow", "prefect", "dagster")


def normalize_terminal_state(native_state: str | None, worker_exit_code: int | None) -> str:
    """Normalize framework-native terminal states without discarding evidence."""
    state = (native_state or "").strip().lower()
    if state in {"success", "completed", "succeeded"}:
        return "success"
    if state in {"failed", "failure", "crashed", "cancelled", "canceled", "error"}:
        return "failure"
    return "success" if worker_exit_code == 0 else "failure"


def _quality_incidents(state_root: Path, run_id: str) -> list[dict[str, Any]]:
    with duckdb.connect(str(state_root / "audit.duckdb"), read_only=True) as database:
        rows = database.execute(
            "SELECT location_id, year, severity, code, message, row_count "
            "FROM quality_incidents WHERE run_id=? ORDER BY location_id, year, code",
            [run_id],
        ).fetchall()
    return [
        {"location_id": location_id, "year": year, "severity": severity, "code": code, "message": message, "row_count": row_count}
        for location_id, year, severity, code, message, row_count in rows
    ]


def _verification_failures(state_root: Path, run_id: str) -> list[str]:
    with duckdb.connect(str(state_root / "audit.duckdb"), read_only=True) as database:
        row = database.execute("SELECT failures_json FROM verification_results WHERE run_id=?", [run_id]).fetchone()
    return [] if row is None else list(ast.literal_eval(row[0]))


def _coverage_ok(record: dict[str, Any]) -> bool:
    return all(
        isinstance(record.get(key), (int, float)) and record[key] >= PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT
        for key in ("worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent")
    ) and not record.get("worker_sampler_errors") and not record.get("control_plane_sampler_errors")


def _assert_condition(condition: str, record: dict[str, Any]) -> None:
    if record["workload_size"] != "medium" or record["topology_version"] != PRIMARY_TOPOLOGY_VERSION:
        raise RuntimeError(f"{condition}: topology/workload diverged")
    if record["configured_partition_concurrency"] != PARTITION_CONCURRENCY:
        raise RuntimeError(f"{condition}: partition concurrency diverged")
    observed_concurrency = record["observed_max_partition_concurrency"]
    if observed_concurrency != PARTITION_CONCURRENCY and not (condition == "invalid_observation" and observed_concurrency is None):
        raise RuntimeError(f"{condition}: observed partition concurrency diverged")
    if not _coverage_ok(record):
        raise RuntimeError(f"{condition}: sampling acceptance failed")
    if condition in {"transient_http", "transient_storage"}:
        required = {
            "semantic_terminal_state": "success",
            "retry_count": 25,
            "successful_partitions": 25,
            "failed_partitions": 0,
            "duplicate_output_count": 0,
            "verification_status": "passed",
            "output_logical_checksum": EXPECTED_MEDIUM_CHECKSUM,
            "resource_validation_status": "passed",
        }
        failures = [f"{key}={record.get(key)!r}, expected {value!r}" for key, value in required.items() if record.get(key) != value]
        if record.get("failure_recovery_seconds") is None:
            failures.append("failure_recovery_seconds is missing")
        if failures:
            raise RuntimeError(f"{condition}: " + "; ".join(failures))
        return
    incidents = record.get("quality_incidents", [])
    hard_precipitation = any(item.get("severity") == "hard" and item.get("code") == "precipitation_bounds" for item in incidents)
    missing_output = any("missing partition" in failure for failure in record.get("verification_failures", []))
    if record.get("semantic_terminal_state") != "failure" or not hard_precipitation or not missing_output or record.get("output_row_count") != 0 or record.get("duplicate_output_count") != 0:
        raise RuntimeError(f"invalid_observation: blocked-output assertion failed: {record}")


def _enrich_record(record: dict[str, Any]) -> dict[str, Any]:
    """Attach fault evidence while retaining the native control-plane state."""
    state_root = FAULT_STATE_ROOT / record["run_id"]
    submission_path = state_root / "submission_result.json"
    submission = json.loads(submission_path.read_text(encoding="utf-8")) if submission_path.exists() else {}
    native_state = submission.get("terminal_state") or record.get("native_orchestration_state") or record.get("final_orchestration_state")
    record["native_orchestration_state"] = native_state
    record["semantic_terminal_state"] = normalize_terminal_state(native_state, record.get("worker_exit_code"))
    record["quality_incidents"] = _quality_incidents(state_root, record["run_id"])
    record["verification_failures"] = _verification_failures(state_root, record["run_id"])
    record["fault_results_root"] = str(FAULT_ROOT)
    record["experiment_type"] = "fault_recovery_data_quality_v1"
    record["condition"] = record.get("condition") or record["scenario"]
    return record


def _saved_valid_fault_record(framework: str, condition: str) -> dict[str, Any] | None:
    for path in sorted(FAULT_RAW_ROOT.glob(f"persistent-{framework}-medium-{condition}-measured-1-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            record = _enrich_record(json.loads(path.read_text(encoding="utf-8")))
            _assert_condition(condition, record)
            return record
        except (OSError, json.JSONDecodeError, duckdb.Error, RuntimeError):
            continue
    return None


def run_fault_recovery_matrix(plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """Run the nine medium fault experiments without touching normal records."""
    validation = validate_benchmark_plan(plan_path)
    if not validation["replay_healthy"] or not all(validation["framework_images_available"].values()):
        raise RuntimeError(f"Fault experiment prerequisites are not ready: {validation}")
    plan = _load_yaml(plan_path)
    partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "medium")
    if len(partitions) != 25:
        raise RuntimeError(f"Fault matrix requires the configured 25 medium partitions, got {len(partitions)}")
    FAULT_RAW_ROOT.mkdir(parents=True, exist_ok=True)
    FAULT_STATE_ROOT.mkdir(parents=True, exist_ok=True)
    FAULT_EVALUATION_ROOT.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for framework in FRAMEWORKS:
        # Fault experiments use the already-built local image so setup cannot
        # trigger a network-dependent rebuild before the measurement boundary.
        _prepare_persistent_stack(framework, reuse_local_image=True)
        try:
            for condition in CONDITIONS:
                saved = _saved_valid_fault_record(framework, condition)
                if saved is not None:
                    results.append(saved)
                    print(f"PASS framework={framework} condition={condition} resumed=true", flush=True)
                    continue
                base = _run_persistent_one(
                    plan, framework, "medium", condition, "measured", 1, partitions, FAULT_RAW_ROOT,
                    state_root=FAULT_STATE_ROOT, raise_on_failure=False,
                )
                record = asdict(base)
                attempts = _replay_attempts(plan.get("replay_control_url", "http://127.0.0.1:8000"))
                total_attempts = sum(item["attempts"] for item in attempts.get("attempts", []))
                initial_attempts = len(attempts.get("attempts", []))
                record.update({
                    "initial_attempt_count": initial_attempts,
                    "total_replay_attempt_count": total_attempts,
                    "retry_count": total_attempts - initial_attempts,
                })
                record = _enrich_record(record)
                _assert_condition(condition, record)
                path = FAULT_RAW_ROOT / f"{record['run_id']}.json"
                path.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
                (FAULT_EVALUATION_ROOT / f"{record['run_id']}.json").write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
                results.append(record)
                print(
                    f"PASS framework={framework} condition={condition} runtime={record['end_to_end_runtime_seconds']:.3f}s "
                    f"retries={record['retry_count']} verification={record['verification_status']}",
                    flush=True,
                )
        finally:
            _stop_persistent_stack(framework)
    print("| framework | condition | runtime_s | retries | successful | failed | duplicates | verification | checksum |", flush=True)
    print("|---|---|---:|---:|---:|---:|---:|---|---|", flush=True)
    for record in results:
        print(
            f"| {record['orchestrator']} | {record['condition']} | {record['end_to_end_runtime_seconds']:.3f} | "
            f"{record['retry_count']} | {record['successful_partitions']} | {record['failed_partitions']} | "
            f"{record['duplicate_output_count']} | {record['verification_status']} | {record['output_logical_checksum'] or '-'} |",
            flush=True,
        )
    return results
