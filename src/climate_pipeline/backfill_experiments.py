"""Isolated framework-native reprocessing experiment for RQ4.

The application topology intentionally uses one ordered, parameter-driven
workflow rather than scheduler or asset partitions.  This module therefore
records each framework's genuine *framework-native reprocessing* submission
mechanism without representing the result as an equivalent historical-
partition API.
"""
from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .benchmark import (
    FRAMEWORKS,
    PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT,
    _load_yaml,
    _logical_output,
    _prepare_persistent_stack,
    _run_persistent_one,
    _stop_persistent_stack,
    validate_benchmark_plan,
    workload_partitions,
)
from .fault_experiments import normalize_terminal_state
from .topology import PARTITION_CONCURRENCY, PRIMARY_TOPOLOGY_VERSION


BACKFILL_ROOT = Path("data/benchmarks/backfill")
BACKFILL_RAW_ROOT = BACKFILL_ROOT / "raw"
BACKFILL_STATE_ROOT = BACKFILL_ROOT / "state"
BACKFILL_OUTPUT_ROOT = BACKFILL_ROOT / "outputs"
BACKFILL_EVALUATION_ROOT = BACKFILL_ROOT / "evaluations"
REFERENCE_ROOT = Path("data/benchmarks/reference/medium-single-workflow-v2")
EXPECTED_MEDIUM_CHECKSUM = "d2d7ba2ddcee843ca27216cec361873203ec3165459705afd37f1b2317257dbb"

NATIVE_REPROCESSING_MECHANISMS = {
    "airflow": "Airflow native labelled DagRun submitted with `airflow dags trigger`",
    "prefect": "Prefect native labelled deployment flow run created through the deployment API",
    "dagster": "Dagster native labelled job run launched through the GraphQL run API",
}

OPERATIONAL_STEPS = {
    "airflow": [
        "Start the warmed Airflow control plane.",
        "Submit one new labelled medium DAG run with the ordered partition configuration.",
    ],
    "prefect": [
        "Start Prefect Server, register the existing deployment, and start its one-worker pool.",
        "Create one new labelled deployment flow run with the ordered partition parameters.",
    ],
    "dagster": [
        "Start the Dagster code server, webserver, and daemon.",
        "Launch one new labelled job run through Dagster's GraphQL run API with the ordered run config.",
    ],
}


def _output_evidence(root: Path) -> dict[str, Any]:
    """Return read-only logical evidence for a curated output tree."""
    rows, duplicates, checksum, schema_problem = _logical_output(root)
    files = sorted((root / "curated" / "observations").glob("location_id=*/year=*/data.parquet")) if (root / "curated" / "observations").exists() else []
    return {
        "file_count": len(files),
        "partition_count": len(files),
        "row_count": rows,
        "natural_key_duplicate_count": duplicates,
        "logical_checksum": checksum,
        "schema_status": "passed" if schema_problem is None else schema_problem,
    }


def _reference_evidence() -> dict[str, Any]:
    """Validate the immutable verified baseline without writing into it."""
    summary_path = REFERENCE_ROOT / "reference_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Verified medium reference summary is missing: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    evidence = _output_evidence(REFERENCE_ROOT)
    if (
        summary.get("verification") != "passed"
        or summary.get("checksum") != EXPECTED_MEDIUM_CHECKSUM
        or evidence["file_count"] != 25
        or evidence["natural_key_duplicate_count"] != 0
        or evidence["logical_checksum"] != EXPECTED_MEDIUM_CHECKSUM
        or evidence["schema_status"] != "passed"
    ):
        raise RuntimeError(f"Immutable medium reference is not suitable for reprocessing setup: {summary}, {evidence}")
    return evidence


def _prepopulate_output(output_root: Path) -> dict[str, Any]:
    """Copy only verified curated output into a new, framework-isolated root."""
    reference = _reference_evidence()
    if output_root.exists():
        raise FileExistsError(f"Refusing to replace existing reprocessing output evidence: {output_root}")
    source = REFERENCE_ROOT / "curated"
    output_root.mkdir(parents=True)
    shutil.copytree(source, output_root / "curated")
    evidence = _output_evidence(output_root)
    if evidence != reference:
        raise RuntimeError(f"Pre-populated output differs from immutable reference: {evidence} != {reference}")
    return evidence


def _repository_relative(path: Path) -> Path:
    """Return a Docker-mounted repository-relative path for an output root."""
    try:
        return path.resolve().relative_to(Path.cwd().resolve())
    except ValueError as exc:
        raise ValueError(f"Reprocessing output root must be inside the repository: {path}") from exc


def _coverage_ok(record: dict[str, Any]) -> bool:
    return all(
        isinstance(record.get(key), (int, float)) and record[key] >= PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT
        for key in ("worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent")
    ) and not record.get("worker_sampler_errors") and not record.get("control_plane_sampler_errors")


def _assert_reprocessing_record(record: dict[str, Any]) -> None:
    """Apply the pre-registered RQ4 success and overwrite expectations."""
    required = {
        "experiment_type": "framework_native_reprocessing_v1",
        "workload_size": "medium",
        "requested_partitions": 25,
        "completed_partitions": 25,
        "successful_partitions": 25,
        "failed_partitions": 0,
        "duplicate_output_count": 0,
        "verification_status": "passed",
        "output_logical_checksum": EXPECTED_MEDIUM_CHECKSUM,
        "resource_validation_status": "passed",
        "semantic_terminal_state": "success",
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "configured_partition_concurrency": PARTITION_CONCURRENCY,
        "observed_max_partition_concurrency": PARTITION_CONCURRENCY,
    }
    failures = [f"{key}={record.get(key)!r}, expected {value!r}" for key, value in required.items() if record.get(key) != value]
    before = record.get("before_output_evidence", {})
    after = record.get("after_output_evidence", {})
    if before.get("file_count") != 25 or before.get("natural_key_duplicate_count") != 0 or before.get("logical_checksum") != EXPECTED_MEDIUM_CHECKSUM:
        failures.append(f"unexpected before-output evidence: {before}")
    if after.get("file_count") != 25 or after.get("natural_key_duplicate_count") != 0 or after.get("logical_checksum") != EXPECTED_MEDIUM_CHECKSUM:
        failures.append(f"unexpected after-output evidence: {after}")
    overwrite = record.get("idempotency_output_overwrite", {})
    if overwrite.get("preexisting_partition_count") != 25 or overwrite.get("post_reprocessing_partition_count") != 25 or overwrite.get("duplicate_output_files_created") != 0 or overwrite.get("deterministic_atomic_replace") is not True:
        failures.append(f"overwrite/idempotency assertion failed: {overwrite}")
    if not _coverage_ok(record):
        failures.append("sampling acceptance failed")
    if failures:
        raise RuntimeError("framework-native reprocessing: " + "; ".join(failures))


def _saved_valid_record(framework: str) -> dict[str, Any] | None:
    for path in sorted(BACKFILL_RAW_ROOT.glob(f"framework-native-reprocessing-{framework}-medium-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            _assert_reprocessing_record(record)
            return record
        except (OSError, json.JSONDecodeError, RuntimeError):
            continue
    return None


def run_framework_native_reprocessing(plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """Run one isolated medium reprocessing experiment per framework."""
    validation = validate_benchmark_plan(plan_path)
    if not validation["replay_healthy"] or not all(validation["framework_images_available"].values()):
        raise RuntimeError(f"Framework-native reprocessing prerequisites are not ready: {validation}")
    plan = _load_yaml(plan_path)
    partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "medium")
    if len(partitions) != 25:
        raise RuntimeError(f"Framework-native reprocessing requires the configured 25 medium partitions, got {len(partitions)}")
    for root in (BACKFILL_RAW_ROOT, BACKFILL_STATE_ROOT, BACKFILL_OUTPUT_ROOT, BACKFILL_EVALUATION_ROOT):
        root.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    for framework in FRAMEWORKS:
        saved = _saved_valid_record(framework)
        if saved is not None:
            results.append(saved)
            print(f"PASS framework={framework} experiment=framework-native-reprocessing resumed=true", flush=True)
            continue

        run_id = f"framework-native-reprocessing-{framework}-medium-measured-1-{uuid.uuid4().hex[:8]}"
        output_root = BACKFILL_OUTPUT_ROOT / run_id
        before = _prepopulate_output(output_root)
        # Stack setup and output pre-population are intentionally outside the
        # run timestamps written by the framework-native submission client.
        _prepare_persistent_stack(framework, reuse_local_image=True)
        try:
            base = _run_persistent_one(
                plan,
                framework,
                "medium",
                "normal",
                "framework_native_reprocessing",
                1,
                partitions,
                BACKFILL_RAW_ROOT,
                state_root=BACKFILL_STATE_ROOT,
                execution_root=_repository_relative(output_root),
                run_id=run_id,
            )
            record = asdict(base)
            native_state = json.loads((output_root / "submission_result.json").read_text(encoding="utf-8")).get("terminal_state")
            after = _output_evidence(output_root)
            record.update({
                "experiment_type": "framework_native_reprocessing_v1",
                "framework_native_reprocessing_mechanism": NATIVE_REPROCESSING_MECHANISMS[framework],
                "framework_native_reprocessing_label": run_id,
                "framework_native_reprocessing_label_transport": "The unique framework-native-reprocessing run_id is included in every ordered partition item; Airflow uses it in the DagRun ID and Prefect/Dagster retain it in native run configuration.",
                "requested_partitions": len(partitions),
                "completed_partitions": record["successful_partitions"],
                "native_orchestration_state": native_state,
                "final_orchestration_state": native_state,
                "semantic_terminal_state": normalize_terminal_state(native_state, record["worker_exit_code"]),
                "framework_native_reprocessing_results_root": str(BACKFILL_ROOT),
                "isolated_output_root": str(output_root),
                "before_output_evidence": before,
                "after_output_evidence": after,
                "idempotency_output_overwrite": {
                    "storage_write_method": "atomic os.replace at fixed location_id/year data.parquet path",
                    "preexisting_partition_count": before["partition_count"],
                    "post_reprocessing_partition_count": after["partition_count"],
                    "duplicate_output_files_created": max(0, after["file_count"] - before["file_count"]),
                    "deterministic_atomic_replace": before["file_count"] == 25 and after["file_count"] == 25 and after["natural_key_duplicate_count"] == 0 and after["logical_checksum"] == EXPECTED_MEDIUM_CHECKSUM,
                },
                "framework_specific_operational_steps": OPERATIONAL_STEPS[framework],
            })
            _assert_reprocessing_record(record)
            raw_path = BACKFILL_RAW_ROOT / f"{run_id}.json"
            raw_path.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
            (BACKFILL_EVALUATION_ROOT / f"{run_id}.json").write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
            results.append(record)
            print(
                f"PASS framework={framework} experiment=framework-native-reprocessing "
                f"runtime={record['end_to_end_runtime_seconds']:.3f}s verification={record['verification_status']}",
                flush=True,
            )
        finally:
            _stop_persistent_stack(framework)

    print("| framework | mechanism | runtime_s | completed | failed | duplicates | verification | checksum |", flush=True)
    print("|---|---|---:|---:|---:|---:|---|---|", flush=True)
    for record in results:
        print(
            f"| {record['orchestrator']} | {record['framework_native_reprocessing_mechanism']} | "
            f"{record['end_to_end_runtime_seconds']:.3f} | {record['completed_partitions']} | "
            f"{record['failed_partitions']} | {record['duplicate_output_count']} | "
            f"{record['verification_status']} | {record['output_logical_checksum'] or '-'} |",
            flush=True,
        )
    return results
