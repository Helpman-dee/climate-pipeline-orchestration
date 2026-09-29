import json
from pathlib import Path

import pytest

from climate_pipeline import backfill_experiments as backfill
from climate_pipeline.topology import PRIMARY_TOPOLOGY_VERSION


def _evidence() -> dict:
    return {
        "file_count": 25,
        "partition_count": 25,
        "row_count": 9130,
        "natural_key_duplicate_count": 0,
        "logical_checksum": backfill.EXPECTED_MEDIUM_CHECKSUM,
        "schema_status": "passed",
    }


def _record() -> dict:
    evidence = _evidence()
    return {
        "experiment_type": "framework_native_reprocessing_v1",
        "workload_size": "medium",
        "requested_partitions": 25,
        "completed_partitions": 25,
        "successful_partitions": 25,
        "failed_partitions": 0,
        "duplicate_output_count": 0,
        "verification_status": "passed",
        "output_logical_checksum": backfill.EXPECTED_MEDIUM_CHECKSUM,
        "resource_validation_status": "passed",
        "semantic_terminal_state": "success",
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "configured_partition_concurrency": 1,
        "observed_max_partition_concurrency": 1,
        "worker_sampling_coverage_percent": 100.0,
        "control_plane_sampling_coverage_percent": 100.0,
        "worker_sampler_errors": [],
        "control_plane_sampler_errors": [],
        "before_output_evidence": evidence,
        "after_output_evidence": evidence,
        "idempotency_output_overwrite": {
            "preexisting_partition_count": 25,
            "post_reprocessing_partition_count": 25,
            "duplicate_output_files_created": 0,
            "deterministic_atomic_replace": True,
        },
    }


def test_prepopulation_copies_verified_curated_outputs_without_touching_reference(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    reference = tmp_path / "reference"
    (reference / "curated").mkdir(parents=True)
    (reference / "curated" / "baseline.parquet").write_bytes(b"immutable baseline")
    (reference / "reference_summary.json").write_text(json.dumps({"verification": "passed", "checksum": backfill.EXPECTED_MEDIUM_CHECKSUM}), encoding="utf-8")
    monkeypatch.setattr(backfill, "REFERENCE_ROOT", reference)
    monkeypatch.setattr(backfill, "_output_evidence", lambda root: _evidence())

    target = tmp_path / "isolated-output"
    before = backfill._prepopulate_output(target)

    assert before == _evidence()
    assert (target / "curated" / "baseline.parquet").read_bytes() == b"immutable baseline"
    assert (reference / "curated" / "baseline.parquet").read_bytes() == b"immutable baseline"


def test_isolated_output_root_is_resolved_before_docker_path_conversion(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)

    assert backfill._repository_relative(Path("data") / "benchmarks" / "backfill") == Path("data") / "benchmarks" / "backfill"


def test_reprocessing_assertion_requires_overwrite_and_sampling_evidence() -> None:
    record = _record()
    backfill._assert_reprocessing_record(record)

    record["idempotency_output_overwrite"]["duplicate_output_files_created"] = 1
    with pytest.raises(RuntimeError, match="overwrite/idempotency"):
        backfill._assert_reprocessing_record(record)


def test_valid_completed_reprocessing_record_is_resumed(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(backfill, "BACKFILL_RAW_ROOT", tmp_path)
    path = tmp_path / "framework-native-reprocessing-airflow-medium-measured-1-test.json"
    path.write_text(json.dumps(_record()), encoding="utf-8")

    assert backfill._saved_valid_record("airflow") == _record()
