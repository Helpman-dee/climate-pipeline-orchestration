import json

import pytest

from climate_pipeline.fault_experiments import _assert_condition, normalize_terminal_state
from climate_pipeline import persistent_submitter
from climate_pipeline.topology import PRIMARY_TOPOLOGY_VERSION


def test_successful_framework_terminal_states_normalize_to_semantic_success() -> None:
    assert normalize_terminal_state("success", 0) == "success"
    assert normalize_terminal_state("completed", 0) == "success"
    assert normalize_terminal_state("succeeded", 0) == "success"


def test_failed_framework_terminal_states_normalize_to_semantic_failure() -> None:
    assert normalize_terminal_state("failed", 1) == "failure"
    assert normalize_terminal_state("failure", 1) == "failure"
    assert normalize_terminal_state("crashed", 1) == "failure"


def test_invalid_observation_allows_unobserved_partition_concurrency() -> None:
    record = {
        "workload_size": "medium",
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "configured_partition_concurrency": 1,
        "observed_max_partition_concurrency": None,
        "worker_sampling_coverage_percent": 100.0,
        "control_plane_sampling_coverage_percent": 100.0,
        "worker_sampler_errors": [],
        "control_plane_sampler_errors": [],
        "semantic_terminal_state": "failure",
        "quality_incidents": [{"severity": "hard", "code": "precipitation_bounds"}],
        "verification_failures": ["missing partition output"],
        "output_row_count": 0,
        "duplicate_output_count": 0,
    }

    _assert_condition("invalid_observation", record)


def test_invalid_observation_rejects_observed_concurrency_divergence() -> None:
    record = {
        "workload_size": "medium",
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "configured_partition_concurrency": 1,
        "observed_max_partition_concurrency": 2,
        "worker_sampling_coverage_percent": 100.0,
        "control_plane_sampling_coverage_percent": 100.0,
        "worker_sampler_errors": [],
        "control_plane_sampler_errors": [],
    }

    with pytest.raises(RuntimeError, match="observed partition concurrency diverged"):
        _assert_condition("invalid_observation", record)


def test_failed_submission_retains_submitted_topology(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    items = [
        {
            "location_id": f"location-{index}",
            "year": 2000 + index,
            "run_id": "test-run",
            "replay_base_url": "http://replay:8000",
            "seed": 1,
            "data_root": "/tmp/test-run",
        }
        for index in range(25)
    ]

    def fail_submission(items: list[dict], timeout: int) -> None:
        raise RuntimeError("expected workflow failure")

    monkeypatch.setattr(persistent_submitter, "_airflow", fail_submission)
    result = persistent_submitter.main(
        ["--framework", "airflow", "--items-json", json.dumps(items), "--data-root", str(tmp_path)]
    )
    submission = json.loads((tmp_path / "submission_result.json").read_text(encoding="utf-8"))

    assert result == 1
    assert submission["topology_version"] == PRIMARY_TOPOLOGY_VERSION
    assert submission["top_level_workflow_run_count"] == 1
    assert submission["partition_work_unit_count"] == 25
    assert submission["configured_partition_concurrency"] == 1
