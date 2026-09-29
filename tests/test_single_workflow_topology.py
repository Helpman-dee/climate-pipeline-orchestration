from climate_pipeline.persistent_submitter import _workload_name
from climate_pipeline.benchmark import _fill_missing_topology_metadata, _observed_partition_concurrency
from climate_pipeline.topology import (
    PARTITION_CONCURRENCY,
    PRIMARY_TOPOLOGY_VERSION,
    ordered_partition_list,
    topology_metadata,
)


def test_topology_metadata_keeps_configured_location_year_order():
    items = [
        {"location_id": "lagos", "year": 2001},
        {"location_id": "lagos", "year": 2002},
        {"location_id": "abuja", "year": 2001},
    ]

    assert ordered_partition_list(items) == [
        {"location_id": "lagos", "year": 2001},
        {"location_id": "lagos", "year": 2002},
        {"location_id": "abuja", "year": 2001},
    ]
    metadata = topology_metadata(items)
    assert metadata["topology_version"] == PRIMARY_TOPOLOGY_VERSION
    assert metadata["top_level_workflow_run_count"] == 1
    assert metadata["partition_work_unit_count"] == 3
    assert metadata["configured_partition_concurrency"] == PARTITION_CONCURRENCY == 1
    assert metadata["observed_max_partition_concurrency"] is None
    assert metadata["ordered_partition_list"] == ordered_partition_list(items)


def test_missing_submission_topology_is_filled_from_submitted_items():
    items = [
        {"location_id": "lagos", "year": 2001},
        {"location_id": "abuja", "year": 2002},
    ]
    submission = {"status": "error", "topology_version": "unexpected"}

    _fill_missing_topology_metadata(submission, items)

    assert submission["topology_version"] == "unexpected"
    assert submission["top_level_workflow_run_count"] == 1
    assert submission["partition_work_unit_count"] == 2
    assert submission["configured_partition_concurrency"] == PARTITION_CONCURRENCY
    assert submission["ordered_partition_list"] == ordered_partition_list(items)
    assert "observed_max_partition_concurrency" not in submission


def test_single_workflow_submitter_uses_known_workload_sizes():
    assert _workload_name([{}]) == "small"
    assert _workload_name([{}] * 25) == "medium"
    assert _workload_name([{}] * 120) == "large"


def test_observed_concurrency_uses_durable_partition_audit(tmp_path):
    import duckdb

    database = duckdb.connect(str(tmp_path / "audit.duckdb"))
    database.execute("CREATE TABLE pipeline_runs (run_id VARCHAR, status VARCHAR, started_at VARCHAR, finished_at VARCHAR)")
    database.execute(
        "INSERT INTO pipeline_runs VALUES "
        "('run-1', 'success', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:01+00:00'), "
        "('run-1', 'success', '2026-01-01T00:00:01+00:00', '2026-01-01T00:00:02+00:00')"
    )
    database.close()

    assert _observed_partition_concurrency(tmp_path, "run-1") == 1
