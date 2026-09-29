from datetime import datetime, timedelta, timezone
from subprocess import CompletedProcess
from unittest.mock import patch

import json

from climate_pipeline import benchmark
from climate_pipeline.benchmark import _PersistentProcessTreeSampler, _prepare_persistent_dagster, _prepare_persistent_prefect, _summarise_benchmark_stack_memory, _summarise_persistent_samples


def _sample(timestamp: datetime) -> dict:
    return {"timestamp": timestamp.isoformat(), "service": "worker", "cpu": 12.5, "memory": 1234}


def test_persistent_sampling_rejects_insufficient_coverage() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=5)
    # Two observations in a five-second, 0.5 second cadence are only 20% of
    # the expected ten samples and must fail validation.
    summary = _summarise_persistent_samples([_sample(start + timedelta(seconds=0.5)), _sample(start + timedelta(seconds=1.0))], ["worker"], start.isoformat(), end.isoformat())
    assert summary["expected_sample_count"] == 10
    assert summary["sampling_coverage_percent"] == 20.0
    assert summary["status"] == "failed_sampling_coverage"


def test_persistent_sampling_accepts_required_coverage() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=5)
    samples = [_sample(start + timedelta(seconds=index * 0.5)) for index in range(1, 8)]
    summary = _summarise_persistent_samples(samples, ["worker"], start.isoformat(), end.isoformat())
    assert summary["expected_sample_count"] == 10
    assert summary["sampling_coverage_percent"] == 70.0
    assert summary["status"] == "measured"


def test_cgroup_memory_is_primary_and_process_rss_is_diagnostic() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sample = _sample(start + timedelta(seconds=0.5))
    sample.update({"cgroup_memory_bytes": 1000, "memory": 1000, "process_tree_rss_bytes": 5000})
    summary = _summarise_persistent_samples([sample], ["worker"], expected_duration_seconds=0.5)
    assert summary["memory_measurement"] == "cgroup_v2_memory_current"
    assert summary["average_memory_bytes"] == 1000
    assert summary["diagnostic_process_tree_rss_average_bytes"] == 5000


def test_stack_memory_sums_cgroup_totals_once_per_complete_round() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    samples = [
        {"timestamp": (start + timedelta(seconds=0.5)).isoformat(), "service": "control", "cgroup_memory_bytes": 100},
        {"timestamp": (start + timedelta(seconds=0.6)).isoformat(), "service": "worker", "cgroup_memory_bytes": 200},
        {"timestamp": (start + timedelta(seconds=1.0)).isoformat(), "service": "control", "cgroup_memory_bytes": 110},
        {"timestamp": (start + timedelta(seconds=1.1)).isoformat(), "service": "worker", "cgroup_memory_bytes": 210},
    ]
    summary = _summarise_benchmark_stack_memory(samples, ["control", "worker"])
    assert summary["valid_sample_count"] == 2
    assert summary["average_memory_bytes"] == 310
    assert summary["peak_memory_bytes"] == 320


def test_long_duration_coverage_is_calculated_from_actual_half_second_samples() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=120)
    # Controlled long-duration validation: 216 actual observations over 120
    # seconds is 90% coverage, above the mandatory 70% threshold.
    samples = [_sample(start + timedelta(seconds=index * 0.5)) for index in range(1, 217)]
    summary = _summarise_persistent_samples(samples, ["worker"], start.isoformat(), end.isoformat())
    assert summary["expected_sample_count"] == 240
    assert summary["sampling_coverage_percent"] == 90.0
    assert summary["status"] == "measured"


def test_durable_sampler_event_files_preserve_long_run_samples_and_diagnostics(tmp_path) -> None:
    """A delayed host reader must not discard actual sampler events.

    The file models a 120-second sampler session already completed in the
    monitoring container.  It contains only genuine emitted events: no sample
    is invented by the collector while it is later read on the host.
    """
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sampler = _PersistentProcessTreeSampler(["worker"], output_directory=tmp_path)
    sampler._output_directory.mkdir(parents=True)
    event_file = sampler._output_directory / "worker.ndjson"
    sampler._event_files["worker"] = event_file
    events = [
        {"event": "ready", "service": "worker", "timestamp": start.isoformat()},
        *[_sample(start + timedelta(seconds=index * 0.5)) for index in range(1, 217)],
        {"event": "skipped_interval", "service": "worker", "timestamp": (start + timedelta(seconds=119)).isoformat(), "count": 1, "reason": "sampling_overrun", "late_by_seconds": 0.02},
        {"event": "error", "service": "worker", "timestamp": (start + timedelta(seconds=119.5)).isoformat(), "phase": "sample", "error": "OSError: synthetic read failure"},
    ]
    event_file.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    sampler._read_event_files()

    summary = _summarise_persistent_samples(sampler.samples, ["worker"], start.isoformat(), (start + timedelta(seconds=120)).isoformat(), sampler_errors=sampler.errors, skipped_intervals=sampler.skipped_intervals)
    assert summary["valid_sample_count"] == 216
    assert summary["sampling_coverage_percent"] == 90.0
    assert summary["sampler_skipped_intervals"] == [{"service": "worker", "timestamp": (start + timedelta(seconds=119)).isoformat(), "count": 1, "reason": "sampling_overrun", "late_by_seconds": 0.02}]
    assert "synthetic read failure" in summary["sampler_errors"][0]
    assert summary["status"] == "failed_sampler_error"


def test_dagster_start_recreates_only_stale_network_containers() -> None:
    stale_network = "failed to set up container networking: network abc not found"
    initial = CompletedProcess([], 1, stdout="", stderr=stale_network)
    recreated = CompletedProcess([], 0, stdout="recreated", stderr="")
    api_ready = CompletedProcess([], 0, stdout="", stderr="")
    with patch.object(benchmark, "_compose", side_effect=[initial, recreated, api_ready]) as compose, patch.object(benchmark, "_wait_healthy") as wait_healthy:
        _prepare_persistent_dagster()

    assert compose.call_args_list[0].args == ("--profile", "dagster", "up", "-d", "dagster-code", "dagster-webserver", "dagster-daemon")
    assert compose.call_args_list[1].args == ("--profile", "dagster", "up", "-d", "--force-recreate", "dagster-code", "dagster-webserver", "dagster-daemon")
    wait_healthy.assert_called_once_with(["dagster-code", "dagster-webserver", "dagster-daemon"])


def test_persistent_stack_rebuilds_framework_image_before_startup() -> None:
    built = CompletedProcess([], 0, stdout="built", stderr="")
    with patch.object(benchmark, "_compose", return_value=built) as compose, patch.object(benchmark, "_prepare_persistent_prefect") as prepare:
        benchmark._prepare_persistent_stack("prefect")

    compose.assert_called_once_with("--profile", "prefect", "build", "prefect", timeout=600)
    prepare.assert_called_once_with(no_build=False)


def test_fault_setup_reuses_existing_local_framework_image() -> None:
    with patch.object(benchmark, "_command_value", return_value="sha256:local") as image, patch.object(benchmark, "_compose") as compose, patch.object(benchmark, "_prepare_persistent_prefect") as prepare:
        benchmark._prepare_persistent_stack("prefect", reuse_local_image=True)

    image.assert_called_once_with(["docker", "image", "inspect", "csc796-climate-pipeline-orchestration-prefect:latest"])
    compose.assert_not_called()
    prepare.assert_called_once_with(no_build=True)


def test_prefect_setup_reuses_equivalent_existing_deployment() -> None:
    success = CompletedProcess([], 0, stdout="ok", stderr="")
    with patch.object(benchmark, "_compose", side_effect=[success, success, success, success]) as compose, patch.object(benchmark, "_wait_healthy") as wait_healthy:
        _prepare_persistent_prefect(no_build=True)

    assert len(compose.call_args_list) == 4
    assert "deployments/name/nigeria_climate_pipeline/benchmark" in str(compose.call_args_list[2].args)
    assert not any("to_deployment" in str(call.args) for call in compose.call_args_list)
    assert wait_healthy.call_args_list == [
        ((["prefect-server"],), {}),
        ((["prefect-worker"],), {}),
    ]


def test_compose_output_uses_robust_utf8_decoding() -> None:
    completed = CompletedProcess([], 0, stdout="output", stderr="")
    with patch.object(benchmark.subprocess, "run", return_value=completed) as run:
        assert benchmark._compose("build", "prefect", timeout=600) is completed

    run.assert_called_once_with(
        ["docker", "compose", "build", "prefect"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=600,
    )
