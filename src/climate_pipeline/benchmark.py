"""Controlled, serial benchmark planning and execution helpers."""
from __future__ import annotations

import csv
import duckdb
import json
import math
import platform
import re
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import psutil
import requests
import yaml

from .config import load_config
from .checksum import LOGICAL_OUTPUT_COLUMNS, logical_content_checksum
from .models import PartitionRequest
from .pipeline import run_partition
from .topology import LEGACY_TOPOLOGY_VERSION, PARTITION_CONCURRENCY, PRIMARY_TOPOLOGY_VERSION, ordered_partition_list, topology_metadata
from .verification import create_expected_manifest, verify_run

FRAMEWORKS = ("airflow", "prefect", "dagster")
OUTPUT_COLUMNS = LOGICAL_OUTPUT_COLUMNS


def run_local_benchmark(partitions: list[tuple[str, int]], replay_base_url: str, data_root: str | Path = "data", fault_scenario: str | None = None, seed: int = 0) -> dict[str, Any]:
    """Legacy local-core helper retained for compatibility; not used by the harness."""
    run_id = str(uuid.uuid4()); create_expected_manifest(run_id, partitions, data_root)
    results = [run_partition(PartitionRequest(location_id, year, run_id, replay_base_url, fault_scenario, seed), data_root) for location_id, year in partitions]
    verification = verify_run(run_id, data_root)
    return {"run_id": run_id, "statuses": [result.status for result in results], "verification": verification.status, "environment": machine_metadata()}


def _command_value(args: list[str]) -> str | None:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL, timeout=15).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def machine_metadata() -> dict[str, Any]:
    return {"timestamp": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(), "python_version": platform.python_version(), "cpu_logical_cores": psutil.cpu_count(), "ram_bytes": psutil.virtual_memory().total, "git_commit": _command_value(["git", "rev-parse", "HEAD"]), "docker_version": _command_value(["docker", "--version"])}


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def workload_partitions(workloads_path: str | Path, workload: str) -> list[tuple[str, int]]:
    workloads = _load_yaml(workloads_path)
    if workload not in workloads:
        raise ValueError(f"Unknown workload {workload!r}")
    definition = workloads[workload]
    locations = definition["locations"]
    configured_locations = {item["id"] for item in load_config()["locations"]}
    if not set(locations) <= configured_locations:
        raise ValueError(f"Workload {workload!r} includes an unknown location")
    years = definition["years"]
    year_values = list(range(years["start"], years["end"] + 1)) if isinstance(years, dict) else list(years)
    return [(location_id, year) for location_id in locations for year in year_values]


def validate_benchmark_plan(plan_path: str | Path = "configs/benchmark_plan.yaml") -> dict[str, Any]:
    plan = _load_yaml(plan_path)
    if plan.get("frameworks") != list(FRAMEWORKS):
        raise ValueError("Plan frameworks must be Airflow, Prefect, and Dagster in that order")
    repetitions = plan.get("repetitions", {})
    if repetitions.get("warmup") != 1 or repetitions.get("measured") != 3:
        raise ValueError("Initial validation plan must specify exactly 1 warm-up and 3 measured repetitions")
    workloads_path = Path(plan.get("workloads_file", "configs/workloads.yaml"))
    counts = {name: len(workload_partitions(workloads_path, name)) for name in ("small", "medium", "large")}
    if counts != {"small": 1, "medium": 25, "large": 120}:
        raise ValueError(f"Unexpected workload partition counts: {counts}")
    required = {"normal", "transient_http", "transient_storage", "timeout", "malformed_json", "missing_field", "duplicate_source", "invalid_observation", "storage_failure", "historical_backfill"}
    if set(plan.get("scenarios", {})) != required:
        raise ValueError(f"Scenario set must be exactly {sorted(required)}")
    config = load_config(); canonical_root = Path(plan.get("canonical_data_root", "data")); missing = []
    for location_id, year in workload_partitions(workloads_path, "large"):
        path = canonical_root / "raw" / "canonical" / config["dataset_version"] / location_id / f"{year}.json"
        if not path.exists(): missing.append(path)
    if missing:
        raise FileNotFoundError(f"Canonical dataset is incomplete; first missing file: {missing[0]}")
    images = {framework: _command_value(["docker", "image", "inspect", f"csc796-climate-pipeline-orchestration-{framework}:latest"]) is not None for framework in FRAMEWORKS}
    control_url = plan.get("replay_control_url", "http://127.0.0.1:8000")
    try: replay_healthy = requests.get(f"{control_url.rstrip('/')}/health", timeout=5).status_code == 200
    except requests.RequestException: replay_healthy = False
    return {"plan": str(Path(plan_path)), "workload_partition_counts": counts, "canonical_dataset_complete": True, "framework_images_available": images, "replay_healthy": replay_healthy, "scenarios": sorted(required)}


def _parse_bytes(value: str) -> int:
    match = re.fullmatch(r"([0-9.]+)\s*(B|KiB|MiB|GiB|TiB)", value.strip())
    if match is None:
        raise ValueError(f"Unsupported Docker memory value: {value!r}")
    number, unit = match.groups()
    return int(float(number) * {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}[unit])


def _docker_stats(container_name: str) -> tuple[float, int] | None:
    result = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}", container_name], capture_output=True, text=True, timeout=10)
    if result.returncode or "|" not in result.stdout: return None
    cpu, memory = result.stdout.strip().split("|", 1)
    try: return float(cpu.strip().removesuffix("%")), _parse_bytes(memory.split("/")[0])
    except (ValueError, IndexError, KeyError): return None


def _monitor_containers(container_names: list[str], done: threading.Event, samples: list[tuple[float, int]]) -> None:
    while not done.wait(0.25):
        samples.extend(_sample_controls(container_names))


def _control_plane_names(framework: str) -> list[str]:
    return ["csc796-climate-pipeline-orchestration-prefect-server-1"] if framework == "prefect" else []


def _sample_controls(names: list[str]) -> list[tuple[float, int]]:
    return [sample for name in names if (sample := _docker_stats(name)) is not None]


def _idle_baseline(framework: str) -> dict[str, Any]:
    names = _control_plane_names(framework); samples = []
    for _ in range(3):
        samples.extend(_sample_controls(names)); time.sleep(0.5)
    cpu = [item[0] for item in samples]; memory = [item[1] for item in samples]
    return {"component_names": names, "valid_sample_count": len(samples), "cpu_utilisation_mean_percent": sum(cpu) / len(cpu) if cpu else None, "peak_memory_bytes": max(memory) if memory else None, "average_memory_bytes": sum(memory) / len(memory) if memory else None, "status": "measured" if samples else ("failed_required_metrics_missing" if names else "not_applicable")}


def _logical_output(root: Path) -> tuple[int, int, str | None, str | None]:
    directory = root / "curated" / "observations"
    paths = sorted(directory.glob("location_id=*/year=*/data.parquet")) if directory.exists() else []
    if not paths: return 0, 0, None, None
    frame = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
    duplicates = int(frame.duplicated(["location_id", "observation_date"]).sum())
    if list(frame.columns) != OUTPUT_COLUMNS: return len(frame), duplicates, None, "schema_mismatch"
    return len(frame), duplicates, logical_content_checksum(frame), None


def _replay_attempts(control_url: str, reset: bool = False) -> dict[str, Any]:
    url = f"{control_url.rstrip('/')}/attempts"
    response = requests.post(f"{url}/reset", timeout=5) if reset else requests.get(url, timeout=5)
    response.raise_for_status(); return response.json()


def _prepare_airflow(run_root: Path) -> None:
    home = "/app/" + run_root.as_posix()
    base = ["docker", "compose", "--profile", "airflow", "run", "--rm", "-e", f"AIRFLOW_HOME={home}/airflow_home", "-e", "AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags", "--entrypoint", "airflow", "airflow"]
    for command in ([*base, "db", "migrate"], [*base, "dags", "reserialize"]):
        completed = subprocess.run(command, capture_output=True, text=True, timeout=120)
        if completed.returncode: raise RuntimeError(f"Airflow setup failed: {completed.stderr[-1000:] or completed.stdout[-1000:]}")


def _prepare_prefect_server() -> None:
    completed = subprocess.run(["docker", "compose", "--profile", "prefect", "up", "-d", "prefect-server"], capture_output=True, text=True, timeout=90)
    if completed.returncode: raise RuntimeError(f"Prefect API setup failed: {completed.stderr[-1000:] or completed.stdout[-1000:]}")
    # Probe from the same Compose network as the worker.  A running container
    # is insufficient: first startup may still be creating Prefect's database.
    probe = ["docker", "compose", "--profile", "prefect", "run", "--rm", "--no-deps", "-e", "PREFECT_API_URL=http://prefect-server:4200/api", "--entrypoint", "python", "prefect", "-c", "import os, time, requests; url=os.environ['PREFECT_API_URL'] + '/health'; deadline=time.monotonic()+120; last=None\nwhile time.monotonic()<deadline:\n    try:\n        response=requests.get(url, timeout=3); response.raise_for_status(); print(response.text); raise SystemExit(0)\n    except requests.RequestException as exc:\n        last=exc; time.sleep(2)\nraise SystemExit(f'Prefect API readiness timeout: {last}')"]
    readiness = subprocess.run(probe, capture_output=True, text=True, timeout=140)
    if readiness.returncode:
        logs = subprocess.run(["docker", "compose", "logs", "--tail", "200", "prefect-server"], capture_output=True, text=True, timeout=20)
        raise RuntimeError(f"Prefect API readiness failed: {readiness.stderr[-1000:] or readiness.stdout[-1000:]}\nServer logs:\n{logs.stdout[-4000:]}")


def _stop_prefect_server() -> None:
    subprocess.run(["docker", "compose", "--profile", "prefect", "stop", "prefect-server"], capture_output=True, text=True, timeout=45)


@dataclass
class BenchmarkRecord:
    run_id: str; orchestrator: str; orchestrator_version: str | None; python_version: str | None; scenario: str; workload_size: str; repetition_number: int; phase: str; random_seed: int; start_time: str; end_time: str; end_to_end_runtime_seconds: float; cpu_utilisation_mean_percent: float | None; cpu_utilisation_peak_percent: float | None; peak_memory_bytes: int | None; average_memory_bytes: float | None; resource_valid_sample_count: int; resource_measurement_start_time: str | None; resource_measurement_end_time: str | None; resource_validation_status: str; control_plane_idle_baseline: dict[str, Any]; control_plane_valid_sample_count: int; control_plane_cpu_utilisation_mean_percent: float | None; control_plane_cpu_utilisation_peak_percent: float | None; control_plane_peak_memory_bytes: int | None; retry_count: int; failure_recovery_seconds: float | None; successful_partitions: int; failed_partitions: int; output_row_count: int; duplicate_output_count: int; output_logical_checksum: str | None; output_schema_status: str; verification_status: str; backfill_completion_seconds: float | None; git_commit: str | None; environment: dict[str, Any]; worker_exit_code: int; worker_log: str
    runtime_measurement_definition: str | None = None; worker_component_names: list[str] = field(default_factory=list); worker_idle_baseline: dict[str, Any] = field(default_factory=dict); control_plane_component_names: list[str] = field(default_factory=list); control_plane_average_memory_bytes: float | None = None; runtime_includes: str | None = None; runtime_excludes: str | None = None; worker_expected_sample_count: int = 0; worker_sampling_coverage_percent: float | None = None; control_plane_expected_sample_count: int = 0; control_plane_sampling_coverage_percent: float | None = None; worker_resource_samples: list[dict[str, Any]] = field(default_factory=list); control_plane_resource_samples: list[dict[str, Any]] = field(default_factory=list); worker_sampler_errors: list[str] = field(default_factory=list); worker_sampler_skipped_intervals: list[dict[str, Any]] = field(default_factory=list); control_plane_sampler_errors: list[str] = field(default_factory=list); control_plane_sampler_skipped_intervals: list[dict[str, Any]] = field(default_factory=list); worker_process_tree_rss_average_bytes: float | None = None; worker_process_tree_rss_peak_bytes: int | None = None; control_plane_process_tree_rss_average_bytes: float | None = None; control_plane_process_tree_rss_peak_bytes: int | None = None; benchmark_stack_cgroup_memory_average_bytes: float | None = None; benchmark_stack_cgroup_memory_peak_bytes: int | None = None; benchmark_stack_cgroup_memory_valid_sample_count: int = 0; primary_memory_measurement: str | None = None; expected_partitions: int = 0; topology_version: str = LEGACY_TOPOLOGY_VERSION; top_level_workflow_run_count: int | None = None; partition_work_unit_count: int | None = None; configured_partition_concurrency: int | None = None; observed_max_partition_concurrency: int | None = None; ordered_partition_list: list[dict[str, int | str]] = field(default_factory=list)


def _run_one(plan: dict[str, Any], framework: str, workload: str, scenario: str, phase: str, repetition: int, partitions: list[tuple[str, int]], raw_root: Path) -> BenchmarkRecord:
    run_id = f"{framework}-{workload}-{scenario}-{phase}-{repetition}-{uuid.uuid4().hex[:8]}"; run_root = Path("data") / "benchmarks" / "state" / run_id; absolute_root = Path.cwd() / run_root
    if absolute_root.exists(): shutil.rmtree(absolute_root)
    absolute_root.mkdir(parents=True); create_expected_manifest(run_id, partitions, absolute_root, canonical_data_root=plan.get("canonical_data_root", "data"))
    control_url = plan.get("replay_control_url", "http://127.0.0.1:8000"); _replay_attempts(control_url, reset=True)
    if framework == "airflow": _prepare_airflow(run_root)
    idle_baseline = _idle_baseline(framework)
    definition = plan["scenarios"][scenario]; seed = int(plan["random_seed"]); container_name = f"benchmark-{run_id}".lower(); worker_path = "/app/" + run_root.as_posix()
    environment_args = ["-e", f"AIRFLOW_HOME={worker_path}/airflow_home", "-e", "AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags"]
    if framework == "prefect": environment_args.extend(["-e", "PREFECT_API_URL=http://prefect-server:4200/api"])
    command = ["docker", "compose", "--profile", framework, "run", "--rm", "--name", container_name, *environment_args, "--entrypoint", "python", framework, "-m", "climate_pipeline.benchmark_worker", "--framework", framework, "--partitions-json", json.dumps(partitions), "--run-id", run_id, "--data-root", worker_path, "--replay-url", "http://replay:8000", "--fault-scenario", definition.get("fault_scenario") or "", "--seed", str(seed)]
    control_samples: list[tuple[float, int]] = []; done = threading.Event(); monitor = threading.Thread(target=_monitor_containers, args=(_control_plane_names(framework), done, control_samples), daemon=True); environment = machine_metadata(); started_at = datetime.now(timezone.utc); start = time.perf_counter(); monitor.start()
    completed = subprocess.run(command, capture_output=True, text=True, timeout=int(plan.get("worker_timeout_seconds", 600))); elapsed = time.perf_counter() - start; done.set(); monitor.join(timeout=2); finished_at = datetime.now(timezone.utc)
    attempts = _replay_attempts(control_url); total_attempts = sum(item["attempts"] for item in attempts.get("attempts", [])); retries = max(0, total_attempts - len(partitions)); events = attempts.get("events", []); faults = [item for item in events if item.get("outcome") in {"http_503", "timeout_injected"}]; successes = [item for item in events if item.get("outcome") == "success"]
    recovery = None
    if faults and successes:
        first_fault = datetime.fromisoformat(faults[0]["timestamp"]); later = [datetime.fromisoformat(item["timestamp"]) for item in successes if datetime.fromisoformat(item["timestamp"]) >= first_fault]
        if later: recovery = (min(later) - first_fault).total_seconds()
    verification = verify_run(run_id, absolute_root); rows, duplicates, checksum, schema_problem = _logical_output(absolute_root); result_path = absolute_root / "worker_result.json"; worker = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}; resources = worker.get("resources", {}); statuses = worker.get("partition_statuses", []); successful = sum(status == "success" for status in statuses); control_names = _control_plane_names(framework); valid_resources = resources.get("valid_sample_count", 0) > 0 and resources.get("cpu_utilisation_mean_percent") is not None and resources.get("peak_memory_bytes") is not None and resources.get("average_memory_bytes") is not None and (not control_names or (idle_baseline["valid_sample_count"] > 0 and len(control_samples) > 0)); resource_status = "passed" if valid_resources else "failed_required_metrics_missing"
    record = BenchmarkRecord(run_id, framework, worker.get("orchestrator_version"), worker.get("python_version"), scenario, workload, repetition, phase, seed, started_at.isoformat(), finished_at.isoformat(), elapsed, resources.get("cpu_utilisation_mean_percent"), resources.get("cpu_utilisation_peak_percent"), resources.get("peak_memory_bytes"), resources.get("average_memory_bytes"), resources.get("valid_sample_count", 0), resources.get("measurement_start_time"), resources.get("measurement_end_time"), resource_status, idle_baseline, len(control_samples), (sum(cpu for cpu, _ in control_samples) / len(control_samples)) if control_samples else None, max((cpu for cpu, _ in control_samples), default=None), max((memory for _, memory in control_samples), default=None), retries, recovery, successful, len(statuses) - successful, rows, duplicates, checksum, "passed" if schema_problem is None else schema_problem, verification.status, elapsed if scenario == "historical_backfill" else None, environment["git_commit"], environment, completed.returncode, (completed.stdout + completed.stderr)[-8000:])
    raw_root.mkdir(parents=True, exist_ok=True); (raw_root / f"{run_id}.json").write_text(json.dumps(asdict(record), indent=2, sort_keys=True), encoding="utf-8")
    if resource_status != "passed": raise RuntimeError(f"Required resource metrics missing for {run_id}")
    return record


def run_benchmark_validation(plan_path: str | Path = "configs/benchmark_plan.yaml", workload: str = "small", scenario: str = "normal") -> list[dict[str, Any]]:
    validation = validate_benchmark_plan(plan_path)
    if not all(validation["framework_images_available"].values()) or not validation["replay_healthy"]: raise RuntimeError(f"Benchmark prerequisites unavailable: {validation}")
    if workload != "small" or scenario != "normal": raise ValueError("Validation execution is intentionally restricted to the small normal workload")
    plan = _load_yaml(plan_path); partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), workload); raw_root = Path(plan.get("raw_results_root", "data/benchmarks/raw")); records: list[BenchmarkRecord] = []
    for framework in FRAMEWORKS:
        if framework == "prefect": _prepare_prefect_server()
        try:
            records.append(_run_one(plan, framework, workload, scenario, "warmup", 0, partitions, raw_root))
            for repetition in range(1, int(plan["repetitions"]["measured"]) + 1): records.append(_run_one(plan, framework, workload, scenario, "measured", repetition, partitions, raw_root))
        finally:
            if framework == "prefect": _stop_prefect_server()
    reference = next((record.output_logical_checksum for record in records if record.phase == "measured" and record.output_logical_checksum), None); results = []
    for record in records:
        item = asdict(record); item["equivalent_to_reference"] = bool(reference and record.output_logical_checksum == reference and record.output_schema_status == "passed" and record.duplicate_output_count == 0); results.append(item)
    (raw_root / "small-normal-validation.json").write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    with (raw_root / "small-normal-validation.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[key for key in results[0] if key != "environment"]); writer.writeheader(); writer.writerows([{key: value for key, value in item.items() if key != "environment"} for item in results])
    return results


def run_framework_small_validation(framework: str, plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """Run the fixed small/normal validation sequence for one orchestrator."""
    if framework not in FRAMEWORKS:
        raise ValueError(f"Unknown framework {framework!r}")
    validation = validate_benchmark_plan(plan_path)
    if not validation["framework_images_available"][framework] or not validation["replay_healthy"]:
        raise RuntimeError(f"{framework} benchmark prerequisites unavailable: {validation}")
    plan = _load_yaml(plan_path); partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "small"); raw_root = Path(plan.get("raw_results_root", "data/benchmarks/raw")); records: list[BenchmarkRecord] = []
    if framework == "prefect": _prepare_prefect_server()
    try:
        records.append(_run_one(plan, framework, "small", "normal", "warmup", 0, partitions, raw_root))
        for repetition in range(1, int(plan["repetitions"]["measured"]) + 1): records.append(_run_one(plan, framework, "small", "normal", "measured", repetition, partitions, raw_root))
    finally:
        if framework == "prefect": _stop_prefect_server()
    reference = next((record.output_logical_checksum for record in records if record.phase == "measured" and record.output_logical_checksum), None); results = []
    for record in records:
        item = asdict(record); item["equivalent_to_reference"] = bool(reference and record.output_logical_checksum == reference and record.output_schema_status == "passed" and record.duplicate_output_count == 0); results.append(item)
    (raw_root / f"{framework}-small-normal-validation.json").write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    return results


def run_prefect_small_validation(plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """Compatibility wrapper for the dedicated Prefect validation action."""
    return run_framework_small_validation("prefect", plan_path)


# Persistent deployment harness -------------------------------------------------
# These helpers intentionally sit beside the earlier in-process harness so old
# smoke commands remain usable.  The persistent action below is the only one
# used for comparative benchmark results.
PERSISTENT_COMPONENTS = {
    "airflow": {"control": ["airflow-api", "airflow-db"], "worker": ["airflow-scheduler"]},
    "prefect": {"control": ["prefect-server"], "worker": ["prefect-worker"]},
    "dagster": {"control": ["dagster-webserver", "dagster-code"], "worker": ["dagster-daemon"]},
}
PERSISTENT_SAMPLE_INTERVAL_SECONDS = 0.5
PERSISTENT_IDLE_BASELINE_SECONDS = 20.0
PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT = 70.0
PERSISTENT_SAMPLER_IMAGE = "csc796-climate-pipeline-orchestration-airflow:latest"


def _compose(*args: str, timeout: int = 90, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", "compose", *args], capture_output=capture, text=True, encoding="utf-8", errors="replace", timeout=timeout)


def _service_container_ids(services: list[str]) -> list[str]:
    ids: list[str] = []
    for service in services:
        result = _compose("ps", "-q", service, timeout=20)
        container_id = result.stdout.strip()
        if container_id:
            ids.append(container_id)
    return ids


def _wait_healthy(services: list[str], timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds; last = "not started"
    while time.monotonic() < deadline:
        ids = _service_container_ids(services)
        if len(ids) == len(services):
            states = []
            for container_id in ids:
                inspected = subprocess.run(["docker", "inspect", "--format", "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", container_id], capture_output=True, text=True, timeout=15)
                states.append(inspected.stdout.strip())
            last = ", ".join(states)
            if all(state == "running|healthy" for state in states):
                return
        time.sleep(2)
    raise RuntimeError(f"Persistent service readiness timeout for {services}: {last}")


class _PersistentProcessTreeSampler:
    """Run durable PID-namespace samplers without streaming samples via Docker.

    Only the startup handshake uses the attached Docker CLI.  Samples and
    sampler diagnostics are appended to bind-mounted NDJSON files, preventing
    a stalled host reader or Docker stdout relay from back-pressuring a long
    running sampler.
    """

    def __init__(self, services: list[str], interval_seconds: float = PERSISTENT_SAMPLE_INTERVAL_SECONDS, output_directory: Path | None = None) -> None:
        self.services = services
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.skipped_intervals: list[dict[str, Any]] = []
        self._processes: list[tuple[str, subprocess.Popen[str]]] = []
        self._readers: list[threading.Thread] = []
        self._ready: dict[str, threading.Event] = {service: threading.Event() for service in services}
        self._lock = threading.Lock()
        self._session_id = uuid.uuid4().hex
        self._output_directory = (output_directory or Path("data") / "benchmarks" / "sampler-events") / self._session_id
        self._event_files: dict[str, Path] = {}

    def _read_stdout(self, process: subprocess.Popen[str], service: str) -> None:
        assert process.stdout is not None
        for line in process.stdout:
            try:
                sample = json.loads(line)
                if sample.get("event") == "ready":
                    if sample.get("service") != service:
                        raise ValueError("sampler ready service identity mismatch")
                    self._ready[service].set()
                    continue
            except (json.JSONDecodeError, ValueError) as exc:
                with self._lock:
                    self.errors.append(f"{service}: invalid sampler output: {exc}: {line[-300:]}")

    def _read_stderr(self, process: subprocess.Popen[str], service: str) -> None:
        assert process.stderr is not None
        for line in process.stderr:
            with self._lock:
                self.errors.append(f"{service}: sampler stderr: {line.strip()[-1000:]}")

    def _record_event(self, service: str, event: dict[str, Any]) -> None:
        if event.get("service") != service:
            self.errors.append(f"{service}: sampler event service identity mismatch: {event.get('service')}")
            return
        kind = event.get("event")
        if kind == "ready":
            return
        if kind == "error":
            self.errors.append(f"{service}: sampler {event.get('phase', 'sample')} error: {event.get('error')}")
            return
        if kind == "skipped_interval":
            self.skipped_intervals.append({"service": service, "timestamp": event.get("timestamp"), "count": event.get("count"), "reason": event.get("reason"), "late_by_seconds": event.get("late_by_seconds")})
            return
        if kind is not None:
            self.errors.append(f"{service}: unknown sampler event: {kind}")
            return
        self.samples.append(event)

    def _read_event_files(self) -> None:
        for service, path in self._event_files.items():
            if not path.exists():
                self.errors.append(f"{service}: sampler event file was not created: {path}")
                continue
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError("event is not an object")
                    self._record_event(service, event)
                except (json.JSONDecodeError, ValueError) as exc:
                    self.errors.append(f"{service}: invalid sampler event file line {line_number}: {exc}: {line[-300:]}")

    def start(self) -> None:
        container_ids = _service_container_ids(self.services)
        if len(container_ids) != len(self.services):
            raise RuntimeError(f"Cannot start persistent sampler; missing service container: {self.services}")
        self._output_directory.mkdir(parents=True, exist_ok=False)
        for service, container_id in zip(self.services, container_ids, strict=True):
            monitor_name = f"benchmark-sampler-{uuid.uuid4().hex[:12]}"
            event_file = self._output_directory / f"{service}.ndjson"
            self._event_files[service] = event_file
            command = ["docker", "run", "--rm", "--name", monitor_name, "-e", "PYTHONWARNINGS=ignore", "--pid", f"container:{container_id}", "--cgroupns", "host", "--network", "none", "--mount", f"type=bind,src={self._output_directory.resolve()},dst=/sampler-output", "--entrypoint", "python", PERSISTENT_SAMPLER_IMAGE, "-m", "climate_pipeline.container_sampler", "--service", service, "--container-id", container_id, "--interval-seconds", str(self.interval_seconds), "--output-file", f"/sampler-output/{event_file.name}"]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
            self._processes.append((monitor_name, process))
            reader = threading.Thread(target=self._read_stdout, args=(process, service), daemon=True)
            reader.start(); self._readers.append(reader)
            stderr_reader = threading.Thread(target=self._read_stderr, args=(process, service), daemon=True)
            stderr_reader.start(); self._readers.append(stderr_reader)
        for service, ready in self._ready.items():
            if not ready.wait(timeout=20):
                self.stop()
                raise RuntimeError(f"Persistent sampler failed to prime for {service}: {self.errors}")

    def stop(self) -> list[dict[str, Any]]:
        for monitor_name, process in self._processes:
            if process.poll() is not None:
                with self._lock:
                    self.errors.append(f"{monitor_name}: sampler exited before stop with code {process.returncode}")
        for monitor_name, _ in self._processes:
            # Docker Desktop does not reliably forward a terminated host CLI
            # process to an attached `docker run`; remove this named, temporary
            # monitor container explicitly so it cannot leak beyond a run.
            subprocess.run(["docker", "rm", "-f", monitor_name], capture_output=True, text=True, timeout=15)
        for _, process in self._processes:
            if process.poll() is None:
                process.terminate()
        for _, process in self._processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
            # Errors emitted before the ready handshake are represented by a
            # missing ready event; normal termination has no sampling error.
        for reader in self._readers:
            reader.join(timeout=5)
        with self._lock:
            self._read_event_files()
            return list(self.samples)


def _persistent_idle_baseline(services: list[str], duration_seconds: float = PERSISTENT_IDLE_BASELINE_SECONDS, output_directory: Path | None = None) -> dict[str, Any]:
    sampler = _PersistentProcessTreeSampler(services, output_directory=output_directory)
    sampler.start()
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        time.sleep(duration_seconds)
    finally:
        samples = sampler.stop()
    return _summarise_persistent_samples(samples, services, expected_duration_seconds=duration_seconds, sampler_errors=sampler.errors, skipped_intervals=sampler.skipped_intervals)


def _summarise_persistent_samples(samples: list[dict[str, Any]], services: list[str], start: str | None = None, end: str | None = None, expected_duration_seconds: float | None = None, sampler_errors: list[str] | None = None, skipped_intervals: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if start and end:
        lower = datetime.fromisoformat(start); upper = datetime.fromisoformat(end)
        samples = [sample for sample in samples if lower <= datetime.fromisoformat(sample["timestamp"]) <= upper]
        expected_duration_seconds = (upper - lower).total_seconds()
    cpu = [sample["cpu"] for sample in samples]; memory = [sample["memory"] for sample in samples]; diagnostic_rss = [sample["process_tree_rss_bytes"] for sample in samples if sample.get("process_tree_rss_bytes") is not None]
    expected = math.ceil(expected_duration_seconds / PERSISTENT_SAMPLE_INTERVAL_SECONDS) * len(services) if expected_duration_seconds is not None else 0
    coverage = (100.0 * len(samples) / expected) if expected else None
    if not samples:
        status = "failed_required_metrics_missing"
    elif coverage is not None and coverage < PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT:
        status = "failed_sampling_coverage"
    elif sampler_errors:
        status = "failed_sampler_error"
    else:
        status = "measured"
    return {"component_names": services, "valid_sample_count": len(samples), "expected_sample_count": expected, "sampling_coverage_percent": coverage, "sample_interval_seconds": PERSISTENT_SAMPLE_INTERVAL_SECONDS, "cpu_utilisation_mean_percent": sum(cpu) / len(cpu) if cpu else None, "cpu_utilisation_peak_percent": max(cpu) if cpu else None, "peak_memory_bytes": max(memory) if memory else None, "average_memory_bytes": sum(memory) / len(memory) if memory else None, "memory_measurement": "cgroup_v2_memory_current", "diagnostic_process_tree_rss_average_bytes": sum(diagnostic_rss) / len(diagnostic_rss) if diagnostic_rss else None, "diagnostic_process_tree_rss_peak_bytes": max(diagnostic_rss) if diagnostic_rss else None, "samples": samples, "sampler_errors": sampler_errors or [], "sampler_skipped_intervals": skipped_intervals or [], "status": status}


def _summarise_benchmark_stack_memory(samples: list[dict[str, Any]], services: list[str]) -> dict[str, Any]:
    """Sum per-container cgroup totals once per complete sampling round."""
    latest: dict[str, dict[str, Any]] = {}; updated: set[str] = set(); totals: list[int] = []
    for sample in sorted(samples, key=lambda item: item["timestamp"]):
        service = sample["service"]
        if service not in services:
            continue
        latest[service] = sample; updated.add(service)
        if len(latest) == len(services) and updated == set(services):
            timestamps = [datetime.fromisoformat(latest[name]["timestamp"]) for name in services]
            if (max(timestamps) - min(timestamps)).total_seconds() <= PERSISTENT_SAMPLE_INTERVAL_SECONDS * 2:
                totals.append(sum(int(latest[name]["cgroup_memory_bytes"]) for name in services))
            updated.clear()
    return {"measurement": "sum_of_container_cgroup_v2_memory_current", "valid_sample_count": len(totals), "average_memory_bytes": sum(totals) / len(totals) if totals else None, "peak_memory_bytes": max(totals) if totals else None}


def _airflow_client_environment() -> list[str]:
    return ["-e", "AIRFLOW__CORE__EXECUTOR=LocalExecutor", "-e", "AIRFLOW__CORE__LOAD_EXAMPLES=False", "-e", "AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags", "-e", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-db/airflow", "-e", "AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api:8080/execution/", "-e", "AIRFLOW__API_AUTH__JWT_SECRET=4M3hQ5mV4oOAX3asG6-zrZvgMwRifAkS6AwSOrFj1AQ", "-e", "AIRFLOW__API__SECRET_KEY=4M3hQ5mV4oOAX3asG6-zrZvgMwRifAkS6AwSOrFj1AQ", "-e", "AIRFLOW__CORE__FERNET_KEY=4M3hQ5mV4oOAX3asG6-zrZvgMwRifAkS6AwSOrFj1AQ="]


def _prepare_persistent_airflow(no_build: bool = False) -> None:
    started = _compose("--profile", "airflow", "up", "-d", *("--no-build",) if no_build else (), "airflow-db", timeout=120)
    if started.returncode: raise RuntimeError(f"Airflow database start failed: {started.stderr[-1000:]}")
    _wait_healthy(["airflow-db"])
    migrated = _compose("--profile", "airflow", "run", "--rm", "--no-deps", *_airflow_client_environment(), "--entrypoint", "airflow", "airflow", "db", "migrate", timeout=180)
    if migrated.returncode: raise RuntimeError(f"Airflow metadata migration failed: {migrated.stderr[-2000:] or migrated.stdout[-2000:]}")
    started = _compose("--profile", "airflow", "up", "-d", "airflow-api", "airflow-scheduler", timeout=120)
    if started.returncode: raise RuntimeError(f"Airflow control-plane start failed: {started.stderr[-1000:]}")
    _wait_healthy(["airflow-api", "airflow-scheduler"])
    serialised = _compose("--profile", "airflow", "run", "--rm", "--no-deps", *_airflow_client_environment(), "--entrypoint", "airflow", "airflow", "dags", "reserialize", timeout=120)
    if serialised.returncode: raise RuntimeError(f"Airflow DAG serialization failed: {serialised.stderr[-1500:] or serialised.stdout[-1500:]}")
    for workload in ("small", "medium", "large"):
        dag_id = f"nigeria_climate_pipeline_{workload}"
        unpaused = _compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "unpause", dag_id, timeout=60)
        if unpaused.returncode: raise RuntimeError(f"Airflow DAG unpause failed for {dag_id}: {unpaused.stderr[-1000:] or unpaused.stdout[-1000:]}")
    checked = _compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "list", "--output", "json", timeout=60)
    required_dags = {f"nigeria_climate_pipeline_{workload}" for workload in ("small", "medium", "large")}
    if checked.returncode or not all(dag_id in checked.stdout for dag_id in required_dags):
        raise RuntimeError(f"Airflow DAG readiness failed: {checked.stderr[-1000:] or checked.stdout[-1000:]}")


def _prepare_persistent_prefect(no_build: bool = False) -> None:
    started = _compose("--profile", "prefect", "up", "-d", *("--no-build",) if no_build else (), "prefect-server", timeout=120)
    if started.returncode: raise RuntimeError(f"Prefect server start failed: {started.stderr[-1000:]}")
    _wait_healthy(["prefect-server"])
    inspect = _compose("--profile", "prefect", "run", "--rm", "--no-deps", "-e", "PREFECT_API_URL=http://prefect-server:4200/api", "--entrypoint", "prefect", "prefect", "work-pool", "inspect", "climate-benchmark-pool", timeout=60)
    if inspect.returncode:
        created = _compose("--profile", "prefect", "run", "--rm", "--no-deps", "-e", "PREFECT_API_URL=http://prefect-server:4200/api", "--entrypoint", "prefect", "prefect", "work-pool", "create", "--type", "process", "climate-benchmark-pool", timeout=60)
        if created.returncode: raise RuntimeError(f"Prefect work-pool creation failed: {created.stderr[-1000:] or created.stdout[-1000:]}")
    # Deployment registration can hold the embedded Prefect database lock for
    # an extended time.  Reuse the existing, equivalent native deployment
    # rather than rewriting it before every independent experiment run.
    deployment_probe = "import os, requests; base=os.environ['PREFECT_API_URL'].rstrip('/'); response=requests.get(base + '/deployments/name/nigeria_climate_pipeline/benchmark', timeout=15); response.raise_for_status(); deployment=response.json(); assert deployment.get('work_pool_name') == 'climate-benchmark-pool'; assert deployment.get('entrypoint') == 'climate_pipeline.orchestrators.prefect_flow.climate_flow'"
    deployed = _compose("--profile", "prefect", "run", "--rm", "--no-deps", "-e", "PREFECT_API_URL=http://prefect-server:4200/api", "--entrypoint", "python", "prefect", "-c", deployment_probe, timeout=30)
    if deployed.returncode:
        deployed = _compose("--profile", "prefect", "run", "--rm", "--no-deps", "-e", "PREFECT_API_URL=http://prefect-server:4200/api", "--entrypoint", "python", "prefect", "-c", "from prefect.deployments.runner import EntrypointType; from climate_pipeline.orchestrators.prefect_flow import climate_flow; climate_flow.to_deployment(name='benchmark', work_pool_name='climate-benchmark-pool', entrypoint_type=EntrypointType.MODULE_PATH, job_variables={'working_dir': '/app'}).apply()", timeout=90)
        if deployed.returncode: raise RuntimeError(f"Prefect deployment registration failed: {deployed.stderr[-1500:] or deployed.stdout[-1500:]}")
    started = _compose("--profile", "prefect", "up", "-d", *("--no-build",) if no_build else (), "prefect-worker", timeout=90)
    if started.returncode: raise RuntimeError(f"Prefect worker start failed: {started.stderr[-1000:]}")
    _wait_healthy(["prefect-worker"])


def _prepare_persistent_dagster(no_build: bool = False) -> None:
    services = ("dagster-code", "dagster-webserver", "dagster-daemon")
    started = _compose("--profile", "dagster", "up", "-d", *("--no-build",) if no_build else (), *services, timeout=120)
    output = f"{started.stdout}\n{started.stderr}"
    # Docker can retain stopped Compose containers whose NetworkSettings point
    # at a network removed by Docker Desktop.  A normal `up` then fails before
    # the Dagster benchmark loop starts.  Recreate only that stale Dagster
    # control plane; service startup is outside the benchmark timing boundary.
    if started.returncode and "failed to set up container networking" in output and "network" in output and "not found" in output:
        started = _compose("--profile", "dagster", "up", "-d", "--force-recreate", *("--no-build",) if no_build else (), *services, timeout=120)
        output = f"{started.stdout}\n{started.stderr}"
    if started.returncode: raise RuntimeError(f"Dagster control-plane start failed: {output[-1000:]}")
    _wait_healthy(["dagster-code", "dagster-webserver", "dagster-daemon"])
    ready = _compose("exec", "-T", "dagster-webserver", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/server_info', timeout=5)", timeout=30)
    if ready.returncode: raise RuntimeError(f"Dagster API readiness failed: {ready.stderr[-1000:] or ready.stdout[-1000:]}")


def _prepare_persistent_stack(framework: str, reuse_local_image: bool = False) -> None:
    image = f"csc796-climate-pipeline-orchestration-{framework}:latest"
    if not reuse_local_image or _command_value(["docker", "image", "inspect", image]) is None:
        built = _compose("--profile", framework, "build", framework, timeout=600)
        if built.returncode:
            raise RuntimeError(f"Persistent {framework} image build failed: {built.stderr[-2000:] or built.stdout[-2000:]}")
    {"airflow": _prepare_persistent_airflow, "prefect": _prepare_persistent_prefect, "dagster": _prepare_persistent_dagster}[framework](no_build=reuse_local_image)


def _stop_persistent_stack(framework: str) -> None:
    services = list(PERSISTENT_COMPONENTS[framework]["control"] + PERSISTENT_COMPONENTS[framework]["worker"])
    _compose("--profile", framework, "stop", *services, timeout=90)


def _persistent_readiness(framework: str) -> None:
    components = PERSISTENT_COMPONENTS[framework]
    _wait_healthy(components["control"] + components["worker"], timeout_seconds=60)


def _write_partition_items(absolute_root: Path, items: list[dict[str, Any]]) -> Path:
    """Persist versioned partition input under the already-mounted run root."""
    path = absolute_root / "partition-items.v1.json"
    path.write_text(json.dumps(items, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _persistent_submission_command(framework: str, items_file: str, worker_path: str, timeout: int) -> list[str]:
    environment: list[str] = []
    if framework == "airflow": environment.extend(_airflow_client_environment())
    if framework == "prefect": environment.extend(["-e", "PREFECT_API_URL=http://prefect-server:4200/api"])
    if framework == "dagster": environment.extend(["-e", "DAGSTER_GRAPHQL_URL=http://dagster-webserver:3000/graphql"])
    return ["docker", "compose", "--profile", framework, "run", "--rm", "--no-deps", *environment, "--entrypoint", "python", framework, "-m", "climate_pipeline.persistent_submitter", "--framework", framework, "--items-file", items_file, "--data-root", worker_path, "--timeout", str(timeout)]


def _persistent_partition_statuses(data_root: Path, run_id: str) -> list[str]:
    try:
        with duckdb.connect(str(data_root / "audit.duckdb"), read_only=True) as database:
            return [row[0] for row in database.execute("SELECT status FROM pipeline_runs WHERE run_id=? ORDER BY location_id, year", [run_id]).fetchall()]
    except duckdb.Error:
        return []


def _observed_partition_concurrency(data_root: Path, run_id: str) -> int | None:
    """Calculate actual overlap from durable partition audit timestamps."""
    try:
        with duckdb.connect(str(data_root / "audit.duckdb"), read_only=True) as database:
            intervals = database.execute(
                "SELECT started_at, finished_at FROM pipeline_runs "
                "WHERE run_id=? AND status='success' ORDER BY started_at, finished_at",
                [run_id],
            ).fetchall()
    except duckdb.Error:
        return None
    events: list[tuple[datetime, int]] = []
    for started_at, finished_at in intervals:
        if not started_at or not finished_at:
            return None
        try:
            events.append((datetime.fromisoformat(started_at), 1))
            events.append((datetime.fromisoformat(finished_at), -1))
        except ValueError:
            return None
    # Finish events sort first, so adjacent work units are not overlapping.
    active = maximum = 0
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        maximum = max(maximum, active)
    return maximum if intervals else None


def _fill_missing_topology_metadata(submission: dict[str, Any], items: list[dict[str, Any]]) -> None:
    for key, value in topology_metadata(items).items():
        if key != "observed_max_partition_concurrency":
            submission.setdefault(key, value)


def _run_persistent_one(plan: dict[str, Any], framework: str, workload: str, scenario: str, phase: str, repetition: int, partitions: list[tuple[str, int]], raw_root: Path, state_root: Path = Path("data/benchmarks/state"), raise_on_failure: bool = True, execution_root: Path | None = None, run_id: str | None = None) -> BenchmarkRecord:
    """Submit one persistent run, optionally separating audit state from outputs.

    Normal and fault experiments retain the default single run directory.  The
    framework-native reprocessing experiment supplies an already-populated
    execution root so it can prove overwrite/idempotency behaviour without
    touching any historical benchmark output.
    """
    run_id = run_id or f"persistent-{framework}-{workload}-{scenario}-{phase}-{repetition}-{uuid.uuid4().hex[:8]}"
    run_root = state_root / run_id
    absolute_root = Path.cwd() / run_root
    if absolute_root.exists():
        raise FileExistsError(f"Refusing to replace existing persistent run state: {absolute_root}")
    absolute_root.mkdir(parents=True)
    absolute_execution_root = Path.cwd() / execution_root if execution_root is not None else absolute_root
    if execution_root is not None and not absolute_execution_root.exists():
        raise FileNotFoundError(f"Persistent execution root must be prepared before submission: {absolute_execution_root}")
    absolute_execution_root.mkdir(parents=True, exist_ok=True)
    create_expected_manifest(run_id, partitions, absolute_execution_root, canonical_data_root=plan.get("canonical_data_root", "data"))
    control_url = plan.get("replay_control_url", "http://127.0.0.1:8000"); _replay_attempts(control_url, reset=True); _persistent_readiness(framework)
    components = PERSISTENT_COMPONENTS[framework]
    # The 20-second control baseline is intentionally outside the runtime
    # boundary.  Worker idleness is still recorded, without lengthening each
    # repetition by a second full baseline window.
    sampler_output = absolute_root / "sampler-events"
    control_idle = _persistent_idle_baseline(components["control"], output_directory=sampler_output)
    worker_idle = _persistent_idle_baseline(components["worker"], duration_seconds=2.0, output_directory=sampler_output)
    definition = plan["scenarios"][scenario]; seed = int(plan["random_seed"])
    try:
        worker_path = "/app/" + absolute_execution_root.relative_to(Path.cwd()).as_posix()
    except ValueError as exc:
        raise ValueError(f"Persistent execution root must be inside the repository: {absolute_execution_root}") from exc
    items = [{"location_id": location_id, "year": year, "run_id": run_id, "replay_base_url": "http://replay:8000", "fault_scenario": definition.get("fault_scenario") or None, "seed": seed, "data_root": worker_path} for location_id, year in partitions]
    expected_order = [{"location_id": location_id, "year": year} for location_id, year in partitions]
    if ordered_partition_list(items) != expected_order:
        raise RuntimeError("Persistent benchmark partition order diverged from configured location/year order")
    items_path = _write_partition_items(absolute_root, items); worker_items_path = "/app/" + items_path.relative_to(Path.cwd()).as_posix()
    monitored = components["control"] + components["worker"]
    sampler = _PersistentProcessTreeSampler(monitored, output_directory=sampler_output)
    sampler.start()
    environment = machine_metadata()
    try:
        scaled_timeout = int(plan.get("worker_timeout_seconds", 600)) * len(partitions)
        command = _persistent_submission_command(framework, worker_items_path, worker_path, scaled_timeout)
        completed = subprocess.run(command, capture_output=True, text=True, timeout=scaled_timeout + 90)
    finally:
        samples = sampler.stop()
    submission_path = absolute_execution_root / "submission_result.json"; submission = json.loads(submission_path.read_text(encoding="utf-8")) if submission_path.exists() else {}; start_time = submission.get("submission_started_at"); end_time = submission.get("completion_verified_at")
    worker_samples = [sample for sample in samples if sample["service"] in components["worker"]]
    control_samples = [sample for sample in samples if sample["service"] in components["control"]]
    worker_stats = _summarise_persistent_samples(worker_samples, components["worker"], start_time, end_time, sampler_errors=sampler.errors, skipped_intervals=sampler.skipped_intervals) if start_time and end_time else _summarise_persistent_samples([], components["worker"], sampler_errors=sampler.errors, skipped_intervals=sampler.skipped_intervals)
    control_stats = _summarise_persistent_samples(control_samples, components["control"], start_time, end_time, sampler_errors=sampler.errors, skipped_intervals=sampler.skipped_intervals) if start_time and end_time else _summarise_persistent_samples([], components["control"], sampler_errors=sampler.errors, skipped_intervals=sampler.skipped_intervals)
    runtime_samples = [sample for sample in samples if start_time and end_time and datetime.fromisoformat(start_time) <= datetime.fromisoformat(sample["timestamp"]) <= datetime.fromisoformat(end_time)]
    control_cgroup_memory = _summarise_benchmark_stack_memory([sample for sample in runtime_samples if sample["service"] in components["control"]], components["control"])
    if control_cgroup_memory["valid_sample_count"]:
        control_stats["average_memory_bytes"] = control_cgroup_memory["average_memory_bytes"]
        control_stats["peak_memory_bytes"] = control_cgroup_memory["peak_memory_bytes"]
        control_stats["memory_measurement"] = control_cgroup_memory["measurement"]
    stack_memory = _summarise_benchmark_stack_memory(runtime_samples, monitored)
    attempts = _replay_attempts(control_url); total_attempts = sum(attempt["attempts"] for attempt in attempts.get("attempts", [])); initial_attempts = len(attempts.get("attempts", [])); retries = max(0, total_attempts - initial_attempts); events = attempts.get("events", []); fault_events = [event for event in events if event.get("outcome") in {"http_503", "timeout_injected", "storage_failure_injected"}]; recovery = None
    if fault_events:
        first_fault = datetime.fromisoformat(fault_events[0]["timestamp"]); later_successes = [datetime.fromisoformat(event["timestamp"]) for event in events if event.get("outcome") == "success" and datetime.fromisoformat(event["timestamp"]) >= first_fault]
        if later_successes: recovery = (min(later_successes) - first_fault).total_seconds()
    verification = verify_run(run_id, absolute_execution_root); rows, duplicates, checksum, schema_problem = _logical_output(absolute_execution_root); statuses = _persistent_partition_statuses(absolute_execution_root, run_id); successful = sum(status == "success" for status in statuses); observed_concurrency = _observed_partition_concurrency(absolute_execution_root, run_id)
    _fill_missing_topology_metadata(submission, items)
    topology_valid = submission.get("topology_version") == PRIMARY_TOPOLOGY_VERSION and submission.get("top_level_workflow_run_count") == 1 and submission.get("partition_work_unit_count") == len(partitions) and submission.get("configured_partition_concurrency") == PARTITION_CONCURRENCY and submission.get("ordered_partition_list") == expected_order
    valid = completed.returncode == 0 and submission.get("status") == "success" and topology_valid and worker_stats["status"] == "measured" and control_stats["status"] == "measured" and control_idle["status"] == "measured" and worker_idle["status"] == "measured"; resource_status = "passed" if valid else "failed_required_metrics_missing_or_submission_failed"
    record = BenchmarkRecord(run_id=run_id, orchestrator=framework, orchestrator_version=submission.get("orchestrator_version"), python_version=submission.get("python_version"), scenario=scenario, workload_size=workload, repetition_number=repetition, phase=phase, random_seed=seed, start_time=start_time or environment["timestamp"], end_time=end_time or environment["timestamp"], end_to_end_runtime_seconds=float(submission.get("workflow_runtime_seconds", 0.0)), cpu_utilisation_mean_percent=worker_stats["cpu_utilisation_mean_percent"], cpu_utilisation_peak_percent=worker_stats["cpu_utilisation_peak_percent"], peak_memory_bytes=worker_stats["peak_memory_bytes"], average_memory_bytes=worker_stats["average_memory_bytes"], resource_valid_sample_count=worker_stats["valid_sample_count"], resource_measurement_start_time=start_time, resource_measurement_end_time=end_time, resource_validation_status=resource_status, control_plane_idle_baseline=control_idle, control_plane_valid_sample_count=control_stats["valid_sample_count"], control_plane_cpu_utilisation_mean_percent=control_stats["cpu_utilisation_mean_percent"], control_plane_cpu_utilisation_peak_percent=control_stats["cpu_utilisation_peak_percent"], control_plane_peak_memory_bytes=control_stats["peak_memory_bytes"], retry_count=retries, failure_recovery_seconds=recovery, successful_partitions=successful, failed_partitions=len(statuses) - successful, output_row_count=rows, duplicate_output_count=duplicates, output_logical_checksum=checksum, output_schema_status="passed" if schema_problem is None else schema_problem, verification_status=verification.status, backfill_completion_seconds=float(submission.get("workflow_runtime_seconds", 0.0)) if scenario == "historical_backfill" else None, git_commit=environment["git_commit"], environment=environment, worker_exit_code=completed.returncode, worker_log=(completed.stdout + completed.stderr)[-8000:], runtime_measurement_definition="UTC timestamp immediately before persistent-control-plane submission through UTC timestamp when that control plane reports terminal success", worker_component_names=components["worker"], worker_idle_baseline=worker_idle, control_plane_component_names=components["control"], control_plane_average_memory_bytes=control_stats["average_memory_bytes"], runtime_includes="workflow submission API call, persistent scheduling/dispatch, task execution, retry handling, and terminal-state verification", runtime_excludes="image build, Compose/container startup, stack initialization, readiness checks, and idle-baseline sampling", worker_expected_sample_count=worker_stats["expected_sample_count"], worker_sampling_coverage_percent=worker_stats["sampling_coverage_percent"], control_plane_expected_sample_count=control_stats["expected_sample_count"], control_plane_sampling_coverage_percent=control_stats["sampling_coverage_percent"], worker_resource_samples=worker_stats["samples"], control_plane_resource_samples=control_stats["samples"], worker_sampler_errors=worker_stats["sampler_errors"], worker_sampler_skipped_intervals=worker_stats["sampler_skipped_intervals"], control_plane_sampler_errors=control_stats["sampler_errors"], control_plane_sampler_skipped_intervals=control_stats["sampler_skipped_intervals"], worker_process_tree_rss_average_bytes=worker_stats["diagnostic_process_tree_rss_average_bytes"], worker_process_tree_rss_peak_bytes=worker_stats["diagnostic_process_tree_rss_peak_bytes"], control_plane_process_tree_rss_average_bytes=control_stats["diagnostic_process_tree_rss_average_bytes"], control_plane_process_tree_rss_peak_bytes=control_stats["diagnostic_process_tree_rss_peak_bytes"], benchmark_stack_cgroup_memory_average_bytes=stack_memory["average_memory_bytes"], benchmark_stack_cgroup_memory_peak_bytes=stack_memory["peak_memory_bytes"], benchmark_stack_cgroup_memory_valid_sample_count=stack_memory["valid_sample_count"], primary_memory_measurement="container_cgroup_v2_memory_current", expected_partitions=len(partitions), topology_version=submission.get("topology_version", LEGACY_TOPOLOGY_VERSION), top_level_workflow_run_count=submission.get("top_level_workflow_run_count"), partition_work_unit_count=submission.get("partition_work_unit_count"), configured_partition_concurrency=submission.get("configured_partition_concurrency"), observed_max_partition_concurrency=observed_concurrency, ordered_partition_list=submission.get("ordered_partition_list", []))
    raw_root.mkdir(parents=True, exist_ok=True); (raw_root / f"{run_id}.json").write_text(json.dumps(asdict(record), indent=2, sort_keys=True), encoding="utf-8")
    if raise_on_failure and resource_status != "passed":
        # Include the captured client traceback in the outer Python exception.
        # The PowerShell runner redirects that exception to its stderr file.
        diagnostic = completed.stderr.strip() or submission.get("traceback") or submission.get("error", "no submitter diagnostic was written")
        raise RuntimeError(f"Persistent benchmark failed resource/submission validation for {run_id}:\n{diagnostic}")
    if raise_on_failure and (verification.status != "passed" or successful != len(partitions) or len(statuses) != len(partitions) or duplicates != 0 or schema_problem is not None):
        raise RuntimeError(f"Persistent benchmark output validation failed for {run_id}: verification={verification.status}, expected_partitions={len(partitions)}, successful={successful}, statuses={len(statuses)}, rows={rows}, duplicates={duplicates}, schema={schema_problem}")
    return record


def run_persistent_small_validation(plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """Run the only permitted persistent-deployment validation: small/normal."""
    validation = validate_benchmark_plan(plan_path)
    if not all(validation["framework_images_available"].values()) or not validation["replay_healthy"]: raise RuntimeError(f"Persistent benchmark prerequisites unavailable: {validation}")
    plan = _load_yaml(plan_path); partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "small"); raw_root = Path(plan.get("raw_results_root", "data/benchmarks/raw")); records: list[BenchmarkRecord] = []
    for framework in FRAMEWORKS:
        _prepare_persistent_stack(framework)
        try:
            records.append(_run_persistent_one(plan, framework, "small", "normal", "warmup", 0, partitions, raw_root))
            for repetition in range(1, int(plan["repetitions"]["measured"]) + 1): records.append(_run_persistent_one(plan, framework, "small", "normal", "measured", repetition, partitions, raw_root))
        finally:
            _stop_persistent_stack(framework)
    reference = next((record.output_logical_checksum for record in records if record.phase == "measured" and record.output_logical_checksum), None); results = []
    for record in records:
        item = asdict(record); item["equivalent_to_reference"] = bool(reference and record.output_logical_checksum == reference and record.output_schema_status == "passed" and record.duplicate_output_count == 0); results.append(item)
    (raw_root / "persistent-small-normal-validation.json").write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    with (raw_root / "persistent-small-normal-validation.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[key for key in results[0] if key != "environment"]); writer.writeheader(); writer.writerows([{key: value for key, value in item.items() if key != "environment"} for item in results])
    return results


def run_persistent_small_memory_validation(plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """Run one small/normal measured validation per persistent engine.

    This is deliberately separate from the three-repetition benchmark action:
    it validates memory accounting without creating a new performance series.
    """
    validation = validate_benchmark_plan(plan_path)
    if not all(validation["framework_images_available"].values()) or not validation["replay_healthy"]:
        raise RuntimeError(f"Persistent benchmark prerequisites unavailable: {validation}")
    plan = _load_yaml(plan_path); partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "small"); raw_root = Path(plan.get("raw_results_root", "data/benchmarks/raw")); records: list[BenchmarkRecord] = []
    for framework in FRAMEWORKS:
        _prepare_persistent_stack(framework)
        try:
            records.append(_run_persistent_one(plan, framework, "small", "normal", "measured", 1, partitions, raw_root))
        finally:
            _stop_persistent_stack(framework)
    results = [asdict(record) for record in records]
    for item in results:
        item["equivalent_to_reference"] = bool(item["output_logical_checksum"] and item["output_logical_checksum"] == "4d2b1e512b56fa31ba481464619a42f28ef7ff843c1041a84ae19e94b8f37955" and item["output_schema_status"] == "passed" and item["duplicate_output_count"] == 0)
    (raw_root / "persistent-small-memory-validation.json").write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    return results


def run_persistent_normal_scaling(plan_path: str | Path = "configs/benchmark_plan.yaml", workloads: tuple[str, ...] = ("medium", "large")) -> list[dict[str, Any]]:
    """Run the approved medium and large normal-success protocol serially."""
    validation = validate_benchmark_plan(plan_path)
    if not all(validation["framework_images_available"].values()) or not validation["replay_healthy"]:
        raise RuntimeError(f"Persistent benchmark prerequisites unavailable: {validation}")
    plan = _load_yaml(plan_path); raw_root = Path(plan.get("raw_results_root", "data/benchmarks/raw")); all_results: list[dict[str, Any]] = []
    for workload in workloads:
        partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), workload)
        records: list[BenchmarkRecord] = []; workload_checksum: str | None = None
        for framework in FRAMEWORKS:
            _prepare_persistent_stack(framework)
            try:
                framework_records = [_run_persistent_one(plan, framework, workload, "normal", "warmup", 0, partitions, raw_root)]
                framework_records.extend(_run_persistent_one(plan, framework, workload, "normal", "measured", repetition, partitions, raw_root) for repetition in range(1, int(plan["repetitions"]["measured"]) + 1))
            finally:
                _stop_persistent_stack(framework)
            checksums = {record.output_logical_checksum for record in framework_records}
            if len(checksums) != 1 or None in checksums:
                raise RuntimeError(f"Logical checksum instability for {framework}/{workload}: {checksums}")
            checksum = checksums.pop()
            if workload_checksum is None:
                workload_checksum = checksum
            elif checksum != workload_checksum:
                raise RuntimeError(f"Logical checksum mismatch for {framework}/{workload}: expected {workload_checksum}, got {checksum}")
            records.extend(framework_records)
        results = []
        for record in records:
            item = asdict(record); item["equivalent_to_workload_reference"] = item["output_logical_checksum"] == workload_checksum; results.append(item)
        (raw_root / f"persistent-{workload}-normal-scaling.json").write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
        with (raw_root / f"persistent-{workload}-normal-scaling.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=[key for key in results[0] if key not in {"environment", "worker_resource_samples", "control_plane_resource_samples"}]); writer.writeheader(); writer.writerows([{key: value for key, value in item.items() if key not in {"environment", "worker_resource_samples", "control_plane_resource_samples"}} for item in results])
        all_results.extend(results)
    (raw_root / "persistent-normal-scaling.json").write_text(json.dumps(all_results, indent=2, sort_keys=True), encoding="utf-8")
    return all_results


def _framework_neutral_reference(plan: dict[str, Any], workload: str, partitions: list[tuple[str, int]]) -> tuple[int, str]:
    """Materialize an ordered direct-core reference without an orchestrator."""
    reference_root = Path("data") / "benchmarks" / "reference" / f"{workload}-single-workflow-v2"
    reference_run_id = f"framework-neutral-{workload}-single-workflow-v2"
    if reference_root.exists():
        shutil.rmtree(reference_root)
    reference_root.mkdir(parents=True)
    create_expected_manifest(reference_run_id, partitions, reference_root, canonical_data_root=plan.get("canonical_data_root", "data"))
    for location_id, year in partitions:
        result = run_partition(PartitionRequest(location_id, year, reference_run_id, "http://127.0.0.1:8000", None, int(plan["random_seed"])), reference_root)
        if result.status != "success":
            raise RuntimeError(f"Framework-neutral {workload} reference failed for {location_id}/{year}: {result.error}")
    verification = verify_run(reference_run_id, reference_root); rows, duplicates, checksum, schema_problem = _logical_output(reference_root)
    if verification.status != "passed" or duplicates != 0 or schema_problem is not None or checksum is None:
        raise RuntimeError(f"Framework-neutral {workload} reference invalid: verification={verification.status}, rows={rows}, duplicates={duplicates}, schema={schema_problem}, checksum={checksum}")
    (reference_root / "reference_summary.json").write_text(json.dumps({"run_id": reference_run_id, "workload": workload, "topology_version": PRIMARY_TOPOLOGY_VERSION, "row_count": rows, "checksum": checksum, "verification": verification.status}, indent=2, sort_keys=True), encoding="utf-8")
    return rows, checksum


def _saved_v2_validation_record(raw_root: Path, framework: str, workload: str, expected_rows: int, expected_checksum: str, partition_count: int) -> dict[str, Any] | None:
    """Return a prior valid v2 validation record without changing raw history."""
    required = {
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "top_level_workflow_run_count": 1,
        "partition_work_unit_count": partition_count,
        "configured_partition_concurrency": PARTITION_CONCURRENCY,
        "observed_max_partition_concurrency": 1,
        "output_row_count": expected_rows,
        "duplicate_output_count": 0,
        "output_logical_checksum": expected_checksum,
        "verification_status": "passed",
        "resource_validation_status": "passed",
    }
    candidates: list[tuple[float, dict[str, Any]]] = []
    for path in raw_root.glob(f"persistent-{framework}-{workload}-normal-topology_validation-1-*.json"):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if any(record.get(key) != value for key, value in required.items()):
            continue
        coverage = (record.get("worker_sampling_coverage_percent"), record.get("control_plane_sampling_coverage_percent"))
        if not all(isinstance(value, (int, float)) and value >= PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT for value in coverage):
            continue
        if record.get("worker_sampler_errors") or record.get("control_plane_sampler_errors"):
            continue
        candidates.append((path.stat().st_mtime, record))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def run_single_workflow_topology_validation(plan_path: str | Path = "configs/benchmark_plan.yaml") -> list[dict[str, Any]]:
    """One v2 validation run per framework and workload; no benchmark repetitions."""
    validation = validate_benchmark_plan(plan_path)
    if not all(validation["framework_images_available"].values()) or not validation["replay_healthy"]:
        raise RuntimeError(f"Persistent benchmark prerequisites unavailable: {validation}")
    plan = _load_yaml(plan_path); raw_root = Path(plan.get("raw_results_root", "data/benchmarks/raw"))
    expected = {"small": (365, "4d2b1e512b56fa31ba481464619a42f28ef7ff843c1041a84ae19e94b8f37955"), "large": (43830, "af5c386f92932617709bcd7ac77e1d7aab970d330912f2b543221e60beab37c9")}
    medium_partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "medium")
    expected["medium"] = _framework_neutral_reference(plan, "medium", medium_partitions)
    records: list[dict[str, Any]] = []
    for workload in ("small", "medium", "large"):
        partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), workload)
        expected_rows, expected_checksum = expected[workload]
        for framework in FRAMEWORKS:
            saved = _saved_v2_validation_record(raw_root, framework, workload, expected_rows, expected_checksum, len(partitions))
            if saved is not None:
                records.append(saved)
                continue
            _prepare_persistent_stack(framework)
            try:
                record = _run_persistent_one(plan, framework, workload, "normal", "topology_validation", 1, partitions, raw_root)
            finally:
                _stop_persistent_stack(framework)
            item = asdict(record)
            failures = []
            for key, value in {"topology_version": PRIMARY_TOPOLOGY_VERSION, "top_level_workflow_run_count": 1, "partition_work_unit_count": len(partitions), "configured_partition_concurrency": 1, "observed_max_partition_concurrency": 1, "output_row_count": expected_rows, "duplicate_output_count": 0, "output_logical_checksum": expected_checksum, "verification_status": "passed"}.items():
                if item.get(key) != value: failures.append(f"{key}={item.get(key)!r}, expected {value!r}")
            for key in ("worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent"):
                value = item.get(key)
                if not isinstance(value, (int, float)) or value < PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT: failures.append(f"{key}={value!r}, requires >= {PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT}")
            if failures: raise RuntimeError(f"Single-workflow v2 validation failed for {framework}/{workload}: {'; '.join(failures)}")
            records.append(item)
    (raw_root / "single-workflow-v2-validation.json").write_text(json.dumps(records, indent=2, sort_keys=True), encoding="utf-8")
    return records
