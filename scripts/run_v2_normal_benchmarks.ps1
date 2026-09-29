[CmdletBinding()]
param(
    [string]$PlanPath = "configs/benchmark_plan.yaml"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $projectRoot

$logDirectory = Join-Path $projectRoot "data/benchmarks/logs"
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
$runTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logPath = Join-Path $logDirectory ("v2-normal-benchmarks-{0}.log" -f $runTimestamp)
$stderrPath = Join-Path $logDirectory ("v2-normal-benchmarks-{0}.stderr.log" -f $runTimestamp)
$stdoutPath = Join-Path $logDirectory ("v2-normal-benchmarks-{0}.stdout.log" -f $runTimestamp)

# This runner intentionally calls the already-validated persistent harness.
# Its Python process only submits work; each benchmark record is written by
# _run_persistent_one before the next run can begin.
$runner = @'
from __future__ import annotations

import json
import sys
import traceback
from dataclasses import asdict
from pathlib import Path

ROOT = Path.cwd()
sys.path.insert(0, str(ROOT / "src"))

from climate_pipeline.benchmark import (
    PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT,
    _load_yaml,
    _prepare_persistent_stack,
    _run_persistent_one,
    _stop_persistent_stack,
    validate_benchmark_plan,
    workload_partitions,
)
from climate_pipeline.topology import (
    PARTITION_CONCURRENCY,
    PRIMARY_TOPOLOGY_VERSION,
)

FRAMEWORKS = ("airflow", "prefect", "dagster")
WORKLOADS = ("small", "medium", "large")
RUNS = (("warmup", 0), ("measured", 1), ("measured", 2), ("measured", 3))
EXPECTED = {
    "small": {
        "partitions": 1,
        "rows": 365,
        "checksum": "4d2b1e512b56fa31ba481464619a42f28ef7ff843c1041a84ae19e94b8f37955",
    },
    "large": {
        "partitions": 120,
        "rows": 43_830,
        "checksum": "af5c386f92932617709bcd7ac77e1d7aab970d330912f2b543221e60beab37c9",
    },
}


def load_medium_reference() -> dict:
    """Use the validated framework-neutral reference; never invent Medium."""
    path = ROOT / "data/benchmarks/reference/medium-single-workflow-v2/reference_summary.json"
    try:
        reference = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "Verified Medium reference is unavailable. Run the v2 topology validation before measured benchmarks."
        ) from exc
    if reference.get("topology_version") != PRIMARY_TOPOLOGY_VERSION or reference.get("verification") != "passed":
        raise RuntimeError(f"Medium reference is not a verified v2 reference: {reference}")
    if not isinstance(reference.get("row_count"), int) or not isinstance(reference.get("checksum"), str):
        raise RuntimeError(f"Medium reference is incomplete: {reference}")
    return {"partitions": 25, "rows": reference["row_count"], "checksum": reference["checksum"]}


def expected_order(partitions: list[tuple[str, int]]) -> list[dict[str, str | int]]:
    return [{"location_id": location_id, "year": year} for location_id, year in partitions]


def validation_failures(record: dict, workload: str, partitions: list[tuple[str, int]]) -> list[str]:
    expected = EXPECTED[workload]
    required = {
        "resource_validation_status": "passed",
        "verification_status": "passed",
        "output_schema_status": "passed",
        "output_row_count": expected["rows"],
        "duplicate_output_count": 0,
        "output_logical_checksum": expected["checksum"],
        "successful_partitions": expected["partitions"],
        "failed_partitions": 0,
        "expected_partitions": expected["partitions"],
        "topology_version": PRIMARY_TOPOLOGY_VERSION,
        "top_level_workflow_run_count": 1,
        "partition_work_unit_count": expected["partitions"],
        "configured_partition_concurrency": PARTITION_CONCURRENCY,
        "observed_max_partition_concurrency": PARTITION_CONCURRENCY,
        "ordered_partition_list": expected_order(partitions),
    }
    failures = [
        f"{key}={record.get(key)!r}, expected {value!r}"
        for key, value in required.items()
        if record.get(key) != value
    ]
    for key in ("worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent"):
        value = record.get(key)
        if not isinstance(value, (int, float)) or value < PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT:
            failures.append(f"{key}={value!r}, requires >= {PERSISTENT_MIN_SAMPLING_COVERAGE_PERCENT}")
    for key in ("worker_sampler_errors", "control_plane_sampler_errors"):
        if record.get(key, []):
            failures.append(f"{key} contains recorded sampler errors: {record[key]!r}")
    return failures


def saved_valid_run(raw_root: Path, framework: str, workload: str, phase: str, repetition: int, partitions: list[tuple[str, int]]) -> dict | None:
    """Resume only an exactly matching, valid v2 raw record."""
    pattern = f"persistent-{framework}-{workload}-normal-{phase}-{repetition}-*.json"
    candidates: list[tuple[float, dict]] = []
    for path in raw_root.glob(pattern):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"IGNORE unreadable record {path.name}: {exc}", flush=True)
            continue
        failures = validation_failures(record, workload, partitions)
        if failures:
            # Legacy-v1 records necessarily fail the topology check and can
            # never be used as a v2 resume point.
            print(f"IGNORE nonmatching/invalid record {path.name}: {'; '.join(failures)}", flush=True)
            continue
        candidates.append((path.stat().st_mtime, record))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def assert_valid(record: dict, label: str, workload: str, partitions: list[tuple[str, int]]) -> None:
    failures = validation_failures(record, workload, partitions)
    if failures:
        raise RuntimeError(f"STOP {label}: {'; '.join(failures)}")


def summary_row(record: dict) -> dict:
    """Emit every requested measurement from the durable raw record."""
    return {
        "framework": record["orchestrator"],
        "workload": record["workload_size"],
        "phase": record["phase"],
        "repetition": record["repetition_number"],
        "runtime_seconds": record["end_to_end_runtime_seconds"],
        "worker_cpu_mean_percent": record["cpu_utilisation_mean_percent"],
        "worker_cpu_peak_percent": record["cpu_utilisation_peak_percent"],
        "worker_cgroup_memory_mean_bytes": record["average_memory_bytes"],
        "worker_cgroup_memory_peak_bytes": record["peak_memory_bytes"],
        "control_cpu_mean_percent": record["control_plane_cpu_utilisation_mean_percent"],
        "control_cpu_peak_percent": record["control_plane_cpu_utilisation_peak_percent"],
        "control_cgroup_memory_mean_bytes": record["control_plane_average_memory_bytes"],
        "control_cgroup_memory_peak_bytes": record["control_plane_peak_memory_bytes"],
        "persistent_stack_memory_mean_bytes": record["benchmark_stack_cgroup_memory_average_bytes"],
        "persistent_stack_memory_peak_bytes": record["benchmark_stack_cgroup_memory_peak_bytes"],
        "worker_sampling_coverage_percent": record["worker_sampling_coverage_percent"],
        "control_sampling_coverage_percent": record["control_plane_sampling_coverage_percent"],
        "rows": record["output_row_count"],
        "duplicates": record["duplicate_output_count"],
        "checksum": record["output_logical_checksum"],
        "verification": record["verification_status"],
        "topology_version": record["topology_version"],
        "top_level_workflow_runs": record["top_level_workflow_run_count"],
        "partition_work_units": record["partition_work_unit_count"],
        "configured_concurrency": record["configured_partition_concurrency"],
        "observed_concurrency": record["observed_max_partition_concurrency"],
    }


def run() -> int:
    plan_path = Path(sys.argv[1])
    plan = _load_yaml(plan_path)
    EXPECTED["medium"] = load_medium_reference()
    raw_root = Path(plan["raw_results_root"])
    raw_root.mkdir(parents=True, exist_ok=True)
    partition_sets = {
        workload: workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), workload)
        for workload in WORKLOADS
    }
    for workload, partitions in partition_sets.items():
        if len(partitions) != EXPECTED[workload]["partitions"]:
            raise RuntimeError(f"{workload} has {len(partitions)} configured partitions; expected {EXPECTED[workload]['partitions']}")
    prerequisites = validate_benchmark_plan(plan_path)
    if not prerequisites["replay_healthy"] or not all(prerequisites["framework_images_available"].values()):
        raise RuntimeError(f"Benchmark prerequisites are not ready: {prerequisites}")

    completed: dict[tuple[str, str, str, int], dict] = {}
    for framework in FRAMEWORKS:
        for workload in WORKLOADS:
            partitions = partition_sets[workload]
            for phase, repetition in RUNS:
                saved = saved_valid_run(raw_root, framework, workload, phase, repetition, partitions)
                if saved is not None:
                    completed[(framework, workload, phase, repetition)] = saved
                    print(f"SKIP valid v2: framework={framework} workload={workload} phase={phase} repetition={repetition} run_id={saved['run_id']}", flush=True)

    for framework in FRAMEWORKS:
        pending = [
            (workload, phase, repetition)
            for workload in WORKLOADS
            for phase, repetition in RUNS
            if (framework, workload, phase, repetition) not in completed
        ]
        if not pending:
            print(f"SKIP framework={framework}: all v2 normal runs are already valid", flush=True)
            continue
        print(f"START framework={framework} pending={pending}", flush=True)
        _prepare_persistent_stack(framework)
        try:
            for workload, phase, repetition in pending:
                partitions = partition_sets[workload]
                label = f"framework={framework} workload={workload} phase={phase} repetition={repetition}"
                print(f"RUN {label}", flush=True)
                record = asdict(_run_persistent_one(plan, framework, workload, "normal", phase, repetition, partitions, raw_root))
                # The raw JSON was saved atomically by the harness before this
                # assertion; any failure prevents the next submission.
                assert_valid(record, label, workload, partitions)
                completed[(framework, workload, phase, repetition)] = record
                print(
                    f"PASS {label} runtime={record['end_to_end_runtime_seconds']:.3f}s "
                    f"worker_coverage={record['worker_sampling_coverage_percent']:.2f}% "
                    f"control_coverage={record['control_plane_sampling_coverage_percent']:.2f}%",
                    flush=True,
                )
        finally:
            _stop_persistent_stack(framework)

    print("\nFINAL V2 NORMAL-BENCHMARK SUMMARY", flush=True)
    print("| framework | workload | phase | repetition | runtime_s | rows | duplicates | verification | worker_cov | control_cov | topology | top_runs | units | configured | observed |", flush=True)
    print("|---|---|---|---:|---:|---:|---:|---|---:|---:|---|---:|---:|---:|---:|", flush=True)
    for framework in FRAMEWORKS:
        for workload in WORKLOADS:
            for phase, repetition in RUNS:
                record = completed[(framework, workload, phase, repetition)]
                row = summary_row(record)
                print(
                    f"| {row['framework']} | {row['workload']} | {row['phase']} | {row['repetition']} | "
                    f"{row['runtime_seconds']:.3f} | {row['rows']} | {row['duplicates']} | {row['verification']} | "
                    f"{row['worker_sampling_coverage_percent']:.2f}% | {row['control_sampling_coverage_percent']:.2f}% | "
                    f"{row['topology_version']} | {row['top_level_workflow_runs']} | {row['partition_work_units']} | "
                    f"{row['configured_concurrency']} | {row['observed_concurrency']} |",
                    flush=True,
                )
                print("METRICS " + json.dumps(row, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except BaseException:
        # PowerShell redirects this stream before a nonzero native exit can be
        # represented as NativeCommandError.
        traceback.print_exc(file=sys.stderr)
        raise
'@

$python = (Get-Command python -ErrorAction Stop).Source
$runnerPath = Join-Path ([System.IO.Path]::GetTempPath()) ("run_v2_normal_benchmarks-{0}.py" -f [guid]::NewGuid().ToString("N"))
$runner | Set-Content -LiteralPath $runnerPath -Encoding utf8

"V2 normal benchmark log: $logPath" | Tee-Object -FilePath $logPath
"V2 normal benchmark stderr: $stderrPath" | Tee-Object -FilePath $logPath -Append
"Runs: Airflow, Prefect, Dagster × Small, Medium, Large × warm-up + 3 measured. Valid v2 raw records resume; legacy-v1 records never qualify." | Tee-Object -FilePath $logPath -Append

$runningServices = @(& docker compose ps --services --filter status=running 2>$null)
if ($LASTEXITCODE -ne 0) { $runningServices = @() }
if (-not ($runningServices -contains "replay")) {
    & docker compose up -d replay 2>&1 | Tee-Object -FilePath $logPath -Append
    if ($LASTEXITCODE -ne 0) { throw "Could not start replay. Review $logPath." }
} else {
    "Replay service already running; skipping startup." | Tee-Object -FilePath $logPath -Append
}

$pythonExitCode = 1
try {
    # Redirect at process creation, before PowerShell sees either stream. This
    # guarantees that an unhandled Python traceback is durable in $stderrPath
    # and cannot be replaced by a NativeCommandError.
    $pythonProcess = Start-Process -FilePath $python -ArgumentList @("-u", $runnerPath, $PlanPath) -WorkingDirectory $projectRoot -NoNewWindow -PassThru -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $pythonProcess.WaitForExit()
    $pythonExitCode = $pythonProcess.ExitCode
    if (Test-Path -LiteralPath $stdoutPath) {
        Get-Content -LiteralPath $stdoutPath | Tee-Object -FilePath $logPath -Append
    }
}
finally {
    Remove-Item -LiteralPath $runnerPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
}
if ($pythonExitCode -ne 0) {
    throw "V2 normal benchmark stopped with exit code $pythonExitCode. No later pending runs were submitted. Review $logPath and $stderrPath."
}

"All v2 normal benchmark runs completed successfully. Log: $logPath" | Tee-Object -FilePath $logPath -Append
