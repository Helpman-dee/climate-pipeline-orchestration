[CmdletBinding()]
param(
    [string]$PlanPath = "configs/benchmark_plan.yaml"
)

$ErrorActionPreference = "Stop"
# On Windows PowerShell 5.1, Docker's informational "Container ... Running"
# line can be surfaced on stderr and converted into a NativeCommandError even
# when the compose command exits successfully. Invoking the docker CLI through
# cmd.exe preserves the real exit code without tripping the script.
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $projectRoot

$logDirectory = Join-Path $projectRoot "data/benchmarks/logs"
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
$logPath = Join-Path $logDirectory ("large-normal-{0}.log" -f (Get-Date -Format "yyyyMMdd-HHmmss"))

$runner = @'
from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path.cwd()
sys.path.insert(0, str(ROOT / "src"))

from climate_pipeline.benchmark import (
    _load_yaml,
    _prepare_persistent_stack,
    _run_persistent_one,
    _stop_persistent_stack,
    validate_benchmark_plan,
    workload_partitions,
)

EXPECTED_CHECKSUM = "af5c386f92932617709bcd7ac77e1d7aab970d330912f2b543221e60beab37c9"
EXPECTED_ROWS = 43_830
EXPECTED_PARTITIONS = 120
MINIMUM_COVERAGE_PERCENT = 70.0
TOPOLOGY_VERSION = "single_workflow_partition_units_v2"
RUNS = (("warmup", 0), ("measured", 1), ("measured", 2), ("measured", 3))
FRAMEWORKS = ("airflow", "prefect", "dagster")


def validation_failures(record: dict) -> list[str]:
    """Return every reason a persisted run cannot be safely resumed from."""
    required = {
        "resource_validation_status": "passed",
        "verification_status": "passed",
        "output_schema_status": "passed",
        "output_row_count": EXPECTED_ROWS,
        "duplicate_output_count": 0,
        "output_logical_checksum": EXPECTED_CHECKSUM,
        "successful_partitions": EXPECTED_PARTITIONS,
        "failed_partitions": 0,
        "expected_partitions": EXPECTED_PARTITIONS,
        "topology_version": TOPOLOGY_VERSION,
        "top_level_workflow_run_count": 1,
        "partition_work_unit_count": EXPECTED_PARTITIONS,
        "configured_partition_concurrency": 1,
    }
    failures = [f"{key}={record.get(key)!r}, expected {value!r}" for key, value in required.items() if record.get(key) != value]
    for key in ("worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent"):
        value = record.get(key)
        if not isinstance(value, (int, float)) or value < MINIMUM_COVERAGE_PERCENT:
            failures.append(f"{key}={value!r}, requires >= {MINIMUM_COVERAGE_PERCENT}")
    for key in ("worker_sampler_errors", "control_plane_sampler_errors"):
        errors = record.get(key, [])
        if errors:
            failures.append(f"{key} contains recorded sampler errors: {errors!r}")
    return failures


def saved_valid_run(raw_root: Path, framework: str, phase: str, repetition: int) -> dict | None:
    pattern = f"persistent-{framework}-large-normal-{phase}-{repetition}-*.json"
    candidates: list[tuple[float, dict]] = []
    for path in raw_root.glob(pattern):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Ignoring unreadable saved record {path}: {exc}")
            continue
        failures = validation_failures(record)
        if failures:
            print(f"Ignoring invalid saved record {path.name}: {'; '.join(failures)}")
            continue
        candidates.append((path.stat().st_mtime, record))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def assert_valid(record: dict, label: str) -> None:
    failures = validation_failures(record)
    if failures:
        raise RuntimeError(f"{label} failed required correctness or sampling checks: {'; '.join(failures)}")


def run() -> int:
    plan_path = Path(sys.argv[1])
    plan = _load_yaml(plan_path)
    raw_root = Path(plan["raw_results_root"])
    raw_root.mkdir(parents=True, exist_ok=True)
    partitions = workload_partitions(plan.get("workloads_file", "configs/workloads.yaml"), "large")
    if len(partitions) != EXPECTED_PARTITIONS:
        raise RuntimeError(f"Large workload has {len(partitions)} partitions; expected {EXPECTED_PARTITIONS}")
    prerequisites = validate_benchmark_plan(plan_path)
    if not prerequisites["replay_healthy"] or not all(prerequisites["framework_images_available"].values()):
        raise RuntimeError(f"Benchmark prerequisites are not ready: {prerequisites}")

    completed: dict[tuple[str, str, int], dict] = {}
    for framework in FRAMEWORKS:
        for phase, repetition in RUNS:
            saved = saved_valid_run(raw_root, framework, phase, repetition)
            if saved:
                completed[(framework, phase, repetition)] = saved
                print(f"SKIP valid saved run: {framework} {phase} {repetition} ({saved['run_id']})")

    for framework in FRAMEWORKS:
        pending = [(phase, repetition) for phase, repetition in RUNS if (framework, phase, repetition) not in completed]
        if not pending:
            print(f"SKIP {framework}: all four large-workload runs already validate.")
            continue
        print(f"START {framework}: {pending}")
        _prepare_persistent_stack(framework)
        try:
            for phase, repetition in pending:
                label = f"{framework} large normal {phase} {repetition}"
                print(f"RUN {label}")
                record = asdict(_run_persistent_one(plan, framework, "large", "normal", phase, repetition, partitions, raw_root))
                # _run_persistent_one saves the record before raising on a
                # failed validation.  This additional assertion enforces the
                # standardized full-workload checksum before any later run.
                assert_valid(record, label)
                completed[(framework, phase, repetition)] = record
                print(f"PASS {label}: checksum={record['output_logical_checksum']}, worker_coverage={record['worker_sampling_coverage_percent']:.2f}%, control_coverage={record['control_plane_sampling_coverage_percent']:.2f}%")
        finally:
            _stop_persistent_stack(framework)

    print("\nFINAL LARGE-WORKLOAD SUMMARY")
    for framework in FRAMEWORKS:
        for phase, repetition in RUNS:
            record = completed[(framework, phase, repetition)]
            print(json.dumps({
                "framework": framework,
                "phase": phase,
                "repetition": repetition,
                "run_id": record["run_id"],
                "worker_coverage_percent": record["worker_sampling_coverage_percent"],
                "control_coverage_percent": record["control_plane_sampling_coverage_percent"],
                "rows": record["output_row_count"],
                "duplicates": record["duplicate_output_count"],
                "checksum": record["output_logical_checksum"],
                "verification": record["verification_status"],
            }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
'@

$python = (Get-Command python -ErrorAction Stop).Source
$runnerPath = Join-Path ([System.IO.Path]::GetTempPath()) ("run_large_benchmarks-{0}.py" -f [guid]::NewGuid().ToString("N"))
$runner | Set-Content -LiteralPath $runnerPath -Encoding utf8

"Large benchmark log: $logPath" | Tee-Object -FilePath $logPath
"The script resumes only records that pass checksum, correctness, verification, and >=70% sampler coverage." | Tee-Object -FilePath $logPath -Append
$runningServices = @()
$runningServices = (& docker compose ps --services --filter status=running 2>$null)
if ($LASTEXITCODE -ne 0) {
    $runningServices = @()
}
if (-not ($runningServices -contains "replay")) {
    & docker compose up -d replay 2>&1 | Tee-Object -FilePath $logPath -Append
    if ($LASTEXITCODE -ne 0) {
        throw "Could not start the existing replay service. Review $logPath."
    }
} else {
    "Replay service already running; skipping docker compose up." | Tee-Object -FilePath $logPath -Append
}
try {
    & $python $runnerPath $PlanPath 2>&1 | Tee-Object -FilePath $logPath -Append
    $pythonExitCode = $LASTEXITCODE
}
finally {
    Remove-Item -LiteralPath $runnerPath -Force -ErrorAction SilentlyContinue
}
if ($pythonExitCode -ne 0) {
    throw "Large benchmark stopped with exit code $pythonExitCode. Review $logPath; no later runs were started."
}

"All large-workload runs completed successfully. Log: $logPath" | Tee-Object -FilePath $logPath -Append
