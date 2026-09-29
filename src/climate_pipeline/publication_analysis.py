"""Reproducible, descriptive publication analysis for frozen experiments.

Raw benchmark, fault, and reprocessing evidence is read-only.  This module
creates a separate analysis tree containing a master dataset, provenance,
tables, figures, and a concise evidence-bound research summary.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

from .backfill_experiments import _assert_reprocessing_record
from .fault_experiments import _assert_condition, _enrich_record
from .topology import PARTITION_CONCURRENCY, PRIMARY_TOPOLOGY_VERSION


ANALYSIS_VERSION = "publication-analysis-v1"
EXPECTED_NORMAL = {
    "small": {"partitions": 1, "rows": 365, "checksum": "4d2b1e512b56fa31ba481464619a42f28ef7ff843c1041a84ae19e94b8f37955"},
    "medium": {"partitions": 25, "rows": 9130, "checksum": "d2d7ba2ddcee843ca27216cec361873203ec3165459705afd37f1b2317257dbb"},
    "large": {"partitions": 120, "rows": 43830, "checksum": "af5c386f92932617709bcd7ac77e1d7aab970d330912f2b543221e60beab37c9"},
}
FRAMEWORK_ORDER = ["airflow", "prefect", "dagster"]
WORKLOAD_ORDER = ["small", "medium", "large"]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json_records(path: Path) -> tuple[list[dict[str, Any]], str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [], f"unreadable_json:{type(exc).__name__}"
    if isinstance(payload, dict):
        return [payload], None
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], "aggregate_or_legacy_json_array"
    return [], "non_record_json"


def _normal_selection(root: Path) -> tuple[dict[tuple[str, str, int], tuple[str, str | float]], Path]:
    """Read the final resumable runner's accepted v2 record IDs."""
    logs = sorted((root / "benchmarks" / "logs").glob("v2-normal-benchmarks-*.log"), key=lambda path: path.stat().st_mtime, reverse=True)
    expression = re.compile(r"^SKIP valid v2: framework=(airflow|prefect|dagster) workload=(small|medium|large) phase=measured repetition=([123]) run_id=(\S+)$")
    completed = re.compile(r"^PASS framework=(airflow|prefect|dagster) workload=(small|medium|large) phase=measured repetition=([123]) runtime=([0-9.]+)s ")
    for log in logs:
        selected: dict[tuple[str, str, int], tuple[str, str | float]] = {}
        raw = log.read_bytes()
        text = raw.decode("utf-16") if raw.startswith((b"\xff\xfe", b"\xfe\xff")) else raw.decode("utf-8", errors="replace")
        for line in text.splitlines():
            match = expression.match(line.strip())
            if match:
                framework, workload, repetition, run_id = match.groups()
                selected[(framework, workload, int(repetition))] = ("run_id", run_id)
                continue
            match = completed.match(line.strip())
            if match:
                framework, workload, repetition, runtime = match.groups()
                selected[(framework, workload, int(repetition))] = ("runtime_seconds", float(runtime))
        if len(selected) == 27:
            return selected, log
    raise RuntimeError("No final v2 normal-runner log contains all 27 accepted measured record IDs")


def _normal_failures(record: dict[str, Any]) -> list[str]:
    workload = record.get("workload_size")
    expected = EXPECTED_NORMAL.get(workload)
    if expected is None:
        return ["unknown_workload"]
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
    }
    failures = [key for key, value in required.items() if record.get(key) != value]
    for key in ("worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent"):
        value = record.get(key)
        if not isinstance(value, (int, float)) or value < 70.0:
            failures.append(f"{key}_below_70")
    if record.get("worker_sampler_errors") or record.get("control_plane_sampler_errors"):
        failures.append("sampler_errors")
    if record.get("worker_exit_code") != 0:
        failures.append("worker_exit_code")
    return failures


def _normal_records(root: Path, provenance: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected_targets, selection_log = _normal_selection(root)
    accepted: list[dict[str, Any]] = []
    raw_root = root / "benchmarks" / "raw"
    for path in sorted(raw_root.glob("*.json")):
        records, structural_reason = _read_json_records(path)
        if structural_reason:
            provenance.append({"source_path": str(path), "source_sha256": _sha256(path), "analysis_domain": "normal", "included": False, "reason": structural_reason, "run_id": None})
        for record in records:
            run_id = record.get("run_id")
            is_candidate = isinstance(run_id, str) and run_id.startswith("persistent-") and record.get("scenario") == "normal"
            key = (record.get("orchestrator"), record.get("workload_size"), record.get("repetition_number"))
            target = selected_targets.get(key)
            selected_match = target is not None and record.get("phase") == "measured" and ((target[0] == "run_id" and run_id == target[1]) or (target[0] == "runtime_seconds" and abs(float(record.get("end_to_end_runtime_seconds", -1)) - float(target[1])) < 0.0005))
            if selected_match:
                failures = _normal_failures(record)
                if failures:
                    raise RuntimeError(f"Authoritative normal record {run_id} fails correctness checks: {failures}")
                enriched = dict(record)
                enriched.update({"analysis_domain": "normal", "source_path": str(path), "source_sha256": _sha256(path), "selection_basis": "final_v2_resumable_runner_log", "framework": record["orchestrator"], "workload": record["workload_size"], "runtime_seconds": record["end_to_end_runtime_seconds"]})
                accepted.append(enriched)
                provenance.append({"source_path": str(path), "source_sha256": enriched["source_sha256"], "analysis_domain": "normal", "included": True, "reason": "authoritative_v2_measured_selection", "run_id": run_id})
            elif is_candidate:
                reason = "warmup_not_primary_observation" if record.get("phase") == "warmup" else ("not_authoritative_v2_selection" if not _normal_failures(record) else "invalid_or_legacy_normal_record:" + ",".join(_normal_failures(record)))
                provenance.append({"source_path": str(path), "source_sha256": _sha256(path), "analysis_domain": "normal", "included": False, "reason": reason, "run_id": run_id})
    if len(accepted) != 27:
        raise RuntimeError(f"Expected exactly 27 accepted normal measured records, got {len(accepted)}")
    if len({(item["framework"], item["workload"], item["repetition_number"]) for item in accepted}) != 27:
        raise RuntimeError("Accepted normal records are not one per framework/workload/measured repetition")
    return accepted, {"selection_log": str(selection_log), "selection_log_sha256": _sha256(selection_log), "selected_records": {f"{framework}/{workload}/{repetition}": {kind: value} for (framework, workload, repetition), (kind, value) in selected_targets.items()}}


def _fault_records(root: Path, provenance: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: dict[tuple[str, str], tuple[float, dict[str, Any], Path]] = {}
    for path in sorted((root / "benchmarks" / "fault_recovery" / "raw").glob("*.json")):
        records, structural_reason = _read_json_records(path)
        if structural_reason:
            provenance.append({"source_path": str(path), "source_sha256": _sha256(path), "analysis_domain": "fault", "included": False, "reason": structural_reason, "run_id": None})
        for record in records:
            condition = record.get("condition") or record.get("scenario")
            framework = record.get("orchestrator")
            try:
                enriched = _enrich_record(dict(record))
                _assert_condition(condition, enriched)
            except Exception as exc:
                provenance.append({"source_path": str(path), "source_sha256": _sha256(path), "analysis_domain": "fault", "included": False, "reason": f"invalid_fault_record:{type(exc).__name__}", "run_id": record.get("run_id")})
                continue
            key = (framework, condition)
            prior = candidates.get(key)
            if prior is None or path.stat().st_mtime > prior[0]:
                candidates[key] = (path.stat().st_mtime, enriched, path)
    if set(candidates) != {(framework, condition) for framework in FRAMEWORK_ORDER for condition in ("transient_http", "transient_storage", "invalid_observation")}:
        raise RuntimeError(f"Validated fault matrix is incomplete: {sorted(candidates)}")
    accepted = []
    for _, record, path in candidates.values():
        enriched = dict(record)
        enriched.update({"analysis_domain": "fault", "source_path": str(path), "source_sha256": _sha256(path), "framework": record["orchestrator"], "runtime_seconds": record["end_to_end_runtime_seconds"]})
        accepted.append(enriched)
        provenance.append({"source_path": str(path), "source_sha256": enriched["source_sha256"], "analysis_domain": "fault", "included": True, "reason": "latest_valid_condition_record", "run_id": record["run_id"]})
    return accepted


def _reprocessing_records(root: Path, provenance: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: dict[str, tuple[float, dict[str, Any], Path]] = {}
    for path in sorted((root / "benchmarks" / "backfill" / "raw").glob("*.json")):
        records, structural_reason = _read_json_records(path)
        if structural_reason:
            provenance.append({"source_path": str(path), "source_sha256": _sha256(path), "analysis_domain": "reprocessing", "included": False, "reason": structural_reason, "run_id": None})
        for record in records:
            try:
                _assert_reprocessing_record(record)
            except Exception as exc:
                provenance.append({"source_path": str(path), "source_sha256": _sha256(path), "analysis_domain": "reprocessing", "included": False, "reason": f"invalid_reprocessing_record:{type(exc).__name__}", "run_id": record.get("run_id")})
                continue
            framework = record["orchestrator"]
            prior = candidates.get(framework)
            if prior is None or path.stat().st_mtime > prior[0]:
                candidates[framework] = (path.stat().st_mtime, record, path)
    if set(candidates) != set(FRAMEWORK_ORDER):
        raise RuntimeError(f"Validated framework-native reprocessing set is incomplete: {sorted(candidates)}")
    accepted = []
    for _, record, path in candidates.values():
        enriched = dict(record)
        enriched.update({"analysis_domain": "reprocessing", "source_path": str(path), "source_sha256": _sha256(path), "framework": record["orchestrator"], "runtime_seconds": record["end_to_end_runtime_seconds"]})
        accepted.append(enriched)
        provenance.append({"source_path": str(path), "source_sha256": enriched["source_sha256"], "analysis_domain": "reprocessing", "included": True, "reason": "validated_framework_native_reprocessing", "run_id": record["run_id"]})
    return accepted


def _markdown(frame: pd.DataFrame) -> str:
    columns = list(frame.columns)
    rows = [["" if pd.isna(value) else str(value) for value in row] for row in frame.itertuples(index=False, name=None)]
    widths = [max(len(str(column)), *(len(row[index]) for row in rows)) for index, column in enumerate(columns)]
    head = "| " + " | ".join(str(column).ljust(widths[index]) for index, column in enumerate(columns)) + " |"
    divider = "|" + "|".join("-" * (width + 2) for width in widths) + "|"
    body = ["| " + " | ".join(row[index].ljust(widths[index]) for index in range(len(columns))) + " |" for row in rows]
    return "\n".join([head, divider, *body]) + "\n"


def _write_table(frame: pd.DataFrame, name: str, tables: Path) -> None:
    frame.to_csv(tables / f"{name}.csv", index=False)
    (tables / f"{name}.md").write_text(_markdown(frame), encoding="utf-8")


def _normal_tables(normal: pd.DataFrame, tables: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    normal = normal.copy()
    normal["framework"] = pd.Categorical(normal["framework"], FRAMEWORK_ORDER, ordered=True)
    normal["workload"] = pd.Categorical(normal["workload"], WORKLOAD_ORDER, ordered=True)
    _write_table(normal.sort_values(["framework", "workload", "repetition_number"])[["framework", "workload", "repetition_number", "run_id", "runtime_seconds", "output_row_count", "worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent"]], "normal_measured_individual_runs", tables)
    runtime = normal.groupby(["framework", "workload"], observed=True)["runtime_seconds"].agg(n="count", mean="mean", median="median", sample_sd="std", minimum="min", maximum="max").reset_index()
    runtime["coefficient_of_variation_percent"] = 100 * runtime["sample_sd"] / runtime["mean"]
    runtime = runtime.round(3)
    _write_table(runtime, "normal_runtime_descriptive_statistics", tables)
    aggregate = {
        "cpu_utilisation_mean_percent": "worker_cpu_mean_percent",
        "cpu_utilisation_peak_percent": "worker_cpu_peak_percent",
        "control_plane_cpu_utilisation_mean_percent": "control_plane_cpu_mean_percent",
        "control_plane_cpu_utilisation_peak_percent": "control_plane_cpu_peak_percent",
        "average_memory_bytes": "worker_memory_mean_mib",
        "peak_memory_bytes": "worker_memory_peak_mib",
        "control_plane_average_memory_bytes": "control_plane_memory_mean_mib",
        "control_plane_peak_memory_bytes": "control_plane_memory_peak_mib",
        "benchmark_stack_cgroup_memory_average_bytes": "persistent_stack_memory_mean_mib",
        "benchmark_stack_cgroup_memory_peak_bytes": "persistent_stack_memory_peak_mib",
        "worker_sampling_coverage_percent": "worker_sampling_coverage_percent",
        "control_plane_sampling_coverage_percent": "control_plane_sampling_coverage_percent",
    }
    resource = normal.groupby(["framework", "workload"], observed=True).size().rename("n").reset_index()
    for source, target in aggregate.items():
        values = normal.groupby(["framework", "workload"], observed=True)[source]
        if source.endswith("bytes"):
            # The source names use "average_memory", rather than "mean_memory".
            # Aggregate the per-run averages as a mean; peaks remain a group max.
            resource[target] = (values.mean().to_numpy() if "average_memory" in source else values.max().to_numpy()) / (1024 ** 2)
        elif "coverage" in source:
            resource[f"{target}_mean"] = values.mean().to_numpy()
            resource[f"{target}_min"] = values.min().to_numpy()
        else:
            resource[target] = values.mean().to_numpy() if "mean" in source else values.max().to_numpy()
    resource = resource.round(3)
    _write_table(resource, "normal_resource_use_statistics", tables)
    return runtime, resource


def _fault_table(fault: pd.DataFrame, normal_runtime: pd.DataFrame, tables: Path) -> pd.DataFrame:
    median = normal_runtime.loc[normal_runtime["workload"] == "medium", ["framework", "median"]].rename(columns={"median": "normal_medium_median_runtime_seconds"})
    result = fault.merge(median, on="framework", how="left")
    recovery = result["condition"].isin(["transient_http", "transient_storage"])
    result["descriptive_runtime_overhead_seconds"] = (result["runtime_seconds"] - result["normal_medium_median_runtime_seconds"]).where(recovery)
    result["descriptive_runtime_overhead_ratio"] = (result["runtime_seconds"] / result["normal_medium_median_runtime_seconds"]).where(recovery)
    result["quality_gate_evidence"] = result.apply(lambda row: "hard precipitation_bounds; terminal failure; missing blocked output; no curated output" if row["condition"] == "invalid_observation" else "recovery success", axis=1)
    columns = ["framework", "condition", "runtime_seconds", "descriptive_runtime_overhead_seconds", "descriptive_runtime_overhead_ratio", "retry_count", "failure_recovery_seconds", "successful_partitions", "failed_partitions", "duplicate_output_count", "verification_status", "output_logical_checksum", "native_orchestration_state", "semantic_terminal_state", "worker_sampling_coverage_percent", "control_plane_sampling_coverage_percent", "quality_gate_evidence"]
    result = result[columns].sort_values(["framework", "condition"]).round(3)
    _write_table(result, "fault_recovery_data_quality_outcomes", tables)
    return result


def _reprocessing_table(reprocessing: pd.DataFrame, tables: Path) -> pd.DataFrame:
    result = pd.DataFrame({
        "framework": reprocessing["framework"],
        "framework_native_reprocessing_mechanism": reprocessing["framework_native_reprocessing_mechanism"],
        "duration_seconds": reprocessing["runtime_seconds"],
        "requested_partitions": reprocessing["requested_partitions"],
        "completed_partitions": reprocessing["completed_partitions"],
        "failed_partitions": reprocessing["failed_partitions"],
        "before_partition_files": reprocessing["before_output_evidence"].map(lambda value: value["file_count"]),
        "after_partition_files": reprocessing["after_output_evidence"].map(lambda value: value["file_count"]),
        "before_natural_key_duplicates": reprocessing["before_output_evidence"].map(lambda value: value["natural_key_duplicate_count"]),
        "after_natural_key_duplicates": reprocessing["after_output_evidence"].map(lambda value: value["natural_key_duplicate_count"]),
        "before_checksum": reprocessing["before_output_evidence"].map(lambda value: value["logical_checksum"]),
        "after_checksum": reprocessing["after_output_evidence"].map(lambda value: value["logical_checksum"]),
        "deterministic_atomic_replace": reprocessing["idempotency_output_overwrite"].map(lambda value: value["deterministic_atomic_replace"]),
        "verification_status": reprocessing["verification_status"],
        "worker_sampling_coverage_percent": reprocessing["worker_sampling_coverage_percent"],
        "control_plane_sampling_coverage_percent": reprocessing["control_plane_sampling_coverage_percent"],
    }).sort_values("framework").round(3)
    _write_table(result, "framework_native_reprocessing_idempotency_outcomes", tables)
    return result


def _save_figure(figure: plt.Figure, output: Path, name: str) -> list[Path]:
    paths = [output / f"{name}.png", output / f"{name}.pdf"]
    for path in paths:
        figure.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(figure)
    return paths


def _figures(normal: pd.DataFrame, runtime: pd.DataFrame, resource: pd.DataFrame, fault: pd.DataFrame, figures: Path) -> list[Path]:
    plt.style.use("seaborn-v0_8-whitegrid")
    colors = {"airflow": "#4C78A8", "prefect": "#F58518", "dagster": "#54A24B"}
    outputs: list[Path] = []
    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    positions = {name: index for index, name in enumerate(WORKLOAD_ORDER)}
    for framework in FRAMEWORK_ORDER:
        subset = normal[normal["framework"] == framework]
        for workload in WORKLOAD_ORDER:
            values = subset.loc[subset["workload"] == workload, "runtime_seconds"]
            axis.scatter([positions[workload]] * len(values), values, color=colors[framework], alpha=.75, s=38, label=framework if workload == "small" else None)
        medians = subset.groupby("workload", observed=True)["runtime_seconds"].median().reindex(WORKLOAD_ORDER)
        axis.plot(range(3), medians, color=colors[framework], marker="o", linewidth=2.2)
    axis.set_yscale("log"); axis.set_xticks(range(3), ["Small (1)", "Medium (25)", "Large (120)"])
    axis.set_ylabel("Workflow runtime (seconds, log scale)"); axis.set_xlabel("Ordered partition workload (count)")
    axis.set_title("Runtime scaling: individual measured runs and median (n=3)"); axis.legend(title="Framework")
    outputs += _save_figure(fig, figures, "runtime_scaling_individual_and_median")
    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    for index, framework in enumerate(FRAMEWORK_ORDER):
        subset = runtime[runtime["framework"] == framework]
        offset = (index - 1) * .24
        axis.bar([WORKLOAD_ORDER.index(value) + offset for value in subset["workload"]], subset["coefficient_of_variation_percent"], width=.23, color=colors[framework], label=framework)
    axis.set_xticks(range(3), ["Small", "Medium", "Large"]); axis.set_ylabel("Runtime coefficient of variation (%)")
    axis.set_title("Observed runtime variability across three measured repetitions"); axis.legend(title="Framework")
    outputs += _save_figure(fig, figures, "runtime_variability_coefficient_of_variation")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharex=True)
    for index, framework in enumerate(FRAMEWORK_ORDER):
        subset = resource[resource["framework"] == framework]
        offset = (index - 1) * .24
        positions = [WORKLOAD_ORDER.index(value) + offset for value in subset["workload"]]
        axes[0].bar(positions, subset["worker_memory_peak_mib"], .23, color=colors[framework], label=framework)
        axes[1].bar(positions, subset["worker_cpu_peak_percent"], .23, color=colors[framework], label=framework)
    for axis, ylabel, title in zip(axes, ["Peak worker memory (MiB)", "Peak worker CPU (%)"], ["Worker memory footprint", "Worker CPU peak"]):
        axis.set_xticks(range(3), ["Small", "Medium", "Large"]); axis.set_ylabel(ylabel); axis.set_title(title)
    axes[1].legend(title="Framework"); fig.suptitle("Resource footprint across measured normal runs (n=3 per group)")
    outputs += _save_figure(fig, figures, "resource_footprint_comparison")
    compact = fault[["framework", "condition", "retry_count", "successful_partitions", "failed_partitions", "verification_status", "semantic_terminal_state"]].copy()
    compact.columns = ["Framework", "Condition", "Retries", "Succeeded", "Failed", "Verification", "Terminal state"]
    fig, axis = plt.subplots(figsize=(10.2, 3.2)); axis.axis("off")
    table = axis.table(cellText=compact.values, colLabels=compact.columns, cellLoc="center", loc="center")
    table.auto_set_font_size(False); table.set_fontsize(8.5); table.scale(1, 1.45)
    axis.set_title("Fault recovery and hard data-quality gate behaviour (one validated run per condition)", pad=12)
    outputs += _save_figure(fig, figures, "fault_recovery_and_quality_gate_behaviour")
    return outputs


def _summary(root: Path, runtime: pd.DataFrame, fault: pd.DataFrame, reprocessing: pd.DataFrame, output: Path) -> Path:
    lines = [
        "# Analysis summary",
        "",
        "This report is descriptive. It uses only the frozen, validated v2 normal records selected by the final resumable-run log (27 measured observations: 3 frameworks × 3 workloads × 3 repetitions), the 9 validated fault/data-quality records, and the 3 validated framework-native reprocessing records. Warm-ups, legacy topology records, infrastructure diagnostics, and superseded attempts are retained in analysis provenance but excluded from primary quantitative tables.",
        "",
        "## Verified normal-runtime findings",
        "",
        _markdown(runtime),
        "Median runtime is the primary central measure because each group has n=3. Mean, sample SD, and CV are reported as supporting descriptive statistics; no inferential significance claims are made.",
        "",
        "## Correctness and controlled topology",
        "",
        "Every included normal record passed verification, had the expected workload row count and checksum, zero duplicate natural keys, `single_workflow_partition_units_v2`, one configured and observed partition worker, and worker/control sampling coverage of at least 70%.",
        "",
        "## Fault recovery and data quality (RQ2/RQ3)",
        "",
        "All six transient recovery runs completed 25 partitions with 25 retries, zero failed partitions, zero duplicates, passed verification, and the expected medium checksum. Runtime and recovery overhead values are descriptive only: there is one experimental run per framework-condition pair. In all three invalid-observation runs, the `precipitation_bounds` hard incident produced semantic terminal failure; the expected output was missing and corrupted curated output was blocked.",
        "",
        _markdown(fault),
        "",
        "## Framework-native reprocessing/backfill-style runs (RQ4)",
        "",
        "The three runs are operational comparisons, not equivalent first-class native backfills. Each began with 25 verified existing partition files and ended with 25 files, zero duplicate natural keys, the same medium checksum, and deterministic atomic replacement at the fixed partition path.",
        "",
        _markdown(reprocessing),
        "",
        "## Limitations",
        "",
        "The normal comparison has three measured repetitions per group; variability can therefore be substantial and is shown rather than removed. Fault and framework-native reprocessing conditions each have one validated run per framework, so their overheads are descriptive and not estimates of a stable distribution. Resource values are environment-specific container/process measurements with the recorded coverage thresholds.",
        "",
        "## Research-question traceability",
        "",
        "- **RQ1:** normal runtime scaling, variability, resource footprints, correctness, and topology controls.\n- **RQ2:** retry counts, recovery duration, and successful output verification under transient extraction/storage faults.\n- **RQ3:** hard quality-gate termination and blocked corrupted output.\n- **RQ4:** framework-native reprocessing/backfill-style operational mechanisms and idempotent overwrite evidence.",
    ]
    path = output / "analysis_summary.md"; path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_publication_analysis(data_root: str | Path = "data") -> dict[str, Any]:
    """Build all derived publication artifacts without modifying raw evidence."""
    root = Path(data_root); output = root / "benchmarks" / "analysis"; tables = output / "tables"; figures = output / "figures"
    for directory in (output, tables, figures): directory.mkdir(parents=True, exist_ok=True)
    provenance: list[dict[str, Any]] = []
    normal_records, selection = _normal_records(root, provenance)
    fault_records = _fault_records(root, provenance)
    reprocessing_records = _reprocessing_records(root, provenance)
    master = pd.json_normalize([*normal_records, *fault_records, *reprocessing_records], sep=".")
    master.to_parquet(output / "master_analysis_dataset.parquet", index=False)
    master.to_csv(output / "master_analysis_dataset.csv", index=False)
    pd.DataFrame(provenance).to_csv(output / "excluded_and_included_provenance.csv", index=False)
    manifest = {"analysis_version": ANALYSIS_VERSION, "normal_selection": selection, "included_counts": {"normal": len(normal_records), "fault": len(fault_records), "reprocessing": len(reprocessing_records)}, "raw_records_read_only": True}
    (output / "analysis_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str), encoding="utf-8")
    normal = pd.DataFrame(normal_records); fault = pd.DataFrame(fault_records); reprocessing = pd.DataFrame(reprocessing_records)
    runtime, resource = _normal_tables(normal, tables)
    fault_table = _fault_table(fault, runtime, tables)
    reprocessing_table = _reprocessing_table(reprocessing, tables)
    figure_paths = _figures(normal, runtime, resource, fault_table, figures)
    summary = _summary(root, runtime, fault_table, reprocessing_table, Path("docs"))
    return {"analysis_root": output, "master_dataset": output / "master_analysis_dataset.parquet", "tables": sorted(tables.glob("*")), "figures": figure_paths, "summary": summary, "manifest": manifest}


if __name__ == "__main__":
    artifacts = build_publication_analysis()
    print(json.dumps({key: str(value) if isinstance(value, Path) else [str(item) for item in value] if isinstance(value, list) else value for key, value in artifacts.items()}, indent=2, default=str))
