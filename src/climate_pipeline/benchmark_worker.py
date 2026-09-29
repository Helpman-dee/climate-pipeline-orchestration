"""In-container native-orchestrator execution used by the benchmark harness."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil
from .storage import audit_db


class ProcessTreeMonitor:
    """Sample this worker plus descendants from inside the runner container."""
    def __init__(self, interval_seconds: float = 0.25):
        self.interval_seconds = interval_seconds; self.samples: list[dict] = []; self.done = threading.Event(); self.process = psutil.Process(); self.started_at: str | None = None; self.finished_at: str | None = None
    def _tree(self) -> list[psutil.Process]:
        try: return [self.process, *self.process.children(recursive=True)]
        except psutil.Error: return [self.process]
    def _sample(self) -> None:
        processes = self._tree(); cpu = 0.0; rss = 0
        for process in processes:
            try: cpu += process.cpu_percent(None); rss += process.memory_info().rss
            except psutil.Error: pass
        self.samples.append({"timestamp": datetime.now(timezone.utc).isoformat(), "cpu_percent": cpu, "process_tree_rss_bytes": rss, "process_count": len(processes)})
    def start(self) -> None:
        self.started_at = datetime.now(timezone.utc).isoformat()
        for process in self._tree():
            try: process.cpu_percent(None)
            except psutil.Error: pass
        self.thread = threading.Thread(target=self._run, daemon=True); self.thread.start()
    def _run(self) -> None:
        while not self.done.wait(self.interval_seconds): self._sample()
    def stop(self) -> dict:
        self._sample(); self.done.set(); self.thread.join(timeout=2); self.finished_at = datetime.now(timezone.utc).isoformat()
        cpu = [sample["cpu_percent"] for sample in self.samples]; memory = [sample["process_tree_rss_bytes"] for sample in self.samples]
        return {"measurement_start_time": self.started_at, "measurement_end_time": self.finished_at, "sample_interval_seconds": self.interval_seconds, "valid_sample_count": len(self.samples), "cpu_utilisation_samples_percent": cpu, "cpu_utilisation_mean_percent": sum(cpu) / len(cpu) if cpu else None, "cpu_utilisation_peak_percent": max(cpu) if cpu else None, "peak_memory_bytes": max(memory) if memory else None, "average_memory_bytes": sum(memory) / len(memory) if memory else None, "memory_measurement": "process_tree_rss_sum"}


def _version(framework: str) -> str | None:
    package = {"airflow": "apache-airflow", "prefect": "prefect", "dagster": "dagster"}[framework]
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None


def _run_prefect(items: list[dict]) -> None:
    from .orchestrators.prefect_flow import climate_flow
    if climate_flow is None:
        raise RuntimeError("Prefect is not installed")
    climate_flow(items, parallelism=1)


def _run_dagster(items: list[dict]) -> None:
    from dagster import materialize
    from .orchestrators.dagster_assets import climate_partition
    if climate_partition is None:
        raise RuntimeError("Dagster is not installed")
    for item in items:
        result = materialize([climate_partition], run_config={"ops": {"climate_partition": {"config": item}}})
        if not result.success:
            raise RuntimeError("Dagster materialization failed")


def _run_airflow(items: list[dict]) -> None:
    from .orchestrators.airflow_dag import nigeria_climate_pipeline
    for item in items:
        nigeria_climate_pipeline(**item).test()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--framework", choices=("airflow", "prefect", "dagster"), required=True)
    parser.add_argument("--partitions-json", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--replay-url", required=True)
    parser.add_argument("--fault-scenario", default="")
    parser.add_argument("--seed", type=int, required=True)
    args = parser.parse_args(argv)
    items = [{"location_id": location_id, "year": year, "run_id": args.run_id, "replay_base_url": args.replay_url, "fault_scenario": args.fault_scenario or None, "seed": args.seed, "data_root": args.data_root} for location_id, year in json.loads(args.partitions_json)]
    error = None; monitor = ProcessTreeMonitor(); monitor.start()
    try:
        {"airflow": _run_airflow, "prefect": _run_prefect, "dagster": _run_dagster}[args.framework](items)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    resources = monitor.stop()
    statuses: list[str] = []
    try:
        import duckdb
        with duckdb.connect(str(audit_db(args.data_root))) as db:
            statuses = [row[0] for row in db.execute("SELECT status FROM pipeline_runs WHERE run_id=? ORDER BY location_id, year", [args.run_id]).fetchall()]
    except Exception as exc:
        error = error or f"AuditReadError: {exc}"
    result = {"framework": args.framework, "orchestrator_version": _version(args.framework), "python_version": sys.version.split()[0], "partition_statuses": statuses, "error": error, "resources": resources}
    target = Path(args.data_root) / "worker_result.json"; target.parent.mkdir(parents=True, exist_ok=True); target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return 1 if error else 0


if __name__ == "__main__":
    raise SystemExit(main())
