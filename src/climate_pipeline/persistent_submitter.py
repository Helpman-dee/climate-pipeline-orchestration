"""Submit one already-defined workflow to a warmed persistent control plane.

The timestamps written by this client, rather than its container lifetime, are
the benchmark runtime boundary.  This keeps image/container startup outside the
normal-workflow timing definition for every framework.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

from .topology import PRIMARY_TOPOLOGY_VERSION, topology_metadata


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write(data_root: str, payload: dict) -> None:
    Path(data_root).mkdir(parents=True, exist_ok=True)
    (Path(data_root) / "submission_result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_items(items_json: str | None, items_file: str | None) -> list[dict]:
    """Load partition input without making the Docker command line the payload.

    ``--items-file`` is the normal benchmark transport.  ``--items-json`` is
    retained only for small, interactive invocations.
    """
    try:
        payload = json.loads(Path(items_file).read_text(encoding="utf-8")) if items_file else json.loads(items_json or "")
    except (OSError, json.JSONDecodeError) as exc:
        source = items_file or "--items-json"
        raise ValueError(f"invalid partition-items JSON from {source}: {exc}") from exc
    if not isinstance(payload, list) or not payload:
        raise ValueError("partition items must be a non-empty JSON list")
    required = {"location_id", "year", "run_id", "replay_base_url", "seed", "data_root"}
    seen: set[tuple[str, int]] = set()
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"partition item {index} must be an object")
        missing = sorted(required.difference(item))
        if missing:
            raise ValueError(f"partition item {index} is missing required keys: {', '.join(missing)}")
        if not isinstance(item["location_id"], str) or not item["location_id"] or not isinstance(item["year"], int):
            raise ValueError(f"partition item {index} has invalid location_id/year")
        key = (item["location_id"], item["year"])
        if key in seen:
            raise ValueError(f"partition items contain duplicate location/year: {key[0]}/{key[1]}")
        seen.add(key)
    return payload


def _workload_name(items: list[dict]) -> str:
    return {1: "small", 25: "medium", 120: "large"}.get(len(items), "")


def _airflow(items: list[dict], timeout: int) -> dict:
    workload = _workload_name(items)
    if not workload:
        raise ValueError(f"Unsupported Airflow single-workflow partition count: {len(items)}")
    dag_id = f"nigeria_climate_pipeline_{workload}"
    dag_run_id = f"{items[0]['run_id']}--single-workflow"
    started_at = _utc(); started = time.perf_counter()
    command = ["airflow", "dags", "trigger", dag_id, "--run-id", dag_run_id, "--conf", json.dumps({"partitions": items, "topology_version": PRIMARY_TOPOLOGY_VERSION})]
    created = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if created.returncode:
        raise RuntimeError(f"Airflow single-workflow submission failed: {created.stderr[-1000:] or created.stdout[-1000:]}")
    deadline = time.monotonic() + timeout; last = "unknown"
    while time.monotonic() < deadline:
        listed = subprocess.run(["airflow", "dags", "list-runs", dag_id, "--output", "json"], capture_output=True, text=True, timeout=30)
        if listed.returncode:
            raise RuntimeError(f"Airflow run polling failed: {listed.stderr[-1000:] or listed.stdout[-1000:]}")
        try:
            runs = json.loads(listed.stdout); match = next(run for run in runs if run.get("run_id") == dag_run_id)
            last = str(match.get("state", "unknown")).lower()
        except (json.JSONDecodeError, StopIteration):
            last = "not_found"
        if last == "success":
            return {"workflow_runtime_seconds": time.perf_counter() - started, "submission_started_at": started_at, "completion_verified_at": _utc(), "terminal_state": "success", "external_run_id": dag_run_id, "dag_id": dag_id}
        if last in {"failed", "upstream_failed"}:
            raise RuntimeError(f"Airflow single-workflow terminal state: {last}")
        time.sleep(0.5)
    raise RuntimeError(f"Airflow single-workflow completion timeout; last state: {last}")


def _prefect(items: list[dict], timeout: int) -> dict:
    base = os.environ["PREFECT_API_URL"].rstrip("/")
    deployment = requests.get(f"{base}/deployments/name/nigeria_climate_pipeline/benchmark", timeout=10); deployment.raise_for_status()
    started_at = _utc(); started = time.perf_counter()
    created = requests.post(f"{base}/deployments/{deployment.json()['id']}/create_flow_run", json={"parameters": {"partitions": items, "parallelism": 1}}, timeout=10); created.raise_for_status()
    flow_run_id = created.json()["id"]; deadline = time.monotonic() + timeout; last = "unknown"
    while time.monotonic() < deadline:
        flow_run = requests.get(f"{base}/flow_runs/{flow_run_id}", timeout=10); flow_run.raise_for_status(); last = str(flow_run.json().get("state_type", "UNKNOWN")).lower()
        if last == "completed":
            return {"workflow_runtime_seconds": time.perf_counter() - started, "submission_started_at": started_at, "completion_verified_at": _utc(), "terminal_state": last, "external_run_id": flow_run_id}
        if last in {"failed", "crashed", "cancelled"}:
            raise RuntimeError(f"Prefect terminal state: {last}")
        time.sleep(0.5)
    raise RuntimeError(f"Prefect completion timeout; last state: {last}")


def _dagster(items: list[dict], timeout: int) -> dict:
    endpoint = os.environ.get("DAGSTER_GRAPHQL_URL", "http://dagster-webserver:3000/graphql")
    mutation = """mutation($params: ExecutionParams!) { launchPipelineExecution(executionParams: $params) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } } }"""
    started_at = _utc(); started = time.perf_counter()
    query = "query($runId: ID!) { runOrError(runId: $runId) { __typename ... on Run { status } ... on PythonError { message } } }"
    workload = _workload_name(items)
    if not workload:
        raise ValueError(f"Unsupported Dagster single-workflow partition count: {len(items)}")
    job_name = f"climate_{workload}_benchmark_job"
    variables = {
        "params": {
            "selector": {"jobName": job_name, "repositoryLocationName": "climate_code", "repositoryName": "__repository__"},
            "runConfigData": {"ops": {"ordered_partitions": {"config": {"partitions": items, "expected_partition_count": len(items)}}}},
            "mode": "default",
        }
    }
    response = requests.post(endpoint, json={"query": mutation, "variables": variables}, timeout=15); response.raise_for_status(); result = response.json()
    launched = result.get("data", {}).get("launchPipelineExecution", {})
    if launched.get("__typename") != "LaunchRunSuccess":
        raise RuntimeError(f"Dagster single-workflow submission failed: {result}")
    run_id = launched["run"]["runId"]; deadline = time.monotonic() + timeout; last = "unknown"
    while time.monotonic() < deadline:
        polled = requests.post(endpoint, json={"query": query, "variables": {"runId": run_id}}, timeout=15); polled.raise_for_status(); found = polled.json().get("data", {}).get("runOrError", {}); last = str(found.get("status", "UNKNOWN")).lower()
        if last == "success":
            return {"workflow_runtime_seconds": time.perf_counter() - started, "submission_started_at": started_at, "completion_verified_at": _utc(), "terminal_state": "success", "external_run_id": run_id, "job_name": job_name}
        if last in {"failure", "canceled"}:
            raise RuntimeError(f"Dagster single-workflow terminal state: {last}")
        time.sleep(0.5)
    raise RuntimeError(f"Dagster single-workflow completion timeout; last state: {last}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--framework", choices=("airflow", "prefect", "dagster"), required=True); input_source = parser.add_mutually_exclusive_group(required=True); input_source.add_argument("--items-json"); input_source.add_argument("--items-file"); parser.add_argument("--data-root", required=True); parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args(argv); items = _load_items(args.items_json, args.items_file)
    attempted_at = _utc()
    try:
        result = {"airflow": _airflow, "prefect": _prefect, "dagster": _dagster}[args.framework](items, args.timeout)
        package = {"airflow": "apache-airflow", "prefect": "prefect", "dagster": "dagster"}[args.framework]
        result.update(topology_metadata(items)); result["framework"] = args.framework; result["status"] = "success"; result["orchestrator_version"] = importlib.metadata.version(package); result["python_version"] = sys.version.split()[0]; _write(args.data_root, result); return 0
    except Exception as exc:
        error_traceback = traceback.format_exc()
        # The persistent harness captures this client process. Emit the full
        # traceback so the outer runner can save it before PowerShell handles
        # its nonzero exit code.
        print(error_traceback, file=sys.stderr, end="")
        _write(args.data_root, {"framework": args.framework, "status": "error", "terminal_state": "failed", "submission_started_at": attempted_at, "completion_verified_at": _utc(), **topology_metadata(items), "error": f"{type(exc).__name__}: {exc}", "traceback": error_traceback}); return 1


if __name__ == "__main__":
    raise SystemExit(main())
