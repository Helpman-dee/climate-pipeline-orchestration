from __future__ import annotations
import argparse, json, shutil
from pathlib import Path
from .acquisition import acquire_canonical_dataset
from .analysis import generate_figures, summarize_benchmarks
from .benchmark import run_benchmark_validation, run_framework_small_validation, run_local_benchmark, run_persistent_normal_scaling, run_persistent_small_memory_validation, run_persistent_small_validation, run_prefect_small_validation, run_single_workflow_topology_validation, validate_benchmark_plan
from .fault_experiments import run_fault_recovery_matrix
from .backfill_experiments import run_framework_native_reprocessing
from .config import load_config
from .replay_api.app import create_app

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="climate-pipeline")
    parser.add_argument("--data-root", default="data"); parser.add_argument("--config", default="configs/dataset.yaml")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("acquire-data")
    replay = commands.add_parser("start-replay"); replay.add_argument("--port", type=int, default=8000)
    check = commands.add_parser("preflight")
    benchmark = commands.add_parser("benchmark"); benchmark.add_argument("location_id"); benchmark.add_argument("year", type=int); benchmark.add_argument("--replay-url", default="http://127.0.0.1:8000"); benchmark.add_argument("--fault")
    benchmark_plan = commands.add_parser("benchmark-plan"); benchmark_plan.add_argument("action", choices=("validate", "run-small-normal", "run-prefect-small-normal", "run-airflow-small-normal", "run-dagster-small-normal", "run-persistent-small-normal", "run-persistent-small-memory-validation", "run-persistent-normal-scaling", "run-persistent-large-normal-scaling", "run-single-workflow-topology-validation", "run-fault-recovery-matrix", "run-framework-native-reprocessing")); benchmark_plan.add_argument("--plan", default="configs/benchmark_plan.yaml")
    analyse = commands.add_parser("analyse"); analyse.add_argument("results")
    figures = commands.add_parser("figures"); figures.add_argument("results")
    args = parser.parse_args(argv)
    if args.command == "acquire-data": print(json.dumps(acquire_canonical_dataset(args.data_root, args.config).__dict__, default=str)); return 0
    if args.command == "start-replay":
        import uvicorn; uvicorn.run(create_app(args.data_root, args.config), host="0.0.0.0", port=args.port); return 0
    if args.command == "preflight":
        report = {"docker_available": bool(shutil.which("docker")), "config_exists": Path(args.config).exists(), "data_root": str(Path(args.data_root).resolve())}; print(json.dumps(report, indent=2)); return 0 if all(report.values()) else 1
    if args.command == "benchmark": print(json.dumps(run_local_benchmark([(args.location_id, args.year)], args.replay_url, args.data_root, args.fault), indent=2)); return 0
    if args.command == "benchmark-plan":
        if args.action == "validate": result = validate_benchmark_plan(args.plan)
        elif args.action == "run-fault-recovery-matrix":
            run_fault_recovery_matrix(args.plan)
            return 0
        elif args.action == "run-framework-native-reprocessing":
            run_framework_native_reprocessing(args.plan)
            return 0
        elif args.action == "run-single-workflow-topology-validation": result = run_single_workflow_topology_validation(args.plan)
        elif args.action == "run-persistent-large-normal-scaling": result = run_persistent_normal_scaling(args.plan, workloads=("large",))
        elif args.action == "run-persistent-normal-scaling": result = run_persistent_normal_scaling(args.plan)
        elif args.action == "run-persistent-small-memory-validation": result = run_persistent_small_memory_validation(args.plan)
        elif args.action == "run-persistent-small-normal": result = run_persistent_small_validation(args.plan)
        elif args.action == "run-prefect-small-normal": result = run_prefect_small_validation(args.plan)
        elif args.action in {"run-airflow-small-normal", "run-dagster-small-normal"}: result = run_framework_small_validation(args.action.removeprefix("run-").removesuffix("-small-normal"), args.plan)
        else: result = run_benchmark_validation(args.plan)
        print(json.dumps(result, indent=2)); return 0
    if args.command == "analyse": print(summarize_benchmarks(args.results).to_csv(index=False)); return 0
    if args.command == "figures": print(*generate_figures(args.results, Path(args.data_root) / "results" / "figures"), sep="\n"); return 0
    return 1

if __name__ == "__main__": raise SystemExit(main())
