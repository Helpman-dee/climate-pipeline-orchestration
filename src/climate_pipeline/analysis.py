from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np

def summarize_benchmarks(path: str | Path, metric: str = "wall_clock_seconds") -> pd.DataFrame:
    frame = pd.read_parquet(path) if str(path).endswith(".parquet") else pd.read_csv(path)
    grouped = frame.groupby(["framework", "workload", "fault_scenario"], dropna=False)[metric]
    result = grouped.agg(["count", "median", "mean", "std", "min", "max"]).reset_index()
    result["p95"] = grouped.quantile(.95).to_numpy()
    result["iqr"] = (grouped.quantile(.75) - grouped.quantile(.25)).to_numpy()
    rng = np.random.default_rng(0)
    intervals = []
    for _, values in grouped:
        values = values.dropna().to_numpy()
        if len(values) < 2:
            intervals.append((np.nan, np.nan))
        else:
            means = np.array([rng.choice(values, size=len(values), replace=True).mean() for _ in range(1000)])
            intervals.append(tuple(np.quantile(means, [.025, .975])))
    result[["mean_ci95_low", "mean_ci95_high"]] = intervals
    return result

def cliffs_delta(left: pd.Series, right: pd.Series) -> float:
    """Non-parametric effect size; callers decide whether sample size supports inference."""
    pairs = np.subtract.outer(left.dropna().to_numpy(), right.dropna().to_numpy())
    return float((np.sum(pairs > 0) - np.sum(pairs < 0)) / pairs.size) if pairs.size else float("nan")

def generate_figures(results_path: str | Path, output_dir: str | Path) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns
    frame = pd.read_parquet(results_path) if str(results_path).endswith(".parquet") else pd.read_csv(results_path)
    output = Path(output_dir); output.mkdir(parents=True, exist_ok=True)
    fig, axis = plt.subplots(figsize=(8, 4))
    sns.boxplot(data=frame, x="framework", y="wall_clock_seconds", hue="workload", ax=axis)
    axis.set_title("End-to-end runtime distributions")
    path = output / "runtime_distributions.png"; fig.savefig(path, dpi=300, bbox_inches="tight"); plt.close(fig)
    return [path]
