from __future__ import annotations
import json, os
from pathlib import Path
import duckdb, nbformat, pandas as pd
from nbclient import NotebookClient
from climate_pipeline.config import load_config
from climate_pipeline.storage import initialise

def test_notebooks_execute_against_fixture_artifacts(tmp_path, monkeypatch):
    config = load_config(); manifest = tmp_path / "manifests" / f"{config['dataset_version']}.json"; manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"entries": [{"location_id":"abuja", "year":2001, "sha256":"fixture"}]}))
    initialise(tmp_path)
    with duckdb.connect(str(tmp_path / "audit.duckdb")) as db:
        db.execute("INSERT INTO quality_incidents VALUES ('r','abuja',2001,'warning','fixture','fixture',1)")
        db.execute("INSERT INTO verification_results VALUES ('r','passed',1,1,'[]')")
    results = tmp_path / "results" / "raw"; results.mkdir(parents=True)
    pd.DataFrame([{ "framework":"fixture", "workload":"small", "fault_scenario":"none", "wall_clock_seconds":1.0 }]).to_parquet(results / "fixture.parquet", index=False)
    monkeypatch.setenv("CLIMATE_ARTIFACT_ROOT", str(tmp_path))
    for path in Path("notebooks").glob("*.ipynb"):
        notebook = nbformat.read(path, as_version=4)
        NotebookClient(notebook, timeout=60, kernel_name="python3").execute(cwd=".")

