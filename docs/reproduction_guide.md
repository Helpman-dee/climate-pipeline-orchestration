# Reproduction guide

1. Install Python 3.13 and Docker Desktop, then run `python -m pip install -e ".[test]"`.
2. Confirm prerequisites with `python -m climate_pipeline preflight`.
3. Run `python -m climate_pipeline acquire-data` once. This creates the immutable local NASA POWER snapshot and dataset manifest; do not use it during experiments.
4. Start local replay with `python -m climate_pipeline start-replay`.
5. Run one framework at a time using `make run-airflow`, `make run-prefect`, or `make run-dagster`; save the workload, seed, fault, and repetition configuration with each run.
6. Run `python -m climate_pipeline benchmark abuja 2001` for a local-core smoke benchmark, then analyse a saved raw result with `python -m climate_pipeline analyse <result.parquet>` and regenerate figures with `python -m climate_pipeline figures <result.parquet>`.
7. Run `python -m pytest` and execute the notebooks only against saved artefacts. Record completed experiment facts in `docs/paper_evidence.md`.

Canonical raw snapshots, curated outputs, and results are Git-ignored by design; retain their manifest/checksum files with any submitted reproducibility archive.
