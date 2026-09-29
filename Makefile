.PHONY: bootstrap acquire-data start-replay test analyse figures preflight benchmark run-airflow run-prefect run-dagster
bootstrap:
	python -m pip install -e .[test]
acquire-data:
	python -m climate_pipeline acquire-data
start-replay:
	python -m climate_pipeline start-replay
test:
	python -m pytest
analyse:
	python -m climate_pipeline analyse data/results/raw/results.parquet
figures:
	python -m climate_pipeline figures data/results/raw/results.parquet
preflight:
	python -m climate_pipeline preflight
benchmark:
	python -m climate_pipeline benchmark abuja 2001
run-airflow:
	docker compose --profile airflow run --rm airflow
run-prefect:
	docker compose --profile prefect run --rm prefect
run-dagster:
	docker compose --profile dagster run --rm dagster
