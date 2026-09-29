"""DAG discovery entry point for the version-2 benchmark DAGs."""

# Keep each workload visible to Airflow's DAG-file loader.  Importing only the
# compatibility alias would expose the small DAG and hide medium/large.
from climate_pipeline.orchestrators.airflow_dag import (  # noqa: F401
    airflow_dag_large,
    airflow_dag_medium,
    airflow_dag_small,
)
