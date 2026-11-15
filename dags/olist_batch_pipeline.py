"""DAG Airflow : orchestre le pipeline batch Olist.

check_files >> ingest_data >> transform_data >> validate_output
"""
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"  # racine du projet dans le conteneur


def run_module(module: str) -> str:
    return f"cd {PROJECT_DIR} && python -m {module}"


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="olist_batch_pipeline",
    description="CSV Olist -> Parquet (Pandas) -> Parquet + agrégats (Spark) -> validation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["olist", "batch"],
) as dag:
    check_files = BashOperator(
        task_id="check_files",
        bash_command=run_module("src.utils.check_files"),
    )

    ingest_data = BashOperator(
        task_id="ingest_data",
        bash_command=run_module("src.ingestion.etl_pipeline"),
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=run_module("src.processing.olist_spark_pipeline"),
    )

    validate_output = BashOperator(
        task_id="validate_output",
        bash_command=run_module("src.utils.validate_output"),
    )

    check_files >> ingest_data >> transform_data >> validate_output
