from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    "insurance_batch_pipeline_gcs",
    default_args=default_args,
    description="Production MySQL -> GCS -> BigQuery batch ingestion pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mysql", "gcs", "bigquery", "production"],
) as dag:
    via_gcs = BashOperator(
        task_id="mysql_python_gcs_bigquery",
        bash_command="python -m pipeline.run_pipeline --mode gcs",
    )
