from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.run_api_pipeline import (
    task_fetch_api_and_transform,
    task_upload_api_to_gcs,
    task_load_api_to_bigquery,
)

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
    "change_action_api_pipeline_gcs",
    default_args=default_args,
    description="Production Change Action API -> GCS -> BigQuery batch ingestion pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["api", "3dspace", "gcs", "bigquery", "production"],
) as dag:

    # Task 1: Fetch from API / sample.json & Transform to Parquet
    fetch_api_and_transform = PythonOperator(
        task_id="fetch_api_and_transform",
        python_callable=task_fetch_api_and_transform,
    )

    # Task 2: Upload Parquet Files to GCS
    upload_api_to_gcs = PythonOperator(
        task_id="upload_api_to_gcs",
        python_callable=task_upload_api_to_gcs,
    )

    # Task 3: Load Parquet from GCS into BigQuery (table: change_actions_clean)
    load_api_to_bigquery = PythonOperator(
        task_id="load_api_to_bigquery",
        python_callable=task_load_api_to_bigquery,
    )

    # Pipeline sequence
    fetch_api_and_transform >> upload_api_to_gcs >> load_api_to_bigquery
