from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.run_pipeline import (
    task_extract_and_transform,
    task_upload_to_gcs,
    task_load_to_bigquery,
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
    "insurance_batch_pipeline_gcs",
    default_args=default_args,
    description="Production MySQL -> GCS -> BigQuery batch ingestion pipeline (Modular Graph)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mysql", "gcs", "bigquery", "production"],
) as dag:

    # ----------------------------------------------------
    # Task 1: Extract from MySQL & Clean Parquet Generation
    # ----------------------------------------------------
    extract_and_transform = PythonOperator(
        task_id="extract_and_transform",
        python_callable=task_extract_and_transform,
    )

    # ----------------------------------------------------
    # Task 2: Upload Parquet Files to Google Cloud Storage
    # ----------------------------------------------------
    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=task_upload_to_gcs,
    )

    # ----------------------------------------------------
    # Task 3: Load from GCS into Google BigQuery Table
    # ----------------------------------------------------
    load_to_bigquery = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=task_load_to_bigquery,
    )

    # Define Execution Pipeline Graph
    extract_and_transform >> upload_to_gcs >> load_to_bigquery
