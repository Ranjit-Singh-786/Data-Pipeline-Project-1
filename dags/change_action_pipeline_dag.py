from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.run_api_pipeline import (
    task_extract_to_bronze_gcs,
    task_transform_bronze_to_silver,
    task_load_silver_to_bigquery,
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
    description="Production Change Action Medallion Architecture: Bronze (Raw JSON) -> Silver (Parquet) -> Gold (BigQuery WRITE_TRUNCATE)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["api", "medallion", "bronze", "silver", "gold", "gcs", "bigquery", "production"],
) as dag:

    # Task 1 (Bronze): Extract raw API/fallback data to GCS bronze/ddmmyyyy/rawfile.json
    extract_to_bronze = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=task_extract_to_bronze_gcs,
    )

    # Task 2 (Silver): Read from GCS, validate & transform, save to GCS processed/ddmmyyyy/processed_data.parquet
    transform_bronze_to_silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=task_transform_bronze_to_silver,
    )

    # Task 3 (Gold): Load Parquet from GCS Silver layer into BigQuery with WRITE_TRUNCATE
    load_silver_to_bigquery = PythonOperator(
        task_id="load_silver_to_bigquery",
        python_callable=task_load_silver_to_bigquery,
    )

    # Pipeline sequence
    extract_to_bronze >> transform_bronze_to_silver >> load_silver_to_bigquery
