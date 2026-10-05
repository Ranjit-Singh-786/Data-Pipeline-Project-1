# Insurance batch pipeline POC

Two Airflow-triggerable paths are included:

`MySQL -> Python (unbuffered batches, validation, cleaning, PyArrow Parquet) -> BigQuery`

`MySQL -> Python -> Parquet -> GCS -> BigQuery`

## Prerequisites

- Python 3.12 recommended
- Docker Desktop with the Linux engine running
- A Google Cloud project for BigQuery and GCS
- A GCP service-account JSON key for local testing, or an Airflow secret in production

Airflow is not officially supported directly on native Windows. Use WSL2, Docker, or a Linux Airflow environment for scheduling. DAG import tests can still be run locally on Windows.

## Local setup

1. Install packages: `python -m pip install -r requirements.txt`.
2. Copy `.env.example` to `.env` and set the GCP values.
3. Start MySQL: `docker compose -f docker-compose.mysql.yml up -d`.
4. Verify MySQL: `docker exec insurance-mysql mysqladmin ping -uinsurance_user -pinsurance_password`.
5. Load the CSV: `python scripts/load_csv_to_mysql.py`.
6. Verify rows: `docker exec insurance-mysql mysql -uinsurance_user -pinsurance_password insurance -e "SELECT COUNT(*) FROM insurance;"`.
7. Run either path: `python -m pipeline.run_pipeline --mode direct` or `python -m pipeline.run_pipeline --mode gcs`.

Both BigQuery paths use `WRITE_APPEND`. Existing BigQuery rows are never overwritten or deleted. During POC reruns, duplicate rows are possible and can be manually removed if needed.

The included CSV should load `1338` rows. The loader appends rows, so do not run it repeatedly unless duplicate records are intentional.

## Environment file

Copy `.env.example` to `.env` and configure these values:

```dotenv
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DATABASE=insurance
MYSQL_USER=insurance_user
MYSQL_PASSWORD=insurance_password
MYSQL_SOURCE_TABLE=insurance
GCP_PROJECT_ID=your-gcp-project-id
GCP_LOCATION=US
GCP_DATASET=insurance_poc
GCP_TABLE=insurance_clean
GCS_BUCKET=your-existing-bucket-name
GCS_PREFIX=insurance-poc
GOOGLE_APPLICATION_CREDENTIALS=C:\\path\\to\\service-account.json
BATCH_SIZE=250
ARTIFACT_DIR=artifacts
LOG_DIR=logs
```

Never commit `.env` or the service-account JSON key.

## Automated tests

Run transformation tests:

```powershell
python -m pytest -q tests/test_transform.py
```

This project disables pytest's cache provider in `pytest.ini`, so `.pytest_cache` and `pytest-cache-files-*` folders are not created in the working directory.

Run the full suite. On Windows, use a writable Airflow home:

```powershell
New-Item -ItemType Directory -Force -Path .airflow | Out-Null
$env:AIRFLOW_HOME=(Resolve-Path .airflow).Path
python -m pytest -q tests
```

The tests cover cleaning, invalid-row rejection, column validation, Parquet schema, and Airflow DAG tasks.

Every batch and major action is written as JSON lines to `logs/pipeline.jsonl`; rejected rows are counted and never silently loaded.

For Airflow, place this project (or mount it) in the Airflow `dags` folder, install the same requirements in the scheduler/worker image, and create the `.env` values as Airflow environment variables or a secrets backend. Trigger `insurance_batch_pipelines`; its two tasks can be run independently.

## Airflow execution

For Linux, WSL2, or Docker-based Airflow:

1. Install `requirements.txt` in the scheduler and worker environment.
2. Mount or copy `dags/` into the Airflow DAGs directory.
3. Make the project root available on the worker `PYTHONPATH` so `pipeline/` can be imported.
4. Provide `.env` values as Airflow environment variables or through a Secret Backend.
5. Start Airflow and trigger `insurance_batch_pipelines` in the UI.

The DAG has two independent tasks: `mysql_python_bigquery` and `mysql_python_gcs_bigquery`.

## GCP credentials

For the POC, create a service account with:

- BigQuery Job User at project level
- BigQuery Data Editor on the target dataset
- Storage Object Creator on the target bucket

Create the BigQuery dataset and GCS bucket before running Pipeline 2. Set `GOOGLE_APPLICATION_CREDENTIALS` to the local JSON key only for local testing. Production should use Airflow Secret Backend, an Airflow Google connection, or Workload Identity instead of committing a key.

## Logs and artifacts

Every batch and major action is written as JSON lines to `logs/pipeline.jsonl`. Generated Parquet files are stored below `artifacts/<run_id>/`. Logs include batches, rows read, rows rejected, Parquet paths, GCS URIs, BigQuery loads, and final counts.

## Troubleshooting

If Docker access is denied, check `docker context show`, make sure Docker Desktop's Linux engine is running, and retry from an elevated terminal. If MySQL exits, inspect `docker logs insurance-mysql` and run `docker compose -f docker-compose.mysql.yml up -d` again. If Airflow cannot create its default Windows log directory, set `AIRFLOW_HOME` to the writable `.airflow` folder as shown above. GCP authentication errors indicate a missing project ID, credentials path, permission, dataset, or bucket configuration.
