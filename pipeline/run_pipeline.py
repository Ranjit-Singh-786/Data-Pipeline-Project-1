import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Tuple
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage
from google.api_core.exceptions import GoogleAPIError
from pipeline.config import Settings
from pipeline.logging_utils import get_logger, log_step
from pipeline.transform import validate_and_transform

SCHEMA = pa.schema([
    ("age", pa.int64()), ("sex", pa.string()), ("bmi", pa.float64()),
    ("children", pa.int64()), ("smoker", pa.string()), ("region", pa.string()),
    ("charges", pa.float64()),
])

def write_parquet(records: List[dict], path: Path) -> None:
    table = pa.Table.from_pylist(records, schema=SCHEMA)
    pq.write_table(table, path, compression="snappy")

def extract(settings: Settings, logger) -> Generator[Tuple[List[str], list], None, None]:
    log_step(logger, "mysql_extract_start", host=settings.mysql_host, port=settings.mysql_port, database=settings.mysql_database, table=settings.source_table)
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host=settings.mysql_host,
            port=settings.mysql_port,
            database=settings.mysql_database,
            user=settings.mysql_user,
            password=settings.mysql_password,
            connection_timeout=10,
        )
        cursor = connection.cursor(buffered=False)
        cursor.execute(f"SELECT age, sex, bmi, children, smoker, region, charges FROM `{settings.source_table}`")
        columns = [item[0] for item in cursor.description]
        total_extracted = 0
        while True:
            rows = cursor.fetchmany(settings.batch_size)
            if not rows:
                break
            total_extracted += len(rows)
            yield columns, rows
        log_step(logger, "mysql_extract_complete", total_rows=total_extracted)
    except mysql.connector.Error as err:
        log_step(logger, "mysql_extract_error", error=str(err))
        raise RuntimeError(f"MySQL extraction failed: {err}") from err
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

def save_rejected_records(rejected_records: list, output_path: Path) -> None:
    """Quarantine dead-letter records to a JSONL file for auditing."""
    if not rejected_records:
        return
    with open(output_path, "w", encoding="utf-8") as f:
        for rec in rejected_records:
            f.write(json.dumps(rec, default=str) + "\n")

# ---------------------------------------------------------------------------
# STEP 1: Extract & Transform to Parquet
# ---------------------------------------------------------------------------
def task_extract_and_transform(run_id: Optional[str] = None, dry_run: bool = False, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("insurance_pipeline", log_file=settings.log_file)
    
    if not run_id:
        # Check Airflow task instance execution context or generate timestamp
        ti = kwargs.get("ti")
        if ti and hasattr(ti, "execution_date") and ti.execution_date:
            run_id = ti.execution_date.strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    log_step(logger, "step_extract_transform_start", run_id=run_id, dry_run=dry_run)

    artifact_dir = Path(settings.artifact_dir) / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    log_step(logger, "artifact_directory_created", path=str(artifact_dir))

    files: List[Path] = []
    all_rejected: list = []
    total_clean = 0

    try:
        for batch_no, (columns, rows) in enumerate(extract(settings, logger), 1):
            log_step(logger, "batch_read", batch=batch_no, rows=len(rows))
            clean, bad = validate_and_transform(rows, columns)
            total_clean += len(clean)
            
            if bad:
                all_rejected.extend(bad)
                log_step(logger, "rows_rejected", batch=batch_no, count=len(bad))
            
            if clean:
                part_path = artifact_dir / f"part-{batch_no:05d}.parquet"
                write_parquet(clean, part_path)
                files.append(part_path)
                log_step(logger, "batch_transformed_to_parquet", batch=batch_no, rows=len(clean), path=str(part_path))

        if all_rejected:
            quarantine_path = artifact_dir / "rejected.jsonl"
            save_rejected_records(all_rejected, quarantine_path)
            log_step(logger, "dead_letter_quarantine_saved", path=str(quarantine_path), count=len(all_rejected))

        result_payload = {
            "run_id": run_id,
            "artifact_dir": str(artifact_dir),
            "files": [str(p) for p in files],
            "clean_rows": total_clean,
            "rejected_rows": len(all_rejected),
            "dry_run": dry_run,
        }

        # Save manifest for downstream steps or independent CLI resumption
        manifest_path = artifact_dir / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result_payload, f, indent=2)

        log_step(logger, "step_extract_transform_complete", **result_payload)
        return result_payload

    except Exception as e:
        log_step(logger, "step_extract_transform_failed", run_id=run_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 2: Upload Parquet Files to GCS
# ---------------------------------------------------------------------------
def task_upload_to_gcs(run_id: Optional[str] = None, dry_run: bool = False, files: Optional[List[str]] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("insurance_pipeline", log_file=settings.log_file)

    # Pull from Airflow XCom if available
    ti = kwargs.get("ti")
    if ti and (not run_id or not files):
        upstream_data = ti.xcom_pull(task_ids="extract_and_transform")
        if upstream_data:
            run_id = upstream_data.get("run_id", run_id)
            files = upstream_data.get("files", files)
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        raise ValueError("Missing run_id for task_upload_to_gcs.")

    if not dry_run:
        settings.validate_gcp_config()

    artifact_dir = Path(settings.artifact_dir) / run_id
    if not files and artifact_dir.exists():
        files = [str(p) for p in artifact_dir.glob("part-*.parquet")]

    if not files:
        log_step(logger, "step_upload_gcs_skipped", run_id=run_id, message="No parquet files to upload.")
        return {"run_id": run_id, "uris": [], "files_count": 0, "dry_run": dry_run}

    log_step(logger, "step_upload_gcs_start", run_id=run_id, files_count=len(files), dry_run=dry_run)

    uris: List[str] = []
    try:
        if dry_run:
            bucket_name = settings.bucket or "mock-bucket"
            for file_path in files:
                p = Path(file_path)
                fake_uri = f"gs://{bucket_name}/{settings.gcs_prefix}/{run_id}/{p.name}"
                uris.append(fake_uri)
                log_step(logger, "gcs_upload_simulated", path=str(p), uri=fake_uri)
            log_step(logger, "step_upload_gcs_complete", run_id=run_id, uris_count=len(uris), dry_run=True)
        else:
            storage_client = storage.Client(project=settings.project_id)
            bucket = storage_client.bucket(settings.bucket)
            for file_path in files:
                p = Path(file_path)
                blob = bucket.blob(f"{settings.gcs_prefix}/{run_id}/{p.name}")
                blob.upload_from_filename(str(p))
                uri = f"gs://{settings.bucket}/{blob.name}"
                uris.append(uri)
                log_step(logger, "gcs_upload", path=str(p), uri=uri)
            log_step(logger, "step_upload_gcs_complete", run_id=run_id, uris_count=len(uris), dry_run=False)

        result_payload = {
            "run_id": run_id,
            "uris": uris,
            "files_count": len(files),
            "dry_run": dry_run,
        }
        return result_payload

    except Exception as e:
        log_step(logger, "step_upload_gcs_failed", run_id=run_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 3: Load from GCS into BigQuery
# ---------------------------------------------------------------------------
def task_load_to_bigquery(run_id: Optional[str] = None, dry_run: bool = False, uris: Optional[List[str]] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("insurance_pipeline", log_file=settings.log_file)

    # Pull from Airflow XCom if available
    ti = kwargs.get("ti")
    if ti and (not run_id or not uris):
        upstream_data = ti.xcom_pull(task_ids="upload_to_gcs")
        if upstream_data:
            run_id = upstream_data.get("run_id", run_id)
            uris = upstream_data.get("uris", uris)
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        raise ValueError("Missing run_id for task_load_to_bigquery.")

    if not uris:
        log_step(logger, "step_load_bigquery_skipped", run_id=run_id, message="No URIs to load into BigQuery.")
        return {"run_id": run_id, "status": "SKIPPED", "loaded_uris": 0}

    table_id = f"{settings.project_id}.{settings.dataset}.{settings.table}" if settings.project_id else f"{settings.dataset}.{settings.table}"
    log_step(logger, "step_load_bigquery_start", run_id=run_id, table=table_id, uris_count=len(uris), dry_run=dry_run)

    try:
        if dry_run:
            log_step(logger, "bigquery_load_dry_run", run_id=run_id, uris=len(uris), table=table_id)
            log_step(logger, "step_load_bigquery_complete", run_id=run_id, table=table_id, status="SUCCESS_DRY_RUN")
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS_DRY_RUN", "loaded_uris": len(uris)}
        else:
            bq_client = bigquery.Client(project=settings.project_id)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            load_job = bq_client.load_table_from_uri(uris, table_id, job_config=job_config)
            load_job.result()
            log_step(logger, "step_load_bigquery_complete", run_id=run_id, table=table_id, job_id=load_job.job_id)
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS", "job_id": load_job.job_id, "loaded_uris": len(uris)}

    except Exception as e:
        log_step(logger, "step_load_bigquery_failed", run_id=run_id, table=table_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# CLI / Standalone Runner
# ---------------------------------------------------------------------------
def run(mode: str = "gcs", dry_run: bool = False, run_id: Optional[str] = None) -> None:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("insurance_pipeline", log_file=settings.log_file)
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    log_step(logger, "pipeline_start", mode=mode, run_id=run_id, dry_run=dry_run)
    try:
        step1_res = task_extract_and_transform(run_id=run_id, dry_run=dry_run)
        step2_res = task_upload_to_gcs(run_id=run_id, dry_run=dry_run, files=step1_res["files"])
        step3_res = task_load_to_bigquery(run_id=run_id, dry_run=dry_run, uris=step2_res["uris"])
        log_step(logger, "pipeline_complete", mode=mode, run_id=run_id, clean_rows=step1_res["clean_rows"], rejected_rows=step1_res["rejected_rows"], batches=len(step1_res["files"]))
    except Exception as e:
        log_step(logger, "pipeline_failed", mode=mode, run_id=run_id, error=str(e))
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Production insurance data ingestion pipeline")
    parser.add_argument("--mode", choices=["gcs", "direct"], default="gcs", help="Pipeline ingestion mode (default: gcs)")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending data to GCP")
    parser.add_argument("--step", choices=["all", "extract", "upload", "load"], default="all", help="Execute specific pipeline step")
    parser.add_argument("--run-id", type=str, default=None, help="Explicit run_id for step resumption")
    args = parser.parse_args()

    if args.step == "all":
        run(mode=args.mode, dry_run=args.dry_run, run_id=args.run_id)
    elif args.step == "extract":
        task_extract_and_transform(run_id=args.run_id, dry_run=args.dry_run)
    elif args.step == "upload":
        task_upload_to_gcs(run_id=args.run_id, dry_run=args.dry_run)
    elif args.step == "load":
        task_load_to_bigquery(run_id=args.run_id, dry_run=args.dry_run)
