import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Tuple
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

def run(mode: str = "gcs", dry_run: bool = False) -> None:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("insurance_pipeline", log_file=settings.log_file)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    log_step(logger, "pipeline_start", mode=mode, run_id=run_id, dry_run=dry_run)
    
    if not dry_run:
        settings.validate_gcp_config()

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

        # Quarantine dead-letter records if any
        if all_rejected:
            quarantine_path = artifact_dir / "rejected.jsonl"
            save_rejected_records(all_rejected, quarantine_path)
            log_step(logger, "dead_letter_quarantine_saved", path=str(quarantine_path), count=len(all_rejected))

        # Edge case: No data found in MySQL source table
        if not files:
            log_step(logger, "zero_records_found", message="No valid rows found to upload. Exiting gracefully.")
            log_step(logger, "pipeline_complete", mode=mode, loaded=0, rejected=len(all_rejected), batches=0)
            return

        table_id = f"{settings.project_id}.{settings.dataset}.{settings.table}" if settings.project_id else f"{settings.dataset}.{settings.table}"

        if mode == "gcs":
            uris = []
            if dry_run:
                log_step(logger, "gcs_upload_dry_run", bucket=settings.bucket or "mock-bucket", files_count=len(files))
                for path in files:
                    fake_uri = f"gs://{settings.bucket or 'mock-bucket'}/{settings.gcs_prefix}/{run_id}/{path.name}"
                    uris.append(fake_uri)
                    log_step(logger, "gcs_upload_simulated", path=str(path), uri=fake_uri)
                log_step(logger, "bigquery_load_dry_run", uris=len(uris), table=table_id)
            else:
                log_step(logger, "gcs_upload_start", bucket=settings.bucket, files_count=len(files))
                storage_client = storage.Client(project=settings.project_id)
                bq_client = bigquery.Client(project=settings.project_id)
                bucket = storage_client.bucket(settings.bucket)
                
                for path in files:
                    blob = bucket.blob(f"{settings.gcs_prefix}/{run_id}/{path.name}")
                    blob.upload_from_filename(path)
                    uri = f"gs://{settings.bucket}/{blob.name}"
                    uris.append(uri)
                    log_step(logger, "gcs_upload", path=str(path), uri=uri)
                log_step(logger, "gcs_upload_complete", uris_count=len(uris))

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                )
                log_step(logger, "bigquery_load_from_gcs_start", uris_count=len(uris), table=table_id)
                load_job = bq_client.load_table_from_uri(uris, table_id, job_config=job_config)
                load_job.result()
                log_step(logger, "bigquery_load_from_gcs_complete", uris=len(uris), table=table_id, job_id=load_job.job_id)

        elif mode == "direct":
            if dry_run:
                log_step(logger, "bigquery_direct_load_dry_run", files_count=len(files), table=table_id)
            else:
                log_step(logger, "bigquery_load_start", mode="direct", files_count=len(files))
                client = bigquery.Client(project=settings.project_id)
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                )
                for path in files:
                    with path.open("rb") as f:
                        load_job = client.load_table_from_file(f, table_id, job_config=job_config)
                        load_job.result()
                    log_step(logger, "bigquery_load", path=str(path), table=table_id, job_id=load_job.job_id)
                log_step(logger, "bigquery_load_complete", mode="direct", files_loaded=len(files), table=table_id)

        log_step(logger, "pipeline_complete", mode=mode, loaded=total_clean, rejected=len(all_rejected), batches=len(files))

    except (GoogleAPIError, RuntimeError, Exception) as e:
        log_step(logger, "pipeline_failed", mode=mode, error=str(e))
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Production insurance data ingestion pipeline")
    parser.add_argument("--mode", choices=["gcs", "direct"], default="gcs", help="Pipeline ingestion mode (default: gcs)")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending data to GCP (useful when credentials are not configured)")
    args = parser.parse_args()
    run(mode=args.mode, dry_run=args.dry_run)

