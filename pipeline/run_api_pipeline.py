import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any, Union
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from pipeline.config import Settings
from pipeline.logging_utils import get_logger, log_step

# PyArrow Schema for Change Action records in BigQuery
CHANGE_ACTION_SCHEMA = pa.schema([
    ("source", pa.string()),
    ("type", pa.string()),
    ("identifier", pa.string()),
    ("relative_path", pa.string()),
    ("state", pa.string()),
])

# Apache Iceberg Schema corresponding to Change Action records
ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="source", field_type=StringType(), required=False),
    NestedField(field_id=2, name="type", field_type=StringType(), required=False),
    NestedField(field_id=3, name="identifier", field_type=StringType(), required=False),
    NestedField(field_id=4, name="relative_path", field_type=StringType(), required=False),
    NestedField(field_id=5, name="state", field_type=StringType(), required=False),
)

def write_parquet(records: List[dict], path: Path) -> None:
    table = pa.Table.from_pylist(records, schema=CHANGE_ACTION_SCHEMA)
    pq.write_table(table, path, compression="snappy")

def save_rejected_records(rejected_records: list, output_path: Path) -> None:
    if not rejected_records:
        return
    with open(output_path, "w", encoding="utf-8") as f:
        for rec in rejected_records:
            f.write(json.dumps(rec, default=str) + "\n")

def parse_gcs_uri(uri: str) -> Tuple[str, str]:
    """Extracts (bucket, blob_name) from gs://bucket/path/to/blob."""
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")
    parts = uri[5:].split("/", 1)
    bucket = parts[0]
    blob_name = parts[1] if len(parts) > 1 else ""
    return bucket, blob_name

def fetch_raw_api_payload(settings: Settings, logger) -> Any:
    """Fetches raw Change Actions payload from live API or falls back to sample.json."""
    if settings.change_action_api_url:
        import urllib.request
        log_step(logger, "api_fetch_start", url=settings.change_action_api_url)
        try:
            req = urllib.request.Request(settings.change_action_api_url, headers={"User-Agent": "DataPipeline/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            log_step(logger, "api_fetch_error", error=str(e), url=settings.change_action_api_url)
            raise RuntimeError(f"Failed to fetch from API: {e}") from e
    else:
        sample_path = Path(settings.sample_json_path)
        log_step(logger, "api_fallback_to_sample_json", path=str(sample_path))
        if not sample_path.exists():
            raise FileNotFoundError(f"Neither API URL nor sample JSON file found at: {sample_path}")
        with open(sample_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    return data

def fetch_change_actions(settings: Settings, logger) -> List[Dict[str, Any]]:
    """Fetches Change Actions list from live API or falls back to sample.json."""
    data = fetch_raw_api_payload(settings, logger)
    if isinstance(data, dict):
        items = data.get("changeAction") or data.get("items") or data.get("data") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []

    log_step(logger, "api_fetch_complete", total_items=len(items))
    return items

def validate_and_transform_change_actions(raw_items: List[Dict[str, Any]]) -> Tuple[List[dict], List[dict]]:
    cleaned = []
    rejected = []
    for idx, item in enumerate(raw_items, 1):
        try:
            if not isinstance(item, dict):
                raise ValueError("Item must be a JSON object")

            source = str(item.get("source", "UNKNOWN")).strip()
            item_type = str(item.get("type", "Change Action")).strip()
            identifier = str(item.get("identifier", "")).strip()
            relative_path = str(item.get("relativePath") or item.get("relative_path") or "").strip()
            state = str(item.get("state", "")).strip()

            if not identifier:
                raise ValueError("Missing required field 'identifier'")
            if not state:
                raise ValueError("Missing required field 'state'")

            record = {
                "source": source,
                "type": item_type,
                "identifier": identifier,
                "relative_path": relative_path,
                "state": state,
            }
            cleaned.append(record)
        except Exception as exc:
            rejected.append({"row": idx, "reason": str(exc), "raw": item})

    return cleaned, rejected

def sanitize_iceberg_metadata_for_bigquery(metadata_uri: str, settings: Settings, logger) -> None:
    """Sanitizes metadata JSON by removing null integer fields (e.g. parent-snapshot-id) that BigQuery parser rejects."""
    if not metadata_uri.startswith("gs://"):
        return
    try:
        bucket_name, blob_name = parse_gcs_uri(metadata_uri)
        storage_client = storage.Client(project=settings.project_id)
        blob = storage_client.bucket(bucket_name).blob(blob_name)
        if not blob.exists():
            return
        data = json.loads(blob.download_as_text())
        changed = False
        for snap in data.get("snapshots", []):
            for field in ("parent-snapshot-id", "first-row-id", "added-rows"):
                if snap.get(field) is None and field in snap:
                    del snap[field]
                    changed = True
        if changed:
            blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
            log_step(logger, "iceberg_metadata_sanitized", uri=metadata_uri)
    except Exception as exc:
        log_step(logger, "iceberg_metadata_sanitize_warning", error=str(exc))

def append_to_iceberg_table(records: List[dict], settings: Settings, logger, dry_run: bool = False) -> Optional[str]:
    """Appends records to Apache Iceberg table on GCS and returns latest metadata URI."""
    if not records:
        return None
    warehouse_uri = settings.iceberg_warehouse or f"gs://{settings.bucket}/iceberg_warehouse"
    full_table_name = f"{settings.dataset}.{settings.iceberg_table}"

    if dry_run:
        fake_meta_uri = f"{warehouse_uri}/{settings.dataset}/{settings.iceberg_table}/metadata/00001-simulated.metadata.json"
        log_step(logger, "iceberg_append_simulated", table=full_table_name, uri=fake_meta_uri, rows=len(records))
        return fake_meta_uri

    try:
        os.environ["PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID"] = "True"
        catalog_dir = Path(settings.artifact_dir)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        catalog_uri = settings.iceberg_catalog_uri or f"sqlite:///{catalog_dir.resolve()}/iceberg_catalog.db"

        catalog = load_catalog("gcs_iceberg", **{
            "type": "sql",
            "uri": catalog_uri,
            "warehouse": warehouse_uri,
        })

        catalog.create_namespace_if_not_exists(settings.dataset)
        table = catalog.create_table_if_not_exists(full_table_name, schema=ICEBERG_SCHEMA)
        pa_table = pa.Table.from_pylist(records)
        table.append(pa_table)
        metadata_location = table.metadata_location
        sanitize_iceberg_metadata_for_bigquery(metadata_location, settings, logger)
        log_step(logger, "iceberg_append_complete", table=full_table_name, metadata=metadata_location, rows=len(records))
        return metadata_location
    except Exception as e:
        log_step(logger, "iceberg_append_failed", table=full_table_name, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 1 (Bronze Layer): Fetch API Raw Data & Upload to GCS bronze/ddmmyyyy/rawfile.json
# ---------------------------------------------------------------------------
def task_extract_to_bronze_gcs(run_id: Optional[str] = None, dry_run: bool = False, date_str: Optional[str] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    if not run_id:
        ti = kwargs.get("ti")
        if ti and hasattr(ti, "execution_date") and ti.execution_date:
            run_id = ti.execution_date.strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    date_str = date_str or datetime.now(timezone.utc).strftime("%d%m%Y")
    log_step(logger, "bronze_extract_start", run_id=run_id, date_str=date_str, dry_run=dry_run)

    artifact_dir = Path(settings.artifact_dir) / f"api_change_action_{run_id}"
    bronze_dir = artifact_dir / "bronze" / date_str
    bronze_dir.mkdir(parents=True, exist_ok=True)
    bronze_local_path = bronze_dir / "rawfile.json"

    try:
        raw_payload = fetch_raw_api_payload(settings, logger)
        with open(bronze_local_path, "w", encoding="utf-8") as f:
            json.dump(raw_payload, f, indent=2)

        gcs_blob_name = f"bronze/{date_str}/rawfile.json"
        if dry_run:
            bucket_name = settings.bucket or "mock-bucket"
            bronze_uri = f"gs://{bucket_name}/{gcs_blob_name}"
            log_step(logger, "bronze_gcs_upload_simulated", path=str(bronze_local_path), uri=bronze_uri)
        else:
            settings.validate_gcp_config()
            storage_client = storage.Client(project=settings.project_id)
            bucket = storage_client.bucket(settings.bucket)
            blob = bucket.blob(gcs_blob_name)
            blob.upload_from_filename(str(bronze_local_path))
            bronze_uri = f"gs://{settings.bucket}/{gcs_blob_name}"
            log_step(logger, "bronze_gcs_upload", path=str(bronze_local_path), uri=bronze_uri)

        result_payload = {
            "run_id": run_id,
            "bronze_uri": bronze_uri,
            "date_str": date_str,
            "artifact_dir": str(artifact_dir),
            "raw_file": str(bronze_local_path),
            "dry_run": dry_run,
        }

        manifest_path = artifact_dir / "manifest_bronze.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result_payload, f, indent=2)

        log_step(logger, "bronze_extract_complete", **result_payload)
        return result_payload

    except Exception as e:
        log_step(logger, "bronze_extract_failed", run_id=run_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 2 (Silver Layer): Read from GCS, Validate/Transform, Upload Parquet & Write Iceberg
# ---------------------------------------------------------------------------
def task_transform_bronze_to_silver(run_id: Optional[str] = None, dry_run: bool = False, bronze_uri: Optional[str] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    ti = kwargs.get("ti")
    date_str = None
    if ti and not bronze_uri:
        upstream_data = ti.xcom_pull(task_ids="extract_to_bronze") or ti.xcom_pull(task_ids="task_extract_to_bronze_gcs") or ti.xcom_pull(task_ids="fetch_api_and_transform")
        if upstream_data:
            bronze_uri = upstream_data.get("bronze_uri", bronze_uri)
            run_id = upstream_data.get("run_id", run_id)
            date_str = upstream_data.get("date_str")
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        ti_obj = kwargs.get("ti")
        if ti_obj and hasattr(ti_obj, "execution_date") and ti_obj.execution_date:
            run_id = ti_obj.execution_date.strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if not date_str:
        if bronze_uri and "bronze/" in bronze_uri:
            parts = bronze_uri.split("bronze/")[1].split("/")
            date_str = parts[0]
        else:
            date_str = datetime.now(timezone.utc).strftime("%d%m%Y")

    artifact_dir = Path(settings.artifact_dir) / f"api_change_action_{run_id}"
    silver_dir = artifact_dir / "silver" / date_str
    silver_dir.mkdir(parents=True, exist_ok=True)

    log_step(logger, "silver_transform_start", run_id=run_id, bronze_uri=bronze_uri, dry_run=dry_run)

    try:
        # Read raw data from GCS using GCS URI / object path
        if dry_run or not bronze_uri:
            bronze_local_path = artifact_dir / "bronze" / date_str / "rawfile.json"
            if bronze_local_path.exists():
                with open(bronze_local_path, "r", encoding="utf-8") as f:
                    raw_data = json.load(f)
            else:
                raw_data = fetch_raw_api_payload(settings, logger)
        else:
            bucket_name, blob_name = parse_gcs_uri(bronze_uri)
            storage_client = storage.Client(project=settings.project_id)
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            raw_text = blob.download_as_text()
            raw_data = json.loads(raw_text)

        # Extract items
        if isinstance(raw_data, dict):
            raw_items = raw_data.get("changeAction") or raw_data.get("items") or raw_data.get("data") or []
        elif isinstance(raw_data, list):
            raw_items = raw_data
        else:
            raw_items = []

        clean_records, bad_records = validate_and_transform_change_actions(raw_items)

        if bad_records:
            quarantine_path = silver_dir / "rejected.jsonl"
            save_rejected_records(bad_records, quarantine_path)
            log_step(logger, "api_dead_letter_saved", path=str(quarantine_path), count=len(bad_records))

        silver_parquet_path = silver_dir / "processed_data.parquet"
        write_parquet(clean_records, silver_parquet_path)
        log_step(logger, "silver_transformed_to_parquet", rows=len(clean_records), path=str(silver_parquet_path))

        # Save to GCS: processed/ddmmyyyy/processed_data.parquet
        silver_blob_name = f"processed/{date_str}/processed_data.parquet"
        if dry_run:
            bucket_name = settings.bucket or "mock-bucket"
            silver_uri = f"gs://{bucket_name}/{silver_blob_name}"
            log_step(logger, "silver_gcs_upload_simulated", path=str(silver_parquet_path), uri=silver_uri)
        else:
            settings.validate_gcp_config()
            storage_client = storage.Client(project=settings.project_id)
            bucket = storage_client.bucket(settings.bucket)
            blob = bucket.blob(silver_blob_name)
            blob.upload_from_filename(str(silver_parquet_path))
            silver_uri = f"gs://{settings.bucket}/{silver_blob_name}"
            log_step(logger, "silver_gcs_upload", path=str(silver_parquet_path), uri=silver_uri)

        # Apache Iceberg Append
        iceberg_metadata_uri = None
        if settings.iceberg_enabled and clean_records:
            iceberg_metadata_uri = append_to_iceberg_table(clean_records, settings, logger, dry_run=dry_run)

        result_payload = {
            "run_id": run_id,
            "silver_uri": silver_uri,
            "iceberg_metadata_uri": iceberg_metadata_uri,
            "files": [str(silver_parquet_path)],
            "clean_rows": len(clean_records),
            "rejected_rows": len(bad_records),
            "dry_run": dry_run,
        }

        manifest_path = artifact_dir / "manifest_silver.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result_payload, f, indent=2)

        log_step(logger, "silver_transform_complete", **result_payload)
        return result_payload

    except Exception as e:
        log_step(logger, "silver_transform_failed", run_id=run_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 3A (Gold Layer): Load from GCS (Silver Parquet) into BigQuery (WRITE_TRUNCATE)
# ---------------------------------------------------------------------------
def task_load_silver_to_bigquery(run_id: Optional[str] = None, dry_run: bool = False, silver_uri: Optional[str] = None, uris: Optional[List[str]] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    ti = kwargs.get("ti")
    if ti and (not run_id or (not silver_uri and not uris)):
        upstream_data = ti.xcom_pull(task_ids="transform_bronze_to_silver") or ti.xcom_pull(task_ids="task_transform_bronze_to_silver") or ti.xcom_pull(task_ids="upload_api_to_gcs")
        if upstream_data:
            run_id = upstream_data.get("run_id", run_id)
            silver_uri = upstream_data.get("silver_uri", silver_uri)
            if not silver_uri and upstream_data.get("uris"):
                silver_uri = upstream_data["uris"][0]
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        ti_obj = kwargs.get("ti")
        if ti_obj and hasattr(ti_obj, "execution_date") and ti_obj.execution_date:
            run_id = ti_obj.execution_date.strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    target_uris = []
    if silver_uri:
        target_uris.append(silver_uri)
    elif uris:
        target_uris.extend(uris)

    if not target_uris:
        log_step(logger, "api_step_load_bigquery_skipped", run_id=run_id, message="No Silver URIs to load into BigQuery.")
        return {"run_id": run_id, "status": "SKIPPED", "loaded_uris": 0}

    table_id = f"{settings.project_id}.{settings.dataset}.{settings.api_table}" if settings.project_id else f"{settings.dataset}.{settings.api_table}"
    log_step(logger, "api_step_load_bigquery_start", run_id=run_id, table=table_id, uris=target_uris, dry_run=dry_run)

    try:
        if dry_run:
            log_step(logger, "api_bigquery_load_dry_run", run_id=run_id, uris=target_uris, table=table_id, write_disposition="WRITE_TRUNCATE")
            log_step(logger, "api_step_load_bigquery_complete", run_id=run_id, table=table_id, status="SUCCESS_DRY_RUN")
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS_DRY_RUN", "loaded_uris": len(target_uris)}
        else:
            bq_client = bigquery.Client(project=settings.project_id)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            load_job = bq_client.load_table_from_uri(target_uris, table_id, job_config=job_config)
            load_job.result()
            log_step(logger, "api_step_load_bigquery_complete", run_id=run_id, table=table_id, job_id=load_job.job_id)
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS", "job_id": load_job.job_id, "loaded_uris": len(target_uris)}

    except Exception as e:
        log_step(logger, "api_step_load_bigquery_failed", run_id=run_id, table=table_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 3B (Gold Layer): Sync Iceberg Metadata to BigQuery BigLake External Table
# ---------------------------------------------------------------------------
def task_sync_iceberg_to_bigquery(run_id: Optional[str] = None, dry_run: bool = False, iceberg_metadata_uri: Optional[str] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    ti = kwargs.get("ti")
    if ti and not iceberg_metadata_uri:
        upstream_data = ti.xcom_pull(task_ids="transform_bronze_to_silver") or ti.xcom_pull(task_ids="task_transform_bronze_to_silver")
        if upstream_data:
            iceberg_metadata_uri = upstream_data.get("iceberg_metadata_uri", iceberg_metadata_uri)
            run_id = upstream_data.get("run_id", run_id)
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        ti_obj = kwargs.get("ti")
        if ti_obj and hasattr(ti_obj, "execution_date") and ti_obj.execution_date:
            run_id = ti_obj.execution_date.strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    table_id = f"{settings.project_id}.{settings.dataset}.{settings.iceberg_table}" if settings.project_id else f"{settings.dataset}.{settings.iceberg_table}"

    if not iceberg_metadata_uri:
        log_step(logger, "iceberg_sync_bigquery_skipped", run_id=run_id, message="No Iceberg metadata URI to sync.")
        return {"run_id": run_id, "status": "SKIPPED", "table": table_id}

    log_step(logger, "iceberg_sync_bigquery_start", run_id=run_id, table=table_id, metadata_uri=iceberg_metadata_uri, dry_run=dry_run)

    try:
        if dry_run:
            log_step(logger, "iceberg_sync_bigquery_simulated", run_id=run_id, table=table_id, metadata_uri=iceberg_metadata_uri)
            log_step(logger, "iceberg_sync_bigquery_complete", run_id=run_id, table=table_id, status="SUCCESS_DRY_RUN")
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS_DRY_RUN", "metadata_uri": iceberg_metadata_uri}
        else:
            bq_client = bigquery.Client(project=settings.project_id)
            bq_client.delete_table(table_id, not_found_ok=True)
            sql = f"""
            CREATE EXTERNAL TABLE `{table_id}`
            WITH CONNECTION `{settings.biglake_connection}`
            OPTIONS (
              format = 'ICEBERG',
              uris = ['{iceberg_metadata_uri}']
            )
            """
            bq_client.query(sql).result()
            log_step(logger, "iceberg_sync_bigquery_complete", run_id=run_id, table=table_id, metadata_uri=iceberg_metadata_uri)
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS", "metadata_uri": iceberg_metadata_uri}

    except Exception as e:
        log_step(logger, "iceberg_sync_bigquery_failed", run_id=run_id, table=table_id, error=str(e))
        raise

# Backward compatibility aliases
task_fetch_api_and_transform = task_extract_to_bronze_gcs
task_upload_api_to_gcs = task_transform_bronze_to_silver
task_load_api_to_bigquery = task_load_silver_to_bigquery

# ---------------------------------------------------------------------------
# CLI / Standalone Runner
# ---------------------------------------------------------------------------
def run(dry_run: bool = False, run_id: Optional[str] = None) -> None:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    log_step(logger, "api_pipeline_start", run_id=run_id, dry_run=dry_run)
    try:
        step1 = task_extract_to_bronze_gcs(run_id=run_id, dry_run=dry_run)
        step2 = task_transform_bronze_to_silver(run_id=run_id, dry_run=dry_run, bronze_uri=step1["bronze_uri"])
        step3 = task_load_silver_to_bigquery(run_id=run_id, dry_run=dry_run, silver_uri=step2["silver_uri"])

        if settings.iceberg_enabled and step2.get("iceberg_metadata_uri"):
            task_sync_iceberg_to_bigquery(run_id=run_id, dry_run=dry_run, iceberg_metadata_uri=step2["iceberg_metadata_uri"])

        log_step(logger, "api_pipeline_complete", run_id=run_id, clean_rows=step2["clean_rows"], rejected_rows=step2["rejected_rows"])
    except Exception as e:
        log_step(logger, "api_pipeline_failed", run_id=run_id, error=str(e))
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Change Action API data ingestion pipeline (Medallion Architecture + Iceberg)")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending data to GCP")
    parser.add_argument("--step", choices=["all", "bronze", "silver", "gold", "iceberg", "extract", "upload", "load"], default="all", help="Execute specific pipeline step")
    parser.add_argument("--run-id", type=str, default=None, help="Explicit run_id for step resumption")
    parser.add_argument("--bronze-uri", type=str, default=None, help="GCS URI for Bronze raw file")
    parser.add_argument("--silver-uri", type=str, default=None, help="GCS URI for Silver parquet file")
    parser.add_argument("--iceberg-meta-uri", type=str, default=None, help="GCS URI for Iceberg metadata file")
    args = parser.parse_args()

    if args.step == "all":
        run(dry_run=args.dry_run, run_id=args.run_id)
    elif args.step in ("bronze", "extract"):
        task_extract_to_bronze_gcs(run_id=args.run_id, dry_run=args.dry_run)
    elif args.step in ("silver", "upload"):
        task_transform_bronze_to_silver(run_id=args.run_id, dry_run=args.dry_run, bronze_uri=args.bronze_uri)
    elif args.step in ("gold", "load"):
        task_load_silver_to_bigquery(run_id=args.run_id, dry_run=args.dry_run, silver_uri=args.silver_uri)
    elif args.step == "iceberg":
        task_sync_iceberg_to_bigquery(run_id=args.run_id, dry_run=args.dry_run, iceberg_metadata_uri=args.iceberg_meta_uri)
