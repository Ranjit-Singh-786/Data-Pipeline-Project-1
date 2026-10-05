import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage
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

def write_parquet(records: List[dict], path: Path) -> None:
    table = pa.Table.from_pylist(records, schema=CHANGE_ACTION_SCHEMA)
    pq.write_table(table, path, compression="snappy")

def save_rejected_records(rejected_records: list, output_path: Path) -> None:
    if not rejected_records:
        return
    with open(output_path, "w", encoding="utf-8") as f:
        for rec in rejected_records:
            f.write(json.dumps(rec, default=str) + "\n")

def fetch_change_actions(settings: Settings, logger) -> List[Dict[str, Any]]:
    """Fetches Change Actions from live API or falls back to sample.json."""
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

    # Extract list of change actions
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

# ---------------------------------------------------------------------------
# STEP 1: Fetch API & Transform to Parquet
# ---------------------------------------------------------------------------
def task_fetch_api_and_transform(run_id: Optional[str] = None, dry_run: bool = False, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    if not run_id:
        ti = kwargs.get("ti")
        if ti and hasattr(ti, "execution_date") and ti.execution_date:
            run_id = ti.execution_date.strftime("%Y%m%dT%H%M%SZ")
        else:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    log_step(logger, "api_step_extract_transform_start", run_id=run_id, dry_run=dry_run)

    artifact_dir = Path(settings.artifact_dir) / f"api_change_action_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    try:
        raw_items = fetch_change_actions(settings, logger)
        clean_records, bad_records = validate_and_transform_change_actions(raw_items)

        if bad_records:
            quarantine_path = artifact_dir / "rejected.jsonl"
            save_rejected_records(bad_records, quarantine_path)
            log_step(logger, "api_dead_letter_saved", path=str(quarantine_path), count=len(bad_records))

        files = []
        if clean_records:
            # Batch parquet write if large
            part_path = artifact_dir / "part-00001.parquet"
            write_parquet(clean_records, part_path)
            files.append(part_path)
            log_step(logger, "api_batch_transformed_to_parquet", rows=len(clean_records), path=str(part_path))

        result_payload = {
            "run_id": run_id,
            "artifact_dir": str(artifact_dir),
            "files": [str(p) for p in files],
            "clean_rows": len(clean_records),
            "rejected_rows": len(bad_records),
            "dry_run": dry_run,
        }

        manifest_path = artifact_dir / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(result_payload, f, indent=2)

        log_step(logger, "api_step_extract_transform_complete", **result_payload)
        return result_payload

    except Exception as e:
        log_step(logger, "api_step_extract_transform_failed", run_id=run_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 2: Upload API Parquet Files to GCS
# ---------------------------------------------------------------------------
def task_upload_api_to_gcs(run_id: Optional[str] = None, dry_run: bool = False, files: Optional[List[str]] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    ti = kwargs.get("ti")
    if ti and (not run_id or not files):
        upstream_data = ti.xcom_pull(task_ids="fetch_api_and_transform")
        if upstream_data:
            run_id = upstream_data.get("run_id", run_id)
            files = upstream_data.get("files", files)
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        raise ValueError("Missing run_id for task_upload_api_to_gcs.")

    if not dry_run:
        settings.validate_gcp_config()

    artifact_dir = Path(settings.artifact_dir) / f"api_change_action_{run_id}"
    if not files and artifact_dir.exists():
        files = [str(p) for p in artifact_dir.glob("part-*.parquet")]

    if not files:
        log_step(logger, "api_step_upload_gcs_skipped", run_id=run_id, message="No parquet files to upload.")
        return {"run_id": run_id, "uris": [], "files_count": 0, "dry_run": dry_run}

    log_step(logger, "api_step_upload_gcs_start", run_id=run_id, files_count=len(files), dry_run=dry_run)
    uris = []
    try:
        if dry_run:
            bucket_name = settings.bucket or "mock-bucket"
            for file_path in files:
                p = Path(file_path)
                fake_uri = f"gs://{bucket_name}/{settings.api_gcs_prefix}/{run_id}/{p.name}"
                uris.append(fake_uri)
                log_step(logger, "api_gcs_upload_simulated", path=str(p), uri=fake_uri)
        else:
            storage_client = storage.Client(project=settings.project_id)
            bucket = storage_client.bucket(settings.bucket)
            for file_path in files:
                p = Path(file_path)
                blob = bucket.blob(f"{settings.api_gcs_prefix}/{run_id}/{p.name}")
                blob.upload_from_filename(str(p))
                uri = f"gs://{settings.bucket}/{blob.name}"
                uris.append(uri)
                log_step(logger, "api_gcs_upload", path=str(p), uri=uri)

        result_payload = {
            "run_id": run_id,
            "uris": uris,
            "files_count": len(files),
            "dry_run": dry_run,
        }
        log_step(logger, "api_step_upload_gcs_complete", **result_payload)
        return result_payload

    except Exception as e:
        log_step(logger, "api_step_upload_gcs_failed", run_id=run_id, error=str(e))
        raise

# ---------------------------------------------------------------------------
# STEP 3: Load from GCS into BigQuery (Table: change_actions_clean)
# ---------------------------------------------------------------------------
def task_load_api_to_bigquery(run_id: Optional[str] = None, dry_run: bool = False, uris: Optional[List[str]] = None, **kwargs) -> dict:
    settings = Settings()
    dry_run = dry_run or settings.dry_run
    logger = get_logger("change_action_api_pipeline", log_file=settings.log_file)

    ti = kwargs.get("ti")
    if ti and (not run_id or not uris):
        upstream_data = ti.xcom_pull(task_ids="upload_api_to_gcs")
        if upstream_data:
            run_id = upstream_data.get("run_id", run_id)
            uris = upstream_data.get("uris", uris)
            dry_run = upstream_data.get("dry_run", dry_run)

    if not run_id:
        raise ValueError("Missing run_id for task_load_api_to_bigquery.")

    if not uris:
        log_step(logger, "api_step_load_bigquery_skipped", run_id=run_id, message="No URIs to load into BigQuery.")
        return {"run_id": run_id, "status": "SKIPPED", "loaded_uris": 0}

    table_id = f"{settings.project_id}.{settings.dataset}.{settings.api_table}" if settings.project_id else f"{settings.dataset}.{settings.api_table}"
    log_step(logger, "api_step_load_bigquery_start", run_id=run_id, table=table_id, uris_count=len(uris), dry_run=dry_run)

    try:
        if dry_run:
            log_step(logger, "api_bigquery_load_dry_run", run_id=run_id, uris=len(uris), table=table_id)
            log_step(logger, "api_step_load_bigquery_complete", run_id=run_id, table=table_id, status="SUCCESS_DRY_RUN")
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
            log_step(logger, "api_step_load_bigquery_complete", run_id=run_id, table=table_id, job_id=load_job.job_id)
            return {"run_id": run_id, "table": table_id, "status": "SUCCESS", "job_id": load_job.job_id, "loaded_uris": len(uris)}

    except Exception as e:
        log_step(logger, "api_step_load_bigquery_failed", run_id=run_id, table=table_id, error=str(e))
        raise

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
        step1 = task_fetch_api_and_transform(run_id=run_id, dry_run=dry_run)
        step2 = task_upload_api_to_gcs(run_id=run_id, dry_run=dry_run, files=step1["files"])
        step3 = task_load_api_to_bigquery(run_id=run_id, dry_run=dry_run, uris=step2["uris"])
        log_step(logger, "api_pipeline_complete", run_id=run_id, clean_rows=step1["clean_rows"], rejected_rows=step1["rejected_rows"])
    except Exception as e:
        log_step(logger, "api_pipeline_failed", run_id=run_id, error=str(e))
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Change Action API data ingestion pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending data to GCP")
    parser.add_argument("--step", choices=["all", "extract", "upload", "load"], default="all", help="Execute specific pipeline step")
    parser.add_argument("--run-id", type=str, default=None, help="Explicit run_id for step resumption")
    args = parser.parse_args()

    if args.step == "all":
        run(dry_run=args.dry_run, run_id=args.run_id)
    elif args.step == "extract":
        task_fetch_api_and_transform(run_id=args.run_id, dry_run=args.dry_run)
    elif args.step == "upload":
        task_upload_api_to_gcs(run_id=args.run_id, dry_run=args.dry_run)
    elif args.step == "load":
        task_load_api_to_bigquery(run_id=args.run_id, dry_run=args.dry_run)
