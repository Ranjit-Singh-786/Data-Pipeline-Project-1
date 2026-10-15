import json
from pathlib import Path
import pytest
from pipeline.run_api_pipeline import (
    validate_and_transform_change_actions,
    fetch_change_actions,
    CHANGE_ACTION_SCHEMA,
    write_parquet,
    task_extract_to_bronze_gcs,
    task_transform_bronze_to_silver,
    task_load_silver_to_bigquery,
)
from pipeline.config import Settings

def test_validate_and_transform_change_actions_valid():
    raw = [
        {"source": "3DSPACE", "type": "Change Action", "identifier": "10001", "relativePath": "path/10001", "state": "In Work"},
        {"source": "3DSPACE", "type": "Change Action", "identifier": "10002", "relative_path": "path/10002", "state": "Complete"},
    ]
    clean, rejected = validate_and_transform_change_actions(raw)
    assert len(clean) == 2
    assert len(rejected) == 0
    assert clean[0]["identifier"] == "10001"
    assert clean[0]["relative_path"] == "path/10001"

def test_validate_and_transform_change_actions_with_invalid():
    raw = [
        {"source": "3DSPACE", "type": "Change Action", "identifier": "", "state": "In Work"}, # missing identifier
        {"source": "3DSPACE", "type": "Change Action", "identifier": "10003", "state": ""}, # missing state
        {"source": "3DSPACE", "type": "Change Action", "identifier": "10004", "state": "In Approval"},
    ]
    clean, rejected = validate_and_transform_change_actions(raw)
    assert len(clean) == 1
    assert len(rejected) == 2
    assert clean[0]["identifier"] == "10004"

def test_fetch_change_actions_from_sample_json(tmp_path):
    sample_file = tmp_path / "test_sample.json"
    sample_data = {
        "changeAction": [
            {"source": "3DSPACE", "type": "Change Action", "identifier": "9999", "relativePath": "path/9999", "state": "In Work"}
        ]
    }
    sample_file.write_text(json.dumps(sample_data), encoding="utf-8")
    settings = Settings(sample_json_path=str(sample_file), change_action_api_url="")
    
    from pipeline.logging_utils import get_logger
    logger = get_logger("test_api_fetch", log_dir=str(tmp_path))
    items = fetch_change_actions(settings, logger)
    assert len(items) == 1
    assert items[0]["identifier"] == "9999"

def test_write_parquet_schema(tmp_path):
    records = [{"source": "3DSPACE", "type": "Change Action", "identifier": "1", "relative_path": "p/1", "state": "In Work"}]
    pq_path = tmp_path / "part.parquet"
    write_parquet(records, pq_path)
    import pyarrow.parquet as pq
    assert pq.read_schema(pq_path) == CHANGE_ACTION_SCHEMA

def test_change_action_dag_structure():
    pytest.importorskip("airflow")
    from dags.change_action_pipeline_dag import dag
    assert dag.dag_id == "change_action_api_pipeline_gcs"
    assert {t.task_id for t in dag.tasks} == {
        "extract_to_bronze", "transform_bronze_to_silver", "load_silver_to_bigquery"
    }

def test_medallion_pipeline_dry_run():
    # Bronze step
    bronze_res = task_extract_to_bronze_gcs(run_id="test_medallion_run", dry_run=True, date_str="15102026")
    assert "bronze/15102026/rawfile.json" in bronze_res["bronze_uri"]
    assert Path(bronze_res["raw_file"]).exists()

    # Silver step
    silver_res = task_transform_bronze_to_silver(
        run_id="test_medallion_run",
        dry_run=True,
        bronze_uri=bronze_res["bronze_uri"]
    )
    assert "processed/15102026/processed_data.parquet" in silver_res["silver_uri"]
    assert silver_res["clean_rows"] > 0

    # Gold step (WRITE_TRUNCATE)
    gold_res = task_load_silver_to_bigquery(
        run_id="test_medallion_run",
        dry_run=True,
        silver_uri=silver_res["silver_uri"]
    )
    assert gold_res["status"] == "SUCCESS_DRY_RUN"
    assert gold_res["loaded_uris"] == 1
