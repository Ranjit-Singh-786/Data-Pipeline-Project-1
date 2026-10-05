import pytest
pytest.importorskip("airflow")

from dags.insurance_pipeline_dag import dag

def test_gcs_modular_dag_configuration():
    assert dag.dag_id == "insurance_batch_pipeline_gcs"
    expected_tasks = {"extract_and_transform", "upload_to_gcs", "load_to_bigquery"}
    assert {task.task_id for task in dag.tasks} == expected_tasks
    assert dag.default_args["retries"] == 3
    assert dag.catchup is False

def test_dag_dependency_chain():
    extract_task = dag.get_task("extract_and_transform")
    upload_task = dag.get_task("upload_to_gcs")
    load_task = dag.get_task("load_to_bigquery")

    assert upload_task in extract_task.downstream_list
    assert load_task in upload_task.downstream_list
