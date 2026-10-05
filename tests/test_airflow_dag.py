import pytest
pytest.importorskip("airflow")

from dags.insurance_pipeline_dag import dag

def test_gcs_production_dag_configuration():
    assert dag.dag_id == "insurance_batch_pipeline_gcs"
    assert {task.task_id for task in dag.tasks} == {"mysql_python_gcs_bigquery"}
    assert dag.default_args["retries"] == 3
    assert dag.catchup is False

