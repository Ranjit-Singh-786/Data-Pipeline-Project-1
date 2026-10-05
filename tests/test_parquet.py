import pytest
pa = pytest.importorskip("pyarrow")
from pipeline.run_pipeline import write_parquet, SCHEMA

def test_parquet_has_expected_schema(tmp_path):
    path = tmp_path / "part.parquet"
    write_parquet([{"age": 19, "sex": "female", "bmi": 27.9, "children": 0,
                    "smoker": "yes", "region": "southwest", "charges": 10.5}], path)
    import pyarrow.parquet as pq
    assert pq.read_schema(path) == SCHEMA
