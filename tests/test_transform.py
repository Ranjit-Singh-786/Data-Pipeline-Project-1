from pipeline.transform import EXPECTED_COLUMNS, validate_and_transform

def test_valid_row_is_cleaned_and_typed():
    rows = [("19", " FEMALE ", "27.9", "0", "YES", "Southwest", "16884.924")]
    clean, rejected = validate_and_transform(rows, EXPECTED_COLUMNS)
    assert rejected == []
    assert clean == [{"age": 19, "sex": "female", "bmi": 27.9, "children": 0,
                      "smoker": "yes", "region": "southwest", "charges": 16884.924}]

def test_invalid_rows_are_rejected_without_stopping_batch():
    rows = [(19, "female", 27.9, 0, "yes", "southwest", 10),
            (999, "female", 27.9, 0, "yes", "southwest", 10),
            (20, "unknown", 27.9, 0, "yes", "southwest", 10)]
    clean, rejected = validate_and_transform(rows, EXPECTED_COLUMNS)
    assert len(clean) == 1
    assert len(rejected) == 2

def test_column_mismatch_fails_fast():
    try:
        validate_and_transform([], ["age"])
    except ValueError as exc:
        assert "Invalid columns" in str(exc)
    else:
        raise AssertionError("Expected schema validation error")
