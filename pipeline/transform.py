EXPECTED_COLUMNS = ["age", "sex", "bmi", "children", "smoker", "region", "charges"]

def validate_and_transform(rows, columns):
    if list(columns) != EXPECTED_COLUMNS:
        raise ValueError(f"Invalid columns: expected {EXPECTED_COLUMNS}, got {list(columns)}")
    cleaned = []
    rejected = []
    for row_number, row in enumerate(rows, 1):
        try:
            record = dict(zip(columns, row))
            record["age"] = int(record["age"])
            record["bmi"] = float(record["bmi"])
            record["children"] = int(record["children"])
            record["charges"] = float(record["charges"])
            for field in ("sex", "smoker", "region"):
                record[field] = str(record[field]).strip().lower()
            if not (0 < record["age"] <= 120 and record["bmi"] > 0 and record["children"] >= 0 and record["charges"] >= 0):
                raise ValueError("range validation failed")
            if record["sex"] not in {"male", "female"} or record["smoker"] not in {"yes", "no"}:
                raise ValueError("categorical validation failed")
            if record["region"] not in {"southwest", "southeast", "northwest", "northeast"}:
                raise ValueError("region validation failed")
            cleaned.append(record)
        except (TypeError, ValueError, KeyError) as exc:
            rejected.append({"row": row_number, "reason": str(exc), "raw": list(row)})
    return cleaned, rejected
