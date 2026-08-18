import great_expectations as gx

from src.tlc.expectations import critical_expectations, log_only_expectations


def test_critical_expectations_yellow_checks_dropoff_columns():
    expectations = critical_expectations("yellow")

    columns = {e.column for e in expectations}
    assert columns == {"tpep_dropoff_datetime", "DOLocationID"}
    assert all(isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations)


def test_critical_expectations_fhv_uses_fhv_column_names():
    expectations = critical_expectations("fhv")

    columns = {e.column for e in expectations}
    assert columns == {"dropOff_datetime", "DOlocationID"}


def test_log_only_expectations_yellow_includes_passenger_and_distance_checks():
    expectations = log_only_expectations("yellow")

    types_and_columns = {
        (type(e).__name__, getattr(e, "column", None)) for e in expectations
    }
    assert ("ExpectColumnValuesToBeBetween", "passenger_count") in types_and_columns
    assert ("ExpectColumnValuesToBeBetween", "trip_distance") in types_and_columns


def test_log_only_expectations_fhv_excludes_passenger_and_distance_checks():
    expectations = log_only_expectations("fhv")

    columns = {getattr(e, "column", None) for e in expectations}
    assert "passenger_count" not in columns
    assert "trip_distance" not in columns


def test_log_only_expectations_fhvhv_checks_trip_miles_as_trip_distance():
    expectations = log_only_expectations("fhvhv")

    columns = {getattr(e, "column", None) for e in expectations}
    assert "trip_miles" in columns
    assert "passenger_count" not in columns


def test_log_only_expectations_includes_row_count_check():
    expectations = log_only_expectations("yellow")

    assert any(
        isinstance(e, gx.expectations.ExpectTableRowCountToBeBetween)
        for e in expectations
    )
